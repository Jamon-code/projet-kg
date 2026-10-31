"""API REST (Phase 7 du sujet : API + monitoring + controle d'acces).

Expose, derriere une cle d'acces, les services du projet :
  - service d'extraction LLM        : POST /extract
  - Graph Analytics (Cypher)        : POST /ask/cypher
  - GraphRAG                        : POST /ask/graphrag
  - pipeline complet (crawl->Neo4j) : POST /pipeline
  - etat du graphe / index          : GET  /graph/stats, /graph/indexes

Monitoring : chaque requete est chronometree et comptee (GET /metrics, /metrics/prometheus),
plus un /health et un healthcheck Docker.
Controle d'acces : les endpoints de donnees exigent l'en-tete  X-API-Key: <API_KEY>.

Lancee automatiquement par docker-compose (service cine-api, port 8000).
Documentation interactive : http://localhost:8000/docs
"""
import os
import sys
import time
import logging

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

from fastapi import FastAPI, Header, HTTPException, Depends   # noqa: E402
from fastapi.responses import PlainTextResponse               # noqa: E402
from pydantic import BaseModel                                # noqa: E402

import config          # noqa: E402
import extract         # noqa: E402
import analytics       # noqa: E402
import graphrag        # noqa: E402
import pipeline        # noqa: E402
import load_neo4j      # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("cine-api")

API_KEY = os.environ.get("API_KEY", "cine-demo-key")

app = FastAPI(
    title="Cinema Knowledge Graph API",
    version="1.0",
    description="Service LLM extraction + GraphRAG + Graph Analytics (Phase 7 : API, "
                "monitoring, controle d'acces).",
)

# --------------------------------------------------------------- monitoring
METRICS = {}  # chemin -> {count, errors, latence_totale}


def _record(path, dt, err=False):
    m = METRICS.setdefault(path, {"count": 0, "errors": 0, "latence_totale": 0.0})
    m["count"] += 1
    m["latence_totale"] += dt
    if err:
        m["errors"] += 1


@app.middleware("http")
async def monitor(request, call_next):
    t0 = time.time()
    try:
        resp = await call_next(request)
    except Exception:
        _record(request.url.path, time.time() - t0, err=True)
        log.exception("ERREUR %s", request.url.path)
        raise
    dt = time.time() - t0
    _record(request.url.path, dt, err=(resp.status_code >= 500))
    resp.headers["X-Process-Time"] = "%.3f" % dt
    log.info("%s %s -> %s (%.3fs)", request.method, request.url.path, resp.status_code, dt)
    return resp


# --------------------------------------------------------------- acces
def require_key(x_api_key: str = Header(default="")):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401,
                            detail="Cle API invalide ou absente (en-tete X-API-Key).")
    return True


# --------------------------------------------------------------- driver partage
_driver = None


@app.on_event("startup")
def _startup():
    global _driver
    _driver = config.get_driver()
    try:
        load_neo4j.create_indexes(_driver)
        log.info("Index Neo4j prets.")
    except Exception as e:
        log.warning("Creation des index differee : %s", e)


@app.on_event("shutdown")
def _shutdown():
    if _driver is not None:
        _driver.close()


# --------------------------------------------------------------- schemas
class Question(BaseModel):
    question: str


class TextIn(BaseModel):
    text: str


class Source(BaseModel):
    type: str
    value: str
    name: str = ""


class PipelineIn(BaseModel):
    sources: list
    clear: bool = True
    compute_quality: bool = False


def _as_dict(m):
    return m.model_dump() if hasattr(m, "model_dump") else m.dict()


# --------------------------------------------------------------- endpoints ouverts
@app.get("/health")
def health():
    detail = {}
    ok = True
    try:
        with _driver.session() as s:
            s.run("RETURN 1").consume()
        detail["neo4j"] = "ok"
    except Exception as e:
        ok = False
        detail["neo4j"] = str(e)[:80]
    return {"status": "ok" if ok else "degraded", **detail}


@app.get("/metrics")
def metrics_json():
    out = {}
    for path, m in METRICS.items():
        out[path] = {"count": m["count"], "errors": m["errors"],
                     "latence_moy_s": round(m["latence_totale"] / m["count"], 3) if m["count"] else 0}
    return {"endpoints": out}


@app.get("/metrics/prometheus", response_class=PlainTextResponse)
def metrics_prometheus():
    lines = []
    for path, m in METRICS.items():
        lab = 'path="%s"' % path
        avg = (m["latence_totale"] / m["count"]) if m["count"] else 0
        lines.append("api_requests_total{%s} %d" % (lab, m["count"]))
        lines.append("api_errors_total{%s} %d" % (lab, m["errors"]))
        lines.append("api_latency_seconds_avg{%s} %.4f" % (lab, avg))
    return "\n".join(lines) + "\n"


# --------------------------------------------------------------- endpoints proteges
@app.post("/extract", dependencies=[Depends(require_key)])
def api_extract(inp: TextIn):
    """Service d'extraction LLM : texte brut -> entites + relations."""
    return config.llm_json(extract.SYSTEM, "Texte a analyser :\n\n" + inp.text)


@app.post("/ask/cypher", dependencies=[Depends(require_key)])
def api_cypher(q: Question):
    """Graph Analytics : question -> Cypher -> reponse exacte."""
    return analytics.answer_cypher(q.question, _driver)


@app.post("/ask/graphrag", dependencies=[Depends(require_key)])
def api_graphrag(q: Question):
    """GraphRAG : question -> sous-graphe -> reponse LLM sourcee."""
    return graphrag.answer_graphrag(q.question, _driver)


@app.post("/pipeline", dependencies=[Depends(require_key)])
def api_pipeline(inp: PipelineIn):
    """Pipeline complet Crawl -> HDFS -> Extraction -> Spark -> Neo4j."""
    sources = [s if isinstance(s, dict) else _as_dict(s) for s in inp.sources]
    res = pipeline.run_full_pipeline(sources, driver=_driver,
                                     clear=inp.clear, compute_quality=inp.compute_quality)
    res.pop("extracted", None)  # trop volumineux pour la reponse HTTP
    return res


@app.get("/graph/stats", dependencies=[Depends(require_key)])
def api_stats():
    with _driver.session() as s:
        nodes = [r.data() for r in s.run(
            "MATCH (n) RETURN labels(n)[0] AS type, count(*) AS nb ORDER BY type")]
        rels = [r.data() for r in s.run(
            "MATCH ()-[r]->() RETURN type(r) AS type, count(*) AS nb ORDER BY type")]
    return {"noeuds": nodes, "relations": rels}


@app.get("/graph/indexes", dependencies=[Depends(require_key)])
def api_indexes():
    with _driver.session() as s:
        idx = [r.data() for r in s.run(
            "SHOW INDEXES YIELD name, labelsOrTypes, properties "
            "RETURN name, labelsOrTypes, properties")]
    return {"indexes": idx}
