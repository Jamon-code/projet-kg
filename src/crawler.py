"""Crawler : collecte de sources (URL / fichier / texte), parsing, metadonnees,
puis pipeline live (crawl -> extraction LLM -> resolution -> Neo4j).

C'est la case 'Crawler (Collection, Parsing, Metadata)' de l'architecture.
"""
import os
import re
import time
import datetime
import urllib.request

import config
import load_neo4j
from extract import SYSTEM as EXTRACT_SYSTEM


def _strip_html(html):
    html = re.sub(r"<script.*?</script>", " ", html, flags=re.S | re.I)
    html = re.sub(r"<style.*?</style>", " ", html, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", html)
    return re.sub(r"\s+", " ", text).strip()


def fetch_url(url):
    """Telecharge une page web et renvoie son texte nettoye."""
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        html = r.read().decode("utf-8", "ignore")
    return _strip_html(html)


def crawl(sources):
    """sources : liste de {"type": "url"|"file"|"text", "value": ..., "name": ...}.
    Renvoie (docs, manifest). docs = [{name, text}], manifest = metadonnees."""
    docs, manifest = [], []
    for s in sources:
        t, v = s.get("type"), s.get("value", "")
        try:
            if t == "url":
                text, name, origine = fetch_url(v), v, v
            elif t == "file":
                with open(v, encoding="utf-8") as f:
                    raw = f.read()
                text = _strip_html(raw) if v.lower().endswith((".html", ".htm")) else raw
                name, origine = os.path.basename(v), v
            else:  # text colle
                text, name, origine = v, s.get("name", "texte_colle"), "saisie_manuelle"
        except Exception as e:
            text, name, origine = "", s.get("name", str(v)), "ERREUR: %s" % e
        docs.append({"name": name, "text": text})
        manifest.append({
            "source": name, "type": t, "origine": origine,
            "taille_caracteres": len(text),
            "date_ingestion": datetime.datetime.now().isoformat(timespec="seconds"),
        })
    return docs, manifest


def extract_from_docs(docs, max_chars=6000):
    """Extraction LLM (entites + relations) sur des documents deja crawles."""
    ents, rels = [], []
    for d in docs:
        if not d["text"].strip():
            continue
        try:
            data = config.llm_json(EXTRACT_SYSTEM, "Texte a analyser :\n\n" + d["text"][:max_chars])
        except Exception:
            data = {"entities": [], "relations": []}
        for e in data.get("entities", []):
            e["source"] = d["name"]
        for r in data.get("relations", []):
            r["source_doc"] = d["name"]
        ents += data.get("entities", [])
        rels += data.get("relations", [])
    return {"entities": ents, "relations": rels}


def run_pipeline(sources, driver=None, clear=True):
    """Pipeline complet LIVE : crawl -> extraction -> resolution -> Neo4j."""
    t0 = time.time()
    docs, manifest = crawl(sources)
    extracted = extract_from_docs(docs)
    resolved = load_neo4j.resolve_extraction(extracted)
    close = False
    if driver is None:
        driver = config.get_driver()
        close = True
    if clear:
        load_neo4j.clear_graph(driver)
    load_neo4j.load_graph(resolved, driver=driver, csv_path=None)
    if close:
        driver.close()
    return {
        "manifest": manifest,
        "nb_docs": len(docs),
        "extracted": extracted,
        "resolved": resolved,
        "duree_s": round(time.time() - t0, 2),
    }
