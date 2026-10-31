"""Resolution d'entites + chargement du graphe dans Neo4j (avec provenance)."""
import os
import json
import csv as _csv
import unicodedata
import config

GT_DIR = "ground_truth"

REL_ENDPOINT_TYPES = {
    "DIRECTED": ("Person", "Film"),
    "ACTED_IN": ("Person", "Film"),
    "PRODUCED_BY": ("Film", "Studio"),
    "HAS_GENRE": ("Film", "Genre"),
}

# mots vides ignores lors de la comparaison des noms (entity resolution)
STOP = {"studios", "studio", "productions", "production", "films", "film",
        "pictures", "le", "la", "les", "de", "des", "du", "l"}


def normalize(s):
    """Normalise un nom : enleve accents/ligatures, espaces multiples."""
    if not s:
        return ""
    s = s.strip()
    s = s.replace("œ", "oe").replace("Œ", "OE")  # oe ligature
    s = s.replace("æ", "ae").replace("Æ", "AE")  # ae ligature
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return " ".join(s.split())


def _tokens(s):
    s = normalize(s).lower()
    s = "".join(c if (c.isalnum() or c.isspace()) else " " for c in s)
    return {t for t in s.split() if t and t not in STOP}


def load_canonical():
    """Charge la liste des entites canoniques (verite terrain) pour la resolution."""
    with open(os.path.join(GT_DIR, "entities.json"), encoding="utf-8") as f:
        gt = json.load(f)
    canon = {"Film": [], "Person": [], "Studio": [], "Genre": []}
    canon["Film"] = [f["name"] for f in gt["films"]]
    canon["Person"] = [p["name"] for p in gt["persons"]]
    canon["Studio"] = list(gt["studios"])
    canon["Genre"] = list(gt["genres"])
    return canon


def resolve(name, etype, canon):
    """Associe un nom extrait a l'entite canonique la plus proche (entity resolution)."""
    cands = canon.get(etype, [])
    n = normalize(name).lower()
    # 1) correspondance normalisee exacte
    for c in cands:
        if normalize(c).lower() == n:
            return c
    # 2) correspondance par chevauchement de tokens (gere casse, accents, ordre des mots)
    nt = _tokens(name)
    best, best_score = None, 0.0
    for c in cands:
        ct = _tokens(c)
        if not ct:
            continue
        score = len(nt & ct) / max(1, min(len(nt), len(ct)))
        if score > best_score:
            best, best_score = c, score
    if best_score >= 0.6:
        return best
    return name.strip()   # non resolu : on garde le nom tel quel


def resolve_extraction(extracted):
    """Applique la resolution d'entites a toute l'extraction brute."""
    canon = load_canonical()
    ents = []
    for e in extracted.get("entities", []):
        t = e.get("type")
        if t not in canon:
            continue
        ents.append({"name": resolve(e.get("name", ""), t, canon),
                     "type": t, "source": e.get("source")})
    rels = []
    for r in extracted.get("relations", []):
        rt = r.get("type")
        if rt not in REL_ENDPOINT_TYPES:
            continue
        st, tt = REL_ENDPOINT_TYPES[rt]
        rels.append({
            "source": resolve(r.get("source", ""), st, canon), "source_type": st,
            "type": rt,
            "target": resolve(r.get("target", ""), tt, canon), "target_type": tt,
            "source_doc": r.get("source_doc"),
        })
    return {"entities": ents, "relations": rels}


def clear_graph(driver):
    with driver.session() as s:
        s.run("MATCH (n) DETACH DELETE n")


def create_indexes(driver):
    """Cree les index de graphe Neo4j (case 'Graph indexes' du schema du prof).

    Un index par label sur la propriete `name` : accelere les recherches par nom
    utilisees par Cypher (filtres WHERE name=...) et par le GraphRAG (entity linking)."""
    for lb in ("Film", "Person", "Studio", "Genre", "Document"):
        with driver.session() as s:
            s.run("CREATE INDEX idx_%s_name IF NOT EXISTS FOR (n:%s) ON (n.name)"
                  % (lb.lower(), lb))


def load_graph(resolved, driver=None, csv_path="corpus/data/boxoffice.csv"):
    """Charge entites + relations dans Neo4j, avec provenance (noeud Document + MENTIONS)."""
    close = False
    if driver is None:
        driver = config.get_driver()
        close = True
    create_indexes(driver)          # case 'Graph indexes' du schema
    with driver.session() as s:
        # 1) noeuds
        for e in resolved["entities"]:
            s.run("MERGE (n:%s {name:$name})" % e["type"], name=e["name"])
        # 2) relations + provenance
        for r in resolved["relations"]:
            s.run(
                """
                MERGE (a:%s {name:$s})
                MERGE (b:%s {name:$t})
                MERGE (a)-[rel:%s]->(b)
                SET rel.source = $doc
                MERGE (d:Document {name:$doc})
                MERGE (d)-[:MENTIONS]->(a)
                MERGE (d)-[:MENTIONS]->(b)
                """ % (r["source_type"], r["target_type"], r["type"]),
                s=r["source"], t=r["target"], doc=(r.get("source_doc") or "inconnu"),
            )
        # 3) enrichissement structure depuis le CSV (annee, box office, studio propre)
        if csv_path and os.path.exists(csv_path):
            with open(csv_path, encoding="utf-8") as f:
                for row in _csv.DictReader(f):
                    s.run(
                        """
                        MERGE (f:Film {name:$film})
                        SET f.annee = toInteger($annee),
                            f.box_office_millions = toInteger($bo)
                        MERGE (st:Studio {name:$studio})
                        MERGE (f)-[:PRODUCED_BY]->(st)
                        """,
                        film=row["film"], annee=row["annee"],
                        bo=row["box_office_millions"], studio=row["studio"],
                    )
    if close:
        driver.close()
