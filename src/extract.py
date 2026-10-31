"""Extraction d'entites et de relations depuis le corpus, via DeepSeek (LLM)."""
import os
import re
import glob
import config

SYSTEM = (
    "Tu es un systeme d'extraction d'information pour un graphe de connaissances de cinema. "
    "A partir d'un texte, tu extrais UNIQUEMENT les entites et relations explicitement mentionnees. "
    "Types d'entites autorises : Film, Person, Studio, Genre. "
    "Types de relations autorises (respecte le sens de la fleche) : "
    "DIRECTED (Person -> Film), ACTED_IN (Person -> Film), "
    "PRODUCED_BY (Film -> Studio), HAS_GENRE (Film -> Genre). "
    "Reponds STRICTEMENT en JSON avec ce format exact : "
    '{"entities":[{"name":"...","type":"Film|Person|Studio|Genre"}], '
    '"relations":[{"source":"...","type":"DIRECTED|ACTED_IN|PRODUCED_BY|HAS_GENRE","target":"..."}]}. '
    "N'invente rien : si une information n'est pas dans le texte, ne la mets pas."
)


def _read_text(path):
    with open(path, encoding="utf-8") as f:
        txt = f.read()
    if path.lower().endswith((".html", ".htm")):
        txt = re.sub(r"<[^>]+>", " ", txt)      # retire les balises HTML
        txt = re.sub(r"\s+", " ", txt)
    return txt.strip()


def list_documents(corpus_dir="corpus"):
    paths = []
    for ext in ("*.txt", "*.html", "*.htm"):
        paths += glob.glob(os.path.join(corpus_dir, "**", ext), recursive=True)
    return sorted(paths)


def extract_document(path):
    """Extrait entites + relations d'un seul document, avec la provenance (nom du fichier)."""
    text = _read_text(path)
    try:
        data = config.llm_json(SYSTEM, "Texte a analyser :\n\n" + text)
    except Exception as e:
        print("  [!] Extraction echouee pour %s : %s" % (path, e))
        return {"entities": [], "relations": []}
    src = os.path.basename(path)
    for e in data.get("entities", []):
        e["source"] = src
    for r in data.get("relations", []):
        r["source_doc"] = src
    return data


def extract_corpus(corpus_dir="corpus"):
    """Parcourt tout le corpus et concatene les extractions."""
    all_entities, all_relations = [], []
    for path in list_documents(corpus_dir):
        print("  Extraction :", path)
        d = extract_document(path)
        all_entities += d.get("entities", [])
        all_relations += d.get("relations", [])
    return {"entities": all_entities, "relations": all_relations}
