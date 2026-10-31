"""Metriques PARTAGEES par le dashboard (calcul live) et evaluation.py (batch).

Regroupe au meme endroit les mesures qui repondent aux 5 questions du sujet :
  Q1  qualite d'extraction        -> relations_metrics / extraction_quality (precision, rappel, F1)
  Q2  effet de la resolution      -> extraction_quality (AVEC vs SANS + gain)
  Q3  Cypher vs GraphRAG          -> match_gt_question + score_cypher / score_graphrag
  Q4  cout d'inference            -> approx_tokens
  Q5  provenance                  -> (gere dans l'app : sources citees + noeuds Document)

Comme ca, le chiffre affiche "en direct" dans le dashboard et le chiffre du rapport
sont calcules par EXACTEMENT le meme code.
"""
import os
import json

import load_neo4j

GT_DIR = "ground_truth"

# Quelles clefs lire dans ground_truth/relations.json pour chaque type de relation.
GT_KEYS = {
    "DIRECTED": ("person", "film"),
    "ACTED_IN": ("person", "film"),
    "PRODUCED_BY": ("film", "studio"),
    "HAS_GENRE": ("film", "genre"),
}


def norm(x):
    """Cle canonique de comparaison : sans accents/ligatures, minuscules."""
    return load_neo4j.normalize(str(x)).lower()


def _surface(x):
    """Forme de SURFACE : minuscule + espaces normalises, mais accents/ligatures CONSERVES.
    C'est la forme brute telle que le LLM l'extrait du texte francais. On s'en sert comme
    baseline 'sans resolution d'entites' : elle ne reconcilie PAS 'Lumière Studios' (texte)
    avec 'Lumiere Studios' (base canonique)."""
    return " ".join(str(x).strip().lower().split())


# =================================================================== Q1 & Q2
def relations_metrics(extracted, use_resolution):
    """Compare les relations extraites a la verite terrain -> precision / rappel / F1.

    extracted : {"relations": [{"source","type","target",...}, ...]} (extraction brute).
    use_resolution :
      - True  : on resout chaque nom vers l'entite canonique (accents, ligatures, casse,
                ordre des mots, mots vides) puis on compare sur la cle canonique.
      - False : baseline SANS resolution -> on compare la forme de surface brute (accents
                conserves) a la base canonique (accent-free). Ecart = valeur apportee par
                la resolution d'entites.
    """
    gt = json.load(open(os.path.join(GT_DIR, "relations.json"), encoding="utf-8"))
    canon = load_neo4j.load_canonical()

    gt_set = set()
    for rt, (k1, k2) in GT_KEYS.items():
        for r in gt.get(rt, []):
            gt_set.add((rt, norm(r[k1]), norm(r[k2])))

    pred = set()
    for r in extracted.get("relations", []):
        rt = r.get("type")
        if rt not in load_neo4j.REL_ENDPOINT_TYPES:
            continue
        st, tt = load_neo4j.REL_ENDPOINT_TYPES[rt]
        s, t = r.get("source", ""), r.get("target", "")
        if use_resolution:
            s = load_neo4j.resolve(s, st, canon)
            t = load_neo4j.resolve(t, tt, canon)
            pred.add((rt, norm(s), norm(t)))
        else:
            pred.add((rt, _surface(s), _surface(t)))

    tp = len(gt_set & pred)
    fp = len(pred - gt_set)
    fn = len(gt_set - pred)
    P = tp / (tp + fp) if tp + fp else 0.0
    R = tp / (tp + fn) if tp + fn else 0.0
    F = 2 * P * R / (P + R) if P + R else 0.0
    return {"precision": round(P, 3), "rappel": round(R, 3), "f1": round(F, 3),
            "corrects": tp, "faux_positifs": fp, "manques": fn,
            "nb_verite_terrain": len(gt_set), "nb_predits": len(pred)}


def extraction_quality(extracted):
    """Q1 (F1 de l'extraction) + Q2 (effet de la resolution d'entites)."""
    sans = relations_metrics(extracted, use_resolution=False)
    avec = relations_metrics(extracted, use_resolution=True)
    return {
        "sans_resolution": sans,
        "avec_resolution": avec,
        "gain_f1_points": round(100 * (avec["f1"] - sans["f1"]), 1),
        "gain_rappel_points": round(100 * (avec["rappel"] - sans["rappel"]), 1),
    }


# =================================================================== Q4
def approx_tokens(*texts):
    """Estimation du cout d'inference : ~1 token pour 4 caracteres."""
    return sum(len(t or "") for t in texts) // 4


# =================================================================== Q3
_QCACHE = None


def questions():
    """Charge (une fois) les questions de reference / verite terrain."""
    global _QCACHE
    if _QCACHE is None:
        with open(os.path.join(GT_DIR, "questions.json"), encoding="utf-8") as f:
            _QCACHE = json.load(f)["questions"]
    return _QCACHE


def match_gt_question(question):
    """Si la question posee correspond a une question de reference, renvoie son item
    (avec 'expected' et 'category'), sinon None. -> permet l'exactitude AUTOMATIQUE."""
    qn = norm(question)
    for item in questions():
        if norm(item["question"]) == qn:
            return item
    return None


def score_cypher(expected, cypher_answer_list):
    """Exactitude + rappel de la reponse Cypher vs la reponse attendue."""
    E = {norm(x) for x in expected}
    P = {norm(x) for x in cypher_answer_list}
    inter = len(P & E)
    return {"exact": bool(E) and (P == E),
            "rappel": round(inter / len(E), 3) if E else 0.0}


def score_graphrag(expected, graphrag_answer_text):
    """Exactitude + rappel de la reponse GraphRAG (texte) vs la reponse attendue."""
    E = {norm(x) for x in expected}
    gtext = norm(graphrag_answer_text)
    found = sum(1 for e in E if e in gtext)
    return {"exact": bool(E) and (found == len(E)),
            "rappel": round(found / len(E), 3) if E else 0.0}
