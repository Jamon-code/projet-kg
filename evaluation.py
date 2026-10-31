"""Evaluation BATCH — repond aux 5 questions du sujet, en 'un seul coup' (pour le rapport).

C'est la version hors-ligne du Dashboard de l'app : les deux utilisent EXACTEMENT le meme
code de mesure (src/metrics.py), donc les chiffres sont identiques.

  Q1 : qualite d'extraction (precision / rappel / F1)
  Q2 : effet de la resolution d'entites (AVEC vs SANS + gain)
  Q3 : Graph Analytics (Cypher) vs GraphRAG (exactitude + rappel par categorie)
  Q4 : compromis -> latence + cout d'inference (tokens approx)
  Q5 : provenance (sources citees par GraphRAG + noeuds Document/MENTIONS)

Usage (dans le conteneur cine-ai, depuis ~/work) :
    python evaluation.py
Produit : data/evaluation.json + data/evaluation_qa.csv
"""
import sys
import os
import json
import csv

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))
import config          # noqa: E402
import analytics       # noqa: E402
import graphrag        # noqa: E402
import extract         # noqa: E402
import metrics         # noqa: E402


def get_extracted():
    """Recupere l'extraction brute du corpus (cache dans data/extracted.json)."""
    path = "data/extracted.json"
    if os.path.exists(path):
        return json.load(open(path, encoding="utf-8"))
    print("  (data/extracted.json absent -> extraction du corpus via DeepSeek...)")
    ext = extract.extract_corpus("corpus")
    os.makedirs("data", exist_ok=True)
    json.dump(ext, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    return ext


def evaluate_qa(driver):
    rows = []
    for item in metrics.questions():
        q, exp = item["question"], item["expected"]
        a = analytics.answer_cypher(q, driver)
        g = graphrag.answer_graphrag(q, driver)
        cs = metrics.score_cypher(exp, a["answer"])
        gs = metrics.score_graphrag(exp, g["answer"])
        nsrc = len({f.get("source") for f in g["facts"] if f.get("source")})
        rows.append({
            "id": item["id"], "categorie": item["category"],
            "cypher_exact": cs["exact"], "cypher_rappel": cs["rappel"],
            "cypher_latence_s": a["latency_s"],
            "cypher_tokens": metrics.approx_tokens(analytics.CYPHER_SYSTEM, q),
            "graphrag_exact": gs["exact"], "graphrag_rappel": gs["rappel"],
            "graphrag_latence_s": g["latency_s"],
            "graphrag_tokens": metrics.approx_tokens(graphrag.GRAPHRAG_SYSTEM, g["context"], q),
            "graphrag_sources": nsrc,
        })
    return rows


def main():
    os.makedirs("data", exist_ok=True)

    print("== Q1 & Q2 : Extraction — effet de la resolution d'entites ==")
    gq = metrics.extraction_quality(get_extracted())
    print("  SANS resolution :", gq["sans_resolution"])
    print("  AVEC resolution :", gq["avec_resolution"])
    print("  -> gain de F1 grace a la resolution : +%s points (rappel +%s points)"
          % (gq["gain_f1_points"], gq["gain_rappel_points"]))

    print("\n== Q3 & Q4 : Graph Analytics vs GraphRAG (exactitude + latence + cout) ==")
    driver = config.get_driver()
    rows = evaluate_qa(driver)
    n = len(rows) or 1
    agg = {
        "cypher_exactitude": round(sum(r["cypher_exact"] for r in rows) / n, 3),
        "graphrag_exactitude": round(sum(r["graphrag_exact"] for r in rows) / n, 3),
        "cypher_rappel_moy": round(sum(r["cypher_rappel"] for r in rows) / n, 3),
        "graphrag_rappel_moy": round(sum(r["graphrag_rappel"] for r in rows) / n, 3),
        "cypher_latence_moy_s": round(sum(r["cypher_latence_s"] for r in rows) / n, 2),
        "graphrag_latence_moy_s": round(sum(r["graphrag_latence_s"] for r in rows) / n, 2),
        "cypher_tokens_moy": round(sum(r["cypher_tokens"] for r in rows) / n),
        "graphrag_tokens_moy": round(sum(r["graphrag_tokens"] for r in rows) / n),
        "graphrag_sources_moy": round(sum(r["graphrag_sources"] for r in rows) / n, 2),
    }
    for r in rows:
        print("  [%-4s %-20s] Cypher exact=%-5s %5ss | GraphRAG exact=%-5s %5ss"
              % (r["id"], r["categorie"], r["cypher_exact"], r["cypher_latence_s"],
                 r["graphrag_exact"], r["graphrag_latence_s"]))
    print("\n== Synthese ==")
    for k, v in agg.items():
        print("  %-24s %s" % (k, v))
    ratio = round(agg["graphrag_tokens_moy"] / max(1, agg["cypher_tokens_moy"]), 1)
    print("  cout d'inference        1 appel LLM / question pour chaque mode ; "
          "GraphRAG ~%sx plus de tokens (il envoie le sous-graphe au LLM)" % ratio)
    print("  Q5 provenance           GraphRAG cite en moyenne %s source(s) par reponse"
          % agg["graphrag_sources_moy"])

    out = {"extraction": gq, "qa": rows, "aggregates": agg}
    json.dump(out, open("data/evaluation.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=2)
    with open("data/evaluation_qa.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    driver.close()
    print("\nSauvegarde : data/evaluation.json + data/evaluation_qa.csv")


if __name__ == "__main__":
    main()
