"""Construit le knowledge graph cinema DE BOUT EN BOUT en une commande (backup CLI du
bouton 'Lancer le pipeline' de l'app).

    Crawl (corpus) -> HDFS -> Extraction LLM + resolution -> Spark -> Neo4j

Affiche aussi Q1 (F1 extraction) et Q2 (effet resolution) puisqu'on tourne sur le corpus
cinema (verite terrain disponible), et met en cache l'extraction brute dans
data/extracted.json (reutilise ensuite par evaluation.py, sans re-appeler le LLM).

Usage (dans le conteneur cine-ai, depuis ~/work) :
    python run_pipeline.py
"""
import sys
import os
import glob
import json

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))
import pipeline   # noqa: E402


def main():
    files = sorted(glob.glob("corpus/**/*.txt", recursive=True)
                   + glob.glob("corpus/**/*.html", recursive=True))
    sources = [{"type": "file", "value": f} for f in files]
    print("Sources : %d documents du corpus" % len(sources))

    res = pipeline.run_full_pipeline(sources, clear=True, compute_quality=True)
    for line in res["log"]:
        print("  -", line)

    # cache de l'extraction brute pour evaluation.py (evite de re-appeler DeepSeek)
    os.makedirs("data", exist_ok=True)
    json.dump(res["extracted"], open("data/extracted.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=2)

    print("\nDocuments : %d | relations avant Spark : %d | apres dedup : %d | duree : %ss"
          % (res["nb_docs"], res["relations_avant_spark"],
             res["relations_apres_spark"], res["duree_s"]))
    if res.get("qualite"):
        q = res["qualite"]
        print("Q1  exactitude extraction (F1 avec resolution) : %.0f%%"
              % (100 * q["avec_resolution"]["f1"]))
        print("Q2  effet de la resolution d'entites           : +%.1f points de F1"
              % q["gain_f1_points"])
    print("\nGraphe construit. Ouvre Neo4j Browser (http://localhost:7474) : MATCH (n) RETURN n")


if __name__ == "__main__":
    main()
