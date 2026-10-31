"""Mode 1 : GRAPH ANALYTICS.

Deux fonctions :
- answer_cypher(question) : traduit la question en Cypher (via DeepSeek), l'execute
  sur Neo4j, et renvoie la reponse EXACTE calculee par la base.
- graph_analytics_stats() : applique les algorithmes GDS (PageRank + communautes)
  sur tout le graphe (pour l'exploration / la demo).
"""
import time
import config

SCHEMA = """
Noeuds :
  (:Film {name, annee, box_office_millions})
  (:Person {name})
  (:Studio {name})
  (:Genre {name})
Relations :
  (:Person)-[:DIRECTED]->(:Film)
  (:Person)-[:ACTED_IN]->(:Film)
  (:Film)-[:PRODUCED_BY]->(:Studio)
  (:Film)-[:HAS_GENRE]->(:Genre)
"""

CYPHER_SYSTEM = (
    "Tu traduis une question en UNE seule requete Cypher en lecture seule pour Neo4j.\n"
    "Schema du graphe :\n" + SCHEMA + "\n"
    "Les genres possibles sont exactement : Science-fiction, Thriller, Drame, Comedie, Aventure.\n"
    "Regles :\n"
    "- Utilise EXACTEMENT les labels et types de relation ci-dessus.\n"
    "- Pour filtrer sur un nom ou une valeur texte, NE mets PAS {name:'...'} dans le motif. "
    "Utilise une comparaison insensible a la casse : WHERE toLower(x.name) = toLower('...') "
    "(ex : WHERE toLower(g.name) = toLower('science-fiction')).\n"
    "- Les noms dans la base sont sans accents (ex : 'Lea Vidal', 'Coeurs de Sable').\n"
    "- Nomme la colonne de resultat 'reponse'.\n"
    "- Pas de MERGE/CREATE/DELETE : lecture seule.\n"
    "Reponds STRICTEMENT en JSON : {\"cypher\": \"...\"}."
)


def question_to_cypher(question):
    data = config.llm_json(CYPHER_SYSTEM, "Question : " + question)
    return (data.get("cypher") or "").strip()


def answer_cypher(question, driver=None):
    close = False
    if driver is None:
        driver = config.get_driver()
        close = True
    t0 = time.time()
    cypher = question_to_cypher(question)
    answer, rows, error = [], [], None
    try:
        with driver.session() as s:
            rows = [r.data() for r in s.run(cypher)]
        for r in rows:
            for v in r.values():
                answer.append(v)
    except Exception as e:
        error = str(e)
    dt = round(time.time() - t0, 2)
    if close:
        driver.close()
    return {"mode": "Graph Analytics (Cypher)", "question": question,
            "cypher": cypher, "answer": answer, "rows": rows,
            "error": error, "latency_s": dt}


def graph_analytics_stats(driver=None):
    """Algorithmes de graphe. Essaie d'abord Neo4j GDS (PageRank + communautes Louvain) ;
    si le plugin GDS n'est pas installe (ex. telechargement bloque par le reseau), bascule
    sur une CENTRALITE DE DEGRE calculee en Cypher pur (aucun plugin requis), pour que la
    demo fonctionne quand meme. Renvoie {"methode": ..., "rows": [...]}."""
    close = False
    if driver is None:
        driver = config.get_driver()
        close = True
    try:
        with driver.session() as s:
            s.run("CALL gds.graph.drop('kg', false)")
            s.run("""
                CALL gds.graph.project('kg',
                  ['Film','Person','Studio','Genre'],
                  {DIRECTED:{orientation:'UNDIRECTED'},
                   ACTED_IN:{orientation:'UNDIRECTED'},
                   PRODUCED_BY:{orientation:'UNDIRECTED'},
                   HAS_GENRE:{orientation:'UNDIRECTED'}})
            """)
            s.run("CALL gds.pageRank.write('kg', {writeProperty:'pagerank'})")
            s.run("CALL gds.louvain.write('kg', {writeProperty:'community'})")
            top = [r.data() for r in s.run("""
                MATCH (n) WHERE n.pagerank IS NOT NULL
                RETURN labels(n)[0] AS type, n.name AS name,
                       round(n.pagerank,3) AS pagerank, n.community AS communaute
                ORDER BY pagerank DESC LIMIT 10
            """)]
        result = {"methode": "Neo4j GDS (PageRank + communautes Louvain)", "rows": top}
    except Exception:
        # Repli sans plugin : centralite de degre en Cypher pur
        with driver.session() as s:
            top = [r.data() for r in s.run("""
                MATCH (n) WHERE n.name IS NOT NULL
                RETURN labels(n)[0] AS type, n.name AS name,
                       COUNT{ (n)--() } AS degre
                ORDER BY degre DESC LIMIT 10
            """)]
        result = {"methode": "Cypher — centralite de degre (plugin GDS non installe)",
                  "rows": top}
    if close:
        driver.close()
    return result


if __name__ == "__main__":
    import sys
    q = " ".join(sys.argv[1:]) or "Qui a realise Nebula ?"
    print(answer_cypher(q))
