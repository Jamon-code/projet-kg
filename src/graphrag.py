"""Mode 2 : GRAPHRAG.

On relie la question a des entites du graphe, on recupere un sous-graphe pertinent
(faits a 1-2 sauts, avec leur provenance), puis DeepSeek genere la reponse en langage
naturel en s'appuyant UNIQUEMENT sur ces faits (et cite ses sources).
"""
import time
import config
import load_neo4j   # pour reutiliser normalize()


def _all_names(driver):
    with driver.session() as s:
        return [r["name"] for r in s.run(
            "MATCH (n) WHERE n.name IS NOT NULL RETURN DISTINCT n.name AS name")]


def link_entities(question, driver):
    """Trouve quelles entites du graphe sont citees dans la question (matching normalise)."""
    qn = load_neo4j.normalize(question).lower()
    found = []
    for nm in _all_names(driver):
        if nm and load_neo4j.normalize(nm).lower() in qn:
            found.append(nm)
    # garde les noms les plus longs d'abord (evite les doublons partiels)
    return sorted(set(found), key=len, reverse=True)


def retrieve_subgraph(entities, driver):
    """Recupere les faits (triplets) a 1-2 sauts autour des entites, avec provenance."""
    facts, seen = [], set()
    with driver.session() as s:
        for name in entities:
            rows = s.run("""
                MATCH p=(n {name:$name})
                      -[:DIRECTED|ACTED_IN|PRODUCED_BY|HAS_GENRE*1..2]-(m)
                WHERE m.name IS NOT NULL
                UNWIND relationships(p) AS r
                RETURN DISTINCT startNode(r).name AS a, type(r) AS rel,
                       endNode(r).name AS b, r.source AS source
            """, name=name)
            for r in rows:
                key = (r["a"], r["rel"], r["b"])
                if key not in seen:
                    seen.add(key)
                    facts.append({"a": r["a"], "rel": r["rel"],
                                  "b": r["b"], "source": r["source"]})
    return facts


def facts_to_text(facts):
    lines = []
    for f in facts:
        src = (" [source: %s]" % f["source"]) if f.get("source") else ""
        lines.append("- %s -%s-> %s%s" % (f["a"], f["rel"], f["b"], src))
    return "\n".join(lines)


GRAPHRAG_SYSTEM = (
    "Tu es un assistant cinema qui repond a une question UNIQUEMENT a partir des faits "
    "fournis (issus d'un graphe de connaissances). "
    "Ne te sers QUE de ces faits ; n'invente rien. Si l'information n'y est pas, "
    "dis clairement que tu ne sais pas. "
    "Reponds en francais, de maniere concise, et cite entre parentheses les documents "
    "sources ([source: ...]) qui justifient ta reponse."
)


def answer_graphrag(question, driver=None):
    close = False
    if driver is None:
        driver = config.get_driver()
        close = True
    t0 = time.time()
    entities = link_entities(question, driver)
    facts = retrieve_subgraph(entities, driver)
    context = facts_to_text(facts) or "(aucun fait pertinent trouve dans le graphe)"
    user = "Faits du graphe :\n%s\n\nQuestion : %s" % (context, question)
    answer = config.llm_text(GRAPHRAG_SYSTEM, user)
    dt = round(time.time() - t0, 2)
    if close:
        driver.close()
    return {"mode": "GraphRAG", "question": question, "entities": entities,
            "facts": facts, "context": context, "answer": answer, "latency_s": dt}


if __name__ == "__main__":
    import sys
    q = " ".join(sys.argv[1:]) or "Qui a realise Nebula ?"
    res = answer_graphrag(q)
    print("Entites:", res["entities"])
    print("Reponse:", res["answer"])
