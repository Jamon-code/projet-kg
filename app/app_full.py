"""Application de demonstration — STACK COMPLET + DASHBOARD CUMULATIF.

Tout le pipeline du sujet tourne dans ce stack :
    Sources -> Crawler -> HDFS -> Extraction LLM + resolution -> Spark -> Neo4j
            -> Graph Analytics (Cypher + GDS)  vs  GraphRAG

A CHAQUE question posee, le Dashboard met a jour (en cumule) TOUS les indicateurs
qui repondent aux 5 questions du sujet : exactitude, rappel, latence, cout d'inference
(tokens), accord des approches, provenance. L'exactitude est AUTOMATIQUE quand la
question fait partie de la verite terrain, et via 👍/👎 pour toute autre question.

Lancement (dans le conteneur cine-ai, depuis ~/work) :
    streamlit run app/app_full.py --server.port 8501 --server.address 0.0.0.0
"""
import sys
import os
import glob

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))
import streamlit as st     # noqa: E402
import config              # noqa: E402
import analytics           # noqa: E402
import graphrag            # noqa: E402
import pipeline            # noqa: E402
import load_neo4j          # noqa: E402
import metrics             # noqa: E402

st.set_page_config(page_title="KG Cinema - pipeline complet", page_icon="🎬", layout="wide")


@st.cache_resource
def get_driver():
    return config.get_driver()


def norm(x):
    return metrics.norm(x)


def mean(xs):
    xs = [x for x in xs if x is not None]
    return (sum(xs) / len(xs)) if xs else 0.0


# ------------------------------------------------- etat cumulatif (session)
if "metrics" not in st.session_state:
    st.session_state.metrics = []        # une ligne par question posee
if "last" not in st.session_state:
    st.session_state.last = None         # derniere ligne (pour le 👍/👎)
if "graph_quality" not in st.session_state:
    st.session_state.graph_quality = None  # Q1/Q2 mesures a la construction du graphe


st.title("🎬 Knowledge Graph Cinema — pipeline complet")
st.caption("Crawl → HDFS → Extraction LLM + resolution → Spark (validation/dedup) → Neo4j → "
           "Graph Analytics (Cypher) vs GraphRAG — le Dashboard se remplit a chaque question.")

tabs = st.tabs(["🏠 Architecture", "📥 Sources & Pipeline", "🕸️ Knowledge Graph",
                "💬 Poser une question", "📊 Dashboard"])

# =============================================================== Architecture
with tabs[0]:
    st.subheader("Pipeline du projet (tout tourne dans ce stack)")
    st.graphviz_chart("""
    digraph {
      rankdir=TB; node [shape=box, style=rounded, fontname="Arial"];
      S  [label="Sources\\n(URLs, documents, texte)"];
      C  [label="Crawler\\n(collecte + parsing + metadonnees)"];
      H  [label="HDFS\\n(documents bruts)", style="rounded,filled", fillcolor="#fff2cc"];
      E  [label="Extraction LLM (DeepSeek)\\n+ resolution d'entites"];
      SP [label="Spark\\n(validation + deduplication)", style="rounded,filled", fillcolor="#ffe0cc"];
      N  [label="Neo4j\\n(Knowledge Graph + provenance)", style="rounded,filled", fillcolor="#cde4ff"];
      GA [label="Graph Analytics\\n(Cypher + GDS)", style="rounded,filled", fillcolor="#d9f2d9"];
      GR [label="GraphRAG\\n(sous-graphe + LLM)", style="rounded,filled", fillcolor="#ffe0cc"];
      D  [label="Dashboard\\n(comparaison cumulative)"];
      S -> C -> H -> E -> SP -> N; N -> GA; N -> GR; GA -> D; GR -> D;
    }
    """)
    st.markdown(
        "- **Batch (ingestion)** : l'onglet *Sources & Pipeline* execute toute la chaine "
        "**Crawl → HDFS → Extraction → Spark → Neo4j** sur les sources choisies. "
        "Si on change de sources, HDFS/Spark/Neo4j refletent les nouvelles donnees.\n"
        "- **Interactif (requetes)** : l'onglet *Poser une question* compare **Cypher** et "
        "**GraphRAG** sur le meme graphe, et **chaque question alimente le Dashboard**.")

# =============================================================== Sources & Pipeline
with tabs[1]:
    st.subheader("1) Choisis tes sources, puis lance le pipeline COMPLET")
    mode = st.radio("Type de source",
                    ["Corpus fourni (cinema)", "URL(s) a crawler", "Texte colle"])
    sources, is_cinema = [], False
    if mode == "Corpus fourni (cinema)":
        is_cinema = True
        files = sorted(glob.glob("corpus/**/*.txt", recursive=True)
                       + glob.glob("corpus/**/*.html", recursive=True))
        st.write("**%d documents** dans le corpus (articles, interviews, emails, communiques)." % len(files))
        with st.expander("Voir les fichiers"):
            st.write(files)
        sources = [{"type": "file", "value": f} for f in files]
    elif mode == "URL(s) a crawler":
        urls = st.text_area("Une URL par ligne",
                            "https://fr.wikipedia.org/wiki/Inception", height=120)
        sources = [{"type": "url", "value": u.strip()} for u in urls.splitlines() if u.strip()]
    else:
        txt = st.text_area("Colle ton texte", height=160,
                           value="Le realisateur Hugo Marchand a tourne le thriller Le Dernier Train "
                                 "pour Atlas Pictures, avec Sofia Marin et Rayan Delattre.")
        sources = [{"type": "text", "value": txt, "name": "texte_colle"}]

    st.caption("Astuce demo : le **corpus cinema** active en plus la mesure automatique de la "
               "qualite d'extraction (Q1) et de l'effet de la resolution (Q2) dans le Dashboard.")

    if st.button("🚀 Lancer le pipeline (Crawl → HDFS Bronze/Silver/Gold → Spark → Neo4j)",
                 type="primary"):
        try:
            with st.status("Pipeline complet en cours...", expanded=True) as status:
                res = pipeline.run_full_pipeline(sources, driver=get_driver(),
                                                 clear=True, compute_quality=is_cinema)
                for line in res["log"]:
                    st.write("• " + line)
                status.update(label="Pipeline termine en %ss" % res["duree_s"], state="complete")
            res.pop("extracted", None)
            st.session_state.pipeline_result = res
            if res.get("qualite"):
                st.session_state.graph_quality = res["qualite"]
        except Exception as e:
            st.error("Erreur : %s" % e)

    # ---- preuve d'execution (reste affichee meme en changeant d'onglet)
    pr = st.session_state.get("pipeline_result")
    if pr:
        st.divider()
        st.markdown("#### Preuve d'execution — architecture medaillon (HDFS + Spark)")
        pc = st.columns(3)
        pc[0].metric("Bronze — documents", pr["nb_docs"])
        pc[1].metric("Silver — relations validees", pr["relations_avant_spark"])
        pc[2].metric("Gold — relations dedup.", pr["relations_apres_spark"])
        st.table(pr["medallion"])
        if pr.get("hdfs_ok"):
            st.success("✅ HDFS : Bronze/Silver/Gold ecrits, et %d relations **relues depuis HDFS** "
                       "(preuve de persistance). Verifie : HDFS UI http://localhost:9870 → "
                       "Utilities → Browse the file system → /cinema"
                       % (pr.get("hdfs_readback") or pr["relations_apres_spark"]))
        else:
            st.warning("⚠️ HDFS n'a pas confirme l'ecriture (souvent : HDFS encore en *safe mode* au "
                       "demarrage). Le graphe est quand meme construit par Spark. Attends ~1 min que "
                       "le datanode soit actif (http://localhost:9870) puis relance le pipeline.")
        if pr.get("qualite"):
            qq = pr["qualite"]
            st.info("Qualite extraction : F1 **%.0f%%** avec resolution vs %.0f%% sans "
                    "→ detaille dans l'onglet Dashboard."
                    % (100 * qq["avec_resolution"]["f1"], 100 * qq["sans_resolution"]["f1"]))
        with st.expander("Log complet (Crawl → Bronze → Extraction → Silver → Gold → Neo4j)"):
            for line in pr["log"]:
                st.write("• " + line)
        with st.expander("Metadonnees du crawler (provenance)"):
            st.table(pr["manifest"])

# =============================================================== Knowledge Graph
with tabs[2]:
    st.subheader("Contenu du graphe + analyse GDS")
    if st.button("Analyser le graphe (comptes + PageRank + communautes)"):
        try:
            d = get_driver()
            with d.session() as s:
                nodes = [r.data() for r in s.run(
                    "MATCH (n) RETURN labels(n)[0] AS type, count(*) AS nb ORDER BY type")]
                rels = [r.data() for r in s.run(
                    "MATCH ()-[r]->() RETURN type(r) AS type, count(*) AS nb ORDER BY type")]
            c1, c2 = st.columns(2)
            c1.markdown("**Noeuds**"); c1.table(nodes)
            c2.markdown("**Relations**"); c2.table(rels)
            st.markdown("**Top 10 — algorithme de graphe (centralite)**")
            with st.spinner("Calcul..."):
                ga = analytics.graph_analytics_stats(d)
            st.caption("Methode : " + ga["methode"])
            st.table(ga["rows"])
            with st.expander("Index de graphe Neo4j (case 'Graph indexes' du schema)"):
                with d.session() as s:
                    idx = [r.data() for r in s.run(
                        "SHOW INDEXES YIELD name, labelsOrTypes, properties "
                        "RETURN name, labelsOrTypes, properties")]
                st.table(idx)
        except Exception as e:
            st.error("Erreur (Neo4j demarre ? graphe charge ?) : %s" % e)

# =============================================================== Poser une question
with tabs[3]:
    st.subheader("Une question, deux approches")
    src_q = st.radio("Source de la question",
                     ["📚 Question de reference (verite terrain)", "✍️ Question libre"],
                     horizontal=True)
    if src_q.startswith("📚"):
        item = st.selectbox("Choisis une question de reference", metrics.questions(),
                            format_func=lambda it: "%s — %s" % (it["id"], it["question"]))
        q = item["question"]
        st.caption("Categorie : **%s** · reponse attendue : **%s** "
                   "→ l'exactitude sera calculee automatiquement."
                   % (item["category"], ", ".join(item["expected"])))
    else:
        q = st.text_input("Ta question",
                          "Quel realisateur a dirige un film avec Marcus Bonnet ?")
        st.caption("Question libre : juge la reponse avec 👍/👎 pour alimenter l'exactitude terrain.")

    if st.button("Comparer Graph Analytics vs GraphRAG", type="primary"):
        d = get_driver()
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("### 🟢 Graph Analytics (Cypher)")
            with st.spinner("..."):
                a = analytics.answer_cypher(q, d)
            st.code(a["cypher"] or "(aucune requete)", language="cypher")
            if a["error"]:
                st.error(a["error"])
            st.success(", ".join(str(x) for x in a["answer"]) if a["answer"] else "(aucun resultat)")
            st.caption("⏱️ %.2f s · ~%d tokens" % (a["latency_s"],
                       metrics.approx_tokens(analytics.CYPHER_SYSTEM, q)))
        with col2:
            st.markdown("### 🟠 GraphRAG")
            with st.spinner("..."):
                g = graphrag.answer_graphrag(q, d)
            st.info(g["answer"])
            nsrc = len({f.get("source") for f in g["facts"] if f.get("source")})
            st.caption("⏱️ %.2f s · ~%d tokens · sources : %d · entites : %s"
                       % (g["latency_s"],
                          metrics.approx_tokens(graphrag.GRAPHRAG_SYSTEM, g["context"], q),
                          nsrc, ", ".join(g["entities"]) or "-"))
            with st.expander("Faits recuperes (contexte GraphRAG — provenance)"):
                st.text(g["context"])

        # --- accord des deux approches
        gtext = norm(g["answer"])
        agree = bool(a["answer"]) and all(norm(x) in gtext for x in a["answer"])

        # --- exactitude AUTOMATIQUE si la question est une question de reference
        gt = metrics.match_gt_question(q)
        cyc = metrics.score_cypher(gt["expected"], a["answer"]) if gt else None
        grc = metrics.score_graphrag(gt["expected"], g["answer"]) if gt else None

        rec = {
            "question": q,
            "cypher_ok": bool(a["answer"]),
            "cypher_latence": a["latency_s"],
            "graphrag_latence": g["latency_s"],
            "cypher_tokens": metrics.approx_tokens(analytics.CYPHER_SYSTEM, q),
            "graphrag_tokens": metrics.approx_tokens(graphrag.GRAPHRAG_SYSTEM, g["context"], q),
            "sources": nsrc,
            "accord": agree,
            "feedback": None,
            "has_gt": bool(gt),
            "categorie": gt["category"] if gt else "(libre)",
            "cypher_exact": cyc["exact"] if gt else None,
            "cypher_rappel": cyc["rappel"] if gt else None,
            "graphrag_exact": grc["exact"] if gt else None,
            "graphrag_rappel": grc["rappel"] if gt else None,
        }
        st.session_state.metrics.append(rec)
        st.session_state.last = rec

        st.markdown("**Accord des 2 approches : %s**" % ("✅ oui" if agree else "⚠️ non"))
        if gt:
            st.markdown("**Exactitude automatique (vs verite terrain)** — "
                        "Cypher : %s · GraphRAG : %s"
                        % ("✅" if cyc["exact"] else "❌", "✅" if grc["exact"] else "❌"))
        st.info("➡️ Indicateurs ajoutes au **Dashboard** (cumul : %d question(s))."
                % len(st.session_state.metrics))

    # --- feedback terrain 👍/👎 (pour toute question, surtout les libres)
    if st.session_state.last is not None:
        st.divider()
        st.write("Cette derniere reponse est-elle correcte ? (alimente l'exactitude terrain)")
        b1, b2, _ = st.columns([1, 1, 6])
        if b1.button("👍 correcte"):
            st.session_state.last["feedback"] = "ok"; st.success("Enregistre.")
        if b2.button("👎 fausse"):
            st.session_state.last["feedback"] = "ko"; st.warning("Enregistre.")

# =============================================================== Dashboard
with tabs[4]:
    top = st.columns([6, 1])
    top[0].subheader("📊 Dashboard — pipeline + comparaison cumulative")
    if top[1].button("↺ Reset questions"):
        st.session_state.metrics = []
        st.session_state.last = None
        st.rerun()

    gq = st.session_state.get("graph_quality")
    pr = st.session_state.get("pipeline_result")

    # ===== (toujours affiche) Pipeline execute : architecture medaillon =====
    if pr:
        st.markdown("#### Pipeline execute (architecture medaillon HDFS + Spark)")
        pcc = st.columns(3)
        pcc[0].metric("Bronze — documents", pr["nb_docs"])
        pcc[1].metric("Silver — relations validees", pr["relations_avant_spark"])
        pcc[2].metric("Gold — relations dedup.", pr["relations_apres_spark"])
        st.table(pr["medallion"])
        if pr.get("hdfs_ok"):
            st.caption("✅ Couches ecrites dans HDFS ; %d relations relues depuis HDFS (preuve)."
                       % (pr.get("hdfs_readback") or pr["relations_apres_spark"]))

    # ===== (toujours affiche) Qualite du graphe : Q1 extraction & Q2 resolution =====
    st.markdown("#### Qualite du graphe construit (Q1 extraction · Q2 resolution)")
    if not gq:
        st.caption("Lance le pipeline sur le **corpus cinema** (onglet Sources & Pipeline) "
                   "pour mesurer la qualite d'extraction et l'effet de la resolution.")
    else:
        s_, a_ = gq["sans_resolution"], gq["avec_resolution"]
        st.table([
            {"Mesure": "Precision", "Sans resolution": "%.0f%%" % (100 * s_["precision"]),
             "Avec resolution": "%.0f%%" % (100 * a_["precision"])},
            {"Mesure": "Rappel", "Sans resolution": "%.0f%%" % (100 * s_["rappel"]),
             "Avec resolution": "%.0f%%" % (100 * a_["rappel"])},
            {"Mesure": "F1", "Sans resolution": "%.0f%%" % (100 * s_["f1"]),
             "Avec resolution": "%.0f%%" % (100 * a_["f1"])},
            {"Mesure": "Relations correctes", "Sans resolution": s_["corrects"],
             "Avec resolution": a_["corrects"]},
        ])
        st.success("Effet de la resolution d'entites : **+%.1f points de F1** (+%.1f points de rappel)."
                   % (gq["gain_f1_points"], gq["gain_rappel_points"]))

    # ===== Comparaison cumulative (par question) =====
    st.markdown("#### Comparaison Cypher vs GraphRAG (cumul des questions)")
    m = st.session_state.metrics
    if not m:
        st.info("Pose des questions dans l'onglet precedent : la comparaison s'accumule ici.")
    else:
        n = len(m)
        cy_cov = mean([r["cypher_ok"] for r in m])
        agree = mean([r["accord"] for r in m])
        cy_lat = mean([r["cypher_latence"] for r in m])
        gr_lat = mean([r["graphrag_latence"] for r in m])
        cy_tok = mean([r["cypher_tokens"] for r in m])
        gr_tok = mean([r["graphrag_tokens"] for r in m])
        src_moy = mean([r["sources"] for r in m])

        fb = [r["feedback"] for r in m if r["feedback"]]
        acc_fb = (fb.count("ok") / len(fb)) if fb else None

        gtq = [r for r in m if r["has_gt"]]
        cy_exact = mean([r["cypher_exact"] for r in gtq]) if gtq else None
        gr_exact = mean([r["graphrag_exact"] for r in gtq]) if gtq else None
        cy_rec = mean([r["cypher_rappel"] for r in gtq]) if gtq else None
        gr_rec = mean([r["graphrag_rappel"] for r in gtq]) if gtq else None

        # ---- bandeau de tetes de chapitre
        c = st.columns(5)
        c[0].metric("Questions posees", n)
        c[1].metric("Couverture Cypher", "%.0f%%" % (100 * cy_cov))
        c[2].metric("Accord des 2", "%.0f%%" % (100 * agree))
        c[3].metric("Latence Cypher", "%.2f s" % cy_lat)
        c[4].metric("Latence GraphRAG", "%.2f s" % gr_lat)

        # ---- tableau comparatif central (Q3 + Q4 + Q5)
        st.markdown("#### Cypher (Graph Analytics) vs GraphRAG")

        def pct(x):
            return "-" if x is None else "%.0f%%" % (100 * x)

        comp = [
            {"Indicateur": "Exactitude (auto, sur %d quest. de ref.)" % len(gtq),
             "Cypher": pct(cy_exact), "GraphRAG": pct(gr_exact)},
            {"Indicateur": "Rappel (auto)",
             "Cypher": pct(cy_rec), "GraphRAG": pct(gr_rec)},
            {"Indicateur": "Latence moyenne",
             "Cypher": "%.2f s" % cy_lat, "GraphRAG": "%.2f s" % gr_lat},
            {"Indicateur": "Cout d'inference (~tokens/question)",
             "Cypher": "~%d" % cy_tok, "GraphRAG": "~%d" % gr_tok},
            {"Indicateur": "Explicabilite",
             "Cypher": "requete exacte", "GraphRAG": "~%.1f sources/reponse" % src_moy},
        ]
        st.table(comp)

        try:
            import pandas as pd
            cc = st.columns(2)
            cc[0].caption("Latence moyenne (s)")
            cc[0].bar_chart(pd.DataFrame({"latence (s)": {"Cypher": cy_lat, "GraphRAG": gr_lat}}))
            cc[1].caption("Cout d'inference (~tokens/question)")
            cc[1].bar_chart(pd.DataFrame({"tokens ~": {"Cypher": cy_tok, "GraphRAG": gr_tok}}))
        except Exception:
            pass

        if acc_fb is not None:
            st.metric("Exactitude terrain (via 👍/👎, %d jugee(s))" % len(fb),
                      "%.0f%%" % (100 * acc_fb))

        # ---- ou chaque approche gagne (par categorie de question) — Q3
        if gtq:
            st.markdown("#### Ou chaque approche est forte (par categorie)")
            cats = {}
            for r in gtq:
                d_ = cats.setdefault(r["categorie"], {"n": 0, "cy": 0, "gr": 0})
                d_["n"] += 1
                d_["cy"] += 1 if r["cypher_exact"] else 0
                d_["gr"] += 1 if r["graphrag_exact"] else 0
            st.table([{"Categorie": k, "Questions": v["n"],
                       "Cypher exact": "%d/%d" % (v["cy"], v["n"]),
                       "GraphRAG exact": "%d/%d" % (v["gr"], v["n"])}
                      for k, v in sorted(cats.items())])

        # ---- synthese : reponses aux 5 questions du sujet
        with st.expander("🧾 Reponses aux 5 questions du sujet (synthese live)", expanded=False):
            if gq:
                st.markdown("**Q1 — Exactitude de l'extraction LLM :** F1 = **%.0f%%** "
                            "(precision %.0f%%, rappel %.0f%%) sur la verite terrain."
                            % (100 * gq["avec_resolution"]["f1"],
                               100 * gq["avec_resolution"]["precision"],
                               100 * gq["avec_resolution"]["rappel"]))
                st.markdown("**Q2 — Effet de la resolution d'entites :** +%.1f points de F1 "
                            "(elle rapproche les variantes de noms, ex. « Lumière Studios » → "
                            "« Lumiere Studios », et augmente le rappel)."
                            % gq["gain_f1_points"])
            else:
                st.markdown("**Q1 / Q2 :** lance le pipeline sur le corpus cinema pour les chiffres.")
            if gtq:
                st.markdown("**Q3 — GraphRAG vs Cypher :** exactitude Cypher **%s** vs GraphRAG **%s** "
                            "sur %d questions de reference. Cypher excelle sur le factuel/agregation "
                            "(valeur exacte) ; GraphRAG gere le semantique et explique en langage naturel."
                            % (pct(cy_exact), pct(gr_exact), len(gtq)))
            else:
                st.markdown("**Q3 :** pose des questions de reference pour comparer l'exactitude.")
            st.markdown("**Q4 — Compromis :** latence Cypher %.2fs vs GraphRAG %.2fs ; "
                        "cout ~%d vs ~%d tokens/question (GraphRAG ~%.1fx, car il envoie le "
                        "sous-graphe au LLM). Cypher = rapide/exact/peu couteux mais rigide ; "
                        "GraphRAG = souple/explicable mais plus lent et plus couteux."
                        % (cy_lat, gr_lat, cy_tok, gr_tok, (gr_tok / cy_tok) if cy_tok else 0))
            st.markdown("**Q5 — Provenance & explicabilite :** chaque fait porte sa source "
                        "(noeud `Document` + `MENTIONS`). GraphRAG cite ~%.1f source(s) par reponse ; "
                        "Cypher fournit la requete exacte executee. Les deux sont donc auditables."
                        % src_moy)

        # ---- provenance reelle dans le graphe (Q5)
        with st.expander("🔎 Provenance dans le graphe (noeuds Document / MENTIONS)"):
            try:
                d = get_driver()
                with d.session() as s:
                    docn = s.run("MATCH (d:Document) RETURN count(d) AS n").single()["n"]
                    menn = s.run("MATCH (:Document)-[r:MENTIONS]->() RETURN count(r) AS n").single()["n"]
                st.write("Documents sources : **%d** · liens MENTIONS : **%d**" % (docn, menn))
            except Exception as e:
                st.caption("Neo4j non interrogeable : %s" % e)

        # ---- note ethique
        st.caption("Note ethique : corpus synthetique (aucune donnee personnelle reelle) ; "
                   "chaque fait conserve sa provenance ; le LLM n'ecrit jamais dans la base "
                   "(requetes en lecture seule), ce qui limite les hallucinations persistees.")

        # ---- detail par question
        with st.expander("Detail par question"):
            st.table([{"question": r["question"][:60],
                       "cat": r["categorie"],
                       "cy_exact": r["cypher_exact"], "gr_exact": r["graphrag_exact"],
                       "cy_s": r["cypher_latence"], "gr_s": r["graphrag_latence"],
                       "accord": r["accord"], "feedback": r["feedback"]} for r in m])
