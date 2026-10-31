# Plateforme d'analyse de Knowledge Graph assistée par IA — thème *Cinéma*

Plateforme qui construit **automatiquement** un graphe de connaissances à partir de
documents hétérogènes (articles, interviews, emails, communiqués), puis compare **deux
façons de l'interroger** sur **le même graphe** :

- **Graph Analytics** : requêtes **Cypher** + algorithmes **Neo4j GDS** (PageRank, Louvain) ;
- **GraphRAG** : récupération d'un sous-graphe pertinent → réponse d'un **LLM** avec ses sources.

Tout tourne dans **un seul `docker compose`** : HDFS, Spark, Neo4j, le service d'extraction
LLM / GraphRAG (API REST) et l'application.

## Architecture (chaîne complète)

```
Sources (URLs / documents / texte)
   → Crawler              : collecte + parsing + métadonnées (provenance)
   → HDFS (Bronze)        : stockage des documents bruts
   → Extraction LLM       : entités + relations (DeepSeek)
   → Résolution d'entités : normalisation / rapprochement des noms → entités canoniques
   → Spark (Silver→Gold)  : validation + transformation (Silver), déduplication (Gold) — écrits dans HDFS
   → Neo4j                : Knowledge Graph (+ index de graphe + provenance Document/MENTIONS)
        ├── Graph Analytics : Cypher + GDS
        └── GraphRAG        : sous-graphe → réponse LLM (avec sources)
   → Évaluation           : Dashboard CUMULATIF (exactitude, rappel, latence, coût, provenance)
   → API REST + monitoring + contrôle d'accès (Phase 7)
```

**Honnêteté du pipeline** : Spark et HDFS traitent les données **réellement crawlées**.
Si on change de sources, HDFS / Spark / Neo4j reflètent les nouvelles données — rien n'est
codé en dur.

## Prérequis

- **Docker Desktop** lancé.
- Une **clé API DeepSeek**.

## Démarrage

1. **Clé API** : copie `.env.example` en `.env` et colle ta clé :
   ```
   DEEPSEEK_API_KEY=sk-....
   ```
   (`API_KEY` est facultatif : sans rien, l'API utilise `cine-demo-key`.)
2. **Lancer tout le stack** (dans le dossier du projet) :
   ```
   docker compose -f docker-compose.full.yml up -d --build
   ```
   | Service | URL | Rôle |
   |---|---|---|
   | Application (Streamlit) | http://localhost:8501 | démo + Dashboard |
   | API REST (Swagger) | http://localhost:8000/docs | Phase 7 |
   | Neo4j Browser | http://localhost:7474 | graphe (neo4j / password123) |
   | Jupyter Lab | http://localhost:8888 | notebooks |
   | HDFS NameNode UI | http://localhost:9870 | monitoring HDFS |
3. **Démarrer l'application** (le conteneur lance Jupyter par défaut ; on lance l'app à côté) :
   ```
   docker exec -it cine-ai bash -lc "cd ~/work && streamlit run app/app_full.py --server.port 8501 --server.address 0.0.0.0"
   ```
   Laisse ce terminal ouvert, puis ouvre **http://localhost:8501**.

## Phase 7 — API REST, monitoring, contrôle d'accès

Le service **`cine-api`** (port **8000**) expose le pipeline derrière une API, lancée
automatiquement par le `docker compose`. Documentation interactive : **http://localhost:8000/docs**.

| Méthode | Endpoint | Rôle | Accès |
|---|---|---|---|
| GET | `/health` | état du service + Neo4j | ouvert |
| GET | `/metrics` | monitoring (nb requêtes, erreurs, latence moyenne par endpoint) | ouvert |
| GET | `/metrics/prometheus` | mêmes métriques au format Prometheus | ouvert |
| POST | `/extract` | service d'extraction LLM (texte → entités/relations) | 🔒 clé |
| POST | `/ask/cypher` | Graph Analytics (question → Cypher → réponse) | 🔒 clé |
| POST | `/ask/graphrag` | GraphRAG (question → sous-graphe → réponse sourcée) | 🔒 clé |
| POST | `/pipeline` | pipeline complet Crawl→HDFS→Spark→Neo4j | 🔒 clé |
| GET | `/graph/stats` | comptage nœuds / relations | 🔒 clé |
| GET | `/graph/indexes` | liste des index Neo4j | 🔒 clé |

- **Contrôle d'accès** : les endpoints de données exigent l'en-tête `X-API-Key: cine-demo-key`
  (modifiable via `API_KEY` dans `.env`). Sans la clé → `401`.
- **Monitoring** : chaque requête est chronométrée, comptée et journalisée (logs `cine-api`) ;
  `/metrics` agrège le tout ; un *healthcheck* Docker surveille le service.
- **Index de graphe** : créés automatiquement au chargement (un index `name` par label).

## Procédure de test complète

**0. Pré-vol.** `.env` créé (clé DeepSeek). Aucun ancien conteneur sur les ports
`7474 / 7687 / 8888 / 9870 / 8000 / 8501` (stoppe les conteneurs du lab précédent).

**1. Lancer et attendre que tout soit « healthy ».**
```
docker compose -f docker-compose.full.yml up -d --build
docker compose -f docker-compose.full.yml ps
```
Tu dois voir `cine-namenode`, `cine-datanode`, `cine-neo4j`, `cine-ai`, `cine-api` en *running*
(neo4j et api en *healthy* après ~1 min).

**2. Construire le graphe + voir le pipeline.** Démarre l'app (commande ci-dessus) →
onglet **Sources & Pipeline** → *Corpus fourni (cinéma)* → **Lancer le pipeline**.
✅ attendu : log Crawl → HDFS → Extraction → Spark (N → M relations) → Neo4j, et un
message « F1 X% avec résolution ».

**3. Vérifier le graphe.** Onglet **Knowledge Graph** → **Analyser** : comptage des nœuds /
relations, Top 10 PageRank + communautés, et la liste des **index**.
(Ou dans Neo4j Browser http://localhost:7474 : `MATCH (n) RETURN n` puis `SHOW INDEXES`.)

**4. Comparer les 2 approches.** Onglet **Poser une question** → *Question de référence* →
choisis-en plusieurs → **Comparer Cypher vs GraphRAG**. Juge éventuellement avec 👍/👎.

**5. Lire les insights.** Onglet **Dashboard** : tout s'est cumulé (exactitude auto, rappel,
latence, coût/tokens, accord, provenance) + **synthèse des 5 questions** + qualité du graphe
(Q1/Q2). C'est là que se lit la comparaison.

**6. Tester l'API (Phase 7).** Ouvre **http://localhost:8000/docs** :
- `GET /health` → *Execute* → `{"status":"ok","neo4j":"ok"}`.
- `POST /ask/graphrag` → *Try it out* → mets `cine-demo-key` dans le champ **x-api-key**,
  corps `{"question":"Qui a realise Nebula ?"}` → *Execute* → réponse + sources.
- `GET /metrics` → tu vois le nombre de requêtes et la latence moyenne (monitoring).

   En ligne de commande Windows (`curl.exe`) :
   ```
   curl.exe http://localhost:8000/health
   curl.exe -X POST http://localhost:8000/ask/graphrag -H "X-API-Key: cine-demo-key" -H "Content-Type: application/json" -d "{\"question\":\"Qui a realise Nebula ?\"}"
   ```

**7. (Facultatif) Chiffres pour le rapport**, en ligne de commande :
```
docker exec -it cine-ai bash -lc "cd ~/work && python run_pipeline.py"
docker exec -it cine-ai bash -lc "cd ~/work && python evaluation.py"
```

## Les 5 questions du sujet — où elles sont traitées

| # | Question | Où |
|---|----------|----|
| Q1 | Exactitude de l'extraction LLM | Dashboard → *Qualité du graphe* (précision/rappel/F1) |
| Q2 | Effet de la résolution d'entités | Dashboard → F1 **sans** vs **avec** résolution (+ points) |
| Q3 | GraphRAG vs Cypher (multi-saut / sémantique) | Dashboard → exactitude & rappel par **catégorie** |
| Q4 | Compromis (qualité / latence / coût d'inférence) | Dashboard → latence & tokens moyens par approche |
| Q5 | Provenance → fiabilité / explicabilité | Dashboard → sources citées + nœuds `Document`/`MENTIONS` |

## Structure du projet

```
projet_cinema_kg/
├── docker-compose.full.yml   # LE stack unique : HDFS + Spark + Neo4j + IA/app + API
├── Dockerfile.full           # image du service IA (LLM + Spark local + Streamlit + FastAPI)
├── .env.example              # → à copier en .env (clé DeepSeek, + API_KEY facultatif)
├── corpus/                   # documents hétérogènes (sources crawlées)
├── ground_truth/             # vérité terrain : entités, relations, questions de benchmark
├── src/
│   ├── config.py             # connexions DeepSeek (HTTP) + Neo4j
│   ├── crawler.py            # collecte + parsing + métadonnées
│   ├── extract.py            # extraction LLM (entités + relations)
│   ├── load_neo4j.py         # résolution d'entités + index + chargement + provenance
│   ├── pipeline.py           # chaîne complète Crawl→HDFS→Extraction→Spark→Neo4j
│   ├── analytics.py          # Graph Analytics : Cypher + GDS
│   ├── graphrag.py           # GraphRAG : sous-graphe + réponse LLM sourcée
│   └── metrics.py            # mesures des 5 questions (partagées app + API + evaluation)
├── app/app_full.py           # application Streamlit (démo + Dashboard cumulatif)
├── api/main.py               # API REST (Phase 7) : extraction + GraphRAG + monitoring + accès
├── run_pipeline.py           # backup CLI : construit le graphe en une commande
└── evaluation.py             # backup CLI : recalcule les 5 indicateurs (pour le rapport)
```

## Note éthique

Le corpus est **synthétique** (univers fictif) : aucune donnée personnelle réelle n'est
collectée. Chaque fait du graphe conserve sa **source** (provenance via `Document`/`MENTIONS`)
pour l'explicabilité. L'accès à l'API est **contrôlé par clé**. Le LLM n'écrit **jamais** dans
la base (requêtes en lecture seule), ce qui évite de persister d'éventuelles hallucinations.
Conformément au sujet, les associations inférées ne sont pas présentées comme des faits
vérifiés : la réponse GraphRAG cite toujours les documents qui la justifient.
