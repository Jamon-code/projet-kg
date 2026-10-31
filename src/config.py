"""Configuration centrale : connexion au LLM (DeepSeek) et a Neo4j.

L'appel au LLM se fait directement en HTTP (urllib, standard Python) pour eviter
tout conflit de version avec le SDK openai dans l'image Jupyter.
"""
import os
import json
import urllib.request
import urllib.error
from neo4j import GraphDatabase

# ---------- DeepSeek (API compatible OpenAI, appelee en HTTP direct) ----------
DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "").strip()
DEEPSEEK_URL = "https://api.deepseek.com/chat/completions"
LLM_MODEL = "deepseek-chat"


def _chat(messages, temperature=0, json_mode=False):
    if not DEEPSEEK_API_KEY:
        raise RuntimeError(
            "DEEPSEEK_API_KEY manquante. Copie .env.example en .env, colle ta cle, "
            "puis relance (docker compose up -d) pour que le conteneur la prenne en compte."
        )
    payload = {"model": LLM_MODEL, "messages": messages, "temperature": temperature}
    if json_mode:
        payload["response_format"] = {"type": "json_object"}
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        DEEPSEEK_URL, data=data, method="POST",
        headers={"Authorization": "Bearer " + DEEPSEEK_API_KEY,
                 "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            out = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "ignore")
        raise RuntimeError("Erreur API DeepSeek (%s) : %s" % (e.code, body))
    return out["choices"][0]["message"]["content"]


def llm_json(system, user, temperature=0):
    """Appelle DeepSeek et renvoie la reponse parsee en dict (mode JSON)."""
    return json.loads(_chat(
        [{"role": "system", "content": system},
         {"role": "user", "content": user}],
        temperature=temperature, json_mode=True,
    ))


def llm_text(system, user, temperature=0):
    """Appelle DeepSeek et renvoie du texte libre (pour le GraphRAG)."""
    return _chat(
        [{"role": "system", "content": system},
         {"role": "user", "content": user}],
        temperature=temperature, json_mode=False,
    )


# ---------- Neo4j ----------
NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "password123")


def get_driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
