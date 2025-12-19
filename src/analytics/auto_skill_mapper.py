import json
import os
from pathlib import Path
from neo4j import GraphDatabase
from dotenv import load_dotenv
from openai import OpenAI

BASE_DIR = Path(__file__).resolve().parents[2]
ENV_PATH = BASE_DIR / "config" / "secrets.env"
load_dotenv(ENV_PATH)

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASS = os.getenv("NEO4J_PASS")
NEO4J_DB   = os.getenv("NEO4J_DB", "neo4j")

MAPPING_PATH = BASE_DIR / "data" / "computed_skill_mapping.json"
client = OpenAI(api_key=os.getenv("GOOGLE_API_KEY"))


def neo4j_session():
    return GraphDatabase.driver(
        NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS), encrypted=False
    ).session(database=NEO4J_DB)


def fetch_all_domains():
    with neo4j_session() as session:
        rows = session.run("""
            MATCH (c:Competency)
            RETURN DISTINCT c.domain AS domain
        """)

        raw_domains = [r["domain"] for r in rows]
        domains = sorted(
            set([d.strip() for d in raw_domains if d and d.strip()]))

        return domains


def call_gpt_for_mapping(domains):
    """
    Input: list of domains
    Output: JSON mapping domain -> skill group
    """

    messages = [
        {
            "role": "system",
            "content": (
                "You are an expert skill classifier."
                "Your task: Receive an input JSON containing a list of domains, "
                "and classify each domain into exactly one of the following skill groups: "
                "FE, BE, NETWORK, MOBILE, DATA, AI, QLDA. "
                "If a domain belongs to general/basic/foundational courses "
                "(e.g., math, physics, algorithms, introduction to IT, soft skills, etc.), "
                "do NOT assign a skill group — instead return the value 'None'. "
                "Output must be a pure JSON object in the form {domain: group}. "
                "Do NOT add explanations."
            )
        },
        {
            "role": "user",
            "content": json.dumps({"domains": domains}, ensure_ascii=False)
        }
    ]

    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        temperature=0,
        response_format={ "type": "json_object" }
    )

    raw = completion.choices[0].message.content
    result = json.loads(raw)

    filtered = {k: v for k, v in result.items() if k in domains}

    return filtered


def save_mapping(mapping):
    MAPPING_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(MAPPING_PATH, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)


def generate_skill_mapping():
    domains = fetch_all_domains()

    mapping = call_gpt_for_mapping(domains)
    save_mapping(mapping)

    return mapping


if __name__ == "__main__":
    mapping = generate_skill_mapping()
