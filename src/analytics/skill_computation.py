import json
from neo4j import GraphDatabase
from pathlib import Path
from dotenv import load_dotenv
import os

BASE_DIR = Path(__file__).resolve().parents[2]
ENV_PATH = BASE_DIR / "config" / "secrets.env"
load_dotenv(ENV_PATH)

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASS = os.getenv("NEO4J_PASS")
NEO4J_DB   = os.getenv("NEO4J_DB", "neo4j")


def neo4j_session():
    """Neo4j session creator"""
    return GraphDatabase.driver(
        NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS), encrypted=False
    ).session(database=NEO4J_DB)

MAPPING_PATH = BASE_DIR / "data" / "computed_skill_mapping.json"

def load_domain_mapping():
    """Load mapping module_id → skill_key từ file JSON"""
    if not MAPPING_PATH.exists():
        raise Exception("❌ File computed_skill_mapping.json chưa tồn tại!")

    with open(MAPPING_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

domain_map = load_domain_mapping()

SKILL_KEYS = ["FE", "BE", "NET", "MOBILE", "DATA", "AI", "QLDA", "None"]

TOTAL_MODULES_PER_SKILL = {k: 0 for k in SKILL_KEYS}

for mid, skill in domain_map.items():
    if skill in TOTAL_MODULES_PER_SKILL:
        TOTAL_MODULES_PER_SKILL[skill] += 1

def fetch_student_mastery(student_id):
    """
    Trả về list:
    [
        {"mid": 56, "mastery": 0.8},
        {"mid": 62, "mastery": 0.9},
        ...
    ]
    """
    with neo4j_session() as session:
        rows = session.run("""
            MATCH (:Student {id:$sid})-[r:mastery_on]->(m:Module)
            RETURN m.id AS mid, coalesce(r.mastery, 0) AS mastery
        """, sid=student_id)

        return [dict(row) for row in rows]


def compute_skill_percentages(student_id):
    """
    Trả về dict:
    {
      "FE": 35.5,
      "BE": 82,
      "NET": 20.3,
      ...
    }
    """

    mastered_modules = fetch_student_mastery(student_id)

    sum_mastery = {k: 0 for k in SKILL_KEYS}

    for row in mastered_modules:
        mid = str(row["mid"])
        mid = mid[7:9]
        mastery = float(row["mastery"])

        skill_key = domain_map.get(mid)

        if skill_key:
            sum_mastery[skill_key] += mastery

    result = {}

    for skill_key in SKILL_KEYS:
        total_modules = TOTAL_MODULES_PER_SKILL.get(skill_key, 0)

        if total_modules == 0:
            result[skill_key] = 0
            continue

        percent = (sum_mastery[skill_key] / total_modules) * 100
        result[skill_key] = round(percent, 2)

    return result

if __name__ == "__main__":
    sid = "user_69"
    skills = compute_skill_percentages(sid)
