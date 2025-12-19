import os
import pandas as pd
from neo4j import GraphDatabase
from dotenv import load_dotenv

CSV_FILE = "data/processed/student_competency_by_module.csv"
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
ENV_PATH = os.path.join(BASE_DIR, "config", "secrets.env")
load_dotenv(ENV_PATH)

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASS")
NEO4J_DB   = os.getenv("NEO4J_DB")

MERGE_CYPHER = """
MERGE (s:Student {name: $student_id})
MERGE (m:Module {name: $module_id})
MERGE (s)-[r:mastery_on]->(m)
SET   r.mastery = $mastery,
      r.quizzes = $quizzes,
      r.total_points = $total_points
"""

def main():
    if not os.path.exists(CSV_FILE):
        return
    df = pd.read_csv(CSV_FILE)
    if df.empty:
        return

    for col in ["total_score","total_points","module_mastery"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["quizzes"] = pd.to_numeric(df.get("quizzes", 0), errors="coerce").fillna(0).astype(int)
    df["total_points"] = pd.to_numeric(df.get("total_points", 0), errors="coerce").fillna(0.0)
    df["module_mastery"] = pd.to_numeric(df.get("module_mastery", 0), errors="coerce").fillna(0.0).clip(0,1)

    df["student_id"] = "user_" + df["user_id"].astype(str)

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    total = 0
    with driver.session(database=NEO4J_DB) as session:
        for _, row in df.iterrows():
            params = {
                "student_id": row["student_id"],
                "module_id": row["module_id"],
                "mastery": float(row["module_mastery"]),
                "quizzes": int(row["quizzes"]),
                "total_points": float(row["total_points"]),
            }
            session.run(MERGE_CYPHER, **params)
            total += 1
    driver.close()

if __name__ == "__main__":
    main()
