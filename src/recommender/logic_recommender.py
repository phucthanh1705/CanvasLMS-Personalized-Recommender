import os
import csv
import argparse
from pathlib import Path
from neo4j import GraphDatabase
from dotenv import load_dotenv

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
ENV_PATH = os.path.join(BASE_DIR, "config", "secrets.env")
load_dotenv(ENV_PATH)

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASS = os.getenv("NEO4J_PASS", "neo4j")
NEO4J_DB   = os.getenv("NEO4J_DB",  "neo4j")

EXPORT_DIR = Path(os.path.join(BASE_DIR, "data", "exports"))
EXPORT_DIR.mkdir(parents=True, exist_ok=True)
EXPORT_FILE = EXPORT_DIR / "recommendations.csv"


CYPHER_COMPUTE_CENTER = """
MATCH (s:Student)-[r:mastery_on]->(m:Module)
WHERE m.embedding IS NOT NULL AND r.mastery IS NOT NULL AND toFloat(r.mastery) > 0
WITH s, collect({vec: m.embedding, w: toFloat(r.mastery)}) AS items
WITH s, items,
     reduce(wsum = 0.0, x IN items | wsum + x.w) AS wsum,
     size(items[0].vec) AS dim
WHERE wsum > 0 AND dim IS NOT NULL
UNWIND range(0, dim-1) AS i
WITH s, wsum, reduce(acc=0.0, it IN items | acc + (it.vec[i] * it.w)) / wsum AS comp
WITH s, collect(comp) AS center
SET s.embedding = center
RETURN count(s) AS studentsUpdated
"""

CYPHER_RECOMMEND_ONE = """
MATCH (s:Student {id: $student}), (m:Module)
WHERE m.embedding IS NOT NULL
  AND NOT (s)-[:mastery_on]->(m)

  // NEW: kiểm tra sinh viên đã học hết mọi tiên quyết
  AND NOT EXISTS {
    MATCH (m)-[:REQUIRES*1..]->(pre)
    WHERE NOT (s)-[:mastery_on]->(pre)
  }

WITH s, m, gds.similarity.cosine(s.embedding, m.embedding) AS similarity
RETURN m AS module, similarity
ORDER BY similarity DESC
LIMIT $topk
"""
CYPHER_STUDENTS = """
MATCH (s:Student)-[:mastery_on]->(:Module)
RETURN DISTINCT s.name AS student
ORDER BY student
"""

CYPHER_LEARNED = """
MATCH (s:Student {name:$student})-[r:mastery_on]->(m:Module)
RETURN m.name AS module, toFloat(r.mastery) AS mastery
ORDER BY mastery DESC
"""
def get_driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))

def compute_centers(session):
    res = session.run(CYPHER_COMPUTE_CENTER)
    rec = res.single()
    updated = rec["studentsUpdated"] if rec else 0
    print(f"Student embeddings updated in DB: {updated}")

def list_students(session):
    return [r["student"] for r in session.run(CYPHER_STUDENTS)]

def list_learned(session, student):
    return [(r["module"], r["mastery"]) for r in session.run(CYPHER_LEARNED, student=student)]

def recommend_for_student(session, student, topk=5):
    rows = session.run(CYPHER_RECOMMEND_ONE, student=student, topk=topk)
    return [(r["module"], float(r["similarity"])) for r in rows if r["similarity"] is not None]

def main(student=None, topk=5, write_csv=True):
    driver = get_driver()
    all_rows = []
    with driver.session(database=NEO4J_DB) as session:
        compute_centers(session)

        students = [student] if student else list_students(session)
        if not students:
            print("No students with mastery_on found.")
            return

        for s in students:
            learned = list_learned(session, s)
            recs = recommend_for_student(session, s, topk=topk)
            print(f"\n {s} | learned={len(learned)} | recs={len(recs)}")
            for mod, sim in recs:
                title = mod.get("title", str(mod.element_id))
                print(f"{title:20s}  cosine={sim:.4f}")
                module_id = mod.get("id", mod.element_id)
                module_title = mod.get("title", mod.get("name", ""))
                all_rows.append({
                    "student": s,
                    "module_id": module_id,
                    "module_title": module_title,
                    "similarity": round(sim, 6)
                })

    driver.close()

    if write_csv and all_rows:
        with open(EXPORT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["student", "module_id", "module_title", "similarity"])
            writer.writeheader()
            writer.writerows(all_rows)
        print(f"\nSaved: {EXPORT_FILE} ({len(all_rows)} rows)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--student", type=str, default=None, help="Student id in graph, e.g., user_39")
    parser.add_argument("--topk", type=int, default=5)
    args = parser.parse_args()
    main(student=args.student, topk=args.topk)
