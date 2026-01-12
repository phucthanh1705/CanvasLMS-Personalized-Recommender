import os
from py2neo import Graph
from dotenv import load_dotenv

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
ENV_PATH = os.path.join(BASE_DIR, "config", "secrets.env")

load_dotenv(ENV_PATH)

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASS = os.getenv("NEO4J_PASS")
NEO4J_DB   = os.getenv("NEO4J_DB")

graph = Graph(
    NEO4J_URI,
    auth=(NEO4J_USER, NEO4J_PASS),
    name=NEO4J_DB
)

MASTERY_THRESHOLD = 0.5

def derive_competency_mastery():
    query = f"""
    MATCH (s:Student)-[rm:mastery_on]->(m:Module)
    WHERE rm.mastery > {MASTERY_THRESHOLD}
    MATCH (m)-[:HAS_COMPETENCY]->(c:Competency)
    MERGE (s)-[rc:mastery_on]->(c)
    SET rc.mastery = rm.mastery
    RETURN count(rc) AS created_or_updated
    """

    result = graph.run(query).data()
    count = result[0]["created_or_updated"] if result else 0

    print(f"Derived competency mastery relationships: {count}")

if __name__ == "__main__":
    derive_competency_mastery()

    # Quick stats
    stats = graph.run("""
        MATCH (s:Student)-[r:mastery_on]->(c:Competency)
        RETURN count(r) AS competency_mastery_count
    """).data()

    print(f"Total Student→Competency mastery: {stats[0]['competency_mastery_count']}")
