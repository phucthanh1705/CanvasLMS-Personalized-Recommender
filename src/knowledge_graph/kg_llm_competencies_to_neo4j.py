import os
import pandas as pd
from neo4j import GraphDatabase
from dotenv import load_dotenv

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
ENV_PATH = os.path.join(BASE_DIR, "config", "secrets.env")

load_dotenv(ENV_PATH)

NEO4J_URI  = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASS = os.getenv("NEO4J_PASS")
NEO4J_DB   = os.getenv("NEO4J_DB")

CSV_PATH = os.path.join(BASE_DIR, "data", "llm", "competencies", "course_competencies_llm.csv")

def neo4j_session():
    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USER, NEO4J_PASS),
        encrypted=False   
    )
    return driver.session(database=NEO4J_DB)

def import_llm_competencies():

    if not os.path.exists(CSV_PATH):
        return
    
    df = pd.read_csv(CSV_PATH)
    total = len(df)

    with neo4j_session() as session:

        for index, row in df.iterrows():

            module_id     = row["module_id"]
            competency_id = row["competency_id"]
            name          = row.get("name", "")
            description   = row.get("description", "")
            domain        = row.get("domain", "")

            session.run("""
                MERGE (c:Competency {id: $cid})
                SET  c.name        = $name,
                     c.description = $desc,
                     c.domain      = $domain
            """, cid=competency_id, name=name, desc=description, domain=domain)

            session.run("""
                MATCH (m:Module {id:$mid})
                MATCH (c:Competency {id:$cid})
                MERGE (m)-[:HAS_COMPETENCY]->(c)
            """, mid=module_id, cid=competency_id)

            if index % 200 == 0:
                print(f"Imported {index}/{total}...")


if __name__ == "__main__":
    import_llm_competencies()
