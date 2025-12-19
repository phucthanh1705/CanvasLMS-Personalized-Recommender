import os
import pandas as pd
from py2neo import Graph, Node
from dotenv import load_dotenv

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
TRIPLE_DIR = os.path.join(BASE_DIR, "data", "triples")
ENV_PATH = os.path.join(BASE_DIR, "config", "secrets.env")

load_dotenv(ENV_PATH)

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASS = os.getenv("NEO4J_PASS")
DB_NAME = os.getenv("NEO4J_DB")

graph = Graph(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS), name=DB_NAME)

def import_lti_users(file_path):
    df = pd.read_csv(file_path)

    count_update, count_created = 0, 0

    for _, row in df.iterrows():
        user_id = str(row.get("user_id")).strip()
        name = row.get("name")
        lti_id = row.get("lti_id")

        node = graph.nodes.match("Student", id=user_id).first()

        if node:
            node["lti_id"] = lti_id
            node["name"] = name
            graph.push(node)
            count_update += 1
        else:
            new_node = Node(
                "Student",
                id=user_id,
                name=name,
                lti_id=lti_id
            )
            graph.create(new_node)
            count_created += 1

def main():
    lti_path = os.path.join(TRIPLE_DIR, "canvas_user_lti_export.csv")

    if os.path.exists(lti_path):
        import_lti_users(lti_path)
    else:
        print("canvas_user_lti_export.csv not found!")
    node_count = graph.evaluate("MATCH (n) RETURN count(n)")
    rel_count = graph.evaluate("MATCH ()-[r]->() RETURN count(r)")
    print(f"Neo4j now has {node_count} nodes and {rel_count} relationships.")

if __name__ == "__main__":
    main()
