import os
import pandas as pd
from py2neo import Graph, Node, Relationship
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

def import_nodes(file_path):
    df = pd.read_csv(file_path)
    for _, row in df.iterrows():
        label = row.get("label", "Entity").strip()
        node = Node(label,
                    id=row.get("id"),
                    name=row.get("name"))
        graph.merge(node, label, "id")

def import_edges(file_path):
    df = pd.read_csv(file_path)

    count_ok, count_fail = 0, 0
    for _, row in df.iterrows():
        src_label = str(row.get("source_label", "")).strip()
        tgt_label = str(row.get("target_label", "")).strip()

        src = graph.nodes.match(src_label, id=row["source"]).first()
        tgt = graph.nodes.match(tgt_label, id=row["target"]).first()

        if src and tgt:
            rel = Relationship(src, row["relation"], tgt)
            graph.merge(rel)
            count_ok += 1
        else:
            count_fail += 1

    if count_fail:
        print(f"Skipped {count_fail} edges (node not found)")

def main():
    nodes_path = os.path.join(TRIPLE_DIR, "nodes.csv")
    edges_path = os.path.join(TRIPLE_DIR, "edges.csv")
    prereq_path = os.path.join(TRIPLE_DIR, "course_prerequisites_edges.csv")

    if os.path.exists(nodes_path):
        import_nodes(nodes_path)

    if os.path.exists(edges_path):
        import_edges(edges_path)

    if os.path.exists(prereq_path):
        import_edges(prereq_path)
    else:
        print("course_prerequisites_edges.csv not found!")

    node_count = graph.evaluate("MATCH (n) RETURN count(n)")
    rel_count = graph.evaluate("MATCH ()-[r]->() RETURN count(r)")
    print(f"Database now has {node_count} nodes and {rel_count} relationships.")

if __name__ == "__main__":
    main()
