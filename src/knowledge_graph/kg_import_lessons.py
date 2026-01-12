import os
from bs4 import BeautifulSoup
from py2neo import Graph, Node, Relationship
from dotenv import load_dotenv

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
PROCESSED_DIR = os.path.join(
    BASE_DIR,
    "data",
    "processed",
    "courses",
    "course_4",
    "modules"
)

ENV_PATH = os.path.join(BASE_DIR, "config", "secrets.env")
load_dotenv(ENV_PATH)

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASS = os.getenv("NEO4J_PASS")
DB_NAME = os.getenv("NEO4J_DB")

graph = Graph(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS), name=DB_NAME)
print(f"Connected to Neo4j database: {DB_NAME}")

def extract_text_from_html(file_path: str) -> str:
    with open(file_path, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f.read(), "html.parser")

    for tag in soup(["script", "style"]):
        tag.decompose()

    text = soup.get_text(separator="\n")
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    return "\n".join(lines)


def lesson_id_from_filename(filename: str) -> str:
    return filename.replace("lesson_", "").replace(".html", "")

def import_lessons():
    course_id = "4"
    course_node = Node("Course", id=course_id)
    graph.merge(course_node, "Course", "id")

    total_lessons = 0

    for module_dir in os.listdir(PROCESSED_DIR):
        if not module_dir.startswith("module_"):
            continue

        module_id = module_dir.replace("module_", "")
        module_node = Node("Module", id=module_id)
        graph.merge(module_node, "Module", "id")

        graph.merge(Relationship(course_node, "HAS_MODULE", module_node))

        lessons_dir = os.path.join(
            PROCESSED_DIR,
            module_dir,
            "lessons",
            "contents"
        )

        if not os.path.isdir(lessons_dir):
            continue

        print(f"\nImporting lessons for Module {module_id}")

        for file in os.listdir(lessons_dir):
            if not file.endswith(".html"):
                continue

            lesson_id = lesson_id_from_filename(file)
            lesson_name = lesson_id.replace("-", " ").title()
            file_path = os.path.join(lessons_dir, file)

            content = extract_text_from_html(file_path)

            if len(content) < 50:
                print(f"Skip empty lesson: {file}")
                continue

            lesson_node = Node(
                "Lesson",
                id=lesson_id,
                name=lesson_name,
                data=content
            )

            graph.merge(lesson_node, "Lesson", "id")
            graph.merge(Relationship(module_node, "has_lesson", lesson_node))

            total_lessons += 1
            print(f"Imported: {lesson_id}")

    print(f"\nDONE – Total lessons imported: {total_lessons}")

if __name__ == "__main__":
    import_lessons()

    node_count = graph.evaluate("MATCH (n) RETURN count(n)")
    rel_count = graph.evaluate("MATCH ()-[r]->() RETURN count(r)")
    print(f"Neo4j now has {node_count} nodes and {rel_count} relationships.")
