import csv
import os


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

INPUT_PATH  = os.path.join(BASE_DIR, "data", "processed", "course_prerequisites.csv")
OUTPUT_PATH = os.path.join(BASE_DIR, "data", "triples", "course_prerequisites_edges.csv")

os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

def build_edges():
    print("Reading:", INPUT_PATH)

    rows = []
    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            module = row["module"]
            prereq = row["prereq"]

            rows.append({
                "source": module,
                "source_label": "Module",
                "relation": "REQUIRES",
                "target": prereq,
                "target_label": "Module"
            })

    print(f"Writing {len(rows)} edges to:", OUTPUT_PATH)

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["source", "source_label", "relation", "target", "target_label"]
        )
        writer.writeheader()
        writer.writerows(rows)

    print("DONE! File created:", OUTPUT_PATH)


if __name__ == "__main__":
    build_edges()
