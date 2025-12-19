import subprocess
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parents[2] 
sys.path.append(str(BASE))  

def run(label, path):
    result = subprocess.run(
        [sys.executable, str(path)],
        cwd=str(BASE),        
        stdout=sys.stdout,
        stderr=sys.stderr,
        text=False,
        shell=False
    )

    if result.returncode != 0:
        raise RuntimeError(f"Pipeline error at step: {label}")

def run_single_step(label):
    step_map = {
        #"Fetch Canvas data": BASE / "src/canvas_api/fetch_canvas.py",
        #"Parse Canvas data": BASE / "src/canvas_api/parse_canvas.py",
        "Build Knowledge Graph": BASE / "src/knowledge_graph/build_kg.py",
        "Build Prereq Edges": BASE / "src/tools/build_prereq_edges.py",
        "Import to Neo4j": BASE / "src/knowledge_graph/kg_to_neo4j.py",
        "Compute Competencies": BASE / "src/analytics/compute_competency.py",
        "Build Student Profiles": BASE / "src/analytics/student_profile.py",
        "Logic Recommender": BASE / "src/recommender/logic_recommender.py",
        "Import LLM Competencies": BASE / "src/knowledge_graph/kg_llm_competencies_to_neo4j.py"
    }

    if label not in step_map:
        raise RuntimeError(f"[run_single_step] Unknown step: {label}")

    run(label, step_map[label])

def run_pipeline_once():
    steps = [
        "Fetch Canvas data",
        "Parse Canvas data",
        "Build Knowledge Graph",
        "Build Prereq Edges",
        "Import to Neo4j",
        "Compute Competencies",
        "Build Student Profiles",
        "Logic Recommender",
        "Import LLM Competencies"
    ]

    for label in steps:
        run_single_step(label)

if __name__ == "__main__":
    run_pipeline_once()
