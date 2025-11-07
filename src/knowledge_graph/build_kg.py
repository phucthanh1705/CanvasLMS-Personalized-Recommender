# -*- coding: utf-8 -*-
"""
Build a hybrid EduKG from your processed Canvas data.
Outputs structural knowledge graph files only:
  - nodes.csv
  - edges.csv (with source_label, target_label)
  - triples.csv

Visualization is handled separately by kg_visualize.py
"""

import os, json, csv
from pathlib import Path

# ---------- CONFIG ----------
ROOT_PROCESSED = os.environ.get("KG_PROCESSED_DIR", "data/processed")
OUT_DIR = os.environ.get("KG_OUT_DIR", "data/triples")
os.makedirs(OUT_DIR, exist_ok=True)

# ---------- Helpers ----------
def read_json(p, default=None):
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def add_node(nodes, node_id, label, name=None, **attrs):
    if node_id not in nodes:
        nodes[node_id] = {"id": node_id, "label": label, "name": name or node_id, **attrs}

def add_edge(edges, src, src_label, rel, tgt, tgt_label, score=None, **attrs):
    """Thêm edge có đủ thông tin label cho cả 2 đầu"""
    e = {
        "source": src,
        "source_label": src_label,
        "relation": rel,
        "target": tgt,
        "target_label": tgt_label
    }
    if score is not None:
        e["score"] = score
    e.update(attrs)
    edges.append(e)

# ---------- Build structural KG ----------
def build_structural(course_dir):
    nodes, edges, triples = {}, [], []

    meta_course = read_json(course_dir / "meta_course_4.json", {})
    course_id = meta_course.get("course", course_dir.name)
    add_node(nodes, course_id, "Course", name=course_id)

    # Course -> Module
    for mod in meta_course.get("modules", []):
        mod_dir = course_dir / "modules" / mod
        if not mod_dir.exists():
            continue
        add_node(nodes, mod, "Module", name=mod)
        add_edge(edges, course_id, "Course", "includes", mod, "Module")
        triples.append((course_id, "includes", mod))

        # Lessons
        for lp in sorted((mod_dir / "lessons" / "contents").glob("lesson_*.json")):
            lj = read_json(lp, {})
            les_id = lp.stem
            title = lj.get("title") or les_id
            add_node(nodes, les_id, "Lesson", name=title)
            add_edge(edges, mod, "Module", "has_lesson", les_id, "Lesson")
            triples.append((mod, "has_lesson", les_id))

        # Quizzes
        quiz_dir = mod_dir / "quizzes"
        for qp in sorted(quiz_dir.glob("quiz_*.json")):
            qid = qp.stem
            add_node(nodes, qid, "Quiz", name=qid)
            add_edge(edges, mod, "Module", "has_quiz", qid, "Quiz")
            triples.append((mod, "has_quiz", qid))

            qlist = read_json(qp, [])
            for qobj in qlist or []:
                q_item_id = f"question_{qobj.get('id', '')}"
                q_text = qobj.get("question_text") or q_item_id
                if q_item_id != "question_":
                    add_node(nodes, q_item_id, "Question", name=q_text)
                    add_edge(edges, qid, "Quiz", "has_question", q_item_id, "Question")
                    triples.append((qid, "has_question", q_item_id))

        # Submissions (scores)
        subs_dir = quiz_dir / "submissions"
        for sp in sorted(subs_dir.glob("cleaned_quiz_*_submissions.json")):
            arr = read_json(sp, [])
            try:
                quiz_num = sp.stem.split("_")[2]
                quiz_node = f"quiz_{quiz_num}"
            except Exception:
                quiz_node = None
            for rec in arr or []:
                uid = rec.get("user_id")
                if uid is None or quiz_node is None:
                    continue
                student = f"user_{uid}"
                add_node(nodes, student, "Student", name=student)
                add_edge(edges, student, "Student", "attempted", quiz_node, "Quiz")
                triples.append((student, "attempted", quiz_node))
                s = rec.get("score")
                try:
                    s = float(s) if s is not None else None
                except Exception:
                    s = None
                add_edge(edges, student, "Student", "scored_on", quiz_node, "Quiz", score=s)
                if s is not None:
                    triples.append((student, "has_score", f"{quiz_node}:{s}"))

    return nodes, edges, triples

# ---------- Merge semantic triples (optional from LLM) ----------
def load_semantic_triples(llm_triples_dir):
    t = []
    p = Path(llm_triples_dir) / "edutriples.csv"
    if not p.exists():
        return t
    with open(p, "r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            try:
                score = float(r.get("score", "0"))
            except Exception:
                score = 0.0
            if score >= 8.0:
                t.append((r["entity1"], r["relation"], r["entity2"]))
    return t

# ---------- Writers ----------
def write_nodes(nodes):
    out = os.path.join(OUT_DIR, "nodes.csv")
    with open(out, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["id", "label", "name"])
        w.writeheader()
        for n in nodes.values():
            w.writerow(n)
    print(f"📁 nodes.csv written → {out}")

def write_edges(edges):
    out = os.path.join(OUT_DIR, "edges.csv")
    with open(out, "w", encoding="utf-8", newline="") as f:
        fieldnames = ["source", "source_label", "relation", "target", "target_label", "score"]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for e in edges:
            w.writerow(e)
    print(f"📁 edges.csv written → {out}")

def write_triples(triples):
    out = os.path.join(OUT_DIR, "triples.csv")
    with open(out, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["entity1", "relation", "entity2"])
        for a, b, c in triples:
            w.writerow([a, b, c])
    print(f"📁 triples.csv written → {out}")

# ---------- Entry ----------
def main():
    courses_root = Path(ROOT_PROCESSED) / "courses"
    course_dir = next((p for p in courses_root.glob("course_*") if p.is_dir()), None)
    if not course_dir:
        raise SystemExit(f"❌ Không tìm thấy thư mục course_* trong {courses_root}")

    nodes, edges, triples = build_structural(course_dir)
    triples.extend(load_semantic_triples(OUT_DIR))

    write_nodes(nodes)
    write_edges(edges)
    write_triples(triples)

    print(f"\n✅ KG build complete!")
    print(f"📦 Output directory: {OUT_DIR}")
    print(f"   - nodes: {len(nodes)}")
    print(f"   - edges: {len(edges)}")
    print(f"   - triples: {len(triples)}")

if __name__ == "__main__":
    main()
