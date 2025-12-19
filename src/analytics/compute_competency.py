import os
import json
from pathlib import Path
import pandas as pd

PROCESSED_ROOT = Path("data/processed/courses")
OUT_QUIZ = Path("data/processed/student_scores_by_quiz.csv")
OUT_MODULE = Path("data/processed/student_competency_by_module.csv")

def iter_submission_files(root: Path):
    for p in root.rglob("quizzes/submissions/cleaned_quiz_*_submissions.json"):
        parts = p.parts
        try:
            course_idx = parts.index("courses") + 1
            module_idx = parts.index("modules") + 1
            course_id = parts[course_idx]         
            module_id = parts[module_idx]         
        except ValueError:
            course_id = None
            module_id = None

        stem = p.stem  
        quiz_id = None
        if stem.startswith("cleaned_quiz_") and stem.endswith("_submissions"):
            quiz_id = stem.replace("cleaned_quiz_", "").replace("_submissions", "")

        yield p, course_id, module_id, quiz_id

def load_submissions():
    rows = []
    if not PROCESSED_ROOT.exists():
        return pd.DataFrame(columns=[
            "user_id","quiz_id","course_id","module_id",
            "score","quiz_points_possible","attempt","workflow_state"
        ])

    for file_path, course_id, module_id, quiz_id in iter_submission_files(PROCESSED_ROOT):
        try:
            data = json.loads(file_path.read_text(encoding="utf-8"))
        except Exception:
            continue

        if not isinstance(data, list):
            continue

        for rec in data:
            rows.append({
                "user_id": rec.get("user_id"),
                "quiz_id": int(quiz_id) if quiz_id is not None else rec.get("quiz_id"),
                "course_id": course_id,
                "module_id": module_id,
                "score": rec.get("score"),
                "quiz_points_possible": rec.get("quiz_points_possible"),
                "attempt": rec.get("attempt"),
                "workflow_state": rec.get("workflow_state")
            })

    df = pd.DataFrame(rows)
    return df

def compute_mastery():
    df = load_submissions()
    if df.empty:
        return

    df = df[df["workflow_state"].astype(str).str.lower().eq("complete")].copy()

    df = df[pd.to_numeric(df["score"], errors="coerce").notna()]
    df = df[pd.to_numeric(df["quiz_points_possible"], errors="coerce").fillna(0) > 0]

    df["score"] = df["score"].astype(float)
    df["quiz_points_possible"] = df["quiz_points_possible"].astype(float)
    df["quiz_mastery"] = (df["score"] / df["quiz_points_possible"]).clip(0, 1)

    quiz_cols = ["user_id","quiz_id","course_id","module_id","score","quiz_points_possible","quiz_mastery","attempt"]
    df_quiz = df[quiz_cols].sort_values(["user_id","module_id","quiz_id"])
    OUT_QUIZ.parent.mkdir(parents=True, exist_ok=True)
    df_quiz.to_csv(OUT_QUIZ, index=False, encoding="utf-8")

    grp = df.groupby(["user_id","course_id","module_id"], dropna=False).agg(
        total_score=("score","sum"),
        total_points=("quiz_points_possible","sum"),
        quizzes=("quiz_id","nunique")
    ).reset_index()
    grp["module_mastery"] = (grp["total_score"] / grp["total_points"]).clip(0, 1)

    OUT_MODULE.parent.mkdir(parents=True, exist_ok=True)
    grp.to_csv(OUT_MODULE, index=False, encoding="utf-8")

if __name__ == "__main__":
    compute_mastery()
