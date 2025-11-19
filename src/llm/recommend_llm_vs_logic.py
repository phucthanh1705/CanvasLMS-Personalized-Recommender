# -*- coding: utf-8 -*-
"""
LLM + Logic Recommender Merger
Kết hợp:
  (1) Gợi ý từ LLM-KG (embedding neo4j)
  (2) Gợi ý từ Logic (recommendations.csv)
Output:
  data/exports/final_recommendations.csv
"""

import os
import pandas as pd
import numpy as np
from pathlib import Path
from neo4j import GraphDatabase
from dotenv import load_dotenv


# ============================================================
# -------- Paths & ENV--------
# ============================================================

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
ENV_PATH = os.path.join(BASE_DIR, "config", "secrets.env")
load_dotenv(ENV_PATH)

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASS = os.getenv("NEO4J_PASS", "neo4j")
NEO4J_DB   = os.getenv("NEO4J_DB",  "neo4j")

EXPORT_DIR = Path(os.path.join(BASE_DIR, "data", "exports"))
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

LLM_EXPORT = EXPORT_DIR / "final_recommendations.csv"
LOGIC_EXPORT = EXPORT_DIR / "recommendations.csv"


# ============================================================
# -------- CONNECT NEO4J --------
# ============================================================

def neo4j_session():
    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USER, NEO4J_PASS),
        encrypted=False
    )
    return driver.session(database=NEO4J_DB)

# ============================================================
# -------- LẤY DANH SÁCH USER_ID --------
# ============================================================

def get_all_students():
    """
    Trả về danh sách student.id trong graph
    """
    with neo4j_session() as session:
        rows = session.run("""
            MATCH (s:Student)
            RETURN s.id AS id
            ORDER BY id
        """)
        return [r["id"] for r in rows]

# ============================================================
# -------- 4. GỢI Ý CHO TẤT CẢ STUDENTS --------
# ============================================================

def recommend_for_all_students(topk=10):
    students = get_all_students()

    print(f"\n📌 Found {len(students)} students in Neo4j")

    all_results = []
    for sid in students:
        print(f"\n🎯 Merging recommendations for {sid}...")
        rows = final_recommend_one_user(sid, topk=topk, export=False)
        all_results.extend(rows)

    # xuất file cuối
    df = pd.DataFrame(all_results)
    df.to_csv(LLM_EXPORT, index=False)
    print(f"\n💾 Final merged recommendations saved → {LLM_EXPORT}")

    return all_results

# ============================================================
# -------- LẤY 1 USER_ID DUY NHẤT --------
# ============================================================

def get_one_student(id):
    """
    Lấy đúng 1 student.id cố định.
    Thay đổi student_id tại đây.
    """
    userId = "user_"+ str(id)
    return userId


# ============================================================
# -------- 1. GỢI Ý TỪ ĐỒ THỊ LLM-KG --------
# ============================================================

def recommend_from_llm_KG(student_id, topk=5):

    with neo4j_session() as session:

        stu = session.run("""
            MATCH (s:Student {id:$sid})
            RETURN s.embedding AS emb
        """, sid=student_id).single()

        if not stu or stu["emb"] is None:
            print(f"⚠️ Student {student_id} không có embedding trong Neo4j.")
            return []

        stu_vec = np.array(stu["emb"])

        results = session.run("""
            MATCH (m:Module)
            WHERE m.embedding IS NOT NULL
            RETURN m.id AS module_id, m.embedding AS emb
        """)

        recs = []
        for row in results:
            mod_vec = np.array(row["emb"])
            sim = float(stu_vec @ mod_vec) / (np.linalg.norm(stu_vec) * np.linalg.norm(mod_vec))
            recs.append((row["module_id"], sim))

        return sorted(recs, key=lambda x: x[1], reverse=True)[:topk]


# ============================================================
# -------- 2. GỢI Ý LOGIC CSV --------
# ============================================================

def load_logic_recommendations(student_id):
    df = pd.read_csv(LOGIC_EXPORT)
    df = df[df["student"] == student_id]
    return [(row.module_id, float(row.similarity)) for _, row in df.iterrows()]


# ============================================================
# -------- 3. HỢP NHẤT (INTERSECTION) --------
# ============================================================

def final_recommend_one_user(student_id, export=True):

    topk = 5  # 🔥 chỉ gợi ý 5 môn

    llm_rec = recommend_from_llm_KG(student_id, topk)
    logic_rec = load_logic_recommendations(student_id)

    llm_modules   = {m for m, _ in llm_rec}
    logic_modules = {m for m, _ in logic_rec}

    final = llm_modules.intersection(logic_modules)

    final_rows = [{"student": student_id, "module_id": module} for module in final]

    if export:
        df = pd.DataFrame(final_rows)
        df.to_csv(LLM_EXPORT, index=False)
        print(f"💾 Saved final recommendations → {LLM_EXPORT}")

    return final_rows


# ============================================================
# -------- 4. MAIN — CHỈ GỌI 1 HÀM --------
# ============================================================
def recommend_llm_vs_logic(id):
    student = get_one_student(id)   # 🔥 Lấy đúng 1 user_id duy nhất từ Neo4j

    if student is None:
        print("⚠️ Không tìm thấy Student nào trong hệ thống.")
    else:
        print(f"\n🎯 Gợi ý 5 môn học cho user: {student}")
        result = final_recommend_one_user(student)

        print("\n⭐ Kết quả gợi ý cuối cùng:")
        for r in result:
            print(" -", r["module_id"])

    
if __name__ == "__main__":
    recommend_llm_vs_logic(71)