import os
from pathlib import Path
import pandas as pd
import numpy as np
from neo4j import GraphDatabase
from dotenv import load_dotenv


# ============================================================
# 1. LOAD ENV – GIỐNG Y FILE config.py CỦA BẠN
# ============================================================

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
ENV_PATH = os.path.join(BASE_DIR, "config", "secrets.env")

load_dotenv(ENV_PATH)

NEO4J_URI  = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASS = os.getenv("NEO4J_PASS")
NEO4J_DB   = os.getenv("NEO4J_DB")

EXPORT_DIR = Path(os.path.join(BASE_DIR, "data", "exports"))
EXPORT_DIR.mkdir(parents=True, exist_ok=True)
FINAL_EXPORT = EXPORT_DIR / "final_recommendations.csv"


# ============================================================
# 2. NEO4J SESSION (FIX encrypted=False)
# ============================================================

def neo4j_session():
    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USER, NEO4J_PASS),
        encrypted=False   # 🔥 BẮT BUỘC CHO NEO4J DESKTOP
    )
    return driver.session(database=NEO4J_DB)


# ============================================================
# 3. GỢI Ý TỪ LLM-KG NEO4J
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
            emb = np.array(row["emb"])
            sim = float(stu_vec @ emb) / (np.linalg.norm(stu_vec) * np.linalg.norm(emb))
            recs.append((row["module_id"], sim))

        return sorted(recs, key=lambda x: x[1], reverse=True)[:topk]


# ============================================================
# 4. GỢI Ý TỪ LOGIC CSV
# ============================================================

def load_logic_recommendations(student_id):
    csv_path = EXPORT_DIR / "recommendations.csv"
    df = pd.read_csv(csv_path)
    df = df[df["student"] == student_id]
    return [(row.module_id, float(row.similarity)) for _, row in df.iterrows()]


# ============================================================
# 5. FINAL RECOMMEND (INTERSECTION)
# ============================================================

def final_recommend(student_id, topk=10, export=True):
    llm_rec = recommend_from_llm_KG(student_id, topk)
    logic_rec = load_logic_recommendations(student_id)

    llm_modules   = {m for m, _ in llm_rec}
    logic_modules = {m for m, _ in logic_rec}

    final = llm_modules.intersection(logic_modules)

    final_list = [{"student": student_id, "module_id": m} for m in final]

    if export:
        df = pd.DataFrame(final_list)
        df.to_csv(FINAL_EXPORT, index=False)
        print(f"\n📄 File gợi ý cuối đã xuất: {FINAL_EXPORT}")

    return final_list


# ============================================================
# 6. TEST
# ============================================================

if __name__ == "__main__":
    student = "user_38"
    print("🎯 Đang tạo danh sách môn nên học tiếp theo cho", student)

    result = final_recommend(student, topk=10, export=True)

    print("\n⭐ MÔN HỌC CUỐI CÙNG NÊN GỢI Ý:")
    for r in result:
        print(" -", r["module_id"])
