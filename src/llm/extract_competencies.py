from pathlib import Path
import json
import csv
import time

from src.utils.io_utils import clean_html
from src.llm.llm_client import call_llm

COURSE_DIR = Path("data/processed/courses")
OUT_DIR = Path("data/llm/competencies/")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = OUT_DIR / "course_competencies_llm.csv"

SYSTEM_PROMPT = """
Bạn là trợ lý phân tích chương trình học.
Hãy đọc nội dung bài học và tạo ra danh sách COMPETENCY.

TRẢ VỀ DUY NHẤT MỘT MẢNG JSON:
[
  {
    "competency_id": "HTML_Basics",
    "name": "HTML cơ bản",
    "description": "Hiểu cấu trúc tài liệu HTML",
    "domain": "Web_Frontend"
  }
]
"""


def iter_lessons():
    """Duyệt tất cả file bài học."""
    for path in COURSE_DIR.rglob("lessons/contents/*.json"):
        parts = path.parts
        course_id = parts[parts.index("courses") + 1]
        module_id = parts[parts.index("modules") + 1]
        yield course_id, module_id, path


def safe_json_extract(text: str):
    """Extract JSON array từ output nhiều rác của LLM."""
    try:
        return json.loads(text)
    except:
        start = text.find("[")
        end = text.rfind("]") + 1
        return json.loads(text[start:end])


def main():
    rows = []
    all_lessons = list(iter_lessons())
    total = len(all_lessons)

    start_all = time.time()

    for idx, (course_id, module_id, path) in enumerate(all_lessons, start=1):

        percent = (idx / total) * 100

        print(f"\n==============================")
        print(f"▶ [{idx}/{total}] ({percent:.2f}%)  {course_id} / {module_id}")
        print(f"Đang xử lý: {path}")
        print("==============================")

        start_item = time.time()

        data = json.loads(path.read_text(encoding="utf-8"))
        body = clean_html(data.get("body", ""))

        if not body:
            continue

        user_prompt = f"""
Course: {course_id}
Module: {module_id}

Nội dung bài học:

\"\"\"{body}\"\"\"
"""

        lesson_length = len(body)

        if lesson_length > 3000:
            selected_model = "qwen2.5:7b"
        elif lesson_length > 1500:
            selected_model = "qwen2.5:3b-instruct"
        else:
            selected_model = "mistral"

        raw = call_llm(
            system_prompt=SYSTEM_PROMPT,
            user_prompt=user_prompt,
            model=selected_model
        )

        if not raw or raw.strip() == "":
            continue

        try:
            parsed = safe_json_extract(raw)
        except Exception as e:
            print("Không parse được JSON:", e)
            print("Raw:", raw)
            continue

        for c in parsed:
            if isinstance(c, str):
                rows.append({
                    "course_id": course_id,
                    "module_id": module_id,
                    "competency_id": c.lower().replace(" ", "_").replace("-", "_"),
                    "name": c,
                    "description": "",
                    "domain": ""
                })
                continue

            rows.append({
                "course_id": course_id,
                "module_id": module_id,
                "competency_id": c.get("competency_id", "").strip(),
                "name": c.get("name", "").strip(),
                "description": c.get("description", "").strip(),
                "domain": c.get("domain", "").strip(),
            })


    end_all = time.time()
    print(f"\n==============================")
    print(f"Hoàn thành tất cả bài học!")
    print(f"Tổng thời gian chạy: {end_all - start_all:.2f} giây")
    print("==============================\n")

    with OUT_FILE.open("w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(
            f,
            fieldnames=["course_id", "module_id", "competency_id", "name",
                        "description", "domain"]
        )
        wr.writeheader()
        wr.writerows(rows)

    print(f"📌 File competency đã lưu tại: {OUT_FILE}")


if __name__ == "__main__":
    main()
