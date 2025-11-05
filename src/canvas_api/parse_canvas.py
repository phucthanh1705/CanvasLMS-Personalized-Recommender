# Chuyển JSON thành cấu trúc học (Course–Module–Item)
import os, json, shutil
from pathlib import Path
from ..utils.io_utils import load_json, save_json, clean_html

RAW_PATH = Path("data/raw/raw/courses")
PROCESSED_PATH = Path("data/processed/courses")

# ===== 1️⃣ HÀM LÀM SẠCH CẤP TRƯỜNG DỮ LIỆU =====
def clean_dict(data: dict, allowed_fields: list):
    """Giữ lại các trường cần thiết"""
    return {k: v for k, v in data.items() if k in allowed_fields and v not in [None, "", []]}


def process_module(module_dir: Path, out_module_dir: Path):
    """
    Làm sạch và sao chép toàn bộ dữ liệu của một module:
    - Giữ nguyên cấu trúc lessons / quizzes
    - Đọc nội dung bài học từ file HTML (hoặc từ JSON nếu có)
    - Giữ quiz và chỉ xuất bản cleaned submissions
    - Sinh metadata chi tiết cho module
    """
    out_module_dir.mkdir(parents=True, exist_ok=True)
    module_meta = {
        "id": module_dir.name,
        "lessons": [],
        "quizzes": [],
        "quiz_submissions": []
    }

        # ==============================
    # 1️⃣ LÀM SẠCH BÀI HỌC (LESSONS)
    # ==============================
    lessons_src = module_dir / "lessons" / "contents"
    lessons_dst = out_module_dir / "lessons" / "contents"
    if lessons_src.exists():
        lessons_dst.mkdir(parents=True, exist_ok=True)
        lesson_count = 0

        for html_file in lessons_src.glob("*.html"):
            json_file = html_file.with_suffix(".json")

            # Đọc nội dung HTML gốc
            with open(html_file, "r", encoding="utf-8") as f:
                html_content = f.read().strip()

            # Nếu có file JSON đi kèm → đọc thêm metadata (title, id, updated_at)
            if json_file.exists():
                lesson_meta = load_json(json_file)
                title = lesson_meta.get("title") or html_file.stem
                updated_at = lesson_meta.get("updated_at")
                lesson_id = lesson_meta.get("id") or html_file.stem
            else:
                title = html_file.stem
                updated_at = None
                lesson_id = html_file.stem

            # Tạo bản ghi bài học có nội dung HTML trong field "body"
            cleaned = {
                "id": lesson_id,
                "title": title,
                "body": html_content,
                "updated_at": updated_at
            }

            # Ghi file JSON mới sang processed
            save_json(cleaned, lessons_dst / json_file.name)
            # Copy luôn HTML gốc sang processed (để bạn dùng sau)
            shutil.copy2(html_file, lessons_dst / html_file.name)

            module_meta["lessons"].append({
                "id": cleaned["id"],
                "title": cleaned["title"]
            })
            lesson_count += 1

        print(f"📚 Found {lesson_count} lesson(s) (HTML-based) in {module_dir.name}")
    else:
        print(f"⚠️ No lessons found in {module_dir.name}")


    # ==============================
    # 2️⃣ XỬ LÝ QUIZZES & SUBMISSIONS
    # ==============================
    quizzes_src = module_dir / "quizzes"
    quizzes_dst = out_module_dir / "quizzes"
    if quizzes_src.exists():
        quizzes_dst.mkdir(parents=True, exist_ok=True)

        quiz_count, submission_count, total_avg_score = 0, 0, 0.0

        # ---- 2.1. DUYỆT QUIZ CHÍNH ----
        for f in quizzes_src.glob("quiz_*.json"):
            # Bỏ qua file submissions trong thư mục chính
            if "submissions" in f.name.lower():
                continue

            shutil.copy2(f, quizzes_dst / f.name)
            quiz_data = load_json(f)
            quiz_count += 1

            quiz_entry = {
                "file": f.name,
                "question_count": 0,
                "questions": []
            }

            # Tìm danh sách câu hỏi
            question_list = []
            if isinstance(quiz_data, list):
                question_list = quiz_data
            elif isinstance(quiz_data, dict):
                for key in ["questions", "quiz_questions", "data"]:
                    if key in quiz_data and isinstance(quiz_data[key], list):
                        question_list = quiz_data[key]
                        break

            for q in question_list:
                q_text = q.get("question_text") or q.get("text") or q.get("description") or ""
                if q_text:
                    quiz_entry["questions"].append(clean_html(q_text))

            quiz_entry["question_count"] = len(quiz_entry["questions"])
            module_meta["quizzes"].append(quiz_entry)

        # ---- 2.2. DUYỆT SUBMISSIONS (quizzes/submissions/) ----
        submissions_src = quizzes_src / "submissions"
        if submissions_src.exists():
            submissions_dst = quizzes_dst / "submissions"
            submissions_dst.mkdir(parents=True, exist_ok=True)

            for f in submissions_src.glob("*.json"):
                submissions_data = load_json(f)
                subs = submissions_data.get("quiz_submissions", [])
                submission_count += len(subs)

                student_scores = []
                total_score = 0.0

                for s in subs:
                    raw_score = s.get("score")
                    try:
                        score = float(raw_score) if raw_score is not None else 0.0
                    except (ValueError, TypeError):
                        score = 0.0

                    total_score += score
                    student_scores.append({
                        "user_id": s.get("user_id"),
                        "quiz_id": s.get("quiz_id"),
                        "submission_id": s.get("id"),
                        "score": score,
                        "quiz_points_possible": s.get("quiz_points_possible"),
                        "time_spent": s.get("time_spent"),
                        "attempt": s.get("attempt"),
                        "workflow_state": s.get("workflow_state")
                    })

                avg_score = round(total_score / len(subs), 2) if subs else 0
                total_avg_score += avg_score
                top_students = sorted(student_scores, key=lambda x: x["score"], reverse=True)[:5]

                cleaned_submission_path = submissions_dst / f"cleaned_{f.name}"
                save_json(student_scores, cleaned_submission_path)

                # Chỉ ghi bản cleaned vào processed
                module_meta["quiz_submissions"].append({
                    "file_cleaned": f"submissions/{cleaned_submission_path.name}",
                    "count": len(subs),
                    "average_score": avg_score,
                    "top_5_students": top_students
                })

        avg_total = round(total_avg_score / max(quiz_count, 1), 2)
        print(f"🧩 Found {quiz_count} quiz(es), {submission_count} submission(s) in {module_dir.name} | Avg Score: {avg_total}")
    else:
        print(f"⚠️ No quizzes found in {module_dir.name}")

    # ==============================
    # 3️⃣ LƯU METADATA CUỐI CÙNG
    # ==============================
    save_json(module_meta, out_module_dir / f"meta_{module_dir.name}.json")
    print(f"✅ Processed module {module_dir.name}")



# ===== 3️⃣ HÀM LÀM SẠCH MỘT COURSE =====
def process_course(course_dir: Path):
    course_name = course_dir.name
    out_dir = PROCESSED_PATH / course_name
    out_dir.mkdir(parents=True, exist_ok=True)

    # --- copy users ---
    users_src = course_dir / "users"
    users_dst = out_dir / "users"
    users_dst.mkdir(parents=True, exist_ok=True)
    for f in users_src.glob("*.json"):
        shutil.copy2(f, users_dst / f.name)

    # --- copy course.json ---
    course_file = course_dir / "courses.json"
    if course_file.exists():
        shutil.copy2(course_file, out_dir / "courses.json")

    # --- process each module ---
    modules_src = course_dir / "modules"
    modules_dst = out_dir / "modules"
    modules_dst.mkdir(parents=True, exist_ok=True)
    meta_summary = {"course": course_name, "modules": []}

    for module_dir in modules_src.iterdir():
        if not module_dir.is_dir():
            continue
        out_module_dir = modules_dst / module_dir.name
        process_module(module_dir, out_module_dir)
        meta_summary["modules"].append(module_dir.name)

    save_json(meta_summary, out_dir / f"meta_{course_name}.json")
    print(f"🎓 Finished course {course_name}")


# ===== 4️⃣ HÀM CHẠY TỔNG HỢP =====
def run_parse_pipeline():
    print(f"🔍 Looking for raw courses in: {RAW_PATH.resolve()}")
    for course_dir in RAW_PATH.glob("course_*"):
        print(f"📘 Found course: {course_dir.name}")
        process_course(course_dir)


if __name__ == "__main__":
    run_parse_pipeline()
