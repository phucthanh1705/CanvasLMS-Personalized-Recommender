import os, json, requests, time
from config.config import BASE_URL, HEADERS, DATA_DIR

# ==================== Utility ====================

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def save_json(path, data):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"💾 Saved: {path}")

def get_json(url):
    for attempt in range(3):
        try:
            res = requests.get(url, headers=HEADERS)
            if res.status_code == 200:
                return res.json()
            else:
                print(f"⚠️ Error {res.status_code}: {url}")
        except Exception as e:
            print("❌ Request error:", e)
            time.sleep(1)
    return []

# ==================== Fetch Functions ====================

def fetch_users(course_id, course_dir):
    url = f"{BASE_URL}/courses/{course_id}/users?per_page=100"
    data = get_json(url)
    save_json(os.path.join(course_dir, "users_course.json"), data)
    return data

def fetch_courses():
    url = f"{BASE_URL}/courses"
    data = get_json(url)
    save_json(os.path.join(DATA_DIR, "raw", "courses.json"), data)
    return data

def fetch_modules(course_id, course_dir):
    url = f"{BASE_URL}/courses/{course_id}/modules?per_page=100"
    data = get_json(url)
    save_json(os.path.join(course_dir, "modules", f"modules_course_{course_id}.json"), data)
    return data

def fetch_items(course_id, module_id, module_dir):
    url = f"{BASE_URL}/courses/{course_id}/modules/{module_id}/items?per_page=100"
    data = get_json(url)
    save_json(os.path.join(module_dir, f"items_module_{module_id}.json"), data)
    return data

def fetch_lesson(course_id, page_url, lesson_dir):
    url = f"{BASE_URL}/courses/{course_id}/pages/{page_url}"
    data = get_json(url)
    save_json(os.path.join(lesson_dir, f"lesson_{page_url}.json"), data)
    # Nếu có content_html thì lưu ra file riêng
    if "body" in data:
        html_path = os.path.join(lesson_dir, "contents", f"lesson_{page_url}.html")
        ensure_dir(os.path.dirname(html_path))
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(data["body"])
    return data

def fetch_quiz(course_id, quiz_id, quiz_dir):
    # Thông tin câu hỏi
    url = f"{BASE_URL}/courses/{course_id}/quizzes/{quiz_id}/questions?include[]=answers&per_page=100"
    data = get_json(url)
    save_json(os.path.join(quiz_dir, f"quiz_{quiz_id}.json"), data)
    return data

def fetch_quiz_submissions(course_id, quiz_id, quiz_dir):
    url = f"{BASE_URL}/courses/{course_id}/quizzes/{quiz_id}/submissions?include[]=user&per_page=100&page=1"
    data = get_json(url)
    save_json(os.path.join(quiz_dir, "submissions", f"quiz_{quiz_id}_submissions.json"), data)
    return data

# ==================== Main Pipeline ====================

def run_pipeline():
    raw_dir = os.path.join(DATA_DIR, "raw")
    courses_root = os.path.join(raw_dir, "courses")
    users_root = os.path.join(raw_dir, "users")

    ensure_dir(courses_root)
    ensure_dir(users_root)

    courses = fetch_courses()
    print(f"📘 Found {len(courses)} courses")

    for course in courses:
        course_id = course["id"]
        course_name = course.get("name", "unnamed")
        course_dir = os.path.join(courses_root, f"course_{course_id}")
        ensure_dir(course_dir)

        print(f"\n🏫 Course: {course_name} (ID={course_id})")

        # === Users in course ===
        users = fetch_users(course_id, course_dir)
        save_json(os.path.join(users_root, f"users_course_{course_id}.json"), users)

        # === Modules ===
        modules = fetch_modules(course_id, course_dir)
        for mod in modules:
            module_id = mod["id"]
            module_name = mod["name"]
            module_dir = os.path.join(course_dir, "modules", f"module_{module_id}")
            ensure_dir(module_dir)
            print(f"   📂 Module: {module_name} (ID={module_id})")

            # === Items in each module ===
            items = fetch_items(course_id, module_id, module_dir)
            lessons, quizzes = [], []

            for item in items:
                t = item.get("type")
                if t == "Page":
                    lessons.append(item)
                elif t == "Quiz":
                    quizzes.append(item)

            save_json(os.path.join(module_dir, f"lessons_module_{module_id}.json"), lessons)
            save_json(os.path.join(module_dir, f"quizzes_module_{module_id}.json"), quizzes)

            # === Fetch each lesson ===
            lesson_dir = os.path.join(module_dir, "lessons")
            for l in lessons:
                page_url = l.get("page_url")
                if page_url:
                    print(f"      📖 Lesson: {page_url}")
                    fetch_lesson(course_id, page_url, lesson_dir)

            # === Fetch each quiz ===
            quiz_dir = os.path.join(module_dir, "quizzes")
            for q in quizzes:
                quiz_id = q.get("content_id") or q.get("id")
                title = q.get("title", "")
                print(f"      🧩 Quiz: {title} (ID={quiz_id})")
                fetch_quiz(course_id, quiz_id, quiz_dir)
                fetch_quiz_submissions(course_id, quiz_id, quiz_dir)

    print("\n✅ Canvas data fetching completed successfully.")


if __name__ == "__main__":
    run_pipeline()
