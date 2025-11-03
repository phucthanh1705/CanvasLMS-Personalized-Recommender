# Gọi API Canvas → lấy courses, modules, items, quiz
import os, json, requests, time
from config.config import BASE_URL, HEADERS, DATA_DIR
print("Using headers:", HEADERS)

# ====== Utility ======
def save_json(filename, data):
    path = os.path.join(DATA_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"💾 Saved: {filename}")

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

# ====== Fetch functions ======
def fetch_courses():
    url = f"{BASE_URL}/courses"
    data = get_json(url)
    save_json("courses.json", data)
    return data

def fetch_modules(course_id):
    url = f"{BASE_URL}/courses/{course_id}/modules?per_page=100"
    data = get_json(url)
    save_json(f"modules_course_{course_id}.json", data)
    return data

def fetch_items(course_id, module_id):
    url = f"{BASE_URL}/courses/{course_id}/modules/{module_id}/items?per_page=100"
    data = get_json(url)
    save_json(f"items_module_{module_id}.json", data)
    return data

def fetch_quiz(course_id, quiz_id):
    url = f"{BASE_URL}/courses/{course_id}/quizzes/{quiz_id}"
    data = get_json(url)
    save_json(f"quiz_{quiz_id}.json", data)
    return data

def fetch_quiz_submissions(course_id, quiz_id):
    url = f"{BASE_URL}/courses/{course_id}/quizzes/{quiz_id}/submissions?per_page=100"
    data = get_json(url)
    save_json(f"quiz_{quiz_id}_submissions.json", data)
    return data

def fetch_quiz_questions(submission_id):
    url = f"{BASE_URL}/quiz_submissions/{submission_id}/questions?include[]=quiz_question"
    data = get_json(url)
    save_json(f"quiz_submission_{submission_id}_questions.json", data)
    return data

# ====== Main pipeline ======
def run_pipeline():
    courses = fetch_courses()
    print(f"📘 Found {len(courses)} courses")

    for course in courses:
        course_id = course["id"]
        course_name = course["name"]
        print(f"\n🏫 Course: {course_name} (ID={course_id})")

        modules = fetch_modules(course_id)
        for mod in modules:
            module_id = mod["id"]
            module_name = mod["name"]
            print(f"   📂 Module: {module_name} (ID={module_id})")

            items = fetch_items(course_id, module_id)
            lessons, assignments, quizzes = [], [], []

            for it in items:
                t = it.get("type")
                if t == "Page":
                    lessons.append(it)
                elif t == "Assignment":
                    assignments.append(it)
                elif t == "Quiz":
                    quizzes.append(it)

            save_json(f"lessons_module_{module_id}.json", lessons)
            save_json(f"assignments_module_{module_id}.json", assignments)
            save_json(f"quizzes_module_{module_id}.json", quizzes)

            # Lấy chi tiết quiz + bài làm
            for q in quizzes:
                quiz_id = q.get("content_id") or q.get("id")
                print(f"      🧩 Quiz: {q['title']} (ID={quiz_id})")

                quiz_info = fetch_quiz(course_id, quiz_id)
                submissions = fetch_quiz_submissions(course_id, quiz_id)

                for sub in submissions:
                    sub_id = sub.get("id") or sub.get("quiz_submission_id")
                    fetch_quiz_questions(sub_id)

    print("\n✅ Canvas data fetching completed.")

if __name__ == "__main__":
    run_pipeline()
