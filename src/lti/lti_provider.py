from flask import Flask, request, jsonify, redirect
import json
import jwt
import time
from jwt import PyJWK, PyJWKClient
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
import uuid
import os
import sys
from pathlib import Path
import psycopg2
import threading
import time
import traceback
from datetime import datetime

last_sync_time = None
current_progress = ""
progress_percent = 0
from canvas_api_manager import get_assigned_users, assign_user, send_canvas_message

app = Flask(__name__)

CLIENT_ID = "10000000000022"
CLIENT_SECRET = "MUNu2xHB2Uvc6a9KFQzuKGLELRVtzwrVtYUVaFQE6Lx3cZ9WwEh8tMUeQKeMEMh4"
PLATFORM_ISS = "http://localhost:3000"
CANVAS_JWKS = "http://localhost:3000/api/lti/security/jwks"
TOOL_REDIRECT_URI = "http://127.0.0.1.nip.io:5000/lti/launch"
TOOL_LOGIN_URI = "http://127.0.0.1.nip.io:5000/lti/login"

JWKS_URL = "http://127.0.0.1.nip.io:5000/.well-known/jwks.json"

BASE_DIR = Path(__file__).resolve().parents[2]   
SRC_DIR = BASE_DIR / "src"
sys.path.append(str(SRC_DIR))

from llm.recommend_llm_vs_logic import recommend_binary
from llm.recommend_llm_vs_logic import neo4j_session
from analytics.skill_computation import compute_skill_percentages
from lti.log_service import (
    log_lti_launch,
    log_system_error,
    log_lti_event,
    update_platform_last_launch,
)
from app.run_full_pipeline import run_pipeline_once

private_key = rsa.generate_private_key(
    public_exponent=65537,
    key_size=2048,
)

public_key = private_key.public_key()

public_numbers = public_key.public_numbers()
e = public_numbers.e
n = public_numbers.n

jwk = {
    "kty": "RSA",
    "use": "sig",
    "alg": "RS256",
    "kid": "mysmart-key",
    "n": jwt.utils.base64url_encode(n.to_bytes((n.bit_length() + 7) // 8, "big")).decode(),
    "e": jwt.utils.base64url_encode(e.to_bytes((e.bit_length() + 7) // 8, "big")).decode(),
}

jwks = {"keys": [jwk]}

def get_module_name(mid: str, course_id: str):
    """
    mid: dạng module_46 hoặc module_50
    course_id: '4' hoặc 4 đều được
    """

    try:
        course_id = int(course_id)
    except:
        return ""

    modules_file = Path(f"data/raw/raw/courses/course_{course_id}/modules/modules_course_{course_id}.json")

    if not modules_file.exists():
        return ""

    try:
        with open(modules_file, "r", encoding="utf-8") as f:
            modules = json.load(f)
    except:
        return ""

    if isinstance(mid, str) and mid.startswith("module_"):
        try:
            module_id = int(mid.replace("module_", ""))
        except:
            return ""
    else:
        try:
            module_id = int(mid)
        except:
            return ""

    for m in modules:
        if m.get("id") == module_id:
            return m.get("name", "")

    return ""


def get_course_students(course_id):
    users_file = Path(f"data/raw/raw/users/users_course_{course_id}.json")
    course_user_map = {}
    if users_file.exists():
        with open(users_file, "r", encoding="utf-8") as f:
            course_users = json.load(f)
        for u in course_users:
            course_user_map[u["id"]] = u
    else:
        print(f"Không tìm thấy file users_{course_id}.json")
    return course_user_map

def get_learned_modules(student_id):
    with neo4j_session() as session:
        rows = session.run("""
            MATCH (:Student {id:$sid})-[r:mastery_on]->(m:Module)
            RETURN m.id AS id, r.mastery AS mastery
            ORDER BY id
        """, sid=student_id)

        return [(r["id"], r["mastery"] or 0) for r in rows]

def get_student_modules(student_id):
    with neo4j_session() as session:
        completed_rows = session.run(
            """
            MATCH (:Student {id:$sid})-[:mastery_on]->(m:Module)
            RETURN m.id AS id, m.name AS name
            ORDER BY id
        """,
            sid=student_id,
        )
        completed = [dict(r) for r in completed_rows]

        inprog_rows = session.run(
            """
            MATCH (:Student {id:$sid})-[:attempted]->(q:Quiz)<-[:has_quiz]-(m:Module)
            WHERE NOT (:Student {id:$sid})-[:mastery_on]->(m)
            RETURN DISTINCT m.id AS id, m.name AS name
            ORDER BY id
        """,
            sid=student_id,
        )
        in_progress = [dict(r) for r in inprog_rows]

        all_rows = session.run(
            """
            MATCH (m:Module)
            RETURN m.id AS id, m.name AS name
        """
        )
        all_modules = [dict(r) for r in all_rows]

    done_ids = {m["id"] for m in completed}
    prog_ids = {m["id"] for m in in_progress}
    not_started = [m for m in all_modules if m["id"] not in done_ids | prog_ids]

    return completed, in_progress, not_started


def get_recommended_modules(student_id, topk=5):
    numeric_id = int(student_id.replace("user_", ""))

    results = recommend_binary(numeric_id, topk=topk)

    module_ids = [r["module_id"] for r in results]

    if not module_ids:
        return []

    with neo4j_session() as session:
        mrows = session.run(
            """
            MATCH (m:Module)
            WHERE m.id IN $ids
            RETURN m.id AS id, m.name AS name
            ORDER BY id
        """,
            ids=module_ids,
        )
        return [dict(r) for r in mrows]


def render_module_list(title, modules, course_id):
    if not modules:
        return f"<h3>{title}</h3><p><em>Không có dữ liệu.</em></p>"
    items = "".join(f"<li>{get_module_name(m.get('id'), course_id)}</li>" for m in modules)
    return f"<h3>{title}</h3><ul>{items}</ul>"

def render_module_template(mode, module_name="", student_id="", items=""):
    template_path = Path(__file__).resolve().parents[1] / "lti" / "module_result.html"
    html = template_path.read_text(encoding="utf-8")

    html = html.replace("{module_name}", module_name)
    html = html.replace("{student_id}", student_id)
    html = html.replace("{items}", items)

    html = html.replace("{insufficient_class}", "visible" if mode == "insufficient" else "hidden")
    html = html.replace("{recommended_class}", "visible" if mode == "recommended" else "hidden")

    return html

@app.route("/.well-known/jwks.json")
def serve_jwks():
    return jsonify(jwks)

@app.route("/lti/login", methods=["POST"])
def lti_login():
    login_hint = request.form.get("login_hint")
    lti_message_hint = request.form.get("lti_message_hint")

    state = str(uuid.uuid4())
    nonce = str(uuid.uuid4())

    redirect_url = (
        f"{PLATFORM_ISS}/api/lti/authorize_redirect"
        f"?response_type=id_token"
        f"&client_id={CLIENT_ID}"
        f"&redirect_uri={TOOL_REDIRECT_URI}"
        f"&scope=openid"
        f"&response_mode=form_post"
        f"&prompt=none"
        f"&state={state}"
        f"&nonce={nonce}"
        f"&login_hint={login_hint}"
        f"&lti_message_hint={lti_message_hint}"
    )

    return redirect(redirect_url)

@app.route("/api/recommend/<student_id>")
def api_recommend(student_id):
    try:
        course_id = request.args.get("course_id")
        if not course_id:
            return "<p>❌ Chưa chọn khóa học.</p>"

        rec_modules = get_recommended_modules(student_id, topk=1)
        if not rec_modules:
            return "<p><em>Không có gợi ý.</em></p>"

        learned_modules = get_learned_modules(student_id)

        insufficient = []
        for mid, mastery in learned_modules:
            if mastery < 0.5:
                insufficient.append((mid, mastery))

        if insufficient:
            html_list = "".join(
                f"<li>{mid} – mastery: {round(mastery*100, 2)}%</li>"
                for mid, mastery in insufficient
            )

            return render_module_template(
            mode="insufficient",
            items=html_list
        )



        recommended = rec_modules[0]       
        module_id_str = recommended.get("id")
        try:
            module_id_int = int(str(module_id_str).replace("module_", ""))
        except:
            return f"<p>Không parse được module ID từ {module_id_str}.</p>"
        try:
            user_numeric_id = int(student_id.replace("user_", ""))
            user_id_to_assign = str(user_numeric_id)
        except:
            return f"<p>Không parse được ID dạng số từ {student_id}.</p>"

        print(f"\nGán user {user_id_to_assign} vào module {module_id_int} ---")

        get_assigned_users(module_id_int)
        assign_user(user_id_to_assign, module_id_int)

        module_name = get_module_name(recommended.get("id"), course_id)

        subject = f"[SmartSchool] 📚 Môn học mới được gợi ý cho bạn"
        body = (
            f"<p>Chào bạn **{student_id}** (Canvas ID: **{user_id_to_assign}**),</p>"
            f"<p>Hệ thống SmartSchool đã phân tích dữ liệu và gán bạn vào module:</p>"
            f"<ul><li>{module_name}</li></ul>"
            f"<p>Bạn có thể vào trang <b>Modules</b> trong khóa học để bắt đầu học.</p>"
            f"<p>Chúc bạn học tốt!</p>"
        )

        send_canvas_message(
            recipient_ids=[user_id_to_assign],
            subject=subject,
            body=body,
            course_id=int(course_id)
        )

        return render_module_template(
            mode="recommended",
            module_name=module_name,
            student_id=student_id
        )

    except Exception as e:
        return f"<p>❌ Lỗi khi gợi ý môn: {str(e)}</p>"

@app.route("/lti/launch", methods=["POST"])
def lti_launch():
    id_token = request.form.get("id_token")
    if not id_token:
        return "❌ Không nhận được id_token từ Canvas (Canvas chưa launch đúng)."

    try:
        jwk_client = PyJWKClient(CANVAS_JWKS)
        signing_key = jwk_client.get_signing_key_from_jwt(id_token).key

        decoded = jwt.decode(
            id_token,
            signing_key,
            algorithms=["RS256"],
            audience=CLIENT_ID,
            options={"verify_exp": False},
        )

        sub = decoded.get("sub")  
        roles = decoded.get("https://purl.imsglobal.org/spec/lti/claim/roles", [])
        context = decoded.get("https://purl.imsglobal.org/spec/lti/claim/context", {})
        course_url = context.get("tool_consumer_instance_guid") or context.get("custom_canvas_course_url")
        course_id_str = request.form.get("course_id") or request.args.get("course_id") or "4" 
        try:
            course_id = int(course_id_str)
        except:
            return "<p>Không parse được course_id</p>"

        course_name = f"Course {course_id}"
        if course_id is not None:
            course_id = int(course_id)
        else:
            return "<p>Khóa học này không tồn tại trong hệ thống.</p>"
        is_student = any("Learner" in r or "Student" in r for r in roles)
        user_role = "Unknown"
        for r in roles:
            if "Instructor" in r:
                user_role = "Instructor"
                break
            elif "Learner" in r or "Student" in r:
                user_role = "Student"
                break
        
        try:
            log_lti_launch(
                platform="Canvas LMS",
                user=sub,  
                resource="Student Dashboard" if is_student else "Admin Dashboard",
                status="Success",
            )

            update_platform_last_launch("Canvas LMS")
        except Exception as log_err:
            print("[LogService Error]", log_err)
            
        with neo4j_session() as session:
            row = session.run(
                """
                MATCH (s:Student {lti_id:$lti})
                RETURN s.id AS sid, s.name AS name
            """,
                lti=sub,
            ).single()

        if not row:
            return f"❌ Không tìm thấy Student nào có lti_id = {sub} trong Neo4j."

        student_id = row["sid"]      
        student_name = row["name"]
        
        student_numeric_id = int(student_id.replace("user_", ""))  
        course_users = get_course_students(course_id)
        user_info = {
            "id": student_id,
            "name": student_name,
            "role": user_role
        }

    

        if not course_id:
            return "<p>❌ Chưa chọn khóa học.</p>"
        
        if is_student:
            if student_numeric_id not in course_users:
                return "<p>❌ Bạn không thuộc khóa học này.</p>"

            skill_data = compute_skill_percentages(student_id)

            skill_data_json = json.dumps(skill_data)

            template_path = Path(__file__).resolve().parents[1] / "lti" / "lti_recommend.html"
            with open(template_path, "r", encoding="utf-8") as f:
                html_template = f.read()

            completed, in_progress, not_started = get_student_modules(student_id)
            html = (
                html_template
                .replace("{{STUDENT_NAME}}", student_name)
                .replace("{{STUDENT_ID}}", student_id)
                .replace("{{STUDENT_LTI_ID}}", sub)
                .replace("{{COURSE_ID}}", str(course_id))
                .replace("{{SKILL_DATA_JSON}}", skill_data_json) 
            )
            return html

        else:
            from lti.lti_admin_service import get_admin_dashboard_data
            dashboard_data = get_admin_dashboard_data(course_id)
            template_path = Path(__file__).resolve().parent / "lti_admin.html"
            with open(template_path, "r", encoding="utf-8") as f:
                html_template = f.read()
            html = (
                html_template
                .replace("{{USER_ROLE}}", user_role)
                .replace("{{DASHBOARD_DATA}}", json.dumps(dashboard_data))
            )

            return html

    except Exception as e:
        try:
            log_system_error(
                message=str(e),
                context="lti_launch"
            )

            log_lti_event(
                event_type="ERROR",
                message="LTI launch failed",
                detail=str(e)
            )
        except Exception as log_err:
            print("[LogService Error in except lti_launch]", log_err)
        return f"Lỗi verify id_token hoặc xử lý KG: {str(e)}"

@app.route("/lti/student_view")
def student_view():
    student_id = request.args.get("student_id")
    course_id = request.args.get("course_id", "4")
    print(f"LTI Student View for {student_id} in course {course_id} ---")

    if not student_id:
        return "<p>Missing student_id.</p>"

    with neo4j_session() as session:
        row = session.run("""
            MATCH (s:Student {id:$sid})
            RETURN s.name AS name, s.lti_id AS lti_id
        """, sid=student_id).single()

    if not row:
        return f"<p>Student {student_id} not found.</p>"

    student_name = row["name"]
    lti_id = row["lti_id"]

    skill_data = compute_skill_percentages(student_id)
    skill_data_json = json.dumps(skill_data)

    with neo4j_session() as session:
        rows = list(session.run("""
            MATCH (:Student {id:$sid})-[r:mastery_on]->(m:Module)
            RETURN m.id AS mid, r.mastery AS mastery
            ORDER BY m.id
        """, sid=student_id))

    completed_modules = []
    for r in rows:
        mid = r["mid"]  
        mastery_percent = round((r["mastery"] or 0) * 100, 2)

        module_name = get_module_name(mid, course_id)

        completed_modules.append({
            "name": module_name or mid,  
            "mastery": mastery_percent
        })

    completed_json = json.dumps(completed_modules, ensure_ascii=False)

    template_path = Path(__file__).resolve().parents[1] / "lti" / "lti_view_users.html"
    html = template_path.read_text(encoding="utf-8")

    html = (
        html.replace("{{STUDENT_NAME}}", student_name)
            .replace("{{STUDENT_ID}}", student_id)
            .replace("{{STUDENT_LTI_ID}}", lti_id)
            .replace("{{COURSE_ID}}", str(course_id))
            .replace("{{SKILL_DATA_JSON}}", skill_data_json)
            .replace("{{COMPLETED_MODULES_JSON}}", completed_json)
    )

    return html

def run_pipeline_background():
    from src.app.run_full_pipeline import run_single_step
    global last_sync_time, current_progress, progress_percent

    progress_percent = 0
    current_progress = "Running"

    try:
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

        actual_steps = [s for s in steps]
        total_steps = len(actual_steps)

        increment = 100 / total_steps 

        for step_index, label in enumerate(actual_steps, start=1):
            current_progress = f"{label}..."

            progress_percent = min(99, int((step_index * increment)))

            print(f"{label} ({progress_percent}%)")

            run_single_step(label)

        progress_percent = 100
        current_progress = "Completed"

    except Exception as e:
        current_progress = f"Error: {str(e)}"
        progress_percent = -1  

    last_sync_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

@app.route("/api/sync", methods=["POST"])
def api_sync():
    global current_progress

    if current_progress not in ["", "Completed"]:
        return jsonify({
            "status": "running",
            "message": "Pipeline is already running..."
        })

    current_progress = "Running..."

    t = threading.Thread(target=run_pipeline_background)
    t.start()

    return jsonify({
        "status": "started",
        "message": "Pipeline started successfully!"
    })

@app.route("/api/sync/status")
def api_sync_status():
    return jsonify({
        "last_sync": last_sync_time,
        "progress": current_progress,
        "percent": progress_percent
    })

def auto_sync_loop():
    global current_progress
    while True:
        time.sleep(7200)  

        if current_progress in ["", "Completed"]:
            print("Auto-sync triggered...")
            t = threading.Thread(target=run_pipeline_background, daemon=True)
            t.start()
        else:
            print("Skip auto-sync because pipeline is running...")

threading.Thread(target=auto_sync_loop, daemon=True).start()

@app.route("/")
def index():
    return "SmartSchool Python LTI Tool is running."


def run_server():
    app.run(host="0.0.0.0", port=5000, debug=True)


if __name__ == "__main__":
    run_server()
