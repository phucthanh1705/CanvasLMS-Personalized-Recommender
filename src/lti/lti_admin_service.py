import requests
import datetime
import json
import os
from llm.recommend_llm_vs_logic import neo4j_session

CANVAS_BASE_URL = "http://localhost:3000"
COURSE_ID = 4
ACCESS_TOKEN = "ZZvPyEyhuwcMkGxKXTv6KU3eCyeMaAXKTPcFzuNUTU6322JTumrxf3MBVYDLCZF3c"


headers = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

def canvas_get(path, params=None):
    try:
        url = f"{CANVAS_BASE_URL}{path}"
        r = requests.get(url, headers=headers, params=params)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print("[Canvas API Error]", e)
        return None

def get_total_requests_24h():
    log_path = "data/log/lti_launch_log.json"

    if not os.path.exists(log_path):
        return 0

    now = datetime.datetime.now()
    count = 0

    with open(log_path, "r", encoding="utf-8") as f:
        logs = json.load(f)

    for entry in logs:
        try:
            t = datetime.datetime.fromisoformat(entry["time"])
            if (now - t).total_seconds() <= 24 * 3600:
                count += 1
        except:
            continue

    return count

def get_lti_launches_today():
    log_path = "data/log/lti_launch_log.json"  

    if not os.path.exists(log_path):
        return 0

    today = datetime.date.today()
    count = 0

    with open(log_path, "r", encoding="utf-8") as f:
        logs = json.load(f)

        for entry in logs:
            if(entry.get("resource") == "LTI Launch"):
                t = datetime.datetime.fromisoformat(entry["time"]).date()
                if t == today:
                    count += 1

    return count

def get_last_canvas_sync():
    sync_path = "data/log/last_canvas_sync.json" 

    if not os.path.exists(sync_path):
        return "Never"

    with open(sync_path, "r") as f:
        data = json.load(f)
        return data.get("last_sync", "Unknown")

def get_system_errors_24h():
    error_path = "data/log/system_errors.json" 

    if not os.path.exists(error_path):
        return 0

    today = datetime.date.today()
    count = 0

    with open(error_path, "r") as f:
        logs = json.load(f)

        for e in logs:
            t = datetime.datetime.fromisoformat(e["time"]).date()
            if t == today:
                count += 1

    return count

def get_registered_platforms():
    path = "data/log/lti_platforms.json"  

    if not os.path.exists(path):
        return []

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def get_recent_lti_launches(limit=10):
    log_path = "data/log/lti_launch_log.json"  
    if not os.path.exists(log_path):
        return []

    with open(log_path, "r", encoding="utf-8") as f:
        logs = json.load(f)

    logs_sorted = sorted(logs, key=lambda x: x["time"], reverse=True)
    return logs_sorted[:limit]

def get_lti_events():
    event_path = "data/log/lti_events.json"  

    if not os.path.exists(event_path):
        return []

    with open(event_path, "r", encoding="utf-8") as f:
        return json.load(f)

def get_admin_dashboard_data(course_id):
    return {
        "total_requests_24h": get_total_requests_24h(),
        "lti_launches_today": get_lti_launches_today(),
        "last_canvas_sync": get_last_canvas_sync(),
        "system_errors_24h": get_system_errors_24h(),
        "platforms": get_registered_platforms(),
        "recent_launches": get_recent_lti_launches(),
        "lti_events": get_lti_events(),
        "launches_by_hour": get_launches_by_hour(),
        "students": get_all_students(),
        "course_id": course_id
    }

def get_launches_by_hour():
    log_path = "data/log/lti_launch_log.json"
    
    hourly = {h: 0 for h in range(24)}

    if not os.path.exists(log_path):
        return hourly

    with open(log_path, "r", encoding="utf-8") as f:
        logs = json.load(f)

    today = datetime.date.today()

    for entry in logs:
        t = datetime.datetime.fromisoformat(entry["time"])
        if t.date() == today:
            hourly[t.hour] += 1

    return hourly

def get_all_students():
    students = []

    with neo4j_session() as session:
        rows = session.run("""
            MATCH (s:Student)
            OPTIONAL MATCH (s)-[:mastery_on]->(m:Module)
            RETURN s.id AS sid, s.name AS name, count(m) AS completed
            ORDER BY sid
        """)

        for r in rows:
            sid = r["sid"]
            completed = r["completed"]

            students.append({
                "name": r["name"],
                "student_id": sid,
                "status": "Active" if completed > 0 else "Inactive",
            })

    return students


if __name__ == "__main__":
    print(json.dumps(get_admin_dashboard_data(), indent=2, ensure_ascii=False))
