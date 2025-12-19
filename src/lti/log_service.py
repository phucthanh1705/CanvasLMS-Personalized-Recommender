import json
import os
from datetime import datetime

LOG_DIR = "data/log"
MAX_LOG_ITEMS = 100

os.makedirs(LOG_DIR, exist_ok=True)

def load_json(filename, default):
    path = f"{LOG_DIR}/{filename}"
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return default

def save_json(filename, data):
    path = f"{LOG_DIR}/{filename}"

    if isinstance(data, list):
        data = data[-MAX_LOG_ITEMS:]

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def log_lti_launch(platform, user, resource, status="Success"):
    logs = load_json("lti_launch_log.json", default=[])
    
    logs.append({
        "platform": platform,
        "user": user,
        "resource": resource,
        "time": datetime.now().isoformat(),
        "status": status
    })

    save_json("lti_launch_log.json", logs)

def log_system_error(message, context=""):
    logs = load_json("system_errors.json", default=[])

    logs.append({
        "time": datetime.now().isoformat(),
        "message": message,
        "context": context
    })

    save_json("system_errors.json", logs)

def log_lti_event(event_type, message, detail=""):
    events = load_json("lti_events.json", default=[])

    events.append({
        "type": event_type,
        "message": message,
        "detail": detail,
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

    save_json("lti_events.json", events)

def log_platform_registration(name, client_id, deployment_id, status="Active"):
    platforms = load_json("lti_platforms.json", default=[])

    platforms.append({
        "name": name,
        "client_id": client_id,
        "deployment_id": deployment_id,
        "status": status,
        "last_launch": "Never"
    })

    save_json("lti_platforms.json", platforms)

def update_platform_last_launch(name):
    platforms = load_json("lti_platforms.json", default=[])

    found = False
    for p in platforms:
        if p["name"] == name:
            p["last_launch"] = datetime.now().strftime("%Y-%m-%d %H:%M")
            found = True
            break

    if not found:
        platforms.append({
            "name": name,
            "client_id": None,
            "deployment_id": None,
            "status": "Active",
            "last_launch": datetime.now().strftime("%Y-%m-%d %H:%M")
        })

    save_json("lti_platforms.json", platforms)

def log_canvas_sync():
    syncs = load_json("last_canvas_sync.json", default=[])

    syncs.append({
        "last_sync": datetime.now().strftime("%Y-%m-%d %H:%M")
    })

    save_json("last_canvas_sync.json", syncs)
