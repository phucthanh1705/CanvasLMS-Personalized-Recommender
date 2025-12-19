import requests
import json
CANVAS_BASE_URL = "http://localhost:3000"
COURSE_ID = 4
ACCESS_TOKEN = "2ZvPyEyhuwcMkGxKXTv6KU3eCyeMaAXKTPcFzuNUTU632JTurmxf3MBVYDLCZF3c"

CURRENT_USER_IDS = []
OVERRIDE_ID = None


headers = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

def get_assigned_users(module_id):
    global CURRENT_USER_IDS, OVERRIDE_ID

    url = f"{CANVAS_BASE_URL}/api/v1/courses/{COURSE_ID}/modules/{module_id}/assignment_overrides?per_page=100"

    resp = requests.get(url, headers=headers)

    if resp.status_code != 200:
        return

    data = resp.json()

    if len(data) == 0:
        CURRENT_USER_IDS = []
        OVERRIDE_ID = None
        return

    override = data[0]

    OVERRIDE_ID = str(override["id"])
    CURRENT_USER_IDS = [str(s["id"]) for s in override.get("students", [])]

def assign_user(new_user_id, module_id):
    global CURRENT_USER_IDS, OVERRIDE_ID

    if OVERRIDE_ID is None:
        return

    new_user_id = str(new_user_id)

    if new_user_id not in CURRENT_USER_IDS:
        CURRENT_USER_IDS.append(new_user_id)
    payload = {
        "overrides": [
            {
                "student_ids": CURRENT_USER_IDS,
                "id": OVERRIDE_ID
            }
        ]
    }

    url = f"{CANVAS_BASE_URL}/api/v1/courses/{COURSE_ID}/modules/{module_id}/assignment_overrides"

    resp = requests.put(url, headers=headers, data=json.dumps(payload))
    try:
        print("Response:", resp.json())
    except:
        print("Raw:", resp.text)

def send_canvas_message(recipient_ids, subject, body, course_id):
    global headers 

    url = f"{CANVAS_BASE_URL}/api/v1/conversations"
    
    payload = {
        "recipients": recipient_ids,
        "subject": subject,
        "body": body,
        "group_conversation": "false",
        "context_code": f"course_{course_id}"
    }

    post_headers = headers.copy()
    if "Content-Type" in post_headers:
        del post_headers["Content-Type"]

    resp = requests.post(url, headers=post_headers, data=payload) 

    if resp.status_code == 201 or resp.status_code == 200:
        return True
    else:
        return False
