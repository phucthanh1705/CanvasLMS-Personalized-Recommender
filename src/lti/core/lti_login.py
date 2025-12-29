from flask import Blueprint, request, redirect
import uuid

lti_login_bp = Blueprint("lti_login_bp", __name__)

PLATFORM_ISS = "http://localhost:3000"

TOOLS = {
    "chat": {
        "client_id": "10000000000024",
        "redirect_uri": "http://127.0.0.1.nip.io:5000/lti/chat/launch",
    },
    "recommender": {
        "client_id": "10000000000022",
        "redirect_uri": "http://127.0.0.1.nip.io:5000/lti/launch",
    },
}

@lti_login_bp.route("/lti/login", methods=["POST"])
def lti_login():
    login_hint = request.form.get("login_hint")
    lti_message_hint = request.form.get("lti_message_hint")

    if not login_hint:
        return "Missing login_hint", 400

    target = request.form.get("target_link_uri", "")

    if "/lti/chat/" in target:
        tool = TOOLS["chat"]
    else:
        tool = TOOLS["recommender"]

    state = str(uuid.uuid4())
    nonce = str(uuid.uuid4())

    redirect_url = (
        f"{PLATFORM_ISS}/api/lti/authorize_redirect"
        f"?response_type=id_token"
        f"&client_id={tool['client_id']}"
        f"&redirect_uri={tool['redirect_uri']}"
        f"&scope=openid"
        f"&response_mode=form_post"
        f"&prompt=none"
        f"&state={state}"
        f"&nonce={nonce}"
        f"&login_hint={login_hint}"
        f"&lti_message_hint={lti_message_hint}"
    )

    return redirect(redirect_url)
