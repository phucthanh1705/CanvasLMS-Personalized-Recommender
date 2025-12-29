from flask import Blueprint, request
from pathlib import Path
from lti.core.lti_verify import verify_id_token
from chatbot.storage.neo4j_repo import Neo4jRepo

chat_bp = Blueprint("chat_bp", __name__)

CHAT_CLIENT_ID = "10000000000024"
CANVAS_JWKS = "http://localhost:3000/api/lti/security/jwks"

@chat_bp.route("/lti/chat/launch", methods=["POST"])
def chat_launch():
    id_token = request.form.get("id_token")

    decoded = verify_id_token(
        id_token=id_token,
        canvas_jwks_url=CANVAS_JWKS,
        client_id=CHAT_CLIENT_ID
    )

    user_id = decoded.get("sub", "unknown_user")
    repo = Neo4jRepo()
    try:
        student_id = repo.get_student_id_by_lti_id(user_id)
    finally:
        repo.close()
    html = (
        Path(__file__).resolve()
        .parents[2]
        / "templates"
        / "lti"
        / "chat.html"
    ).read_text(encoding="utf-8")

    return html.replace("{{USER_ID}}", student_id)
