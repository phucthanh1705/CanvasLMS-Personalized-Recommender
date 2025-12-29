from flask import Blueprint, request, jsonify
from chatbot.api.chat import handle_chat
import random
import string
import re

chat_routes_bp = Blueprint("chat_routes_bp", __name__)

import re

def strip_source_tags(text: str) -> str:
    """
    Remove [SOURCE x] tags from answer for UI display
    """
    if not text:
        return text
    return re.sub(r"\s*\[SOURCE\s*\d+\]", "", text).strip()

@chat_routes_bp.route("/lti/chat/message", methods=["POST"])
def chat_message():
    data = request.json
    result = handle_chat(data)
    print("Chat response:", result)
    if isinstance(result, dict) and "answer" in result:
        result["answer"] = strip_source_tags(result["answer"])
    return jsonify(result)
