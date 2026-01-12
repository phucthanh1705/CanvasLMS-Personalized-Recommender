import json
from pathlib import Path
from chatbot.llm.llm_client import get_llm_client
from chatbot.data.normalizer import normalize_text

PROBLEMS = json.loads(
    Path("src/chatbot/core/supported_problems.json").read_text(encoding="utf-8")
)

LESSON_KEYWORDS = [
    "la gi", "khai niem", "giai thich",
    "the ", "html", "css", "java",
    "spring", "mvc", "hibernate",
    "vi du", "so sanh", "khac nhau",
    "bai tap", "quiz", "loi", "error"
]

def detect_problem(message: str) -> dict:
    m = normalize_text(message.lower())

    if any(k in m for k in LESSON_KEYWORDS):
        return {
            "problem": "lesson_explain",
            "confidence": 0.95
        }

    prompt = f"""
Bạn KHÔNG trả lời câu hỏi.
Bạn CHỈ phân loại câu hỏi theo VẤN ĐỀ.

QUY TẮC BẮT BUỘC:
- Nếu câu hỏi hỏi về KHÁI NIỆM / ĐỊNH NGHĨA / KIẾN THỨC → lesson_explain
- CHỈ chọn vấn đề năng lực nếu câu hỏi có các từ:
  "năng lực", "kỹ năng", "đạt được", "cần có", "thiếu", "lộ trình"

Danh sách vấn đề hệ thống hỗ trợ:
{json.dumps(PROBLEMS, ensure_ascii=False, indent=2)}

Luật:
- Chỉ chọn 1 key trong danh sách
- Nếu không phù hợp, trả về "unsupported"

Chỉ trả JSON:
{{
  "problem": "...",
  "confidence": 0.0
}}

Câu hỏi:
\"{message}\"
"""

    llm = get_llm_client()
    raw = llm.generate(
        system_prompt="You are a strict problem classifier. Do not explain.",
        user_prompt=prompt
    )

    try:
        data = json.loads(raw)
        if "problem" in data and "confidence" in data:
            return data
    except Exception:
        pass

    return {"problem": "unsupported", "confidence": 0.0}
