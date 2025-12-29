from __future__ import annotations
from chatbot.schemas.chat_response import ChatResponse, SourceItem

def refusal_response(reason: str) -> ChatResponse:
    return ChatResponse(
        in_scope=False,
        answer=(
            "Mình chỉ hỗ trợ các câu hỏi liên quan đến **giáo trình/môn học hiện tại**.\n"
            f"Lý do: {reason}\n"
            "Bạn hãy hỏi lại theo nội dung bài học (ví dụ: khái niệm, ví dụ, bài tập trong module)."
        ),
        sources=[],
        followups=[],
        refusal_reason=reason,
    )

def success_response(answer: str, sources: list[SourceItem], followups: list[str]) -> ChatResponse:
    return ChatResponse(in_scope=True, answer=answer, sources=sources, followups=followups)
