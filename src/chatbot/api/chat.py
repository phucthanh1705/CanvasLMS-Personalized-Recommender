from __future__ import annotations
from fastapi import APIRouter, HTTPException
from chatbot.schemas.chat_request import ChatRequest
from chatbot.schemas.chat_response import SourceItem
from chatbot.config.settings import settings
from chatbot.core.context_builder import build_context
from chatbot.core.scope_gate import quick_blacklist_check
from chatbot.retrieval.bm25 import retrieve as bm25_retrieve
from chatbot.retrieval.ranker import pass_threshold
from chatbot.core.knowledge_pack import KnowledgePack, Snippet
from chatbot.llm.llm_client import get_llm_client, load_prompt
from chatbot.core.grounding_check import enforce_grounding
from chatbot.core.response_formatter import refusal_response, success_response
from chatbot.storage.logs import log_event
from chatbot.storage.neo4j_repo import Neo4jRepo
from chatbot.core.id_resolver import IDResolver
from chatbot.core.problem_detector import detect_problem
from chatbot.core.intent_classifier import classify_intent_by_problem
from chatbot.storage import neo4j_repo

router = APIRouter()


def _build_followups(module_id: str) -> list[str]:
    return [
        "Bạn có thể cho mình ví dụ cụ thể bạn đang làm trong bài không?",
        "Bạn muốn mình giải thích theo cách dễ hiểu hay kèm ví dụ code?",
        f"Bạn đang học phần nào trong Module {module_id}?"
    ]
def handle_chat(data: dict) -> dict:
    student_id = data.get("student_id")
    print("student_id =", student_id)
    message = data.get("message", "")
    course_id = data.get("course_id")
    module_id = data.get("module_id")
    page_id = data.get("page_id")    
    resolver = IDResolver()

    if not resolver.resolve_student(student_id):
        resolver.close()
        return {
            "status": "success",
            "answer": "Student không tồn tại trong hệ thống.",
            "sources": [],
            "followups": []
        }

    course_id = resolver.resolve_course_id(course_id)
    if not course_id:
        resolver.close()
        return {
            "status": "success",
            "answer": "Course không tồn tại hoặc không hợp lệ.",
            "sources": [],
            "followups": []
        }

    module_id = None
    if module_id:
        module_id = resolver.resolve_module_id(
            course_id,
            resolver._normalize_module_id(module_id)
        )

    if not module_id:
      module_id = resolver.resolve_module_from_question_nlp(course_id, message)


    if not module_id:
        resolver.close()
        return {
            "status": "success",
            "answer": "Không xác định được module liên quan đến câu hỏi.",
            "sources": [],
            "followups": []
        }
        


    resolver.close()
    ctx = build_context(
        student_id=student_id,
        course_id=course_id,
        module_id=module_id,
        page_id=page_id
    )

    result = detect_problem(message)
    problem = result["problem"]
    confidence = result.get("confidence", 0.0)
    if problem == "unsupported" or confidence < 0.7:
        
        return {
            "status": "success",
            "answer": "Câu hỏi này chưa nằm trong phạm vi hỗ trợ.",
            "sources": [],
            "followups": []
        }
    intent = classify_intent_by_problem(problem)


    if settings.use_neo4j and intent in [
        "competency_status",
        "competency_status_module",
        "competency_missing",
        "recommend_next"
    ]:
        repo = Neo4jRepo()
        try:
            if intent == "competency_status_module":
                comps = repo.get_competencies_of_module(ctx.module_id)

                if not comps:
                    return {
                        "status": "success",
                        "answer": "Module này chưa được cấu hình năng lực đầu ra.",
                        "sources": [],
                        "followups": []
                    }

                answer = "Sau khi học xong module này, bạn sẽ đạt được các năng lực:\n"
                for c in comps:
                    answer += f"- {c}\n"
                return {
                    "status": "success",
                    "answer": answer,
                    "sources": [],
                    "followups": [
                        "Bạn muốn xem năng lực hiện tại của mình không?",
                        "Bạn muốn biết còn thiếu năng lực gì để đạt môn này không?"
                    ]
                }

            if intent == "competency_status":
                competencies = repo.get_student_competencies(student_id)

                if not competencies:
                    return {
                        "status": "success",
                        "answer": "Hiện tại bạn chưa đạt năng lực nào theo hệ thống đánh giá.",
                        "sources": [],
                        "followups": ["Bạn muốn mình gợi ý lộ trình học không?"]
                    }

                answer = "Dựa trên kết quả học tập, hiện tại bạn có các năng lực sau:\n"
                for c in competencies:
                    answer += f"- {c['name']} (mức độ: {round(c['mastery'], 2)})\n"
                return {
                    "status": "success",
                    "answer": answer,
                    "sources": [],
                    "followups": ["Bạn muốn xem năng lực theo từng môn không?"]
                }

            if intent == "competency_missing":
                required = set(repo.get_competencies_of_module(ctx.module_id))
                achieved = {
                    c["name"]
                    for c in repo.get_achieved_competencies_of_module(
                        student_id,
                        ctx.module_id
                    )
                }

                missing = sorted(required - achieved)
                if not missing:
                    answer = "Bạn đã có đầy đủ năng lực cần thiết cho module này."
                else:
                    answer = "Để học tốt module này, bạn cần bổ sung các năng lực:\n"
                    for m in missing:
                        answer += f"- {m}\n"

                return {
                    "status": "success",
                    "answer": answer,
                    "sources": [],
                    "followups": []
                }

            if intent == "recommend_next":
                recs = repo.recommend_modules(student_id)

                if not recs:
                    return {
                        "status": "success",
                        "answer": "Hiện tại hệ thống chưa đủ dữ liệu để gợi ý module tiếp theo.",
                        "sources": [],
                        "followups": []
                    }

                answer = "Dựa trên năng lực hiện tại, bạn có thể học tiếp các module sau:\n"
                for r in recs:
                    answer += f"- Module {r['module_id']} (phù hợp {r['score']} năng lực)\n"

                return {
                    "status": "success",
                    "answer": answer,
                    "sources": [],
                    "followups": []
                }

        finally:
            repo.close()

    bl = quick_blacklist_check(message)
    if bl is not None and not bl.in_scope:
        log_event({"type": "refusal_blacklist", "ctx": ctx.__dict__, "message": message})
        return {
            "status": "refusal",
            "answer": bl.reason or "Ngoài phạm vi.",
            "sources": [],
            "followups": []
        }
    

    if settings.use_neo4j:
        repo = Neo4jRepo()
        raw_items = repo.get_module_snippets(
            course_id=ctx.course_id,
            module_id=ctx.module_id
        )
        if raw_items:
            print("first_source =", raw_items[0].get("source"))
            print("first_len =", len(raw_items[0].get("content") or ""))

        repo.close()

        items = [{
            "score": 1.0,
            "source": r["source"],
            "content": r["content"],
            "page_id": r.get("page_id"),
            "section": r.get("section"),
        } for r in raw_items]
    else:
        items = bm25_retrieve(
            course_id=ctx.course_id,
            module_id=ctx.module_id,
            query=message,
            top_k=settings.top_k
        )

    if not pass_threshold(items):
        return {
            "status": "refusal",
            "answer": "Câu hỏi này không khớp với nội dung giáo trình/module hiện tại.",
            "sources": [],
            "followups": []
        }

    snippets = [Snippet(source=i["source"], text=i["content"]) for i in items]
    kp = KnowledgePack(
        course_id=ctx.course_id,
        module_id=ctx.module_id,
        page_id=ctx.page_id,
        intent=intent,
        snippets=snippets
    )

    system = load_prompt("system.txt")
    template = load_prompt("quiz_help.txt") if intent == "quiz_help" else load_prompt("explain.txt")
    user_prompt = template.format(knowledge_pack=kp.to_prompt_block(), question=message)
    llm = get_llm_client()
    raw_answer = llm.generate(system_prompt=system, user_prompt=user_prompt)

    grounded = enforce_grounding(raw_answer, allowed_sources=[s.source for s in snippets])
    if grounded is None:
        return {
            "status": "refusal",
            "answer": "Bot không đủ thông tin trong giáo trình để trả lời chắc chắn.",
            "sources": [],
            "followups": []
        }

    sources = [
        SourceItem(
            source=s.source,
            snippet=s.text[:220] + ("..." if len(s.text) > 220 else "")
        )
        for s in snippets
    ]

    return {
        "status": "success",
        "answer": grounded,
        "sources": [
            {
                "source": s.source,
                "snippet": s.snippet
            }
            for s in sources
        ],
        "followups": _build_followups(ctx.module_id)
    }