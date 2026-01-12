from chatbot.data.normalizer import normalize_text

def classify_intent_by_problem(problem: str) -> str:
    """
    Map semantic problem → execution intent
    """

    PROBLEM_TO_INTENT = {
        # ===== COMPETENCY =====
        "competency_achieved_module": "competency_achieved_module",  
        "competency_status_module": "competency_status_module", 
        "competency_missing": "competency_missing",
        "competency_status": "competency_status",
        "recommend_next": "recommend_next",

        # ===== LESSON =====
        "lesson_compare": "compare",
        "lesson_example": "example",
        "lesson_quiz": "quiz_help",
        "lesson_debug": "debug",
        "lesson_summarize": "summarize",
    }

    return PROBLEM_TO_INTENT.get(problem, "explain")