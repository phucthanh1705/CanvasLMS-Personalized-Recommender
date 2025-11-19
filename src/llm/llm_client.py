import ollama

DEFAULT_MODEL = "qwen2.5:3b-instruct"   # model bạn đang có

def call_llm(system_prompt: str, user_prompt: str, model=None):
    """
    Gọi Ollama local với hệ thống + người dùng.
    """

    # Nếu không truyền model → dùng DEFAULT_MODEL
    if model is None:
        model = DEFAULT_MODEL

    try:
        resp = ollama.chat(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
        )

        return resp["message"]["content"]

    except Exception as e:
        print("❌ Lỗi Ollama:", e)
        return ""
