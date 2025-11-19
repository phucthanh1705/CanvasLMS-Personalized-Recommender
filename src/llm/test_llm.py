from src.llm.llm_client import call_llm

print("Gọi LLM test...")
resp = call_llm("Bạn là AI.", "Xin chào bạn!")
print("Kết quả:", repr(resp))
