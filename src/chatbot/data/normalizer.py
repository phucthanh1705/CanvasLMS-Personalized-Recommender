import re

def normalize_text(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def tokenize_vi_simple(s: str) -> list[str]:
    # Simple whitespace tokenization; good enough for BM25 with Vietnamese text.
    s = normalize_text(s)
    s = re.sub(r"[^\w\s<>/:-]", " ", s, flags=re.UNICODE)
    return [t for t in s.split() if t]
