from __future__ import annotations
import re
from typing import List

def has_any_source_citation(answer: str) -> bool:
    # Require pattern like [SOURCE 1] or (SOURCE 2)
    return bool(re.search(r"\[SOURCE\s+\d+\]", answer))

def enforce_grounding(answer: str, allowed_sources: List[str]) -> str | None:
    # Very lightweight: check answer cites at least one source and doesn't include obvious "web/internet" claims.
    if not has_any_source_citation(answer):
        return None
    lowered = answer.lower()
    forbidden = ["theo internet", "trên mạng", "wikipedia", "google", "mình biết rằng"]
    if any(x in lowered for x in forbidden):
        return None
    return answer
