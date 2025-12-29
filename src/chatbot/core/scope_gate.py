from __future__ import annotations
from dataclasses import dataclass
from chatbot.config.constants import BLACKLIST_PHRASES
from chatbot.data.normalizer import normalize_text

@dataclass
class ScopeDecision:
    in_scope: bool
    reason: str | None = None

def quick_blacklist_check(message: str) -> ScopeDecision | None:
    m = normalize_text(message)
    for p in BLACKLIST_PHRASES:
        if p in m:
            return ScopeDecision(in_scope=False, reason="Câu hỏi không thuộc phạm vi hỗ trợ học tập của môn học.")
    return None
