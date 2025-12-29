from __future__ import annotations
from typing import List, Dict, Any
from chatbot.config.settings import settings

def pass_threshold(items: List[Dict[str, Any]]) -> bool:
    if not items:
        return False
    best = float(items[0].get("score", 0.0))
    return best >= settings.bm25_min_score
