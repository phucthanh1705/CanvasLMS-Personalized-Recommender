from __future__ import annotations
from typing import Protocol, List, Dict, Any

class Retriever(Protocol):
    def retrieve(self, course_id: str, module_id: str, query: str, top_k: int) -> List[Dict[str, Any]]:
        ...
