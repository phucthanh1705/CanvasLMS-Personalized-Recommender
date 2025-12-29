from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any

@dataclass
class Snippet:
    source: str
    text: str

@dataclass
class KnowledgePack:
    course_id: str
    module_id: str
    page_id: str | None
    intent: str
    snippets: List[Snippet]

    def to_prompt_block(self, max_chars_per_snippet: int = 1600) -> str:
        parts = []
        for i, sn in enumerate(self.snippets, 1):
            txt = sn.text.strip()
            if len(txt) > max_chars_per_snippet:
                txt = txt[:max_chars_per_snippet] + "..."
            parts.append(f"[SOURCE {i}] {sn.source}\n{txt}")
        return "\n\n".join(parts)
