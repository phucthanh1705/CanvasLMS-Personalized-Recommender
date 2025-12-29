from __future__ import annotations
from dataclasses import dataclass

@dataclass
class Context:
    student_id: str
    course_id: str
    module_id: str
    page_id: str | None = None

def build_context(student_id: str, course_id: str, module_id: str, page_id: str | None) -> Context:
    # In LTI integration, you'd decode the launch JWT to get these ids.
    return Context(student_id=student_id, course_id=course_id, module_id=module_id, page_id=page_id)
