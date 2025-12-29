from pydantic import BaseModel

class SourceItem(BaseModel):
    source: str
    snippet: str

class ChatResponse(BaseModel):
    in_scope: bool
    answer: str
    sources: list[SourceItem] = []
    followups: list[str] = []
    refusal_reason: str | None = None
