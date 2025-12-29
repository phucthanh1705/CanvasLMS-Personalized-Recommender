from pydantic import BaseModel, Field

class ChatRequest(BaseModel):
    student_id: str = Field(..., description="Canvas/Neo4j student id")
    course_id: str = Field(..., description="Course id/code")
    module_id: str | None = None
    page_id: str | None = Field(None, description="Optional page id if known")
    message: str = Field(..., description="Student message")
    conversation_id: str | None = Field(None, description="Optional conversation id")
