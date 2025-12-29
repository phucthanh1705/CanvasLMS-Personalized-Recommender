from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.chat import router as chat_router

app = FastAPI(title="SmartSchool TA Bot", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # set your domains later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(chat_router)

@app.get("/health")
def health():
    return {"status": "ok"}
