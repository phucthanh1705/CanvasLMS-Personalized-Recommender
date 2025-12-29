from pydantic import BaseModel
from dotenv import load_dotenv
from pathlib import Path
import os
PROJECT_ROOT = Path(__file__).resolve().parents[3]
ENV_PATH = PROJECT_ROOT / "config" / "secrets.env"
load_dotenv(dotenv_path=ENV_PATH)

class Settings(BaseModel):
    app_host: str = os.getenv("APP_HOST", "0.0.0.0")
    app_port: int = int(os.getenv("APP_PORT", "8000"))

    # LLM
    openai_api_key: str = os.getenv("OPENAI_API_KEY", "")
    openai_model: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    openai_org: str | None = os.getenv("OPENAI_ORG") or None
    openai_project: str | None = os.getenv("OPENAI_PROJECT") or None

    # Retrieval
    top_k: int = int(os.getenv("TOP_K", "4"))
    bm25_min_score: float = float(os.getenv("BM25_MIN_SCORE", "2.0"))

    # Redis
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    # Neo4j
    use_neo4j: bool = os.getenv("USE_NEO4J", "false").lower() == "true"
    neo4j_uri: str = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user: str = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password: str = os.getenv("NEO4J_PASS", "neo4j_password")
    neo4j_database: str = os.getenv("NEO4J_DB", "neo4j")

settings = Settings()
