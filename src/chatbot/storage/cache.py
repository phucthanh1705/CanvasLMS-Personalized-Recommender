from __future__ import annotations
from chatbot.config.settings import settings

try:
    import redis
except Exception:
    redis = None

class Cache:
    def __init__(self):
        self._client = None
        if redis is not None:
            try:
                self._client = redis.from_url(settings.redis_url, decode_responses=True)
            except Exception:
                self._client = None

    def get(self, key: str) -> str | None:
        if not self._client:
            return None
        try:
            return self._client.get(key)
        except Exception:
            return None

    def set(self, key: str, value: str, ttl_seconds: int = 3600) -> None:
        if not self._client:
            return
        try:
            self._client.setex(key, ttl_seconds, value)
        except Exception:
            return

cache = Cache()
