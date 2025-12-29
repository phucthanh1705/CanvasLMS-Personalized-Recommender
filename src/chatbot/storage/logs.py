import json
from pathlib import Path
from datetime import datetime

LOG_DIR = Path("data/logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

def log_event(event: dict) -> None:
    ts = datetime.utcnow().strftime("%Y%m%d")
    fp = LOG_DIR / f"events_{ts}.jsonl"
    event = {**event, "ts_utc": datetime.utcnow().isoformat()}
    with fp.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")
