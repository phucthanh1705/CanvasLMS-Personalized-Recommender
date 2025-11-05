# Hàm đọc/ghi file, log, json
import json, re
from bs4 import BeautifulSoup
from pathlib import Path

def load_json(path):
    path = Path(path)
    if not path.exists():
        return {}
    with open(path, 'r', encoding='utf-8') as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return {}

def save_json(data, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def clean_html(html_text):
    soup = BeautifulSoup(html_text, "html.parser")
    return re.sub(r'\s+', ' ', soup.get_text()).strip()
