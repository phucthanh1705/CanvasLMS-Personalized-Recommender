# Cấu hình chung: URL Canvas, Token, DB, Paths
import os
from dotenv import load_dotenv
env_path = os.path.join(os.path.dirname(__file__), "secrets.env")

load_dotenv(dotenv_path=env_path)  # Đọc file .env

BASE_URL = os.getenv("CANVAS_BASE_URL", "http://localhost:3000/api/v1")
TOKEN = os.getenv("CANVAS_TOKEN")  # Dán token vào file .env
DATA_DIR = "data/raw"

os.makedirs(DATA_DIR, exist_ok=True)
HEADERS = {"Authorization": f"Bearer {TOKEN}"}