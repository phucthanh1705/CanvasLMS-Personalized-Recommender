from __future__ import annotations
import json
from pathlib import Path
from typing import List, Dict, Any
from rank_bm25 import BM25Okapi
from chatbot.data.normalizer import tokenize_vi_simple

class BM25Corpus:
    def __init__(self, corpus_path: str = "data/processed/corpus.jsonl"):
        self.corpus_path = Path(corpus_path)
        self.docs: List[Dict[str, Any]] = []
        self.tokens: List[List[str]] = []
        self.bm25: BM25Okapi | None = None

    def load(self):
        self.docs.clear()
        self.tokens.clear()
        if not self.corpus_path.exists():
            raise FileNotFoundError(f"Corpus not found: {self.corpus_path}")
        with self.corpus_path.open("r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                doc = json.loads(line)
                self.docs.append(doc)
                self.tokens.append(tokenize_vi_simple(doc["content"]))
        self.bm25 = BM25Okapi(self.tokens)

    def is_ready(self) -> bool:
        return self.bm25 is not None and len(self.docs) > 0

CORPUS = BM25Corpus()

def ensure_loaded():
    if not CORPUS.is_ready():
        CORPUS.load()

def retrieve(course_id: str, module_id: str, query: str, top_k: int) -> List[Dict[str, Any]]:
    ensure_loaded()
    q_tokens = tokenize_vi_simple(query)
    scores = CORPUS.bm25.get_scores(q_tokens)

    # Filter by course/module
    candidates = []
    for idx, score in enumerate(scores):
        d = CORPUS.docs[idx]
        if str(d.get("course_id")) != str(course_id):
            continue
        if str(d.get("module_id")) != str(module_id):
            continue
        candidates.append((float(score), d))

    candidates.sort(key=lambda x: x[0], reverse=True)
    out = []
    for score, d in candidates[:top_k]:
        out.append({
            "score": score,
            "source": d.get("source", ""),
            "content": d.get("content", ""),
            "page_id": d.get("page_id"),
            "section": d.get("section"),
        })
    return out
