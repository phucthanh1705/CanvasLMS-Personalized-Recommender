from __future__ import annotations
from sentence_transformers import SentenceTransformer, util
import torch
import re
import unicodedata

_model = SentenceTransformer("all-MiniLM-L6-v2")


def _normalize(text: str) -> str:
    """Lower + remove accents + collapse spaces"""
    text = text.lower().strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9\s]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def _strip_html_fast(html: str) -> str:
    # nhẹ, tránh thêm bs4 ở runtime
    html = re.sub(r"<script.*?>.*?</script>", " ", html, flags=re.S | re.I)
    html = re.sub(r"<style.*?>.*?</style>", " ", html, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"&nbsp;|&amp;|&lt;|&gt;|&quot;|&#39;", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def split_into_blocks(content: str, max_chars: int = 350) -> list[str]:
    """
    Tách nội dung lesson thành các block nhỏ:
    - ưu tiên tách theo đoạn (p, br, xuống dòng)
    - mỗi block ~ 150-350 chars để embedding ổn định
    """
    if not isinstance(content, str) or not content.strip():
        return []

    raw = content.strip()

    # nếu có HTML nhiều, strip nhanh
    if "<" in raw and ">" in raw:
        raw = _strip_html_fast(raw)

    # tách theo câu/đoạn
    # (đơn giản, đủ tốt cho “khái niệm là gì”)
    parts = re.split(r"(?<=[\.\?\!])\s+|\n{2,}", raw)

    blocks: list[str] = []
    buf = ""
    for p in parts:
        p = p.strip()
        if len(p) < 40:  # bỏ đoạn quá ngắn/nhiễu
            continue

        if not buf:
            buf = p
        elif len(buf) + 1 + len(p) <= max_chars:
            buf = buf + " " + p
        else:
            blocks.append(buf)
            buf = p

    if buf:
        blocks.append(buf)

    # giới hạn số block để khỏi quá nặng
    return blocks[:60]


def resolve_module_by_explicit_subject(
    question: str,
    module_rows: list[dict],
    min_token_hit: int = 2
) -> str | None:
    """
    Rule/lexical match: nếu câu hỏi có nhắc thẳng tên môn (hoặc phần đặc trưng của tên môn)
    thì chọn luôn, không cần embedding.
    """
    if not module_rows:
        return None

    q = _normalize(question)
    print("Normalized question for explicit subject match:", q)
    if not q:
        return None

    best_mid = None
    best_score = 0

    for r in module_rows:
        mid = r.get("module_id")
        name = r.get("subject_name") or ""
        if not isinstance(name, str) or not name.strip():
            continue

        n = _normalize(name)

        # 1) substring match mạnh nhất
        if n and n in q:
            return mid

        # 2) token overlap (tránh các từ quá chung)
        tokens = [t for t in n.split() if len(t) >= 4]  # bỏ từ quá ngắn
        hit = sum(1 for t in tokens if t in q)

        if hit >= min_token_hit and hit > best_score:
            best_score = hit
            best_mid = mid
    return best_mid

def resolve_module_by_lesson_blocks_topk(
    question: str,
    lesson_rows: list[dict],
    threshold: float = 0.23,
    top_k_blocks_per_module: int = 6,
    max_chars_block: int = 350
) -> str | None:
    """
    Concept routing chuẩn hơn:
    - Tách mỗi lesson thành nhiều block nhỏ (đoạn giải thích)
    - Embed question ↔ block
    - Gom điểm theo module bằng mean(top-k blocks)
    """
    if not lesson_rows:
        return None

    block_texts: list[str] = []
    block_module_ids: list[str] = []

    for r in lesson_rows:
        content = r.get("content")
        mid = r.get("module_id")
        if not isinstance(content, str) or not content.strip() or not mid:
            continue

        blocks = split_into_blocks(content, max_chars=max_chars_block)
        for b in blocks:
            block_texts.append(b)
            block_module_ids.append(mid)

    if not block_texts:
        return None

    # encode blocks + question
    block_emb = _model.encode(
        block_texts,
        convert_to_tensor=True,
        normalize_embeddings=True
    )
    q_emb = _model.encode(
        question,
        convert_to_tensor=True,
        normalize_embeddings=True
    )

    scores = util.cos_sim(q_emb, block_emb)[0]

    # bucket scores by module
    bucket: dict[str, list[float]] = {}
    for idx, sc in enumerate(scores):
        mid = block_module_ids[idx]
        bucket.setdefault(mid, []).append(float(sc))

    if not bucket:
        return None

    module_scores: dict[str, float] = {}
    for mid, vals in bucket.items():
        vals_sorted = sorted(vals, reverse=True)
        top_vals = vals_sorted[:top_k_blocks_per_module]
        module_scores[mid] = sum(top_vals) / len(top_vals)

    best_module, best_score = max(module_scores.items(), key=lambda x: x[1])

    if best_score < threshold:
        return None

    return best_module

