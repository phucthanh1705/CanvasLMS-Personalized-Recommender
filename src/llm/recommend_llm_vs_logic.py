import os
import json
import argparse
import re
from pathlib import Path

import pandas as pd
from neo4j import GraphDatabase
from dotenv import load_dotenv
import requests


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
ENV_PATH = os.path.join(BASE_DIR, "config", "secrets.env")
load_dotenv(ENV_PATH)

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASS = os.getenv("NEO4J_PASS")
NEO4J_DB   = os.getenv("NEO4J_DB", "neo4j")

EXPORT_DIR = Path(os.path.join(BASE_DIR, "data", "exports"))
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

LOGIC_EXPORT = EXPORT_DIR / "recommendations.csv"
FINAL_EXPORT = EXPORT_DIR / "final_recommendations.csv"

OPENAI_KEY = os.getenv("GOOGLE_API_KEY")

def neo4j_session():
    return GraphDatabase.driver(
        NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS), encrypted=False
    ).session(database=NEO4J_DB)

def call_llm(json_payload, model="gpt-4o-mini"):
    if not OPENAI_KEY:
        raise Exception("OPENAI_API_KEY missing")

    url = "https://api.openai.com/v1/chat/completions"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENAI_KEY}"
    }

    messages = [
        {"role": "system", "content": "You only read JSON and output JSON. No natural language."},
        {"role": "user", "content": json.dumps(json_payload, ensure_ascii=False)}
    ]

    payload = {
        "model": model,
        "messages": messages,
        "temperature": 0.0
    }

    response = requests.post(url, json=payload, headers=headers)
    data = response.json()

    if "error" in data:
        raise Exception(f"OpenAI Error: {data['error']}")

    return data["choices"][0]["message"]["content"]

def load_logic_candidates(student_id: str, topk: int = 5):
    if not LOGIC_EXPORT.exists():
        return []

    df = pd.read_csv(LOGIC_EXPORT)
    df = df[df["student"] == student_id]

    if df.empty:
        return []

    df = df.sort_values("similarity", ascending=False).head(topk)

    return [
        {
            "module_id": r["module_id"],
            "title": r.get("module_title", ""),
            "similarity": float(r["similarity"]),
        }
        for _, r in df.iterrows()
    ]

def get_module_competencies(module_ids):
    if not module_ids:
        return {}

    cypher = """
    UNWIND $ids AS mid
    MATCH (m:Module {id: mid})-[:HAS_COMPETENCY]->(c:Competency)
    RETURN mid AS module_id,
           collect(DISTINCT c.id) AS comp_ids,
           collect(DISTINCT c.domain) AS domains,
           collect(DISTINCT c.description) AS descriptions
    """

    with neo4j_session() as session:
        rows = session.run(cypher, ids=module_ids)
        return {
            r["module_id"]: {
                "competency_ids": r["comp_ids"],
                "domains": r["domains"],
                "descriptions": r["descriptions"],
            }
            for r in rows
        }

def build_prompt(candidates, comp_map):

    modules = []
    for c in candidates:
        info = comp_map.get(c["module_id"], {})
        full_desc = " ".join(info.get("descriptions", []))[:1000]
        short_desc = [d[:200] for d in info.get("descriptions", [])]

        modules.append({
            "module_id": c["module_id"],
            "competency_ids": info.get("competency_ids", []),
            "domains": info.get("domains", []),
            "descriptions": short_desc,
            "full_description": full_desc,
        })

    prompt_json = {
        "task": "validate_modules",
        "rules": {
            "only_json": True,
            "missing_info_equals_unsuitable": True,
            "output_schema": {
                "validation": [
                    {
                        "module_id": "string",
                        "suitable": "boolean",
                        "reason": "string"
                    }
                ]
            }
        },
        "candidate_module_ids": [c["module_id"] for c in candidates],
        "modules_detail": modules,
        "output_format": {
            "validation": [
                {
                    "module_id": "<id_from_list>",
                    "suitable": True,
                    "reason": "<short_reason>"
                }
            ]
        }
    }

    return prompt_json

def safe_parse(raw):
    try:
        stack = []
        start = None

        for i, ch in enumerate(raw):
            if ch == "{":
                if start is None:
                    start = i
                stack.append("{")
            elif ch == "}":
                if stack:
                    stack.pop()
                    if not stack: 
                        return json.loads(raw[start:i+1])
        return None
    except Exception:
        return None

def choose_final(candidates, validation):

    suitable_ids = [v["module_id"] for v in validation if v.get("suitable") is True]

    if suitable_ids:
        candidates_sorted = sorted(
            candidates,
            key=lambda x: x["similarity"],
            reverse=True
        )
        for c in candidates_sorted:
            if c["module_id"] in suitable_ids:
                return c["module_id"], "Suitable by LLM"
    else:
        best = max(candidates, key=lambda x: x["similarity"])
        return best["module_id"], "Fallback: highest similarity"

def recommend_binary(student_int_id: int, topk=5, model="gpt-4o-mini"):

    student_id = f"user_{student_int_id}"

    candidates = load_logic_candidates(student_id, topk)
    if not candidates:
        return []

    module_ids = [c["module_id"] for c in candidates]
    comp_map = get_module_competencies(module_ids)

    rule_filtered = []
    for c in candidates:
        info = comp_map.get(c["module_id"], {})

        if not info.get("competency_ids"):
            continue

        module_domains = set(info.get("domains", []))
        if not module_domains:
            continue

        desc_list = info.get("descriptions", [])
        if not desc_list or all(len(d.strip()) < 20 for d in desc_list):
            continue

        rule_filtered.append(c)

    if not rule_filtered:
        best = max(candidates, key=lambda x: x["similarity"])
        return [{
            "student": student_id,
            "module_id": best["module_id"],
            "reason": "Rule-based fallback"
        }]

    candidates = rule_filtered

    prompt_json = build_prompt(candidates, comp_map)

    raw = call_llm(prompt_json, model=model)
    print("\n===== RAW LLM OUTPUT =====")
    print(raw)
    print("==========================\n")

    resp = safe_parse(raw)

    if not resp or "validation" not in resp:
        best = max(candidates, key=lambda x: x["similarity"])
        return [{"module_id": best["module_id"], "reason": "fallback"}]

    validation = resp["validation"]

    seen = set()
    deduped = []
    for v in validation:
        mid = v.get("module_id")
        if mid not in seen:
            seen.add(mid)
            deduped.append(v)
    validation = deduped

    module_ids = [c["module_id"] for c in candidates]
    validated_map = {v["module_id"]: v for v in validation if v["module_id"] in module_ids}

    cleaned = []
    for mid in module_ids:
        if mid in validated_map:
            cleaned.append(validated_map[mid])
        else:
            cleaned.append({
                "module_id": mid,
                "suitable": False,
                "reason": "LLM did not evaluate this module_id."
            })

    for v in cleaned:
        print(f" - {v['module_id']}: suitable={v['suitable']}, reason={v['reason']}")

    final_id, reason = choose_final(candidates, cleaned)

    out = [{
        "student": student_id,
        "module_id": final_id,
        "reason": reason
    }]

    pd.DataFrame(out).to_csv(FINAL_EXPORT, index=False)
    print(f"\nSaved → {FINAL_EXPORT}")
    print("FINAL:", final_id, "-", reason)

    return out

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sid", type=int, default=69)
    parser.add_argument("--topk", type=int, default=5)
    args = parser.parse_args()
    recommend_binary(args.sid, topk=args.topk)