"""
transform.py — Clean, balance, and stage reviews
"""
import os
import re
import random
import logging
import pandas as pd

log = logging.getLogger(__name__)

KEYWORD_MAP = {
    "positive": ["อร่อย","ดี","เยี่ยม","ชอบ","แนะนำ","สด","สะอาด","คุ้ม","เร็ว","สวย","เลิศ","ถูกใจ","หอม","นุ่ม"],
    "negative": ["แย่","ไม่อร่อย","ช้า","แพง","สกปรก","เหม็น","ห่วย","น้อย","เค็ม","จืด","ดิบ","รอนาน","ผิดหวัง","แมลงสาบ","แข็ง"],
}


def clean_text(text: str) -> str:
    if not isinstance(text, str):
        return ""
    text = text.replace("\n", " ").strip()
    text = re.sub(r"\([^)]*\)", "", text)
    text = re.sub(r"[^\u0E00-\u0E7Fa-zA-Z0-9\s\.\,\!\?]", "", text)
    return re.sub(r"\s+", " ", text).strip()


def pre_classify(text: str) -> str:
    pos = sum(1 for w in KEYWORD_MAP["positive"] if w in str(text))
    neg = sum(1 for w in KEYWORD_MAP["negative"] if w in str(text))
    if neg > pos: return "bad"
    if pos > neg: return "good"
    return "neutral"


def transform_data(config: dict) -> dict:
    """
    Clean + balanced sample. Returns staged path and counts.
    """
    restaurant_id = config.get("restaurant_id", "default")
    sample_size   = config.get("sample_size", 2000)
    raw_path      = f"data/raw/{restaurant_id}_raw.csv"
    staged_dir    = f"data/staged"
    staged_path   = f"{staged_dir}/{restaurant_id}_clean.csv"
    os.makedirs(staged_dir, exist_ok=True)

    df = pd.read_csv(raw_path, engine='python', on_bad_lines='skip', escapechar='\\')

    # Detect review column
    text_col = next((c for c in ['review_body','text','review','review_text'] if c in df.columns), df.columns[0])
    log.info(f"  Review column: '{text_col}'")

    # Pre-classify for balanced sampling
    df['_sent'] = df[text_col].astype(str).apply(pre_classify)
    pool_good = df[df['_sent'] == 'good']
    pool_bad  = df[df['_sent'] == 'bad']

    good_ratio      = random.uniform(0.5, 0.85)
    target_good     = int(sample_size * good_ratio)
    target_bad      = sample_size - target_good

    sample_good = pool_good.sample(n=min(len(pool_good), target_good), random_state=42)
    sample_bad  = pool_bad.sample(n=min(len(pool_bad),  target_bad),  random_state=42)

    balanced = pd.concat([sample_good, sample_bad]).sample(frac=1, random_state=42).reset_index(drop=True)
    balanced['review_text'] = balanced[text_col].apply(clean_text)
    balanced = balanced[balanced['review_text'].str.len() > 5]

    balanced[['review_text']].to_csv(staged_path, index=False)

    log.info(f"  Staged {len(balanced)} reviews → {staged_path} ({good_ratio:.0%} good)")
    return {
        "rows_staged": len(balanced),
        "staged_path": staged_path,
        "good_count": len(sample_good),
        "bad_count": len(sample_bad)
    }
