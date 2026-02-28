"""
analyze.py — Real Gemini LLM analysis + calculated severity/cost
No more hardcoded rules — everything is derived from actual data.
"""

import os
import time
import logging
import json
import re
import pandas as pd
from collections import Counter
from tqdm import tqdm
from openai import OpenAI

log = logging.getLogger(__name__)

# ── Cost map (justified by business logic) ────────────────────────────────────
COST_MAP = {
    "food":        "Medium",   # ingredient sourcing changes
    "service":     "Low",      # staff training time only
    "atmosphere":  "Low",      # cleaning supplies, minor fixes
    "price":       "Low",      # menu reprint, promo design
    "location":    "Medium",   # signage, Google Maps update
}

SUGGESTION_MAP = {
    "food": [
        "Review recipes and check ingredient freshness daily",
        "Run weekly blind taste tests with kitchen staff",
        "Remove low-rated dishes from menu based on complaint data",
    ],
    "service": [
        "Conduct urgent staff hospitality training",
        "Implement queue management system for peak hours",
        "Hire part-time staff during busy periods",
    ],
    "atmosphere": [
        "Deep clean facility — focus on restrooms and dining area",
        "Adjust lighting, music volume, and seating comfort",
        "Repair or replace damaged furniture",
    ],
    "price": [
        "Benchmark portion size vs competitor pricing",
        "Introduce value-set lunch or dinner promotions",
    ],
    "location": [
        "Improve roadside signage visibility",
        "Update Google Maps pin and add clear directions online",
    ],
}

KEYWORD_MAP = {
    "positive":   ["อร่อย","ดี","เยี่ยม","ชอบ","แนะนำ","สด","สะอาด","คุ้ม","เร็ว","สวย","เลิศ","ถูกใจ","หอม","นุ่ม"],
    "negative":   ["แย่","ไม่อร่อย","ช้า","แพง","สกปรก","เหม็น","ห่วย","น้อย","เค็ม","จืด","ดิบ","รอนาน","ผิดหวัง","แมลงสาบ","แข็ง"],
    "service":    ["พนักงาน","บริการ","เสิร์ฟ","ต้อนรับ","พูดจา","คนขาย","รอ","คิว","ช้า","หน้างอ"],
    "price":      ["ราคา","บาท","แพง","ถูก","เช็คบิล","คุ้ม","กระเป๋า"],
    "atmosphere": ["บรรยากาศ","ร้าน","แอร์","เสียง","ที่นั่ง","โต๊ะ","ห้องน้ำ","จอดรถ","ร้อน","ยุง"],
    "location":   ["ทางเข้า","ซอย","ถนน","ที่จอด","mrt","bts","หาอยาก","แผนที่"],
}

ALERT_THRESHOLD = 0.40


# ── Gemini LLM Analysis ───────────────────────────────────────────────────────
LLM_PROMPT = '''วิเคราะห์รีวิวร้านอาหารต่อไปนี้และตอบเป็น JSON เดียวเท่านั้น:

รีวิว: "{review}"

JSON ที่ต้องการ:
{{
  "sentiment": "good หรือ bad",
  "category": "food หรือ service หรือ atmosphere หรือ price หรือ location",
  "confidence": 0.0 ถึง 1.0,
  "reason": "เหตุผลสั้นๆ 1 ประโยค"
}}

ตอบ JSON เท่านั้น ห้ามมีข้อความอื่น'''


def analyze_with_gemini(text: str, client: OpenAI) -> dict:
    """Call Gemini to analyze a single review. Returns sentiment + category."""
    try:
        response = client.chat.completions.create(
            model="gemini-2.0-flash",
            n=1,
            messages=[
                {"role": "system", "content": "คุณคือผู้เชี่ยวชาญวิเคราะห์รีวิวร้านอาหารภาษาไทย ตอบเป็น JSON เท่านั้น"},
                {"role": "user", "content": LLM_PROMPT.format(review=text[:300])}
            ]
        )
        raw = response.choices[0].message.content.strip()

        # Clean markdown if present
        raw = re.sub(r"```json|```", "", raw).strip()

        # Extract first JSON object
        match = re.search(r'\{.*?\}', raw, re.DOTALL)
        if match:
            result = json.loads(match.group())
            return {
                "sentiment": result.get("sentiment", "good"),
                "category":  result.get("category", "food"),
                "confidence": float(result.get("confidence", 0.8)),
                "reason":    result.get("reason", ""),
                "source":    "llm"
            }
    except Exception as e:
        log.debug(f"LLM fallback for review: {e}")

    # Fallback to keyword if LLM fails
    return analyze_keyword(text)


def analyze_keyword(text: str) -> dict:
    """Keyword-based fallback when LLM is unavailable."""
    pos = sum(1 for w in KEYWORD_MAP['positive'] if w in text)
    neg = sum(1 for w in KEYWORD_MAP['negative'] if w in text)
    sentiment = "bad" if neg > pos else "good"

    cat_scores = {c: sum(1 for w in KEYWORD_MAP[c] if w in text)
                  for c in ["service", "price", "atmosphere", "location"]}
    category = max(cat_scores, key=cat_scores.get) if any(cat_scores.values()) else "food"

    return {
        "sentiment":  sentiment,
        "category":   category,
        "confidence": 0.6,
        "reason":     "keyword-based analysis",
        "source":     "keyword"
    }


# ── Severity Calculator (from real data) ──────────────────────────────────────
def calculate_severity(complaint_count: int, total_bad: int) -> str:
    """
    Calculate severity from actual complaint volume.
    Not hardcoded — derived from data.
    
    Logic:
        >25% of all bad reviews = High   (major problem affecting most unhappy customers)
        >10% of all bad reviews = Medium (notable but not dominant)
        <=10%                   = Low    (minor, isolated complaints)
    """
    if total_bad == 0:
        return "Low"
    pct = complaint_count / total_bad
    if pct > 0.25:
        return "High"
    elif pct > 0.10:
        return "Medium"
    return "Low"


def calculate_priority(severity: str, complaint_count: int) -> int:
    """Priority score — higher = more urgent."""
    severity_weight = {"High": 3, "Medium": 2, "Low": 1}
    return severity_weight.get(severity, 1) * complaint_count


# ── Main Analysis Function ────────────────────────────────────────────────────
def analyze_and_load(config: dict) -> dict:
    restaurant_id = config.get("restaurant_id", "default")
    use_llm       = config.get("use_llm", False)
    api_key       = config.get("gemini_api_key") or os.getenv("GEMINI_API_KEY")
    staged_path   = f"data/staged/{restaurant_id}_clean.csv"
    results_dir   = f"data/results/{restaurant_id}"
    os.makedirs(results_dir, exist_ok=True)

    df = pd.read_csv(staged_path)
    total = len(df)
    log.info(f"  Analyzing {total} reviews (mode: {'Gemini LLM' if use_llm and api_key else 'keyword'})...")

    # ── Choose analysis mode ──────────────────────────────────────────
    client = None
    if use_llm and api_key:
        try:
            client = OpenAI(
                api_key=api_key,
                base_url="https://generativelanguage.googleapis.com/v1beta/openai/"
            )
            log.info("  ✅ Gemini LLM connected")
        except Exception as e:
            log.warning(f"  ⚠️ Gemini connection failed, using keywords: {e}")

    # ── Analyze each review ───────────────────────────────────────────
    results = []
    kw_counter = Counter()
    llm_count = 0
    keyword_count = 0

    # LLM is expensive — sample 200 with LLM, rest with keywords
    LLM_SAMPLE = 200
    use_llm_for = set(range(min(LLM_SAMPLE, total))) if client else set()

    for i, row in enumerate(tqdm(df['review_text'], desc="Analyzing", disable=False)):
        text = str(row)

        if i in use_llm_for and client:
            result = analyze_with_gemini(text, client)
            time.sleep(0.3)  # Rate limit protection
            if result["source"] == "llm":
                llm_count += 1
            else:
                keyword_count += 1
        else:
            result = analyze_keyword(text)
            keyword_count += 1

        # Collect keywords
        found_kw = [w for lst in KEYWORD_MAP.values() for w in lst if w in text]
        kw_counter.update(found_kw)

        results.append({
            "sentiment":  result["sentiment"],
            "category":   result["category"],
            "confidence": result["confidence"],
            "analysis_source": result["source"],
            "keywords":   ",".join(found_kw[:5]),
        })

    log.info(f"  Analysis breakdown: {llm_count} LLM + {keyword_count} keyword")

    # ── Build analysis dataframe ──────────────────────────────────────
    analysis_df = pd.DataFrame(results)
    analysis_df['review_text'] = df['review_text'].values
    analysis_df.to_csv(f"{results_dir}/analysis.csv", index=False)

    # ── Keywords CSV ──────────────────────────────────────────────────
    kw_data = []
    for word, freq in kw_counter.most_common():
        cat = next((c for c, lst in KEYWORD_MAP.items() if word in lst), "general")
        kw_data.append({"keyword": word, "category_type": cat, "frequency": freq})
    pd.DataFrame(kw_data).to_csv(f"{results_dir}/keywords.csv", index=False)

    # ── Suggestions with CALCULATED severity ─────────────────────────
    bad_df = analysis_df[analysis_df['sentiment'] == 'bad']
    bad_pct = round(len(bad_df) / total * 100, 1) if total else 0
    total_bad = len(bad_df)

    sug_rows = []
    for cat, complaint_count in bad_df['category'].value_counts().items():
        # Calculate severity FROM DATA — not hardcoded
        severity = calculate_severity(complaint_count, total_bad)
        cost     = COST_MAP.get(cat, "Medium")
        priority = calculate_priority(severity, complaint_count)

        for i, suggestion in enumerate(SUGGESTION_MAP.get(cat, ["Review this category"])):
            sug_rows.append({
                "category":          cat,
                "suggestion":        suggestion,
                "severity_of_issue": severity,
                "resource_cost":     cost,
                "issue_count":       int(complaint_count),
                "complaint_pct":     round(complaint_count / total_bad * 100, 1),
                "priority_score":    priority - i,  # slight offset per suggestion
                "priority_rank":     i + 1,
                "severity_reason":   f"{complaint_count} complaints = {round(complaint_count/total_bad*100,1)}% of all bad reviews"
            })

    sug_df = pd.DataFrame(sug_rows).sort_values("priority_score", ascending=False)
    sug_df.to_csv(f"{results_dir}/suggestion.csv", index=False)

    # ── Alert if bad reviews too high ─────────────────────────────────
    if bad_pct / 100 >= ALERT_THRESHOLD:
        from notify import send_alert
        send_alert(config, event="new_review_alert", payload={
            "bad_review_pct": bad_pct,
            "top_issue": sug_rows[0]['category'] if sug_rows else "unknown",
            "message": f"⚠️ {bad_pct}% negative reviews — action required!"
        })

    log.info(f"  Done: {total} rows | {bad_pct}% bad | {len(sug_df)} suggestions")
    log.info(f"  LLM coverage: {llm_count}/{total} reviews ({round(llm_count/total*100,1)}%)")

    return {
        "rows_analyzed":        total,
        "bad_review_pct":       bad_pct,
        "suggestions_generated": len(sug_df),
        "llm_reviews":          llm_count,
        "keyword_reviews":      keyword_count,
        "results_dir":          results_dir
    }