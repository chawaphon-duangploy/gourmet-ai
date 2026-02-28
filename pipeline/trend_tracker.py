"""
trend_tracker.py — Week-over-week trend detection
Detects spikes in complaint keywords and fires alerts when things get worse.

Example alert: "ช้า" complaints up 40% this week vs last week
"""

import os
import json
import logging
import pandas as pd
from datetime import datetime, timedelta
from collections import Counter

log = logging.getLogger(__name__)

# Alert if a keyword increases by more than this % week-over-week
SPIKE_THRESHOLD_PCT = 30


def load_weekly_snapshot(restaurant_id: str, week_offset: int = 0) -> dict:
    """
    Load saved keyword counts for a given week.
    week_offset=0 → current week, week_offset=1 → last week
    """
    week_label = _get_week_label(week_offset)
    path = f"data/results/{restaurant_id}/trends/{week_label}.json"

    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}


def save_weekly_snapshot(restaurant_id: str, keyword_counts: dict):
    """Save this week's keyword counts for future comparison."""
    week_label = _get_week_label(0)
    dir_path = f"data/results/{restaurant_id}/trends"
    os.makedirs(dir_path, exist_ok=True)

    path = f"{dir_path}/{week_label}.json"
    with open(path, "w") as f:
        json.dump({
            "week": week_label,
            "saved_at": datetime.now().isoformat(),
            "counts": keyword_counts
        }, f, ensure_ascii=False, indent=2)

    log.info(f"  Trend snapshot saved: {path}")


def detect_spikes(current_counts: dict, previous_counts: dict) -> list:
    """
    Compare current vs previous keyword counts.
    Returns list of spikes: [{keyword, current, previous, change_pct, severity}]
    """
    spikes = []

    for keyword, current_count in current_counts.items():
        prev_count = previous_counts.get(keyword, 0)

        # Skip keywords with very low volume (noise)
        if current_count < 3:
            continue

        if prev_count == 0:
            # New keyword appearing — always flag
            change_pct = 100
        else:
            change_pct = ((current_count - prev_count) / prev_count) * 100

        if change_pct >= SPIKE_THRESHOLD_PCT:
            severity = "HIGH" if change_pct >= 80 else "MEDIUM" if change_pct >= 50 else "LOW"
            spikes.append({
                "keyword": keyword,
                "current_count": current_count,
                "previous_count": prev_count,
                "change_pct": round(change_pct, 1),
                "severity": severity,
                "alert_message": f"⚠️ '{keyword}' mentions up {round(change_pct)}% vs last week ({prev_count} → {current_count})"
            })

    # Sort by severity then change_pct
    severity_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    spikes.sort(key=lambda x: (severity_order[x["severity"]], -x["change_pct"]))
    return spikes


def run_trend_analysis(restaurant_id: str, current_keywords_df: pd.DataFrame) -> dict:
    """
    Full trend analysis flow:
    1. Load last week's snapshot
    2. Compare with current keywords
    3. Detect spikes
    4. Save current as new snapshot
    5. Return trend report
    """
    # Build current keyword counts
    current_counts = dict(zip(
        current_keywords_df['keyword'],
        current_keywords_df['frequency']
    ))

    # Load previous week
    previous_snapshot = load_weekly_snapshot(restaurant_id, week_offset=1)
    previous_counts = previous_snapshot.get("counts", {})

    # Detect spikes
    spikes = detect_spikes(current_counts, previous_counts)

    # Save current snapshot
    save_weekly_snapshot(restaurant_id, current_counts)

    # Build staff correlation hints
    staff_hints = _staff_correlation_hints(spikes)

    report = {
        "restaurant_id": restaurant_id,
        "week": _get_week_label(0),
        "previous_week": _get_week_label(1),
        "total_keywords_tracked": len(current_counts),
        "spikes_detected": len(spikes),
        "spikes": spikes,
        "staff_hints": staff_hints,
        "has_previous_data": bool(previous_counts),
        "summary": _build_summary(spikes)
    }

    log.info(f"  Trend analysis: {len(spikes)} spikes detected")
    return report


def _staff_correlation_hints(spikes: list) -> list:
    """
    Map complaint spikes to likely staff/operational causes.
    This is the 'staff performance tracking' feature.
    """
    SERVICE_KEYWORDS = {"พนักงาน", "บริการ", "เสิร์ฟ", "ต้อนรับ", "หน้างอ", "ไม่สุภาพ"}
    SPEED_KEYWORDS = {"ช้า", "รอนาน", "รอ", "คิว", "นาน"}
    FOOD_KEYWORDS = {"เค็ม", "จืด", "ดิบ", "แข็ง", "เหม็น"}

    hints = []
    for spike in spikes:
        kw = spike["keyword"]
        if kw in SERVICE_KEYWORDS:
            hints.append({
                "keyword": kw,
                "likely_cause": "Front-of-house staff behavior",
                "action": "Review CCTV footage and check shift schedule for this period",
                "dept": "ฝ่ายบริการ/พนักงานเสิร์ฟ"
            })
        elif kw in SPEED_KEYWORDS:
            hints.append({
                "keyword": kw,
                "likely_cause": "Kitchen throughput or understaffing",
                "action": "Check if new menu items or staff shortage coincided with this period",
                "dept": "ฝ่ายครัว/เชฟ"
            })
        elif kw in FOOD_KEYWORDS:
            hints.append({
                "keyword": kw,
                "likely_cause": "Recipe consistency or ingredient quality change",
                "action": "Check if ingredient supplier or chef changed recently",
                "dept": "ฝ่ายครัว/จัดซื้อ"
            })

    return hints


def _build_summary(spikes: list) -> str:
    if not spikes:
        return "✅ No significant trend changes this week — performance stable"
    high = [s for s in spikes if s["severity"] == "HIGH"]
    if high:
        kws = ", ".join([f"'{s['keyword']}'" for s in high[:3]])
        return f"🚨 HIGH ALERT: Significant spike in {kws} — immediate review recommended"
    return f"⚠️ {len(spikes)} complaint keyword(s) trending up — monitor closely"


def _get_week_label(offset: int = 0) -> str:
    """Returns ISO week label like '2025-W23'"""
    target = datetime.now() - timedelta(weeks=offset)
    return target.strftime("%Y-W%W")
