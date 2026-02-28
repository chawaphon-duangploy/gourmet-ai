"""
monitor.py — Real-time review monitoring
Polls for new reviews every N minutes and triggers analysis automatically.

Run standalone:
    python pipeline/monitor.py --config config/default.json

Or import and run in background from main.py
"""

import os
import time
import json
import logging
import hashlib
import pandas as pd
from datetime import datetime
from pathlib import Path

log = logging.getLogger(__name__)

SEEN_REVIEWS_DIR = "data/seen"


def get_review_hash(text: str) -> str:
    """Create unique fingerprint for a review to detect duplicates."""
    return hashlib.md5(text.strip().encode()).hexdigest()


def load_seen_hashes(restaurant_id: str) -> set:
    """Load previously seen review hashes from disk."""
    path = f"{SEEN_REVIEWS_DIR}/{restaurant_id}_seen.json"
    if os.path.exists(path):
        with open(path) as f:
            return set(json.load(f))
    return set()


def save_seen_hashes(restaurant_id: str, hashes: set):
    """Persist seen hashes so we don't re-analyze old reviews."""
    os.makedirs(SEEN_REVIEWS_DIR, exist_ok=True)
    path = f"{SEEN_REVIEWS_DIR}/{restaurant_id}_seen.json"
    with open(path, "w") as f:
        json.dump(list(hashes), f)


def fetch_new_reviews(config: dict) -> list:
    """
    Fetch reviews and return only ones we haven't seen before.
    
    Currently reads from CSV file.
    Future: swap this function for Wongnai API / Google Maps API call.
    
    Returns list of dicts: [{"text": "...", "rating": 4.0}, ...]
    """
    restaurant_id = config.get("restaurant_id", "default")
    csv_path = config.get("csv_path") or f"data/raw/{restaurant_id}_raw.csv"

    if not os.path.exists(csv_path):
        log.warning(f"  No CSV found at {csv_path} — nothing to monitor")
        return []

    df = pd.read_csv(csv_path)
    text_col = next((c for c in ['review_text','review_body','text','review'] if c in df.columns), df.columns[0])
    rating_col = next((c for c in ['rating','review_rating','score'] if c in df.columns), None)

    seen_hashes = load_seen_hashes(restaurant_id)
    new_reviews = []
    new_hashes = set()

    for _, row in df.iterrows():
        text = str(row[text_col]).strip()
        if not text or text == 'nan':
            continue
        h = get_review_hash(text)
        if h not in seen_hashes:
            new_reviews.append({
                "text": text,
                "rating": float(row[rating_col]) if rating_col else 5.0,
                "detected_at": datetime.now().isoformat()
            })
            new_hashes.add(h)

    # Save all hashes (old + new)
    save_seen_hashes(restaurant_id, seen_hashes | new_hashes)

    log.info(f"  Monitor: {len(new_reviews)} new reviews detected")
    return new_reviews


def monitor_loop(config: dict, interval_minutes: int = 30):
    """
    Continuously poll for new reviews.
    Triggers full analysis pipeline when new reviews are found.
    
    interval_minutes: how often to check (default 30 min)
    """
    from pipeline.main import run_pipeline
    from pipeline.notify import notify_n8n

    log.info(f"🔍 Monitor started — checking every {interval_minutes} min")
    log.info(f"   Restaurant: {config.get('restaurant_name')}")

    while True:
        log.info(f"\n⏰ [{datetime.now().strftime('%H:%M:%S')}] Checking for new reviews...")

        try:
            new_reviews = fetch_new_reviews(config)

            if new_reviews:
                log.info(f"  🆕 {len(new_reviews)} new reviews — triggering pipeline")

                # Save new reviews to a temp CSV for pipeline to pick up
                restaurant_id = config.get("restaurant_id", "default")
                temp_path = f"data/raw/{restaurant_id}_new_batch.csv"
                pd.DataFrame(new_reviews).rename(
                    columns={"text": "review_text"}
                ).to_csv(temp_path, index=False)

                # Trigger pipeline on new reviews only
                batch_config = {**config, "csv_path": temp_path}
                result = run_pipeline(batch_config)

                # Notify via LINE + n8n
                notify_n8n(config, event="new_reviews_detected", payload={
                    "count": len(new_reviews),
                    "pipeline_status": result.get("status"),
                    "bad_pct": result.get("steps", {}).get("analyze", {}).get("bad_review_pct", 0)
                })
            else:
                log.info("  ✅ No new reviews — all caught up")

        except Exception as e:
            log.error(f"  ❌ Monitor error: {e}")

        log.info(f"  💤 Sleeping {interval_minutes} min...")
        time.sleep(interval_minutes * 60)


# ── Run standalone ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/default.json")
    parser.add_argument("--interval", type=int, default=30, help="Minutes between checks")
    args = parser.parse_args()

    with open(args.config) as f:
        config = json.load(f)

    monitor_loop(config, interval_minutes=args.interval)
