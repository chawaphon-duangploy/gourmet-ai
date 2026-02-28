"""
extract.py — Pull reviews from Wongnai dataset or CSV upload
"""
import os
import logging
import pandas as pd

log = logging.getLogger(__name__)

RAW_DIR = "data/raw"


def extract_data(config: dict) -> dict:
    """
    Extract reviews from:
      1. Local CSV (if config["csv_path"] is set)
      2. Hugging Face Wongnai dataset (default)

    Returns: {"rows_extracted": int, "raw_path": str}
    """
    os.makedirs(RAW_DIR, exist_ok=True)
    restaurant_id = config.get("restaurant_id", "default")
    raw_path = f"{RAW_DIR}/{restaurant_id}_raw.csv"

    # ── Option A: Use uploaded CSV ──────────────────────────────────
    if config.get("csv_path") and os.path.exists(config["csv_path"]):
        log.info(f"  Using local CSV: {config['csv_path']}")
        df = pd.read_csv(config["csv_path"])
        df.to_csv(raw_path, index=False, escapechar='\\')
        return {"rows_extracted": len(df), "raw_path": raw_path, "source": "local_csv"}

    # ── Option B: Use cache if valid ────────────────────────────────
    if os.path.exists(raw_path):
        try:
            existing = pd.read_csv(raw_path)
            if len(existing) > 1000:
                log.info(f"  Raw cache found: {len(existing)} rows")
                return {"rows_extracted": len(existing), "raw_path": raw_path, "source": "cache"}
            else:
                log.warning("  Cache too small — re-downloading")
                os.remove(raw_path)
        except Exception:
            log.warning("  Cache corrupted — re-downloading")
            os.remove(raw_path)

    # ── Option C: Download from Hugging Face ────────────────────────
    try:
        from datasets import load_dataset
        log.info("  Downloading from Hugging Face...")
        dataset = load_dataset("iamwarint/wongnai-restaurant-review")
        df = pd.DataFrame(dataset['train'])
        df.to_csv(raw_path, index=False, escapechar='\\')
        log.info(f"  Saved {len(df)} rows → {raw_path}")
        return {"rows_extracted": len(df), "raw_path": raw_path, "source": "huggingface"}

    except Exception as e:
        raise RuntimeError(f"Extraction failed: {e}")