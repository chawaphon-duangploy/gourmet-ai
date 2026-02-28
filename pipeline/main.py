import os
import sys
import json
import time
import logging
import argparse
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from extract import extract_data
from transform import transform_data
from analyze import analyze_and_load
from notify import send_alert

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


def run_pipeline(config: dict) -> dict:
    job_id = f"{config['restaurant_id']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    result = {"job_id": job_id, "status": "running", "steps": {}}

    log.info(f"▶ Pipeline started | job_id={job_id} | restaurant={config['restaurant_name']}")

    steps = [
        ("extract",   extract_data,     "🔽 Extracting reviews..."),
        ("transform", transform_data,   "🔧 Cleaning & balancing..."),
        ("analyze",   analyze_and_load, "🤖 Analyzing sentiments..."),
    ]

    for step_name, fn, label in steps:
        log.info(label)
        t0 = time.time()
        try:
            step_result = fn(config)
            elapsed = round(time.time() - t0, 2)
            result["steps"][step_name] = {"status": "ok", "elapsed_sec": elapsed, **step_result}
            log.info(f"  ✅ {step_name} done in {elapsed}s")
        except Exception as e:
            elapsed = round(time.time() - t0, 2)
            result["steps"][step_name] = {"status": "error", "error": str(e), "elapsed_sec": elapsed}
            result["status"] = "failed"
            log.error(f"  ❌ {step_name} failed: {e}")
            send_alert(config, event="pipeline_failed", payload={
                "job_id": job_id, "step": step_name, "error": str(e)
            })
            return result

    result["status"] = "completed"
    result["completed_at"] = datetime.now().isoformat()

    restaurant_id = config.get("restaurant_id", "default")
    manifest_path = f"data/results/{restaurant_id}/manifest.json"
    os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
    with open(manifest_path, "w") as f:
        json.dump(result, f, indent=2)

    log.info(f"✅ Pipeline complete | job_id={job_id}")
    send_alert(config, event="pipeline_completed", payload=result)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/default.json")
    args = parser.parse_args()

    with open(args.config) as f:
        config = json.load(f)

    result = run_pipeline(config)
    print(json.dumps(result, indent=2, ensure_ascii=False))
