"""
api/server.py — FastAPI webhook server
n8n calls POST /run-pipeline to trigger analysis on demand.

Run:
    uvicorn api.server:app --reload --port 8000

n8n HTTP Request node → POST http://your-server:8000/run-pipeline
"""

from fastapi import FastAPI, BackgroundTasks, HTTPException, Header
from pydantic import BaseModel
from typing import Optional
import json, os, logging

from pipeline.main import run_pipeline

log = logging.getLogger(__name__)
app = FastAPI(title="Gourmet AI API", version="2.0")

# Simple API key auth (set env: GOURMET_API_KEY)
API_KEY = os.getenv("GOURMET_API_KEY", "dev-key-change-me")


# ─────────────────────────────────────────────
# Request / Response models
# ─────────────────────────────────────────────
class PipelineRequest(BaseModel):
    restaurant_id: str
    restaurant_name: str
    source_url: Optional[str] = None          # Wongnai page URL
    sample_size: Optional[int] = 2000
    n8n_webhook_url: Optional[str] = None     # Callback URL for status updates
    notify_on_complete: Optional[bool] = True
    gemini_api_key: Optional[str] = None      # Override default key


class PipelineResponse(BaseModel):
    job_id: str
    status: str
    message: str


# ─────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "service": "gourmet-ai"}


@app.post("/run-pipeline", response_model=PipelineResponse)
async def trigger_pipeline(
    req: PipelineRequest,
    background_tasks: BackgroundTasks,
    x_api_key: Optional[str] = Header(None)
):
    """
    Trigger full ETL + analysis pipeline.
    Runs async — returns immediately, posts result to n8n_webhook_url when done.
    """
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

    config = req.dict()
    job_id = f"{req.restaurant_id}_{__import__('datetime').datetime.now().strftime('%Y%m%d_%H%M%S')}"

    background_tasks.add_task(run_pipeline, config)

    log.info(f"📥 Pipeline queued: {job_id}")
    return PipelineResponse(
        job_id=job_id,
        status="queued",
        message=f"Pipeline started for {req.restaurant_name}. Callback: {req.n8n_webhook_url}"
    )


@app.get("/results/{restaurant_id}")
def get_results(restaurant_id: str, x_api_key: Optional[str] = Header(None)):
    """Return latest analysis manifest for a restaurant."""
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

    manifest_path = f"data/results/{restaurant_id}/manifest.json"
    if not os.path.exists(manifest_path):
        raise HTTPException(status_code=404, detail="No results found. Run the pipeline first.")

    with open(manifest_path) as f:
        return json.load(f)


@app.get("/results/{restaurant_id}/summary")
def get_summary(restaurant_id: str, x_api_key: Optional[str] = Header(None)):
    """Return actionable summary — top issues + recommendations."""
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

    import pandas as pd
    paths = {
        "analysis": f"data/results/{restaurant_id}/analysis.csv",
        "suggestions": f"data/results/{restaurant_id}/suggestion.csv",
    }
    for k, p in paths.items():
        if not os.path.exists(p):
            raise HTTPException(status_code=404, detail=f"Missing {k} data.")

    df_analysis = pd.read_csv(paths["analysis"])
    df_sug = pd.read_csv(paths["suggestions"])

    total = len(df_analysis)
    good_pct = round(df_analysis[df_analysis['sentiment'] == 'good'].shape[0] / total * 100, 1)

    return {
        "restaurant_id": restaurant_id,
        "total_reviews": total,
        "satisfaction_pct": good_pct,
        "top_issues": df_sug.head(3)[["category", "suggestion", "severity_of_issue"]].to_dict("records"),
        "category_breakdown": df_analysis["category"].value_counts().to_dict()
    }
