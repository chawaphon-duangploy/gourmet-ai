import os
import requests
import logging
from datetime import datetime

log = logging.getLogger(__name__)

def notify_line(message: str, token: str = None, user_id: str = None):
    token   = token   or os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
    user_id = user_id or os.getenv("LINE_USER_ID")
    if not token or not user_id:
        log.warning("No LINE credentials — skipping")
        return False
    try:
        r = requests.post(
            "https://api.line.me/v2/bot/message/push",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"to": user_id, "messages": [{"type": "text", "text": message}]},
            timeout=10
        )
        log.info(f"LINE sent: HTTP {r.status_code}")
        return True
    except Exception as e:
        log.warning(f"LINE failed: {e}")
        return False

def notify_n8n(config: dict, event: str, payload: dict = None):
    webhook_url = config.get("n8n_webhook_url") or os.getenv("N8N_WEBHOOK_URL")
    if not webhook_url:
        return
    try:
        requests.post(webhook_url, json={"event": event, "data": payload or {}}, timeout=10)
        log.info(f"n8n notified: {event}")
    except Exception as e:
        log.warning(f"n8n failed: {e}")

def send_alert(config: dict, event: str, payload: dict = None):
    restaurant_name = config.get("restaurant_name", "ร้านของคุณ")
    payload = payload or {}

    if event == "pipeline_completed":
        msg = (f"✅ วิเคราะห์รีวิวเสร็จแล้ว!\n"
               f"ร้าน: {restaurant_name}\n"
               f"รีวิวทั้งหมด: {payload.get('rows_analyzed', 0)} รีวิว\n"
               f"รีวิวเชิงลบ: {payload.get('bad_review_pct', 0)}%")
    elif event == "new_review_alert":
        msg = (f"🚨 แจ้งเตือนรีวิวแย่!\n"
               f"ร้าน: {restaurant_name}\n"
               f"รีวิวเชิงลบ: {payload.get('bad_review_pct', 0)}%\n"
               f"ปัญหาหลัก: {payload.get('top_issue', 'ไม่ระบุ')}")
    elif event == "pipeline_failed":
        msg = (f"❌ Pipeline ล้มเหลว\n"
               f"ร้าน: {restaurant_name}\n"
               f"Error: {str(payload.get('error', ''))[:80]}")
    else:
        msg = f"[Gourmet AI] {event}\nร้าน: {restaurant_name}"

    notify_line(msg)
    notify_n8n(config, event=event, payload=payload)
