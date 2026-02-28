"""
responder.py — AI-powered review response suggestions
Uses Gemini to generate professional Thai responses to negative reviews.

Usage:
    from pipeline.responder import suggest_response
    reply = suggest_response(review_text, sentiment, category, restaurant_name)
"""

import os
import logging
from openai import OpenAI

log = logging.getLogger(__name__)


def get_gemini_client(api_key: str) -> OpenAI:
    return OpenAI(
        api_key=api_key,
        base_url="https://generativelanguage.googleapis.com/v1beta/openai/"
    )


RESPONSE_PROMPT = """
คุณคือผู้จัดการร้านอาหาร "{restaurant_name}" ที่มีมารยาทและเป็นมืออาชีพ

รีวิวจากลูกค้า:
"{review_text}"

ประเภทความรู้สึก: {sentiment}
หมวดหมู่ปัญหา: {category}

กรุณาเขียนตอบกลับรีวิวนี้เป็นภาษาไทย โดย:
- ถ้าเป็นรีวิวเชิงลบ: ขอโทษอย่างจริงใจ รับทราบปัญหา และบอกว่าจะแก้ไขอย่างไร
- ถ้าเป็นรีวิวเชิงบวก: ขอบคุณอย่างอบอุ่น และเชิญกลับมาใช้บริการ
- ความยาว: 2-3 ประโยค กระชับและจริงใจ
- ไม่ต้องใส่ชื่อร้านซ้ำในการตอบ

ตอบเฉพาะข้อความตอบกลับเท่านั้น ไม่ต้องมีคำอธิบายอื่น
"""


def suggest_response(
    review_text: str,
    sentiment: str,
    category: str,
    restaurant_name: str,
    api_key: str
) -> dict:
    """
    Generate a professional response to a review.
    
    Returns:
        {
            "suggested_reply": "...",
            "sentiment": "negative/positive",
            "category": "service/food/...",
            "approved": False   ← manager must approve before posting
        }
    """
    try:
        client = get_gemini_client(api_key)
        prompt = RESPONSE_PROMPT.format(
            restaurant_name=restaurant_name,
            review_text=review_text,
            sentiment=sentiment,
            category=category
        )

        response = client.chat.completions.create(
            model="gemini-2.0-flash",
            n=1,
            messages=[
                {"role": "system", "content": "คุณคือผู้เชี่ยวชาญด้านการบริการลูกค้าของร้านอาหาร"},
                {"role": "user", "content": prompt}
            ]
        )

        reply_text = response.choices[0].message.content.strip()
        log.info(f"  ✅ Response generated ({len(reply_text)} chars)")

        return {
            "suggested_reply": reply_text,
            "sentiment": sentiment,
            "category": category,
            "approved": False,
            "original_review": review_text[:100] + "..." if len(review_text) > 100 else review_text
        }

    except Exception as e:
        log.error(f"  ❌ Response generation failed: {e}")
        return {
            "suggested_reply": "ขอบคุณสำหรับรีวิวของคุณ ทางร้านจะนำความคิดเห็นไปปรับปรุงการบริการต่อไปครับ/ค่ะ",
            "sentiment": sentiment,
            "category": category,
            "approved": False,
            "error": str(e)
        }


def batch_generate_responses(analysis_df, config: dict) -> list:
    """
    Generate responses for all negative reviews in a dataframe.
    Returns list of response dicts, sorted by priority (worst reviews first).
    """
    api_key = config.get("gemini_api_key") or os.getenv("GEMINI_API_KEY")
    restaurant_name = config.get("restaurant_name", "ร้านของเรา")

    if not api_key:
        log.error("No Gemini API key — cannot generate responses")
        return []

    # Focus on negative reviews only
    negative_reviews = analysis_df[analysis_df['sentiment'] == 'bad'].copy()
    log.info(f"  Generating responses for {len(negative_reviews)} negative reviews...")

    responses = []
    for _, row in negative_reviews.iterrows():
        result = suggest_response(
            review_text=row['review_text'],
            sentiment=row['sentiment'],
            category=row['category'],
            restaurant_name=restaurant_name,
            api_key=api_key
        )
        responses.append(result)

    log.info(f"  ✅ Generated {len(responses)} response suggestions")
    return responses
