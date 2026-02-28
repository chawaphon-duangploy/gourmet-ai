"""
competitor.py — Competitor benchmarking using Google Places API
Compares your restaurant's sentiment vs nearby competitors.

Requires: GOOGLE_PLACES_API_KEY in .env
"""

import os
import logging
import requests
import pandas as pd
from datetime import datetime

log = logging.getLogger(__name__)

GOOGLE_PLACES_BASE = "https://maps.googleapis.com/maps/api/place"


def search_nearby_competitors(lat: float, lng: float, radius_m: int = 1000, api_key: str = None) -> list:
    """
    Find competing restaurants within radius using Google Places API.
    Returns list of {name, place_id, rating, total_ratings, address}
    """
    api_key = api_key or os.getenv("GOOGLE_PLACES_API_KEY")
    if not api_key:
        log.warning("No Google Places API key — using mock competitor data")
        return _mock_competitors()

    url = f"{GOOGLE_PLACES_BASE}/nearbysearch/json"
    params = {
        "location": f"{lat},{lng}",
        "radius": radius_m,
        "type": "restaurant",
        "key": api_key
    }

    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        results = r.json().get("results", [])

        competitors = []
        for place in results[:10]:  # Top 10 nearby
            competitors.append({
                "name": place.get("name"),
                "place_id": place.get("place_id"),
                "rating": place.get("rating", 0),
                "total_ratings": place.get("user_ratings_total", 0),
                "address": place.get("vicinity", ""),
                "price_level": place.get("price_level", "N/A")
            })

        log.info(f"  Found {len(competitors)} nearby competitors")
        return competitors

    except Exception as e:
        log.error(f"  Google Places API error: {e}")
        return _mock_competitors()


def _mock_competitors() -> list:
    """Fallback mock data when API key not available — for demo/testing."""
    return [
        {"name": "ร้านข้าวต้มริมน้ำ", "rating": 4.2, "total_ratings": 312, "price_level": 2},
        {"name": "ครัวคุณป้า", "rating": 4.5, "total_ratings": 891, "price_level": 1},
        {"name": "อาหารไทยแม่กลอง", "rating": 3.8, "total_ratings": 156, "price_level": 2},
        {"name": "ริมน้ำซีฟู้ด", "rating": 4.0, "total_ratings": 445, "price_level": 3},
    ]


def build_benchmark_report(your_restaurant: dict, competitors: list) -> dict:
    """
    Compare your restaurant metrics against competitors.
    
    your_restaurant: {
        "name": "...",
        "rating": 4.1,
        "satisfaction_pct": 73.0,
        "top_issues": ["service", "price"],
        "total_reviews": 200
    }
    """
    if not competitors:
        return {"error": "No competitor data available"}

    competitor_ratings = [c["rating"] for c in competitors if c.get("rating")]
    avg_competitor_rating = round(sum(competitor_ratings) / len(competitor_ratings), 2) if competitor_ratings else 0

    your_rating = your_restaurant.get("rating", 0)
    rating_gap = round(your_rating - avg_competitor_rating, 2)

    # Rank your restaurant among competitors
    all_ratings = sorted(competitor_ratings + [your_rating], reverse=True)
    your_rank = all_ratings.index(your_rating) + 1

    report = {
        "generated_at": datetime.now().isoformat(),
        "your_restaurant": your_restaurant["name"],
        "your_rating": your_rating,
        "your_satisfaction_pct": your_restaurant.get("satisfaction_pct", 0),
        "competitor_avg_rating": avg_competitor_rating,
        "rating_gap": rating_gap,
        "your_rank": f"{your_rank} of {len(all_ratings)}",
        "competitors": competitors,
        "insights": _generate_insights(your_restaurant, competitors, rating_gap),
        "action_items": _generate_actions(your_restaurant, rating_gap)
    }

    # Save to disk
    restaurant_id = your_restaurant.get("id", "default")
    os.makedirs(f"data/results/{restaurant_id}", exist_ok=True)
    pd.DataFrame(competitors).to_csv(
        f"data/results/{restaurant_id}/competitors.csv", index=False
    )

    return report


def _generate_insights(your: dict, competitors: list, gap: float) -> list:
    insights = []

    if gap > 0.3:
        insights.append(f"✅ You're rated {gap} stars ABOVE the local average — maintain your strengths")
    elif gap < -0.3:
        insights.append(f"⚠️ You're rated {abs(gap)} stars BELOW the local average — action needed")
    else:
        insights.append("📊 Your rating is on par with local competitors")

    top_competitors = sorted(competitors, key=lambda x: x.get("rating", 0), reverse=True)
    if top_competitors:
        best = top_competitors[0]
        insights.append(
            f"🏆 Top competitor: {best['name']} ({best['rating']}⭐, {best['total_ratings']} reviews)"
        )

    if your.get("top_issues"):
        insights.append(
            f"🎯 Your weak spots: {', '.join(your['top_issues'])} — focus here to close the gap"
        )

    return insights


def _generate_actions(your: dict, gap: float) -> list:
    actions = []
    if gap < 0:
        actions.append("Identify what top-rated competitors do differently in your weak categories")
        actions.append("Consider mystery shopping at your top 2 competitors this week")
    if "service" in your.get("top_issues", []):
        actions.append("Service improvements have highest ROI on rating — prioritize staff training")
    if "price" in your.get("top_issues", []):
        actions.append("Review your price-to-portion ratio vs competitors in the same price tier")
    return actions
