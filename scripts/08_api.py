# PURPOSE: FastAPI REST API with Redis caching for AdTech analytics 

import json
import mysql.connector
from pymongo import MongoClient
from fastapi import FastAPI, HTTPException
import redis
from dotenv import load_dotenv
import os

load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("DB_HOST"),
    "port":     int(os.getenv("DB_PORT")),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME")
}

MONGO_URI  = os.getenv("MONGO_URI")
REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

TTL_CAMPAIGN    = 30
TTL_ADVERTISER  = 300
TTL_USER        = 120

# App and connections
app = FastAPI(
    title="AdTech Analytics API",
    description="REST API for AdTech performance metrics with Redis caching",
    version="1.0.0"
)

# Redis client 
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True
)

def get_mysql():
    """Create a new MySQL connection."""
    return mysql.connector.connect(**DB_CONFIG)

def get_mongo():
    """Create a new MongoDB connection."""
    return MongoClient(MONGO_URI)

def get_cached(key: str):
    """
    Try to get a value from Redis.
    Returns parsed dict if found, None if not cached.
    """
    value = redis_client.get(key)
    if value:
        return json.loads(value)
    return None

def set_cached(key: str, data: dict, ttl: int):
    """Store a value in Redis with a TTL (expiry time)."""
    redis_client.setex(key, ttl, json.dumps(data))


# ENDPOINT 1: Campaign Performance
@app.get("/campaign/{campaign_id}/performance")
def campaign_performance(campaign_id: int):
    cache_key = f"campaign:{campaign_id}:performance"

    cached = get_cached(cache_key)
    if cached:
        cached["cache"] = "HIT"
        return cached

    conn   = get_mysql()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT
            c.campaign_id,
            c.campaign_name,
            a.advertiser_name,
            COUNT(e.event_id)                                        AS impressions,
            SUM(e.was_clicked)                                       AS clicks,
            ROUND(SUM(e.was_clicked) / COUNT(e.event_id) * 100, 2)   AS ctr_percent,
            ROUND(SUM(e.ad_cost), 2)                                 AS total_spend
        FROM ad_events e
        JOIN campaigns c ON e.campaign_id = c.campaign_id
        JOIN advertisers a ON c.advertiser_id = a.advertiser_id
        WHERE e.campaign_id = %s
        GROUP BY c.campaign_id, c.campaign_name, a.advertiser_name
    """, (campaign_id,))

    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Campaign not found")

    result = {
        "campaign_id":     row["campaign_id"],
        "campaign_name":   row["campaign_name"],
        "advertiser_name": row["advertiser_name"],
        "impressions":     row["impressions"],
        "clicks":          int(row["clicks"]),
        "ctr_percent":     float(row["ctr_percent"]),
        "total_spend":     float(row["total_spend"]),
        "cache":           "MISS"
    }

    set_cached(cache_key, result, TTL_CAMPAIGN)
    return result


# ENDPOINT 2: Advertiser Spending
@app.get("/advertiser/{advertiser_id}/spending")
def advertiser_spending(advertiser_id: int):
    cache_key = f"advertiser:{advertiser_id}:spending"

    cached = get_cached(cache_key)
    if cached:
        cached["cache"] = "HIT"
        return cached

    conn   = get_mysql()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT
            a.advertiser_id,
            a.advertiser_name,
            COUNT(e.event_id)        AS total_impressions,
            SUM(e.was_clicked)       AS total_clicks,
            ROUND(SUM(e.ad_cost), 2) AS total_spend
        FROM ad_events e
        JOIN campaigns c ON e.campaign_id = c.campaign_id
        JOIN advertisers a ON c.advertiser_id = a.advertiser_id
        WHERE a.advertiser_id = %s
        GROUP BY a.advertiser_id, a.advertiser_name
    """, (advertiser_id,))

    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Advertiser not found")

    result = {
        "advertiser_id":    row["advertiser_id"],
        "advertiser_name":  row["advertiser_name"],
        "total_impressions": int(row["total_impressions"]),
        "total_clicks":     int(row["total_clicks"]),
        "total_spend":      float(row["total_spend"]),
        "cache":            "MISS"
    }

    set_cached(cache_key, result, TTL_ADVERTISER)
    return result


# ENDPOINT 3: User Engagements

@app.get("/user/{user_id}/engagements")
def user_engagements(user_id: int):
    cache_key = f"user:{user_id}:engagements"

    cached = get_cached(cache_key)
    if cached:
        cached["cache"] = "HIT"
        return cached

    client = get_mongo()
    col    = client["adtech"]["user_engagement"]

    doc = col.find_one(
        {"user_id": user_id},
        {"user_id": 1, "age": 1, "gender": 1,
         "location": 1, "interests": 1, "sessions": 1}
    )
    client.close()

    if not doc:
        raise HTTPException(status_code=404, detail="User not found")

    total_impressions = 0
    total_clicks      = 0
    recent_ads        = []

    for session in doc.get("sessions", []):
        for imp in session.get("impressions", []):
            total_impressions += 1
            if imp.get("was_clicked"):
                total_clicks += 1
            recent_ads.append({
                "campaign_name":   imp.get("campaign_name"),
                "advertiser_name": imp.get("advertiser_name"),
                "timestamp":       imp.get("timestamp"),
                "device":          session.get("device"),
                "was_clicked":     imp.get("was_clicked")
            })

    recent_ads = sorted(
        recent_ads,
        key=lambda x: x["timestamp"] or "",
        reverse=True
    )[:10]

    result = {
        "user_id":           user_id,
        "age":               doc.get("age"),
        "gender":            doc.get("gender"),
        "location":          doc.get("location"),
        "interests":         doc.get("interests"),
        "total_impressions": total_impressions,
        "total_clicks":      total_clicks,
        "recent_ads":        recent_ads,
        "cache":             "MISS"
    }

    set_cached(cache_key, result, TTL_USER)
    return result


@app.get("/health")
def health():
    return {"status": "ok"}