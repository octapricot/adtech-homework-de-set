# PURPOSE: Execute all 5 MongoDB queries and save results as JSON

from pymongo import MongoClient
import json
from datetime import datetime
import os

MONGO_URI = "mongodb://root:adtech123@127.0.0.1:27017/adtech?authSource=admin"
MONGO_DB  = "adtech"
MONGO_COL = "user_engagement"

SAMPLE_USER_ID = 59472

SAMPLE_ADVERTISER = "Advertiser_19"
DATE_FROM = "2024-10-01T00:00:00"
DATE_TO   = "2024-10-01T23:59:59"


def query1_user_interactions(col):
    """All ad interactions for a specific user."""
    doc = col.find_one(
        { "user_id": SAMPLE_USER_ID },
        { "user_id": 1, "age": 1, "gender": 1,
          "location": 1, "interests": 1, "sessions": 1 }
    )
    if not doc:
        return []
    doc.pop("_id", None)
    return [doc]


def query2_last_5_sessions(col):
    """User's last 5 ad sessions."""
    doc = col.find_one(
        { "user_id": SAMPLE_USER_ID },
        { "user_id": 1, "sessions": { "$slice": -5 } }
    )
    if not doc:
        return []
    doc.pop("_id", None)
    return [doc]


def query3_clicks_per_hour(col):
    """Clicks per hour per campaign in 24h window for an advertiser."""
    pipeline = [
        { "$match": {
            "sessions.impressions.advertiser_name": SAMPLE_ADVERTISER,
            "sessions.impressions.timestamp": {
                "$gte": DATE_FROM,
                "$lte": DATE_TO
            }
        }},
        { "$unwind": "$sessions" },
        { "$unwind": "$sessions.impressions" },
        { "$match": {
            "sessions.impressions.advertiser_name": SAMPLE_ADVERTISER,
            "sessions.impressions.was_clicked": True,
            "sessions.impressions.timestamp": {
                "$gte": DATE_FROM,
                "$lte": DATE_TO
            }
        }},
        { "$group": {
            "_id": {
                "campaign": "$sessions.impressions.campaign_name",
                "hour":     { "$substr": ["$sessions.impressions.timestamp", 0, 13] }
            },
            "clicks": { "$sum": 1 }
        }},
        { "$sort": { "_id.hour": 1, "_id.campaign": 1 } }
    ]
    results = list(col.aggregate(pipeline))
    return [
        {
            "campaign": r["_id"]["campaign"],
            "hour":     r["_id"]["hour"],
            "clicks":   r["clicks"]
        }
        for r in results
    ]


def query4_ad_fatigue(col):
    """Users who saw the same ad 5+ times but never clicked."""
    pipeline = [
        { "$unwind": "$sessions" },
        { "$unwind": "$sessions.impressions" },
        { "$group": {
            "_id": {
                "user_id":       "$user_id",
                "campaign_name": "$sessions.impressions.campaign_name"
            },
            "impression_count": { "$sum": 1 },
            "click_count": { "$sum": {
                "$cond": ["$sessions.impressions.was_clicked", 1, 0]
            }}
        }},
        { "$match": {
            "impression_count": { "$gte": 5 },
            "click_count": 0
        }},
        { "$group": {
            "_id":               "$_id.user_id",
            "fatigued_campaigns": { "$push": "$_id.campaign_name" },
            "total_fatigued":    { "$sum": 1 }
        }},
        { "$sort": { "total_fatigued": -1 } },
        { "$limit": 10 }
    ]
    results = list(col.aggregate(pipeline))
    return [
        {
            "user_id":            r["_id"],
            "fatigued_campaigns": r["fatigued_campaigns"],
            "total_fatigued":     r["total_fatigued"]
        }
        for r in results
    ]


def query5_top_campaigns(col):
    """Top 3 most engaged campaigns for a user."""
    pipeline = [
        { "$match": { "user_id": SAMPLE_USER_ID } },
        { "$unwind": "$sessions" },
        { "$unwind": "$sessions.impressions" },
        { "$match": { "sessions.impressions.was_clicked": True } },
        { "$group": {
            "_id":    "$sessions.impressions.campaign_name",
            "clicks": { "$sum": 1 }
        }},
        { "$sort": { "clicks": -1 } },
        { "$limit": 3 },
        { "$project": {
            "campaign": "$_id",
            "clicks":   1,
            "_id":      0
        }}
    ]
    return list(col.aggregate(pipeline))


def main():
    print("Connecting to MongoDB ...")
    client = MongoClient(MONGO_URI)
    col    = client[MONGO_DB][MONGO_COL]
    print("Connected\n")

    queries = {
        "user_ad_interactions": {
            "description": f"All ad interactions for user {SAMPLE_USER_ID}",
            "fn": query1_user_interactions
        },
        "last_5_sessions": {
            "description": f"Last 5 sessions for user {SAMPLE_USER_ID}",
            "fn": query2_last_5_sessions
        },
        "clicks_per_hour": {
            "description": f"Clicks per hour for {SAMPLE_ADVERTISER} on {DATE_FROM[:10]}",
            "fn": query3_clicks_per_hour
        },
        "ad_fatigue_users": {
            "description": "Users with 5+ impressions on same ad but 0 clicks",
            "fn": query4_ad_fatigue
        },
        "top_engaged_campaigns": {
            "description": f"Top 3 most engaged campaigns for user {SAMPLE_USER_ID}",
            "fn": query5_top_campaigns
        }
    }

    report = {
        "generated_at": datetime.now().isoformat(),
        "results": {}
    }

    for key, q in queries.items():
        print(f"Running: {q['description']} ...")
        rows = q["fn"](col)
        report["results"][key] = {
            "description": q["description"],
            "row_count":   len(rows),
            "data":        rows
        }
        print(f"  {len(rows)} result(s) returned")

    client.close()

    os.makedirs("reports", exist_ok=True)
    output_path = "reports/mongo_report.json"
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"\nReport saved to {output_path}")


if __name__ == "__main__":
    main()