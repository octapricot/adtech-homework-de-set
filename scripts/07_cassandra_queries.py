# URPOSE: Executes the 5 CQL queries and save results as JSON

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra import ConsistencyLevel
from cassandra.policies import RetryPolicy
from dotenv import load_dotenv
import os
import json
from datetime import date, datetime
from collections import defaultdict

load_dotenv()

CASSANDRA_HOST     = os.getenv("CASSANDRA_HOST", "127.0.0.1")
CASSANDRA_PORT     = int(os.getenv("CASSANDRA_PORT", 9042))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "adtech")

DATE_FROM = date(2024, 10, 1)
DATE_TO   = date(2024, 10, 31)
REGION    = "USA"


def get_session():
    profile = ExecutionProfile(
        request_timeout=300,
        retry_policy=RetryPolicy(),
        consistency_level=ConsistencyLevel.LOCAL_ONE
    )
    cluster = Cluster(
        [CASSANDRA_HOST],
        port=CASSANDRA_PORT,
        execution_profiles={EXEC_PROFILE_DEFAULT: profile}
    )
    return cluster.connect(CASSANDRA_KEYSPACE)


def query1_ctr_per_campaign(session):
    """
    CTR per campaign per day
    """
    rows = session.execute("""
        SELECT campaign_id, campaign_name, advertiser_name,
               event_date, impressions, clicks
        FROM campaign_performance_by_day
        WHERE campaign_id = 191
          AND event_date >= %s
          AND event_date <= %s
    """, (DATE_FROM, DATE_TO))

    results = []
    for row in rows:
        ctr = round(row.clicks / row.impressions * 100, 2) \
              if row.impressions > 0 else 0
        results.append({
            "campaign_id":   row.campaign_id,
            "campaign_name": row.campaign_name,
            "event_date":    str(row.event_date),
            "impressions":   row.impressions,
            "clicks":        row.clicks,
            "ctr_percent":   ctr
        })
    return results


def query2_top_advertisers_by_spend(session):
    """
    Top 5 advertisers by total spend in the 30-day window.
    """
    rows = session.execute("""
        SELECT advertiser_id, advertiser_name,
               event_date, total_spend
        FROM advertiser_spend
        WHERE event_date >= %s
          AND event_date <= %s
        ALLOW FILTERING
    """, (DATE_FROM, DATE_TO))

    totals = defaultdict(lambda: {"advertiser_name": "", "total_spend": 0.0})
    for row in rows:
        totals[row.advertiser_id]["advertiser_name"] = row.advertiser_name
        totals[row.advertiser_id]["total_spend"]    += float(row.total_spend)

    ranked = sorted(
        totals.items(),
        key=lambda x: x[1]["total_spend"],
        reverse=True
    )[:5]

    return [
        {
            "advertiser_id":   adv_id,
            "advertiser_name": v["advertiser_name"],
            "total_spend":     round(v["total_spend"], 2)
        }
        for adv_id, v in ranked
    ]


def query3_last_10_ads_for_user(session):
    """
    Last 10 ads seen by user 59472.
    """
    rows = session.execute("""
        SELECT user_id, timestamp, campaign_name,
               advertiser_name, device, was_clicked, ad_revenue
        FROM user_ad_history
        WHERE user_id = 59472
        LIMIT 10
    """)

    return [
        {
            "user_id":         row.user_id,
            "timestamp":       str(row.timestamp),
            "campaign_name":   row.campaign_name,
            "advertiser_name": row.advertiser_name,
            "device":          row.device,
            "was_clicked":     row.was_clicked,
            "ad_revenue":      float(row.ad_revenue)
        }
        for row in rows
    ]


def query4_top_10_active_users(session):
    """
    Top 10 users by click count in the 30-day window.
    """
    rows = session.execute("""
        SELECT event_date, user_id, click_count
        FROM user_click_counts
        WHERE event_date >= %s
          AND event_date <= %s
        ALLOW FILTERING
    """, (DATE_FROM, DATE_TO))

    user_clicks = defaultdict(int)
    for row in rows:
        user_clicks[row.user_id] += row.click_count

    ranked = sorted(
        user_clicks.items(),
        key=lambda x: x[1],
        reverse=True
    )[:10]

    return [
        {"user_id": uid, "total_clicks": clicks}
        for uid, clicks in ranked
    ]


def query5_top_advertisers_by_region(session):
    """
    Top 5 advertisers by spend in USA over 30 days.
    """
    rows = session.execute("""
        SELECT location, event_date, advertiser_id,
               advertiser_name, total_spend
        FROM advertiser_spend_by_region
        WHERE location = %s
          AND event_date >= %s
          AND event_date <= %s
        ALLOW FILTERING
    """, (REGION, DATE_FROM, DATE_TO))

    totals = defaultdict(lambda: {"advertiser_name": "", "total_spend": 0.0})
    for row in rows:
        totals[row.advertiser_id]["advertiser_name"] = row.advertiser_name
        totals[row.advertiser_id]["total_spend"]    += float(row.total_spend)

    ranked = sorted(
        totals.items(),
        key=lambda x: x[1]["total_spend"],
        reverse=True
    )[:5]

    return [
        {
            "location":        REGION,
            "advertiser_id":   adv_id,
            "advertiser_name": v["advertiser_name"],
            "total_spend":     round(v["total_spend"], 2)
        }
        for adv_id, v in ranked
    ]


def main():
    print("Connecting to Cassandra ...")
    session = get_session()
    print("Connected\n")

    queries = {
        "ctr_per_campaign_per_day": {
            "description": "CTR for Campaign 191 per day in October 2024",
            "fn": query1_ctr_per_campaign
        },
        "top_5_advertisers_by_spend": {
            "description": "Top 5 advertisers by total spend Oct 2024",
            "fn": query2_top_advertisers_by_spend
        },
        "last_10_ads_for_user": {
            "description": "Last 10 ads seen by user 59472",
            "fn": query3_last_10_ads_for_user
        },
        "top_10_active_users": {
            "description": "Top 10 users by click count in Oct 2024",
            "fn": query4_top_10_active_users
        },
        "top_5_advertisers_by_region": {
            "description": f"Top 5 advertisers by spend in {REGION} Oct 2024",
            "fn": query5_top_advertisers_by_region
        }
    }

    report = {
        "generated_at": datetime.now().isoformat(),
        "period": {"from": str(DATE_FROM), "to": str(DATE_TO)},
        "results": {}
    }

    for key, q in queries.items():
        print(f"Running: {q['description']} ...")
        rows = q["fn"](session)
        report["results"][key] = {
            "description": q["description"],
            "row_count":   len(rows),
            "data":        rows
        }
        print(f"  {len(rows)} result(s) returned")

    os.makedirs("reports", exist_ok=True)
    output_path = "reports/cassandra_report.json"
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"\nReport saved to {output_path}")


if __name__ == "__main__":
    main()