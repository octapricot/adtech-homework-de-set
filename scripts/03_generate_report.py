# PURPOSE: "Execute all 7 analysis queries and save results as a structured JSON report."

import mysql.connector
import json
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME")
}

DATE_FROM = "2024-10-01"
DATE_TO   = "2024-10-31 23:59:59"

QUERIES = {
    "top_5_campaigns_by_ctr": {
        "description": "Top 5 campaigns with the highest CTR over the 30-day period",
        "sql": """
            SELECT
                c.campaign_name,
                a.advertiser_name,
                e.impressions,
                e.clicks,
                ROUND(e.clicks / e.impressions * 100, 2) AS ctr_percent
            FROM (
                SELECT campaign_id,
                    COUNT(event_id)  AS impressions,
                    SUM(was_clicked) AS clicks
                FROM ad_events
                WHERE timestamp BETWEEN %s AND %s
                GROUP BY campaign_id
            ) e
            JOIN campaigns c ON e.campaign_id = c.campaign_id
            JOIN advertisers a ON c.advertiser_id = a.advertiser_id
            ORDER BY ctr_percent DESC
            LIMIT 5
        """
    },
    "top_advertisers_by_spend": {
        "description": "Advertisers ranked by total ad spend in the 30-day period",
        "sql": """
            SELECT
                a.advertiser_name,
                SUM(e.impressions)           AS total_impressions,
                ROUND(SUM(e.total_spend), 2) AS total_spend
            FROM (
                SELECT campaign_id,
                    COUNT(event_id) AS impressions,
                    SUM(ad_cost)    AS total_spend
                FROM ad_events
                WHERE timestamp BETWEEN %s AND %s
                GROUP BY campaign_id
            ) e
            JOIN campaigns c ON e.campaign_id = c.campaign_id
            JOIN advertisers a ON c.advertiser_id = a.advertiser_id
            GROUP BY a.advertiser_id, a.advertiser_name
            ORDER BY total_spend DESC
            LIMIT 10
        """
    },
    "cpc_and_cpm_per_campaign": {
        "description": "Average CPC and CPM for each campaign",
        "sql": """
            SELECT
                c.campaign_name,
                a.advertiser_name,
                e.impressions,
                e.clicks,
                ROUND(e.total_spend, 2)                               AS total_spend,
                ROUND(e.total_spend / NULLIF(e.clicks, 0), 2)         AS cpc,
                ROUND(e.total_spend / e.impressions * 1000, 2)        AS cpm
            FROM (
                SELECT campaign_id,
                    COUNT(event_id)  AS impressions,
                    SUM(was_clicked) AS clicks,
                    SUM(ad_cost)     AS total_spend
                FROM ad_events
                WHERE timestamp BETWEEN %s AND %s
                GROUP BY campaign_id
            ) e
            JOIN campaigns c ON e.campaign_id = c.campaign_id
            JOIN advertisers a ON c.advertiser_id = a.advertiser_id
            ORDER BY total_spend DESC
        """
    },
    "top_locations_by_revenue": {
        "description": "Top locations ranked by total ad revenue",
        "sql": """
            SELECT
                location,
                COUNT(event_id)           AS total_impressions,
                SUM(was_clicked)          AS total_clicks,
                ROUND(SUM(ad_revenue), 2) AS total_revenue
            FROM ad_events
            WHERE timestamp BETWEEN %s AND %s
            GROUP BY location
            ORDER BY total_revenue DESC
        """
    },
    "top_10_engaged_users": {
        "description": "Top 10 users with the most ad clicks",
        "sql": """
            SELECT
                u.user_id,
                u.age,
                u.gender,
                u.location,
                e.impressions AS total_impressions,
                e.clicks      AS total_clicks
            FROM (
                SELECT user_id,
                    COUNT(event_id)  AS impressions,
                    SUM(was_clicked) AS clicks
                FROM ad_events
                WHERE timestamp BETWEEN %s AND %s
                GROUP BY user_id
            ) e
            JOIN users u ON e.user_id = u.user_id
            ORDER BY total_clicks DESC
            LIMIT 10
        """
    },
    "campaigns_near_budget_limit": {
        "description": "Campaigns that have spent more than 80% of their budget",
        "sql": """
            SELECT
                c.campaign_name,
                a.advertiser_name,
                ROUND(c.budget, 2)                                         AS total_budget,
                ROUND(c.budget - c.remaining_budget, 2)                    AS amount_spent,
                ROUND((c.budget - c.remaining_budget) / c.budget * 100, 2) AS budget_used_percent
            FROM campaigns c
            JOIN advertisers a ON c.advertiser_id = a.advertiser_id
            WHERE (c.budget - c.remaining_budget) / c.budget >= 0.80
            ORDER BY budget_used_percent DESC
        """
    },
    "ctr_by_device": {
        "description": "CTR comparison across device types",
        "sql": """
            SELECT
                device,
                COUNT(event_id)                                       AS impressions,
                SUM(was_clicked)                                      AS clicks,
                ROUND(SUM(was_clicked) / COUNT(event_id) * 100, 2)    AS ctr_percent
            FROM ad_events
            WHERE timestamp BETWEEN %s AND %s
            GROUP BY device
            ORDER BY ctr_percent DESC
        """
    }
}

def run_query(cursor, sql, params=None):
    """
    Executes a query and returns results as a list of dictionaries.
    """
    cursor.execute(sql, params or [])
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    return [dict(zip(columns, row)) for row in rows]

def main():
    print("Connecting to MySQL ...")
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print("Connected\n")

    report = {
        "generated_at": datetime.now().isoformat(),
        "period": {"from": DATE_FROM, "to": DATE_TO},
        "results": {}
    }

    for key, query in QUERIES.items():
        print(f"Running: {query['description']} ...")

        needs_dates = "%s" in query["sql"]
        params = [DATE_FROM, DATE_TO] if needs_dates else None

        rows = run_query(cursor, query["sql"], params)

        report["results"][key] = {
            "description": query["description"],
            "row_count": len(rows),
            "data": rows
        }
        print(f"  {len(rows)} rows returned")

    cursor.close()
    conn.close()

    output_path = "reports/adtech_report.json"
    os.makedirs("reports", exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"\n Report saved to {output_path}")

if __name__ == "__main__":
    main()