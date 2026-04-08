# PURPOSE: Read the three CSV files and load them into the normalized MySQL schema. Uses bulk inserts for performance

import pandas as pd
import mysql.connector
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

CAMPAIGNS_PATH = "data/campaigns.csv"
USERS_PATH     = "data/users.csv"
EVENTS_PATH    = "data/ad_events.csv"

BATCH_SIZE = 5000

def get_connection():
    conn = mysql.connector.connect(**DB_CONFIG)
    print("Connected to MySQL")
    return conn


def insert_in_batches(cursor, conn, query, rows):
    """
    Helper function that takes a list of rows and inserts them in chunks of BATCH_SIZE.
    """
    total = len(rows)
    for i in range(0, total, BATCH_SIZE):
        batch = rows[i: i + BATCH_SIZE]
        cursor.executemany(query, batch)
        conn.commit()
        print(f"  Inserted {min(i + BATCH_SIZE, total):,} / {total:,} rows", end="\r")
    print()  


def load_campaigns(conn, df_campaigns):
    cursor = conn.cursor()

    unique_advertisers = df_campaigns["AdvertiserName"].unique()
    print(f"Inserting {len(unique_advertisers)} advertisers ...")

    advertiser_rows = [(name,) for name in unique_advertisers]
    insert_in_batches(
        cursor, conn,
        "INSERT IGNORE INTO advertisers (advertiser_name) VALUES (%s)",
        advertiser_rows
    )

    cursor.execute("SELECT advertiser_id, advertiser_name FROM advertisers")
    advertiser_map = {name: aid for aid, name in cursor.fetchall()}

    print(f"Inserting {len(df_campaigns)} campaigns ...")

    campaign_rows = [
        (
            int(row["CampaignID"]),
            advertiser_map[row["AdvertiserName"]],
            row["CampaignName"],
            row["CampaignStartDate"],
            row["CampaignEndDate"],
            float(row["Budget"]),
            float(row["RemainingBudget"])
        )
        for _, row in df_campaigns.iterrows()
    ]

    insert_in_batches(cursor, conn, """
        INSERT IGNORE INTO campaigns
            (campaign_id, advertiser_id, campaign_name,
             start_date, end_date, budget, remaining_budget)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, campaign_rows)

    print("Advertisers and campaigns loaded")
    cursor.close()


def load_users(conn, df_users):
    cursor = conn.cursor()
    print(f"Inserting {len(df_users)} users ...")

    user_rows = []
    for _, row in df_users.iterrows():
        interests = str(row["Interests"])
        interests = interests.strip("[]").replace("'", "").replace('"', "")
        user_rows.append((
            int(row["UserID"]),
            int(row["Age"]),
            row["Gender"],
            row["Location"],
            interests,
            row["SignupDate"]
        ))

    insert_in_batches(cursor, conn, """
        INSERT IGNORE INTO users
            (user_id, age, gender, location, interests, signup_date)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, user_rows)

    print("Users loaded")
    cursor.close()


def load_ad_events(conn, df_events, df_campaigns):
    cursor = conn.cursor()

    campaign_map = {
        (row["AdvertiserName"], row["CampaignName"]): int(row["CampaignID"])
        for _, row in df_campaigns.iterrows()
    }

    print(f"Preparing {len(df_events):,} ad events ...")

    event_rows = []
    click_rows = []
    skipped = 0

    for _, row in df_events.iterrows():
        key = (row["AdvertiserName"], row["CampaignName"])
        campaign_id = campaign_map.get(key)

        if campaign_id is None:
            skipped += 1
            continue

        targeting = " | ".join([
            str(row["CampaignTargetingCriteria"]),
            str(row["CampaignTargetingInterest"]),
            str(row["CampaignTargetingCountry"])
        ])

        was_clicked = bool(row["WasClicked"])

        event_rows.append((
            row["EventID"],
            campaign_id,
            int(row["UserID"]),
            targeting,
            row["AdSlotSize"],
            row["Device"],
            row["Location"],
            row["Timestamp"],
            float(row["BidAmount"]),
            float(row["AdCost"]),
            float(row["AdRevenue"]),
            was_clicked
        ))

        if was_clicked and pd.notna(row["ClickTimestamp"]):
            click_rows.append((
                row["EventID"],
                row["ClickTimestamp"],
                float(row["AdRevenue"])
            ))

    if skipped > 0:
        print(f"Skipped {skipped:,} events with no matching campaign")

    print(f"Inserting {len(event_rows):,} ad events in batches ...")
    insert_in_batches(cursor, conn, """
        INSERT IGNORE INTO ad_events
            (event_id, campaign_id, user_id, targeting_criteria,
             ad_slot_size, device, location, timestamp,
             bid_amount, ad_cost, ad_revenue, was_clicked)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, event_rows)

    print(f"Inserting {len(click_rows):,} clicks in batches ...")
    insert_in_batches(cursor, conn, """
        INSERT IGNORE INTO clicks
            (event_id, click_timestamp, ad_revenue)
        VALUES (%s, %s, %s)
    """, click_rows)

    print("Ad events and clicks loaded")
    cursor.close()


def main():
    print("Reading CSV files ...")
    df_campaigns = pd.read_csv(CAMPAIGNS_PATH)
    df_users     = pd.read_csv(USERS_PATH)
    df_events    = pd.read_csv(EVENTS_PATH)

    print(f"  campaigns.csv : {len(df_campaigns):,} rows")
    print(f"  users.csv     : {len(df_users):,} rows")
    print(f"  ad_events.csv : {len(df_events):,} rows")

    conn = get_connection()

    load_campaigns(conn, df_campaigns)
    load_users(conn, df_users)
    load_ad_events(conn, df_events, df_campaigns)

    conn.close()
    print("\nAll data loaded successfully!")


if __name__ == "__main__":
    main()