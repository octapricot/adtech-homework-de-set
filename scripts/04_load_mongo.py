# PURPOSE: Reads CSV files and load user engagement data into MongoDB as nested documents.
# Uses chunked reading to avoid memory issues on my low-RAM Mac :( 

import pandas as pd
from pymongo import MongoClient, ASCENDING
from dotenv import load_dotenv
import os

load_dotenv()

MONGO_URI = "mongodb://root:adtech123@127.0.0.1:27017/adtech?authSource=admin"
MONGO_DB  = "adtech"
MONGO_COL = "user_engagement"

USERS_PATH  = "data/users.csv"
EVENTS_PATH = "data/ad_events.csv"
CAMPS_PATH  = "data/campaigns.csv"

CHUNK_SIZE = 100_000

BATCH_SIZE = 500

def parse_interests(raw):
    raw = str(raw).strip("[]").replace("'", "").replace('"', "")
    return [i.strip() for i in raw.split(",") if i.strip()]

def make_session_key(device, timestamp):
    """ Session = same device + same calendar day (e.g. "Mobile_2024-10-01") """
    day = str(timestamp)[:10]
    return f"{device}_{day}"


def build_impression(event, camp_map):
    """Builds a single impression object from one CSV row."""
    was_clicked = bool(event["WasClicked"])
    click_ts    = event["ClickTimestamp"]
    camp        = camp_map.get(event["CampaignName"], {})

    return {
        "event_id":           event["EventID"],
        "campaign_id":        camp.get("campaign_id"),
        "campaign_name":      event["CampaignName"],
        "advertiser_name":    event["AdvertiserName"],
        "ad_slot_size":       event["AdSlotSize"],
        "targeting_criteria": (
            f"{event['CampaignTargetingCriteria']} | "
            f"{event['CampaignTargetingInterest']} | "
            f"{event['CampaignTargetingCountry']}"
        ),
        "timestamp":          event["Timestamp"],
        "bid_amount":         float(event["BidAmount"]),
        "ad_cost":            float(event["AdCost"]),
        "ad_revenue":         float(event["AdRevenue"]),
        "was_clicked":        was_clicked,
        "click": {
            "click_timestamp": click_ts,
            "ad_revenue":      float(event["AdRevenue"])
        } if was_clicked and pd.notna(click_ts) else None
    }

def process_chunk(chunk, user_docs, user_map, camp_map):
    """ Processes one chunk of ad_events rows. """
    for _, event in chunk.iterrows():
        user_id = int(event["UserID"])

        if user_id not in user_map:
            continue

        if user_id not in user_docs:
            user_docs[user_id] = user_map[user_id].copy()
            user_docs[user_id]["sessions"] = {}

        key = make_session_key(event["Device"], event["Timestamp"])
        if key not in user_docs[user_id]["sessions"]:
            user_docs[user_id]["sessions"][key] = {
                "session_id":    f"{user_id}_{key}",
                "device":        event["Device"],
                "session_start": event["Timestamp"],
                "session_end":   event["Timestamp"],
                "impressions":   []
            }

        user_docs[user_id]["sessions"][key]["session_end"] = event["Timestamp"]

        impression = build_impression(event, camp_map)
        user_docs[user_id]["sessions"][key]["impressions"].append(impression)

def finalize_documents(user_docs):
    """ Converts sessions from dict to list for MongoDB storage. """
    final = []
    for user_id, doc in user_docs.items():
        doc = doc.copy()
        doc["sessions"] = list(doc["sessions"].values())
        final.append(doc)
    return final

def load_to_mongo(documents):
    client = MongoClient(MONGO_URI)
    db     = client[MONGO_DB]
    col    = db[MONGO_COL]

    col.drop()
    print("Inserting documents into MongoDB ...")

    total = len(documents)
    for i in range(0, total, BATCH_SIZE):
        batch = documents[i: i + BATCH_SIZE]
        col.insert_many(batch)
        print(f"  Inserted {min(i + BATCH_SIZE, total):,} / {total:,}", end="\r")

    print(f"\nAll documents inserted")

    print("Creating indexes ...")
    col.create_index("user_id", unique=True)
    col.create_index("location")
    col.create_index("interests")
    col.create_index([("sessions.impressions.campaign_name", ASCENDING)])
    col.create_index([("sessions.impressions.was_clicked",  ASCENDING)])
    col.create_index([("sessions.impressions.timestamp",    ASCENDING)])
    print("Indexes created")

    client.close()


def main():
    print("Reading users and campaigns ...")
    df_users = pd.read_csv(USERS_PATH)
    df_camps = pd.read_csv(CAMPS_PATH)
    print(f"  users.csv     : {len(df_users):,} rows")
    print(f"  campaigns.csv : {len(df_camps):,} rows")

    user_map = {
        int(row["UserID"]): {
            "user_id":     int(row["UserID"]),
            "age":         int(row["Age"]),
            "gender":      row["Gender"],
            "location":    row["Location"],
            "interests":   parse_interests(row["Interests"]),
            "signup_date": str(row["SignupDate"])
        }
        for _, row in df_users.iterrows()
    }

    camp_map = {
        row["CampaignName"]: {
            "campaign_id":     int(row["CampaignID"]),
            "campaign_name":   row["CampaignName"],
            "advertiser_name": row["AdvertiserName"]
        }
        for _, row in df_camps.iterrows()
    }

    user_docs = {}

    print(f"\nProcessing ad_events.csv in chunks of {CHUNK_SIZE:,}...")
    chunk_num  = 0
    total_rows = 0

    for chunk in pd.read_csv(EVENTS_PATH, chunksize=CHUNK_SIZE):
        chunk_num  += 1
        total_rows += len(chunk)
        print(f"  Chunk {chunk_num}: {total_rows:,} rows processed, "
              f"{len(user_docs):,} users seen so far", end="\r")
        process_chunk(chunk, user_docs, user_map, camp_map)

    print(f"\nFinished processing {total_rows:,} rows")
    print(f"Built {len(user_docs):,} user documents")

    print("\nFinalizing documents ...")
    documents = finalize_documents(user_docs)

    print("Loading into MongoDB ...")
    load_to_mongo(documents)

    print("\nMongoDB loading complete!")

if __name__ == "__main__":
    main()