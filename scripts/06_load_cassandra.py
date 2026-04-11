# PURPOSE: Load all ad events into 5 Cassandra tables. Loads one table at a time, because my laptop's weak :)

import pandas as pd
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra import ConsistencyLevel
from cassandra.policies import RetryPolicy
from dotenv import load_dotenv
import os
from datetime import date

load_dotenv()

CASSANDRA_HOST     = os.getenv("CASSANDRA_HOST", "127.0.0.1")
CASSANDRA_PORT     = int(os.getenv("CASSANDRA_PORT", 9042))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "adtech")

EVENTS_PATH = "data/ad_events.csv"
CAMPS_PATH  = "data/campaigns.csv"

CHUNK_SIZE = 100_000


def get_session():
    profile = ExecutionProfile(
        request_timeout=600,
        retry_policy=RetryPolicy(),
        consistency_level=ConsistencyLevel.LOCAL_ONE
    )
    cluster = Cluster(
        [CASSANDRA_HOST],
        port=CASSANDRA_PORT,
        execution_profiles={EXEC_PROFILE_DEFAULT: profile}
    )
    session = cluster.connect(CASSANDRA_KEYSPACE)
    print("Connected to Cassandra")
    return session


def build_camp_map(df_camps):
    camp_map       = {}
    adv_name_to_id = {}
    adv_counter    = 1
    for _, row in df_camps.iterrows():
        adv_name = row["AdvertiserName"]
        if adv_name not in adv_name_to_id:
            adv_name_to_id[adv_name] = adv_counter
            adv_counter += 1
        camp_map[row["CampaignName"]] = {
            "campaign_id":   int(row["CampaignID"]),
            "advertiser_id": adv_name_to_id[adv_name]
        }
    return camp_map


# -------------------------------------------------------
# PASS 1: Load aggregated tables
# -------------------------------------------------------

def load_aggregated_tables(session, camp_map):
    """
    Single pass through all chunks building aggregates,
    then inserting into 4 tables at the end of each chunk.
    """
    stmt_camp = session.prepare("""
        INSERT INTO campaign_performance_by_day
            (campaign_id, event_date, campaign_name,
             advertiser_name, impressions, clicks, total_spend)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """)
    stmt_adv = session.prepare("""
        INSERT INTO advertiser_spend
            (event_date, advertiser_id, advertiser_name,
             total_spend, total_impressions)
        VALUES (?, ?, ?, ?, ?)
    """)
    stmt_clicks = session.prepare("""
        INSERT INTO user_click_counts
            (event_date, user_id, click_count, impression_count)
        VALUES (?, ?, ?, ?)
    """)
    stmt_region = session.prepare("""
        INSERT INTO advertiser_spend_by_region
            (location, event_date, advertiser_id,
             advertiser_name, total_spend)
        VALUES (?, ?, ?, ?, ?)
    """)

    chunk_num  = 0
    total_rows = 0

    for chunk in pd.read_csv(EVENTS_PATH, chunksize=CHUNK_SIZE):
        chunk_num  += 1
        total_rows += len(chunk)
        chunk["Timestamp"] = pd.to_datetime(chunk["Timestamp"])

        camp_perf   = {}
        adv_spend   = {}
        user_clicks = {}
        adv_region  = {}

        for _, row in chunk.iterrows():
            camp_name   = row["CampaignName"]
            camp        = camp_map.get(camp_name, {})
            camp_id     = camp.get("campaign_id", 0)
            adv_name    = row["AdvertiserName"]
            adv_id      = camp.get("advertiser_id", 0)
            event_date  = str(row["Timestamp"])[:10]
            was_clicked = bool(row["WasClicked"])
            location    = row["Location"]

            key = (camp_id, event_date)
            if key not in camp_perf:
                camp_perf[key] = {
                    "campaign_name":   camp_name,
                    "advertiser_name": adv_name,
                    "impressions": 0, "clicks": 0, "total_spend": 0.0
                }
            camp_perf[key]["impressions"] += 1
            camp_perf[key]["clicks"]      += int(was_clicked)
            camp_perf[key]["total_spend"] += float(row["AdCost"])

            key2 = (adv_id, event_date)
            if key2 not in adv_spend:
                adv_spend[key2] = {
                    "advertiser_name": adv_name,
                    "total_spend": 0.0, "total_impressions": 0
                }
            adv_spend[key2]["total_spend"]       += float(row["AdCost"])
            adv_spend[key2]["total_impressions"] += 1

            key3 = (int(row["UserID"]), event_date)
            if key3 not in user_clicks:
                user_clicks[key3] = {"clicks": 0, "impressions": 0}
            user_clicks[key3]["impressions"] += 1
            user_clicks[key3]["clicks"]      += int(was_clicked)

            key4 = (location, adv_id, event_date)
            if key4 not in adv_region:
                adv_region[key4] = {
                    "advertiser_name": adv_name,
                    "total_spend": 0.0
                }
            adv_region[key4]["total_spend"] += float(row["AdCost"])

        # Insert aggregated rows for this chunk
        for k, v in camp_perf.items():
            session.execute(stmt_camp, (
                k[0], date.fromisoformat(k[1]),
                v["campaign_name"], v["advertiser_name"],
                v["impressions"], v["clicks"],
                round(v["total_spend"], 2)
            ))

        for k, v in adv_spend.items():
            session.execute(stmt_adv, (
                date.fromisoformat(k[1]), k[0],
                v["advertiser_name"],
                round(v["total_spend"], 2),
                v["total_impressions"]
            ))

        for k, v in user_clicks.items():
            session.execute(stmt_clicks, (
                date.fromisoformat(k[1]), k[0],
                v["clicks"], v["impressions"]
            ))

        for k, v in adv_region.items():
            session.execute(stmt_region, (
                k[0], date.fromisoformat(k[2]), k[1],
                v["advertiser_name"],
                round(v["total_spend"], 2)
            ))

        print(f"  Chunk {chunk_num}: {total_rows:,} rows processed", end="\r")

    print(f"\nAggregated tables loaded ({total_rows:,} rows processed)")


# -------------------------------------------------------
# PASS 2: Load user_ad_history
# -------------------------------------------------------

def load_user_history(session, camp_map):
    stmt = session.prepare("""
        INSERT INTO user_ad_history
            (user_id, timestamp, event_id, campaign_name,
             advertiser_name, device, was_clicked, ad_revenue)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """)

    chunk_num  = 0
    total_rows = 0

    for chunk in pd.read_csv(EVENTS_PATH, chunksize=CHUNK_SIZE):
        chunk_num  += 1
        total_rows += len(chunk)
        chunk["Timestamp"] = pd.to_datetime(chunk["Timestamp"])

        for _, row in chunk.iterrows():
            session.execute(stmt, (
                int(row["UserID"]),
                row["Timestamp"].to_pydatetime(),
                row["EventID"],
                row["CampaignName"],
                row["AdvertiserName"],
                row["Device"],
                bool(row["WasClicked"]),
                float(row["AdRevenue"])
            ))

        print(f"  Chunk {chunk_num}: {total_rows:,} rows processed", end="\r")

    print(f"\nuser_ad_history loaded ({total_rows:,} rows processed)")


def main():
    print("Reading campaigns ...")
    df_camps = pd.read_csv(CAMPS_PATH)
    camp_map = build_camp_map(df_camps)

    session = get_session()

    print("\n--- PASS 1: Loading aggregated tables ---")
    load_aggregated_tables(session, camp_map)

    print("\n--- PASS 2: Loading user_ad_history ---")
    load_user_history(session, camp_map)

    print("\nCassandra loading complete!")


if __name__ == "__main__":
    main()