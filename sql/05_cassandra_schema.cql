-- PURPOSE: Creates Cassandra keyspace and tables for AdTech

CREATE KEYSPACE IF NOT EXISTS adtech
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE adtech;

-- -------------------------------------------------------
-- TABLE 1: campaign_performance_by_day
-- QUERY: CTR per campaign per day
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS campaign_performance_by_day (
    campaign_id      INT,
    event_date       DATE,
    campaign_name    TEXT,
    advertiser_name  TEXT,
    impressions      INT,
    clicks           INT,
    total_spend      DECIMAL,
    PRIMARY KEY (campaign_id, event_date)
) WITH CLUSTERING ORDER BY (event_date DESC);

-- -------------------------------------------------------
-- TABLE 2: advertiser_spend
-- QUERY: Top advertisers by total spend in last 30 days
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS advertiser_spend (
    event_date        DATE,
    advertiser_id     INT,
    advertiser_name   TEXT,
    total_spend       DECIMAL,
    total_impressions INT,
    PRIMARY KEY (event_date, advertiser_id)
);

-- -------------------------------------------------------
-- TABLE 3: user_ad_history
-- QUERY: Last 10 ads a user saw
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_ad_history (
    user_id         BIGINT,
    timestamp       TIMESTAMP,
    event_id        TEXT,
    campaign_name   TEXT,
    advertiser_name TEXT,
    device          TEXT,
    was_clicked     BOOLEAN,
    ad_revenue      DECIMAL,
    PRIMARY KEY (user_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

-- -------------------------------------------------------
-- TABLE 4: user_click_counts
-- QUERY: Top 10 most active users by clicks in 30 days
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_click_counts (
    event_date       DATE,
    user_id          BIGINT,
    click_count      INT,
    impression_count INT,
    PRIMARY KEY (event_date, click_count, user_id)
) WITH CLUSTERING ORDER BY (click_count DESC, user_id ASC);

-- -------------------------------------------------------
-- TABLE 5: advertiser_spend_by_region
-- QUERY: Top 5 advertisers by spend in a specific region
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS advertiser_spend_by_region (
    location        TEXT,
    event_date      DATE,
    advertiser_id   INT,
    advertiser_name TEXT,
    total_spend     DECIMAL,
    PRIMARY KEY ((location, event_date), total_spend, advertiser_id)
) WITH CLUSTERING ORDER BY (total_spend DESC, advertiser_id ASC);