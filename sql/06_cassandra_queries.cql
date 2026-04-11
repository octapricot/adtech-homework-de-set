-- PURPOSE: 5 CQL queries answering business questions

-- -------------------------------------------------------
-- QUERY 1: CTR per campaign per day
-- -------------------------------------------------------
SELECT
    campaign_id,
    campaign_name,
    advertiser_name,
    event_date,
    impressions,
    clicks
FROM campaign_performance_by_day
WHERE campaign_id = 191
  AND event_date >= '2024-10-01'
  AND event_date <= '2024-10-31';

-- -------------------------------------------------------
-- QUERY 2: Top 5 advertisers by total spend in 30 days
-- -------------------------------------------------------
SELECT
    advertiser_id,
    advertiser_name,
    event_date,
    total_spend,
    total_impressions
FROM advertiser_spend
WHERE event_date >= '2024-10-01'
  AND event_date <= '2024-10-31'
ALLOW FILTERING;

-- -------------------------------------------------------
-- QUERY 3: Last 10 ads a user saw
-- -------------------------------------------------------
SELECT
    user_id,
    timestamp,
    campaign_name,
    advertiser_name,
    device,
    was_clicked,
    ad_revenue
FROM user_ad_history
WHERE user_id = 59472
LIMIT 10;

-- -------------------------------------------------------
-- QUERY 4: Top 10 most active users by clicks in 30 days
-- -------------------------------------------------------
SELECT
    event_date,
    user_id,
    click_count,
    impression_count
FROM user_click_counts
WHERE event_date >= '2024-10-01'
  AND event_date <= '2024-10-31'
ALLOW FILTERING;

-- ---------------------------------------------------------
-- QUERY 5: Top 5 advertisers by spend in a specific region
-- ---------------------------------------------------------
SELECT
    location,
    event_date,
    advertiser_id,
    advertiser_name,
    total_spend
FROM advertiser_spend_by_region
WHERE location = 'USA'
  AND event_date >= '2024-10-01'
  AND event_date <= '2024-10-31'
ALLOW FILTERING;