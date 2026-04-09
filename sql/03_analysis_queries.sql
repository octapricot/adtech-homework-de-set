-- PURPOSE: Answer 7 business questions about AdTech performance; 30-day window: 2024-10-01 to 2024-10-31

-- --------------------------------------------------------------------------
-- QUERY 1: Top 5 campaigns (we pre-aggregate ad_events first in a subquery,
-- then join the small result to campaigns and advertisers.)
-- ---------------------------------------------------------------------------
SELECT
    c.campaign_name,
    a.advertiser_name,
    e.impressions,
    e.clicks,
    ROUND(e.clicks / e.impressions * 100, 2) AS ctr_percent
FROM (
    SELECT
        campaign_id,
        COUNT(event_id)    AS impressions,
        SUM(was_clicked)   AS clicks
    FROM ad_events
    WHERE timestamp BETWEEN '2024-10-01' AND '2024-10-31 23:59:59'
    GROUP BY campaign_id
) e
JOIN campaigns c ON e.campaign_id = c.campaign_id
JOIN advertisers a ON c.advertiser_id = a.advertiser_id
ORDER BY ctr_percent DESC
LIMIT 5;

-- -------------------------------------------------------------------------------------------
-- QUERY 2: Top advertisers by total spend (ad_cost is what was actually paid per impression)
-- -------------------------------------------------------------------------------------------
SELECT
    a.advertiser_name,
    SUM(e.impressions)              AS total_impressions,
    ROUND(SUM(e.total_spend), 2)    AS total_spend
FROM (
    SELECT
        campaign_id,
        COUNT(event_id)    AS impressions,
        SUM(ad_cost)       AS total_spend
    FROM ad_events
    WHERE timestamp BETWEEN '2024-10-01' AND '2024-10-31 23:59:59'
    GROUP BY campaign_id
) e
JOIN campaigns c ON e.campaign_id = c.campaign_id
JOIN advertisers a ON c.advertiser_id = a.advertiser_id
GROUP BY a.advertiser_id, a.advertiser_name
ORDER BY total_spend DESC
LIMIT 10;

-- -------------------------------------------------------
-- QUERY 3: CPC and CPM per campaign
-- -------------------------------------------------------
SELECT
    c.campaign_name,
    a.advertiser_name,
    e.impressions,
    e.clicks,
    ROUND(e.total_spend, 2)                               AS total_spend,
    ROUND(e.total_spend / NULLIF(e.clicks, 0), 2)         AS cpc,
    ROUND(e.total_spend / e.impressions * 1000, 2)        AS cpm
FROM (
    SELECT
        campaign_id,
        COUNT(event_id)    AS impressions,
        SUM(was_clicked)   AS clicks,
        SUM(ad_cost)       AS total_spend
    FROM ad_events
    WHERE timestamp BETWEEN '2024-10-01' AND '2024-10-31 23:59:59'
    GROUP BY campaign_id
) e
JOIN campaigns c ON e.campaign_id = c.campaign_id
JOIN advertisers a ON c.advertiser_id = a.advertiser_id
ORDER BY total_spend DESC;

-- -------------------------------------------------------
-- QUERY 4: Top locations by ad revenue
-- -------------------------------------------------------
SELECT
    location,
    COUNT(event_id)           AS total_impressions,
    SUM(was_clicked)          AS total_clicks,
    ROUND(SUM(ad_revenue), 2) AS total_revenue
FROM ad_events
WHERE timestamp BETWEEN '2024-10-01' AND '2024-10-31 23:59:59'
GROUP BY location
ORDER BY total_revenue DESC;

-- -------------------------------------------------------
-- QUERY 5: Top 10 most engaged users
-- -------------------------------------------------------
SELECT
    u.user_id,
    u.age,
    u.gender,
    u.location,
    e.impressions       AS total_impressions,
    e.clicks            AS total_clicks
FROM (
    SELECT
        user_id,
        COUNT(event_id)  AS impressions,
        SUM(was_clicked) AS clicks
    FROM ad_events
    WHERE timestamp BETWEEN '2024-10-01' AND '2024-10-31 23:59:59'
    GROUP BY user_id
) e
JOIN users u ON e.user_id = u.user_id
ORDER BY total_clicks DESC
LIMIT 10;

-- -------------------------------------------------------
-- QUERY 6: Campaigns that have spent more than 80% of budget
-- -------------------------------------------------------
SELECT
    c.campaign_name,
    a.advertiser_name,
    ROUND(c.budget, 2)                                         AS total_budget,
    ROUND(c.budget - c.remaining_budget, 2)                    AS amount_spent,
    ROUND((c.budget - c.remaining_budget) / c.budget * 100, 2) AS budget_used_percent
FROM campaigns c
JOIN advertisers a ON c.advertiser_id = a.advertiser_id
WHERE (c.budget - c.remaining_budget) / c.budget >= 0.80
ORDER BY budget_used_percent DESC;

-- --------------------------------------------------------------------------------
-- QUERY 7: CTR by device type (same CTR formula as Query 1 but grouped by device)
-- --------------------------------------------------------------------------------
SELECT
    device,
    COUNT(event_id)                                        AS impressions,
    SUM(was_clicked)                                       AS clicks,
    ROUND(SUM(was_clicked) / COUNT(event_id) * 100, 2)     AS ctr_percent
FROM ad_events
WHERE timestamp BETWEEN '2024-10-01' AND '2024-10-31 23:59:59'
GROUP BY device
ORDER BY ctr_percent DESC;