-- PURPOSE: Adds indexes to improve query performance on the most frequently filtered and joined columns.

-- Index on timestamp: speeds up all 30-day window filters
CREATE INDEX idx_ad_events_timestamp 
    ON ad_events(timestamp);

-- Index on campaign_id: speeds up JOINs between ad_events and campaigns
CREATE INDEX idx_ad_events_campaign_id 
    ON ad_events(campaign_id);

-- Index on user_id: speeds up JOINs with users table and the top 10 engaged users query
CREATE INDEX idx_ad_events_user_id 
    ON ad_events(user_id);

-- Index on was_clicked: speeds up counting clicks vs impressions for CTR calculations
CREATE INDEX idx_ad_events_was_clicked 
    ON ad_events(was_clicked);

-- Index on location: speeds up the regional revenue query
CREATE INDEX idx_ad_events_location 
    ON ad_events(location);

-- Index on device: speeds up the device comparison query
CREATE INDEX idx_ad_events_device 
    ON ad_events(device);

-- Composite index on campaign_id + timestamp together
CREATE INDEX idx_ad_events_campaign_timestamp 
    ON ad_events(campaign_id, timestamp);