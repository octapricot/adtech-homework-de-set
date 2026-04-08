DROP TABLE IF EXISTS clicks;
DROP TABLE IF EXISTS ad_events;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS campaigns;
DROP TABLE IF EXISTS advertisers;

CREATE TABLE advertisers (
    advertiser_id INT AUTO_INCREMENT PRIMARY KEY,
    advertiser_name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE campaigns (
    campaign_id INT AUTO_INCREMENT PRIMARY KEY,
    advertiser_id INT NOT NULL,
    campaign_name VARCHAR(255) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    budget DECIMAL(12,2) NOT NULL,
    remaining_budget DECIMAL(12,2) NOT NULL,
    FOREIGN KEY (advertiser_id) REFERENCES advertisers(advertiser_id)
);

CREATE TABLE users (
    user_id BIGINT PRIMARY KEY,
    age INT,
    gender VARCHAR(50),
    location VARCHAR(100),
    interests TEXT,
    signup_date DATE
);

CREATE TABLE ad_events (
    event_id VARCHAR(36) PRIMARY KEY,
    campaign_id INT NOT NULL,
    user_id BIGINT NOT NULL,
    targeting_criteria VARCHAR(255),
    ad_slot_size VARCHAR(50),
    device VARCHAR(50),
    location VARCHAR(100),
    timestamp DATETIME NOT NULL,
    bid_amount DECIMAL(10,2),
    ad_cost DECIMAL(10,2),
    ad_revenue DECIMAL(10,2),
    was_clicked BOOLEAN NOT NULL DEFAULT FALSE,
    FOREIGN KEY (campaign_id) REFERENCES campaigns(campaign_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

CREATE TABLE clicks (
    click_id INT AUTO_INCREMENT PRIMARY KEY,
    event_id VARCHAR(36) NOT NULL UNIQUE,
    click_timestamp DATETIME NOT NULL,
    ad_revenue DECIMAL(10,2),
    FOREIGN KEY (event_id) REFERENCES ad_events(event_id)
);