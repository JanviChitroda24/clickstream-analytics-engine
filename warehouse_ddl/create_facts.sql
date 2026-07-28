-- Author: Janvi Chitroda
-- Copyright (c) 2026 Janvi Chitroda. All rights reserved.
-- Project: ClickStream Analytics Engine — Portfolio
-- Unauthorized copying or redistribution of this content is prohibited.

-- ============================================
-- Fact tables only (Hour 21)
-- Prefer warehouse_ddl/create_tables.sql for full run.
-- ============================================

-- DROP TABLE IF EXISTS dbo.fact_sessions;  -- UNCOMMENT TO RESET
CREATE TABLE fact_sessions (
    session_id VARCHAR(50) NOT NULL,
    user_id VARCHAR(20),
    device_type VARCHAR(20),
    app_version VARCHAR(10),
    session_start DATETIME2(6),
    session_end DATETIME2(6),
    duration_seconds INT,
    event_count INT,
    content_plays INT,
    content_completes INT,
    content_abandons INT,
    searches INT,
    ad_impressions INT,
    ad_clicks INT,
    conversions INT,
    session_date DATE,
    session_hour INT,
    is_bounce BIT
);

-- DROP TABLE IF EXISTS dbo.fact_content_engagement;  -- UNCOMMENT TO RESET
CREATE TABLE fact_content_engagement (
    engagement_id VARCHAR(50) NOT NULL,
    user_id VARCHAR(20),
    session_id VARCHAR(50),
    content_id VARCHAR(20),
    event_type VARCHAR(30),
    play_timestamp DATETIME2(6),
    watch_duration_seconds INT,
    content_duration_seconds INT,
    completion_rate DECIMAL(5,2),
    is_completed BIT,
    is_abandoned BIT,
    content_quality VARCHAR(10),
    engagement_date DATE
);

-- DROP TABLE IF EXISTS dbo.fact_ad_attribution;  -- UNCOMMENT TO RESET
CREATE TABLE fact_ad_attribution (
    attribution_id VARCHAR(50) NOT NULL,
    user_id VARCHAR(20),
    session_id VARCHAR(50),
    campaign_id VARCHAR(20),
    advertiser_id VARCHAR(20),
    impression_event_id VARCHAR(50),
    click_event_id VARCHAR(50),
    conversion_event_id VARCHAR(50),
    impression_timestamp DATETIME2(6),
    click_timestamp DATETIME2(6),
    conversion_timestamp DATETIME2(6),
    time_to_click_seconds INT,
    time_to_conversion_seconds INT,
    conversion_value DECIMAL(10,2),
    attribution_date DATE
);
