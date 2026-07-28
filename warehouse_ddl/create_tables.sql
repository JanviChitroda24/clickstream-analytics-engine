-- Author: Janvi Chitroda
-- Copyright (c) 2026 Janvi Chitroda. All rights reserved.
-- Project: ClickStream Analytics Engine — Portfolio
-- Unauthorized copying or redistribution of this content is prohibited.

-- ============================================
-- CLICKSTREAM ANALYTICS ENGINE
-- Synapse Data Warehouse — Star Schema DDL
-- 4 Dimension Tables + 3 Fact Tables
--
-- FIRST RUN: Execute as-is to create all tables
-- RE-RUN: Tables already exist, will error harmlessly
-- RESET: Uncomment the DROP TABLE lines to recreate from scratch
--
-- Verified in Fabric clickstream_warehouse (Hour 21) — no PK/DEFAULT
-- (Fabric Warehouse is picky; keep DDL minimal and portable)
-- ============================================


-- ============================================
-- DIMENSION TABLES
-- ============================================

-- dim_users: Who is the user?
-- SCD-2 enabled: tracks subscription tier changes over time
-- user_key is surrogate key (auto-increment), user_id is natural key
-- One user can have multiple rows if their tier changes
-- DROP TABLE IF EXISTS dbo.dim_users;  -- UNCOMMENT TO RESET
CREATE TABLE dim_users (
    user_key BIGINT IDENTITY,                -- surrogate key (auto-generated)
    user_id VARCHAR(20) NOT NULL,            -- natural key: user_00000 to user_09999
    subscription_tier VARCHAR(20),           -- free / basic / premium
    device_type VARCHAR(20),                 -- mobile_ios / mobile_android / desktop / smart_tv
    app_version VARCHAR(10),                 -- 2.3.0 / 2.2.0
    behavioral_archetype VARCHAR(20),        -- power / regular / casual / churning / new
    churn_risk_score DECIMAL(5,2),           -- 0.00 to 1.00 (derived from activity decay)
    is_current BIT,                          -- SCD-2: 1 = current version, 0 = historical
    valid_from DATETIME2(6) NOT NULL,        -- SCD-2: when this version became active
    valid_to DATETIME2(6)                    -- SCD-2: when this version was superseded (NULL = current)
);


-- dim_content: What content exists on the platform?
-- Populated from the content catalog (200 titles)
-- No SCD needed — content metadata doesn't change after release
-- DROP TABLE IF EXISTS dbo.dim_content;  -- UNCOMMENT TO RESET
CREATE TABLE dim_content (
    content_id VARCHAR(20) NOT NULL,         -- content_000 to content_199
    title VARCHAR(200),                      -- e.g., "The Shadow Legacy"
    genre VARCHAR(50),                       -- drama / comedy / action / documentary / etc
    content_tier VARCHAR(20),                -- free / basic / premium
    duration_seconds INT,                    -- content length (1080 to 9600 seconds)
    release_year INT                         -- 2018 to 2026
);


-- dim_date: When did it happen?
-- Date spine covering the simulation period (July 1-14, 2026)
-- Pre-computed calendar attributes for easy filtering/grouping
-- DROP TABLE IF EXISTS dbo.dim_date;  -- UNCOMMENT TO RESET
CREATE TABLE dim_date (
    date_key INT NOT NULL,                   -- YYYYMMDD format: 20260701
    calendar_date DATE,                      -- 2026-07-01
    day_of_week VARCHAR(10),                 -- Monday, Tuesday, etc
    day_of_week_num INT,                     -- 1=Monday, 7=Sunday
    is_weekend BIT,                          -- 1 for Saturday/Sunday, 0 otherwise
    week_number INT,                         -- ISO week number (1-53)
    month_name VARCHAR(10),                  -- July
    year_num INT                             -- 2026
);


-- dim_campaign: Which ad campaign?
-- Populated from the ad campaign catalog (20 campaigns)
-- Links to fact_ad_attribution for ROAS and funnel analysis
-- DROP TABLE IF EXISTS dbo.dim_campaign;  -- UNCOMMENT TO RESET
CREATE TABLE dim_campaign (
    campaign_id VARCHAR(20) NOT NULL,        -- camp_000 to camp_019
    advertiser_id VARCHAR(20),               -- adv_000 to adv_019
    advertiser_name VARCHAR(100),            -- e.g., "BrightPath Insurance"
    campaign_type VARCHAR(50),               -- brand_awareness / app_install / product_purchase
    budget_tier VARCHAR(20),                 -- small / medium / large
    target_genres VARCHAR(200)               -- comma-separated: "thriller,comedy,documentary"
);


-- ============================================
-- FACT TABLES
-- ============================================

-- fact_sessions: What happened during each platform visit?
-- One row per session (184K rows)
-- Pre-aggregated event counts per session for fast analytics
-- Joins to dim_users (user_id), dim_date (session_date)
-- DROP TABLE IF EXISTS dbo.fact_sessions;  -- UNCOMMENT TO RESET
CREATE TABLE fact_sessions (
    session_id VARCHAR(50) NOT NULL,         -- computed_session_id from sessionization
    user_id VARCHAR(20),                     -- FK to dim_users.user_id
    device_type VARCHAR(20),                 -- denormalized from dim_users for query speed
    app_version VARCHAR(10),                 -- denormalized from dim_users
    session_start DATETIME2(6),              -- first event timestamp in session
    session_end DATETIME2(6),                -- last event timestamp in session
    duration_seconds INT,                    -- session_end - session_start
    event_count INT,                         -- total events in this session
    content_plays INT,                       -- count of content_play events
    content_completes INT,                   -- count of content_complete events
    content_abandons INT,                    -- count of content_abandon events
    searches INT,                            -- count of search events
    ad_impressions INT,                      -- count of ad_impression events
    ad_clicks INT,                           -- count of ad_click events
    conversions INT,                         -- count of conversion events
    session_date DATE,                       -- extracted from session_start for dim_date join
    session_hour INT,                        -- hour of day (0-23) for time-of-day analysis
    is_bounce BIT                            -- 1 if event_count = 1 (single-event session)
);


-- fact_content_engagement: What did users watch?
-- One row per content viewing event (301K rows)
-- Links content plays to completion/abandonment with metrics
-- Joins to dim_content (content_id), dim_users (user_id)
-- DROP TABLE IF EXISTS dbo.fact_content_engagement;  -- UNCOMMENT TO RESET
CREATE TABLE fact_content_engagement (
    engagement_id VARCHAR(50) NOT NULL,      -- unique ID for this engagement
    user_id VARCHAR(20),                     -- FK to dim_users.user_id
    session_id VARCHAR(50),                  -- FK to fact_sessions.session_id
    content_id VARCHAR(20),                  -- FK to dim_content.content_id
    event_type VARCHAR(30),                  -- content_play / content_complete / content_abandon
    play_timestamp DATETIME2(6),             -- when playback started
    watch_duration_seconds INT,              -- how long user actually watched
    content_duration_seconds INT,            -- total length of the content
    completion_rate DECIMAL(5,2),            -- watch_duration / content_duration * 100
    is_completed BIT,                        -- 1 if user finished the content
    is_abandoned BIT,                        -- 1 if user quit before finishing
    content_quality VARCHAR(10),             -- sd / hd / 4k (v2.3.0 only, NULL for v2.2.0)
    engagement_date DATE                     -- extracted date for dim_date join
);


-- fact_ad_attribution: How did ads perform?
-- One row per ad impression with linked click and conversion
-- Pre-joined funnel: impression → click → conversion in one row
-- Joins to dim_campaign (campaign_id), dim_users (user_id)
-- DROP TABLE IF EXISTS dbo.fact_ad_attribution;  -- UNCOMMENT TO RESET
CREATE TABLE fact_ad_attribution (
    attribution_id VARCHAR(50) NOT NULL,     -- unique ID for this attribution chain
    user_id VARCHAR(20),                     -- FK to dim_users.user_id
    session_id VARCHAR(50),                  -- FK to fact_sessions.session_id
    campaign_id VARCHAR(20),                 -- FK to dim_campaign.campaign_id
    advertiser_id VARCHAR(20),               -- denormalized from dim_campaign
    impression_event_id VARCHAR(50),         -- links back to raw impression event
    click_event_id VARCHAR(50),              -- links back to raw click event (NULL if no click)
    conversion_event_id VARCHAR(50),         -- links back to raw conversion event (NULL if no conversion)
    impression_timestamp DATETIME2(6),       -- when the ad was shown
    click_timestamp DATETIME2(6),            -- when user clicked (NULL if no click)
    conversion_timestamp DATETIME2(6),       -- when conversion happened (NULL if no conversion)
    time_to_click_seconds INT,               -- impression → click latency
    time_to_conversion_seconds INT,          -- click → conversion latency
    conversion_value DECIMAL(10,2),          -- dollar value of conversion (NULL if no conversion)
    attribution_date DATE                    -- extracted date for dim_date join
);


-- ============================================
-- VERIFY ALL 7 TABLES CREATED
-- ============================================
SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'dbo'
ORDER BY TABLE_NAME;

-- Expect:
--   dim_campaign, dim_content, dim_date, dim_users
--   fact_ad_attribution, fact_content_engagement, fact_sessions
