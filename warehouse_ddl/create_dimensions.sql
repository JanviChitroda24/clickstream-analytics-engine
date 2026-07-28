-- Author: Janvi Chitroda
-- Copyright (c) 2026 Janvi Chitroda. All rights reserved.
-- Project: ClickStream Analytics Engine — Portfolio
-- Unauthorized copying or redistribution of this content is prohibited.

-- ============================================
-- Dimension tables only (Hour 21)
-- Prefer warehouse_ddl/create_tables.sql for full run.
-- ============================================

-- DROP TABLE IF EXISTS dbo.dim_users;  -- UNCOMMENT TO RESET
CREATE TABLE dim_users (
    user_key BIGINT IDENTITY,
    user_id VARCHAR(20) NOT NULL,
    subscription_tier VARCHAR(20),
    device_type VARCHAR(20),
    app_version VARCHAR(10),
    behavioral_archetype VARCHAR(20),
    churn_risk_score DECIMAL(5,2),
    is_current BIT,
    valid_from DATETIME2(6) NOT NULL,
    valid_to DATETIME2(6)
);

-- DROP TABLE IF EXISTS dbo.dim_content;  -- UNCOMMENT TO RESET
CREATE TABLE dim_content (
    content_id VARCHAR(20) NOT NULL,
    title VARCHAR(200),
    genre VARCHAR(50),
    content_tier VARCHAR(20),
    duration_seconds INT,
    release_year INT
);

-- DROP TABLE IF EXISTS dbo.dim_date;  -- UNCOMMENT TO RESET
CREATE TABLE dim_date (
    date_key INT NOT NULL,
    calendar_date DATE,
    day_of_week VARCHAR(10),
    day_of_week_num INT,
    is_weekend BIT,
    week_number INT,
    month_name VARCHAR(10),
    year_num INT
);

-- DROP TABLE IF EXISTS dbo.dim_campaign;  -- UNCOMMENT TO RESET
CREATE TABLE dim_campaign (
    campaign_id VARCHAR(20) NOT NULL,
    advertiser_id VARCHAR(20),
    advertiser_name VARCHAR(100),
    campaign_type VARCHAR(50),
    budget_tier VARCHAR(20),
    target_genres VARCHAR(200)
);
