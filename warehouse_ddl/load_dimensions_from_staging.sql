-- Author: Janvi Chitroda
-- Copyright (c) 2026 Janvi Chitroda. All rights reserved.
-- Project: ClickStream Analytics Engine — Portfolio
-- Unauthorized copying or redistribution of this content is prohibited.

-- ============================================
-- Hour 22 — Load dimensions from Lakehouse staging
-- Run in: clickstream_warehouse SQL editor
-- Prerequisite: Lakehouse staging tables populated
--   dim_content_staging (200)
--   dim_campaign_staging (20)
--   dim_date_staging (31)
--   dim_users_staging (9,929)
-- Verified: 2026-07-28
-- ============================================

-- Clear existing rows (idempotent re-load)
DELETE FROM dbo.dim_content;
DELETE FROM dbo.dim_campaign;
DELETE FROM dbo.dim_date;
DELETE FROM dbo.dim_users;

-- Load dim_content from Lakehouse staging
INSERT INTO dbo.dim_content
SELECT * FROM clickstream_lakehouse.dbo.dim_content_staging;

-- Load dim_campaign from Lakehouse staging
INSERT INTO dbo.dim_campaign
SELECT * FROM clickstream_lakehouse.dbo.dim_campaign_staging;

-- Load dim_date from Lakehouse staging
INSERT INTO dbo.dim_date
SELECT * FROM clickstream_lakehouse.dbo.dim_date_staging;

-- Load dim_users (exclude user_key — IDENTITY auto-generated)
INSERT INTO dbo.dim_users (
    user_id, subscription_tier, device_type, app_version,
    behavioral_archetype, churn_risk_score, is_current,
    valid_from, valid_to
)
SELECT
    user_id, subscription_tier, device_type, app_version,
    behavioral_archetype, churn_risk_score, is_current,
    valid_from, valid_to
FROM clickstream_lakehouse.dbo.dim_users_staging;

-- Spot-check staging source
SELECT TOP 5 * FROM clickstream_lakehouse.dbo.dim_users_staging;

-- Verify all 4 dimensions in warehouse
SELECT 'dim_content' AS table_name, COUNT(*) AS row_count FROM dbo.dim_content
UNION ALL
SELECT 'dim_campaign', COUNT(*) FROM dbo.dim_campaign
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dbo.dim_date
UNION ALL
SELECT 'dim_users', COUNT(*) FROM dbo.dim_users
ORDER BY table_name;

-- Expected:
--   dim_campaign  20
--   dim_content   200
--   dim_date      31
--   dim_users     9929
