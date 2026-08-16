-- Author: Janvi Chitroda
-- Copyright (c) 2026 Janvi Chitroda. All rights reserved.
-- Project: ClickStream Analytics Engine — Portfolio
-- Unauthorized copying or redistribution of this content is prohibited.

-- ============================================
-- Load ad attribution from Lakehouse staging
-- Run in: clickstream_warehouse SQL editor
-- Prerequisite: notebooks/07_load_ad_attribution.py Cell 4 all ✅
--   fact_ad_attribution_staging  (one row per impression;
--                                 click / conversion columns NULL when absent)
-- Same pattern as load_facts_from_staging.sql
-- ============================================

-- Clear existing rows (idempotent re-load)
DELETE FROM dbo.fact_ad_attribution;

-- Load fact_ad_attribution — explicit column list matches create_tables.sql
INSERT INTO dbo.fact_ad_attribution (
    attribution_id, user_id, session_id, campaign_id, advertiser_id,
    impression_event_id, click_event_id, conversion_event_id,
    impression_timestamp, click_timestamp, conversion_timestamp,
    time_to_click_seconds, time_to_conversion_seconds,
    conversion_value, attribution_date
)
SELECT
    attribution_id, user_id, session_id, campaign_id, advertiser_id,
    impression_event_id, click_event_id, conversion_event_id,
    impression_timestamp, click_timestamp, conversion_timestamp,
    time_to_click_seconds, time_to_conversion_seconds,
    conversion_value, attribution_date
FROM clickstream_lakehouse.dbo.fact_ad_attribution_staging;

-- ============================================
-- Verification
-- ============================================

-- 1. Row count (must match Cell 4 staging count)
SELECT 'fact_ad_attribution' AS table_name, COUNT(*) AS row_count
FROM dbo.fact_ad_attribution;

-- 2. Funnel summary — each stage is a subset of the one above it
SELECT
    COUNT(*)                                                          AS impressions,
    COUNT(click_event_id)                                             AS impressions_with_click,
    COUNT(conversion_event_id)                                        AS clicks_with_conversion,
    SUM(CASE WHEN impression_event_id IS NOT NULL
              AND click_event_id      IS NOT NULL
              AND conversion_event_id IS NOT NULL
             THEN 1 ELSE 0 END)                                       AS full_chains
FROM dbo.fact_ad_attribution;

-- 3. Campaign performance by type — the ROAS view
SELECT
    c.campaign_type,
    COUNT(*)                                                          AS impressions,
    COUNT(a.click_event_id)                                           AS clicks,
    COUNT(a.conversion_event_id)                                      AS conversions,
    CAST(100.0 * COUNT(a.click_event_id)
         / NULLIF(COUNT(*), 0) AS DECIMAL(5,2))                       AS click_through_rate_pct,
    CAST(100.0 * COUNT(a.conversion_event_id)
         / NULLIF(COUNT(a.click_event_id), 0) AS DECIMAL(5,2))        AS conversion_rate_pct,
    CAST(SUM(a.conversion_value) AS DECIMAL(12,2))                    AS total_conversion_value
FROM dbo.fact_ad_attribution a
JOIN dbo.dim_campaign c ON c.campaign_id = a.campaign_id
GROUP BY c.campaign_type
ORDER BY total_conversion_value DESC;

-- 4. Referential integrity — both must return 0 orphans
SELECT 'attribution → dim_campaign' AS check_name, COUNT(*) AS orphans
FROM dbo.fact_ad_attribution a
WHERE NOT EXISTS (SELECT 1 FROM dbo.dim_campaign c WHERE c.campaign_id = a.campaign_id)
UNION ALL
SELECT 'attribution → dim_users', COUNT(*)
FROM dbo.fact_ad_attribution a
WHERE NOT EXISTS (SELECT 1 FROM dbo.dim_users u WHERE u.user_id = a.user_id);
