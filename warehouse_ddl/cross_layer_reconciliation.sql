-- Author: Janvi Chitroda
-- Copyright (c) 2026 Janvi Chitroda. All rights reserved.
-- Project: ClickStream Analytics Engine — Portfolio
-- Unauthorized copying or redistribution of this content is prohibited.

-- ============================================
-- PART 1: CROSS-LAYER RECONCILIATION
-- Proves pipeline correctness from Bronze to Gold
--
-- Run in: clickstream_warehouse SQL editor
-- Reads Lakehouse tables via cross-database reference
-- (clickstream_lakehouse.dbo.*) and Warehouse tables directly.
--
-- Prerequisite: all 4 dims + 3 facts loaded
--   load_dimensions_from_staging.sql
--   load_facts_from_staging.sql
--   load_ad_attribution_from_staging.sql
-- ============================================


-- ============================================
-- 1A. Row count reconciliation across all layers
-- layer_order sorts the readout in PIPELINE order (bronze → gold),
-- not alphabetically — a reconciliation report is only readable
-- when the layers appear in the order data flows through them.
-- ============================================
SELECT 1  AS layer_order, 'bronze_events'           AS layer, COUNT(*) AS row_count FROM clickstream_lakehouse.dbo.bronze_events
UNION ALL
SELECT 2,  'dead_letter_events',      COUNT(*) FROM clickstream_lakehouse.dbo.dead_letter_events
UNION ALL
SELECT 3,  'silver_validated_events', COUNT(*) FROM clickstream_lakehouse.dbo.silver_validated_events
UNION ALL
SELECT 4,  'silver_deduped_events',   COUNT(*) FROM clickstream_lakehouse.dbo.silver_deduped_events
UNION ALL
SELECT 5,  'silver_sessions',         COUNT(*) FROM clickstream_lakehouse.dbo.silver_sessions
UNION ALL
SELECT 6,  'fact_sessions',           COUNT(*) FROM dbo.fact_sessions
UNION ALL
SELECT 7,  'fact_content_engagement', COUNT(*) FROM dbo.fact_content_engagement
UNION ALL
SELECT 8,  'fact_ad_attribution',     COUNT(*) FROM dbo.fact_ad_attribution
UNION ALL
SELECT 9,  'dim_users',               COUNT(*) FROM dbo.dim_users
UNION ALL
SELECT 10, 'dim_content',             COUNT(*) FROM dbo.dim_content
UNION ALL
SELECT 11, 'dim_campaign',            COUNT(*) FROM dbo.dim_campaign
UNION ALL
SELECT 12, 'dim_date',                COUNT(*) FROM dbo.dim_date
ORDER BY layer_order;

-- Expected (verified 2026-08-16):
--   silver_sessions          175,152   fact_sessions            175,152
--   fact_content_engagement  542,219   fact_ad_attribution       46,865
--   dim_users  9,929 · dim_content 200 · dim_campaign 20 · dim_date 31


-- ============================================
-- 1B. Pipeline math check
--   bronze = validated + dead_letter   (validation is a clean partition:
--            status = 'PASS' vs everything else, so nothing is lost)
--   validated - deduped = duplicates removed
-- ============================================
SELECT
    (SELECT COUNT(*) FROM clickstream_lakehouse.dbo.bronze_events)           AS bronze,
    (SELECT COUNT(*) FROM clickstream_lakehouse.dbo.silver_validated_events) AS validated,
    (SELECT COUNT(*) FROM clickstream_lakehouse.dbo.dead_letter_events)      AS dead_letter,
    (SELECT COUNT(*) FROM clickstream_lakehouse.dbo.silver_deduped_events)   AS deduped,
    (SELECT COUNT(*) FROM clickstream_lakehouse.dbo.silver_validated_events)
        - (SELECT COUNT(*) FROM clickstream_lakehouse.dbo.silver_deduped_events) AS duplicates_removed,
    CASE
        WHEN (SELECT COUNT(*) FROM clickstream_lakehouse.dbo.bronze_events) =
             (SELECT COUNT(*) FROM clickstream_lakehouse.dbo.silver_validated_events) +
             (SELECT COUNT(*) FROM clickstream_lakehouse.dbo.dead_letter_events)
        THEN 'PASS' ELSE 'FAIL'
    END AS bronze_split_check;


-- ============================================
-- 1C. Fact table coverage
-- SUM(event_count) across sessions must equal the deduped event count —
-- sessionization assigns every event to exactly one session, so the fact
-- table's pre-aggregated counts have to add back up to the source.
-- ============================================
SELECT
    (SELECT COUNT(*) FROM clickstream_lakehouse.dbo.silver_deduped_events) AS total_deduped_events,
    (SELECT SUM(event_count) FROM dbo.fact_sessions)                       AS events_in_sessions,
    (SELECT COUNT(*) FROM dbo.fact_content_engagement)                     AS content_engagement_rows,
    (SELECT COUNT(*) FROM dbo.fact_ad_attribution)                         AS ad_attribution_rows,
    CASE
        WHEN (SELECT COUNT(*) FROM clickstream_lakehouse.dbo.silver_deduped_events) =
             (SELECT SUM(event_count) FROM dbo.fact_sessions)
        THEN 'PASS' ELSE 'FAIL'
    END AS event_conservation_check;


-- ============================================
-- 1D. Data quality checks on fact tables
-- Every row must return 0 violations.
-- ============================================

-- Duration should be >= 0
SELECT 'negative_duration' AS check_name, COUNT(*) AS violations
FROM dbo.fact_sessions WHERE duration_seconds < 0
UNION ALL
-- Completion rate should be 0-1
SELECT 'completion_rate_out_of_range', COUNT(*)
FROM dbo.fact_content_engagement
WHERE completion_rate IS NOT NULL AND (completion_rate < 0 OR completion_rate > 1)
UNION ALL
-- Bounce sessions should have event_count = 1
SELECT 'bounce_event_mismatch', COUNT(*)
FROM dbo.fact_sessions WHERE is_bounce = 1 AND event_count != 1
UNION ALL
-- Time to click should be >= 0
SELECT 'negative_time_to_click', COUNT(*)
FROM dbo.fact_ad_attribution
WHERE time_to_click_seconds IS NOT NULL AND time_to_click_seconds < 0
UNION ALL
-- Time to conversion should be >= 0
SELECT 'negative_time_to_conv', COUNT(*)
FROM dbo.fact_ad_attribution
WHERE time_to_conversion_seconds IS NOT NULL AND time_to_conversion_seconds < 0
UNION ALL
-- All content_ids should exist in dim_content
SELECT 'orphan_content_id', COUNT(*)
FROM dbo.fact_content_engagement e
WHERE NOT EXISTS (SELECT 1 FROM dbo.dim_content c WHERE c.content_id = e.content_id)
UNION ALL
-- All user_ids in facts should exist in dim_users
SELECT 'orphan_user_sessions', COUNT(*)
FROM dbo.fact_sessions s
WHERE NOT EXISTS (SELECT 1 FROM dbo.dim_users u WHERE u.user_id = s.user_id)
UNION ALL
SELECT 'orphan_user_engagement', COUNT(*)
FROM dbo.fact_content_engagement e
WHERE NOT EXISTS (SELECT 1 FROM dbo.dim_users u WHERE u.user_id = e.user_id)
UNION ALL
SELECT 'orphan_user_attribution', COUNT(*)
FROM dbo.fact_ad_attribution a
WHERE NOT EXISTS (SELECT 1 FROM dbo.dim_users u WHERE u.user_id = a.user_id);
