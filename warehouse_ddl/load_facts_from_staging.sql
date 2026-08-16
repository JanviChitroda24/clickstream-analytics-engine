-- Author: Janvi Chitroda
-- Copyright (c) 2026 Janvi Chitroda. All rights reserved.
-- Project: ClickStream Analytics Engine — Portfolio
-- Unauthorized copying or redistribution of this content is prohibited.

-- ============================================
-- Hour 23 — Load facts from Lakehouse staging
-- Run in: clickstream_warehouse SQL editor
-- Prerequisite: notebooks/06_load_facts.py Cell 3 all ✅
--   fact_sessions_staging            (= silver_sessions)
--   fact_content_engagement_staging  (content_id NOT NULL)
-- Same pattern as Hour 22 load_dimensions_from_staging.sql
--
-- Verified 2026-08-16:
--   fact_sessions            175,152
--   fact_content_engagement  542,219  (content_id 100%, 200 distinct titles)
--   All 4 referential-integrity checks = 0 orphans
-- ============================================

-- Clear existing rows (idempotent re-load)
DELETE FROM dbo.fact_sessions;
DELETE FROM dbo.fact_content_engagement;

-- Load fact_sessions — staging column order matches create_facts.sql
INSERT INTO dbo.fact_sessions (
    session_id, user_id, device_type, app_version,
    session_start, session_end, duration_seconds,
    event_count, content_plays, content_completes, content_abandons,
    searches, ad_impressions, ad_clicks, conversions,
    session_date, session_hour, is_bounce
)
SELECT
    session_id, user_id, device_type, app_version,
    session_start, session_end, duration_seconds,
    event_count, content_plays, content_completes, content_abandons,
    searches, ad_impressions, ad_clicks, conversions,
    session_date, session_hour, is_bounce
FROM clickstream_lakehouse.dbo.fact_sessions_staging;

-- Load fact_content_engagement
INSERT INTO dbo.fact_content_engagement (
    engagement_id, user_id, session_id, content_id, event_type,
    play_timestamp, watch_duration_seconds, content_duration_seconds,
    completion_rate, is_completed, is_abandoned,
    content_quality, engagement_date
)
SELECT
    engagement_id, user_id, session_id, content_id, event_type,
    play_timestamp, watch_duration_seconds, content_duration_seconds,
    completion_rate, is_completed, is_abandoned,
    content_quality, engagement_date
FROM clickstream_lakehouse.dbo.fact_content_engagement_staging;

-- ============================================
-- Verification
-- ============================================

-- Row counts (must match Cell 3 staging counts)
SELECT 'fact_sessions' AS table_name, COUNT(*) AS row_count FROM dbo.fact_sessions
UNION ALL
SELECT 'fact_content_engagement', COUNT(*) FROM dbo.fact_content_engagement
ORDER BY table_name;

-- Properties fix proof: content_id populated, no NULL placeholders
SELECT
    COUNT(*)                                              AS total_rows,
    COUNT(content_id)                                     AS has_content_id,
    COUNT(*) - COUNT(content_id)                          AS null_content_id,  -- must be 0
    COUNT(DISTINCT content_id)                            AS distinct_content  -- expect ~200
FROM dbo.fact_content_engagement;

-- Referential integrity — every fact key must resolve to a dimension
SELECT 'engagement → dim_content' AS check_name, COUNT(*) AS orphans
FROM dbo.fact_content_engagement e
WHERE e.content_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM dbo.dim_content c WHERE c.content_id = e.content_id)
UNION ALL
SELECT 'sessions → dim_users', COUNT(*)
FROM dbo.fact_sessions s
WHERE NOT EXISTS (SELECT 1 FROM dbo.dim_users u WHERE u.user_id = s.user_id)
UNION ALL
SELECT 'sessions → dim_date', COUNT(*)
FROM dbo.fact_sessions s
WHERE NOT EXISTS (SELECT 1 FROM dbo.dim_date d WHERE d.calendar_date = s.session_date)
UNION ALL
SELECT 'engagement → fact_sessions', COUNT(*)
FROM dbo.fact_content_engagement e
WHERE NOT EXISTS (SELECT 1 FROM dbo.fact_sessions s WHERE s.session_id = e.session_id);
-- All four must return 0.

-- First real content analytics — the question that was unanswerable
-- before the properties fix (genre came back NULL).
SELECT TOP 10
    c.genre,
    COUNT(*)                                          AS interactions,
    SUM(CAST(e.is_completed AS INT))                  AS completes,
    SUM(CAST(e.is_abandoned AS INT))                  AS abandons,
    CAST(AVG(e.completion_rate) AS DECIMAL(5,2))      AS avg_completion_rate
FROM dbo.fact_content_engagement e
JOIN dbo.dim_content c ON c.content_id = e.content_id
GROUP BY c.genre
ORDER BY avg_completion_rate DESC;

-- Engagement sanity: bounce rate + avg session duration by device
SELECT
    device_type,
    COUNT(*)                                          AS sessions,
    CAST(AVG(CAST(duration_seconds AS FLOAT)) AS INT) AS avg_duration_sec,
    CAST(100.0 * SUM(CAST(is_bounce AS INT)) / COUNT(*) AS DECIMAL(5,2)) AS bounce_pct
FROM dbo.fact_sessions
GROUP BY device_type
ORDER BY sessions DESC;
