-- Author: Janvi Chitroda
-- Copyright (c) 2026 Janvi Chitroda. All rights reserved.
-- Project: ClickStream Analytics Engine — Portfolio
-- Unauthorized copying or redistribution of this content is prohibited.

-- ============================================
-- PART 2: SEMANTIC LAYER — 6 GOVERNED METRIC VIEWS
-- Each view IS the business definition. One place to change a metric,
-- one answer for every consumer (Power BI, ad-hoc SQL, notebooks).
--
-- Run in: clickstream_warehouse SQL editor
-- Prerequisite: cross_layer_reconciliation.sql all PASS / 0 violations
--
-- CREATE VIEW must be the first statement in its batch — run each
-- DROP + CREATE pair individually, not as one batch.
-- ============================================


-- ============================================
-- View 1: Daily Active Users
-- Definition: unique users who started at least one session on the calendar date.
-- Owner: Product Team | Version: 1.0
-- ============================================
DROP VIEW IF EXISTS dbo.vw_daily_active_users;
GO
CREATE VIEW dbo.vw_daily_active_users AS
SELECT
    session_date                                                        AS activity_date,
    COUNT(DISTINCT user_id)                                             AS dau,
    COUNT(*)                                                            AS total_sessions,
    SUM(CAST(is_bounce AS INT))                                         AS bounce_sessions,
    CAST(100.0 * SUM(CAST(is_bounce AS INT))
         / COUNT(*) AS DECIMAL(5,2))                                    AS bounce_rate_pct
FROM dbo.fact_sessions
GROUP BY session_date;
GO


-- ============================================
-- View 2: Session Duration Distribution
-- Definition: sessions bucketed by duration for histogram visualization.
-- Bucket labels are numbered ('01: …') so lexical sort = natural sort.
-- Owner: Product Team | Version: 1.0
-- ============================================
DROP VIEW IF EXISTS dbo.vw_session_duration_distribution;
GO
CREATE VIEW dbo.vw_session_duration_distribution AS
SELECT
    CASE
        WHEN duration_seconds <   60 THEN '01: 0-1 min'
        WHEN duration_seconds <  300 THEN '02: 1-5 min'
        WHEN duration_seconds <  900 THEN '03: 5-15 min'
        WHEN duration_seconds < 1800 THEN '04: 15-30 min'
        WHEN duration_seconds < 3600 THEN '05: 30-60 min'
        ELSE '06: 60+ min'
    END                                                                 AS duration_bucket,
    COUNT(*)                                                            AS session_count,
    CAST(AVG(CAST(duration_seconds AS FLOAT)) AS INT)                   AS avg_duration_in_bucket,
    COUNT(DISTINCT user_id)                                             AS unique_users
FROM dbo.fact_sessions
GROUP BY
    CASE
        WHEN duration_seconds <   60 THEN '01: 0-1 min'
        WHEN duration_seconds <  300 THEN '02: 1-5 min'
        WHEN duration_seconds <  900 THEN '03: 5-15 min'
        WHEN duration_seconds < 1800 THEN '04: 15-30 min'
        WHEN duration_seconds < 3600 THEN '05: 30-60 min'
        ELSE '06: 60+ min'
    END;
GO


-- ============================================
-- View 3: Funnel Conversion
-- Definition: share of sessions reaching each stage — search → play → complete → convert.
-- NOTE: these are PARALLEL rates over all sessions, not a strict nested funnel;
-- a session can play without searching. Read each as "% of sessions that did X".
-- Owner: Product Team | Version: 1.0
-- ============================================
DROP VIEW IF EXISTS dbo.vw_funnel_conversion;
GO
CREATE VIEW dbo.vw_funnel_conversion AS
SELECT
    COUNT(*)                                                            AS total_sessions,
    SUM(CASE WHEN searches          > 0 THEN 1 ELSE 0 END)              AS sessions_with_search,
    SUM(CASE WHEN content_plays     > 0 THEN 1 ELSE 0 END)              AS sessions_with_play,
    SUM(CASE WHEN content_completes > 0 THEN 1 ELSE 0 END)              AS sessions_with_complete,
    SUM(CASE WHEN conversions       > 0 THEN 1 ELSE 0 END)              AS sessions_with_conversion,
    CAST(100.0 * SUM(CASE WHEN searches          > 0 THEN 1 ELSE 0 END)
         / COUNT(*) AS DECIMAL(5,2))                                    AS search_rate_pct,
    CAST(100.0 * SUM(CASE WHEN content_plays     > 0 THEN 1 ELSE 0 END)
         / COUNT(*) AS DECIMAL(5,2))                                    AS play_rate_pct,
    CAST(100.0 * SUM(CASE WHEN content_completes > 0 THEN 1 ELSE 0 END)
         / COUNT(*) AS DECIMAL(5,2))                                    AS complete_rate_pct,
    -- sessions_with_conversion was computed but had no matching rate
    CAST(100.0 * SUM(CASE WHEN conversions       > 0 THEN 1 ELSE 0 END)
         / COUNT(*) AS DECIMAL(5,2))                                    AS conversion_rate_pct
FROM dbo.fact_sessions;
GO


-- ============================================
-- View 4: Content Completion by Genre
-- Definition: completion rate and engagement metrics per genre + content tier.
-- avg_watch_duration_sec averages only terminal events — watch_duration_seconds
-- is NULL on content_play, and AVG skips NULLs (see concepts §6.4).
-- Owner: Content Team | Version: 1.0
-- ============================================
DROP VIEW IF EXISTS dbo.vw_content_completion;
GO
CREATE VIEW dbo.vw_content_completion AS
SELECT
    c.genre,
    c.content_tier,
    COUNT(*)                                                            AS total_interactions,
    SUM(CAST(e.is_completed AS INT))                                    AS completed,
    SUM(CAST(e.is_abandoned AS INT))                                    AS abandoned,
    COUNT(*) - SUM(CAST(e.is_completed AS INT))
             - SUM(CAST(e.is_abandoned AS INT))                         AS plays_only,
    CAST(AVG(e.completion_rate) AS DECIMAL(5,2))                        AS avg_completion_rate,
    CAST(AVG(CAST(e.watch_duration_seconds AS FLOAT)) AS INT)           AS avg_watch_duration_sec
FROM dbo.fact_content_engagement e
JOIN dbo.dim_content c ON e.content_id = c.content_id
GROUP BY c.genre, c.content_tier;
GO


-- ============================================
-- View 5: Campaign Attribution Summary
-- Definition: ad funnel metrics per campaign — impressions, clicks, conversions, revenue.
-- NULLIF guards both denominators so a campaign with zero clicks returns NULL, not an error.
-- Owner: Ads/Revenue Team | Version: 1.0
-- ============================================
DROP VIEW IF EXISTS dbo.vw_campaign_attribution;
GO
CREATE VIEW dbo.vw_campaign_attribution AS
SELECT
    c.campaign_id,
    c.advertiser_name,
    c.campaign_type,
    c.budget_tier,
    COUNT(*)                                                            AS impressions,
    COUNT(a.click_event_id)                                             AS clicks,
    COUNT(a.conversion_event_id)                                        AS conversions,
    CAST(100.0 * COUNT(a.click_event_id)
         / NULLIF(COUNT(*), 0) AS DECIMAL(5,2))                         AS click_through_rate_pct,
    CAST(100.0 * COUNT(a.conversion_event_id)
         / NULLIF(COUNT(a.click_event_id), 0) AS DECIMAL(5,2))          AS conversion_rate_pct,
    CAST(SUM(a.conversion_value) AS DECIMAL(12,2))                      AS total_conversion_value,
    CAST(AVG(CAST(a.time_to_click_seconds      AS FLOAT)) AS INT)       AS avg_time_to_click_sec,
    CAST(AVG(CAST(a.time_to_conversion_seconds AS FLOAT)) AS INT)       AS avg_time_to_conv_sec
FROM dbo.fact_ad_attribution a
JOIN dbo.dim_campaign c ON a.campaign_id = c.campaign_id
GROUP BY c.campaign_id, c.advertiser_name, c.campaign_type, c.budget_tier;
GO


-- ============================================
-- View 6: User Engagement by Archetype
-- Definition: session and content metrics segmented by behavioral archetype.
--
-- CRITICAL: dim_users is SCD-2. The is_current = 1 predicate is mandatory —
-- without it, the first tier change gives a user two dim rows and every one of
-- their sessions is counted twice. Harmless on the initial load, silently wrong
-- the moment history accumulates.
--
-- Owner: Product Team | Version: 1.0
-- ============================================
DROP VIEW IF EXISTS dbo.vw_user_engagement_by_archetype;
GO
CREATE VIEW dbo.vw_user_engagement_by_archetype AS
SELECT
    u.behavioral_archetype,
    u.subscription_tier,
    COUNT(DISTINCT s.user_id)                                           AS users,
    COUNT(*)                                                            AS total_sessions,
    CAST(AVG(CAST(s.duration_seconds AS FLOAT)) AS INT)                 AS avg_session_duration_sec,
    CAST(AVG(CAST(s.event_count      AS FLOAT)) AS DECIMAL(5,1))        AS avg_events_per_session,
    CAST(AVG(CAST(s.content_plays    AS FLOAT)) AS DECIMAL(5,2))        AS avg_content_plays,
    CAST(100.0 * SUM(CAST(s.is_bounce AS INT))
         / COUNT(*) AS DECIMAL(5,2))                                    AS bounce_rate_pct
FROM dbo.fact_sessions s
JOIN dbo.dim_users u
  ON s.user_id = u.user_id
 AND u.is_current = 1          -- SCD-2 guard: current version only
GROUP BY u.behavioral_archetype, u.subscription_tier;
GO


-- ============================================
-- Verify all 6 views
-- ============================================
SELECT * FROM dbo.vw_daily_active_users            ORDER BY activity_date;
SELECT * FROM dbo.vw_session_duration_distribution ORDER BY duration_bucket;
SELECT * FROM dbo.vw_funnel_conversion;
SELECT * FROM dbo.vw_content_completion            ORDER BY genre, content_tier;
SELECT * FROM dbo.vw_campaign_attribution          ORDER BY total_conversion_value DESC;
SELECT * FROM dbo.vw_user_engagement_by_archetype  ORDER BY behavioral_archetype, subscription_tier;

-- Sanity: view 1 must reconcile back to the fact table
SELECT
    (SELECT SUM(total_sessions) FROM dbo.vw_daily_active_users) AS sessions_via_view,
    (SELECT COUNT(*)            FROM dbo.fact_sessions)         AS sessions_in_fact,
    CASE WHEN (SELECT SUM(total_sessions) FROM dbo.vw_daily_active_users)
            = (SELECT COUNT(*) FROM dbo.fact_sessions)
         THEN 'PASS' ELSE 'FAIL' END                            AS view_reconciliation;
