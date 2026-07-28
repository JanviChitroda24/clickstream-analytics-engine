-- Author: Janvi Chitroda
-- Copyright (c) 2026 Janvi Chitroda. All rights reserved.
-- Project: ClickStream Analytics Engine — Portfolio
-- Unauthorized copying or redistribution of this content is prohibited.

-- ============================================
-- Verify Hour 21 star schema tables
-- ============================================
SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'dbo'
ORDER BY TABLE_NAME;

-- Expect 7 rows:
--   dim_campaign, dim_content, dim_date, dim_users
--   fact_ad_attribution, fact_content_engagement, fact_sessions
