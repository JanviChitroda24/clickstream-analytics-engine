# Author: Janvi Chitroda
# Copyright (c) 2026 Janvi Chitroda. All rights reserved.
# Project: ClickStream Analytics Engine — Portfolio
# Unauthorized copying or redistribution of this content is prohibited.

# Fabric Notebook — Load Fact Tables
# --------------------------------------------
# Local mirror. Fabric name: `06_load_facts`.
# Attach: clickstream_lakehouse
#
# Same pattern as(05_load_dimensions):
#   1) Build facts in Lakehouse as *_staging Delta tables
#   2) INSERT into clickstream_warehouse.dbo.* via Warehouse SQL editor
#      → warehouse_ddl/load_facts_from_staging.sql
#
# WITH REAL PROPERTIES. Bronze now stores properties as a JSON
# STRING (see docs/properties_schema_fix.md), so get_json_object() works.
# Pre-fix, fact_content_engagement was all-NULL for content_id / durations.
#
# PROPERTY SHAPES DIFFER PER EVENT TYPE — see simulator/schemas.py.
# Do NOT assume all three content events carry the same keys:
#
#   content_play     content_id, genre, position_seconds,
#                    content_quality (v2.3.0 ONLY — 60% of users)
#   content_complete content_id, genre, watch_duration_seconds,
#                    content_duration_seconds
#   content_abandon  content_id, genre, position_seconds,
#                    content_duration_seconds, abandon_reason
#
# Consequences baked into Cell 2 below:
#   - abandon has NO watch_duration_seconds → use position_seconds as watched
#   - play has NO content_duration_seconds  → completion_rate is NULL (correct;
#     at play time nothing has been watched yet)
#   - content_quality is a PLAY-TIME attribute → NULL on complete/abandon
#
# NOTE: fact_content_engagement has no `genre` column by design — genre comes
# from joining dim_content on content_id (star schema; don't denormalize).

# =============================================================================
# CELL 0 — Imports + helpers
# =============================================================================
from pyspark.sql.functions import (
    col, lit, when, count, to_date, to_timestamp, unix_timestamp,
    get_json_object, hour, least,
    sum as spark_sum,
    min as spark_min,
    max as spark_max,
    round as spark_round,
)

WAREHOUSE = "clickstream_warehouse"  # docs only — load via Warehouse SQL

CONTENT_ENGAGEMENT_EVENTS = ["content_play", "content_complete", "content_abandon"]

SOURCE_TABLE = "silver_sessionized_events_aware"


def write_staging(df, table_name: str):
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    print(f"✅ {table_name} written to Lakehouse")


def prop(field: str):
    """Extract one field from the properties JSON string column."""
    return get_json_object(col("properties"), f"$.{field}")


# event_timestamp is an ISO STRING — always to_timestamp() before unix_timestamp().
# Same footgun; without it durations come back NULL.
def duration_seconds(start_col: str, end_col: str):
    return (
        unix_timestamp(to_timestamp(col(end_col)))
        - unix_timestamp(to_timestamp(col(start_col)))
    )


events_df = spark.sql(f"SELECT * FROM {SOURCE_TABLE}")
total_events = events_df.count()
print(f"Input ({SOURCE_TABLE}): {total_events:,} events")

# =============================================================================
# CELL 1 — fact_sessions → fact_sessions_staging
# Grain: one row per activity-aware session.
# Column order MUST match warehouse_ddl/create_facts.sql for INSERT … SELECT *.
# =============================================================================
sessions_agg = events_df.groupBy("user_id", "computed_session_id_aware").agg(
    spark_min("device_type").alias("device_type"),
    spark_min("app_version").alias("app_version"),
    spark_min("event_timestamp").alias("session_start"),
    spark_max("event_timestamp").alias("session_end"),
    count("*").alias("event_count"),
    spark_sum(when(col("event_type") == "content_play", 1).otherwise(0)).alias("content_plays"),
    spark_sum(when(col("event_type") == "content_complete", 1).otherwise(0)).alias("content_completes"),
    spark_sum(when(col("event_type") == "content_abandon", 1).otherwise(0)).alias("content_abandons"),
    spark_sum(when(col("event_type") == "search", 1).otherwise(0)).alias("searches"),
    spark_sum(when(col("event_type") == "ad_impression", 1).otherwise(0)).alias("ad_impressions"),
    spark_sum(when(col("event_type") == "ad_click", 1).otherwise(0)).alias("ad_clicks"),
    spark_sum(when(col("event_type") == "conversion", 1).otherwise(0)).alias("conversions"),
)

fact_sessions = (
    sessions_agg
    .withColumn("duration_seconds", duration_seconds("session_start", "session_end"))
    .withColumn("session_date", to_date(to_timestamp(col("session_start"))))
    .withColumn("session_hour", hour(to_timestamp(col("session_start"))))
    # Bounce = single-event session (landed and left without a second interaction)
    .withColumn("is_bounce", when(col("event_count") == 1, lit(1)).otherwise(lit(0)))
    # ISO string → real timestamps for the DATETIME2 warehouse columns
    .withColumn("session_start", to_timestamp(col("session_start")))
    .withColumn("session_end", to_timestamp(col("session_end")))
    .withColumnRenamed("computed_session_id_aware", "session_id")
)

fact_sessions_final = fact_sessions.select(
    "session_id",
    "user_id",
    "device_type",
    "app_version",
    "session_start",
    "session_end",
    "duration_seconds",
    "event_count",
    "content_plays",
    "content_completes",
    "content_abandons",
    "searches",
    "ad_impressions",
    "ad_clicks",
    "conversions",
    "session_date",
    "session_hour",
    "is_bounce",
)

total_sessions = fact_sessions_final.count()
bounces = fact_sessions_final.filter(col("is_bounce") == 1).count()
avg_duration = fact_sessions_final.agg({"duration_seconds": "avg"}).collect()[0][0]
avg_events = fact_sessions_final.agg({"event_count": "avg"}).collect()[0][0]

print(f"fact_sessions: {total_sessions:,} rows")
print(f"  Bounce sessions: {bounces:,} ({round(bounces / total_sessions * 100, 1)}%)")
print(f"  Avg duration:    {avg_duration:.0f}s ({avg_duration / 60:.1f} min)")
print(f"  Avg events:      {avg_events:.1f}")

print("\nDuration distribution — must NOT be all NULL:")
fact_sessions_final.select("duration_seconds").summary(
    "min", "25%", "50%", "75%", "max", "mean"
).show()

fact_sessions_final.show(5, truncate=False)
write_staging(fact_sessions_final, "fact_sessions_staging")

# =============================================================================
# CELL 2 — fact_content_engagement → fact_content_engagement_staging
# Grain: one row per content interaction (play / complete / abandon).
# This is where the properties fix pays off — real content_id, not NULL.
# =============================================================================
content_events = events_df.filter(col("event_type").isin(CONTENT_ENGAGEMENT_EVENTS))
total_content = content_events.count()
print(f"Content events: {total_content:,}")

content_duration_col = prop("content_duration_seconds").cast("int")

# "How much of this title did the user actually consume?"
#   complete → watch_duration_seconds
#   abandon  → position_seconds (where they stopped == how far they got)
#   play     → NULL (nothing watched yet; position_seconds is the START offset,
#              not consumption — do NOT coalesce it in here)
watched_seconds_col = (
    when(col("event_type") == "content_complete", prop("watch_duration_seconds").cast("int"))
    .when(col("event_type") == "content_abandon", prop("position_seconds").cast("int"))
    .otherwise(lit(None).cast("int"))
)

# Parenthesise the comparison — `&` binds tighter than `>` in Python.
completion_rate_col = (
    when(
        watched_seconds_col.isNotNull() & (content_duration_col > 0),
        least(
            lit(1.00),
            spark_round(watched_seconds_col / content_duration_col, 2),
        ),
    )
    .otherwise(lit(None))
    .cast("decimal(5,2)")
)

fact_engagement_final = content_events.select(
    # event_id is already a UUID v7 natural key — deterministic across re-runs,
    # unlike monotonically_increasing_id(), and joins back to bronze.
    col("event_id").alias("engagement_id"),
    col("user_id"),
    col("computed_session_id_aware").alias("session_id"),
    prop("content_id").alias("content_id"),
    col("event_type"),
    to_timestamp(col("event_timestamp")).alias("play_timestamp"),
    watched_seconds_col.alias("watch_duration_seconds"),
    content_duration_col.alias("content_duration_seconds"),
    completion_rate_col.alias("completion_rate"),
    when(col("event_type") == "content_complete", lit(1)).otherwise(lit(0)).alias("is_completed"),
    when(col("event_type") == "content_abandon", lit(1)).otherwise(lit(0)).alias("is_abandoned"),
    prop("content_quality").alias("content_quality"),
    to_date(to_timestamp(col("event_timestamp"))).alias("engagement_date"),
)

plays = fact_engagement_final.filter(col("event_type") == "content_play").count()
completes = fact_engagement_final.filter(col("is_completed") == 1).count()
abandons = fact_engagement_final.filter(col("is_abandoned") == 1).count()
has_content_id = fact_engagement_final.filter(col("content_id").isNotNull()).count()

print(f"\nfact_content_engagement: {total_content:,} rows")
print(f"  content_play:     {plays:,}")
print(f"  content_complete: {completes:,}")
print(f"  content_abandon:  {abandons:,}")
print(f"  Has content_id:   {has_content_id:,} ({round(has_content_id / total_content * 100, 1)}%)")

# Per-event-type NULL profile. Expected shape (NOT bugs — schema differs per type):
#   play    → watch_duration NULL, content_duration NULL, completion_rate NULL
#   complete→ content_quality NULL
#   abandon → content_quality NULL
print("\nNon-NULL counts by event_type (expected NULLs are schema-driven):")
fact_engagement_final.groupBy("event_type").agg(
    count("*").alias("rows"),
    count("content_id").alias("content_id"),
    count("watch_duration_seconds").alias("watch_dur"),
    count("content_duration_seconds").alias("content_dur"),
    count("completion_rate").alias("completion_rate"),
    count("content_quality").alias("content_quality"),
).orderBy("event_type").show(truncate=False)

print("completion_rate distribution (complete + abandon only, must be ≤ 1.00):")
fact_engagement_final.filter(col("completion_rate").isNotNull()).select(
    "completion_rate"
).summary("count", "min", "25%", "50%", "75%", "max", "mean").show()

fact_engagement_final.show(5, truncate=False)
write_staging(fact_engagement_final, "fact_content_engagement_staging")

# =============================================================================
# CELL 3 — Verify Lakehouse staging before the Warehouse INSERT
# =============================================================================
fs_count = spark.sql("SELECT COUNT(*) AS cnt FROM fact_sessions_staging").collect()[0]["cnt"]
fce_count = spark.sql("SELECT COUNT(*) AS cnt FROM fact_content_engagement_staging").collect()[0]["cnt"]

# Facts are built on computed_session_id_aware, so the baseline is
# silver_sessions — the activity-aware sessions written by notebook 03.
aware_count = spark.sql("SELECT COUNT(*) AS cnt FROM silver_sessions").collect()[0]["cnt"]

null_content_id = spark.sql(
    "SELECT COUNT(*) AS cnt FROM fact_content_engagement_staging WHERE content_id IS NULL"
).collect()[0]["cnt"]

dup_sessions = spark.sql(
    """
    SELECT COUNT(*) AS cnt FROM (
        SELECT session_id FROM fact_sessions_staging
        GROUP BY session_id HAVING COUNT(*) > 1
    )
    """
).collect()[0]["cnt"]

dup_engagements = spark.sql(
    """
    SELECT COUNT(*) AS cnt FROM (
        SELECT engagement_id FROM fact_content_engagement_staging
        GROUP BY engagement_id HAVING COUNT(*) > 1
    )
    """
).collect()[0]["cnt"]

bad_rate = spark.sql(
    """
    SELECT COUNT(*) AS cnt FROM fact_content_engagement_staging
    WHERE completion_rate IS NOT NULL
      AND (completion_rate < 0 OR completion_rate > 1)
    """
).collect()[0]["cnt"]

orphan_sessions = spark.sql(
    """
    SELECT COUNT(*) AS cnt
    FROM fact_content_engagement_staging e
    LEFT ANTI JOIN fact_sessions_staging s ON e.session_id = s.session_id
    """
).collect()[0]["cnt"]

print(f"{'=' * 60}")
print("FACT STAGING VERIFICATION (Lakehouse)")
print(f"{'=' * 60}")
print(f"  fact_sessions_staging:            {fs_count:,}")
print(f"  silver_sessions (baseline):       {aware_count:,}")
print(f"  Grain match:                      {'✅ PASS' if fs_count == aware_count else '❌ MISMATCH'}")
print(f"  fact_content_engagement_staging:  {fce_count:,}")
print()
print(f"  NULL content_id:                  {null_content_id:,} {'✅' if null_content_id == 0 else '❌ properties fix not applied'}")
print(f"  Duplicate session_id:             {dup_sessions:,} {'✅' if dup_sessions == 0 else '❌'}")
print(f"  Duplicate engagement_id:          {dup_engagements:,} {'✅' if dup_engagements == 0 else '❌'}")
print(f"  completion_rate outside [0,1]:    {bad_rate:,} {'✅' if bad_rate == 0 else '❌'}")
print(f"  Engagements w/o parent session:   {orphan_sessions:,} {'✅' if orphan_sessions == 0 else '❌'}")

# =============================================================================
# CELL 4 — Next step is Warehouse SQL (verified path)
# Spark cross-item INSERT into the warehouse is flaky — use the SQL editor.
# =============================================================================
print(
    f"""
✅ Lakehouse staging verified.

Next — run in {WAREHOUSE} SQL editor:
  warehouse_ddl/load_facts_from_staging.sql

Expected warehouse counts after INSERT:
  fact_sessions             {fs_count:,}
  fact_content_engagement   {fce_count:,}
"""
)
