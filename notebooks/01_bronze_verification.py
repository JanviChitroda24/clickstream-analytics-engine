# Fabric Notebook — Bronze Layer Verification (Hour 16)
# -------------------------------------------------------
# Local mirror of the Day 4 cold-path verification notebook.
# Run inside a Fabric Notebook attached to `clickstream_lakehouse`.
# `spark` is provided by the Fabric runtime (do NOT create a SparkSession).
#
# Goal: prove Eventstream dual-write landed ~1.14M events as Delta BEFORE
# Hours 17–19 (schema validation, dedup, sessionization).
#
# VERIFIED (2026-07-27):
#   Table name: raw_events (NOT bronze_events)
#   Row count:  1,144,502  (~232 above Eventhouse from Hour 15 trigger tests)
#   Duplicates: 22,504 (1.97%) — matches ~2% chaos rate
#
# KNOWN ISSUE — properties column schema lock:
#   Eventstream inferred properties as STRUCT<test_index: BIGINT> from the
#   Hour 4 smoke-test payloads ({"test_index": 0}). Real simulator fields
#   (content_id, genre, campaign_id, …) did NOT land in that struct —
#   content_play rows show properties = {NULL}.
#   get_json_object() fails: expects STRING, got STRUCT.
#   Fix path (next): recreate Lakehouse destination with open schema /
#   properties as string|JSON, then re-ingest — or recover from raw files
#   if available. Documented in notes Hour 16 Issue Log.

TABLE_NAME = "raw_events"

# =============================================================================
# CELL 1 — List all tables in the Lakehouse
# =============================================================================
display(spark.sql("SHOW TABLES"))
# Expected: tableName = raw_events under clickstream_lakehouse.dbo

# =============================================================================
# CELL 2 — Read Bronze Delta table: row count + schema
# Prefer spark.sql over Tables/ path when the table is registered in the metastore.
# =============================================================================
bronze_df = spark.sql(f"SELECT * FROM {TABLE_NAME}")
# Alternative: spark.read.format("delta").load(f"Tables/{TABLE_NAME}")

print(f"Row count: {bronze_df.count():,}")
print(f"\nSchema:")
bronze_df.printSchema()
# Expected schema highlights:
#   event_timestamp: string
#   properties: struct (test_index: long)  ← WRONG for simulator data (schema lock)
#   EventProcessedUtcTime, PartitionId, EventEnqueuedUtcTime present

# =============================================================================
# CELL 3 — Event type distribution (should match Eventhouse)
# =============================================================================
bronze_df.groupBy("event_type").count().orderBy("count", ascending=False).show(20, truncate=False)
# Verified: content_play 301145, content_complete 152469, … NULL 1701, conversion 647

# =============================================================================
# CELL 4 — Timestamp range (string min/max; ISO sorts correctly)
# =============================================================================
from pyspark.sql.functions import min as spark_min, max as spark_max

bronze_df.select(
    spark_min("event_timestamp").alias("earliest"),
    spark_max("event_timestamp").alias("latest"),
).show(truncate=False)
# Verified: earliest 2026-07-01T06:00:19.000Z, latest "not-a-timestamp"
# (string max puts "not-a-timestamp" above ISO dates — chaos artifact, expected)

# =============================================================================
# CELL 5 — properties column inspection
# NOTE: get_json_object(properties, ...) FAILS — properties is STRUCT, not STRING.
# Show the struct as stored; nested simulator fields are missing (schema lock).
# =============================================================================
from pyspark.sql.functions import col

bronze_df.filter(col("event_type") == "content_play") \
    .select("event_id", "event_type", "properties") \
    .show(5, truncate=False)
# Verified: properties shows {NULL} for content_play — real JSON was dropped.

# Do NOT run this until properties is a STRING again:
# from pyspark.sql.functions import get_json_object
# bronze_df.filter(col("event_type") == "content_play") \
#     .select(
#         "event_id", "user_id", "event_type", "properties",
#         get_json_object("properties", "$.content_id").alias("content_id"),
#         get_json_object("properties", "$.genre").alias("genre"),
#     ).show(5, truncate=False)

# =============================================================================
# CELL 6 — Chaos injector artifacts (nulls / bad timestamps)
# =============================================================================
from pyspark.sql.functions import count, when

bronze_df.select(
    count("*").alias("total"),
    count(when(col("user_id").isNull() | (col("user_id") == ""), 1)).alias("null_user_id"),
    count(when(col("event_type").isNull() | (col("event_type") == ""), 1)).alias("null_event_type"),
    count(when(col("event_timestamp") == "not-a-timestamp", 1)).alias("bad_timestamp"),
).show()
# Verified: total 1144502 | null_user 1868 | null_type 1701 | bad_ts 1715

# =============================================================================
# CELL 7 — Bronze layer summary (screenshot for README / notes)
# =============================================================================
total = bronze_df.count()
distinct_users = bronze_df.select("user_id").distinct().count()
distinct_sessions = bronze_df.select("session_id").distinct().count()
distinct_events = bronze_df.select("event_id").distinct().count()
duplicate_count = total - distinct_events

print(f"{'=' * 60}")
print("BRONZE LAYER VERIFICATION")
print(f"{'=' * 60}")
print(f"  Total rows:         {total:,}")
print(f"  Distinct users:     {distinct_users:,}")
print(f"  Distinct sessions:  {distinct_sessions:,}")
print(f"  Distinct event_ids: {distinct_events:,}")
print(f"  Duplicate rows:     {duplicate_count:,} ({duplicate_count / total * 100:.2f}%)")
print(f"  properties type:    STRUCT<test_index:BIGINT> — SCHEMA LOCK (fix before H17)")
# Verified: 1,144,502 | users 9,932 | sessions 127,386 | event_ids 1,121,998 | dups 22,504 (1.97%)
