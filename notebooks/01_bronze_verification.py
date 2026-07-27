# Fabric Notebook — Bronze Layer Verification (Hour 16)
# -------------------------------------------------------
# Local mirror of the Day 4 cold-path verification notebook.
# Run inside a Fabric Notebook attached to `clickstream_lakehouse`.
# `spark` is provided by the Fabric runtime (do NOT create a SparkSession).
#
# Goal: prove Eventstream dual-write landed ~1.14M events as Delta in the
# Lakehouse BEFORE Hours 17–19 (schema validation, dedup, sessionization).
#
# Table name: Eventstream → Lakehouse destination was configured as
# `bronze_events` in the plan (Hour 4). If your explorer shows `raw_events`
# instead, set TABLE_NAME below accordingly.

TABLE_NAME = "bronze_events"  # or "raw_events" if that's what Hour 4 created

# =============================================================================
# CELL 1 — List all tables in the Lakehouse
# =============================================================================
display(spark.sql("SHOW TABLES"))

# =============================================================================
# CELL 2 — Read Bronze Delta table: row count + schema
# =============================================================================
bronze_df = spark.read.format("delta").load(f"Tables/{TABLE_NAME}")

print(f"Row count: {bronze_df.count():,}")
print(f"\nSchema:")
bronze_df.printSchema()

# =============================================================================
# CELL 3 — Event type distribution (should match Eventhouse ~content_play 301K, etc.)
# =============================================================================
bronze_df.groupBy("event_type").count().orderBy("count", ascending=False).show(20, truncate=False)

# =============================================================================
# CELL 4 — Timestamp range (expect ~July 1 → July 16, incl. late arrivals)
# =============================================================================
from pyspark.sql.functions import min as spark_min, max as spark_max

bronze_df.select(
    spark_min("event_timestamp").alias("earliest"),
    spark_max("event_timestamp").alias("latest"),
).show(truncate=False)

# =============================================================================
# CELL 5 — properties column is parseable JSON (extract nested fields)
# =============================================================================
from pyspark.sql.functions import col, get_json_object

bronze_df.filter(col("event_type") == "content_play") \
    .select(
        "event_id",
        "user_id",
        "event_type",
        "properties",
        get_json_object("properties", "$.content_id").alias("content_id"),
        get_json_object("properties", "$.genre").alias("genre"),
    ) \
    .show(5, truncate=False)

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

# =============================================================================
# CELL 7 — Bronze layer summary (screenshot this for README / notes)
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
