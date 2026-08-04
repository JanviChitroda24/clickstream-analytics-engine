# Author: Janvi Chitroda
# Copyright (c) 2026 Janvi Chitroda. All rights reserved.
# Project: ClickStream Analytics Engine — Portfolio
# Unauthorized copying or redistribution of this content is prohibited.

# Fabric Notebook — Bronze Layer Verification (Hour 16 + properties re-ingest)
# -----------------------------------------------------------------------------
# Local mirror. Run inside a Fabric Notebook attached to `clickstream_lakehouse`.
# `spark` is provided by the Fabric runtime (do NOT create a SparkSession).
#
# ORIGINAL VERIFY (2026-07-27): 1,144,502 rows · 1.97% dups · properties LOCKED
#   properties was STRUCT<test_index:BIGINT> → content_play showed {NULL}.
#
# RE-INGEST FIX (before Hour 23): see docs/properties_schema_fix.md
#   Delete Lakehouse tables → recreate Eventstream Lakehouse dest →
#   python -m simulator.main --days 14 → re-run this notebook.
#   PASS = properties shows content_id / genre (not {NULL}).

TABLE_NAME = "raw_events"

# =============================================================================
# CELL 1 — List all tables in the Lakehouse
# =============================================================================
display(spark.sql("SHOW TABLES"))
# Expected after re-ingest: raw_events (+ Silver tables after notebooks 01–04)

# =============================================================================
# CELL 2 — Read Bronze Delta table: row count + schema
# =============================================================================
bronze_df = spark.sql(f"SELECT * FROM {TABLE_NAME}")

print(f"Row count: {bronze_df.count():,}")
print(f"\nSchema:")
bronze_df.printSchema()
# After fix: properties should NOT be STRUCT<test_index:BIGINT> only.
# Acceptable: STRING (JSON), MAP, or a STRUCT that includes content_id / genre.

# =============================================================================
# CELL 3 — Event type distribution (should match Eventhouse roughly)
# =============================================================================
bronze_df.groupBy("event_type").count().orderBy("count", ascending=False).show(20, truncate=False)

# =============================================================================
# CELL 4 — Timestamp range (string min/max; ISO sorts correctly)
# =============================================================================
from pyspark.sql.functions import min as spark_min, max as spark_max

bronze_df.select(
    spark_min("event_timestamp").alias("earliest"),
    spark_max("event_timestamp").alias("latest"),
).show(truncate=False)
# Chaos may still put "not-a-timestamp" as string max — expected.

# =============================================================================
# CELL 5 — properties column inspection (PASS criteria after re-ingest)
# =============================================================================
from pyspark.sql.functions import col, get_json_object

print("Sample content_play properties (must NOT be {NULL}):")
bronze_df.filter(col("event_type") == "content_play") \
    .select("event_id", "event_type", "properties") \
    .show(5, truncate=False)

# Try nested STRUCT access first; fall back to JSON string parse.
props_type = dict(bronze_df.dtypes).get("properties", "")
print(f"\nproperties dtype: {props_type}")

if props_type.startswith("struct") or props_type.startswith("map"):
    try:
        bronze_df.filter(col("event_type") == "content_play") \
            .select(
                "event_id",
                col("properties.content_id").alias("content_id"),
                col("properties.genre").alias("genre"),
            ).show(5, truncate=False)
    except Exception as e:  # noqa: BLE001 — Fabric dtype variants
        print(f"STRUCT/MAP nested select failed: {e}")
elif "string" in props_type.lower() or props_type == "":
    bronze_df.filter(col("event_type") == "content_play") \
        .select(
            "event_id",
            "user_id",
            "event_type",
            get_json_object("properties", "$.content_id").alias("content_id"),
            get_json_object("properties", "$.genre").alias("genre"),
        ).show(5, truncate=False)

# PASS: content_id like content_042, genre like action/drama — not all null.

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
# CELL 7 — Bronze layer summary
# =============================================================================
total = bronze_df.count()
distinct_users = bronze_df.select("user_id").distinct().count()
distinct_sessions = bronze_df.select("session_id").distinct().count()
distinct_events = bronze_df.select("event_id").distinct().count()
duplicate_count = total - distinct_events

# Non-null content_id sample rate on content_play (fix gate)
content_plays = bronze_df.filter(col("event_type") == "content_play")
cp_total = content_plays.count()
props_ok = "UNKNOWN"
try:
    if props_type.startswith("struct") or props_type.startswith("map"):
        non_null_cid = content_plays.filter(col("properties.content_id").isNotNull()).count()
    else:
        non_null_cid = content_plays.filter(
            get_json_object("properties", "$.content_id").isNotNull()
        ).count()
    pct = (non_null_cid / cp_total * 100) if cp_total else 0.0
    props_ok = f"{non_null_cid:,}/{cp_total:,} content_play have content_id ({pct:.1f}%)"
except Exception as e:  # noqa: BLE001
    props_ok = f"could not score content_id: {e}"

print(f"{'=' * 60}")
print("BRONZE LAYER VERIFICATION")
print(f"{'=' * 60}")
print(f"  Total rows:         {total:,}")
print(f"  Distinct users:     {distinct_users:,}")
print(f"  Distinct sessions:  {distinct_sessions:,}")
print(f"  Distinct event_ids: {distinct_events:,}")
print(f"  Duplicate rows:     {duplicate_count:,} ({duplicate_count / total * 100:.2f}%)")
print(f"  properties check:   {props_ok}")
print(f"  PASS rule:          content_id present on content_play (not schema-locked NULL)")
