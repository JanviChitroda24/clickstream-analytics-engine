# Fabric Notebook — Sessionization: 30-min Inactivity Timeout (Hour 19)
# --------------------------------------------------------------------
# Local mirror. In Fabric, create/rename to `03_sessionization` and attach
# `clickstream_lakehouse`. `spark` is provided by the runtime.
#
# Input:  silver_deduped_events (~1,116,783 unique, validated)
# Output: silver_sessionized_events  — every event + computed_session_id
#         silver_sessions            — one row per session (summary)
#
# Algorithm (gap-based, industry standard — GA / Adobe / Mixpanel):
#   1. Per user, order by event_timestamp
#   2. lag → previous timestamp; gap_seconds via unix_timestamp
#   3. is_new_session = 1 if first event OR gap > 1800s (30 min)
#   4. running sum(is_new_session) → session_number
#   5. computed_session_id = md5(user_id + "_" + session_number)
#
# Server-side sessionization is the source of truth (don't trust client
# session_id). Known limitation: long continuous watches with sparse
# events can split at the timeout — validated in Hour 20 sensitivity.
#
# NOTE: Does NOT require nested properties (H16 schema lock OK for H19
# core sessionization). content_id/genre enrichment can come later.
#
# Pipeline: Bronze → Validate → Dedup → Sessionize (H19) → Sensitivity (H20)

SESSION_TIMEOUT_SECONDS = 1800  # 30 minutes — matches simulator/config.py

# =============================================================================
# CELL 1 — Read Silver deduped events
# =============================================================================
events_df = spark.sql("SELECT * FROM silver_deduped_events")
total_events = events_df.count()
print(f"Input (silver_deduped_events): {total_events:,}")

# =============================================================================
# CELL 2 — Sessionization algorithm (30-min inactivity timeout)
# =============================================================================
from pyspark.sql.functions import (
    col, lag, when, lit, unix_timestamp, to_timestamp,
    sum as spark_sum, concat, md5,
    min as spark_min, max as spark_max,
    count, countDistinct,
)
from pyspark.sql.window import Window

# event_timestamp is STRING (ISO). Parse for gap math; ISO string order is OK for sort.
ts = to_timestamp(col("event_timestamp"))

user_window = Window.partitionBy("user_id").orderBy("event_timestamp")

# Step 1: previous event timestamp (string, for null check) + parsed for gaps
sessionized = events_df.withColumn(
    "prev_timestamp", lag("event_timestamp").over(user_window)
)

# Step 2: gap in seconds between consecutive events
sessionized = sessionized.withColumn(
    "gap_seconds",
    unix_timestamp(ts) - unix_timestamp(to_timestamp(col("prev_timestamp"))),
)

# Step 3: mark session boundaries
sessionized = sessionized.withColumn(
    "is_new_session",
    when(col("prev_timestamp").isNull(), lit(1))  # first event for this user
    .when(col("gap_seconds") > SESSION_TIMEOUT_SECONDS, lit(1))  # gap > 30 min
    .otherwise(lit(0)),
)

# Step 4: running sum of boundary marks = session number per user
# Default frame with orderBy = UNBOUNDED PRECEDING → CURRENT ROW (running sum)
sessionized = sessionized.withColumn(
    "session_number",
    spark_sum("is_new_session").over(user_window),
)

# Step 5: deterministic session_id (reproducible across re-runs)
sessionized = sessionized.withColumn(
    "computed_session_id",
    md5(concat(col("user_id"), lit("_"), col("session_number").cast("string"))),
)

print("Sample sessionized events (user_00259):")
sessionized.filter(col("user_id") == "user_00259") \
    .select(
        "event_timestamp", "event_type", "gap_seconds", "is_new_session",
        "session_number", "computed_session_id",
    ) \
    .orderBy("event_timestamp") \
    .show(20, truncate=False)

# =============================================================================
# CELL 3 — Write silver_sessionized_events
# =============================================================================
output_columns = [
    "event_id", "user_id", "computed_session_id", "event_type",
    "event_timestamp", "device_type", "app_version", "properties",
    "gap_seconds", "is_new_session", "session_number",
    "EventProcessedUtcTime", "PartitionId", "EventEnqueuedUtcTime",
]

sessionized.select(*output_columns) \
    .write.format("delta").mode("overwrite").saveAsTable("silver_sessionized_events")

print(f"✅ silver_sessionized_events: {total_events:,} rows written")

# =============================================================================
# CELL 4 — Session summary (one row per session)
# =============================================================================
session_summary = sessionized.groupBy("user_id", "computed_session_id").agg(
    spark_min("event_timestamp").alias("session_start"),
    spark_max("event_timestamp").alias("session_end"),
    count("*").alias("event_count"),
    countDistinct("event_type").alias("distinct_event_types"),
    spark_min("session_number").alias("session_number"),
)

session_summary = session_summary.withColumn(
    "duration_seconds",
    unix_timestamp(to_timestamp(col("session_end")))
    - unix_timestamp(to_timestamp(col("session_start"))),
)

total_sessions = session_summary.count()
print(f"Total sessions: {total_sessions:,}")

print("\nSession duration distribution (seconds):")
session_summary.select("duration_seconds").summary(
    "min", "25%", "50%", "75%", "max", "mean"
).show()

# =============================================================================
# CELL 5 — Write silver_sessions
# =============================================================================
session_summary.write.format("delta").mode("overwrite").saveAsTable("silver_sessions")
print(f"✅ silver_sessions: {total_sessions:,} rows written")

# =============================================================================
# CELL 6 — Sessionization analytics
# =============================================================================
sessions_df = spark.sql("SELECT * FROM silver_sessions")

print("Events per session distribution:")
sessions_df.select("event_count").summary(
    "min", "25%", "50%", "75%", "max", "mean"
).show()

sessions_per_user = sessions_df.groupBy("user_id").count() \
    .withColumnRenamed("count", "session_count")
print("Sessions per user distribution:")
sessions_per_user.select("session_count").summary(
    "min", "25%", "50%", "75%", "max", "mean"
).show()

print("Top 10 longest sessions:")
sessions_df.orderBy(col("duration_seconds").desc()) \
    .select(
        "user_id", "computed_session_id", "session_start", "session_end",
        "duration_seconds", "event_count",
    ) \
    .show(10, truncate=False)

# =============================================================================
# CELL 7 — Verification summary (screenshot this)
# =============================================================================
sess_event_count = spark.sql(
    "SELECT COUNT(*) as cnt FROM silver_sessionized_events"
).collect()[0]["cnt"]
sess_count = spark.sql(
    "SELECT COUNT(*) as cnt FROM silver_sessions"
).collect()[0]["cnt"]

avg_duration = sessions_df.agg({"duration_seconds": "avg"}).collect()[0][0]
avg_events = sessions_df.agg({"event_count": "avg"}).collect()[0][0]

print(f"{'=' * 60}")
print("SESSIONIZATION SUMMARY")
print(f"{'=' * 60}")
print(f"  Input events:              {total_events:,}")
print(f"  Output events:             {sess_event_count:,}")
print(
    f"  Event count match:         "
    f"{'✅ PASS' if sess_event_count == total_events else '❌ MISMATCH'}"
)
print(f"  Total sessions:            {sess_count:,}")
print(f"  Avg events per session:    {avg_events:.1f}")
print(
    f"  Avg session duration:      {avg_duration:.0f} seconds "
    f"({avg_duration / 60:.1f} minutes)"
)
print(
    f"  Session timeout used:      {SESSION_TIMEOUT_SECONDS} seconds "
    f"({SESSION_TIMEOUT_SECONDS // 60} minutes)"
)
