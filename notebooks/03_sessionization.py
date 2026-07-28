# Fabric Notebook — Dual-Layer Sessionization (Hour 19)
# ----------------------------------------------------
# Local mirror. Fabric name: `03_sessionization`. Attach `clickstream_lakehouse`.
#
# CRITICAL: event_timestamp is an ISO STRING ("2026-07-01T08:36:53.000Z").
# Always wrap with to_timestamp() BEFORE unix_timestamp().
# Without it, gap_seconds / duration_seconds are NULL → one fake session per user.
#
# LAYER 1 — Platform visit sessions ("how many times did they visit?")
#   A) Basic: flat 30-min inactivity timeout → silver_sessionized_events + silver_sessions
#   B) Activity-aware: 30-min default, 60-min during content continuation
#      → silver_sessionized_events_aware + silver_sessions_aware
#
# LAYER 2 — Viewing streaks ("how intensely did they engage with content?")
#   Content events with < 5-min gaps → silver_viewing_streaks (binge = 3+ titles)
#
# Do NOT overwrite silver_sessions with aware rows — that breaks basic vs aware compare.
#
# Pipeline: Dedup → Sessionize (H19) → Sensitivity 15/30/60 (H20)

SESSION_TIMEOUT_SECONDS = 1800  # 30 min — default platform visit / non-content gaps
CONTENT_TIMEOUT_SECONDS = 3600  # 60 min — content continuation (3h was too aggressive)
VIEWING_GAP_SECONDS = 300       # 5 min — viewing streak continuity

CONTENT_ACTIVE_START = ["content_play", "content_resume"]
CONTENT_CONTINUATION = [
    "content_pause", "content_resume", "content_complete",
    "content_abandon", "content_play",
]
CONTENT_EVENTS = [
    "content_play", "content_pause", "content_resume",
    "content_complete", "content_abandon",
]

# =============================================================================
# CELL 1 — Read Silver deduped events + helpers
# =============================================================================
from pyspark.sql.functions import (
    col, lag, when, lit, unix_timestamp, to_timestamp,
    sum as spark_sum, concat, md5, least,
    min as spark_min, max as spark_max,
    count, countDistinct, round as spark_round,
)
from pyspark.sql.window import Window

events_df = spark.sql("SELECT * FROM silver_deduped_events")
total_events = events_df.count()
print(f"Input (silver_deduped_events): {total_events:,}")

user_window = Window.partitionBy("user_id").orderBy("event_timestamp")


def gap_seconds(ts_col, prev_col):
    """ISO string timestamps → epoch-second gap. MUST use to_timestamp first."""
    return (
        unix_timestamp(to_timestamp(col(ts_col)))
        - unix_timestamp(to_timestamp(col(prev_col)))
    )


def duration_seconds(start_col, end_col):
    """ISO string start/end → duration in seconds."""
    return (
        unix_timestamp(to_timestamp(col(end_col)))
        - unix_timestamp(to_timestamp(col(start_col)))
    )


# =============================================================================
# CELL 2 — LAYER 1A: Basic platform visit sessionization (30-min flat)
# =============================================================================
sessionized = (
    events_df.withColumn("prev_timestamp", lag("event_timestamp").over(user_window))
    .withColumn("gap_seconds", gap_seconds("event_timestamp", "prev_timestamp"))
    .withColumn(
        "is_new_session",
        when(col("prev_timestamp").isNull(), lit(1))
        .when(col("gap_seconds") > SESSION_TIMEOUT_SECONDS, lit(1))
        .otherwise(lit(0)),
    )
    .withColumn("session_number", spark_sum("is_new_session").over(user_window))
    .withColumn(
        "computed_session_id",
        md5(concat(col("user_id"), lit("_"), col("session_number").cast("string"))),
    )
)

print("Sample basic sessionization (user_00259) — gap_seconds must NOT be NULL:")
sessionized.filter(col("user_id") == "user_00259").select(
    "event_timestamp",
    "event_type",
    "gap_seconds",
    "is_new_session",
    "session_number",
    "computed_session_id",
).orderBy("event_timestamp").show(20, truncate=False)

null_gaps = sessionized.filter(
    col("prev_timestamp").isNotNull() & col("gap_seconds").isNull()
).count()
print(f"Non-null prev with NULL gap_seconds (should be 0): {null_gaps:,}")

# =============================================================================
# CELL 3 — Write basic silver_sessionized_events
# =============================================================================
basic_event_cols = [
    "event_id",
    "user_id",
    "computed_session_id",
    "event_type",
    "event_timestamp",
    "device_type",
    "app_version",
    "properties",
    "gap_seconds",
    "is_new_session",
    "session_number",
    "EventProcessedUtcTime",
    "PartitionId",
    "EventEnqueuedUtcTime",
]

sessionized.select(*basic_event_cols).write.format("delta").mode("overwrite").saveAsTable(
    "silver_sessionized_events"
)
print(f"✅ silver_sessionized_events: {total_events:,} rows written")

# =============================================================================
# CELL 4 — Basic session summary → silver_sessions (warehouse baseline)
# =============================================================================
session_summary = (
    sessionized.groupBy("user_id", "computed_session_id")
    .agg(
        spark_min("event_timestamp").alias("session_start"),
        spark_max("event_timestamp").alias("session_end"),
        count("*").alias("event_count"),
        countDistinct("event_type").alias("distinct_event_types"),
        spark_min("session_number").alias("session_number"),
    )
    .withColumn("duration_seconds", duration_seconds("session_start", "session_end"))
)

total_sessions_basic = session_summary.count()
print(f"Total basic sessions (30-min flat): {total_sessions_basic:,}")
print("\nSession duration distribution (seconds) — must NOT be all NULL:")
session_summary.select("duration_seconds").summary(
    "min", "25%", "50%", "75%", "max", "mean"
).show()

session_summary.write.format("delta").mode("overwrite").saveAsTable("silver_sessions")
print(f"✅ silver_sessions: {total_sessions_basic:,} rows written")

# =============================================================================
# CELL 5 — Basic sessionization analytics
# =============================================================================
sessions_df = spark.sql("SELECT * FROM silver_sessions")

print("Events per session distribution:")
sessions_df.select("event_count").summary(
    "min", "25%", "50%", "75%", "max", "mean"
).show()

sessions_per_user = sessions_df.groupBy("user_id").count().withColumnRenamed(
    "count", "session_count"
)
print("Sessions per user distribution:")
sessions_per_user.select("session_count").summary(
    "min", "25%", "50%", "75%", "max", "mean"
).show()

print("Top 10 longest basic sessions:")
sessions_df.orderBy(col("duration_seconds").desc()).select(
    "user_id",
    "computed_session_id",
    "session_start",
    "session_end",
    "duration_seconds",
    "event_count",
).show(10, truncate=False)

# =============================================================================
# CELL 6 — LAYER 1B: Activity-aware platform sessions (30m / 60m)
# =============================================================================
aware_df = (
    events_df.withColumn("prev_timestamp", lag("event_timestamp").over(user_window))
    .withColumn("prev_event_type", lag("event_type").over(user_window))
    .withColumn("gap_seconds", gap_seconds("event_timestamp", "prev_timestamp"))
    .withColumn(
        "is_content_continuation",
        when(
            col("prev_event_type").isin(
                CONTENT_ACTIVE_START
                + ["content_complete", "content_pause", "content_abandon"]
            )
            & col("event_type").isin(CONTENT_CONTINUATION),
            lit(True),
        ).otherwise(lit(False)),
    )
    .withColumn(
        "effective_timeout",
        when(col("is_content_continuation"), lit(CONTENT_TIMEOUT_SECONDS)).otherwise(
            lit(SESSION_TIMEOUT_SECONDS)
        ),
    )
    .withColumn(
        "is_new_session_aware",
        when(col("prev_timestamp").isNull(), lit(1))
        .when(col("gap_seconds") > col("effective_timeout"), lit(1))
        .otherwise(lit(0)),
    )
    .withColumn(
        "session_number_aware", spark_sum("is_new_session_aware").over(user_window)
    )
    .withColumn(
        "computed_session_id_aware",
        md5(
            concat(
                col("user_id"),
                lit("_aware_"),
                col("session_number_aware").cast("string"),
            )
        ),
    )
)

print("Activity-aware sample (user_00259):")
aware_df.filter(col("user_id") == "user_00259").select(
    "event_timestamp",
    "event_type",
    "prev_event_type",
    "gap_seconds",
    "is_content_continuation",
    "effective_timeout",
    "is_new_session_aware",
    "session_number_aware",
).orderBy("event_timestamp").show(30, truncate=False)

# Compare basic vs aware from in-memory DFs (not from overwritten tables)
basic_per_user = sessionized.groupBy("user_id").agg(
    spark_max("session_number").alias("basic_session_count")
)
aware_per_user = aware_df.groupBy("user_id").agg(
    spark_max("session_number_aware").alias("aware_session_count")
)
comparison = basic_per_user.join(aware_per_user, "user_id").withColumn(
    "sessions_saved", col("basic_session_count") - col("aware_session_count")
)

total_basic = comparison.agg(spark_sum("basic_session_count")).collect()[0][0]
total_aware = comparison.agg(spark_sum("aware_session_count")).collect()[0][0]
total_saved = comparison.agg(spark_sum("sessions_saved")).collect()[0][0]
users_affected = comparison.filter(col("sessions_saved") > 0).count()

print(f"{'=' * 60}")
print("ACTIVITY-AWARE SESSION COMPARISON")
print(f"{'=' * 60}")
print(f"  Content timeout:                    {CONTENT_TIMEOUT_SECONDS}s (60 min)")
print(f"  Default timeout:                    {SESSION_TIMEOUT_SECONDS}s (30 min)")
print(f"  Basic sessions (30-min flat):       {total_basic:,}")
print(f"  Activity-aware sessions:            {total_aware:,}")
print(f"  False splits prevented:             {total_saved:,}")
print(f"  Users affected:                     {users_affected:,}")
if total_basic:
    print(f"  Reduction:                          {round((total_saved / total_basic) * 100, 2)}%")

print("\nTop 10 users with most sessions saved:")
comparison.filter(col("sessions_saved") > 0).orderBy(
    col("sessions_saved").desc()
).show(10, truncate=False)

aware_cols = [
    "event_id",
    "user_id",
    "computed_session_id_aware",
    "event_type",
    "event_timestamp",
    "device_type",
    "app_version",
    "properties",
    "gap_seconds",
    "is_new_session_aware",
    "session_number_aware",
    "is_content_continuation",
    "effective_timeout",
    "EventProcessedUtcTime",
    "PartitionId",
    "EventEnqueuedUtcTime",
]
aware_df.select(*aware_cols).write.format("delta").mode("overwrite").saveAsTable(
    "silver_sessionized_events_aware"
)
print("✅ silver_sessionized_events_aware written")

# Aware session summary — SEPARATE table (do not overwrite silver_sessions)
aware_summary = (
    aware_df.groupBy("user_id", "computed_session_id_aware")
    .agg(
        spark_min("event_timestamp").alias("session_start"),
        spark_max("event_timestamp").alias("session_end"),
        count("*").alias("event_count"),
        countDistinct("event_type").alias("distinct_event_types"),
        spark_min("session_number_aware").alias("session_number"),
    )
    .withColumn("duration_seconds", duration_seconds("session_start", "session_end"))
)

aware_summary.write.format("delta").mode("overwrite").saveAsTable("silver_sessions_aware")
total_sessions_aware = aware_summary.count()
avg_duration_aware = (
    aware_summary.filter(col("duration_seconds").isNotNull())
    .agg({"duration_seconds": "avg"})
    .collect()[0][0]
)
print(f"✅ silver_sessions_aware: {total_sessions_aware:,} rows written")
print(
    f"   Avg duration (aware): {avg_duration_aware:.0f}s "
    f"({avg_duration_aware / 60:.1f} min)"
)

# =============================================================================
# CELL 7 — LAYER 2: Viewing streak detection (binge / content continuity)
# =============================================================================
streak_df = (
    spark.sql("SELECT * FROM silver_sessionized_events_aware")
    .withColumn(
        "is_content_event",
        when(col("event_type").isin(CONTENT_EVENTS), lit(True)).otherwise(lit(False)),
    )
    .withColumn("prev_timestamp_s", lag("event_timestamp").over(user_window))
    .withColumn("prev_event_type_s", lag("event_type").over(user_window))
    .withColumn(
        "gap_seconds_s", gap_seconds("event_timestamp", "prev_timestamp_s")
    )
    .withColumn(
        "is_new_viewing_streak",
        when(~col("is_content_event"), lit(0))
        .when(col("prev_event_type_s").isNull(), lit(1))
        .when(~col("prev_event_type_s").isin(CONTENT_EVENTS), lit(1))
        .when(col("gap_seconds_s") > VIEWING_GAP_SECONDS, lit(1))
        .otherwise(lit(0)),
    )
    .withColumn(
        "viewing_streak_number", spark_sum("is_new_viewing_streak").over(user_window)
    )
)

viewing_streaks = (
    streak_df.filter(col("is_content_event") == True)
    .groupBy("user_id", "viewing_streak_number")
    .agg(
        spark_min("event_timestamp").alias("streak_start"),
        spark_max("event_timestamp").alias("streak_end"),
        count("*").alias("content_events"),
        countDistinct("event_type").alias("distinct_content_types"),
        spark_sum(when(col("event_type") == "content_play", 1).otherwise(0)).alias(
            "titles_played"
        ),
        spark_sum(
            when(col("event_type") == "content_complete", 1).otherwise(0)
        ).alias("titles_completed"),
        spark_sum(when(col("event_type") == "content_abandon", 1).otherwise(0)).alias(
            "titles_abandoned"
        ),
    )
    .withColumn(
        "streak_duration_seconds", duration_seconds("streak_start", "streak_end")
    )
    .withColumn(
        # Cap at 100 — completes can exceed plays (resume→complete without new play)
        "completion_rate",
        when(
            col("titles_played") > 0,
            least(
                lit(100.0),
                spark_round(
                    col("titles_completed") / col("titles_played") * 100, 1
                ),
            ),
        ).otherwise(lit(0.0)),
    )
)

total_streaks = viewing_streaks.count()
print(f"{'=' * 60}")
print("VIEWING STREAK ANALYSIS")
print(f"{'=' * 60}")
print(f"  Total viewing streaks: {total_streaks:,}")

print("\nViewing streak duration distribution (seconds) — must NOT be all NULL:")
viewing_streaks.select("streak_duration_seconds").summary(
    "min", "25%", "50%", "75%", "max", "mean"
).show()

print("\nTitles per viewing streak:")
viewing_streaks.select("titles_played").summary(
    "min", "25%", "50%", "75%", "max", "mean"
).show()

print("\nCompletion rate per streak (capped at 100%):")
viewing_streaks.select("completion_rate").summary(
    "min", "25%", "50%", "75%", "max", "mean"
).show()

binge_streaks = viewing_streaks.filter(col("titles_played") >= 3)
binge_count = binge_streaks.count()
binge_pct = round(binge_count / total_streaks * 100, 1) if total_streaks else 0.0

print("\nBinge watching (3+ titles in one streak):")
print(f"  Binge streaks: {binge_count:,} ({binge_pct}% of all viewing streaks)")

print("\nTop 10 longest binge sessions (by titles_played):")
binge_streaks.orderBy(col("titles_played").desc()).select(
    "user_id",
    "streak_start",
    "streak_end",
    "streak_duration_seconds",
    "titles_played",
    "titles_completed",
    "titles_abandoned",
    "completion_rate",
).show(10, truncate=False)

viewing_streaks.write.format("delta").mode("overwrite").saveAsTable(
    "silver_viewing_streaks"
)
print(f"✅ silver_viewing_streaks: {total_streaks:,} rows written")

# =============================================================================
# CELL 8 — Dual-layer summary + verification
# =============================================================================
sess_event_count = spark.sql(
    "SELECT COUNT(*) as cnt FROM silver_sessionized_events"
).collect()[0]["cnt"]
basic_count = spark.sql("SELECT COUNT(*) as cnt FROM silver_sessions").collect()[0][
    "cnt"
]
aware_count = spark.sql(
    "SELECT COUNT(*) as cnt FROM silver_sessions_aware"
).collect()[0]["cnt"]
streak_count = spark.sql(
    "SELECT COUNT(*) as cnt FROM silver_viewing_streaks"
).collect()[0]["cnt"]
binge_count_final = spark.sql(
    "SELECT COUNT(*) as cnt FROM silver_viewing_streaks WHERE titles_played >= 3"
).collect()[0]["cnt"]

avg_duration_basic = sessions_df.agg({"duration_seconds": "avg"}).collect()[0][0]
avg_events_basic = sessions_df.agg({"event_count": "avg"}).collect()[0][0]

print(f"{'=' * 60}")
print("DUAL-LAYER SESSIONIZATION SUMMARY")
print(f"{'=' * 60}")
print(f"  Input events:                     {total_events:,}")
print(f"  Output events (basic):            {sess_event_count:,}")
print(
    f"  Event count match:                "
    f"{'✅ PASS' if sess_event_count == total_events else '❌ MISMATCH'}"
)
print()
print("  LAYER 1: Platform Visit Sessions")
print("  ─────────────────────────────────")
print(f"  Basic (30-min flat) [silver_sessions]:           {basic_count:,}")
print(f"  Activity-aware (30m/60m) [silver_sessions_aware]: {aware_count:,}")
print(f"  False splits prevented:                          {basic_count - aware_count:,}")
print(f"  Avg events/session (basic):                      {avg_events_basic:.1f}")
print(
    f"  Avg duration (basic):                            {avg_duration_basic:.0f}s "
    f"({avg_duration_basic / 60:.1f} min)"
)
print(
    f"  Avg duration (aware):                            {avg_duration_aware:.0f}s "
    f"({avg_duration_aware / 60:.1f} min)"
)
print()
print("  LAYER 2: Viewing Streaks")
print("  ─────────────────────────────────")
print(f"  Total viewing streaks:            {streak_count:,}")
print(f"  Binge streaks (3+ titles):        {binge_count_final:,}")
if streak_count:
    print(
        f"  Binge rate:                       "
        f"{round(binge_count_final / streak_count * 100, 1)}%"
    )
print()
print("  Tables written:")
print("    silver_sessionized_events        → basic event grain")
print("    silver_sessions                  → basic summary → warehouse facts")
print("    silver_sessionized_events_aware  → activity-aware event grain")
print("    silver_sessions_aware            → activity-aware summary")
print("    silver_viewing_streaks           → binge / content stickiness")
