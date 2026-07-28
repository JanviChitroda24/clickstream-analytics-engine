# Author: Janvi Chitroda
# Copyright (c) 2026 Janvi Chitroda. All rights reserved.
# Project: ClickStream Analytics Engine — Portfolio
# Unauthorized copying or redistribution of this content is prohibited.

# Fabric Notebook — Sessionization Sensitivity Analysis (Hour 20)
# --------------------------------------------------------------
# Local mirror. Fabric name: `04_sessionization_sensitivity`.
# Attach `clickstream_lakehouse`.
#
# Controlled experiment: same 1.1M deduped events, THREE flat timeouts
# (15 / 30 / 60 min). Only the timeout parameter changes.
#
# This is Layer 1A (basic gap-based) ONLY — intentionally NOT activity-aware.
#   H19 answered: flat vs context-aware?
#   H20 answers:  given a flat timeout, how much does the number matter?
#
# Always to_timestamp() before unix_timestamp() on ISO string timestamps.
#
# Outputs:
#   sessionization_sensitivity  — 3-row comparison Delta table
#   docs/sessionization_tradeoff.md — fill after Fabric run with real numbers

# =============================================================================
# CELL 1 — Setup: read deduped events once (identical input for all 3 runs)
# =============================================================================
from pyspark.sql.functions import (
    col, lag, when, lit, unix_timestamp, to_timestamp,
    sum as spark_sum, count, countDistinct,
    min as spark_min, max as spark_max,
    avg as spark_avg,
)
from pyspark.sql.window import Window

events_df = spark.sql("SELECT * FROM silver_deduped_events")
total_events = events_df.count()
print(f"Events loaded: {total_events:,}")
print("Input is identical for all three timeout runs — only the parameter changes.")

# =============================================================================
# CELL 2 — Reusable flat (basic) sessionization → metrics dict
# =============================================================================
def run_sessionization(df, timeout_seconds):
    """
    Gap-based sessionization with a flat inactivity timeout.
    Returns a metrics dict for the sensitivity comparison.
    """
    timeout_minutes = timeout_seconds // 60
    print(f"\n{'=' * 60}")
    print(f"Running sessionization: {timeout_minutes}-minute timeout")
    print(f"{'=' * 60}")

    user_window = Window.partitionBy("user_id").orderBy("event_timestamp")

    sessionized = (
        df.withColumn("prev_timestamp", lag("event_timestamp").over(user_window))
        .withColumn(
            "gap_seconds",
            unix_timestamp(to_timestamp(col("event_timestamp")))
            - unix_timestamp(to_timestamp(col("prev_timestamp"))),
        )
        .withColumn(
            "is_new_session",
            when(col("prev_timestamp").isNull(), lit(1))
            .when(col("gap_seconds") > timeout_seconds, lit(1))
            .otherwise(lit(0)),
        )
        .withColumn("session_number", spark_sum("is_new_session").over(user_window))
    )

    sessions = (
        sessionized.groupBy("user_id", "session_number")
        .agg(
            spark_min("event_timestamp").alias("session_start"),
            spark_max("event_timestamp").alias("session_end"),
            count("*").alias("event_count"),
            countDistinct("event_type").alias("distinct_event_types"),
        )
        .withColumn(
            "duration_seconds",
            unix_timestamp(to_timestamp(col("session_end")))
            - unix_timestamp(to_timestamp(col("session_start"))),
        )
    )

    total_sessions = sessions.count()
    n_users = sessions.select("user_id").distinct().count()

    dur = sessions.filter(col("duration_seconds").isNotNull())
    avg_duration = dur.agg(spark_avg("duration_seconds")).collect()[0][0] or 0.0

    median_duration = 0.0
    p25_duration = 0.0
    p75_duration = 0.0
    if total_sessions > 0:
        qs = dur.approxQuantile("duration_seconds", [0.25, 0.5, 0.75], 0.01)
        if len(qs) == 3:
            p25_duration, median_duration, p75_duration = qs[0], qs[1], qs[2]

    avg_events = sessions.agg(spark_avg("event_count")).collect()[0][0] or 0.0
    single_event_sessions = sessions.filter(col("event_count") == 1).count()
    bounce_rate = (
        (single_event_sessions / total_sessions * 100) if total_sessions > 0 else 0.0
    )
    sessions_per_user = (total_sessions / n_users) if n_users > 0 else 0.0

    metrics = {
        "timeout_minutes": int(timeout_minutes),
        "total_sessions": int(total_sessions),
        "avg_duration_sec": float(round(avg_duration, 1)),
        "median_duration_sec": float(round(median_duration, 1)),
        "p25_duration_sec": float(round(p25_duration, 1)),
        "p75_duration_sec": float(round(p75_duration, 1)),
        "avg_events_per_session": float(round(avg_events, 1)),
        "bounce_rate_pct": float(round(bounce_rate, 1)),
        "sessions_per_user": float(round(sessions_per_user, 1)),
        "single_event_sessions": int(single_event_sessions),
    }

    print(f"  Total sessions:        {total_sessions:,}")
    print(f"  Sessions per user:     {sessions_per_user:.1f}")
    print(f"  Avg duration:          {avg_duration:.0f}s ({avg_duration / 60:.1f} min)")
    print(
        f"  Median duration:       {median_duration:.0f}s "
        f"({median_duration / 60:.1f} min)"
    )
    print(f"  P25/P75 duration:      {p25_duration:.0f}s / {p75_duration:.0f}s")
    print(f"  Avg events/session:    {avg_events:.1f}")
    print(f"  Single-event sessions: {single_event_sessions:,}")
    print(f"  Bounce rate:           {bounce_rate:.1f}%")

    return metrics


# =============================================================================
# CELL 3 — Run 15 / 30 / 60 minute thresholds
# =============================================================================
results = []

for timeout_sec in [900, 1800, 3600]:  # 15, 30, 60 minutes
    metrics = run_sessionization(events_df, timeout_sec)
    results.append(metrics)

# =============================================================================
# CELL 4 — Side-by-side comparison + % change vs 30-min baseline
# =============================================================================
comparison_df = spark.createDataFrame(results)

print(f"\n{'=' * 60}")
print("SESSIONIZATION SENSITIVITY ANALYSIS")
print(f"{'=' * 60}")
print("\nSide-by-side comparison:")
comparison_df.select(
    "timeout_minutes",
    "total_sessions",
    "sessions_per_user",
    "avg_duration_sec",
    "median_duration_sec",
    "avg_events_per_session",
    "bounce_rate_pct",
).show(truncate=False)

baseline = results[1]  # 30-minute
print("Relative change vs 30-min baseline:")
for r in results:
    timeout = r["timeout_minutes"]
    sess_diff = (
        (r["total_sessions"] - baseline["total_sessions"])
        / baseline["total_sessions"]
        * 100
    )
    dur_diff = (
        (
            (r["avg_duration_sec"] - baseline["avg_duration_sec"])
            / baseline["avg_duration_sec"]
            * 100
        )
        if baseline["avg_duration_sec"] > 0
        else 0.0
    )
    evt_diff = (
        (
            (r["avg_events_per_session"] - baseline["avg_events_per_session"])
            / baseline["avg_events_per_session"]
            * 100
        )
        if baseline["avg_events_per_session"] > 0
        else 0.0
    )

    print(f"  {timeout}-min vs 30-min baseline:")
    print(f"    Sessions:  {sess_diff:+.1f}%")
    print(f"    Duration:  {dur_diff:+.1f}%")
    print(f"    Events:    {evt_diff:+.1f}%")
    print(
        f"    Bounce:    {r['bounce_rate_pct']}% "
        f"(30-min: {baseline['bounce_rate_pct']}%)"
    )
    print()

# =============================================================================
# CELL 5 — Persist comparison table
# =============================================================================
comparison_df.write.format("delta").mode("overwrite").saveAsTable(
    "sessionization_sensitivity"
)
print("✅ sessionization_sensitivity table written")

# =============================================================================
# CELL 6 — Findings narrative (integrates Hour 19 dual-layer)
# =============================================================================
r15, r30, r60 = results[0], results[1], results[2]

print(f"{'=' * 60}")
print("SENSITIVITY ANALYSIS FINDINGS")
print(f"{'=' * 60}")
print(
    f"""
THRESHOLD COMPARISON (FLAT timeout — Layer 1A only):
┌──────────────┬───────────┬───────────┬───────────┐
│ Metric       │  15-min   │  30-min   │  60-min   │
├──────────────┼───────────┼───────────┼───────────┤
│ Sessions     │ {r15['total_sessions']:>9,} │ {r30['total_sessions']:>9,} │ {r60['total_sessions']:>9,} │
│ Sess/User    │ {r15['sessions_per_user']:>9.1f} │ {r30['sessions_per_user']:>9.1f} │ {r60['sessions_per_user']:>9.1f} │
│ Avg Duration │ {r15['avg_duration_sec']:>7.0f}s │ {r30['avg_duration_sec']:>7.0f}s │ {r60['avg_duration_sec']:>7.0f}s │
│ Med Duration │ {r15['median_duration_sec']:>7.0f}s │ {r30['median_duration_sec']:>7.0f}s │ {r60['median_duration_sec']:>7.0f}s │
│ Avg Events   │ {r15['avg_events_per_session']:>9.1f} │ {r30['avg_events_per_session']:>9.1f} │ {r60['avg_events_per_session']:>9.1f} │
│ Bounce Rate  │ {r15['bounce_rate_pct']:>8.1f}% │ {r30['bounce_rate_pct']:>8.1f}% │ {r60['bounce_rate_pct']:>8.1f}% │
└──────────────┴───────────┴───────────┴───────────┘

TRADE-OFF ANALYSIS:

15-minute timeout:
  + Catches micro-sessions and brief browse-then-leave patterns
  + Higher session count → more granular visit tracking
  - Splits content viewing (45-min movie / sparse pauses → 2+ sessions)
  - Inflates bounce rate (mid-watch pauses look like boundaries)
  - Sessions/user inflated → misleading "high engagement"

30-minute timeout (BASELINE / INDUSTRY STANDARD):
  + Comparable to Google Analytics / Adobe Analytics defaults
  + Balanced for mixed navigation + content viewing
  + Bounce rate reflects real quick visits better than 15-min
  - Can still split very long watches with sparse mid-stream events
    → handled by Hour 19 activity-aware extension (30m → 60m on content)

60-minute timeout:
  + Better blanket fit for long-form / binge without activity-aware logic
  + Fewer sessions; each closer to a "complete visit"
  + Lower bounce rate
  - Merges genuinely separate visits (left noon, back 12:45 = same session)
  - Understates visit frequency → misleading "low engagement"
  - Not comparable to industry GA-style benchmarks

HOW THIS RELATES TO HOUR 19 (DUAL-LAYER):
  H20 isolates ONE variable: flat timeout magnitude.
  H19 already answered a DIFFERENT question: flat vs activity-aware.
  Recommendation:
    → Primary platform sessions: 30-min BASE + activity-aware 60-min content extension
       (H19 silver_sessions_aware) — get long-form protection WITHOUT blanket 60-min merge
    → Warehouse / DAU / retention facts: 30-min basic or aware (document which)
    → Binge / stickiness: viewing streaks (H19 Layer 2), not a looser flat timeout

RECOMMENDATION:
  Keep 30 minutes as the primary base timeout.
  Do NOT switch the whole pipeline to flat 60-min just to protect content watches —
  that is what activity-aware + viewing streaks already solve.
"""
)

print("Next: results are locked in docs/sessionization_tradeoff.md — commit when ready.")
