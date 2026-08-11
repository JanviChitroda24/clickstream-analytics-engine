# Author: Janvi Chitroda
# Copyright (c) 2026 Janvi Chitroda. All rights reserved.
# Project: ClickStream Analytics Engine — Portfolio
# Unauthorized copying or redistribution of this content is prohibited.

# Fabric Notebook — Schema Validation: Bronze → Silver (Hour 17)
# ---------------------------------------------------------------
# Local mirror. In Fabric, create/rename the notebook to `01_schema_validation`
# and attach `clickstream_lakehouse`. `spark` is provided by the runtime.
#
# Reads Bronze `raw_events`, applies the schema contract, writes:
#   PASS → silver_validated_events  (base fields clean; ready for H18 dedup)
#   FAIL → dead_letter_events       (quarantined + validation_status + quarantined_at)
#
# Does NOT require nested `properties` for the 5 base-field rules.
# Nested properties fixed via Option 1 re-ingest before H23 (see docs/properties_schema_fix.md).
# Use mode("overwrite") for idempotent re-runs (not append).
#
# Expected (~chaos rates, first-match wins so totals ≤ sum of H16 buckets):
#   Bronze ~1.14M | Dead letter ~5K | Violation ~0.4–0.5% | Silver + DL = Bronze
# Re-verify after properties re-ingest (counts may shift slightly).
#
# VERIFIED (2026-07-27, pre–re-ingest):
#   Bronze 1,144,502 | Silver 1,139,218 | DL 5,284 | rate 0.46% | match PASS
#   DL: null_user 1868 | invalid_ts 1715 | null_type 1701 | null_id 0 | unknown_type 0
#   First-match when-chain = if/elif (primary failure reason only)

# =============================================================================
# CELL 1 — Read Bronze layer
# =============================================================================
bronze_df = spark.sql("SELECT * FROM bronze_events")
total_count = bronze_df.count()
print(f"Bronze events loaded: {total_count:,}")
bronze_df.printSchema()

# =============================================================================
# CELL 2 — Schema contract validation (first matching rule wins)
# =============================================================================
from pyspark.sql.functions import col, when, lit, current_timestamp

# Valid event types from simulator/schemas.py contract
VALID_EVENT_TYPES = [
    "page_view", "search", "content_play", "content_pause",
    "content_resume", "content_complete", "content_abandon",
    "ad_impression", "ad_click", "conversion",
    "add_to_watchlist", "subscription_change",
]

# Apply validation rules — first matching rule wins (primary failure reason)
validated_df = bronze_df.withColumn(
    "validation_status",
    when(col("event_id").isNull() | (col("event_id") == ""), lit("FAIL: null event_id"))
    .when(col("user_id").isNull() | (col("user_id") == ""), lit("FAIL: null user_id"))
    .when(col("event_type").isNull() | (col("event_type") == ""), lit("FAIL: null event_type"))
    .when(
        col("event_timestamp").isNull()
        | (col("event_timestamp") == "")
        | (col("event_timestamp") == "not-a-timestamp"),
        lit("FAIL: invalid timestamp"),
    )
    .when(~col("event_type").isin(VALID_EVENT_TYPES), lit("FAIL: unknown event_type"))
    .otherwise(lit("PASS"))
)

validated_df.groupBy("validation_status").count().orderBy("count", ascending=False).show(truncate=False)

# =============================================================================
# CELL 3 — Split valid vs dead letter
# =============================================================================
valid_events = validated_df.filter(col("validation_status") == "PASS").drop("validation_status")

dead_letter = validated_df.filter(col("validation_status") != "PASS") \
    .withColumn("quarantined_at", current_timestamp())

valid_count = valid_events.count()
dead_letter_count = dead_letter.count()
violation_rate = round((dead_letter_count / total_count) * 100, 2)

print(f"Valid events:       {valid_count:,}")
print(f"Dead letter events: {dead_letter_count:,}")
print(f"Violation rate:     {violation_rate}%")

# =============================================================================
# CELL 4 — Write Silver validated + Dead letter (overwrite = idempotent)
# =============================================================================
valid_events.write.format("delta").mode("overwrite").saveAsTable("silver_validated_events")
dead_letter.write.format("delta").mode("overwrite").saveAsTable("dead_letter_events")

print(f"✅ silver_validated_events: {valid_count:,} rows written")
print(f"✅ dead_letter_events: {dead_letter_count:,} rows written")

# =============================================================================
# CELL 5 — Inspect dead letter (breakdown + sample)
# =============================================================================
dead_letter_df = spark.sql("SELECT * FROM dead_letter_events")
dead_letter_df.groupBy("validation_status").count().orderBy("count", ascending=False).show(truncate=False)

dead_letter_df.select(
    "event_id", "user_id", "event_type", "event_timestamp", "validation_status"
).show(10, truncate=False)

# =============================================================================
# CELL 6 — Final verification summary (screenshot this)
# =============================================================================
silver_count = spark.sql("SELECT COUNT(*) as cnt FROM silver_validated_events").collect()[0]["cnt"]
dl_count = spark.sql("SELECT COUNT(*) as cnt FROM dead_letter_events").collect()[0]["cnt"]

print(f"{'=' * 60}")
print("SCHEMA VALIDATION SUMMARY")
print(f"{'=' * 60}")
print(f"  Bronze input:          {total_count:,}")
print(f"  Silver validated:      {silver_count:,}")
print(f"  Dead letter:           {dl_count:,}")
print(f"  Accounted for:         {silver_count + dl_count:,}")
print(f"  Violation rate:        {round(dl_count / total_count * 100, 2)}%")
print(
    f"  Match check:           "
    f"{'✅ PASS' if silver_count + dl_count == total_count else '❌ MISMATCH'}"
)
