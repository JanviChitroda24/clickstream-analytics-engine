# Fabric Notebook — Deduplication: Silver Validated → Silver Deduped (Hour 18)
# ---------------------------------------------------------------------------
# Local mirror. In Fabric, create/rename the notebook to `02_deduplication`
# and attach `clickstream_lakehouse`. `spark` is provided by the runtime.
#
# Reads Hour 17 output `silver_validated_events`, keeps the first occurrence
# of each event_id (row_number over partitionBy event_id orderBy timestamp),
# writes `silver_deduped_events`.
#
# Why after validation: malformed copies never reach dedup — no edge cases
# like "one copy null user_id, other clean." Why row_number not dropDuplicates:
# deterministic keep-earliest; dropDuplicates keeps an arbitrary copy.
#
# Expected (~chaos 2% on validated input ~1,139,218):
#   Duplicates removed ~22K | Dedup rate ~1.9–2.0% | tolerance 1–3%
#
# VERIFIED (2026-07-27):
#   Input 1,139,218 | Dup event_ids 22,425 | Output 1,116,783
#   Removed 22,435 | Rate 1.97% | PASS (1–3% band)
#   Spot-check: 2 identical content_abandon rows → 1 after
#
# Pipeline: Bronze → Validate (H17) → Dedup (H18) → Sessionize (H19)

# =============================================================================
# CELL 1 — Read Silver validated events
# =============================================================================
validated_df = spark.sql("SELECT * FROM silver_validated_events")
before_count = validated_df.count()
print(f"Input (silver_validated_events): {before_count:,}")

# =============================================================================
# CELL 2 — Identify duplicates BEFORE removing them
# =============================================================================
from pyspark.sql.functions import col, count

duplicate_ids = validated_df.groupBy("event_id") \
    .agg(count("*").alias("occurrence")) \
    .filter(col("occurrence") > 1)

duplicate_id_count = duplicate_ids.count()
print(f"Duplicate event_ids found: {duplicate_id_count:,}")

print("\nSample duplicated event_ids:")
duplicate_ids.orderBy(col("occurrence").desc()).show(10, truncate=False)

# =============================================================================
# CELL 3 — Deduplicate: keep first occurrence (earliest timestamp)
# =============================================================================
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

# Group by event_id; within each group, earliest event_timestamp → row_num 1
window = Window.partitionBy("event_id").orderBy("event_timestamp")

deduped_df = validated_df \
    .withColumn("row_num", row_number().over(window)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

after_count = deduped_df.count()
duplicates_removed = before_count - after_count
dedup_rate = round((duplicates_removed / before_count) * 100, 2)

print(f"Before dedup:       {before_count:,}")
print(f"After dedup:        {after_count:,}")
print(f"Duplicates removed: {duplicates_removed:,}")
print(f"Dedup rate:         {dedup_rate}%")

# =============================================================================
# CELL 4 — Write silver_deduped_events (overwrite = idempotent)
# =============================================================================
deduped_df.write.format("delta").mode("overwrite").saveAsTable("silver_deduped_events")
print(f"✅ silver_deduped_events: {after_count:,} rows written")

# =============================================================================
# CELL 5 — Spot-check: one duplicated event_id → 2 before, 1 after
# =============================================================================
sample_dup_id = duplicate_ids.select("event_id").first()["event_id"]

print(f"Checking event_id: {sample_dup_id}")
print(f"\nIn validated (before dedup):")
validated_df.filter(col("event_id") == sample_dup_id).show(truncate=False)

print(f"In deduped (after dedup):")
deduped_result = spark.sql(
    f"SELECT * FROM silver_deduped_events WHERE event_id = '{sample_dup_id}'"
)
deduped_result.show(truncate=False)
print(f"Rows remaining: {deduped_result.count()}")

# =============================================================================
# CELL 6 — Deduplication summary (screenshot this)
# =============================================================================
silver_count = spark.sql("SELECT COUNT(*) as cnt FROM silver_deduped_events").collect()[0]["cnt"]

print(f"{'=' * 60}")
print("DEDUPLICATION SUMMARY")
print(f"{'=' * 60}")
print(f"  Input (validated):     {before_count:,}")
print(f"  Output (deduped):      {silver_count:,}")
print(f"  Duplicates removed:    {duplicates_removed:,}")
print(f"  Dedup rate:            {dedup_rate}%")
print(f"  Target rate:           2.0%")
print(
    f"  Rate match:            "
    f"{'✅ PASS' if 1.0 <= dedup_rate <= 3.0 else '❌ OUTSIDE TOLERANCE'}"
)
