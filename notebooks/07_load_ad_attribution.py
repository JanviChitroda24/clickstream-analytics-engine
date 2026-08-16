# Author: Janvi Chitroda
# Copyright (c) 2026 Janvi Chitroda. All rights reserved.
# Project: ClickStream Analytics Engine — Portfolio
# Unauthorized copying or redistribution of this content is prohibited.

# Fabric Notebook — Load Ad Attribution Fact Table
# -----------------------------------------------
# Local mirror. Fabric name: `07_load_ad_attribution`.
# Attach: clickstream_lakehouse
#
# Reconstructs impression → click → conversion chains using event ID foreign keys.
# Ad events have natural FK links (no time-window join needed):
#
#   ad_impression  campaign_id, advertiser_id, ad_format, placement
#   ad_click       campaign_id, advertiser_id, landing_url,
#                  impression_event_id  (FK → ad_impression.event_id)
#   conversion     campaign_id, advertiser_id, conversion_type, conversion_value,
#                  click_event_id       (FK → ad_click.event_id)
#
# Because the links are explicit FKs, this is a deterministic chain rebuild — not
# a probabilistic time-window attribution. Each hop is 1:1 in the simulator, so
# the LEFT JOINs cannot multiply rows; Cell 4 asserts that rather than assuming it.
#
# Grain: one row per IMPRESSION. Un-clicked impressions are kept with NULL click /
# conversion columns — dropping them would make click-through rate uncomputable
# from the fact table.

# =============================================================================
# CELL 0 — Imports + helpers
# =============================================================================
from pyspark.sql.functions import (
    col, lit, when, count, to_date, to_timestamp, unix_timestamp,
    get_json_object,
    avg as spark_avg,
)

WAREHOUSE = "clickstream_warehouse"  # docs only — load via Warehouse SQL

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
# Without it, latencies come back NULL.
def duration_seconds(start_col: str, end_col: str):
    return (
        unix_timestamp(to_timestamp(col(end_col)))
        - unix_timestamp(to_timestamp(col(start_col)))
    )


events_df = spark.sql(f"SELECT * FROM {SOURCE_TABLE}")
total_events = events_df.count()
print(f"Input ({SOURCE_TABLE}): {total_events:,} events")

# =============================================================================
# CELL 1 — Extract the three ad event types into FK-joinable frames
# =============================================================================
impressions = events_df.filter(col("event_type") == "ad_impression").select(
    col("event_id").alias("impression_event_id"),
    col("user_id"),
    col("computed_session_id_aware").alias("session_id"),
    col("event_timestamp").alias("impression_timestamp"),
    prop("campaign_id").alias("campaign_id"),
    prop("advertiser_id").alias("advertiser_id"),
    prop("ad_format").alias("ad_format"),
    prop("placement").alias("placement"),
)

clicks = events_df.filter(col("event_type") == "ad_click").select(
    col("event_id").alias("click_event_id"),
    col("event_timestamp").alias("click_timestamp"),
    prop("campaign_id").alias("click_campaign_id"),
    prop("advertiser_id").alias("click_advertiser_id"),
    prop("impression_event_id").alias("impression_event_id"),  # FK → impression
)

conversions = events_df.filter(col("event_type") == "conversion").select(
    col("event_id").alias("conversion_event_id"),
    col("event_timestamp").alias("conversion_timestamp"),
    prop("campaign_id").alias("conv_campaign_id"),
    prop("click_event_id").alias("click_event_id"),  # FK → click
    prop("conversion_type").alias("conversion_type"),
    prop("conversion_value").cast("decimal(10,2)").alias("conversion_value"),
)

impression_count = impressions.count()
click_count = clicks.count()
conversion_count = conversions.count()

ctr = (click_count / impression_count * 100) if impression_count else 0.0
cvr = (conversion_count / click_count * 100) if click_count else 0.0

print(f"ad_impression: {impression_count:,}")
print(f"ad_click:      {click_count:,}")
print(f"conversion:    {conversion_count:,}")
print(f"  Click-through rate: {ctr:.2f}%  (clicks / impressions)")
print(f"  Conversion rate:    {cvr:.2f}%  (conversions / clicks)")

# =============================================================================
# CELL 2 — Rebuild the chain by following the FKs
# Impression is the anchor: LEFT JOIN keeps un-clicked impressions.
# String-form join keys collapse the duplicate FK column automatically.
# =============================================================================
chains = impressions.join(clicks, "impression_event_id", "left").join(
    conversions, "click_event_id", "left"
)

# Campaign must stay constant along a chain — a mismatch means the FK linked
# events from different campaigns, which would corrupt any per-campaign ROAS.
campaign_mismatches = chains.filter(
    (col("click_campaign_id").isNotNull() & (col("click_campaign_id") != col("campaign_id")))
    | (col("conv_campaign_id").isNotNull() & (col("conv_campaign_id") != col("campaign_id")))
).count()
print(f"Campaign mismatches along chain: {campaign_mismatches:,} "
      f"{'✅' if campaign_mismatches == 0 else '❌ FK linked across campaigns'}")

chains = (
    chains
    # NULL propagates automatically when the click / conversion side is absent
    .withColumn("time_to_click_seconds", duration_seconds("impression_timestamp", "click_timestamp"))
    .withColumn("time_to_conversion_seconds", duration_seconds("click_timestamp", "conversion_timestamp"))
    # Impression UUID v7 is already unique — reuse it rather than generating a
    # non-deterministic surrogate, so re-runs are idempotent.
    .withColumn("attribution_id", col("impression_event_id"))
    .withColumn("attribution_date", to_date(to_timestamp(col("impression_timestamp"))))
)

# campaign_id / advertiser_id come from the impression — the anchor of the chain.
fact_ad_attribution_final = chains.select(
    "attribution_id",
    "user_id",
    "session_id",
    "campaign_id",
    "advertiser_id",
    "impression_event_id",
    "click_event_id",
    "conversion_event_id",
    to_timestamp(col("impression_timestamp")).alias("impression_timestamp"),
    to_timestamp(col("click_timestamp")).alias("click_timestamp"),
    to_timestamp(col("conversion_timestamp")).alias("conversion_timestamp"),
    "time_to_click_seconds",
    "time_to_conversion_seconds",
    "conversion_value",
    "attribution_date",
)

# =============================================================================
# CELL 3 — Funnel summary + write staging
# =============================================================================
total_rows = fact_ad_attribution_final.count()
with_click = fact_ad_attribution_final.filter(col("click_event_id").isNotNull()).count()
with_conversion = fact_ad_attribution_final.filter(col("conversion_event_id").isNotNull()).count()
full_chains = fact_ad_attribution_final.filter(
    col("impression_event_id").isNotNull()
    & col("click_event_id").isNotNull()
    & col("conversion_event_id").isNotNull()
).count()

avg_ttc = fact_ad_attribution_final.filter(
    col("time_to_click_seconds").isNotNull()
).agg(spark_avg("time_to_click_seconds")).collect()[0][0]

avg_ttconv = fact_ad_attribution_final.filter(
    col("time_to_conversion_seconds").isNotNull()
).agg(spark_avg("time_to_conversion_seconds")).collect()[0][0]

print(f"{'=' * 60}")
print("AD ATTRIBUTION FUNNEL")
print(f"{'=' * 60}")
print(f"  Attribution rows (= impressions): {total_rows:,} "
      f"{'✅' if total_rows == impression_count else '❌ row multiplication'}")
print(f"  Impressions with a click:         {with_click:,}")
print(f"  Chains reaching conversion:       {with_conversion:,}")
print(f"  Full impression→click→conversion: {full_chains:,}")
if avg_ttc is not None:
    print(f"  Avg time to click:                {avg_ttc:.0f}s")
if avg_ttconv is not None:
    print(f"  Avg time to conversion:           {avg_ttconv:.0f}s")

# Informational: clicks whose parent impression didn't survive validation/dedup
# would never appear above. Non-zero explains a click-count discrepancy.
orphan_clicks = clicks.join(
    impressions.select("impression_event_id"), "impression_event_id", "left_anti"
).count()
print(f"  Clicks without a parent impression: {orphan_clicks:,}")

print("\nconversion_value distribution (converted chains only):")
fact_ad_attribution_final.filter(col("conversion_value").isNotNull()).select(
    "conversion_value"
).summary("count", "min", "25%", "50%", "75%", "max", "mean").show()

print("Sample full chains:")
fact_ad_attribution_final.filter(col("conversion_event_id").isNotNull()).show(5, truncate=False)

write_staging(fact_ad_attribution_final, "fact_ad_attribution_staging")

# =============================================================================
# CELL 4 — Verify Lakehouse staging before the Warehouse INSERT
# =============================================================================
staged_count = spark.sql(
    "SELECT COUNT(*) AS cnt FROM fact_ad_attribution_staging"
).collect()[0]["cnt"]

dup_attribution = spark.sql(
    """
    SELECT COUNT(*) AS cnt FROM (
        SELECT attribution_id FROM fact_ad_attribution_staging
        GROUP BY attribution_id HAVING COUNT(*) > 1
    )
    """
).collect()[0]["cnt"]

null_campaign = spark.sql(
    "SELECT COUNT(*) AS cnt FROM fact_ad_attribution_staging WHERE campaign_id IS NULL"
).collect()[0]["cnt"]

negative_value = spark.sql(
    """
    SELECT COUNT(*) AS cnt FROM fact_ad_attribution_staging
    WHERE conversion_value IS NOT NULL AND conversion_value < 0
    """
).collect()[0]["cnt"]

negative_ttc = spark.sql(
    """
    SELECT COUNT(*) AS cnt FROM fact_ad_attribution_staging
    WHERE time_to_click_seconds IS NOT NULL AND time_to_click_seconds < 0
    """
).collect()[0]["cnt"]

negative_ttconv = spark.sql(
    """
    SELECT COUNT(*) AS cnt FROM fact_ad_attribution_staging
    WHERE time_to_conversion_seconds IS NOT NULL AND time_to_conversion_seconds < 0
    """
).collect()[0]["cnt"]

orphan_sessions = spark.sql(
    """
    SELECT COUNT(*) AS cnt
    FROM fact_ad_attribution_staging a
    LEFT ANTI JOIN fact_sessions_staging s ON a.session_id = s.session_id
    """
).collect()[0]["cnt"]

orphan_campaigns = spark.sql(
    """
    SELECT COUNT(*) AS cnt
    FROM fact_ad_attribution_staging a
    LEFT ANTI JOIN dim_campaign_staging c ON a.campaign_id = c.campaign_id
    """
).collect()[0]["cnt"]

print(f"{'=' * 60}")
print("AD ATTRIBUTION STAGING VERIFICATION (Lakehouse)")
print(f"{'=' * 60}")
print(f"  fact_ad_attribution_staging:      {staged_count:,}")
print()
print(f"  Duplicate attribution_id:         {dup_attribution:,} {'✅' if dup_attribution == 0 else '❌'}")
print(f"  NULL campaign_id:                 {null_campaign:,} {'✅' if null_campaign == 0 else '❌'}")
print(f"  conversion_value < 0:             {negative_value:,} {'✅' if negative_value == 0 else '❌'}")
print(f"  time_to_click < 0:                {negative_ttc:,} {'✅' if negative_ttc == 0 else '❌ click before impression'}")
print(f"  time_to_conversion < 0:           {negative_ttconv:,} {'✅' if negative_ttconv == 0 else '❌ conversion before click'}")
print(f"  Sessions not in fact_sessions:    {orphan_sessions:,} {'✅' if orphan_sessions == 0 else '❌'}")
print(f"  campaign_id not in dim_campaign:  {orphan_campaigns:,} {'✅' if orphan_campaigns == 0 else '❌'}")

# =============================================================================
# CELL 5 — Next step is Warehouse SQL (verified path)
# Spark cross-item INSERT into the warehouse is flaky — use the SQL editor.
# =============================================================================
print(
    f"""
✅ Lakehouse staging verified.

Next — run in {WAREHOUSE} SQL editor:
  warehouse_ddl/load_ad_attribution_from_staging.sql

Expected warehouse count after INSERT:
  fact_ad_attribution       {staged_count:,}
"""
)
