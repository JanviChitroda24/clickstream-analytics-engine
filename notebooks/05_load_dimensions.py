# Author: Janvi Chitroda
# Copyright (c) 2026 Janvi Chitroda. All rights reserved.
# Project: ClickStream Analytics Engine — Portfolio
# Unauthorized copying or redistribution of this content is prohibited.

# Fabric Notebook — Load Dimension Tables (Hour 22)
# ------------------------------------------------
# Local mirror. Fabric name: `05_load_dimensions`.
# Attach: clickstream_lakehouse
#
# Pattern (Fabric-friendly):
#   1) Build dims in Lakehouse as *_staging Delta tables
#   2) INSERT into clickstream_warehouse.dbo.* from staging
#
# BUG FIXED: Cell 4 must write dim_users_final (not content_df) to dim_users_staging.
# Verified 2026-07-28: staging 200/20/31/9929 → warehouse same via
#   warehouse_ddl/load_dimensions_from_staging.sql

# =============================================================================
# CELL 0 — Imports
# =============================================================================
import random
from datetime import date, timedelta

from pyspark.sql.functions import col, lit, when, first, countDistinct
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
)

WAREHOUSE = "clickstream_warehouse"  # docs only — load via Warehouse SQL
GENRES = [
    "drama", "comedy", "action", "documentary",
    "thriller", "sci-fi", "romance", "horror",
]


def write_staging(df, table_name: str):
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    print(f"✅ {table_name} written to Lakehouse")


# =============================================================================
# CELL 1 — dim_content → dim_content_staging (200)
# =============================================================================
TITLE_TEMPLATES = {
    "drama": [
        "The Last {noun}", "Breaking {noun}", "{noun} Heights",
        "Dark {noun}", "The {noun} Diaries",
    ],
    "comedy": [
        "The {noun} Show", "Laugh {noun}", "{noun} & Friends",
        "Funny {noun}", "Almost {noun}",
    ],
    "action": [
        "{noun} Force", "Operation {noun}", "Code {noun}",
        "Strike {noun}", "{noun} Rising",
    ],
    "documentary": [
        "Inside {noun}", "The {noun} Story", "Planet {noun}",
        "Exploring {noun}", "{noun} Revealed",
    ],
    "thriller": [
        "The {noun} Conspiracy", "Silent {noun}", "{noun} Games",
        "Double {noun}", "Cold {noun}",
    ],
    "sci-fi": [
        "{noun} Station", "Beyond {noun}", "The {noun} Paradox",
        "Nova {noun}", "Quantum {noun}",
    ],
    "romance": [
        "Love in {noun}", "The {noun} Letter", "{noun} Hearts",
        "Forever {noun}", "Dear {noun}",
    ],
    "horror": [
        "The {noun} Haunting", "Night of {noun}", "{noun} House",
        "Curse of {noun}", "Blood {noun}",
    ],
}
NOUNS = [
    "River", "Crown", "Steel", "Glass", "Stone", "Shadow", "Silver", "Gold",
    "Iron", "Crystal", "Ember", "Storm", "Frost", "Dawn", "Dusk", "Ocean",
    "Mountain", "Valley", "Forest", "Desert", "City", "Bridge", "Tower", "Gate",
]

random.seed(42)
content_data = []
for i in range(200):
    genre = random.choice(GENRES)
    template = random.choice(TITLE_TEMPLATES.get(genre, TITLE_TEMPLATES["drama"]))
    title = template.format(noun=random.choice(NOUNS))
    if genre == "comedy":
        duration = random.randint(18 * 60, 25 * 60)
    elif genre in ("drama", "thriller", "sci-fi"):
        duration = random.randint(40 * 60, 55 * 60)
    elif genre in ("action", "horror"):
        duration = random.randint(85 * 60, 160 * 60)
    elif genre == "documentary":
        duration = random.randint(45 * 60, 120 * 60)
    else:
        duration = random.randint(25 * 60, 65 * 60)
    tier = random.choices(
        ["free", "basic", "premium"], weights=[0.30, 0.45, 0.25], k=1
    )[0]
    content_data.append((
        f"content_{i:03d}", title, genre, tier, duration, random.randint(2018, 2026),
    ))

content_schema = StructType([
    StructField("content_id", StringType()),
    StructField("title", StringType()),
    StructField("genre", StringType()),
    StructField("content_tier", StringType()),
    StructField("duration_seconds", IntegerType()),
    StructField("release_year", IntegerType()),
])
content_df = spark.createDataFrame(content_data, content_schema)
print(f"dim_content: {content_df.count()} rows")
content_df.show(5, truncate=False)
write_staging(content_df, "dim_content_staging")

# =============================================================================
# CELL 2 — dim_campaign → dim_campaign_staging (20)
# =============================================================================
ADVERTISER_NAMES = [
    "TechNova", "FreshBite Foods", "CloudRun Shoes", "BrightPath Insurance",
    "GreenLeaf Organic", "PixelForge Games", "SkyMiles Travel", "VoltEdge Electronics",
    "PureWave Wellness", "UrbanCraft Furniture", "BlueShift Finance", "ArcticBreeze AC",
    "Solaris Energy", "NexGen Pharma", "WildTrail Outdoor", "DataPulse Software",
    "CrystalView Optics", "SwiftDash Delivery", "OmniHealth Labs", "ZenSpace Meditation",
]

random.seed(42)
campaign_data = []
for i in range(20):
    campaign_type = random.choice(
        ["brand_awareness", "app_install", "product_purchase"]
    )
    budget_tier = random.choices(
        ["small", "medium", "large"], weights=[0.40, 0.40, 0.20], k=1
    )[0]
    target_genres = ",".join(random.sample(GENRES, random.randint(1, 3)))
    campaign_data.append((
        f"camp_{i:03d}",
        f"adv_{i:03d}",
        ADVERTISER_NAMES[i % len(ADVERTISER_NAMES)],
        campaign_type,
        budget_tier,
        target_genres,
    ))

campaign_schema = StructType([
    StructField("campaign_id", StringType()),
    StructField("advertiser_id", StringType()),
    StructField("advertiser_name", StringType()),
    StructField("campaign_type", StringType()),
    StructField("budget_tier", StringType()),
    StructField("target_genres", StringType()),
])
campaign_df = spark.createDataFrame(campaign_data, campaign_schema)
print(f"dim_campaign: {campaign_df.count()} rows")
campaign_df.show(5, truncate=False)
write_staging(campaign_df, "dim_campaign_staging")

# =============================================================================
# CELL 3 — dim_date → dim_date_staging (31)
# =============================================================================
start_date = date(2026, 7, 1)
end_date = date(2026, 7, 31)
date_data = []
current = start_date
while current <= end_date:
    dow_num = current.isoweekday()
    date_data.append((
        int(current.strftime("%Y%m%d")),
        current.isoformat(),
        current.strftime("%A"),
        dow_num,
        1 if dow_num >= 6 else 0,
        current.isocalendar()[1],
        current.strftime("%B"),
        current.year,
    ))
    current += timedelta(days=1)

date_schema = StructType([
    StructField("date_key", IntegerType()),
    StructField("calendar_date", StringType()),
    StructField("day_of_week", StringType()),
    StructField("day_of_week_num", IntegerType()),
    StructField("is_weekend", IntegerType()),
    StructField("week_number", IntegerType()),
    StructField("month_name", StringType()),
    StructField("year_num", IntegerType()),
])
date_df = spark.createDataFrame(date_data, date_schema).withColumn(
    "calendar_date", col("calendar_date").cast("date")
)
print(f"dim_date: {date_df.count()} rows")
date_df.show(10, truncate=False)
write_staging(date_df, "dim_date_staging")

# =============================================================================
# CELL 4 — dim_users → dim_users_staging (~9,929)
# CRITICAL: write dim_users_final — NEVER content_df
# Archetype inferred from observed session counts (behavioral, not user_id ranges)
# =============================================================================
events_df = spark.sql("SELECT * FROM silver_sessionized_events_aware")

users_df = events_df.groupBy("user_id").agg(
    first("device_type").alias("device_type"),
    first("app_version").alias("app_version"),
)

user_behavior = events_df.groupBy("user_id").agg(
    countDistinct("computed_session_id_aware").alias("total_sessions"),
)

users_df = users_df.join(user_behavior, "user_id", "left")

# Infer archetype from session count over 14 days
users_df = users_df.withColumn(
    "behavioral_archetype",
    when(col("total_sessions") >= 40, lit("power"))
    .when(col("total_sessions") >= 10, lit("regular"))
    .when(col("total_sessions") >= 3, lit("casual"))
    .otherwise(lit("low_activity")),
)

# Derive subscription_tier from archetype
users_df = users_df.withColumn(
    "subscription_tier",
    when(col("behavioral_archetype") == "power", lit("premium"))
    .when(col("behavioral_archetype") == "regular", lit("basic"))
    .when(col("behavioral_archetype") == "casual", lit("free"))
    .otherwise(lit("basic")),
)

# Churn risk = inverse of session frequency
users_df = users_df.withColumn(
    "churn_risk_score",
    when(col("total_sessions") >= 40, lit(0.05))
    .when(col("total_sessions") >= 20, lit(0.15))
    .when(col("total_sessions") >= 10, lit(0.35))
    .when(col("total_sessions") >= 3, lit(0.65))
    .otherwise(lit(0.90)),
)

# SCD-2 initial load — all current; valid_to NULL
users_df = users_df.withColumn("is_current", lit(1))
users_df = users_df.withColumn(
    "valid_from", lit("2026-07-01").cast("timestamp")
)
users_df = users_df.withColumn("valid_to", lit(None).cast("timestamp"))

# Exclude user_key (IDENTITY in warehouse)
dim_users_final = users_df.select(
    "user_id",
    "subscription_tier",
    "device_type",
    "app_version",
    "behavioral_archetype",
    "churn_risk_score",
    "is_current",
    "valid_from",
    "valid_to",
)

print(f"dim_users: {dim_users_final.count()} rows")
dim_users_final.show(5, truncate=False)

print("\nArchetype distribution:")
dim_users_final.groupBy("behavioral_archetype").count().orderBy(
    col("count").desc()
).show()

print("Subscription tier distribution:")
dim_users_final.groupBy("subscription_tier").count().orderBy(
    col("count").desc()
).show()

# MUST be dim_users_final — not content_df
dim_users_final.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_users_staging")
print("✅ dim_users_staging written to Lakehouse")

# =============================================================================
# CELL 5 — Verify Lakehouse staging counts (verified 2026-07-28)
# =============================================================================
staging_tables = {
    "dim_content_staging": 200,
    "dim_campaign_staging": 20,
    "dim_date_staging": 31,
    "dim_users_staging": 9929,
}

print(f"{'=' * 60}")
print("DIMENSION STAGING VERIFICATION (Lakehouse)")
print(f"{'=' * 60}")

for table, expected in staging_tables.items():
    count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {table}").collect()[0]["cnt"]
    status = "✅" if count == expected else f"⚠️ expected {expected}"
    print(f"  {table:25s}: {count:,} rows {status}")

# =============================================================================
# CELL 6 — Prefer Warehouse SQL (verified path)
# File: warehouse_ddl/load_dimensions_from_staging.sql
# Spark cross-item INSERT is optional / flaky — SQL editor is the durable path.
# =============================================================================
print(
    """
✅ Lakehouse staging verified.

Next — run in clickstream_warehouse SQL editor:
  warehouse_ddl/load_dimensions_from_staging.sql

Expected warehouse counts after INSERT:
  dim_campaign  20
  dim_content   200
  dim_date      31
  dim_users     9929
"""
)
