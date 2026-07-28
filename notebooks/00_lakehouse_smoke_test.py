# Author: Janvi Chitroda
# Copyright (c) 2026 Janvi Chitroda. All rights reserved.
# Project: ClickStream Analytics Engine — Portfolio
# Unauthorized copying or redistribution of this content is prohibited.

# Fabric Notebook — Lakehouse + Spark Smoke Test (Hour 2)
# -------------------------------------------------------
# Local mirror of the throwaway smoke-test notebook run inside Fabric.
# Purpose: prove Lakehouse + Spark compute + Delta read/write all work on the
# trial capacity BEFORE real events flow in Hour 4.
#
# Run inside a Fabric Notebook attached to `clickstream_lakehouse`.
# `spark` is provided automatically by the Fabric runtime (do NOT create a
# SparkSession manually).
#
# NOTE: This is a smoke test only. `test_events` is deleted at the end — it must
# not linger when real Bronze events start landing.

# CELL 1 — write 3 test rows as a Delta table, then read them back
from pyspark.sql.types import StructType, StructField, StringType

test_data = [
    ("evt_001", "user_001", "page_view", "2026-07-15T10:00:00Z"),
    ("evt_002", "user_001", "search", "2026-07-15T10:01:30Z"),
    ("evt_003", "user_002", "content_play", "2026-07-15T10:02:00Z"),
]
schema = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_timestamp", StringType()),
])

df = spark.createDataFrame(test_data, schema)
df.write.format("delta").mode("overwrite").save("Tables/test_events")

spark.read.format("delta").load("Tables/test_events").show()

# CELL 2 — clean up: drop the test table so it doesn't pollute the Lakehouse
spark.sql("DROP TABLE IF EXISTS test_events")
