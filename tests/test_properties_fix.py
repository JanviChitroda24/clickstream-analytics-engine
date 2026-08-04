# Author: Janvi Chitroda
# Copyright (c) 2026 Janvi Chitroda. All rights reserved.
# Project: ClickStream Analytics Engine — Portfolio
# Unauthorized copying or redistribution of this content is prohibited.

"""
Test: Verify properties stored as string in Lakehouse
------------------------------------------------------
Sends 10 events with 3 different property structures to verify
Eventstream stores properties as STRING (JSON), not STRUCT.

Prereqs:
  1. session_generator uses json.dumps(properties) in _make_event
  2. Lakehouse raw_events table DELETED (no locked schema)
  3. Eventstream lakehouse_raw destination recreated + published
  4. Do NOT run full simulator until this test PASSes in Fabric

Run:
    source csvenv/bin/activate
    python -m tests.test_properties_fix

Then wait 2–3 min and verify in a Lakehouse notebook:
    events_df = spark.sql("SELECT * FROM raw_events")
    events_df.printSchema()   # properties: string  ← PASS
    events_df.select("event_id", "event_type", "properties").show(10, truncate=False)
"""

import json
import sys
from datetime import datetime, timezone

from azure.eventhub import EventHubProducerClient, EventData

from simulator.config import EVENTSTREAM_CONNECTION_STR, EVENTSTREAM_NAME


def main() -> int:
    print("=" * 60)
    print("TEST: Properties String Fix")
    print("=" * 60)

    if not EVENTSTREAM_CONNECTION_STR:
        print("ERROR: Missing EVENTSTREAM_CONNECTION_STR in .env")
        return 1

    producer_kwargs = {"conn_str": EVENTSTREAM_CONNECTION_STR}
    if EVENTSTREAM_NAME:
        producer_kwargs["eventhub_name"] = EVENTSTREAM_NAME

    producer = EventHubProducerClient.from_connection_string(**producer_kwargs)

    try:
        batch = producer.create_batch()
        timestamp = datetime.now(timezone.utc).isoformat()

        # Event 1-3: content_play properties
        for i in range(3):
            event = {
                "event_id": f"propfix_play_{i:04d}",
                "user_id": "user_00001",
                "session_id": "sess_propfix_001",
                "event_type": "content_play",
                "event_timestamp": timestamp,
                "device_type": "desktop",
                "app_version": "2.3.0",
                "properties": json.dumps({
                    "content_id": f"content_{i:03d}",
                    "genre": "drama",
                    "position_seconds": 0,
                    "content_quality": "4k",
                }),
            }
            batch.add(EventData(json.dumps(event)))

        # Event 4-6: ad_impression properties
        for i in range(3):
            event = {
                "event_id": f"propfix_ad_{i:04d}",
                "user_id": "user_00001",
                "session_id": "sess_propfix_001",
                "event_type": "ad_impression",
                "event_timestamp": timestamp,
                "device_type": "desktop",
                "app_version": "2.3.0",
                "properties": json.dumps({
                    "campaign_id": f"camp_{i:03d}",
                    "advertiser_id": f"adv_{i:03d}",
                    "ad_format": "pre_roll",
                    "placement": "home_feed",
                }),
            }
            batch.add(EventData(json.dumps(event)))

        # Event 7-8: page_view properties
        for i in range(2):
            event = {
                "event_id": f"propfix_page_{i:04d}",
                "user_id": "user_00001",
                "session_id": "sess_propfix_001",
                "event_type": "page_view",
                "event_timestamp": timestamp,
                "device_type": "desktop",
                "app_version": "2.3.0",
                "properties": json.dumps({
                    "page": "/home",
                    "referrer": "direct",
                }),
            }
            batch.add(EventData(json.dumps(event)))

        # Event 9-10: search properties
        for i in range(2):
            event = {
                "event_id": f"propfix_search_{i:04d}",
                "user_id": "user_00001",
                "session_id": "sess_propfix_001",
                "event_type": "search",
                "event_timestamp": timestamp,
                "device_type": "desktop",
                "app_version": "2.3.0",
                "properties": json.dumps({
                    "query_text": "action movies",
                    "result_count": 42,
                    "search_type": "title",
                }),
            }
            batch.add(EventData(json.dumps(event)))

        producer.send_batch(batch)
        print(f"\n✅ 10 test events sent at {timestamp}")
        print("   3 content_play  (content_id, genre, position_seconds, content_quality)")
        print("   3 ad_impression (campaign_id, advertiser_id, ad_format, placement)")
        print("   2 page_view     (page, referrer)")
        print("   2 search        (query_text, result_count, search_type)")
        print("\n   Wait 2–3 minutes, then verify in notebook:")
        print("   events_df = spark.sql('SELECT * FROM raw_events')")
        print("   events_df.printSchema()")
        print(
            "   events_df.select('event_id', 'event_type', 'properties')"
            ".show(10, truncate=False)"
        )
        print("\n   PASS: properties is STRING with JSON for ALL event types")
        print("   FAIL: properties is STRUCT or NULL → do not run full simulator yet")
        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR sending: {type(exc).__name__}: {exc}")
        return 1
    finally:
        producer.close()


if __name__ == "__main__":
    sys.exit(main())
