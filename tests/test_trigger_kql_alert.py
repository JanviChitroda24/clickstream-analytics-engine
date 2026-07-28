# Author: Janvi Chitroda
# Copyright (c) 2026 Janvi Chitroda. All rights reserved.
# Project: ClickStream Analytics Engine — Portfolio
# Unauthorized copying or redistribution of this content is prohibited.

"""
Test: KQL Queryset Alert — Schema Violation Spike
--------------------------------------------------
Sends 100 events with 60% malformed rate over ~2 minutes.
This creates a recent 5-minute window with violation rate >> 2%,
which triggers the KQL Queryset scheduled alert.

WHY A SEPARATE SCRIPT (not test_trigger_schema_violation.py)?
    The minimal 8-event script is fine for Eventstream Activator demos,
    but SchemaViolationRate buckets by 5 minutes. Eight events get mixed
    into a bucket that may already hold thousands of historical/sim rows,
    so the aggregated rate never climbs above the 2% gate.
    This script floods the recent window with a 60% malformed burst so the
    rate dominates the lookback and the scheduled alert can fire.

Sends REAL nulls (not marker strings) — production-shaped garbage.

Usage:
    python -m tests.test_trigger_kql_alert
"""

import json
import sys
import time
from datetime import datetime, timezone

from azure.eventhub import EventHubProducerClient, EventData
from simulator.config import EVENTSTREAM_CONNECTION_STR, EVENTSTREAM_NAME


def send_batch(producer, events):
    batch = producer.create_batch()
    for event in events:
        batch.add(EventData(json.dumps(event, default=str)))
    producer.send_batch(batch)


def main():
    print("=" * 60)
    print("KQL ALERT TEST: Schema Violation Spike")
    print("=" * 60)

    if not EVENTSTREAM_CONNECTION_STR:
        print("ERROR: Missing EVENTSTREAM_CONNECTION_STR in .env")
        sys.exit(1)

    producer = EventHubProducerClient.from_connection_string(
        EVENTSTREAM_CONNECTION_STR,
        eventhub_name=EVENTSTREAM_NAME,
    )

    try:
        # Send 5 batches of 20 events each, spread over ~2 minutes
        # Each batch: 12 malformed + 8 clean = 60% malformed rate
        for batch_num in range(5):
            timestamp = datetime.now(timezone.utc).isoformat()
            events = []

            # 12 malformed events (null user_id)
            for i in range(12):
                events.append({
                    "event_id": f"kql_alert_malformed_{batch_num}_{i:04d}",
                    "user_id": None,
                    "session_id": "sess_kql_alert_test",
                    "event_type": "page_view",
                    "event_timestamp": timestamp,
                    "device_type": "desktop",
                    "app_version": "2.3.0",
                    "properties": {"page": "/kql-alert-test"},
                })

            # 8 clean events
            for i in range(8):
                events.append({
                    "event_id": f"kql_alert_clean_{batch_num}_{i:04d}",
                    "user_id": "user_00001",
                    "session_id": "sess_kql_alert_test",
                    "event_type": "page_view",
                    "event_timestamp": timestamp,
                    "device_type": "desktop",
                    "app_version": "2.3.0",
                    "properties": {"page": "/kql-alert-test", "referrer": "direct"},
                })

            send_batch(producer, events)
            print(f"  Batch {batch_num + 1}/5: sent 20 events (12 malformed + 8 clean) at {timestamp}")

            if batch_num < 4:
                print(f"  Waiting 30 seconds...")
                time.sleep(30)

        print(f"\n✅ Total: 100 events sent (60 malformed + 40 clean)")
        print(f"   Malformed rate: 60%")
        print(f"\n   The KQL alert should fire within 5 minutes.")
        print(f"   Verify manually with:")
        print(f"     SchemaViolationRate")
        print(f"     | where ts > ago(10m)")
        print(f"     | summarize total_events = sum(total), total_violations = sum(violations)")
        print(f"     | extend violation_rate = todouble(total_violations) / todouble(total_events) * 100")

    finally:
        producer.close()


if __name__ == "__main__":
    main()
