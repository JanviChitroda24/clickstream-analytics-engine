"""
Test: Data Activator Trigger 2 — Schema Violation Spike
--------------------------------------------------------
Sends a batch of intentionally malformed events to trigger
the schema violation alert.

Usage:
    python -m tests.test_trigger_schema_violation

Sends 3 types of malformed events (MINIMAL mode — 1 each so the
Activator fires at most 1 email; bump the range()s to spike a rate):
    - null user_id (1 event)
    - missing event_type (1 event)
    - bad timestamp (1 event)
Plus 5 clean events for contrast.

Total: 8 events, 3 malformed — enough to fire Trigger 2 once.
"""

import json
import sys
from datetime import datetime, timezone

from azure.eventhub import EventHubProducerClient, EventData
from simulator.config import EVENTSTREAM_CONNECTION_STR, EVENTSTREAM_NAME


def send_malformed_batch():
    if not EVENTSTREAM_CONNECTION_STR:
        print("ERROR: Missing EVENTSTREAM_CONNECTION_STR in .env")
        sys.exit(1)

    producer = EventHubProducerClient.from_connection_string(
        EVENTSTREAM_CONNECTION_STR,
        eventhub_name=EVENTSTREAM_NAME,
    )

    try:
        batch = producer.create_batch()
        timestamp = datetime.now(timezone.utc).isoformat()
        events_added = {"clean": 0, "null_user": 0, "missing_type": 0, "bad_timestamp": 0}

        # 1 event with null user_id (minimal — keeps alert volume to 1 email max)
        for i in range(1):
            event = {
                "event_id": f"malformed_null_user_{i:04d}",
                "user_id": None,
                "session_id": "sess_malformed_test",
                "event_type": "page_view",
                "event_timestamp": timestamp,
                "device_type": "desktop",
                "app_version": "2.3.0",
                "properties": {"page": "/malformed-test"},
            }
            batch.add(EventData(json.dumps(event, default=str)))
            events_added["null_user"] += 1

        # 1 event with missing event_type (minimal)
        for i in range(1):
            event = {
                "event_id": f"malformed_no_type_{i:04d}",
                "user_id": f"user_99999",
                "session_id": "sess_malformed_test",
                "event_timestamp": timestamp,
                "device_type": "desktop",
                "app_version": "2.3.0",
                "properties": {"page": "/malformed-test"},
            }
            # Intentionally no "event_type" key
            batch.add(EventData(json.dumps(event)))
            events_added["missing_type"] += 1

        # 1 event with bad timestamp (minimal)
        for i in range(1):
            event = {
                "event_id": f"malformed_bad_ts_{i:04d}",
                "user_id": f"user_99998",
                "session_id": "sess_malformed_test",
                "event_type": "page_view",
                "event_timestamp": "not-a-timestamp",
                "device_type": "desktop",
                "app_version": "2.3.0",
                "properties": {"page": "/malformed-test"},
            }
            batch.add(EventData(json.dumps(event)))
            events_added["bad_timestamp"] += 1

        # 5 clean events for contrast (minimal)
        for i in range(5):
            event = {
                "event_id": f"malformed_clean_{i:04d}",
                "user_id": f"user_00001",
                "session_id": "sess_clean_test",
                "event_type": "page_view",
                "event_timestamp": timestamp,
                "device_type": "desktop",
                "app_version": "2.3.0",
                "properties": {"page": "/clean-test", "referrer": "direct"},
            }
            batch.add(EventData(json.dumps(event)))
            events_added["clean"] += 1

        producer.send_batch(batch)

        total = sum(events_added.values())
        malformed = total - events_added["clean"]
        print(f"✅ Sent {total} events ({malformed} malformed, {events_added['clean']} clean)")
        print(f"   null_user:    {events_added['null_user']}")
        print(f"   missing_type: {events_added['missing_type']}")
        print(f"   bad_timestamp:{events_added['bad_timestamp']}")
        print(f"   clean:        {events_added['clean']}")
        print(f"\n   Malformed rate: {malformed/total*100:.0f}%")
        print(f"\n   Check your email for the 'Schema Violation' alert.")
        print(f"   Also verify in KQL:")
        print(f"     raw_events")
        print(f"     | where event_id startswith 'malformed_'")
        print(f"     | summarize count() by isnull(user_id), isnull(event_type)")

    finally:
        producer.close()


def main():
    print("=" * 60)
    print("TRIGGER TEST: Schema Violation Spike")
    print("=" * 60)
    print()
    send_malformed_batch()


if __name__ == "__main__":
    main()
