"""
Test: Data Activator Trigger 1 — Pipeline Volume Drop
------------------------------------------------------
Sends a burst of events, then stops. The gap after the burst
should trigger the "no events / volume drop" alert.

Usage:
    python -m tests.test_trigger_volume_drop

Steps:
    1. Sends 50 events (to establish "events are flowing")
    2. Prints "Now waiting — do NOT send any more events"
    3. Waits 6 minutes (long enough for the Activator to detect the gap)
    4. Prints whether you should check your email for the alert
"""

import json
import sys
import time
from datetime import datetime, timezone

from azure.eventhub import EventHubProducerClient, EventData
from simulator.config import EVENTSTREAM_CONNECTION_STR, EVENTSTREAM_NAME


def send_burst(count: int = 50):
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

        for i in range(count):
            event = {
                "event_id": f"trigger_test_volume_{i:04d}",
                "user_id": f"user_00001",
                "session_id": "sess_trigger_test",
                "event_type": "page_view",
                "event_timestamp": timestamp,
                "device_type": "desktop",
                "app_version": "2.3.0",
                "properties": {"page": "/trigger-test", "referrer": "direct"},
            }
            batch.add(EventData(json.dumps(event)))

        producer.send_batch(batch)
        print(f"✅ Burst: {count} events sent at {timestamp}")

    finally:
        producer.close()


def main():
    print("=" * 60)
    print("TRIGGER TEST: Pipeline Volume Drop")
    print("=" * 60)

    print("\nPhase 1: Sending burst of 50 events...")
    send_burst(50)

    print("\nPhase 2: Waiting 6 minutes (simulating pipeline silence)...")
    print("  Do NOT send any more events during this time.")
    print("  The Activator should detect the volume drop and fire an alert.")
    print()

    for remaining in range(360, 0, -30):
        mins = remaining // 60
        secs = remaining % 60
        print(f"  ⏳ {mins}m {secs}s remaining...")
        time.sleep(30)

    print("\n✅ Wait complete.")
    print("  Check your email for the 'Pipeline Volume Drop' alert.")
    print("  If no alert arrived, the Activator may need a longer detection window.")


if __name__ == "__main__":
    main()
