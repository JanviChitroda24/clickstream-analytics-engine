# Author: Janvi Chitroda
# Copyright (c) 2026 Janvi Chitroda. All rights reserved.
# Project: ClickStream Analytics Engine — Portfolio
# Unauthorized copying or redistribution of this content is prohibited.

"""
Eventstream Dual-Write Smoke Test (Hour 4)
------------------------------------------
Sends 10 test events to Fabric Eventstream via the Custom App (Event Hubs-
compatible) endpoint. Eventstream then dual-writes them to:
  - Eventhouse  → raw_events     (hot path)
  - Lakehouse   → raw_events     (cold path; table name may be bronze_events)

CRITICAL — Lakehouse schema-on-write:
  NEVER send events with a toy properties shape (e.g. {"test_index": N}) into
  a Lakehouse destination that will later hold production simulator data.
  Hour 4 originally used test_index → Eventstream locked
  properties as STRUCT<test_index:BIGINT> → real nested fields became NULL.

  This script now uses realistic page_view properties (page, referrer).

  After recreating the Lakehouse destination (see docs/properties_schema_fix.md):
    1. Do NOT run this script first.
    2. Run `python -m simulator.main --days 14` so the first batch is real data.
    3. Use this smoke test only for connectivity checks on a throwaway table,
       or after production ingest has already established schema.

Prereqs:
  1. Eventstream `clickstream_eventstream` created with a Custom App source
     and both destinations configured.
  2. `.env` created from `.env.example` with the real connection string:
        EVENTSTREAM_CONNECTION_STR=Endpoint=sb://...;SharedAccessKey=...;EntityPath=...
        EVENTSTREAM_NAME=<event hub name>
  3. Deps installed:  pip install -r requirements.txt

Run:
    source csvenv/bin/activate
    python test_eventstream.py
"""

import json
import sys
from datetime import datetime, timezone

from uuid_extensions import uuid7str
from azure.eventhub import EventHubProducerClient, EventData

from simulator import config


def build_test_events(n: int = 10) -> list[dict]:
    """
    Build n schema-shaped page_view events.

    properties must match simulator.schemas EVENT_PROPERTIES[PAGE_VIEW]
    (page, referrer) — never a toy key like test_index that can schema-lock
    the Lakehouse destination.
    """
    now = datetime.now(timezone.utc).isoformat()
    events = []
    for i in range(n):
        events.append({
            "event_id": uuid7str(),
            "user_id": "user_test",
            "session_id": "sess_smoketest",
            "event_type": "page_view",
            "event_timestamp": now,
            "device_type": "mobile_ios",
            "app_version": "2.3.0",
# Realistic nested shape as JSON STRING (same as simulator._make_event)
            # Nested dict would re-risk Lakehouse STRUCT inference.
            "properties": json.dumps({
                "page": f"/smoke-test/{i}",
                "referrer": "direct",
            }),
        })
    return events


def main() -> int:
    conn_str = config.EVENTSTREAM_CONNECTION_STR
    eh_name = config.EVENTSTREAM_NAME

    if not conn_str:
        print("ERROR: EVENTSTREAM_CONNECTION_STR is empty. "
              "Create .env from .env.example and paste the real values.")
        return 1

    # eventhub_name is optional if the connection string already has EntityPath
    producer_kwargs = {"conn_str": conn_str}
    if eh_name:
        producer_kwargs["eventhub_name"] = eh_name

    producer = EventHubProducerClient.from_connection_string(**producer_kwargs)

    events = build_test_events(10)
    try:
        with producer:
            batch = producer.create_batch()
            for event in events:
                batch.add(EventData(json.dumps(event)))
            producer.send_batch(batch)
    except Exception as exc:  # noqa: BLE001 - surface any send error clearly
        print(f"ERROR sending to Eventstream: {type(exc).__name__}: {exc}")
        print("Check: connection string whitespace, missing EntityPath, "
              "wrong eventhub name, or Eventstream not fully provisioned yet.")
        return 1

    print(f"10 test events sent to Eventstream at {datetime.now(timezone.utc).isoformat()}")
    print("NOTE: After recreating Lakehouse dest, prefer simulator.main first — "
          "see docs/properties_schema_fix.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
