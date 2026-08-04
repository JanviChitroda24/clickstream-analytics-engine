# Author: Janvi Chitroda
# Copyright (c) 2026 Janvi Chitroda. All rights reserved.
# Project: ClickStream Analytics Engine — Portfolio
# Unauthorized copying or redistribution of this content is prohibited.

"""
Test: session_generator.py validation
---------------------------------------
Generates one session per archetype and verifies event ordering,
required fields, and behavioral patterns.

Run: python tests/test_session_generator.py
"""

import json
from datetime import datetime, timezone
from collections import Counter

from simulator.user_profiles import create_user_population, UserArchetype
from simulator.session_generator import SessionGenerator


def make_test_catalog():
    """Minimal content catalog for testing."""
    return [
        {"content_id": f"content_{i:03d}", "genre": g, "duration_seconds": d}
        for i, (g, d) in enumerate([
            ("drama", 2700), ("comedy", 1320), ("action", 5400),
            ("documentary", 3600), ("thriller", 2700), ("sci-fi", 5400),
            ("romance", 6600), ("horror", 5400), ("drama", 1320), ("comedy", 2700),
        ])
    ]


def make_test_campaigns():
    """Minimal ad campaign catalog for testing."""
    return [
        {"campaign_id": f"camp_{i:03d}", "advertiser_id": f"adv_{i:03d}"}
        for i in range(5)
    ]


def _props(event: dict) -> dict:
    """properties is a JSON string from _make_event (Lakehouse string fix)."""
    raw = event["properties"]
    return json.loads(raw) if isinstance(raw, str) else raw


def test_session_generator():
    print("=" * 60)
    print("SESSION GENERATOR VALIDATION")
    print("=" * 60)

    catalog = make_test_catalog()
    campaigns = make_test_campaigns()
    users = create_user_population()
    start_time = datetime(2026, 7, 16, 10, 0, 0, tzinfo=timezone.utc)

    # --- Generate one session per archetype ---
    archetypes_tested = set()
    all_events = []

    for archetype in UserArchetype:
        user = next(u for u in users if u.archetype == archetype)
        gen = SessionGenerator(user, catalog, campaigns)
        events = gen.generate_session(start_time)
        all_events.extend(events)
        archetypes_tested.add(archetype)

        print(f"\n--- {archetype.value.upper()} user ({user.user_id}) ---")
        print(f"  Events in session: {len(events)}")
        print(f"  Event sequence:")
        for e in events:
            print(f"    {e['event_timestamp']} | {e['event_type']:20s} | {e['session_id']}")

        # Verify first event is page_view
        assert events[0]["event_type"] == "page_view", \
            f"{archetype.value}: First event should be page_view, got {events[0]['event_type']}"

        # Verify all events have required base fields
        for e in events:
            for field in ["event_id", "user_id", "session_id", "event_type",
                          "event_timestamp", "device_type", "app_version", "properties"]:
                assert field in e, f"Missing field '{field}' in event: {e['event_type']}"

        # Verify all events in same session have same session_id
        session_ids = set(e["session_id"] for e in events)
        assert len(session_ids) == 1, f"Multiple session IDs found: {session_ids}"

        # Verify timestamps are non-decreasing
        timestamps = [e["event_timestamp"] for e in events]
        assert timestamps == sorted(timestamps), \
            f"{archetype.value}: Timestamps not in order"

    assert len(archetypes_tested) == 5, "Not all archetypes tested"
    print(f"\n✅ All 5 archetypes generated sessions successfully")

    # --- Event type distribution ---
    print(f"\nEvent type distribution across all test sessions:")
    type_counts = Counter(e["event_type"] for e in all_events)
    for event_type, count in sorted(type_counts.items(), key=lambda x: -x[1]):
        print(f"  {event_type:25s}: {count}")

    # --- Verify content sequence integrity ---
    print(f"\nContent sequence checks:")
    content_plays = [e for e in all_events if e["event_type"] == "content_play"]
    content_completes = [e for e in all_events if e["event_type"] == "content_complete"]
    content_abandons = [e for e in all_events if e["event_type"] == "content_abandon"]
    print(f"  Content plays:     {len(content_plays)}")
    print(f"  Content completes: {len(content_completes)}")
    print(f"  Content abandons:  {len(content_abandons)}")
    assert len(content_plays) > 0, "No content plays generated"
    assert len(content_completes) + len(content_abandons) > 0, "No completions or abandons"
    print(f"  ✅ Content lifecycle events present")

    # --- Verify ad attribution chain ---
    print(f"\nAd attribution chain:")
    impressions = [e for e in all_events if e["event_type"] == "ad_impression"]
    clicks = [e for e in all_events if e["event_type"] == "ad_click"]
    conversions = [e for e in all_events if e["event_type"] == "conversion"]
    print(f"  Impressions:  {len(impressions)}")
    print(f"  Clicks:       {len(clicks)}")
    print(f"  Conversions:  {len(conversions)}")

    # Every click should reference an impression
    impression_ids = {e["event_id"] for e in impressions}
    for click in clicks:
        ref = _props(click)["impression_event_id"]
        assert ref in impression_ids, f"Click references unknown impression: {ref}"
    print(f"  ✅ All clicks reference valid impressions")

    # Every conversion should reference a click
    click_ids = {e["event_id"] for e in clicks}
    for conv in conversions:
        ref = _props(conv)["click_event_id"]
        assert ref in click_ids, f"Conversion references unknown click: {ref}"
    if conversions:
        print(f"  ✅ All conversions reference valid clicks")
    else:
        print(f"  ⚠️  No conversions generated (5% chance per click — normal for small sample)")

    # --- Schema evolution check ---
    print(f"\nSchema evolution (v2.2 vs v2.3):")
    for e in content_plays:
        user = next(u for u in users if u.user_id == e["user_id"])
        has_quality = "content_quality" in _props(e)
        if user.app_version == "2.3.0":
            assert has_quality, f"v2.3.0 user missing content_quality"
        else:
            assert not has_quality, f"v2.2.0 user should NOT have content_quality"
    print(f"  ✅ content_quality present only for v2.3.0 users")

    # --- properties must be JSON string (Lakehouse schema-safe) ---
    assert isinstance(all_events[0]["properties"], str), (
        "properties must be json.dumps string for Lakehouse STRING inference"
    )
    print(f"  ✅ properties is JSON string (not nested dict)")

    # --- Print one complete event as JSON sample ---
    print(f"\nSample event (JSON):")
    print(json.dumps(all_events[0], indent=2))

    print("\n" + "=" * 60)
    print("ALL SESSION GENERATOR TESTS PASSED ✅")
    print("=" * 60)


if __name__ == "__main__":
    test_session_generator()
