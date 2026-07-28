# Author: Janvi Chitroda
# Copyright (c) 2026 Janvi Chitroda. All rights reserved.
# Project: ClickStream Analytics Engine — Portfolio
# Unauthorized copying or redistribution of this content is prohibited.

"""
Test: chaos.py validation
--------------------------
Feeds 10,000 events through the chaos injector and verifies
that measured rates match configured targets within tolerance.

Run: PYTHONPATH=. python tests/test_chaos.py
"""

import copy
from collections import Counter
from simulator.chaos import ChaosInjector


def make_dummy_events(n: int) -> list:
    """Create n minimal valid events for testing."""
    return [
        {
            "event_id": f"evt_{i:06d}",
            "user_id": f"user_{i % 100:05d}",
            "session_id": f"sess_{i:06d}",
            "event_type": "page_view",
            "event_timestamp": "2026-07-16T10:00:00.000Z",
            "device_type": "desktop",
            "app_version": "2.3.0",
            "properties": {"page": "/home", "referrer": "direct"},
        }
        for i in range(n)
    ]


def test_chaos_rates():
    print("=" * 60)
    print("CHAOS INJECTOR VALIDATION")
    print("=" * 60)

    n = 10_000
    events = make_dummy_events(n)
    chaos = ChaosInjector()
    on_time, late_buffer = chaos.inject(events)

    print(f"\nInput events: {n}")
    print(f"Output (on-time): {len(on_time)}")
    print(f"Late buffer: {len(late_buffer)}")
    print(f"\n{chaos.get_stats_summary()}")

    # --- Duplicate rate check ---
    dup_rate = chaos.stats["duplicates_injected"] / n
    print(f"\nDuplicate rate: {dup_rate*100:.2f}% (target: 2.0%)")
    assert 0.01 <= dup_rate <= 0.04, f"Duplicate rate {dup_rate:.3f} outside [1%, 4%]"
    print("  ✅ Duplicate rate within tolerance")

    # --- Verify duplicates have same event_id ---
    event_ids = [e["event_id"] for e in on_time]
    id_counts = Counter(event_ids)
    duplicated_ids = {eid: cnt for eid, cnt in id_counts.items() if cnt > 1}
    print(f"  Duplicated event_ids: {len(duplicated_ids)}")
    assert len(duplicated_ids) == chaos.stats["duplicates_injected"], \
        "Mismatch between duplicate count and duplicated event_ids"
    # Every duplicate should appear exactly twice
    for eid, cnt in duplicated_ids.items():
        assert cnt == 2, f"Event {eid} appeared {cnt} times (expected 2)"
    print("  ✅ All duplicates have identical event_ids (exact copies)")

    # --- Late arrival rate check ---
    late_rate = chaos.stats["late_arrivals_held"] / n
    print(f"\nLate arrival rate: {late_rate*100:.2f}% (target: 5.0%)")
    assert 0.03 <= late_rate <= 0.08, f"Late rate {late_rate:.3f} outside [3%, 8%]"
    print("  ✅ Late arrival rate within tolerance")

    # --- Verify late events have valid timestamps ---
    for event in late_buffer:
        assert "event_timestamp" in event, "Late event missing timestamp"
        assert event["event_timestamp"] == "2026-07-16T10:00:00.000Z", \
            "Late event timestamp should be original (not modified)"
    print("  ✅ Late events retain original timestamps")

    # --- Malformed rate check ---
    malformed_rate = chaos.stats["malformed_injected"] / n
    print(f"\nMalformed rate: {malformed_rate*100:.2f}% (target: 0.5%)")
    assert 0.001 <= malformed_rate <= 0.015, f"Malformed rate {malformed_rate:.3f} outside [0.1%, 1.5%]"
    print("  ✅ Malformed rate within tolerance")

    # --- Verify malformed events are actually broken ---
    malformed_events = [e for e in on_time if "_chaos_type" in e]
    print(f"  Malformed events found: {len(malformed_events)}")

    corruption_types = Counter(e["_chaos_type"] for e in malformed_events)
    print(f"  Corruption distribution: {dict(corruption_types)}")

    for event in malformed_events:
        ctype = event["_chaos_type"]
        if ctype == "null_user":
            assert event["user_id"] is None, "null_user should have None user_id"
        elif ctype == "bad_timestamp":
            assert event["event_timestamp"] == "not-a-timestamp", \
                "bad_timestamp should have invalid timestamp"
        elif ctype == "missing_type":
            assert "event_type" not in event, "missing_type should not have event_type"
    print("  ✅ All malformed events are correctly corrupted")

    # --- Clean events check ---
    clean_rate = chaos.stats["clean_passed"] / n
    print(f"\nClean pass-through rate: {clean_rate*100:.2f}%")
    expected_clean = 1.0 - 0.02 - 0.05 - 0.005
    assert 0.85 <= clean_rate <= 0.97, f"Clean rate {clean_rate:.3f} outside expected range"
    print("  ✅ Clean rate within expected range")

    # --- Output count check ---
    # on_time should have: clean + duplicates*2 + malformed
    expected_on_time = (chaos.stats["clean_passed"]
                        + chaos.stats["duplicates_injected"] * 2
                        + chaos.stats["malformed_injected"])
    assert len(on_time) == expected_on_time, \
        f"Output count {len(on_time)} != expected {expected_on_time}"
    print(f"\n  Output accounting: {chaos.stats['clean_passed']} clean + "
          f"{chaos.stats['duplicates_injected']}×2 duplicates + "
          f"{chaos.stats['malformed_injected']} malformed = {expected_on_time} ✅")

    # --- Total accounting ---
    total_accounted = (chaos.stats["clean_passed"]
                       + chaos.stats["duplicates_injected"]
                       + chaos.stats["late_arrivals_held"]
                       + chaos.stats["malformed_injected"])
    assert total_accounted == n, f"Accounting mismatch: {total_accounted} != {n}"
    print(f"  Total accounting: {total_accounted} = {n} input events ✅")

    print("\n" + "=" * 60)
    print("ALL CHAOS INJECTOR TESTS PASSED ✅")
    print("=" * 60)


def test_schema_variation():
    print("\n" + "=" * 60)
    print("SCHEMA VARIATION TEST")
    print("=" * 60)

    chaos = ChaosInjector()

    # Event with content_quality (v2.3 field)
    event_v23 = {
        "event_type": "content_play",
        "app_version": "2.3.0",
        "properties": {"content_id": "c001", "content_quality": "4k"},
    }
    event_v22 = {
        "event_type": "content_play",
        "app_version": "2.2.0",
        "properties": {"content_id": "c001", "content_quality": "4k"},
    }

    # Mock user profiles
    class MockUser:
        def __init__(self, version):
            self.app_version = version

    # v2.3 user keeps content_quality
    result_v23 = chaos.apply_schema_variation(copy.deepcopy(event_v23), MockUser("2.3.0"))
    assert "content_quality" in result_v23["properties"], "v2.3 should keep content_quality"
    print("  v2.3.0 user: content_quality preserved ✅")

    # v2.2 user loses content_quality
    result_v22 = chaos.apply_schema_variation(copy.deepcopy(event_v22), MockUser("2.2.0"))
    assert "content_quality" not in result_v22["properties"], "v2.2 should NOT have content_quality"
    print("  v2.2.0 user: content_quality removed ✅")

    print("\n" + "=" * 60)
    print("ALL SCHEMA VARIATION TESTS PASSED ✅")
    print("=" * 60)


if __name__ == "__main__":
    test_chaos_rates()
    test_schema_variation()
