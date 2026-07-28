# Author: Janvi Chitroda
# Copyright (c) 2026 Janvi Chitroda. All rights reserved.
# Project: ClickStream Analytics Engine — Portfolio
# Unauthorized copying or redistribution of this content is prohibited.

"""
Test: schemas.py validation
----------------------------
Verifies all 12 event types are defined, base fields are complete,
and every event type has a corresponding property definition.

Run: python tests/test_schemas.py
"""

from simulator.schemas import (
    EventType,
    EVENT_PROPERTIES,
    BASE_FIELDS,
    DEVICE_TYPES,
    APP_VERSIONS,
    GENRES,
    AD_FORMATS,
    AD_PLACEMENTS,
    SEARCH_TYPES,
    ABANDON_REASONS,
    CONVERSION_TYPES,
    PAGE_PATHS,
    SUBSCRIPTION_TIERS,
)


def test_schemas():
    print("=" * 60)
    print("SCHEMA VALIDATION")
    print("=" * 60)

    # --- Event Types ---
    print(f"\nEvent types defined: {len(EventType)}")
    assert len(EventType) == 12, f"Expected 12 event types, got {len(EventType)}"
    print("  ✅ 12 event types confirmed")

    # --- Base Fields ---
    print(f"\nBase fields: {len(BASE_FIELDS)}")
    required_base = ["event_id", "user_id", "session_id", "event_type",
                     "event_timestamp", "device_type", "app_version", "properties"]
    for field in required_base:
        assert field in BASE_FIELDS, f"Missing base field: {field}"
    print("  ✅ All 8 base fields present")

    # --- Every event type has property definition ---
    print(f"\nPer-type properties:")
    for et in EventType:
        assert et in EVENT_PROPERTIES, f"Missing properties for {et.value}"
        props = EVENT_PROPERTIES[et]
        print(f"  {et.value:25s} → {len(props)} properties: {props}")
    print("  ✅ All event types have property definitions")

    # --- Constants ---
    print(f"\nConstants:")
    print(f"  Device types:       {len(DEVICE_TYPES)} → {DEVICE_TYPES}")
    print(f"  App versions:       {len(APP_VERSIONS)} → {APP_VERSIONS}")
    print(f"  Genres:             {len(GENRES)} → {GENRES}")
    print(f"  Ad formats:         {len(AD_FORMATS)} → {AD_FORMATS}")
    print(f"  Ad placements:      {len(AD_PLACEMENTS)} → {AD_PLACEMENTS}")
    print(f"  Search types:       {len(SEARCH_TYPES)} → {SEARCH_TYPES}")
    print(f"  Abandon reasons:    {len(ABANDON_REASONS)} → {ABANDON_REASONS}")
    print(f"  Conversion types:   {len(CONVERSION_TYPES)} → {CONVERSION_TYPES}")
    print(f"  Page paths:         {len(PAGE_PATHS)} → {PAGE_PATHS[:5]}...")
    print(f"  Subscription tiers: {len(SUBSCRIPTION_TIERS)} → {SUBSCRIPTION_TIERS}")

    assert len(DEVICE_TYPES) == 4
    assert len(GENRES) == 8
    assert len(AD_FORMATS) == 4
    assert len(SUBSCRIPTION_TIERS) == 3
    print("  ✅ All constants validated")

    # --- Ad attribution chain verification ---
    print(f"\nAd attribution chain:")
    ad_click_props = EVENT_PROPERTIES[EventType.AD_CLICK]
    conversion_props = EVENT_PROPERTIES[EventType.CONVERSION]
    assert "impression_event_id" in ad_click_props, "ad_click missing impression_event_id"
    assert "click_event_id" in conversion_props, "conversion missing click_event_id"
    print("  ad_impression → ad_click (via impression_event_id) → conversion (via click_event_id)")
    print("  ✅ Attribution chain linked correctly")

    # --- Schema evolution check ---
    print(f"\nSchema evolution (v2.2 vs v2.3):")
    content_play_props = EVENT_PROPERTIES[EventType.CONTENT_PLAY]
    assert "content_quality" in content_play_props, "content_play missing content_quality"
    print("  content_play has 'content_quality' field (v2.3.0 only)")
    print("  v2.2.0 users will NOT have this field → tests schema drift handling")
    print("  ✅ Schema evolution verified")

    print("\n" + "=" * 60)
    print("ALL SCHEMA TESTS PASSED ✅")
    print("=" * 60)


if __name__ == "__main__":
    test_schemas()
