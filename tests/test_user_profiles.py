# Author: Janvi Chitroda
# Copyright (c) 2026 Janvi Chitroda. All rights reserved.
# Project: ClickStream Analytics Engine — Portfolio
# Unauthorized copying or redistribution of this content is prohibited.

"""
Test: user_profiles.py validation
-----------------------------------
Verifies user population distribution, archetype behavior,
app version split, churning decay, and new user settling.

Run: python tests/test_user_profiles.py
"""

from collections import Counter
from simulator.user_profiles import create_user_population, UserArchetype, UserProfile


def test_user_profiles():
    print("=" * 60)
    print("USER PROFILE VALIDATION")
    print("=" * 60)

    # --- Create population ---
    users = create_user_population()
    print(f"\nTotal users created: {len(users)}")
    assert len(users) == 10_000, f"Expected 10,000 users, got {len(users)}"
    print("  ✅ 10,000 users confirmed")

    # --- Archetype distribution ---
    print(f"\nArchetype distribution:")
    expected = {
        "power": 500,
        "regular": 5500,
        "casual": 2500,
        "churning": 1000,
        "new": 500,
    }
    counts = Counter(u.archetype.value for u in users)
    for archetype, count in sorted(counts.items()):
        exp = expected[archetype]
        status = "✅" if count == exp else "❌"
        print(f"  {archetype:10s}: {count:5d} (expected {exp}) {status}")
        assert count == exp, f"{archetype}: expected {exp}, got {count}"
    print("  ✅ All archetype counts match")

    # --- App version split ---
    print(f"\nApp version split:")
    versions = Counter(u.app_version for u in users)
    for ver, count in sorted(versions.items()):
        pct = count / len(users) * 100
        print(f"  v{ver}: {count:5d} ({pct:.1f}%)")

    power_users = [u for u in users if u.archetype == UserArchetype.POWER]
    new_users = [u for u in users if u.archetype == UserArchetype.NEW]
    power_on_23 = sum(1 for u in power_users if u.app_version == "2.3.0")
    new_on_23 = sum(1 for u in new_users if u.app_version == "2.3.0")
    assert power_on_23 == 500, f"Power users not all on v2.3.0: {power_on_23}/500"
    assert new_on_23 == 500, f"New users not all on v2.3.0: {new_on_23}/500"
    print(f"  Power users on v2.3.0: {power_on_23}/500 ✅")
    print(f"  New users on v2.3.0:   {new_on_23}/500 ✅")
    print("  ✅ Version split validated")

    # --- Device type distribution ---
    print(f"\nDevice type distribution:")
    devices = Counter(u.device_type for u in users)
    for device, count in sorted(devices.items()):
        pct = count / len(users) * 100
        print(f"  {device:18s}: {count:5d} ({pct:.1f}%)")
    assert len(devices) == 4, "Expected 4 device types"
    print("  ✅ All 4 device types present")

    # --- Subscription tier distribution ---
    print(f"\nSubscription tier distribution:")
    tiers = Counter(u.subscription_tier for u in users)
    for tier, count in sorted(tiers.items()):
        pct = count / len(users) * 100
        print(f"  {tier:10s}: {count:5d} ({pct:.1f}%)")
    assert len(tiers) == 3, "Expected 3 subscription tiers"
    print("  ✅ All 3 tiers present")

    # --- Sample users ---
    print(f"\nSample users (first 5):")
    for u in users[:5]:
        print(f"  {u}")

    # --- Churning user decay test ---
    print(f"\nChurning user session decay over 14 days:")
    churner = [u for u in users if u.archetype == UserArchetype.CHURNING][0]
    initial_rate = churner.sessions_per_day
    print(f"  Initial rate: {initial_rate:.2f} sessions/day")

    for day in range(15):
        churner.day_number = day
        rate = churner.get_current_session_rate()
        comp = churner.get_current_completion_rate()
        bar = "█" * int(rate * 10)
        print(f"  Day {day:2d}: sessions={rate:.2f}  completion={comp:.2f}  {bar}")

    churner.day_number = 14
    assert churner.get_current_session_rate() == 0.0, "Churner should be at 0 by day 14"
    print("  ✅ Churning decay reaches 0 by day 14")

    # --- New user settling test ---
    print(f"\nNew user settling after day 3:")
    new_user = [u for u in users if u.archetype == UserArchetype.NEW][0]
    initial_new_rate = new_user.sessions_per_day
    print(f"  Initial rate (day 0-3): {initial_new_rate:.2f} sessions/day")

    new_user.day_number = 1
    rate_day1 = new_user.get_current_session_rate()
    new_user.day_number = 5
    rate_day5 = new_user.get_current_session_rate()
    print(f"  Day 1 rate: {rate_day1:.2f} (should be high)")
    print(f"  Day 5 rate: {rate_day5:.2f} (should be settled, 0.3-1.0)")
    assert rate_day1 == initial_new_rate, "New user should keep initial rate in first 3 days"
    assert 0.0 <= rate_day5 <= 1.5, "New user should settle after day 3"
    print("  ✅ New user settling verified")

    # --- Behavioral range sanity checks ---
    print(f"\nBehavioral range sanity checks:")
    for u in users:
        assert 0 < u.sessions_per_day <= 6, f"Bad session rate: {u}"
        assert 0 < u.avg_events_per_session <= 40, f"Bad events/session: {u}"
        assert 0 < u.content_completion_rate <= 1.0, f"Bad completion rate: {u}"
        assert 0 < u.ad_click_rate <= 1.0, f"Bad ad click rate: {u}"
        assert u.subscription_tier in ("free", "basic", "premium"), f"Bad tier: {u}"
    print("  All 10,000 users have valid behavioral ranges ✅")

    print("\n" + "=" * 60)
    print("ALL USER PROFILE TESTS PASSED ✅")
    print("=" * 60)


if __name__ == "__main__":
    test_user_profiles()
