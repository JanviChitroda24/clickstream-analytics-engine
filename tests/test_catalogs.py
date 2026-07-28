# Author: Janvi Chitroda
# Copyright (c) 2026 Janvi Chitroda. All rights reserved.
# Project: ClickStream Analytics Engine — Portfolio
# Unauthorized copying or redistribution of this content is prohibited.

"""
Test: content_catalog.py and ad_campaigns.py validation
---------------------------------------------------------
Verifies catalog sizes, field completeness, and data distributions.

Run: PYTHONPATH=. python tests/test_catalogs.py
"""

from collections import Counter
from simulator.content_catalog import create_content_catalog
from simulator.ad_campaigns import create_ad_campaigns
from simulator.schemas import GENRES


def test_content_catalog():
    print("=" * 60)
    print("CONTENT CATALOG VALIDATION")
    print("=" * 60)

    catalog = create_content_catalog(200)
    print(f"\nCatalog size: {len(catalog)}")
    assert len(catalog) == 200, f"Expected 200, got {len(catalog)}"
    print("  ✅ 200 content items created")

    # --- Required fields ---
    required_fields = ["content_id", "title", "genre", "duration_seconds",
                       "content_tier", "release_year"]
    for item in catalog:
        for field in required_fields:
            assert field in item, f"Missing field '{field}' in {item['content_id']}"
    print("  ✅ All required fields present")

    # --- Genre distribution ---
    print(f"\nGenre distribution:")
    genres = Counter(item["genre"] for item in catalog)
    for genre, count in sorted(genres.items(), key=lambda x: -x[1]):
        print(f"  {genre:15s}: {count}")
    assert all(g in GENRES for g in genres), "Unknown genre found"
    print("  ✅ All genres valid")

    # --- Content tier distribution ---
    print(f"\nContent tier distribution:")
    tiers = Counter(item["content_tier"] for item in catalog)
    for tier, count in sorted(tiers.items()):
        pct = count / len(catalog) * 100
        print(f"  {tier:10s}: {count} ({pct:.0f}%)")
    assert set(tiers.keys()).issubset({"free", "basic", "premium"})
    print("  ✅ All tiers valid")

    # --- Duration distribution ---
    durations = [item["duration_seconds"] for item in catalog]
    avg_min = sum(durations) / len(durations) / 60
    min_min = min(durations) / 60
    max_min = max(durations) / 60
    print(f"\nDuration stats:")
    print(f"  Min:  {min_min:.0f} min")
    print(f"  Max:  {max_min:.0f} min")
    print(f"  Avg:  {avg_min:.0f} min")
    assert min(durations) >= 15 * 60, "Duration too short"
    assert max(durations) <= 170 * 60, "Duration too long"
    print("  ✅ Durations within realistic range")

    # --- Sample items ---
    print(f"\nSample content (first 5):")
    for item in catalog[:5]:
        dur_min = item["duration_seconds"] / 60
        print(f"  {item['content_id']} | {item['title']:35s} | {item['genre']:12s} | "
              f"{dur_min:.0f} min | {item['content_tier']:8s} | {item['release_year']}")

    # --- Unique IDs ---
    ids = [item["content_id"] for item in catalog]
    assert len(set(ids)) == 200, "Duplicate content_ids found"
    print("  ✅ All content_ids unique")

    print("\n" + "=" * 60)
    print("ALL CONTENT CATALOG TESTS PASSED ✅")
    print("=" * 60)


def test_ad_campaigns():
    print("\n" + "=" * 60)
    print("AD CAMPAIGN VALIDATION")
    print("=" * 60)

    campaigns = create_ad_campaigns(20)
    print(f"\nCampaigns created: {len(campaigns)}")
    assert len(campaigns) == 20, f"Expected 20, got {len(campaigns)}"
    print("  ✅ 20 campaigns created")

    # --- Required fields ---
    required_fields = ["campaign_id", "advertiser_id", "advertiser_name",
                       "campaign_type", "budget_tier", "target_genres",
                       "conversion_value_range"]
    for camp in campaigns:
        for field in required_fields:
            assert field in camp, f"Missing field '{field}' in {camp['campaign_id']}"
    print("  ✅ All required fields present")

    # --- Campaign type distribution ---
    print(f"\nCampaign type distribution:")
    types = Counter(camp["campaign_type"] for camp in campaigns)
    for ctype, count in sorted(types.items()):
        print(f"  {ctype:20s}: {count}")
    assert set(types.keys()).issubset({"brand_awareness", "app_install", "product_purchase"})
    print("  ✅ All campaign types valid")

    # --- Budget tier distribution ---
    print(f"\nBudget tier distribution:")
    budgets = Counter(camp["budget_tier"] for camp in campaigns)
    for tier, count in sorted(budgets.items()):
        print(f"  {tier:10s}: {count}")
    print("  ✅ All budget tiers valid")

    # --- Conversion value ranges make sense ---
    print(f"\nConversion value ranges by type:")
    for camp in campaigns:
        low, high = camp["conversion_value_range"]
        assert low < high, f"Bad value range in {camp['campaign_id']}"
        if camp["campaign_type"] == "brand_awareness":
            assert high <= 10.0, f"Brand awareness value too high: {high}"
        elif camp["campaign_type"] == "product_purchase":
            assert low >= 5.0, f"Product purchase value too low: {low}"
    for ctype in ["brand_awareness", "app_install", "product_purchase"]:
        sample = next(c for c in campaigns if c["campaign_type"] == ctype)
        low, high = sample["conversion_value_range"]
        print(f"  {ctype:20s}: ${low:.2f} — ${high:.2f}")
    print("  ✅ Conversion values match campaign types")

    # --- Target genres valid ---
    for camp in campaigns:
        for g in camp["target_genres"]:
            assert g in GENRES, f"Invalid target genre '{g}' in {camp['campaign_id']}"
        assert 1 <= len(camp["target_genres"]) <= 3
    print("  ✅ All target genres valid (1-3 per campaign)")

    # --- Sample campaigns ---
    print(f"\nSample campaigns (first 5):")
    for camp in campaigns[:5]:
        print(f"  {camp['campaign_id']} | {camp['advertiser_name']:22s} | "
              f"{camp['campaign_type']:18s} | {camp['budget_tier']:7s} | "
              f"genres: {camp['target_genres']}")

    # --- Unique IDs ---
    ids = [camp["campaign_id"] for camp in campaigns]
    assert len(set(ids)) == 20, "Duplicate campaign_ids found"
    print("  ✅ All campaign_ids unique")

    print("\n" + "=" * 60)
    print("ALL AD CAMPAIGN TESTS PASSED ✅")
    print("=" * 60)


if __name__ == "__main__":
    test_content_catalog()
    test_ad_campaigns()
