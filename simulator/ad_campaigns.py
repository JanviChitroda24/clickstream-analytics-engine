"""
Ad Campaign Catalog
-------------------
Generates 20 ad campaigns with realistic metadata.
Used by the session generator to populate ad events.

Campaign types:
  - brand_awareness: high impressions, low conversion value
  - app_install: moderate impressions, moderate conversion value
  - product_purchase: targeted impressions, high conversion value

Each campaign has:
  campaign_id, advertiser_id, advertiser_name, campaign_type,
  budget_tier, target_genres, conversion_value_range
"""

import random
from typing import Dict, List
from simulator.schemas import GENRES


ADVERTISER_NAMES = [
    "TechNova", "FreshBite Foods", "CloudRun Shoes", "BrightPath Insurance",
    "GreenLeaf Organic", "PixelForge Games", "SkyMiles Travel", "VoltEdge Electronics",
    "PureWave Wellness", "UrbanCraft Furniture", "BlueShift Finance", "ArcticBreeze AC",
    "Solaris Energy", "NexGen Pharma", "WildTrail Outdoor", "DataPulse Software",
    "CrystalView Optics", "SwiftDash Delivery", "OmniHealth Labs", "ZenSpace Meditation",
]


def create_ad_campaigns(size: int = 20) -> List[Dict]:
    """
    Generate ad campaigns with realistic metadata.

    Returns list of dicts:
        campaign_id, advertiser_id, advertiser_name, campaign_type,
        budget_tier, target_genres, conversion_value_range
    """
    campaigns = []
    campaign_types = ["brand_awareness", "app_install", "product_purchase"]

    for i in range(size):
        campaign_type = random.choice(campaign_types)

        # Conversion value ranges depend on campaign type
        value_ranges = {
            "brand_awareness": (0.50, 5.00),      # low value per conversion
            "app_install": (2.00, 25.00),          # moderate
            "product_purchase": (10.00, 200.00),   # high value
        }

        # Budget tiers
        budget_tier = random.choices(
            ["small", "medium", "large"],
            weights=[0.40, 0.40, 0.20],
            k=1,
        )[0]

        # Target 1-3 genres
        num_target_genres = random.randint(1, 3)
        target_genres = random.sample(GENRES, num_target_genres)

        campaigns.append({
            "campaign_id": f"camp_{i:03d}",
            "advertiser_id": f"adv_{i:03d}",
            "advertiser_name": ADVERTISER_NAMES[i % len(ADVERTISER_NAMES)],
            "campaign_type": campaign_type,
            "budget_tier": budget_tier,
            "target_genres": target_genres,
            "conversion_value_range": value_ranges[campaign_type],
        })

    return campaigns
