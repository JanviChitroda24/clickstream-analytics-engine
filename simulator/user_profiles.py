# Author: Janvi Chitroda
# Copyright (c) 2026 Janvi Chitroda. All rights reserved.
# Project: ClickStream Analytics Engine — Portfolio
# Unauthorized copying or redistribution of this content is prohibited.

"""
User Profile Classes
--------------------
5 behavioral archetypes that control session frequency, engagement depth,
and content completion patterns. Each of the 10,000 simulated users
is assigned one archetype.

Distribution:
  power     —  500 users (5%)  — 20+ sessions/week, high completion
  regular   — 5500 users (55%) — 3-7 sessions/week, moderate completion
  casual    — 2500 users (25%) — 1-2 sessions/week, low completion
  churning  — 1000 users (10%) — declining activity over 14 days
  new       —  500 users (5%)  — heavy first 3 days, then settles
"""

import random
from enum import Enum
from simulator.schemas import DEVICE_TYPES, APP_VERSIONS


class UserArchetype(Enum):
    POWER = "power"
    REGULAR = "regular"
    CASUAL = "casual"
    CHURNING = "churning"
    NEW = "new"


class UserProfile:
    """
    Represents one simulated user with behavioral parameters
    driven by their archetype.
    """

    def __init__(self, user_id: str, archetype: UserArchetype, app_version: str):
        self.user_id = user_id
        self.archetype = archetype
        self.app_version = app_version
        self.device_type = random.choice(DEVICE_TYPES)
        self.sessions_per_day = self._base_session_rate()
        self.avg_events_per_session = self._base_events_per_session()
        self.content_completion_rate = self._base_completion_rate()
        self.ad_click_rate = self._base_ad_click_rate()
        self.subscription_tier = self._initial_tier()
        self.day_number = 0  # incremented by the simulator each day

    # --- Base behavioral rates per archetype ---

    def _base_session_rate(self) -> float:
        """Average sessions per day."""
        rates = {
            UserArchetype.POWER: random.uniform(3.0, 5.0),
            UserArchetype.REGULAR: random.uniform(0.5, 1.5),
            UserArchetype.CASUAL: random.uniform(0.1, 0.4),
            UserArchetype.CHURNING: random.uniform(0.5, 1.5),  # starts like regular
            UserArchetype.NEW: random.uniform(2.0, 4.0),       # heavy exploration
        }
        return rates[self.archetype]

    def _base_events_per_session(self) -> int:
        """Average events generated per session."""
        rates = {
            UserArchetype.POWER: random.randint(15, 35),
            UserArchetype.REGULAR: random.randint(8, 18),
            UserArchetype.CASUAL: random.randint(3, 8),
            UserArchetype.CHURNING: random.randint(5, 12),
            UserArchetype.NEW: random.randint(10, 25),  # exploring a lot
        }
        return rates[self.archetype]

    def _base_completion_rate(self) -> float:
        """Probability of watching content to completion."""
        rates = {
            UserArchetype.POWER: random.uniform(0.6, 0.85),
            UserArchetype.REGULAR: random.uniform(0.35, 0.55),
            UserArchetype.CASUAL: random.uniform(0.15, 0.30),
            UserArchetype.CHURNING: random.uniform(0.20, 0.40),
            UserArchetype.NEW: random.uniform(0.25, 0.45),
        }
        return rates[self.archetype]

    def _base_ad_click_rate(self) -> float:
        """Probability of clicking an ad after seeing an impression."""
        rates = {
            UserArchetype.POWER: random.uniform(0.05, 0.15),    # low — knows to skip
            UserArchetype.REGULAR: random.uniform(0.20, 0.35),
            UserArchetype.CASUAL: random.uniform(0.25, 0.40),   # higher — less ad-savvy
            UserArchetype.CHURNING: random.uniform(0.10, 0.20),
            UserArchetype.NEW: random.uniform(0.30, 0.50),      # curious, clicks more
        }
        return rates[self.archetype]

    def _initial_tier(self) -> str:
        """Starting subscription tier."""
        tier_probs = {
            UserArchetype.POWER: {"free": 0.05, "basic": 0.25, "premium": 0.70},
            UserArchetype.REGULAR: {"free": 0.15, "basic": 0.55, "premium": 0.30},
            UserArchetype.CASUAL: {"free": 0.50, "basic": 0.35, "premium": 0.15},
            UserArchetype.CHURNING: {"free": 0.20, "basic": 0.50, "premium": 0.30},
            UserArchetype.NEW: {"free": 0.60, "basic": 0.30, "premium": 0.10},
        }
        probs = tier_probs[self.archetype]
        return random.choices(
            list(probs.keys()),
            weights=list(probs.values()),
            k=1,
        )[0]

    # --- Dynamic behavioral adjustments ---

    def get_current_session_rate(self) -> float:
        """
        Returns adjusted session rate based on day_number.
        - Churning users: linear decline over 14 days → 0
        - New users: heavy first 3 days, then settle to regular range
        - All others: stable
        """
        if self.archetype == UserArchetype.CHURNING:
            decay = max(0.0, 1.0 - (self.day_number / 14.0))
            return self.sessions_per_day * decay
        elif self.archetype == UserArchetype.NEW and self.day_number > 3:
            return random.uniform(0.3, 1.0)  # settles to casual-regular range
        return self.sessions_per_day

    def get_current_completion_rate(self) -> float:
        """
        Churning users' completion rate decays along with engagement.
        """
        if self.archetype == UserArchetype.CHURNING:
            decay = max(0.1, 1.0 - (self.day_number / 14.0))
            return self.content_completion_rate * decay
        return self.content_completion_rate

    def advance_day(self):
        """Called by the simulator at the start of each new simulated day."""
        self.day_number += 1

    def __repr__(self):
        return (
            f"UserProfile(id={self.user_id}, archetype={self.archetype.value}, "
            f"v={self.app_version}, device={self.device_type}, "
            f"sessions/day={self.sessions_per_day:.1f}, tier={self.subscription_tier})"
        )


def create_user_population() -> list:
    """
    Creates the full 10,000 user population per the locked distribution.

    Returns list of UserProfile objects.
    """
    users = []
    user_counter = 0

    distribution = {
        UserArchetype.POWER: 500,
        UserArchetype.REGULAR: 5500,
        UserArchetype.CASUAL: 2500,
        UserArchetype.CHURNING: 1000,
        UserArchetype.NEW: 500,
    }

    for archetype, count in distribution.items():
        for _ in range(count):
            user_id = f"user_{user_counter:05d}"
            user_counter += 1

            # Assign app version — 60% on 2.3.0, 40% on 2.2.0
            # Power and New users always on latest (early adopters)
            if archetype in (UserArchetype.POWER, UserArchetype.NEW):
                app_version = "2.3.0"
            else:
                app_version = random.choices(
                    list(APP_VERSIONS.keys()),
                    weights=list(APP_VERSIONS.values()),
                    k=1,
                )[0]

            users.append(UserProfile(user_id, archetype, app_version))

    random.shuffle(users)  # mix archetypes so user_ids aren't clustered by type
    return users
