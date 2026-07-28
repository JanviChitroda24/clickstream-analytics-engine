# Author: Janvi Chitroda
# Copyright (c) 2026 Janvi Chitroda. All rights reserved.
# Project: ClickStream Analytics Engine — Portfolio
# Unauthorized copying or redistribution of this content is prohibited.

"""
Session Generator
-----------------
Takes a UserProfile + content catalog and generates a realistic,
time-ordered sequence of clickstream events for one session.

Session flow:
  page_view → [search] → content_play sequences → [ad sequences] → [watchlist]

Each content play sequence:
  content_play → [content_pause → content_resume]* → content_complete | content_abandon

Ad sequence:
  ad_impression → [ad_click] → [conversion]
"""

import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

try:
    from uuid_extensions import uuid7str
except ImportError:
    # Fallback if uuid7 package not available
    uuid7str = lambda: str(uuid.uuid4())

from simulator.schemas import (
    EventType,
    GENRES,
    PAGE_PATHS,
    AD_FORMATS,
    AD_PLACEMENTS,
    SEARCH_TYPES,
    ABANDON_REASONS,
    CONVERSION_TYPES,
)


class SessionGenerator:
    """
    Generates a single browsing session for a given user.

    Usage:
        gen = SessionGenerator(user_profile, content_catalog, ad_campaigns)
        events = gen.generate_session(session_start_time)
    """

    def __init__(self, user_profile, content_catalog: List[Dict], ad_campaigns: List[Dict] = None):
        self.user = user_profile
        self.catalog = content_catalog
        self.campaigns = ad_campaigns or []

    def generate_session(self, session_start_time: datetime) -> List[Dict]:
        """
        Generate a complete session: ordered sequence of events.

        Returns list of event dicts, sorted by event_timestamp.
        """
        session_id = f"sess_{uuid.uuid4().hex[:12]}"
        events = []
        current_time = session_start_time

        # --- Every session starts with page_view ---
        events.append(self._make_event(
            EventType.PAGE_VIEW, current_time, session_id,
            properties={
                "page": random.choice(PAGE_PATHS),
                "referrer": random.choice(["direct", "push_notification", "email", "external_link"]),
            }
        ))
        current_time += timedelta(seconds=random.randint(5, 30))

        # --- 60% chance of search ---
        if random.random() < 0.6:
            query = random.choice([
                "action movies", "new releases", "comedy specials",
                "thriller series", "documentary", "trending now",
                "top rated", "sci-fi", "romance films", "horror",
                "award winning", "family friendly", "classic movies",
            ])
            events.append(self._make_event(
                EventType.SEARCH, current_time, session_id,
                properties={
                    "query_text": query,
                    "result_count": random.randint(0, 150),
                    "search_type": random.choice(SEARCH_TYPES),
                }
            ))
            current_time += timedelta(seconds=random.randint(3, 20))

        # --- Content play sequences ---
        num_plays = self._plays_per_session()
        for _ in range(num_plays):
            content = random.choice(self.catalog)
            content_events, current_time = self._content_sequence(
                current_time, session_id, content
            )
            events.extend(content_events)
            # Gap between content plays
            current_time += timedelta(minutes=random.randint(1, 10))

        # --- Ad events (40% of sessions) ---
        if random.random() < 0.4 and self.campaigns:
            ad_events, current_time = self._ad_sequence(current_time, session_id)
            events.extend(ad_events)

        # --- 15% chance of add_to_watchlist ---
        if random.random() < 0.15 and self.catalog:
            content = random.choice(self.catalog)
            events.append(self._make_event(
                EventType.ADD_TO_WATCHLIST, current_time, session_id,
                properties={
                    "content_id": content["content_id"],
                    "genre": content["genre"],
                }
            ))

        return events

    def _plays_per_session(self) -> int:
        """Number of content plays in a session, driven by archetype."""
        from simulator.user_profiles import UserArchetype
        ranges = {
            UserArchetype.POWER: (2, 5),
            UserArchetype.REGULAR: (1, 3),
            UserArchetype.CASUAL: (1, 2),
            UserArchetype.CHURNING: (1, 2),
            UserArchetype.NEW: (2, 4),  # exploring
        }
        low, high = ranges.get(self.user.archetype, (1, 3))
        return random.randint(low, high)

    def _content_sequence(
        self, start_time: datetime, session_id: str, content: Dict
    ) -> tuple:
        """
        Generate a content viewing sequence:
          content_play → [pause → resume]* → complete | abandon

        Returns (list of events, updated current_time).
        """
        events = []
        current_time = start_time
        content_duration = content.get("duration_seconds", 2700)  # default 45 min

        # --- content_play ---
        position_start = 0
        play_properties = {
            "content_id": content["content_id"],
            "genre": content["genre"],
            "position_seconds": position_start,
        }
        # v2.3.0 only: add content_quality
        if self.user.app_version == "2.3.0":
            play_properties["content_quality"] = random.choice(["sd", "hd", "4k"])

        events.append(self._make_event(
            EventType.CONTENT_PLAY, current_time, session_id,
            properties=play_properties,
        ))

        # Simulate watch duration (fraction of content)
        watch_fraction = random.uniform(0.1, 1.0)
        watch_seconds = int(content_duration * watch_fraction)
        current_time += timedelta(seconds=random.randint(5, 30))

        # --- Optional pause/resume (30% chance, can happen multiple times) ---
        pause_count = 0
        position = position_start
        while random.random() < 0.3 and pause_count < 3:
            position += random.randint(60, 300)  # advance 1-5 minutes
            if position >= watch_seconds:
                break

            # Pause
            events.append(self._make_event(
                EventType.CONTENT_PAUSE, current_time, session_id,
                properties={
                    "content_id": content["content_id"],
                    "position_seconds": position,
                }
            ))
            current_time += timedelta(seconds=random.randint(30, 300))  # paused 30s-5min

            # Resume
            events.append(self._make_event(
                EventType.CONTENT_RESUME, current_time, session_id,
                properties={
                    "content_id": content["content_id"],
                    "position_seconds": position,
                }
            ))
            current_time += timedelta(seconds=random.randint(10, 60))
            pause_count += 1

        # --- Complete or abandon ---
        completion_rate = self.user.get_current_completion_rate()
        current_time += timedelta(seconds=watch_seconds)

        if random.random() < completion_rate:
            # Content complete
            events.append(self._make_event(
                EventType.CONTENT_COMPLETE, current_time, session_id,
                properties={
                    "content_id": content["content_id"],
                    "genre": content["genre"],
                    "watch_duration_seconds": watch_seconds,
                    "content_duration_seconds": content_duration,
                }
            ))
        else:
            # Content abandon
            abandon_position = random.randint(
                int(content_duration * 0.05),
                int(content_duration * 0.90),
            )
            events.append(self._make_event(
                EventType.CONTENT_ABANDON, current_time, session_id,
                properties={
                    "content_id": content["content_id"],
                    "genre": content["genre"],
                    "position_seconds": abandon_position,
                    "content_duration_seconds": content_duration,
                    "abandon_reason": random.choice(ABANDON_REASONS),
                }
            ))

        return events, current_time

    def _ad_sequence(self, start_time: datetime, session_id: str) -> tuple:
        """
        Generate ad funnel:
          ad_impression → [ad_click (30%)] → [conversion (5% of clicks)]

        Returns (list of events, updated current_time).
        """
        events = []
        current_time = start_time
        campaign = random.choice(self.campaigns) if self.campaigns else {
            "campaign_id": f"camp_{random.randint(1, 20):03d}",
            "advertiser_id": f"adv_{random.randint(1, 10):03d}",
        }

        # --- ad_impression ---
        impression_event_id = uuid7str()
        events.append(self._make_event(
            EventType.AD_IMPRESSION, current_time, session_id,
            properties={
                "campaign_id": campaign["campaign_id"],
                "advertiser_id": campaign["advertiser_id"],
                "ad_format": random.choice(AD_FORMATS),
                "placement": random.choice(AD_PLACEMENTS),
            },
            override_event_id=impression_event_id,
        ))
        current_time += timedelta(seconds=random.randint(2, 10))

        # --- ad_click (based on user's ad click rate) ---
        if random.random() < self.user.ad_click_rate:
            click_event_id = uuid7str()
            events.append(self._make_event(
                EventType.AD_CLICK, current_time, session_id,
                properties={
                    "campaign_id": campaign["campaign_id"],
                    "advertiser_id": campaign["advertiser_id"],
                    "landing_url": f"https://advertiser.example.com/{campaign['campaign_id']}",
                    "impression_event_id": impression_event_id,
                },
                override_event_id=click_event_id,
            ))
            current_time += timedelta(seconds=random.randint(5, 30))

            # --- conversion (5% of clicks) ---
            if random.random() < 0.05:
                events.append(self._make_event(
                    EventType.CONVERSION, current_time, session_id,
                    properties={
                        "campaign_id": campaign["campaign_id"],
                        "advertiser_id": campaign["advertiser_id"],
                        "conversion_type": random.choice(CONVERSION_TYPES),
                        "conversion_value": round(random.uniform(1.0, 200.0), 2),
                        "click_event_id": click_event_id,
                    },
                ))

        return events, current_time

    def _make_event(
        self,
        event_type: EventType,
        timestamp: datetime,
        session_id: str,
        properties: Dict,
        override_event_id: str = None,
    ) -> Dict:
        """
        Construct a complete event dict with all base fields + properties.
        """
        return {
            "event_id": override_event_id or uuid7str(),
            "user_id": self.user.user_id,
            "session_id": session_id,
            "event_type": event_type.value,
            "event_timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "device_type": self.user.device_type,
            "app_version": self.user.app_version,
            "properties": properties,
        }
