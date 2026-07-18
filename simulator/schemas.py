"""
Clickstream Event Schemas
-------------------------
Defines 12 event types, base schema, and per-type properties.
This file is the contract between the simulator and the downstream pipeline.

Event taxonomy:
  Navigation:   page_view, search
  Engagement:   content_play, content_pause, content_resume, content_complete, content_abandon
  Monetization:  ad_impression, ad_click, conversion
  Intent:        add_to_watchlist
  Business:      subscription_change
"""

from enum import Enum
from typing import Dict, List, Optional


class EventType(Enum):
    """All 12 event types the simulator can generate."""
    PAGE_VIEW = "page_view"
    SEARCH = "search"
    CONTENT_PLAY = "content_play"
    CONTENT_PAUSE = "content_pause"
    CONTENT_RESUME = "content_resume"
    CONTENT_COMPLETE = "content_complete"
    CONTENT_ABANDON = "content_abandon"
    AD_IMPRESSION = "ad_impression"
    AD_CLICK = "ad_click"
    CONVERSION = "conversion"
    ADD_TO_WATCHLIST = "add_to_watchlist"
    SUBSCRIPTION_CHANGE = "subscription_change"


# --- Base schema (every event has these) ---
BASE_FIELDS = [
    "event_id",           # UUID v7 (time-sortable)
    "user_id",            # user_{00000-09999}
    "session_id",         # sess_{uuid_hex[:12]}
    "event_type",         # one of EventType values
    "event_timestamp",    # ISO 8601 UTC
    "device_type",        # mobile_ios | mobile_android | desktop | smart_tv
    "app_version",        # "2.3.0" or "2.2.0"
    "properties",         # dict — type-specific payload (see below)
]


# --- Per-type property definitions ---
# Each key maps to the list of fields expected inside "properties"

EVENT_PROPERTIES: Dict[EventType, List[str]] = {
    EventType.PAGE_VIEW: [
        "page",             # e.g., "/home", "/browse/drama", "/settings"
        "referrer",         # previous page or "direct"
    ],

    EventType.SEARCH: [
        "query_text",       # search string
        "result_count",     # number of results returned
        "search_type",      # "title" | "genre" | "actor"
    ],

    EventType.CONTENT_PLAY: [
        "content_id",       # references content catalog
        "genre",            # drama, comedy, action, etc.
        "position_seconds", # where playback started (0 for fresh start)
        "content_quality",  # "sd" | "hd" | "4k" — ONLY in v2.3.0
    ],

    EventType.CONTENT_PAUSE: [
        "content_id",
        "position_seconds", # where user paused
    ],

    EventType.CONTENT_RESUME: [
        "content_id",
        "position_seconds", # where user resumed
    ],

    EventType.CONTENT_COMPLETE: [
        "content_id",
        "genre",
        "watch_duration_seconds",   # total time spent watching
        "content_duration_seconds", # total length of the content
    ],

    EventType.CONTENT_ABANDON: [
        "content_id",
        "genre",
        "position_seconds",         # where they stopped
        "content_duration_seconds", # total length
        "abandon_reason",           # "user_exit" | "error" | "ad_break"
    ],

    EventType.AD_IMPRESSION: [
        "campaign_id",      # references ad campaign catalog
        "advertiser_id",    # advertiser identifier
        "ad_format",        # "banner" | "pre_roll" | "mid_roll" | "interstitial"
        "placement",        # "home_feed" | "search_results" | "content_break"
    ],

    EventType.AD_CLICK: [
        "campaign_id",
        "advertiser_id",
        "landing_url",      # destination URL
        "impression_event_id",  # links back to the impression
    ],

    EventType.CONVERSION: [
        "campaign_id",
        "advertiser_id",
        "conversion_type",   # "app_install" | "purchase" | "signup"
        "conversion_value",  # dollar value
        "click_event_id",    # links back to the click
    ],

    EventType.ADD_TO_WATCHLIST: [
        "content_id",
        "genre",
    ],

    EventType.SUBSCRIPTION_CHANGE: [
        "old_tier",          # "free" | "basic" | "premium"
        "new_tier",          # "free" | "basic" | "premium"
        "change_reason",     # "upgrade" | "downgrade" | "cancel" | "reactivate"
    ],
}


# --- Device types ---
DEVICE_TYPES = ["mobile_ios", "mobile_android", "desktop", "smart_tv"]

# --- App versions ---
APP_VERSIONS = {
    "2.3.0": 0.60,   # 60% — has content_quality field
    "2.2.0": 0.40,   # 40% — content_quality absent
}

# --- Page paths for page_view events ---
PAGE_PATHS = [
    "/home",
    "/browse",
    "/browse/drama",
    "/browse/comedy",
    "/browse/action",
    "/browse/documentary",
    "/browse/thriller",
    "/browse/sci-fi",
    "/browse/romance",
    "/browse/horror",
    "/search",
    "/my-list",
    "/settings",
    "/account",
    "/notifications",
    "/continue-watching",
]

# --- Subscription tiers ---
SUBSCRIPTION_TIERS = ["free", "basic", "premium"]

# --- Genre list ---
GENRES = ["drama", "comedy", "action", "documentary", "thriller", "sci-fi", "romance", "horror"]

# --- Ad formats ---
AD_FORMATS = ["banner", "pre_roll", "mid_roll", "interstitial"]

# --- Ad placements ---
AD_PLACEMENTS = ["home_feed", "search_results", "content_break"]

# --- Search types ---
SEARCH_TYPES = ["title", "genre", "actor"]

# --- Abandon reasons ---
ABANDON_REASONS = ["user_exit", "error", "ad_break"]

# --- Conversion types ---
CONVERSION_TYPES = ["app_install", "purchase", "signup"]
