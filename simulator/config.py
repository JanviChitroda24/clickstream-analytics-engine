# Author: Janvi Chitroda
# Copyright (c) 2026 Janvi Chitroda. All rights reserved.
# Project: ClickStream Analytics Engine — Portfolio
# Unauthorized copying or redistribution of this content is prohibited.

"""
Simulator Configuration
-----------------------
Connection strings loaded from environment variables.
Copy .env.example to .env and fill in your Eventstream values.

NEVER commit .env — it's in .gitignore.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# --- Eventstream Connection ---
EVENTSTREAM_CONNECTION_STR = os.getenv("EVENTSTREAM_CONNECTION_STR", "")
EVENTSTREAM_NAME = os.getenv("EVENTSTREAM_NAME", "")

# --- Simulator Parameters ---
TOTAL_USERS = 10_000

USER_DISTRIBUTION = {
    "power": 500,       # 5%  — 20+ sessions/week
    "regular": 5500,    # 55% — 3-7 sessions/week
    "casual": 2500,     # 25% — 1-2 sessions/week
    "churning": 1000,   # 10% — declining over 2 weeks
    "new": 500,         # 5%  — heavy exploration first 3 days
}

APP_VERSION_SPLIT = {
    "2.3.0": 0.60,     # 60% — includes content_quality field
    "2.2.0": 0.40,     # 40% — content_quality absent
}

# --- Chaos Injection ---
CHAOS_CONFIG = {
    "duplicate_rate": 0.02,         # 2% of events duplicated
    "late_arrival_rate": 0.05,      # 5% of events delayed 1-4 hours
    "late_arrival_max_hours": 4,
    "malformed_rate": 0.005,        # 0.5% malformed payloads
}

# --- Eventstream Batching ---
BATCH_SIZE = 500                    # events per batch send
SIMULATION_SPEED = 1.0              # 1.0 = real-time, 10.0 = 10x faster

# --- Content & Campaigns ---
CONTENT_CATALOG_SIZE = 200          # number of shows/movies
AD_CAMPAIGN_COUNT = 20              # number of ad campaigns

# --- Sessionization ---
SESSION_TIMEOUT_MINUTES = 30        # default inactivity timeout
