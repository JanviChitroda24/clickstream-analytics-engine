# Author: Janvi Chitroda
# Copyright (c) 2026 Janvi Chitroda. All rights reserved.
# Project: ClickStream Analytics Engine — Portfolio
# Unauthorized copying or redistribution of this content is prohibited.

"""
Content Catalog
---------------
Generates 200 shows/movies with realistic metadata.
Used by the session generator to populate content engagement events.

Content types:
  - Sitcom episodes: ~22 min
  - Drama episodes: ~45 min
  - Movies: 90-150 min
  - Documentaries: 45-120 min
  - Specials: 30-60 min

Each content item has:
  content_id, title, genre, duration_seconds, content_tier, release_year
"""

import random
from typing import Dict, List
from simulator.schemas import GENRES


# --- Title templates per genre ---
TITLE_TEMPLATES = {
    "drama": [
        "The Last {noun}", "Breaking {noun}", "{noun} Heights",
        "Dark {noun}", "The {noun} Diaries", "Beyond {noun}",
        "Shadows of {noun}", "The {noun} Legacy", "{noun} Falls",
        "Under the {noun}",
    ],
    "comedy": [
        "The {noun} Show", "Laugh {noun}", "{noun} & Friends",
        "Funny {noun}", "The {noun} Report", "Almost {noun}",
        "{noun} Gone Wrong", "The Art of {noun}", "Not Your {noun}",
        "Seriously {noun}",
    ],
    "action": [
        "{noun} Force", "Operation {noun}", "Code {noun}",
        "Strike {noun}", "The {noun} Protocol", "{noun} Rising",
        "Last {noun} Standing", "{noun} Zone", "Red {noun}",
        "Thunder {noun}",
    ],
    "documentary": [
        "Inside {noun}", "The {noun} Story", "Planet {noun}",
        "Exploring {noun}", "{noun} Revealed", "The Truth About {noun}",
        "Making of {noun}", "Wild {noun}", "{noun} Uncovered",
        "Chasing {noun}",
    ],
    "thriller": [
        "The {noun} Conspiracy", "Silent {noun}", "{noun} Games",
        "The {noun} Files", "Double {noun}", "Cold {noun}",
        "Trapped in {noun}", "{noun} at Midnight", "The {noun} Witness",
        "Edge of {noun}",
    ],
    "sci-fi": [
        "{noun} Station", "Beyond {noun}", "The {noun} Paradox",
        "Nova {noun}", "{noun} Horizon", "Quantum {noun}",
        "The {noun} Experiment", "Star {noun}", "{noun} Colony",
        "Neon {noun}",
    ],
    "romance": [
        "Love in {noun}", "The {noun} Letter", "{noun} Hearts",
        "When {noun} Met", "A {noun} Romance", "Forever {noun}",
        "The {noun} Promise", "Summer of {noun}", "Dear {noun}",
        "Falling for {noun}",
    ],
    "horror": [
        "The {noun} Haunting", "Night of {noun}", "{noun} House",
        "Curse of {noun}", "The {noun} Doll", "Whispers of {noun}",
        "{noun} Creek", "The {noun} Descent", "Blood {noun}",
        "Don't Open {noun}",
    ],
}

TITLE_NOUNS = [
    "River", "Crown", "Steel", "Glass", "Stone", "Shadow",
    "Silver", "Gold", "Iron", "Crystal", "Ember", "Storm",
    "Frost", "Dawn", "Dusk", "Ocean", "Mountain", "Valley",
    "Forest", "Desert", "City", "Bridge", "Tower", "Gate",
    "Mirror", "Echo", "Flame", "Wind", "Thunder", "Rain",
]


def _generate_title(genre: str) -> str:
    """Generate a plausible title for a given genre."""
    templates = TITLE_TEMPLATES.get(genre, TITLE_TEMPLATES["drama"])
    template = random.choice(templates)
    noun = random.choice(TITLE_NOUNS)
    return template.format(noun=noun)


def _generate_duration(genre: str) -> int:
    """Generate realistic duration in seconds based on genre/type."""
    content_type = random.choices(
        ["sitcom", "drama_episode", "movie", "documentary", "special"],
        weights={
            "drama": [0.0, 0.5, 0.3, 0.0, 0.2],
            "comedy": [0.5, 0.1, 0.2, 0.0, 0.2],
            "action": [0.0, 0.3, 0.6, 0.0, 0.1],
            "documentary": [0.0, 0.0, 0.2, 0.7, 0.1],
            "thriller": [0.0, 0.4, 0.5, 0.0, 0.1],
            "sci-fi": [0.0, 0.4, 0.5, 0.0, 0.1],
            "romance": [0.1, 0.2, 0.5, 0.0, 0.2],
            "horror": [0.0, 0.3, 0.6, 0.0, 0.1],
        }.get(genre, [0.2, 0.2, 0.2, 0.2, 0.2]),
        k=1,
    )[0]

    durations = {
        "sitcom": random.randint(18 * 60, 25 * 60),         # 18-25 min
        "drama_episode": random.randint(40 * 60, 55 * 60),  # 40-55 min
        "movie": random.randint(85 * 60, 160 * 60),         # 85-160 min
        "documentary": random.randint(45 * 60, 120 * 60),   # 45-120 min
        "special": random.randint(25 * 60, 65 * 60),        # 25-65 min
    }
    return durations[content_type]


def create_content_catalog(size: int = 200) -> List[Dict]:
    """
    Generate a content catalog with realistic titles, genres, durations.

    Returns list of dicts:
        content_id, title, genre, duration_seconds, content_tier, release_year
    """
    catalog = []

    for i in range(size):
        genre = random.choice(GENRES)
        tier = random.choices(
            ["free", "basic", "premium"],
            weights=[0.30, 0.45, 0.25],  # 30% free, 45% basic, 25% premium
            k=1,
        )[0]

        catalog.append({
            "content_id": f"content_{i:03d}",
            "title": _generate_title(genre),
            "genre": genre,
            "duration_seconds": _generate_duration(genre),
            "content_tier": tier,
            "release_year": random.randint(2018, 2026),
        })

    return catalog
