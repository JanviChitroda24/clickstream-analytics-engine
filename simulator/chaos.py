# Author: Janvi Chitroda
# Copyright (c) 2026 Janvi Chitroda. All rights reserved.
# Project: ClickStream Analytics Engine — Portfolio
# Unauthorized copying or redistribution of this content is prohibited.

"""
Chaos Injection Layer
---------------------
Takes clean events from the session generator and introduces
real-world data quality problems:

  1. Duplicates (2%)      — same event_id sent twice (client retry)
  2. Late arrivals (5%)   — event arrives hours after its timestamp
  3. Malformed (0.5%)     — null fields, bad timestamps, missing types
  4. Schema variation      — v2.2 events missing v2.3-only fields

These corrupted events test the downstream pipeline's ability to
handle dirty data — dedup, validation, dead-letter quarantine.

Usage:
    chaos = ChaosInjector()
    on_time_events, late_events = chaos.inject(clean_events)
    # Send on_time_events now
    # Send late_events 1-4 hours later
"""

import copy
import random
from datetime import timedelta
from typing import Dict, List, Tuple

from simulator.config import CHAOS_CONFIG


class ChaosInjector:
    """
    Injects controlled chaos into event streams.

    Each event gets at most ONE type of corruption (if/elif chain).
    Rates are configurable via config dict.
    """

    def __init__(self, config: Dict = None):
        self.config = config or CHAOS_CONFIG
        # Track stats for verification
        self.stats = {
            "total_input": 0,
            "duplicates_injected": 0,
            "late_arrivals_held": 0,
            "malformed_injected": 0,
            "clean_passed": 0,
        }

    def reset_stats(self):
        """Reset counters between runs."""
        for key in self.stats:
            self.stats[key] = 0

    def inject(self, events: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """
        Apply chaos to a list of events.

        Args:
            events: Clean events from the session generator.

        Returns:
            (on_time_events, late_buffer):
                on_time_events — send immediately (includes duplicates + malformed)
                late_buffer — send 1-4 hours later
        """
        output = []
        late_buffer = []

        for event in events:
            self.stats["total_input"] += 1

            # --- Duplicate injection (2%) ---
            if random.random() < self.config["duplicate_rate"]:
                output.append(event)                    # original
                output.append(copy.deepcopy(event))     # exact duplicate (same event_id)
                self.stats["duplicates_injected"] += 1

            # --- Late arrival (5%) ---
            elif random.random() < self.config["late_arrival_rate"]:
                delayed = copy.deepcopy(event)
                # event_timestamp stays original — it records WHEN it happened
                # but it will ARRIVE late — main.py handles the delayed send
                late_buffer.append(delayed)
                self.stats["late_arrivals_held"] += 1

            # --- Malformed payload (0.5%) ---
            elif random.random() < self.config["malformed_rate"]:
                corrupted = copy.deepcopy(event)
                corruption_type = random.choice([
                    "null_user",
                    "bad_timestamp",
                    "missing_type",
                ])

                if corruption_type == "null_user":
                    corrupted["user_id"] = None
                elif corruption_type == "bad_timestamp":
                    corrupted["event_timestamp"] = "not-a-timestamp"
                elif corruption_type == "missing_type":
                    del corrupted["event_type"]

                # Tag it so we can verify in tests
                corrupted["_chaos_type"] = corruption_type
                output.append(corrupted)
                self.stats["malformed_injected"] += 1

            # --- Clean pass-through ---
            else:
                output.append(event)
                self.stats["clean_passed"] += 1

        return output, late_buffer

    def apply_schema_variation(self, event: Dict, user_profile) -> Dict:
        """
        Remove v2.3-only fields for v2.2 users.

        Called by main.py BEFORE inject() — schema variation is
        applied to all events, not probabilistically.
        """
        if user_profile.app_version == "2.2.0":
            props = event.get("properties", {})
            if "content_quality" in props:
                del props["content_quality"]
        return event

    def get_stats_summary(self) -> str:
        """Human-readable summary of chaos injection results."""
        total = self.stats["total_input"]
        if total == 0:
            return "No events processed."

        return (
            f"Chaos injection summary ({total} events):\n"
            f"  Duplicates:    {self.stats['duplicates_injected']:5d} "
            f"({self.stats['duplicates_injected']/total*100:.1f}% — target {self.config['duplicate_rate']*100:.1f}%)\n"
            f"  Late arrivals: {self.stats['late_arrivals_held']:5d} "
            f"({self.stats['late_arrivals_held']/total*100:.1f}% — target {self.config['late_arrival_rate']*100:.1f}%)\n"
            f"  Malformed:     {self.stats['malformed_injected']:5d} "
            f"({self.stats['malformed_injected']/total*100:.1f}% — target {self.config['malformed_rate']*100:.1f}%)\n"
            f"  Clean:         {self.stats['clean_passed']:5d} "
            f"({self.stats['clean_passed']/total*100:.1f}%)"
        )
