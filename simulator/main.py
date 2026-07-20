"""
Clickstream Simulator — Main Orchestrator
------------------------------------------
Generates clickstream events for 10,000 users across 14 simulated days,
applies chaos injection, and sends events to Fabric Eventstream.

Usage:
    # Full 14-day simulation (sends to Eventstream)
    python -m simulator.main

    # Quick test: 1 day, print to console (no Eventstream)
    python -m simulator.main --dry-run --days 1

    # Custom day count
    python -m simulator.main --days 3
"""

import argparse
import json
import random
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from simulator.config import (
    BATCH_SIZE,
    EVENTSTREAM_CONNECTION_STR,
    EVENTSTREAM_NAME,
)
from simulator.user_profiles import create_user_population, UserArchetype
from simulator.session_generator import SessionGenerator
from simulator.chaos import ChaosInjector
from simulator.content_catalog import create_content_catalog
from simulator.ad_campaigns import create_ad_campaigns


def send_to_eventstream(events: List[Dict], dry_run: bool = False) -> int:
    """
    Batch-send events to Fabric Eventstream via Event Hubs SDK.

    Args:
        events: List of event dicts to send.
        dry_run: If True, skip sending (print count instead).

    Returns:
        Number of events sent.
    """
    if dry_run or not EVENTSTREAM_CONNECTION_STR:
        return len(events)

    from azure.eventhub import EventHubProducerClient, EventData

    producer = EventHubProducerClient.from_connection_string(
        EVENTSTREAM_CONNECTION_STR,
        eventhub_name=EVENTSTREAM_NAME,
    )

    sent_count = 0
    try:
        # Send in batches of BATCH_SIZE
        for i in range(0, len(events), BATCH_SIZE):
            batch_events = events[i : i + BATCH_SIZE]
            batch = producer.create_batch()
            for event in batch_events:
                batch.add(EventData(json.dumps(event, default=str)))
            producer.send_batch(batch)
            sent_count += len(batch_events)
    except Exception as e:
        print(f"  ❌ Eventstream send error: {e}")
    finally:
        producer.close()

    return sent_count


def simulate_day(
    users: List,
    catalog: List[Dict],
    campaigns: List[Dict],
    chaos: ChaosInjector,
    sim_date: datetime,
    dry_run: bool = False,
) -> Dict:
    """
    Simulate one day of clickstream activity for all users.

    For each user:
      1. Determine how many sessions they have today (from archetype + day decay)
      2. Generate events for each session
      3. Apply chaos injection
      4. Send on-time events immediately
      5. Collect late events for end-of-day flush

    Returns stats dict.
    """
    day_events_on_time = []
    day_events_late = []
    stats = {
        "total_sessions": 0,
        "total_events_generated": 0,
        "total_events_sent": 0,
        "events_by_type": Counter(),
        "events_by_archetype": Counter(),
        "users_active": 0,
    }

    for user in users:
        # Determine sessions today based on archetype + day decay
        session_rate = user.get_current_session_rate()

        # Poisson-ish: use rate as expected count, add some randomness
        if session_rate <= 0:
            continue

        # Simple approach: probability of at least one session
        # then random count based on rate
        if random.random() > session_rate and session_rate < 1.0:
            continue  # no session today for this user

        num_sessions = max(1, int(random.gauss(session_rate, session_rate * 0.3)))
        stats["users_active"] += 1

        for s in range(num_sessions):
            # Random start time within the day
            hour = random.randint(6, 23)  # sessions between 6am-11pm
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            session_start = sim_date.replace(
                hour=hour, minute=minute, second=second
            )

            # Generate session events
            gen = SessionGenerator(user, catalog, campaigns)
            session_events = gen.generate_session(session_start)

            stats["total_sessions"] += 1
            stats["total_events_generated"] += len(session_events)

            for event in session_events:
                stats["events_by_type"][event["event_type"]] += 1
                stats["events_by_archetype"][user.archetype.value] += 1

            # Apply chaos
            on_time, late = chaos.inject(session_events)
            day_events_on_time.extend(on_time)
            day_events_late.extend(late)

    # Send on-time events
    sent = send_to_eventstream(day_events_on_time, dry_run=dry_run)
    stats["total_events_sent"] += sent

    # Flush late buffer (simulates delayed arrival)
    if day_events_late:
        late_sent = send_to_eventstream(day_events_late, dry_run=dry_run)
        stats["total_events_sent"] += late_sent

    return stats


def print_day_summary(day_num: int, sim_date: datetime, stats: Dict, chaos: ChaosInjector):
    """Print a summary of one simulated day."""
    print(f"\n{'='*60}")
    print(f"DAY {day_num} — {sim_date.strftime('%Y-%m-%d')}")
    print(f"{'='*60}")
    print(f"  Active users:     {stats['users_active']:,}")
    print(f"  Total sessions:   {stats['total_sessions']:,}")
    print(f"  Events generated: {stats['total_events_generated']:,}")
    print(f"  Events sent:      {stats['total_events_sent']:,}")

    print(f"\n  Events by archetype:")
    for archetype, count in sorted(stats["events_by_archetype"].items(), key=lambda x: -x[1]):
        pct = count / stats["total_events_generated"] * 100 if stats["total_events_generated"] > 0 else 0
        print(f"    {archetype:10s}: {count:6,} ({pct:.1f}%)")

    print(f"\n  Events by type:")
    for etype, count in sorted(stats["events_by_type"].items(), key=lambda x: -x[1]):
        print(f"    {etype:25s}: {count:6,}")

    print(f"\n  {chaos.get_stats_summary()}")


def run_simulation(num_days: int = 14, dry_run: bool = False):
    """
    Run the full simulation.

    Args:
        num_days: Number of simulated days (default 14 for full churn cycle).
        dry_run: If True, don't send to Eventstream (console output only).
    """
    print("=" * 60)
    print("CLICKSTREAM SIMULATOR")
    print("=" * 60)

    # --- Initialize components ---
    print("\nInitializing...")
    users = create_user_population()
    catalog = create_content_catalog(200)
    campaigns = create_ad_campaigns(20)
    chaos = ChaosInjector()

    print(f"  Users:      {len(users):,}")
    print(f"  Content:    {len(catalog)} items")
    print(f"  Campaigns:  {len(campaigns)} campaigns")
    print(f"  Days:       {num_days}")
    print(f"  Dry run:    {dry_run}")

    if not dry_run and not EVENTSTREAM_CONNECTION_STR:
        print("\n  ⚠️  No EVENTSTREAM_CONNECTION_STR in .env — switching to dry run")
        dry_run = True

    # --- Simulation loop ---
    base_date = datetime(2026, 7, 1, 0, 0, 0, tzinfo=timezone.utc)
    total_stats = {
        "total_events": 0,
        "total_sessions": 0,
    }

    start_time = time.time()

    for day in range(num_days):
        sim_date = base_date + timedelta(days=day)

        # Advance all users' day counter (affects churning + new behavior)
        for user in users:
            user.day_number = day

        # Reset chaos stats per day
        chaos.reset_stats()

        # Simulate the day
        day_stats = simulate_day(
            users, catalog, campaigns, chaos, sim_date, dry_run=dry_run
        )

        # Print summary
        print_day_summary(day + 1, sim_date, day_stats, chaos)

        total_stats["total_events"] += day_stats["total_events_sent"]
        total_stats["total_sessions"] += day_stats["total_sessions"]

    # --- Final summary ---
    elapsed = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"SIMULATION COMPLETE")
    print(f"{'='*60}")
    print(f"  Total days:     {num_days}")
    print(f"  Total sessions: {total_stats['total_sessions']:,}")
    print(f"  Total events:   {total_stats['total_events']:,}")
    print(f"  Elapsed time:   {elapsed:.1f} seconds")
    print(f"  Throughput:     {total_stats['total_events']/elapsed:,.0f} events/sec")

    if not dry_run:
        print(f"\n  ✅ Events sent to Eventstream → Eventhouse + Lakehouse")
        print(f"  Verify in KQL: raw_events | count")
    else:
        print(f"\n  ℹ️  Dry run — no events sent to Eventstream")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clickstream Simulator")
    parser.add_argument("--days", type=int, default=14, help="Number of simulated days")
    parser.add_argument("--dry-run", action="store_true", help="Don't send to Eventstream")
    args = parser.parse_args()

    run_simulation(num_days=args.days, dry_run=args.dry_run)
