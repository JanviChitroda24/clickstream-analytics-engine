# Sessionization Sensitivity Trade-off (Hour 20)

Verified on Fabric (`04_sessionization_sensitivity`) against `silver_deduped_events`
(**1,116,783** events) — 2026-07-28.

## Question

Given a **flat** gap-based sessionization algorithm (Hour 19 Layer 1A), how much do
session metrics change when the inactivity timeout is **15 / 30 / 60 minutes**?

This is intentionally **not** activity-aware. Hour 19 already compared flat vs
context-aware. Hour 20 isolates a single parameter: timeout magnitude.

## Method

| Item | Value |
|------|--------|
| Input | `silver_deduped_events` (1,116,783) |
| Algorithm | `lag` → gap (`to_timestamp` + `unix_timestamp`) → threshold → running sum |
| Thresholds | 900s / 1800s / 3600s |
| Metrics | sessions, sess/user, avg/median/P25/P75 duration, avg events, bounce % |
| Baseline | 30-minute (industry default) |

## Actual Results

| timeout_minutes | total_sessions | sessions_per_user | avg_duration_sec | median_duration_sec | avg_events_per_session | bounce_rate_pct | single_event |
|-----------------|---------------:|------------------:|-----------------:|--------------------:|-----------------------:|----------------:|-------------:|
| 15 | 347,520 | 35.0 | 389.8 | 240.0 | 3.2 | 14.8 | 51,373 |
| 30 | 268,691 | 27.1 | 892.2 | 360.0 | 4.2 | 12.8 | 34,428 |
| 60 | 178,680 | 18.0 | 2619.9 | 1653.0 | 6.3 | 8.6 | 15,328 |

### Duration quartiles (seconds)

| timeout | P25 | median | P75 | avg |
|---------|----:|-------:|----:|----:|
| 15 | 25 | 240 | 540 | 390 |
| 30 | 30 | 360 | 1280 | 892 |
| 60 | 183 | 1653 | 3637 | 2620 |

### % change vs 30-min baseline

| timeout | sessions | duration | events | bounce |
|---------|----------|----------|--------|--------|
| 15-min | **+29.3%** | **−56.3%** | **−23.8%** | 14.8% (vs 12.8%) |
| 30-min | 0% | 0% | 0% | 12.8% (baseline) |
| 60-min | **−33.5%** | **+193.6%** | **+50.0%** | 8.6% (vs 12.8%) |

## Pattern check

All metrics moved **monotonically** as expected (15 → 30 → 60):

| Metric | 15 | 30 | 60 | OK? |
|--------|---:|---:|---:|-----|
| Sessions | 347,520 | 268,691 | 178,680 | ✅ decreasing |
| Avg duration | 6.5m | 14.9m | 43.7m | ✅ increasing |
| Events/session | 3.2 | 4.2 | 6.3 | ✅ increasing |
| Bounce | 14.8% | 12.8% | 8.6% | ✅ decreasing |
| Sess/user | 35.0 | 27.1 | 18.0 | ✅ decreasing |

**Timeout choice matters a lot on this dataset** — not a 3% tweak. Moving 15↔30↔60 swings session count by ~±30% and duration by −56% / +194%.

## Key Insights

1. **15-min → +29% sessions vs 30-min** — many gaps sit in the 15–30 min band (likely content pauses split incorrectly).
2. **60-min → −34% sessions vs 30-min** — merges separate visits within an hour.
3. **60-min avg duration (43.7 min) ≈ H19 activity-aware (40.9 min)** — aware gets similar long-form protection *without* blanket over-merge on non-content gaps.
4. **Bounce 14.8% (15) vs 8.6% (60)** — extra 15-min “bounces” are often mid-watch pauses.
5. **Avg events 3.2 (15) is barely a visit** — 4.2 (30) / 6.3 (60) are more meaningful.

## Trade-off Summary

### 15-minute — too aggressive for streaming
- +29% more sessions than 30-min; duration cut in half
- Bounce 14.8%; avg only 3.2 events — many mid-watch pauses become fake boundaries
- Inflates sessions/user (35) → misleading “high engagement”

### 30-minute — recommended base
- Industry-comparable (GA/Adobe)
- Bounce 12.8%; 4.2 events/session; median 6 min — plausible mixed browse + watch
- Still can split sparse long watches → mitigated by H19 activity-aware (60m on content)

### 60-minute — too loose as a blanket
- −33.5% sessions; duration nearly **3×** (avg 43.7 min)
- Bounce drops to 8.6% — some real quick visits absorbed into neighbors
- Flat 60 (~178,680) is close to H19 activity-aware (~184,556) — but flat 60 merges *all* 30–60 min gaps, including non-content return visits. Activity-aware only extends for content continuation → better targeted.

## Decision (integrates Hour 19)

| Use case | Definition |
|----------|------------|
| Platform visit / DAU / retention / star schema | **30-min base**; prefer **activity-aware** (`silver_sessions_aware`) for streaming |
| Binge / content stickiness | **Viewing streaks** (H19 Layer 2), not flat 60-min |
| Sensitivity documentation | This file + `sessionization_sensitivity` Delta table |

**Interview line:**  
"I evaluated 15/30/60 flat timeouts on 1.1M events. At 15 minutes, sessions rose 29% and average duration fell 56% — content pauses were splitting visits. At 60 minutes, sessions fell 34% and duration nearly tripled — separate visits were merging. I kept 30 minutes as the industry-comparable base and used activity-aware extension plus viewing streaks for long-form content instead of a blanket 60-minute timeout."

## Artifacts

- Notebook: `notebooks/04_sessionization_sensitivity.py`
- Delta table: `sessionization_sensitivity`
- Related: `notebooks/03_sessionization.py` (dual-layer)
