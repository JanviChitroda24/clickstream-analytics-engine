# Sessionization Sensitivity Trade-off (Hour 20)

> Fill in the **Actual Results** section after running Fabric notebook
> `04_sessionization_sensitivity` on `silver_deduped_events` (~1,116,783 rows).

## Question

Given a **flat** gap-based sessionization algorithm (Hour 19 Layer 1A), how much do
session metrics change when the inactivity timeout is **15 / 30 / 60 minutes**?

This is intentionally **not** activity-aware. Hour 19 already compared flat vs
context-aware. Hour 20 isolates a single parameter: timeout magnitude.

## Method

| Item | Value |
|------|--------|
| Input | `silver_deduped_events` |
| Algorithm | `lag` → gap (`to_timestamp` + `unix_timestamp`) → threshold → running sum |
| Thresholds | 900s / 1800s / 3600s |
| Metrics | sessions, sess/user, avg/median/P25/P75 duration, avg events, bounce % |
| Baseline | 30-minute (industry default) |

## Actual Results

_Paste from Cell 4 after Fabric run:_

| timeout_minutes | total_sessions | sessions_per_user | avg_duration_sec | median_duration_sec | avg_events_per_session | bounce_rate_pct |
|-----------------|---------------:|------------------:|-----------------:|--------------------:|-----------------------:|----------------:|
| 15 | | | | | | |
| 30 | | | | | | |
| 60 | | | | | | |

### % change vs 30-min baseline

| timeout | sessions | duration | events | bounce |
|---------|----------|----------|--------|--------|
| 15-min | | | | |
| 30-min | 0% | 0% | 0% | (baseline) |
| 60-min | | | | |

## Expected Monotonic Pattern

| Metric | 15-min | 30-min | 60-min |
|--------|-------|-------|-------|
| Sessions | highest | middle | lowest |
| Duration | lowest | middle | highest |
| Events/session | lowest | middle | highest |
| Bounce rate | highest | middle | lowest |
| Sessions/user | highest | middle | lowest |

If any metric is non-monotonic, investigate gap distribution (many gaps in that band).

## Trade-off Summary

### 15-minute — aggressive
- **Pros:** micro-sessions, granular visit tracking
- **Cons:** splits content watches; inflates bounce and sessions/user

### 30-minute — recommended base
- **Pros:** GA/Adobe-comparable; balanced for nav + viewing
- **Cons:** still can split sparse long watches → mitigated by H19 activity-aware (60m content extension)

### 60-minute — loose
- **Pros:** long-form friendly as a blanket rule
- **Cons:** merges separate visits; understates frequency; not industry-comparable

## Decision (integrates Hour 19)

| Use case | Definition |
|----------|------------|
| Platform visit / DAU / retention / star schema | **30-min base**, prefer **activity-aware** (`silver_sessions_aware`) for streaming realism |
| Binge / content stickiness | **Viewing streaks** (H19 Layer 2), not flat 60-min |
| Sensitivity documentation | This file + `sessionization_sensitivity` Delta table |

**Interview line:**  
"I evaluated 15/30/60 flat timeouts on 1.1M events. [fill % deltas]. I kept 30 minutes as the industry-comparable base and used activity-aware extension + viewing streaks for long-form content instead of a blanket 60-minute timeout."

## Artifacts

- Notebook: `notebooks/04_sessionization_sensitivity.py`
- Delta table: `sessionization_sensitivity`
- Related: `notebooks/03_sessionization.py` (dual-layer)
