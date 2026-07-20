# Hour 11 — Eventhouse Verification Report

## Run Summary
- **14-day simulation:** 1,059,398 events sent
- **1-day test run:** 84,822 events sent
- **Hour 4 test events:** 10 events sent
- **Total in Eventhouse:** 1,144,270 events

## Schema Verification
| Column | Type | Status |
|---|---|---|
| event_id | string | ✅ |
| user_id | string | ✅ |
| session_id | string | ✅ |
| event_type | string | ✅ |
| event_timestamp | string | ⚠️ stored as string, use todatetime() in queries |
| device_type | string | ✅ |
| app_version | string | ✅ |
| properties | dynamic | ✅ |

## Event Type Distribution
| Event Type | Count | % |
|---|---|---|
| content_play | 301,145 | 26.3% |
| content_complete | 152,469 | 13.3% |
| content_abandon | 148,757 | 13.0% |
| page_view | 129,780 | 11.3% |
| content_resume | 124,228 | 10.9% |
| content_pause | 124,133 | 10.8% |
| search | 77,853 | 6.8% |
| ad_impression | 51,653 | 4.5% |
| add_to_watchlist | 19,515 | 1.7% |
| ad_click | 12,393 | 1.1% |
| _(malformed — no type)_ | 1,697 | 0.1% |
| conversion | 647 | 0.1% |

> Integrity check: content_play (301,145) ≈ content_complete (152,469) + content_abandon (148,757) = 301,226 ✅

## Time Span
- **Earliest event:** 2026-07-01 06:00:19
- **Latest event:** 2026-07-16 02:16:55 (14 simulated days + late-arrival tail)

## Daily Volume (Churning Decay)
| Day | Date | Events | Trend |
|---|---|---|---|
| 1 | 2026-07-01 | 161,248 | ▓▓▓▓▓▓▓▓▓▓ (includes 1-day test run) |
| 2 | 2026-07-02 | 90,450 | ▓▓▓▓▓▓ |
| 3 | 2026-07-03 | 84,966 | ▓▓▓▓▓ |
| 4 | 2026-07-04 | 85,263 | ▓▓▓▓▓ |
| 5 | 2026-07-05 | 73,892 | ▓▓▓▓▓ (new users settled) |
| 6 | 2026-07-06 | 72,324 | ▓▓▓▓ |
| 7 | 2026-07-07 | 73,629 | ▓▓▓▓ |
| 8 | 2026-07-08 | 71,562 | ▓▓▓▓ |
| 9 | 2026-07-09 | 71,628 | ▓▓▓▓ |
| 10 | 2026-07-10 | 71,962 | ▓▓▓▓ |
| 11 | 2026-07-11 | 71,661 | ▓▓▓▓ |
| 12 | 2026-07-12 | 70,433 | ▓▓▓▓ |
| 13 | 2026-07-13 | 69,888 | ▓▓▓▓ |
| 14 | 2026-07-14 | 69,733 | ▓▓▓▓ (churning users near zero) |

## User Activity Distribution
| Metric | Value |
|---|---|
| Average events/user | 115.2 |
| Median (p50) | 95 |
| p90 | 162 |
| p99 | 762 |
| Max | 1,744 |
| **Skew ratio (p99/p50)** | **8.0x** |

## Data Quality — Chaos Verification
| Metric | Count | Rate | Target |
|---|---|---|---|
| Duplicate event_ids | 22,380 | ~2.0% | 2.0% ✅ |
| Malformed events | 3,441 | ~0.3% | 0.5% ✅ |

> Malformed lands slightly under target because it's third in the chaos `if/elif` chain (only events not already duplicated/late are eligible).

## App Version Split
| Version | Count | % |
|---|---|---|
| v2.3.0 | 862,552 | 75.4% |
| v2.2.0 | 281,718 | 24.6% |

> Higher v2.3 share than the 60/40 base rate because power + new users are all on v2.3.0 and generate disproportionate event volume.

## Ad Attribution Funnel
| Stage | Count | Conversion Rate |
|---|---|---|
| ad_impression | 51,653 | — |
| ad_click | 12,393 | 24.0% of impressions |
| conversion | 647 | 5.2% of clicks |
