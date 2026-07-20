# Hour 12 — Pipeline Health Monitoring Report

## Query 1: Event Volume Over Time
Daily volume shows clear churning decay pattern:
- Days 1-4: 85K-161K events/day (Day 1 inflated by earlier test run)
- Days 5-14: gradual decline from 73K → 69K
- Day 15: 3,880 (late-arrival spillover from Day 14)
- Day 16: 40 (Hour 4 test events)
- **Verdict:** Volume trend matches expected churning user decay ✅

## Query 2: Schema Violation Detection
| Violation Type | Count | Description |
|---|---|---|
| null_user | 1,744 | user_id set to None by chaos injector |
| null_event_type | 1,697 | event_type deleted by chaos injector |
| bad_timestamp | 1,711 | event_timestamp set to "not-a-timestamp" |
| **Total violations** | **5,152** | |
| **Violation rate** | **0.45%** | Target: 0.5% ✅ |

Three corruption types are roughly equal (~1,700 each), confirming the chaos
injector's random.choice() distributes evenly across the three modes.

## Query 3: Event Type Distribution
| Event Type | Count | % of Total |
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
| _(malformed)_ | 1,697 | 0.1% |
| conversion | 647 | 0.1% |

**Key ratios verified:**
- content_complete + content_abandon (301,226) ≈ content_play (301,145) ✅
- content_pause (124,133) ≈ content_resume (124,228) ✅
- Ad funnel narrows: 51,653 → 12,393 → 647 ✅

## Query 4: App Version Split
| Version | Count | % |
|---|---|---|
| v2.3.0 | 862,552 | 75.4% |
| v2.2.0 | 281,718 | 24.6% |

Higher v2.3.0 share than the configured 60/40 split because power users
(all v2.3.0) generate disproportionate event volume.

## Query 5: Duplicate Detection
| Metric | Value |
|---|---|
| Duplicate event IDs | 22,380 |
| Total duplicate rows | 44,760 |
| Total events | 1,144,270 |
| **Duplicate rate** | **1.96%** |
| Target rate | 2.0% ✅ |

Each duplicated event_id appears exactly twice (44,760 / 22,380 = 2.0),
confirming chaos injector creates exact copies.

## Query 6: Top 20 Most Active Users
| Rank | User ID | Events | Sessions | Event Types |
|---|---|---|---|---|
| 1 | _(null user_id)_ | 1,744 | 1,726 | 11 |
| 2 | user_00259 | 1,029 | 83 | 11 |
| 3 | user_00151 | 1,004 | 77 | 11 |
| 4 | user_00126 | 971 | 79 | 10 |
| 5 | user_00167 | 959 | 75 | 11 |
| 6 | user_00315 | 958 | 70 | 11 |
| 7 | user_00470 | 950 | 74 | 10 |
| 8 | user_00154 | 948 | 76 | 11 |
| 9 | user_00077 | 930 | 77 | 11 |
| 10 | user_00265 | 922 | 71 | 11 |
| 11 | user_00292 | 921 | 74 | 11 |
| 12 | user_00333 | 915 | 71 | 11 |
| 13 | user_00180 | 903 | 73 | 11 |
| 14 | user_00363 | 899 | 76 | 11 |
| 15 | user_00343 | 898 | 76 | 11 |
| 16 | user_00321 | 892 | 74 | 11 |
| 17 | user_00217 | 888 | 71 | 11 |
| 18 | user_00475 | 885 | 69 | 11 |
| 19 | user_00230 | 882 | 75 | 10 |
| 20 | user_00068 | 882 | 68 | 11 |

**Observations:**
- Rank 1 is aggregated malformed events (null user_id), not a real user
- All real top users are in user_00000–user_00499 range = power archetype ✅
- Power users average ~70-83 sessions over 14 days (5-6 sessions/day) ✅
- All generate 10-11 distinct event types (out of 12) = realistic variety ✅
- Top real user (user_00259: 1,029 events) vs median user (95 events) = 10.8x skew ✅

## Production Query Status
| Query | Simulated Data | Production (ago) |
|---|---|---|
| Event volume | ✅ Works with full range | ⚠️ No results (no live data) |
| Schema violations | ✅ | ✅ (no time filter needed) |
| Event type distribution | ✅ | ✅ (no time filter needed) |
| App version split | ✅ | ✅ (no time filter needed) |
| Duplicate detection | ✅ | ✅ (no time filter needed) |
| Top 20 users | ✅ | ✅ (no time filter needed) |
