<!--
Author: Janvi Chitroda
Copyright (c) 2026 Janvi Chitroda. All rights reserved.
Project: ClickStream Analytics Engine — Portfolio
Unauthorized copying or redistribution of this content is prohibited.
-->

# Fix: Lakehouse `properties` Schema Lock

**Status:** Code ready — probe with `tests.test_properties_fix` before full simulator  
**Chosen approach:** Option 1 (fresh dest + re-ingest) **+** serialize `properties` as JSON **string**

## What Was Wrong Before

```
Hour 4: 10 test events with properties = {"test_index": 0}  (nested object)
        ↓
Eventstream Lakehouse auto-detected STRUCT<test_index: BIGINT>
        ↓
Hour 10: Simulator sent nested objects with content_id, genre, …
        ↓
Forced into wrong STRUCT → properties became {NULL}
```

Nested objects + schema-on-write = STRUCT locked from the first batch. Different event types have different keys → later events lose fields.

## Refined Fix (code)

```python
# simulator/session_generator.py — _make_event
"properties": json.dumps(properties)   # STRING, not nested dict
```

Lakehouse then maps `properties` → **STRING**. Spark reads with `get_json_object` / `from_json`.

Also: `chaos.apply_schema_variation` accepts string or dict; `tests/test_properties_fix.py` sends 10 events with 4 property shapes before a full 14-day run.

## Execution Order

1. Confirm `_make_event` uses `json.dumps` ✅
2. Delete Lakehouse `raw_events` (+ Silver/staging)
3. Delete Eventstream `lakehouse_raw` → recreate → Publish
4. **Probe (not full sim):**
   ```bash
   python -m tests.test_properties_fix
   ```
5. Wait 2–3 minutes → notebook:
   ```python
   events_df = spark.sql("SELECT * FROM raw_events")
   events_df.printSchema()  # PASS: properties: string
   events_df.select("event_id", "event_type", "properties").show(10, truncate=False)
   ```
6. **PASS** (string + JSON for all types) → `python -m simulator.main --days 14`  
   **FAIL** (STRUCT / NULL) → stop; do not waste another full sim run
7. Re-run notebooks 01–05 + warehouse dim reload → Hour 23

## Why Option 1 + STRING (not Eventhouse patch)

| Option | Verdict |
|--------|---------|
| Recreate dest + re-ingest | Chosen — fix source |
| `json.dumps` → STRING column | Chosen — avoids STRUCT inference |
| Eventhouse join patch | Rejected — couples cold→hot |
| Fresh table alone (nested dicts) | Insufficient — STRUCT can still lock wrong |

## Interview line

> "Lakehouse schema-on-write locked nested `properties` from an early smoke test. I serialized properties as a JSON string so the column stays STRING across event types, probed with 10 varied events, then re-ingested — instead of permanently joining Eventhouse into the cold path."
