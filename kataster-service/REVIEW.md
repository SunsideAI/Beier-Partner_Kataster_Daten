# Code Review: Kataster-Lookup-Service

Reviewed against production-readiness criteria for Railway deployment + Pipedrive integration.

---

## Critical

### 1. Port hardcoded to `sys.argv[1]`, not `$PORT`
**File:** `main.py:200`

Railway injects the port as `$PORT` environment variable. The current `__main__` block reads `sys.argv[1]`, which is never set when Railway starts the process via Procfile. The service will always fall back to 8000, regardless of what Railway assigns — or crash if the port is unavailable.

**Fix:** `port = int(os.environ.get("PORT", 8000))`

### 2. No API-key authentication
**File:** `main.py` (all endpoints)

The service is open to any caller on the internet. Once deployed on Railway with a public URL, anyone can hit `/kataster` and consume Nominatim quota + WFS quota.

**Fix:** `X-API-Key` header check via FastAPI `Security` dependency on all non-health endpoints.

---

## Medium

### 3. `print()` used for all logging
**Files:** `main.py`, `geocoder.py`, all `wfs_clients/*.py`

Railway routes stdout to its log aggregator, but `print()` output has no log level, no timestamp, and cannot be filtered. Structured logging (`logging.basicConfig`) makes Railway logs queryable.

**Fix:** Replace all `print(...)` with `logger.info(...)` / `logger.warning(...)`.

### 4. `_last_request_time` is not thread-safe
**File:** `geocoder.py:13`

The global float used for Nominatim rate-limiting is read and written without a lock. Under a multi-worker setup, two concurrent requests could both pass the rate check simultaneously, causing a 429 from Nominatim. 

**Acceptable for now:** Railway single-worker deployment serialises requests through the event loop. The race window is narrow; Nominatim returns `None` gracefully on 429. Add a `# TODO: threading.Lock if multi-worker` comment.

### 5. No retry logic on WFS calls
**Files:** All `wfs_clients/*.py`

German Landesvermessungsamt WFS endpoints are occasionally slow or return transient errors. There is no retry logic; a single failure returns a 404 to the caller.

**Acceptable for now:** The existing 30s timeouts are sensible. Retry logic adds complexity; MVP behaviour (caller retries) is fine for now.

---

## Low

### 6. Duplicate `return result` dead code
**File:** `wfs_clients/schleswig_holstein.py:170`

Line 170 is an unreachable second `return result` statement inside `_lookup_gemarkung_names()`. The function already returns at line 168. Harmless, but confusing.

**Fix:** Delete line 170.

### 7. `bundesland` not at response top-level
**File:** `main.py:168`

`bundesland` is only available inside each `kataster_ergebnisse[i].bundesland`. For Make.com / Pipedrive mapping, a top-level `bundesland` field saves one level of nesting.

**Fix:** Add `"bundesland": geo.bundesland` to the top-level response dict.

### 8. No `weitere_flurstuecke_text` for multi-parcel results
**File:** `main.py`

When `anzahl_flurstuecke > 1`, the additional parcels are in `kataster_ergebnisse[1:]` as structured objects. Pipedrive's text fields cannot hold arrays. Make.com would have to join them manually.

**Fix:** Add `"weitere_flurstuecke_text"` — semicolon-separated human-readable string of additional parcels — so Pipedrive can store it directly in a text field.

### 9. Redundant `kataster` / `kataster_ergebnisse` fields
**File:** `main.py:170-171`

Both `kataster` (single object, first result) and `kataster_ergebnisse` (full list) are returned. This is intentional backward-compatibility. Keep `kataster` for existing Make.com scenarios; prefer `kataster_ergebnisse[0]` in new integrations.

### 10. Missing deployment files
No `Procfile`, `.env.example`, `.gitignore`. Railway deployment requires at minimum a `Procfile`.

### 11. `python-dotenv` missing from `requirements.txt`
`load_dotenv()` is useful for local development (reads `.env`). Not in `requirements.txt`.

### 12. Nominatim `User-Agent` hardcoded
**File:** `geocoder.py:16`

`"KatasterLookup/1.0 (Sachverstaendigenbuero Beier & Partner)"` is hard-coded. Acceptable for single-tenant use. Fine as-is.

### 13. Emoji in `print()` statements
**File:** `main.py:201-203`

Emoji in log output can cause encoding issues in some terminal environments (Windows cmd). Irrelevant once migrated to `logging`, but worth noting.

---

## What Is Already Good

- **All WFS clients have explicit timeouts** (30s on main calls, 15s on zoning lookups) — correct.
- **Nominatim rate-limiting** at 1 req/s is correctly implemented.
- **`filter_by_address`** logic handles Hausnummer + Zusatz matching robustly.
- **Error responses are structured dicts**, not bare strings — easy to parse in Make.com/downstream.
- **`/health` endpoint** exists and is trivial — correct for Railway health probe.
- **`FlurstueckInfo.to_dict()`** produces a flat, consistent structure across all Bundesländer.
- **WFS client interface** (`query_flurstuecke` + `query_gebaeude`) is clean and extensible.
