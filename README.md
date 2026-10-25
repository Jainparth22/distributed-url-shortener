# 🔗 URL Shortener

A high-throughput URL shortening service built with production-grade distributed systems patterns. Converts long URLs into compact 7-character Base62 codes with sub-millisecond redirects via in-memory caching.

**Live:** [http://localhost:8001](http://localhost:8001)  
**Stack:** FastAPI · Python · Vanilla JS

---

## 📦 Where Data Lives

This service features **hybrid persistent + cached storage**:

| Store | What it holds | Implementation |
|-------|--------------|----------------|
| `urls` table in `urls.db` | Persistent URL store, click counts, timestamps, user ownership | SQLite (`aiosqlite`) — durable across restarts |
| `LRUCache` | Read-path accelerator (500 items, TTL-based eviction) | In-Memory `OrderedDict` (swap to Redis in prod) |
| `SingleFlight` | In-flight request deduplication (prevents cache stampedes) | Python `asyncio.Event` synchronization |
| `AnalyticsQueue` | Asynchronous non-blocking click ingestion | Async queue + background worker coroutine |
| `TokenBucket` | Per-IP write rate limiting (5 burst, 0.5/sec refill) | In-memory token bucket |

> 💡 **Production Upgrade Path:** To move to PostgreSQL, change connection string to `postgresql+asyncpg://...` in `main.py`. To swap LRU cache to distributed Redis, set `REDIS_URL`.

---

## 🚀 How to Run

```bash
# From the HLD/ root directory
cd 01_url_shortener
python -m uvicorn main:app --port 8001 --host 0.0.0.0 --reload

# Or use the master launcher from HLD/
python start_all.py
```

Open [http://localhost:8001](http://localhost:8001) in your browser.

---

## 🛠️ API Endpoints

| Method | Endpoint | Description | Auth Required |
|--------|----------|-------------|---------------|
| `POST` | `/api/auth/token` | Issue JWT bearer token for user | No |
| `POST` | `/api/shorten` | Create short URL (rate limited) | **Yes (Bearer JWT)** |
| `GET`  | `/r/{code}` | Public redirect — cache-aside + Single-Flight | No |
| `GET`  | `/api/analytics/{code}` | Click stats: total clicks, breakdown | No |
| `GET`  | `/api/cache/stats` | LRU cache hit rate, stampede dedup count | No |
| `GET`  | `/api/rate-limit/status` | Token bucket state for caller's IP | No |
| `GET`  | `/api/urls` | List all shortened URLs from SQLite | No |
| `GET`  | `/api/health` | Health check (reports DB status) | No |

### Example — Shorten a URL with JWT

```bash
# 1. Get JWT Token
TOKEN=$(curl -s -X POST http://localhost:8001/api/auth/token \
  -H "Content-Type: application/json" \
  -d '{"user_id": "parth"}' | jq -r .access_token)

# 2. Shorten URL using Bearer Token
curl -X POST http://localhost:8001/api/shorten \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"url": "https://github.com/tiangolo/fastapi", "ttl_hours": 24}'
```

```json
{
  "short_code": "aB3xY9k",
  "short_url": "http://localhost:8001/r/aB3xY9k",
  "original_url": "https://github.com/tiangolo/fastapi",
  "expires_at": "2025-10-26T01:30:00",
  "idempotent": false,
  "tokens_remaining": 4.0
}
```

### Example — Redirect

```bash
curl http://localhost:8001/r/aB3xY9k
```

```json
{
  "redirect_to": "https://github.com/tiangolo/fastapi",
  "cache_hit": true,
  "single_flight_deduped": 0
}
```

---

## ⚙️ Concepts Implemented

### 1. Base62 Encoding
- Charset: `[0-9a-zA-Z]` (62 chars, URL-safe — no `+`, `/`, `=`)
- 7 characters → `62⁷ ≈ 3.5 trillion` unique codes
- Generated from MD5 hash of `(url + attempt + nanosecond timestamp)`

```python
CHARSET = string.ascii_letters + string.digits  # 62 chars

def base62_encode(n: int) -> str:
    chars = []
    while n:
        chars.append(CHARSET[n % 62])
        n //= 62
    return "".join(reversed(chars))
```

### 2. LRU Cache with TTL (Cache-Aside Pattern)
- Implemented with `collections.OrderedDict` — O(1) get + O(1) promote-to-front
- Max 500 entries; TTL checked on every `get()` call
- **Why cache?** Redirect reads are 100x more frequent than writes. Serving from cache avoids DB hit entirely.

### 3. Single-Flight (Thundering Herd Prevention)
- When a popular key expires simultaneously for 1,000 requests: only **one** coroutine fetches from storage; the rest `await` the same future
- Prevents cache stampede that would overwhelm DB under load

### 4. Token Bucket Rate Limiter
- Per-IP rate limiting on `/api/shorten`: 5 tokens/bucket, refills at 0.5 tokens/sec
- Returns `HTTP 429` with `retry_after` seconds when bucket empty

### 5. Async Analytics Queue
- Click events are pushed to a non-blocking in-memory queue
- Background coroutine drains the queue every 50ms into analytics store
- Redirect latency is **not** blocked by analytics writes

### 6. Collision Detection
- On hash collision, probe up to 5 times with incrementing `attempt` salt
- Idempotent: same URL always returns same short code (checked before generating new one)

---

## 💡 Interview Talking Points

- **"How do you scale reads?"** → Cache-aside. 99% of traffic is redirects, served from LRU cache in microseconds.
- **"Why Base62 not UUID?"** → UUID is 36 chars; Base62 is 7. Both collision-resistant at scale.
- **"What happens when cache is full?"** → LRU evicts least-recently-used entries. TTL prevents serving expired links.
- **"How do you prevent DB overload on cache miss?"** → Single-Flight deduplicates concurrent misses to one DB query.
- **"How do you handle analytics without slowing redirects?"** → Async queue decouples tracking from the hot redirect path.
