"""
╔══════════════════════════════════════════════════════════════╗
║  URL Shortener  |  Port 8001  — PRODUCTION-GRADE             ║
╠══════════════════════════════════════════════════════════════╣
║  Storage Layer:                                              ║
║  • SQLite (aiosqlite) — persistent URL store & analytics     ║
║    → Production: swap driver to asyncpg for PostgreSQL       ║
║  • In-Memory LRU Cache — read-path acceleration              ║
║    → Production: swap to redis-py (SETEX / GET)              ║
║  Auth:                                                       ║
║  • JWT Bearer tokens (python-jose)                           ║
║    → Production: replace /api/auth/token with OAuth2 flow    ║
║  Patterns:                                                   ║
║  • Single-Flight (cache stampede prevention)                 ║
║  • Token Bucket Rate Limiter                                 ║
║  • Async Analytics Queue (non-blocking redirect path)        ║
║  • Base62 encoding with collision detection                  ║
╚══════════════════════════════════════════════════════════════╝
"""

import asyncio
import hashlib
import os
import string
import threading
import time
from collections import OrderedDict, deque
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import aiosqlite
from fastapi import Depends, FastAPI, HTTPException, Request, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.staticfiles import StaticFiles
from jose import JWTError, jwt
from pydantic import BaseModel

app = FastAPI(title="URL Shortener", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ─────────────────────────────────────────────
#  CONFIG — all overridable via environment variables
#  In production: set these in Railway/Render/Heroku dashboard
# ─────────────────────────────────────────────
SECRET_KEY   = os.getenv("SECRET_KEY", "hld-demo-change-this-in-production")
ALGORITHM    = "HS256"
TOKEN_EXPIRE = int(os.getenv("TOKEN_EXPIRE_HOURS", "24"))
DB_PATH      = os.getenv("DB_PATH", "urls.db")
# REDIS_URL  = os.getenv("REDIS_URL", "")  # Uncomment to enable Redis cache


# ─────────────────────────────────────────────
#  BASE62 ENCODING
#  Why: URL-safe (no +/= like base64), human-readable
#  62^7 ≈ 3.5 trillion unique 7-char codes
# ─────────────────────────────────────────────
CHARSET = string.ascii_letters + string.digits  # 62 chars

def base62_encode(n: int) -> str:
    if n == 0: return CHARSET[0]
    chars = []
    while n:
        chars.append(CHARSET[n % 62])
        n //= 62
    return "".join(reversed(chars))

def generate_code(url: str, attempt: int = 0) -> str:
    raw = f"{url}:{attempt}:{time.time_ns()}"
    hex_val = hashlib.md5(raw.encode()).hexdigest()[:10]
    return base62_encode(int(hex_val, 16))[:7].ljust(7, "a")


# ─────────────────────────────────────────────
#  JWT AUTH
#  Protects URL creation endpoint
#  In production: replace demo token endpoint with OAuth2
#  (Google, GitHub, Auth0) — redirect clients don't need auth
# ─────────────────────────────────────────────
_bearer = HTTPBearer(auto_error=False)

def create_token(user_id: str) -> str:
    exp = datetime.utcnow() + timedelta(hours=TOKEN_EXPIRE)
    return jwt.encode({"sub": user_id, "exp": exp}, SECRET_KEY, algorithm=ALGORITHM)

def require_auth(creds: HTTPAuthorizationCredentials = Security(_bearer)) -> str:
    """FastAPI dependency — inject as: user_id: str = Depends(require_auth)"""
    if not creds:
        raise HTTPException(401, detail={
            "error": "authentication_required",
            "hint": "POST /api/auth/token with {\"user_id\": \"you\"} to get a token",
            "then": "Add header: Authorization: Bearer <token>"
        })
    try:
        payload = jwt.decode(creds.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        return payload.get("sub")
    except JWTError:
        raise HTTPException(401, "Invalid or expired token")


# ─────────────────────────────────────────────
#  LRU CACHE WITH TTL
#  In production: replace with redis-py
#   SET key value EX ttl_seconds
#   GET key
#  O(1) read/write with doubly-linked OrderedDict
# ─────────────────────────────────────────────
class LRUCache:
    def __init__(self, capacity: int = 500):
        self._cache: OrderedDict = OrderedDict()
        self._ttls: Dict[str, float] = {}
        self._capacity = capacity
        self.stats = {"hits": 0, "misses": 0}

    def get(self, key: str):
        if key not in self._cache:
            self.stats["misses"] += 1
            return None
        if time.time() > self._ttls.get(key, float("inf")):
            del self._cache[key]; del self._ttls[key]
            self.stats["misses"] += 1
            return None
        self._cache.move_to_end(key)
        self.stats["hits"] += 1
        return self._cache[key]

    def set(self, key: str, value, ttl: int = 3600):
        self._cache[key] = value
        self._ttls[key] = time.time() + ttl
        self._cache.move_to_end(key)
        if len(self._cache) > self._capacity:
            oldest = next(iter(self._cache))
            del self._cache[oldest]; self._ttls.pop(oldest, None)

    @property
    def hit_rate(self) -> float:
        total = self.stats["hits"] + self.stats["misses"]
        return round(self.stats["hits"] / total * 100, 1) if total else 0.0


# ─────────────────────────────────────────────
#  SINGLE-FLIGHT (thundering herd / cache stampede)
#  When 1,000 requests miss cache simultaneously for the same key:
#  only ONE hits storage; the other 999 await the same Future
# ─────────────────────────────────────────────
class SingleFlight:
    def __init__(self):
        self._inflight: Dict[str, asyncio.Future] = {}
        self.deduped_requests = 0

    async def do(self, key: str, fn):
        if key in self._inflight:
            self.deduped_requests += 1
            return await asyncio.shield(self._inflight[key])
        loop = asyncio.get_event_loop()
        fut = loop.create_future()
        self._inflight[key] = fut
        try:
            result = await fn()
            fut.set_result(result)
            return result
        except Exception as e:
            fut.set_exception(e)
            raise
        finally:
            self._inflight.pop(key, None)


# ─────────────────────────────────────────────
#  TOKEN BUCKET RATE LIMITER (per IP)
#  In production: this state lives in Redis with Lua script
#  for atomic read-modify-write across distributed nodes
# ─────────────────────────────────────────────
class TokenBucket:
    def __init__(self, capacity: int = 5, rate: float = 0.5):
        self.capacity = capacity
        self.rate = rate
        self._buckets: Dict[str, dict] = {}
        self._lock = threading.Lock()

    def consume(self, key: str):
        with self._lock:
            now = time.time()
            b = self._buckets.setdefault(key, {"tokens": float(self.capacity), "ts": now})
            b["tokens"] = min(self.capacity, b["tokens"] + (now - b["ts"]) * self.rate)
            b["ts"] = now
            if b["tokens"] >= 1:
                b["tokens"] -= 1
                return True, round(b["tokens"], 2)
            return False, round((1 - b["tokens"]) / self.rate, 2)

    def status(self, key: str):
        b = self._buckets.get(key, {"tokens": self.capacity})
        return {"tokens": round(b.get("tokens", self.capacity), 2), "capacity": self.capacity}


# ─────────────────────────────────────────────
#  ASYNC ANALYTICS QUEUE
#  Decouples redirect latency from analytics writes
#  In production: publish to Kafka / Kinesis; consume in analytics worker
# ─────────────────────────────────────────────
class AnalyticsQueue:
    def __init__(self):
        self._q: deque = deque(maxlen=10000)
        self._store: Dict[str, list] = {}
        self.processed = 0

    def enqueue(self, event: dict):
        self._q.append(event)

    async def drain(self):
        while True:
            await asyncio.sleep(0.05)
            while self._q:
                e = self._q.popleft()
                self._store.setdefault(e["code"], []).append(e)
                self.processed += 1

    def analytics(self, code: str):
        events = self._store.get(code, [])
        hourly: Dict[str, int] = {}
        for e in events:
            h = e["ts"][:13]
            hourly[h] = hourly.get(h, 0) + 1
        return {
            "total_clicks": len(events),
            "unique_ips":   len({e["ip"] for e in events}),
            "cache_hits":   sum(1 for e in events if e.get("cache_hit")),
            "hourly":       hourly,
            "recent":       events[-5:],
        }


# ─────────────────────────────────────────────
#  SQLITE URL STORE  (persistent)
#  Schema maps 1:1 to PostgreSQL — just swap the driver:
#    aiosqlite.connect()  →  asyncpg.create_pool()
#    db.execute()         →  pool.execute()
#    ? placeholders       →  $1 placeholders
# ─────────────────────────────────────────────
class URLStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._url_index: Dict[str, str] = {}  # original_url → code (in-memory for O(1) idempotency)

    async def init(self):
        """Run DB migrations on startup. Safe to run repeatedly (IF NOT EXISTS)."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS urls (
                    code         TEXT PRIMARY KEY,
                    original_url TEXT NOT NULL,
                    created_at   TEXT NOT NULL,
                    expires_at   TEXT NOT NULL,
                    created_by   TEXT DEFAULT 'anonymous',
                    click_count  INTEGER DEFAULT 0
                )
            """)
            await db.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_urls_original ON urls(original_url)
            """)
            await db.commit()
        # Rebuild in-memory URL index from DB on startup
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT code, original_url FROM urls") as cur:
                async for row in cur:
                    self._url_index[row[1]] = row[0]

    async def save(self, code: str, record: dict):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """INSERT OR IGNORE INTO urls
                   (code, original_url, created_at, expires_at, created_by)
                   VALUES (?, ?, ?, ?, ?)""",
                (code, record["original_url"], record["created_at"],
                 record["expires_at"], record.get("created_by", "anonymous"))
            )
            await db.commit()
        self._url_index[record["original_url"]] = code

    async def by_code(self, code: str) -> Optional[dict]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM urls WHERE code = ?", (code,)) as cur:
                row = await cur.fetchone()
                return dict(row) if row else None

    def by_url(self, url: str) -> Optional[str]:
        return self._url_index.get(url)

    async def all(self) -> list:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM urls ORDER BY created_at DESC LIMIT 50"
            ) as cur:
                return [dict(r) for r in await cur.fetchall()]

    async def count(self) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT COUNT(*) FROM urls") as cur:
                row = await cur.fetchone()
                return row[0] if row else 0

    async def increment_clicks(self, code: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE urls SET click_count = click_count + 1 WHERE code = ?", (code,)
            )
            await db.commit()


# ─── Singletons ───────────────────────────────
cache   = LRUCache(500)
singlef = SingleFlight()
rl      = TokenBucket(5, 0.5)
aq      = AnalyticsQueue()
store   = URLStore(DB_PATH)


# ─────────────────────────────────────────────
#  MODELS
# ─────────────────────────────────────────────
class ShortenReq(BaseModel):
    url: str
    custom_code: Optional[str] = None
    ttl_hours: int = 168  # 7 days default

class TokenReq(BaseModel):
    user_id: str  # In production: validate real credentials


# ─────────────────────────────────────────────
#  STARTUP — init DB, start background tasks, seed demo data
# ─────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    await store.init()
    asyncio.create_task(aq.drain())
    # Seed demo URLs (INSERT OR IGNORE = idempotent)
    for url in [
        "https://github.com/tiangolo/fastapi",
        "https://docs.python.org/3/library/asyncio.html",
        "https://redis.io/docs/manual/eviction/",
    ]:
        if not store.by_url(url):
            code = generate_code(url)
            exp  = (datetime.now() + timedelta(hours=168)).isoformat()
            rec  = {"short_code": code, "original_url": url,
                    "created_at": datetime.now().isoformat(),
                    "expires_at": exp, "created_by": "seed"}
            await store.save(code, rec)
            cache.set(code, rec)


# ─────────────────────────────────────────────
#  ROUTES
# ─────────────────────────────────────────────

@app.post("/api/auth/token")
def get_token(req: TokenReq):
    """
    Demo auth endpoint — returns a JWT for any user_id.
    In production: validate username+password or OAuth2 code → return real JWT.
    """
    token = create_token(req.user_id)
    return {
        "access_token": token,
        "token_type":   "bearer",
        "expires_in":   TOKEN_EXPIRE * 3600,
        "user_id":      req.user_id,
        "usage":        "Authorization: Bearer <token>  (required for POST /api/shorten)"
    }


@app.post("/api/shorten")
async def shorten(req: ShortenReq, request: Request,
                  user_id: str = Depends(require_auth)):
    """
    Create a shortened URL.
    Requires JWT — get one from POST /api/auth/token
    Redirects (GET /r/{code}) are always public.
    """
    ip = request.client.host

    # ── Rate limit (Token Bucket, per IP) ─────
    allowed, info = rl.consume(ip)
    if not allowed:
        raise HTTPException(429, f"Rate limit exceeded. Retry in {info:.1f}s")

    # ── Idempotency: same URL → return existing code ─
    existing_code = store.by_url(req.url)
    if existing_code and not req.custom_code:
        rec = await store.by_code(existing_code)
        return {"short_code": existing_code,
                "short_url":  f"/r/{existing_code}",
                "original_url": req.url,
                "expires_at": rec["expires_at"],
                "idempotent": True,
                "created_by": rec.get("created_by"),
                "tokens_remaining": info}

    # ── Collision-safe code generation ────────
    code = req.custom_code
    for attempt in range(5):
        candidate = code or generate_code(req.url, attempt)
        if not await store.by_code(candidate):
            code = candidate
            break
        code = None
    else:
        raise HTTPException(500, "Collision: could not generate unique code after 5 attempts")

    exp = (datetime.now() + timedelta(hours=req.ttl_hours)).isoformat()
    rec = {"short_code": code, "original_url": req.url,
           "created_at": datetime.now().isoformat(),
           "expires_at": exp, "created_by": user_id}

    await store.save(code, rec)                # ← persisted to SQLite
    cache.set(code, rec, ttl=req.ttl_hours * 3600)  # ← cached in LRU

    return {"short_code": code, "short_url": f"/r/{code}",
            "original_url": req.url, "expires_at": exp,
            "idempotent": False, "created_by": user_id, "tokens_remaining": info}


@app.get("/r/{code}")
async def redirect(code: str, request: Request):
    """Public redirect — no auth required"""
    ip = request.client.host

    # ── Cache-aside: check LRU first ─────────
    rec = cache.get(code)
    cache_hit = rec is not None

    if not rec:
        # ── Single-flight: only one DB query for concurrent misses ─
        async def fetch_from_db(): return await store.by_code(code)
        rec = await singlef.do(code, fetch_from_db)
        if not rec:
            raise HTTPException(404, "Short URL not found")
        if datetime.fromisoformat(rec["expires_at"]) < datetime.now():
            raise HTTPException(410, "Short URL has expired")
        cache.set(code, rec)

    # ── Non-blocking analytics (fire-and-forget) ─
    aq.enqueue({"code": code, "ip": ip,
                "ts": datetime.now().isoformat(), "cache_hit": cache_hit})
    asyncio.create_task(store.increment_clicks(code))  # persist click count to SQLite

    return {"redirect_to": rec["original_url"], "cache_hit": cache_hit,
            "single_flight_deduped": singlef.deduped_requests}


@app.get("/api/analytics/{code}")
async def get_analytics(code: str):
    rec = await store.by_code(code)
    if not rec: raise HTTPException(404, "Code not found")
    return {
        **aq.analytics(code),
        "url_info": rec,
        "persistent_click_count": rec.get("click_count", 0),
        "note": "click_count is durable (SQLite); other analytics are in-memory"
    }

@app.get("/api/cache/stats")
def get_cache_stats():
    return {
        **cache.stats,
        "hit_rate_pct": cache.hit_rate,
        "single_flight_deduped": singlef.deduped_requests,
        "queue_processed": aq.processed,
        "storage": "SQLite (persistent — survives restart)",
        "cache": "In-Memory LRU (production: Redis SETEX)"
    }

@app.get("/api/rate-limit/status")
def rate_status(request: Request):
    return rl.status(request.client.host)

@app.get("/api/urls")
async def list_urls():
    urls = await store.all()
    total = await store.count()
    return {"urls": urls, "total": total, "storage": "SQLite"}

@app.get("/api/health")
async def health():
    total = await store.count()
    return {"status": "ok", "project": "URL Shortener", "port": 8001,
            "storage": "SQLite", "db_path": DB_PATH, "total_urls": total,
            "auth": "JWT (Bearer token)"}

app.mount("/", StaticFiles(directory="static", html=True), name="static")
