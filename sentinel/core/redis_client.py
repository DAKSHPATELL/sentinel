"""
SENTINEL Redis client.
Async Redis connection pool with Streams, Bloom Filter, and Sorted Set operations.
"""
from __future__ import annotations

from typing import Any, Optional

import redis.asyncio as aioredis
import structlog

from sentinel.config import get_config

logger = structlog.get_logger(__name__)


class RedisClient:
    """Async Redis client with connection pooling."""

    def __init__(self) -> None:
        """Initialize Redis client (call connect() to establish connection)."""
        self._pool: Optional[aioredis.ConnectionPool] = None
        self._client: Optional[aioredis.Redis] = None

    async def connect(self) -> None:
        """Establish Redis connection pool."""
        config = get_config()
        self._pool = aioredis.ConnectionPool.from_url(
            config.redis.url,
            max_connections=config.redis.max_connections,
            decode_responses=False,
        )
        self._client = aioredis.Redis(connection_pool=self._pool)
        logger.info("redis_connected", url=config.redis.url)

    @property
    def client(self) -> aioredis.Redis:
        """Get the underlying Redis client."""
        if self._client is None:
            raise RuntimeError("Redis not connected. Call connect() first.")
        return self._client

    async def close(self) -> None:
        """Close the Redis connection pool."""
        if self._client:
            await self._client.aclose()
        if self._pool:
            await self._pool.disconnect()
        logger.info("redis_disconnected")

    # ─── BASIC OPERATIONS ───────────────────────────────────────

    async def get(self, key: str) -> Optional[bytes]:
        """Get a value by key."""
        return await self.client.get(key)

    async def set(self, key: str, value: str | bytes, ex: Optional[int] = None) -> bool:
        """Set a key-value pair with optional expiration in seconds."""
        return await self.client.set(key, value, ex=ex)

    async def delete(self, key: str) -> int:
        """Delete a key. Returns number of keys deleted."""
        return await self.client.delete(key)

    async def exists(self, key: str) -> bool:
        """Check if a key exists."""
        return bool(await self.client.exists(key))

    # ─── STREAM OPERATIONS ──────────────────────────────────────

    async def publish_event(self, stream: str, data: dict[str, str | bytes]) -> str:
        """Publish an event to a Redis Stream."""
        event_id = await self.client.xadd(stream, data)
        return event_id.decode() if isinstance(event_id, bytes) else event_id

    async def consume_events(
        self,
        stream: str,
        group: str,
        consumer: str,
        count: int = 10,
        block: int = 5000,
    ) -> list[tuple[str, dict]]:
        """Consume events from a Redis Stream consumer group."""
        messages = await self.client.xreadgroup(
            groupname=group,
            consumername=consumer,
            streams={stream: ">"},
            count=count,
            block=block,
        )
        results = []
        if messages:
            for stream_name, events in messages:
                for event_id, event_data in events:
                    eid = event_id.decode() if isinstance(event_id, bytes) else event_id
                    decoded = {
                        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
                        for k, v in event_data.items()
                    }
                    results.append((eid, decoded))
        return results

    async def ack_event(self, stream: str, group: str, event_id: str) -> int:
        """Acknowledge a consumed event."""
        return await self.client.xack(stream, group, event_id)

    async def stream_length(self, stream: str) -> int:
        """Get the length of a stream."""
        return await self.client.xlen(stream)

    # ─── SORTED SET OPERATIONS ──────────────────────────────────

    async def add_to_sorted_set(self, key: str, member: str, score: float) -> int:
        """Add a member to a sorted set."""
        return await self.client.zadd(key, {member: score})

    async def get_from_sorted_set(
        self, key: str, start: int = 0, end: int = -1, desc: bool = True
    ) -> list[tuple[str, float]]:
        """Get members from a sorted set with scores."""
        if desc:
            results = await self.client.zrevrange(key, start, end, withscores=True)
        else:
            results = await self.client.zrange(key, start, end, withscores=True)
        return [
            (m.decode() if isinstance(m, bytes) else m, s)
            for m, s in results
        ]

    # ─── BLOOM FILTER OPERATIONS ────────────────────────────────
    # Uses Redis SET as a simple stand-in; real Bloom filter via bloom-filter2 in memory

    async def set_add(self, key: str, member: str) -> int:
        """Add a member to a Redis set (used for URL dedup tracking)."""
        return await self.client.sadd(key, member)

    async def set_is_member(self, key: str, member: str) -> bool:
        """Check if a member exists in a Redis set."""
        return bool(await self.client.sismember(key, member))

    # ─── HEALTH CHECK ───────────────────────────────────────────

    async def health_check(self) -> dict[str, Any]:
        """Check Redis health and return status info."""
        try:
            await self.client.ping()
            info = await self.client.info("memory")
            return {
                "status": "healthy",
                "used_memory_mb": round(info.get("used_memory", 0) / 1024 / 1024, 2),
                "connected_clients": info.get("connected_clients", 0),
            }
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}
