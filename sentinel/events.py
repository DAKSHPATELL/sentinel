"""
SENTINEL event system.
Redis Streams-based event producer, consumer, and bus.
Events are the connective tissue between processing layers.
"""
from __future__ import annotations

import asyncio
from typing import Any, Callable, Optional

import orjson
import structlog

from sentinel.config import get_config

logger = structlog.get_logger(__name__)

# ─── STREAM NAMES ───────────────────────────────────────────────

STREAM_CRAWL_JOBS = "sentinel:stream:crawl_jobs"
STREAM_CRAWL_RESULTS = "sentinel:stream:crawl_results"
STREAM_EXTRACTED = "sentinel:stream:extracted"
STREAM_ENTITIES = "sentinel:stream:entities"
STREAM_SIGNALS = "sentinel:stream:signals"
STREAM_ALERTS = "sentinel:stream:alerts"
STREAM_SYSTEM = "sentinel:stream:system"

ALL_STREAMS = [
    STREAM_CRAWL_JOBS,
    STREAM_CRAWL_RESULTS,
    STREAM_EXTRACTED,
    STREAM_ENTITIES,
    STREAM_SIGNALS,
    STREAM_ALERTS,
    STREAM_SYSTEM,
]


class EventProducer:
    """Publishes events to Redis Streams."""

    def __init__(self, redis_client: Any) -> None:
        """
        Initialize the event producer.

        Args:
            redis_client: Async Redis connection.
        """
        self._redis = redis_client
        self._config = get_config()

    async def emit(self, stream: str, data: dict) -> str:
        """
        Publish an event to a Redis Stream.

        Args:
            stream: Stream name to publish to.
            data: Event data dictionary.

        Returns:
            Event ID assigned by Redis.
        """
        try:
            # Serialize data values to strings for Redis
            serialized = {k: orjson.dumps(v).decode() if not isinstance(v, (str, bytes)) else v for k, v in data.items()}
            event_id = await self._redis.xadd(stream, serialized)

            # Trim stream if exceeds max length
            max_len = self._config.redis.stream_max_len
            stream_len = await self._redis.xlen(stream)
            if stream_len > max_len:
                await self._redis.xtrim(stream, maxlen=max_len, approximate=True)

            logger.debug("event_emitted", stream=stream, event_id=event_id, keys=list(data.keys()))
            return event_id
        except Exception as e:
            logger.error("event_emit_failed", stream=stream, error=str(e))
            raise


class EventConsumer:
    """Subscribes to Redis Streams with consumer groups."""

    def __init__(self, redis_client: Any) -> None:
        """
        Initialize the event consumer.

        Args:
            redis_client: Async Redis connection.
        """
        self._redis = redis_client

    async def _ensure_group(self, stream: str, group: str) -> None:
        """Create consumer group if it doesn't exist."""
        try:
            await self._redis.xgroup_create(stream, group, id="0", mkstream=True)
            logger.info("consumer_group_created", stream=stream, group=group)
        except Exception as e:
            # BUSYGROUP means group already exists — that's fine
            if "BUSYGROUP" in str(e):
                pass
            else:
                raise

    async def listen(
        self,
        stream: str,
        group: str,
        consumer_name: str,
        handler: Callable,
        batch_size: int = 10,
        block_ms: int = 5000,
    ) -> None:
        """
        Listen for events on a stream with a consumer group.

        Runs in a blocking loop, calling handler for each event.

        Args:
            stream: Stream to consume from.
            group: Consumer group name.
            consumer_name: Name of this consumer within the group.
            handler: Async callable to process each event.
            batch_size: Number of events to read per iteration.
            block_ms: Milliseconds to block waiting for events.
        """
        await self._ensure_group(stream, group)

        logger.info("consumer_started", stream=stream, group=group, consumer=consumer_name)

        while True:
            try:
                messages = await self._redis.xreadgroup(
                    groupname=group,
                    consumername=consumer_name,
                    streams={stream: ">"},
                    count=batch_size,
                    block=block_ms,
                )

                if not messages:
                    continue

                for stream_name, events in messages:
                    for event_id, event_data in events:
                        try:
                            # Deserialize data
                            deserialized = {}
                            for k, v in event_data.items():
                                key = k.decode() if isinstance(k, bytes) else k
                                val = v.decode() if isinstance(v, bytes) else v
                                try:
                                    deserialized[key] = orjson.loads(val)
                                except (orjson.JSONDecodeError, ValueError):
                                    deserialized[key] = val

                            await handler(deserialized)

                            # Acknowledge message
                            await self._redis.xack(stream, group, event_id)

                        except Exception as e:
                            logger.error(
                                "event_handler_failed",
                                stream=stream,
                                event_id=event_id,
                                error=str(e),
                            )

            except asyncio.CancelledError:
                logger.info("consumer_stopped", stream=stream, group=group, consumer=consumer_name)
                break
            except Exception as e:
                logger.error("consumer_error", stream=stream, error=str(e))
                await asyncio.sleep(1)


class EventBus:
    """
    Singleton event bus that manages the Redis connection
    and provides produce/consume methods.
    """

    _instance: Optional[EventBus] = None
    _redis: Any = None
    _producer: Optional[EventProducer] = None
    _consumer: Optional[EventConsumer] = None

    def __new__(cls) -> EventBus:
        """Singleton pattern."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    async def initialize(self, redis_client: Any) -> None:
        """
        Initialize the event bus with a Redis connection.

        Args:
            redis_client: Async Redis connection.
        """
        self._redis = redis_client
        self._producer = EventProducer(redis_client)
        self._consumer = EventConsumer(redis_client)
        logger.info("event_bus_initialized")

    @property
    def producer(self) -> EventProducer:
        """Get the event producer."""
        if self._producer is None:
            raise RuntimeError("EventBus not initialized. Call initialize() first.")
        return self._producer

    @property
    def consumer(self) -> EventConsumer:
        """Get the event consumer."""
        if self._consumer is None:
            raise RuntimeError("EventBus not initialized. Call initialize() first.")
        return self._consumer

    async def emit(self, stream: str, data: dict) -> str:
        """Convenience method to emit an event."""
        return await self.producer.emit(stream, data)

    async def close(self) -> None:
        """Close the event bus and its Redis connection."""
        if self._redis:
            await self._redis.aclose()
            self._redis = None
            self._producer = None
            self._consumer = None
            logger.info("event_bus_closed")

    @classmethod
    def reset(cls) -> None:
        """Reset the singleton (for testing)."""
        cls._instance = None
        cls._redis = None
        cls._producer = None
        cls._consumer = None
