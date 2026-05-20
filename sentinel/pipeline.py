"""
SENTINEL Extraction Pipeline Worker.
Listens to crawl_results events and pushes content through:
  crawl result → HTML clean → NER → LLM extract → embed → dedup
  → entity resolve → relationship extract → graph build
  → signal detection → red team → court → alerts

This is the connective tissue that makes SENTINEL autonomous.
"""
from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Optional

import structlog

from sentinel.config import get_config
from sentinel.core.duckdb_client import DuckDBClient
from sentinel.core.lancedb_client import LanceDBClient
from sentinel.core.neo4j_client import Neo4jClient
from sentinel.events import (
    EventBus,
    STREAM_CRAWL_RESULTS,
    STREAM_EXTRACTED,
    STREAM_ENTITIES,
)
from sentinel.extraction import html_cleaner
from sentinel.extraction.ner_engine import NEREngine
from sentinel.extraction.llm_extractor import LLMExtractor
from sentinel.extraction.embedder import Embedder
from sentinel.extraction.deduplicator import Deduplicator
from sentinel.extraction.classifier import RelevanceClassifier
from sentinel.knowledge.entity_resolver import EntityResolver
from sentinel.knowledge.relationship_extractor import RelationshipExtractor
from sentinel.knowledge.graph_builder import GraphBuilder
from sentinel.signals.signal_aggregator import SignalAggregator
from sentinel.intelligence.red_team import RedTeamAgent
from sentinel.intelligence.court import HypothesisCourt
from sentinel.intelligence.immune_explorer import ImmuneExplorer
from sentinel.interface.alerts import AlertManager
from sentinel.models import (
    CrawlResult,
    ExtractedContent,
    SourceType,
    ContentType,
    AlertPriority,
)

logger = structlog.get_logger(__name__)


class ExtractionPipeline:
    """
    Event-driven pipeline that processes crawl results into intelligence.

    Each crawled page flows through 7 stages:
    1. CLEAN — Strip navigation, ads, boilerplate
    2. EXTRACT — NER + LLM structured extraction
    3. EMBED — Generate embeddings for semantic operations
    4. DEDUP — Skip near-duplicate content
    5. RESOLVE — Map entities to canonical graph nodes
    6. GRAPH — Build knowledge graph edges
    7. SIGNAL — Feed time-series data to detectors
    """

    def __init__(
        self,
        event_bus: EventBus,
        lance: LanceDBClient,
        duckdb: DuckDBClient,
        neo4j: Neo4jClient,
        embedder: Embedder,
        signal_aggregator: SignalAggregator,
        red_team: RedTeamAgent,
        court: HypothesisCourt,
        immune: ImmuneExplorer,
        alert_manager: AlertManager,
    ) -> None:
        self._event_bus = event_bus
        self._lance = lance
        self._duckdb = duckdb
        self._neo4j = neo4j
        self._embedder = embedder

        # Extraction components
        self._ner = NEREngine()
        self._llm_extractor = LLMExtractor()
        self._deduplicator = Deduplicator()
        self._classifier = RelevanceClassifier(embedder)

        # Knowledge components
        self._entity_resolver = EntityResolver(lance, embedder)
        self._relationship_extractor = RelationshipExtractor()
        self._graph_builder = GraphBuilder(neo4j, lance)

        # Intelligence components
        self._signal_aggregator = signal_aggregator
        self._red_team = red_team
        self._court = court
        self._immune = immune
        self._alert_manager = alert_manager

        self._config = get_config()
        self._processed_count = 0
        self._running = False

    async def start(self) -> None:
        """Start the pipeline worker listening to crawl results with auto-retry."""
        self._running = True
        logger.info("pipeline_started")

        # Retry loop — survives Redis disconnects and respects shutdown
        while self._running:
            try:
                await self._event_bus.consumer.listen(
                    stream=STREAM_CRAWL_RESULTS,
                    group="extraction_pipeline",
                    consumer_name="pipeline_worker_1",
                    handler=self._process_crawl_result,
                    batch_size=5,
                    block_ms=3000,
                )
            except asyncio.CancelledError:
                logger.info("pipeline_cancelled")
                break
            except Exception as e:
                if not self._running:
                    break
                logger.error("pipeline_connection_lost", error=str(e))
                logger.info("pipeline_reconnecting", wait_seconds=10)
                try:
                    await asyncio.sleep(10)
                except asyncio.CancelledError:
                    break

    def stop(self) -> None:
        """Stop the pipeline worker."""
        self._running = False
        logger.info("pipeline_stopped", processed=self._processed_count)

    async def _process_crawl_result(self, event_data: dict) -> None:
        """
        Process a single crawl result through the full pipeline.

        This is the handler called by the EventConsumer for each event.
        """
        if not event_data or not isinstance(event_data, dict):
            return
        try:
            url = event_data.get("url", "")
            raw_html_path = event_data.get("raw_html_path", "")
            cleaned_text = event_data.get("cleaned_text", "")
            title = event_data.get("title", "")
            source_type = event_data.get("source_type", "web_crawl")

            if not cleaned_text and not raw_html_path:
                return

            # If we have raw HTML but no cleaned text, clean it
            if not cleaned_text and raw_html_path:
                import gzip
                from pathlib import Path
                html_path = Path(raw_html_path)
                if html_path.exists():
                    if html_path.suffix == ".gz":
                        with gzip.open(html_path, "rt", encoding="utf-8") as f:
                            raw_html = f.read()
                    else:
                        raw_html = html_path.read_text(encoding="utf-8")
                    cleaned_text = html_cleaner.clean(raw_html)
                else:
                    return

            if not cleaned_text or len(cleaned_text) < 50:
                return

            # ── Stage 1: Deduplication ────────────────────────────
            import hashlib
            content_hash = hashlib.sha256(cleaned_text.encode()).hexdigest()
            if self._deduplicator.is_exact_duplicate(content_hash):
                logger.debug("pipeline_dedup_skip", url=url)
                return
            if self._deduplicator.is_near_duplicate(cleaned_text, doc_id=url):
                logger.debug("pipeline_near_dedup_skip", url=url)
                return

            # ── Stage 2: NER ──────────────────────────────────────
            entities = self._ner.extract_entities(cleaned_text)

            # ── Stage 3: LLM structured extraction ────────────────
            llm_result = await self._llm_extractor.extract(cleaned_text, url) or {}

            # ── Stage 4: Embedding ────────────────────────────────
            embedding = self._embedder.embed_text(cleaned_text[:2000])

            # ── Stage 5: Relevance classification ─────────────────
            relevance_score = self._classifier.score_relevance(embedding)

            # Build the ExtractedContent record
            content = ExtractedContent(
                crawl_job_id=event_data.get("job_id", "00000000-0000-0000-0000-000000000000"),
                url=url,
                source_type=SourceType(source_type) if source_type in SourceType._value2member_map_ else SourceType.WEB_CRAWL,
                content_type=ContentType.ARTICLE,
                title=llm_result.get("title", title) or title or url,
                summary=llm_result.get("summary", ""),
                full_text=cleaned_text,
                entities=entities,
                topics=llm_result.get("topics", []),
                relevance_score=relevance_score,
                embedding=embedding,
                word_count=len(cleaned_text.split()),
            )

            # Emit extracted content event
            try:
                await self._event_bus.emit(
                    STREAM_EXTRACTED,
                    {
                        "url": url,
                        "title": content.title,
                        "entity_count": len(entities),
                        "relevance": relevance_score,
                    },
                )
            except Exception:
                pass

            # ── Stage 6: Entity Resolution + Knowledge Graph ──────
            entity_names = [e.text for e in entities]
            resolved_nodes = self._entity_resolver.resolve_batch(entities)

            # Build entity name → canonical node ID map for relationship extractor
            entity_id_map: dict[str, str] = {}
            for node in resolved_nodes:
                entity_id_map[node.canonical_name.lower()] = node.id
                for alias in node.aliases:
                    entity_id_map[alias.lower()] = node.id

            # Extract relationships via LLM
            edges = await self._relationship_extractor.extract(
                cleaned_text, entity_names, url, entity_id_map=entity_id_map
            )

            # Build graph
            await self._graph_builder.process_content(content, resolved_nodes, edges)

            # ── Stage 7: Signal Detection Feed ────────────────────
            # Record entity mentions for anomaly/burst detection
            for entity in entities:
                self._signal_aggregator.anomaly_detector.record_mentions(
                    entity_name=entity.text,
                    count=1,
                    source_types=[source_type],
                    relevance=relevance_score,
                )

                # Record for cascade detection
                self._signal_aggregator.cascade_detector.record_appearance(
                    entity_name=entity.text,
                    source_type=source_type,
                    url=url,
                    title=content.title,
                    summary=content.summary or "",
                    relevance=relevance_score,
                )

            # ── Stage 8: Immune Explorer ──────────────────────────
            if embedding:
                try:
                    self._immune.score(url, embedding)
                except Exception:
                    pass

            self._processed_count += 1

            if self._processed_count % 100 == 0:
                logger.info("pipeline_progress", processed=self._processed_count)

        except Exception as e:
            import traceback
            logger.error("pipeline_process_failed", url=event_data.get("url"), error=str(e),
                         traceback=traceback.format_exc()[-300:])


class SignalProcessingLoop:
    """
    Periodic loop that runs signal detection, red team challenges,
    and dispatches alerts.

    Runs on a configurable interval (default: every 15 minutes).
    """

    def __init__(
        self,
        signal_aggregator: SignalAggregator,
        red_team: RedTeamAgent,
        court: HypothesisCourt,
        alert_manager: AlertManager,
        interval_seconds: int = 900,
    ) -> None:
        self._aggregator = signal_aggregator
        self._red_team = red_team
        self._court = court
        self._alerts = alert_manager
        self._interval = interval_seconds
        self._running = False

    async def start(self) -> None:
        """Start the periodic signal processing loop."""
        self._running = True
        logger.info("signal_loop_started", interval_s=self._interval)

        while self._running:
            try:
                await self._run_cycle()
            except asyncio.CancelledError:
                logger.info("signal_loop_cancelled")
                break
            except Exception as e:
                logger.error("signal_loop_error", error=str(e))

            try:
                await asyncio.sleep(self._interval)
            except asyncio.CancelledError:
                logger.info("signal_loop_cancelled")
                break

    def stop(self) -> None:
        self._running = False

    async def _run_cycle(self) -> None:
        """One full signal processing cycle."""
        # 1. Run all detectors
        signals = await self._aggregator.run_detection_cycle()

        if not signals:
            return

        logger.info("signal_cycle_detected", count=len(signals))

        # 2. Red team challenge each signal
        for signal in signals:
            if signal.priority.value in ("high", "critical"):
                try:
                    challenge = await self._red_team.challenge(signal)
                    signal.confidence = challenge.adjusted_confidence
                    signal.metadata["red_team"] = {
                        "adjusted_confidence": challenge.adjusted_confidence,
                        "survived": challenge.survived,
                        "kill_reason": challenge.kill_reason,
                        "details": challenge.details
                    }
                    self._aggregator.update_signal_in_db(signal)
                    if not challenge.survived:
                        logger.info("signal_killed_by_red_team", signal_id=str(signal.id), reason=challenge.kill_reason)
                        continue
                except Exception as e:
                    logger.warning("red_team_challenge_failed", error=str(e))

            # 3. Hypothesis court for high-confidence signals
            if signal.confidence >= 0.6 and signal.priority.value in ("high", "critical"):
                try:
                    verdict = await self._court.deliberate(signal)
                    signal.confidence = verdict.final_confidence
                    signal.metadata["verdict"] = {
                        "approved": verdict.approved,
                        "reasoning": verdict.reasoning,
                        "advocate_argument": verdict.advocate_argument,
                        "skeptic_argument": verdict.skeptic_argument,
                        "final_confidence": verdict.final_confidence
                    }
                    self._aggregator.update_signal_in_db(signal)
                    if not verdict.approved:
                        logger.info("signal_rejected_by_court", signal_id=str(signal.id))
                        continue
                except Exception as e:
                    logger.warning("court_deliberation_failed", error=str(e))

            # 4. Send alert
            await self._alerts.send_alert(signal)
