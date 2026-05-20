"""
SENTINEL CLI.
Click-based command line interface with proper start/stop lifecycle.
"""
from __future__ import annotations

import asyncio
import os
import signal
import sys
from pathlib import Path
from typing import Optional

import click
import structlog

from sentinel.config import load_config
from sentinel.utils.logging import setup_logging

logger = structlog.get_logger(__name__)

# Global shutdown event — set by signal handlers, checked by all loops
_shutdown_event: Optional[asyncio.Event] = None


@click.group()
@click.option("--config", "-c", type=click.Path(), default=None, help="Path to sentinel.toml")
@click.pass_context
def cli(ctx: click.Context, config: Optional[str]) -> None:
    """SENTINEL — Autonomous Web Intelligence System"""
    ctx.ensure_object(dict)
    cfg = load_config(config)
    ctx.obj["config"] = cfg
    setup_logging(cfg.system.log_level, cfg.system.data_dir)


@cli.command()
@click.pass_context
def start(ctx: click.Context) -> None:
    """Start SENTINEL core workers."""
    config = ctx.obj["config"]
    click.echo(click.style("╔══════════════════════════════════════╗", fg="cyan", bold=True))
    click.echo(click.style("║     SENTINEL — Starting Up...       ║", fg="cyan", bold=True))
    click.echo(click.style("╚══════════════════════════════════════╝", fg="cyan", bold=True))
    click.echo()

    async def _start() -> None:
        global _shutdown_event
        from sentinel.core.pidfile import write_pid, remove_pid, is_running, DEFAULT_PID_PATH
        from sentinel.core.redis_client import RedisClient
        from sentinel.core.sqlite_client import SQLiteClient
        from sentinel.core.lancedb_client import LanceDBClient
        from sentinel.core.duckdb_client import DuckDBClient
        from sentinel.core.neo4j_client import Neo4jClient
        from sentinel.core.storage import StorageManager
        from sentinel.events import EventBus
        from sentinel.stealth.rate_limiter import RateLimiter
        from sentinel.stealth.robots_parser import RobotsParser
        from sentinel.ingestion.frontier import URLFrontier
        from sentinel.ingestion.scheduler import SourceScheduler
        from sentinel.ingestion.sources.hackernews import HackerNewsMonitor
        from sentinel.ingestion.sources.github_trending import GitHubTrendingMonitor
        from sentinel.ingestion.sources.arxiv_monitor import ArxivMonitor
        from sentinel.ingestion.sources.rss_aggregator import RSSAggregator
        from sentinel.ingestion.sources.web_crawler import WebCrawler
        from sentinel.ingestion.sources.ct_monitor import CTMonitorSource
        from sentinel.ingestion.sources.patent_monitor import PatentMonitor
        from sentinel.ingestion.sources.commoncrawl import CommonCrawlSource
        from sentinel.ingestion.sources.sitemap_crawler import SitemapCrawler
        from sentinel.stealth.annihilator import AcquisitionOrchestrator
        from sentinel.stealth.temporal_arbitrage import TemporalArbitrageScheduler
        from sentinel.extraction.embedder import Embedder
        from sentinel.extraction.hyde import HydeEngine
        from sentinel.extraction.causal_retriever import CausalRetriever
        from sentinel.intelligence.red_team import RedTeamAgent
        from sentinel.intelligence.immune_explorer import ImmuneExplorer
        from sentinel.intelligence.court import HypothesisCourt
        from sentinel.intelligence.predictor import PredictiveCrawler
        from sentinel.signals.signal_aggregator import SignalAggregator
        from sentinel.knowledge.graph_builder import GraphBuilder
        from sentinel.interface.alerts import AlertManager
        from sentinel.interface.reports import ReportGenerator
        from sentinel.pipeline import ExtractionPipeline, SignalProcessingLoop

        # ── Pre-flight: Check if already running ──────────────
        pid_path = Path(config.system.data_dir) / "sentinel.pid"
        if is_running(pid_path):
            click.echo(click.style("  ✗ SENTINEL is already running!", fg="red", bold=True))
            click.echo("    Use 'sentinel stop' to stop it first.")
            return

        # ── Write PID file ────────────────────────────────────
        write_pid(pid_path)

        # ── Shutdown event — all loops watch this ─────────────
        _shutdown_event = asyncio.Event()

        # ── Signal handlers ───────────────────────────────────
        loop = asyncio.get_running_loop()

        def _handle_shutdown(sig_name: str) -> None:
            click.echo(f"\n  Received {sig_name} — initiating graceful shutdown...")
            logger.info("shutdown_signal_received", signal=sig_name)
            _shutdown_event.set()

        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, _handle_shutdown, sig.name)

        # ── Layer 0: Infrastructure ───────────────────────────
        click.echo("  [1/7] Connecting to Redis...")
        redis = RedisClient()
        await redis.connect()

        click.echo("  [2/7] Initializing SQLite frontier...")
        sqlite = SQLiteClient()
        await sqlite.connect()

        click.echo("  [3/7] Initializing LanceDB...")
        lance = LanceDBClient()
        await lance.connect()
        await lance.initialize_tables()

        click.echo("  [4/7] Initializing DuckDB...")
        duckdb = DuckDBClient()
        duckdb.connect()

        click.echo("  [5/7] Connecting to Neo4j...")
        neo4j = Neo4jClient()
        await neo4j.connect()

        click.echo("  [6/7] Setting up event bus...")
        event_bus = EventBus()
        await event_bus.initialize(redis.client)

        # ── Layer 1: Ingestion ────────────────────────────────
        storage = StorageManager()
        rate_limiter = RateLimiter()
        robots_parser = RobotsParser(sqlite)
        frontier = URLFrontier(sqlite)

        scheduler = SourceScheduler(frontier)
        scheduler.register_source("hackernews", HackerNewsMonitor())
        scheduler.register_source("github", GitHubTrendingMonitor())
        scheduler.register_source("arxiv", ArxivMonitor())
        scheduler.register_source("rss", RSSAggregator())
        scheduler.register_source("ct_monitor", CTMonitorSource())
        scheduler.register_source("patents", PatentMonitor())
        scheduler.register_source("commoncrawl", CommonCrawlSource())
        scheduler.register_source("sitemap", SitemapCrawler(sqlite))

        # ── Layer 2: Stealth ──────────────────────────────────
        orchestrator = AcquisitionOrchestrator(sqlite)
        await orchestrator.initialize()
        temporal_scheduler = TemporalArbitrageScheduler(duckdb)
        temporal_scheduler.initialize()

        # ── Layer 3: Extraction ───────────────────────────────
        embedder = Embedder()
        hyde = HydeEngine(embedder)
        causal = CausalRetriever(lance, embedder)

        # ── Layer 5: Signal Detection ─────────────────────────
        click.echo("  [7/7] Initializing intelligence engines...")
        signal_aggregator = SignalAggregator(duckdb, event_bus)
        signal_aggregator.initialize()

        # ── Layer 4: Knowledge Graph ──────────────────────────
        graph_builder = GraphBuilder(neo4j, lance)
        await graph_builder.initialize()

        # ── Layer 6: Intelligence ─────────────────────────────
        red_team = RedTeamAgent(lance, duckdb, embedder)
        immune = ImmuneExplorer(duckdb)
        immune.initialize()
        court = HypothesisCourt(lance, embedder)
        predictor = PredictiveCrawler(duckdb)
        predictor.initialize()

        # ── Layer 7: Interface ────────────────────────────────
        alert_manager = AlertManager()
        report_generator = ReportGenerator(duckdb)

        # Wire up the FastAPI dependencies
        from sentinel.interface.api import set_dependencies
        set_dependencies(duckdb, lance, neo4j, signal_aggregator, report_generator)

        # ── Pipeline: Connects all layers ─────────────────────
        pipeline = ExtractionPipeline(
            event_bus=event_bus,
            lance=lance,
            duckdb=duckdb,
            neo4j=neo4j,
            embedder=embedder,
            signal_aggregator=signal_aggregator,
            red_team=red_team,
            court=court,
            immune=immune,
            alert_manager=alert_manager,
        )

        signal_loop = SignalProcessingLoop(
            signal_aggregator=signal_aggregator,
            red_team=red_team,
            court=court,
            alert_manager=alert_manager,
            interval_seconds=900,  # Every 15 minutes
        )

        # Set up web crawler
        crawler = WebCrawler(
            frontier, rate_limiter, robots_parser, storage,
            orchestrator, temporal_scheduler, event_bus
        )

        click.echo()
        click.echo(click.style("  ✓ SENTINEL is running — all 7 layers active", fg="green", bold=True))
        click.echo(f"    PID:         {os.getpid()}")
        click.echo(f"    Data dir:    {config.system.data_dir}")
        click.echo(f"    Workers:     {config.system.max_workers}")
        click.echo(f"    LLM:         {config.extraction.llm_model}")
        click.echo(f"    API:         http://{config.interface.api_host}:{config.interface.api_port}")
        click.echo(f"    Signals:     anomaly + burst + cascade detection (15min cycles)")
        click.echo(f"    Alerts:      {', '.join(config.interface.alerts.channels)}")
        click.echo(f"    Stop:        sentinel stop  (or Ctrl+C)")
        click.echo()

        # ── Start all workers ─────────────────────────────────
        tasks: list[asyncio.Task] = []
        try:
            source_task = asyncio.create_task(scheduler.start())
            crawl_task = asyncio.create_task(crawler.run_workers(config.system.max_workers))
            pipeline_task = asyncio.create_task(pipeline.start())
            signal_task = asyncio.create_task(signal_loop.start())

            # Start FastAPI in background
            import uvicorn
            api_config = uvicorn.Config(
                "sentinel.interface.api:app",
                host=config.interface.api_host,
                port=config.interface.api_port,
                log_level="warning",
            )
            api_server = uvicorn.Server(api_config)
            api_task = asyncio.create_task(api_server.serve())

            tasks = [source_task, crawl_task, pipeline_task, signal_task, api_task]

            # Wait for shutdown signal
            await _shutdown_event.wait()

        except Exception as e:
            logger.error("sentinel_fatal", error=str(e))
        finally:
            # ── Graceful shutdown sequence ─────────────────────
            click.echo(click.style("\n  Graceful shutdown in progress...", fg="yellow"))

            # Phase 1: Stop accepting new work
            click.echo("    [1/4] Stopping crawl workers...")
            crawler.stop()
            api_server.should_exit = True

            # Phase 2: Drain in-flight work (give 5 seconds)
            click.echo("    [2/4] Draining in-flight work...")
            pipeline.stop()
            signal_loop.stop()
            await asyncio.sleep(2)

            # Phase 3: Cancel remaining tasks
            click.echo("    [3/4] Cancelling tasks...")
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await scheduler.stop()

            # Phase 4: Close connections
            click.echo("    [4/4] Closing connections...")
            await redis.close()
            await sqlite.close()
            await lance.close()
            duckdb.close()
            await neo4j.close()
            await event_bus.close()

            # Remove PID file
            remove_pid(pid_path)

            click.echo(click.style("  ✓ SENTINEL stopped cleanly", fg="green", bold=True))

    asyncio.run(_start())


@cli.command()
@click.option("--force", "-f", is_flag=True, help="Force kill (SIGKILL) if graceful shutdown fails")
@click.option("--timeout", "-t", default=30, help="Seconds to wait for graceful shutdown")
@click.pass_context
def stop(ctx: click.Context, force: bool, timeout: int) -> None:
    """Stop a running SENTINEL instance."""
    from sentinel.core.pidfile import read_pid, is_running, stop_process, remove_pid
    config = ctx.obj["config"]
    pid_path = Path(config.system.data_dir) / "sentinel.pid"

    pid = read_pid(pid_path)
    if pid is None:
        click.echo(click.style("  SENTINEL is not running (no PID file found)", fg="yellow"))
        return

    if not is_running(pid_path):
        click.echo(click.style("  SENTINEL is not running (stale PID file cleaned up)", fg="yellow"))
        return

    click.echo(click.style(f"  Stopping SENTINEL (PID {pid})...", fg="cyan", bold=True))

    if force:
        # Immediate SIGKILL
        try:
            os.kill(pid, signal.SIGKILL)
            import time
            time.sleep(0.5)
            remove_pid(pid_path)
            click.echo(click.style("  ✓ SENTINEL force-killed", fg="yellow"))
        except ProcessLookupError:
            remove_pid(pid_path)
            click.echo(click.style("  Process already gone", fg="yellow"))
        return

    # Graceful shutdown via SIGTERM
    stopped = stop_process(pid_path, timeout=timeout)
    if stopped:
        click.echo(click.style("  ✓ SENTINEL stopped gracefully", fg="green", bold=True))
    else:
        click.echo(click.style("  ✗ Failed to stop — try: sentinel stop --force", fg="red"))


@cli.command()
@click.pass_context
def status(ctx: click.Context) -> None:
    """Show SENTINEL system status."""
    from sentinel.core.pidfile import read_pid, is_running
    config = ctx.obj["config"]
    pid_path = Path(config.system.data_dir) / "sentinel.pid"

    click.echo(click.style("SENTINEL Status", fg="cyan", bold=True))
    click.echo(f"  Version:  {config.system.version}")
    click.echo(f"  Data dir: {config.system.data_dir}")
    click.echo(f"  Workers:  {config.system.max_workers}")
    click.echo(f"  LLM:      {config.extraction.llm_model}")

    # Process status
    pid = read_pid(pid_path)
    if pid and is_running(pid_path):
        click.echo(click.style(f"  Process:  ✓ Running (PID {pid})", fg="green"))
    elif pid:
        click.echo(click.style(f"  Process:  ✗ Stale PID {pid} (not running)", fg="yellow"))
    else:
        click.echo(click.style("  Process:  ✗ Not running", fg="red"))
    click.echo()

    async def _status() -> None:
        from sentinel.core.redis_client import RedisClient
        from sentinel.core.sqlite_client import SQLiteClient

        click.echo("  Infrastructure:")

        # Redis
        try:
            redis = RedisClient()
            await redis.connect()
            health = await redis.health_check()
            if health["status"] == "healthy":
                click.echo(click.style(f"    Redis: ✓ healthy ({health['used_memory_mb']} MB)", fg="green"))
            else:
                click.echo(click.style(f"    Redis: ✗ {health.get('error', 'unknown')}", fg="red"))
            await redis.close()
        except Exception as e:
            click.echo(click.style(f"    Redis: ✗ {e}", fg="red"))

        # SQLite
        try:
            sqlite = SQLiteClient()
            await sqlite.connect()
            stats = await sqlite.get_frontier_stats()
            click.echo(click.style(f"    SQLite: ✓ frontier ({stats.get('total', 0)} URLs)", fg="green"))
            await sqlite.close()
        except Exception as e:
            click.echo(click.style(f"    SQLite: ✗ {e}", fg="red"))

        click.echo()

    asyncio.run(_status())


@cli.command("frontier")
@click.pass_context
def frontier_stats(ctx: click.Context) -> None:
    """Show URL frontier statistics."""
    async def _stats() -> None:
        from sentinel.core.sqlite_client import SQLiteClient

        sqlite = SQLiteClient()
        await sqlite.connect()
        stats = await sqlite.get_frontier_stats()
        await sqlite.close()

        click.echo(click.style("Frontier Statistics", fg="cyan", bold=True))
        for key, val in stats.items():
            click.echo(f"  {key:20s}: {val}")

    asyncio.run(_stats())


@cli.command("sources")
@click.pass_context
def list_sources(ctx: click.Context) -> None:
    """List configured sources and their status."""
    config = ctx.obj["config"]
    sources = config.ingestion.sources

    click.echo(click.style("Configured Sources", fg="cyan", bold=True))
    click.echo(f"  {'Source':20s} {'Enabled':10s} {'Interval':15s}")
    click.echo(f"  {'─' * 45}")
    click.echo(f"  {'Hacker News':20s} {'✓' if sources.hackernews.enabled else '✗':10s} {sources.hackernews.poll_interval_seconds}s")
    click.echo(f"  {'GitHub':20s} {'✓' if sources.github.enabled else '✗':10s} {sources.github.poll_interval_seconds}s")
    click.echo(f"  {'arXiv':20s} {'✓' if sources.arxiv.enabled else '✗':10s} {sources.arxiv.poll_interval_seconds}s")
    click.echo(f"  {'RSS':20s} {'✓' if sources.rss.enabled else '✗':10s} {sources.rss.poll_interval_seconds}s")
    click.echo(f"  {'Reddit':20s} {'✓' if sources.reddit.enabled else '✗':10s} {sources.reddit.poll_interval_seconds}s")
    click.echo(f"  {'Twitter':20s} {'✓' if sources.twitter.enabled else '✗':10s} {sources.twitter.poll_interval_seconds}s")
    click.echo(f"  {'Web Crawler':20s} {'✓' if sources.web_crawler.enabled else '✗':10s} {'continuous'}")
    click.echo(f"  {'Patents':20s} {'✓' if sources.patents.enabled else '✗':10s} {sources.patents.poll_interval_seconds}s")
    click.echo(f"    CPC codes: {', '.join(sources.patents.cpc_codes)}")
    click.echo(f"    Jurisdictions: {', '.join(sources.patents.jurisdictions)}")
    click.echo(f"  {'Common Crawl':20s} {'✓' if sources.commoncrawl.enabled else '✗':10s} {sources.commoncrawl.poll_interval_seconds}s")
    click.echo(f"    Domains/cycle: {sources.commoncrawl.domains_per_cycle}")
    click.echo(f"  {'Sitemap Crawler':20s} {'✓' if sources.sitemap.enabled else '✗':10s} {sources.sitemap.poll_interval_seconds}s")
    click.echo(f"    Domains/cycle: {sources.sitemap.domains_per_cycle}")


@cli.command("report")
@click.option("--hours", default=24, help="Hours to cover in the report")
@click.pass_context
def generate_report(ctx: click.Context, hours: int) -> None:
    """Generate an intelligence report."""
    config = ctx.obj["config"]

    from sentinel.core.duckdb_client import DuckDBClient
    from sentinel.interface.reports import ReportGenerator

    duckdb = DuckDBClient()
    duckdb.connect()

    generator = ReportGenerator(duckdb)
    report = generator.generate(hours)

    click.echo(report)
    duckdb.close()


@cli.command("api")
@click.pass_context
def run_api(ctx: click.Context) -> None:
    """Start the SENTINEL API server standalone."""
    config = ctx.obj["config"]

    async def _api() -> None:
        from sentinel.core.duckdb_client import DuckDBClient
        from sentinel.core.lancedb_client import LanceDBClient
        from sentinel.core.neo4j_client import Neo4jClient
        from sentinel.signals.signal_aggregator import SignalAggregator
        from sentinel.interface.reports import ReportGenerator
        from sentinel.interface.api import set_dependencies

        duckdb = DuckDBClient()
        duckdb.connect()
        lance = LanceDBClient()
        await lance.connect()
        neo4j = Neo4jClient()
        await neo4j.connect()

        signal_agg = SignalAggregator(duckdb)
        signal_agg.initialize()
        report_gen = ReportGenerator(duckdb)

        set_dependencies(duckdb, lance, neo4j, signal_agg, report_gen)

        import uvicorn
        api_config = uvicorn.Config(
            "sentinel.interface.api:app",
            host=config.interface.api_host,
            port=config.interface.api_port,
            log_level="info",
        )
        server = uvicorn.Server(api_config)
        click.echo(click.style(
            f"  SENTINEL API: http://{config.interface.api_host}:{config.interface.api_port}",
            fg="cyan", bold=True,
        ))
        await server.serve()

    asyncio.run(_api())


@cli.command("signals")
@click.option("--limit", default=20, help="Number of signals to show")
@click.pass_context
def list_signals(ctx: click.Context, limit: int) -> None:
    """Show recent detected signals."""
    from sentinel.core.duckdb_client import DuckDBClient

    duckdb = DuckDBClient()
    duckdb.connect()

    try:
        signals = duckdb.query(
            "SELECT signal_id, signal_type, priority, title, confidence, detected_at FROM signal_log ORDER BY detected_at DESC LIMIT ?",
            (limit,),
        )
        if not signals:
            click.echo("  No signals detected yet.")
            return

        click.echo(click.style("Recent Signals", fg="cyan", bold=True))
        click.echo(f"  {'Priority':10s} {'Type':15s} {'Confidence':12s} {'Title'}")
        click.echo(f"  {'─' * 70}")
        for s in signals:
            priority_color = {"critical": "red", "high": "yellow", "medium": "blue", "low": "white"}.get(s["priority"], "white")
            click.echo(
                click.style(f"  {s['priority']:10s}", fg=priority_color)
                + f" {s['signal_type']:15s} {s['confidence']:10.0%}   {s['title'][:60]}"
            )
    except Exception as e:
        click.echo(f"  No signal data available: {e}")
    finally:
        duckdb.close()


@cli.command("dashboard")
@click.option("--port", default=8050, help="Dashboard port")
@click.option("--no-browser", is_flag=True, help="Don't auto-open browser")
@click.pass_context
def dashboard(ctx: click.Context, port: int, no_browser: bool) -> None:
    """Launch the SENTINEL live intelligence dashboard."""
    config = ctx.obj["config"]

    async def _dashboard() -> None:
        from sentinel.core.pidfile import read_pid, is_running
        from sentinel.core.redis_client import RedisClient
        from sentinel.core.sqlite_client import SQLiteClient
        from sentinel.core.duckdb_client import DuckDBClient
        from sentinel.interface.dashboard import dashboard_app, set_dashboard_deps

        pid_path = Path(config.system.data_dir) / "sentinel.pid"

        click.echo(click.style("╔══════════════════════════════════════╗", fg="cyan", bold=True))
        click.echo(click.style("║  SENTINEL — Intelligence Dashboard   ║", fg="cyan", bold=True))
        click.echo(click.style("╚══════════════════════════════════════╝", fg="cyan", bold=True))
        click.echo()

        # Connect to databases (read-only, doesn't need full pipeline)
        click.echo("  Connecting to data stores...")

        redis_client = None
        try:
            redis = RedisClient()
            await redis.connect()
            redis_client = redis
            click.echo(click.style("    Redis: connected", fg="green"))
        except Exception:
            click.echo(click.style("    Redis: offline (some metrics unavailable)", fg="yellow"))

        sqlite = SQLiteClient()
        await sqlite.connect()
        click.echo(click.style("    SQLite: connected", fg="green"))

        duckdb = None
        try:
            duckdb = DuckDBClient()
            duckdb.connect(read_only=True)
            click.echo(click.style("    DuckDB: connected (read-only)", fg="green"))
        except Exception as e:
            click.echo(click.style(f"    DuckDB: locked by SENTINEL (signals/entities via API instead)", fg="yellow"))
            duckdb = None

        # Set dependencies for dashboard
        set_dashboard_deps(duckdb, sqlite, redis_client, None, config)

        # Check if SENTINEL is running
        if is_running(pid_path):
            pid = read_pid(pid_path)
            click.echo(click.style(f"\n  SENTINEL is running (PID {pid}) — dashboard shows live data", fg="green"))
        else:
            click.echo(click.style("\n  SENTINEL is not running — dashboard shows historical data", fg="yellow"))
            click.echo("  Start SENTINEL: sentinel start")

        url = f"http://localhost:{port}"
        click.echo(click.style(f"\n  Dashboard: {url}", fg="cyan", bold=True))
        click.echo("  Press Ctrl+C to close\n")

        # Open browser
        if not no_browser:
            import webbrowser
            webbrowser.open(url)

        # Run dashboard server
        import uvicorn
        server_config = uvicorn.Config(
            dashboard_app,
            host="0.0.0.0",
            port=port,
            log_level="warning",
        )
        server = uvicorn.Server(server_config)
        await server.serve()

        # Cleanup
        await sqlite.close()
        if duckdb:
            duckdb.close()
        if redis_client:
            await redis_client.close()

    asyncio.run(_dashboard())


if __name__ == "__main__":
    cli()
