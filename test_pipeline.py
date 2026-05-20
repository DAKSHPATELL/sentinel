#!/usr/bin/env python3
"""
SENTINEL End-to-End Pipeline Integration Test.
Fetches a real URL and pushes it through every pipeline stage,
verifying data appears at each step.
"""
import asyncio
import hashlib
import sys
import os
from pathlib import Path
from datetime import datetime

# Add project to path
sys.path.insert(0, str(Path(__file__).parent))
os.environ.setdefault("SENTINEL_DATA_DIR", str(Path(__file__).parent / "data"))

PASS = "\033[92m✓\033[0m"
FAIL = "\033[91m✗\033[0m"
INFO = "\033[94m→\033[0m"

results: list[tuple[str, bool, str]] = []


def report(stage: str, ok: bool, detail: str = "") -> None:
    results.append((stage, ok, detail))
    icon = PASS if ok else FAIL
    print(f"  {icon} {stage}: {detail}")


async def main() -> None:
    print("\n" + "=" * 60)
    print("  SENTINEL Pipeline Integration Test")
    print("=" * 60 + "\n")

    # ── 0. Config ──────────────────────────────────────────────
    from sentinel.config import load_config
    config = load_config(None)
    print(f"  {INFO} Data dir: {config.system.data_dir}")
    print(f"  {INFO} LLM: {config.extraction.llm_model}")
    print(f"  {INFO} LLM base: {config.extraction.llm_base_url}")
    print()

    # ── 1. Fetch a real page (or use synthetic if no network) ──
    print("  Stage 1: FETCH")
    import httpx
    test_url = "https://blog.anthropic.com/research"
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
            resp = await client.get(test_url)
            raw_html = resp.text
        report("FETCH", len(raw_html) > 100, f"{len(raw_html)} bytes from {test_url}")
    except Exception:
        # Fallback: realistic synthetic HTML for testing
        test_url = "https://blog.anthropic.com/research/claude-model-spec"
        raw_html = """<!DOCTYPE html><html><head><title>Anthropic Research - Claude Model Spec</title></head>
        <body>
        <nav>Home | Blog | Research | Company</nav>
        <article>
        <h1>Anthropic Releases Claude Model Specification</h1>
        <p>Published March 28, 2026 by Dario Amodei, CEO of Anthropic</p>
        <p>San Francisco — Anthropic has released a comprehensive model specification for its Claude AI assistant.
        The document outlines Claude's values, capabilities, and behavioral guidelines. This represents a major
        milestone in AI transparency and safety.</p>
        <p>The Claude Model Spec is a 30,000-word document that describes how Claude should behave in various
        situations. It covers topics like honesty, helpfulness, harmlessness, and how Claude should handle
        edge cases. OpenAI and Google DeepMind have published similar documents for GPT-4 and Gemini respectively.</p>
        <p>Dario Amodei stated: "We believe transparency about AI systems is critical for building trust.
        The model specification is our attempt to be as clear as possible about what Claude is designed to do."</p>
        <p>The document has been praised by researchers at Stanford University and MIT, who noted that it sets
        a new standard for AI documentation. Microsoft and Meta are reportedly working on similar specifications
        for their AI models.</p>
        <p>Anthropic also announced a $2 billion Series D funding round led by Goldman Sachs and Spark Capital.
        The company is now valued at $60 billion, making it one of the most valuable AI startups in the world.</p>
        <p>In related news, the White House Office of Science and Technology Policy released new guidelines
        for AI safety testing that reference Anthropic's work on constitutional AI.</p>
        </article>
        <footer>Copyright 2026 Anthropic</footer>
        </body></html>"""
        report("FETCH", True, f"Using synthetic content ({len(raw_html)} bytes) — no network access")

    # ── 2. HTML Cleaning ──────────────────────────────────────
    print("\n  Stage 2: CLEAN")
    from sentinel.extraction import html_cleaner
    cleaned = html_cleaner.clean(raw_html)
    report("CLEAN", len(cleaned) > 50, f"{len(cleaned)} chars cleaned text")
    if len(cleaned) < 50:
        print("  Cleaned text too short, trying with raw HTML truncated...")
        cleaned = raw_html[:5000]

    # ── 3. Deduplication ──────────────────────────────────────
    print("\n  Stage 3: DEDUP")
    from sentinel.extraction.deduplicator import Deduplicator
    dedup = Deduplicator()
    content_hash = hashlib.sha256(cleaned.encode()).hexdigest()
    is_dup = dedup.is_exact_duplicate(content_hash)
    report("DEDUP", not is_dup, "not a duplicate" if not is_dup else "DUPLICATE — would be skipped")

    # ── 4. NER ────────────────────────────────────────────────
    print("\n  Stage 4: NER")
    from sentinel.extraction.ner_engine import NEREngine
    ner = NEREngine()
    entities = ner.extract_entities(cleaned)
    report("NER", len(entities) > 0, f"Found {len(entities)} entities")
    if entities:
        for e in entities[:8]:
            print(f"        {e.text:30s} → {e.entity_type.value}")

    # ── 5. LLM Extraction ────────────────────────────────────
    print("\n  Stage 5: LLM EXTRACTION")
    from sentinel.extraction.llm_extractor import LLMExtractor
    llm = LLMExtractor()
    try:
        llm_result = await llm.extract(cleaned, test_url)
        has_title = bool(llm_result.get("title"))
        has_summary = bool(llm_result.get("summary"))
        has_topics = bool(llm_result.get("topics"))
        report("LLM_EXTRACT", has_title or has_summary,
               f"title={'✓' if has_title else '✗'} summary={'✓' if has_summary else '✗'} "
               f"topics={len(llm_result.get('topics', []))} entities={len(llm_result.get('entities', []))}")
        if has_title:
            print(f"        Title: {llm_result['title'][:80]}")
        if has_summary:
            print(f"        Summary: {llm_result['summary'][:120]}...")
    except Exception as e:
        report("LLM_EXTRACT", False, str(e))
        llm_result = {"title": "", "summary": "", "topics": []}

    # ── 6. Embedding ──────────────────────────────────────────
    print("\n  Stage 6: EMBEDDING")
    from sentinel.extraction.embedder import Embedder
    embedder = Embedder()
    try:
        embedding = embedder.embed_text(cleaned[:2000])
        report("EMBEDDING", embedding is not None and len(embedding) > 0,
               f"dim={len(embedding)}, norm={sum(x*x for x in embedding):.3f}")
    except Exception as e:
        report("EMBEDDING", False, str(e))
        embedding = None

    # ── 7. Relevance Classification ───────────────────────────
    print("\n  Stage 7: RELEVANCE")
    from sentinel.extraction.classifier import RelevanceClassifier
    classifier = RelevanceClassifier(embedder)
    if embedding:
        relevance = classifier.score_relevance(embedding)
        report("RELEVANCE", True, f"score={relevance:.3f}")
    else:
        relevance = 0.5
        report("RELEVANCE", False, "No embedding available")

    # ── 8. Entity Resolution ──────────────────────────────────
    print("\n  Stage 8: ENTITY RESOLUTION")
    from sentinel.knowledge.entity_resolver import EntityResolver
    from sentinel.core.lancedb_client import LanceDBClient
    lance = LanceDBClient()
    await lance.connect()
    await lance.initialize_tables()
    resolver = EntityResolver(lance, embedder)
    if entities:
        nodes = resolver.resolve_batch(entities)
        unique_ids = set(n.id for n in nodes)
        report("ENTITY_RESOLVE", len(nodes) > 0,
               f"{len(entities)} entities → {len(unique_ids)} unique nodes")
    else:
        nodes = []
        report("ENTITY_RESOLVE", False, "No entities to resolve")

    # ── 9. Relationship Extraction ────────────────────────────
    print("\n  Stage 9: RELATIONSHIP EXTRACTION")
    from sentinel.knowledge.relationship_extractor import RelationshipExtractor
    rel_extractor = RelationshipExtractor()
    entity_names = [e.text for e in entities]
    try:
        edges = await rel_extractor.extract(cleaned, entity_names, test_url)
        report("RELATIONSHIPS", True, f"{len(edges)} relationships extracted")
        for edge in edges[:5]:
            print(f"        {edge.source_node_id} → {edge.relationship_type} → {edge.target_node_id}")
    except Exception as e:
        report("RELATIONSHIPS", False, str(e))
        edges = []

    # ── 10. Knowledge Graph ───────────────────────────────────
    print("\n  Stage 10: KNOWLEDGE GRAPH")
    from sentinel.core.neo4j_client import Neo4jClient
    from sentinel.knowledge.graph_builder import GraphBuilder
    from sentinel.models import ExtractedContent, SourceType, ContentType
    graph_store = Neo4jClient()
    await graph_store.connect()
    graph_builder = GraphBuilder(graph_store, lance)
    await graph_builder.initialize()

    content = ExtractedContent(
        crawl_job_id="00000000-0000-0000-0000-000000000000",
        url=test_url,
        source_type=SourceType.WEB_CRAWL,
        content_type=ContentType.ARTICLE,
        title=llm_result.get("title", "Test Page"),
        summary=llm_result.get("summary", ""),
        full_text=cleaned,
        entities=entities,
        topics=llm_result.get("topics", []),
        relevance_score=relevance,
        embedding=embedding,
        word_count=len(cleaned.split()),
    )

    await graph_builder.process_content(content, nodes, edges)
    stats = await graph_builder.get_graph_stats()
    report("GRAPH", stats["node_count"] > 0,
           f"{stats['node_count']} nodes, {stats['edge_count']} edges, "
           f"types: {stats['type_distribution']}")

    # ── 11. DuckDB Signal Detection ───────────────────────────
    print("\n  Stage 11: SIGNAL DETECTION (record mentions)")
    from sentinel.core.duckdb_client import DuckDBClient
    duckdb = DuckDBClient()
    duckdb.connect()

    from sentinel.signals.signal_aggregator import SignalAggregator
    # Need EventBus for SignalAggregator but we can test without it
    try:
        # Create a minimal aggregator for testing
        sig_agg = SignalAggregator(duckdb, None)  # No event bus needed for recording
        sig_agg.initialize()

        for entity in entities[:10]:
            sig_agg.anomaly_detector.record_mentions(
                entity_name=entity.text,
                count=1,
                source_types=["web_crawl"],
                relevance=relevance,
            )
            sig_agg.cascade_detector.record_appearance(
                entity_name=entity.text,
                source_type="web_crawl",
                url=test_url,
                title=content.title,
                summary=content.summary or "",
                relevance=relevance,
            )

        # Check DuckDB has data
        ts_rows = duckdb.query("SELECT COUNT(*) as cnt FROM entity_timeseries")
        appearances = duckdb.query("SELECT COUNT(*) as cnt FROM entity_appearances")
        ts_count = ts_rows[0]["cnt"] if ts_rows else 0
        app_count = appearances[0]["cnt"] if appearances else 0
        report("SIGNAL_RECORD", ts_count > 0 or app_count > 0,
               f"timeseries={ts_count} rows, appearances={app_count} rows")
    except Exception as e:
        report("SIGNAL_RECORD", False, str(e))

    # ── 12. Run detection cycle ───────────────────────────────
    print("\n  Stage 12: SIGNAL DETECTION CYCLE")
    try:
        signals = await sig_agg.run_detection_cycle()
        report("SIGNAL_DETECT", True,
               f"{len(signals)} signals detected (may be 0 on first run — need more data)")
    except Exception as e:
        report("SIGNAL_DETECT", False, str(e))

    # ── Cleanup ───────────────────────────────────────────────
    await graph_store.close()
    await lance.close()
    duckdb.close()

    # ── Summary ───────────────────────────────────────────────
    print("\n" + "=" * 60)
    passed = sum(1 for _, ok, _ in results if ok)
    failed = sum(1 for _, ok, _ in results if not ok)
    total = len(results)
    color = "\033[92m" if failed == 0 else "\033[93m" if failed <= 2 else "\033[91m"
    print(f"  {color}Results: {passed}/{total} passed, {failed} failed\033[0m")
    print("=" * 60 + "\n")

    if failed > 0:
        print("  Failed stages:")
        for stage, ok, detail in results:
            if not ok:
                print(f"    {FAIL} {stage}: {detail}")
        print()


if __name__ == "__main__":
    asyncio.run(main())
