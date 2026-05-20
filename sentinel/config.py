"""
SENTINEL configuration module.
Reads sentinel.toml and provides typed configuration via Pydantic Settings.
Supports environment variable overrides with SENTINEL_ prefix.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


# ─── NESTED CONFIG MODELS ───────────────────────────────────────


class RedisConfig(BaseModel):
    """Redis connection configuration."""
    url: str = "redis://localhost:6379/0"
    max_connections: int = 20
    stream_max_len: int = 100_000


class Neo4jConfig(BaseModel):
    """Neo4j connection configuration."""
    uri: str = "bolt://localhost:7687"
    user: str = "neo4j"
    password: str = "sentinel_secure_password"
    database: str = "sentinel"
    max_connection_pool_size: int = 50


class LanceDBConfig(BaseModel):
    """LanceDB configuration."""
    path: str = "./data/lance"
    embedding_dim: int = 384


class DuckDBConfig(BaseModel):
    """DuckDB configuration."""
    path: str = "./data/duckdb/sentinel.duckdb"
    memory_limit: str = "4GB"
    threads: int = 4


class SQLiteConfig(BaseModel):
    """SQLite configuration."""
    crawl_state_path: str = "./data/crawl_state/frontier.db"
    wal_mode: bool = True


# ─── INGESTION CONFIGS ──────────────────────────────────────────


class FrontierConfig(BaseModel):
    """URL frontier configuration."""
    max_size: int = 10_000_000
    priority_decay: float = 0.95
    min_revisit_hours: int = 24
    max_revisit_hours: int = 720
    max_depth: int = 5


class HackerNewsConfig(BaseModel):
    """Hacker News source configuration."""
    enabled: bool = True
    poll_interval_seconds: int = 120
    min_score_threshold: int = 5
    categories: list[str] = Field(default_factory=lambda: ["new", "top", "best"])


class GitHubConfig(BaseModel):
    """GitHub source configuration."""
    enabled: bool = True
    poll_interval_seconds: int = 300
    token: str = ""
    track_events: list[str] = Field(
        default_factory=lambda: ["CreateEvent", "PublicEvent", "ReleaseEvent", "WatchEvent"]
    )
    trending_languages: list[str] = Field(default_factory=lambda: ["python", "rust", "typescript", "go"])
    trending_since: str = "daily"


class ArxivConfig(BaseModel):
    """arXiv source configuration."""
    enabled: bool = True
    poll_interval_seconds: int = 3600
    categories: list[str] = Field(
        default_factory=lambda: ["cs.AI", "cs.LG", "cs.CL", "cs.CR", "cs.SE", "econ.GN", "q-fin.GN"]
    )
    max_results_per_query: int = 100


class PatentsConfig(BaseModel):
    """Patent monitor configuration."""
    enabled: bool = True
    poll_interval_seconds: int = 43200  # Every 12 hours
    jurisdictions: list[str] = Field(default_factory=lambda: ["US", "EP", "WO", "CN", "JP", "KR"])
    cpc_codes: list[str] = Field(default_factory=lambda: [
        "G06N",  # AI, Machine Learning, Neural Networks
        "G06F",  # Computing & Data Processing
        "G06V",  # Computer Vision & Image Recognition
        "G06Q",  # Business Methods & Fintech
        "G06T",  # Image Data Processing & 3D
        "H04L",  # Telecommunications & Networking
        "H04W",  # Wireless Communication
        "G16H",  # Healthcare Informatics
        "G16B",  # Bioinformatics
        "Y02E",  # Clean Energy Technologies
        "H01L",  # Semiconductor Devices
        "H02J",  # Power Distribution & Battery Systems
        "B25J",  # Robotics & Manipulators
        "A61B",  # Medical Diagnostics
        "G01N",  # Material Analysis & Sensors
    ])


class SecEdgarConfig(BaseModel):
    """SEC EDGAR source configuration."""
    enabled: bool = True
    poll_interval_seconds: int = 3600
    form_types: list[str] = Field(default_factory=lambda: ["8-K", "10-K", "10-Q", "S-1", "DEF 14A"])
    keywords: list[str] = Field(default_factory=list)


class RedditConfig(BaseModel):
    """Reddit source configuration."""
    enabled: bool = True
    poll_interval_seconds: int = 300
    subreddits: list[str] = Field(default_factory=lambda: [
        "technology", "artificial", "MachineLearning", "cryptocurrency",
        "startups", "SaaS", "biotech", "energy", "robotics",
        "singularity", "Futurology", "science", "nanotech",
        "QuantumComputing", "spacex", "longevity", "climatechange",
        "selfhosted", "LocalLLaMA", "webdev", "golang", "rust",
    ])
    min_score: int = 10
    client_id: str = ""
    client_secret: str = ""


class TwitterConfig(BaseModel):
    """Twitter source configuration."""
    enabled: bool = False
    bearer_token: str = ""
    poll_interval_seconds: int = 900
    tracked_accounts: list[str] = Field(default_factory=list)
    search_queries: list[str] = Field(default_factory=list)


class RSSConfig(BaseModel):
    """RSS source configuration."""
    enabled: bool = True
    poll_interval_seconds: int = 1800
    feeds: list[str] = Field(default_factory=list)


class WebCrawlerConfig(BaseModel):
    """Web crawler configuration."""
    enabled: bool = True
    seed_urls: list[str] = Field(default_factory=list)
    max_pages_per_domain: int = 1000
    respect_robots_txt: bool = True
    follow_redirects: bool = True
    max_redirects: int = 5


class ProductHuntConfig(BaseModel):
    """Product Hunt source configuration."""
    enabled: bool = True
    poll_interval_seconds: int = 3600
    min_votes: int = 50


class CrunchbaseConfig(BaseModel):
    """Crunchbase source configuration."""
    enabled: bool = False
    api_key: str = ""
    poll_interval_seconds: int = 86400


class CommonCrawlConfig(BaseModel):
    """Common Crawl source configuration."""
    enabled: bool = True
    poll_interval_seconds: int = 7200  # Every 2 hours
    domains: list[str] = Field(default_factory=list)  # Empty = use built-in HIGH_VALUE_DOMAINS
    domains_per_cycle: int = 10  # Domains to query per check cycle
    max_pages_per_domain: int = 50  # Max CC-INDEX results per domain


class SitemapConfig(BaseModel):
    """Sitemap discovery engine configuration."""
    enabled: bool = True
    poll_interval_seconds: int = 3600  # Every hour
    domains_per_cycle: int = 20  # Domains to process per check cycle
    max_urls_per_domain: int = 500  # Max URLs to extract per domain sitemap


class SourcesConfig(BaseModel):
    """All source configurations."""
    hackernews: HackerNewsConfig = Field(default_factory=HackerNewsConfig)
    github: GitHubConfig = Field(default_factory=GitHubConfig)
    arxiv: ArxivConfig = Field(default_factory=ArxivConfig)
    patents: PatentsConfig = Field(default_factory=PatentsConfig)
    sec_edgar: SecEdgarConfig = Field(default_factory=SecEdgarConfig)
    reddit: RedditConfig = Field(default_factory=RedditConfig)
    twitter: TwitterConfig = Field(default_factory=TwitterConfig)
    rss: RSSConfig = Field(default_factory=RSSConfig)
    web_crawler: WebCrawlerConfig = Field(default_factory=WebCrawlerConfig)
    product_hunt: ProductHuntConfig = Field(default_factory=ProductHuntConfig)
    crunchbase: CrunchbaseConfig = Field(default_factory=CrunchbaseConfig)
    commoncrawl: CommonCrawlConfig = Field(default_factory=CommonCrawlConfig)
    sitemap: SitemapConfig = Field(default_factory=SitemapConfig)


class IngestionConfig(BaseModel):
    """Ingestion layer configuration."""
    default_crawl_delay: float = 2.0
    max_concurrent_domains: int = 50
    max_page_size_bytes: int = 10_485_760
    request_timeout: int = 30
    user_agent: str = "SentinelBot/1.0 (research; +https://yoursite.com/bot)"
    frontier: FrontierConfig = Field(default_factory=FrontierConfig)
    sources: SourcesConfig = Field(default_factory=SourcesConfig)


# ─── STEALTH CONFIGS ────────────────────────────────────────────


class ProxyConfig(BaseModel):
    """Proxy cascade configuration."""
    enabled: bool = False
    cascade_strategy: str = "escalate"
    datacenter_proxies: list[str] = Field(default_factory=list)
    residential_proxies: list[str] = Field(default_factory=list)
    mobile_proxies: list[str] = Field(default_factory=list)
    max_failures_before_escalate: int = 3
    cooldown_after_block_seconds: int = 300


class RateLimitingConfig(BaseModel):
    """Rate limiting configuration."""
    default_requests_per_minute: int = 20
    aggressive_domains: dict[str, int] = Field(default_factory=dict)
    backoff_on_429: bool = True
    backoff_multiplier: float = 2.0
    max_backoff_seconds: int = 3600


class AnnihilatorConfig(BaseModel):
    """Multi-strategy content acquisition configuration."""
    enabled: bool = True
    max_parallel_strategies: int = 5
    max_total_strategies: int = 12
    enable_google_cache: bool = True
    enable_wayback: bool = True
    enable_api_discovery: bool = True
    enable_social_extraction: bool = True
    enable_content_reconstruction: bool = True
    reconstruction_min_fragments: int = 3
    reconstruction_min_confidence: float = 0.6


class TemporalArbitrageConfig(BaseModel):
    """Temporal arbitrage scheduling configuration."""
    enabled: bool = True
    min_success_data_points: int = 20
    retry_boost_priority: float = 2.0
    max_retries_per_url: int = 5


class StealthConfig(BaseModel):
    """Stealth layer configuration."""
    enabled: bool = True
    browser_engine: str = "camoufox"
    headless: bool = True
    max_browser_instances: int = 4
    browser_restart_after: int = 100
    rotate_fingerprint_every: int = 50
    proxy: ProxyConfig = Field(default_factory=ProxyConfig)
    rate_limiting: RateLimitingConfig = Field(default_factory=RateLimitingConfig)
    annihilator: AnnihilatorConfig = Field(default_factory=AnnihilatorConfig)
    temporal_arbitrage: TemporalArbitrageConfig = Field(default_factory=TemporalArbitrageConfig)


# ─── EXTRACTION CONFIGS ─────────────────────────────────────────


class EmbeddingConfig(BaseModel):
    """Embedding pipeline configuration."""
    model: str = "all-MiniLM-L6-v2"
    device: str = "mps"
    batch_size: int = 256
    normalize: bool = True


class NERConfig(BaseModel):
    """NER engine configuration."""
    backend: str = "spacy"
    spacy_model: str = "en_core_web_trf"
    entity_types: list[str] = Field(
        default_factory=lambda: ["ORG", "PRODUCT", "PERSON", "GPE", "TECH", "EVENT", "MONEY"]
    )


class DedupConfig(BaseModel):
    """Deduplication configuration."""
    minhash_num_perm: int = 128
    minhash_threshold: float = 0.5
    semantic_threshold: float = 0.92
    shingle_size: int = 5


class RelevanceConfig(BaseModel):
    """Relevance scoring configuration."""
    min_score: float = 0.3
    topic_embeddings_path: str = "./data/models/topic_embeddings.npy"


class DOMPruningConfig(BaseModel):
    """DOM pruning configuration."""
    enabled: bool = True
    remove_nav: bool = True
    remove_footer: bool = True
    remove_sidebar: bool = True
    remove_ads: bool = True
    remove_scripts: bool = True
    remove_styles: bool = True
    max_text_length: int = 50_000


class ExtractionConfig(BaseModel):
    """Extraction layer configuration."""
    llm_backend: str = "ollama"
    llm_model: str = "qwen3.5:27b"
    llm_base_url: str = "http://localhost:11434"
    llm_temperature: float = 0.1
    llm_max_tokens: int = 2048
    llm_timeout_seconds: int = 180
    batch_size: int = 10
    embedding: EmbeddingConfig = Field(default_factory=EmbeddingConfig)
    ner: NERConfig = Field(default_factory=NERConfig)
    dedup: DedupConfig = Field(default_factory=DedupConfig)
    relevance: RelevanceConfig = Field(default_factory=RelevanceConfig)
    dom_pruning: DOMPruningConfig = Field(default_factory=DOMPruningConfig)


# ─── KNOWLEDGE CONFIGS ──────────────────────────────────────────


class KnowledgeConfig(BaseModel):
    """Knowledge graph configuration."""
    entity_resolution_threshold: float = 0.85
    relationship_extraction_backend: str = "llm"
    community_detection_resolution: float = 1.0
    community_detection_interval_hours: int = 24
    max_relationships_per_extraction: int = 20
    graph_embedding_dim: int = 128
    graph_embedding_algorithm: str = "node2vec"
    node2vec_walk_length: int = 80
    node2vec_num_walks: int = 10


# ─── SIGNAL CONFIGS ─────────────────────────────────────────────


class TimeSeriesConfig(BaseModel):
    """Time-series analysis configuration."""
    aggregation_interval_hours: int = 1
    min_data_points: int = 48
    prophet_changepoint_prior: float = 0.05
    prophet_seasonality_prior: float = 10.0


class SentimentConfig(BaseModel):
    """Sentiment analysis configuration."""
    backend: str = "local_llm"
    update_interval_hours: int = 6


class SignalsConfig(BaseModel):
    """Signal detection configuration."""
    anomaly_window_size: int = 24
    anomaly_threshold_sigma: float = 3.0
    burst_detection_s: float = 2.0
    burst_detection_gamma: float = 1.0
    cross_correlation_max_lag_hours: int = 720
    cross_correlation_min_significance: float = 0.05
    cascade_min_sources: int = 2
    cascade_max_window_hours: int = 2160
    novelty_k_neighbors: int = 10
    novelty_threshold: float = 0.7
    time_series: TimeSeriesConfig = Field(default_factory=TimeSeriesConfig)
    sentiment: SentimentConfig = Field(default_factory=SentimentConfig)


# ─── INTELLIGENCE CONFIGS ───────────────────────────────────────


class SemanticDiffConfig(BaseModel):
    """Semantic diff engine configuration."""
    enabled: bool = True
    check_interval_hours: int = 24
    min_embedding_drift: float = 0.15
    tracked_url_patterns: list[str] = Field(default_factory=list)


class RedTeamConfig(BaseModel):
    """Red team adversarial challenge configuration."""
    enabled: bool = True
    min_signal_priority_to_challenge: str = "medium"
    challenge_timeout_seconds: int = 30
    counterevidence_search_limit: int = 20
    alternative_explanations_count: int = 3
    base_rate_lookback_days: int = 90
    false_positive_similarity_threshold: float = 0.8
    source_reliability_weight: float = 0.3
    challenge_weight: float = 0.7
    survival_threshold: float = 0.3


class ImmuneConfig(BaseModel):
    """Adaptive immune exploration configuration."""
    enabled: bool = True
    population_size: int = 1000
    mutation_sigma: float = 0.05
    elite_fraction: float = 0.10
    cull_fraction: float = 0.20
    activation_threshold: float = 0.6
    evolution_interval_hours: int = 24
    min_diversity: float = 0.3
    diversity_injection_count: int = 50


class IntelligenceConfig(BaseModel):
    """Intelligence layer configuration."""
    optimization_cycle_hours: int = 24
    max_experiments_per_cycle: int = 10
    exploration_rate: float = 0.15
    bandit_algorithm: str = "ucb1"
    reward_decay: float = 0.99
    strategy_llm_model: str = "qwen3.5:27b"
    strategy_llm_temperature: float = 0.7
    semantic_diff: SemanticDiffConfig = Field(default_factory=SemanticDiffConfig)
    red_team: RedTeamConfig = Field(default_factory=RedTeamConfig)
    immune: ImmuneConfig = Field(default_factory=ImmuneConfig)


# ─── INTERFACE CONFIGS ──────────────────────────────────────────


class EmailConfig(BaseModel):
    """Email alert configuration."""
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    from_address: str = ""
    to_addresses: list[str] = Field(default_factory=list)


class SlackConfig(BaseModel):
    """Slack alert configuration."""
    webhook_url: str = ""


class TelegramConfig(BaseModel):
    """Telegram alert configuration."""
    bot_token: str = ""
    chat_id: str = ""


class AlertsConfig(BaseModel):
    """Alert system configuration."""
    enabled: bool = True
    channels: list[str] = Field(default_factory=lambda: ["desktop", "email"])
    min_priority: str = "medium"
    digest_interval_hours: int = 6
    desktop_sound: bool = True
    max_alerts_per_hour: int = 10
    email: EmailConfig = Field(default_factory=EmailConfig)
    slack: SlackConfig = Field(default_factory=SlackConfig)
    telegram: TelegramConfig = Field(default_factory=TelegramConfig)


class ReportsConfig(BaseModel):
    """Report generation configuration."""
    auto_generate: bool = True
    frequency: str = "daily"
    format: str = "markdown"
    include_graph_viz: bool = True
    max_signals_per_report: int = 50


class InterfaceConfig(BaseModel):
    """Interface layer configuration."""
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_workers: int = 2
    enable_cors: bool = True
    cors_origins: list[str] = Field(default_factory=lambda: ["http://localhost:3000"])
    alerts: AlertsConfig = Field(default_factory=AlertsConfig)
    reports: ReportsConfig = Field(default_factory=ReportsConfig)


# ─── SYSTEM CONFIG ──────────────────────────────────────────────


class SystemConfig(BaseModel):
    """Top-level system configuration."""
    name: str = "sentinel"
    version: str = "1.0.0"
    log_level: str = "INFO"
    data_dir: str = "./data"
    max_workers: int = 8
    timezone: str = "Europe/Berlin"

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level."""
        valid = {"DEBUG", "INFO", "WARNING", "ERROR"}
        if v.upper() not in valid:
            raise ValueError(f"log_level must be one of {valid}")
        return v.upper()


# ─── MAIN SETTINGS CLASS ────────────────────────────────────────


class SentinelConfig(BaseSettings):
    """
    Master configuration for SENTINEL.

    Reads from sentinel.toml file, with environment variable
    overrides using SENTINEL_ prefix.
    """

    system: SystemConfig = Field(default_factory=SystemConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    neo4j: Neo4jConfig = Field(default_factory=Neo4jConfig)
    lancedb: LanceDBConfig = Field(default_factory=LanceDBConfig)
    duckdb: DuckDBConfig = Field(default_factory=DuckDBConfig)
    sqlite: SQLiteConfig = Field(default_factory=SQLiteConfig)
    ingestion: IngestionConfig = Field(default_factory=IngestionConfig)
    stealth: StealthConfig = Field(default_factory=StealthConfig)
    extraction: ExtractionConfig = Field(default_factory=ExtractionConfig)
    knowledge: KnowledgeConfig = Field(default_factory=KnowledgeConfig)
    signals: SignalsConfig = Field(default_factory=SignalsConfig)
    intelligence: IntelligenceConfig = Field(default_factory=IntelligenceConfig)
    interface: InterfaceConfig = Field(default_factory=InterfaceConfig)

    model_config = {
        "env_prefix": "SENTINEL_",
        "env_nested_delimiter": "__",
    }


def _find_config_file() -> Optional[Path]:
    """Search for sentinel.toml in current dir and parents."""
    current = Path.cwd()
    for directory in [current, *current.parents]:
        candidate = directory / "sentinel.toml"
        if candidate.exists():
            return candidate
    return None


def load_config(config_path: Optional[str] = None) -> SentinelConfig:
    """
    Load SENTINEL configuration from sentinel.toml file.

    Args:
        config_path: Optional explicit path to sentinel.toml.
                     If None, searches current directory and parents.

    Returns:
        Fully validated SentinelConfig instance.
    """
    if config_path:
        path = Path(config_path)
    else:
        path = _find_config_file()

    if path and path.exists():
        with open(path, "rb") as f:
            toml_data = tomllib.load(f)
        return SentinelConfig(**toml_data)

    # Return defaults if no config file found
    return SentinelConfig()


# Module-level singleton (lazy)
_config: Optional[SentinelConfig] = None


def get_config() -> SentinelConfig:
    """Get the global configuration singleton."""
    global _config
    if _config is None:
        _config = load_config()
    return _config


def reset_config() -> None:
    """Reset the global configuration singleton (for testing)."""
    global _config
    _config = None
