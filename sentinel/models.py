"""
SENTINEL data models.
All shared Pydantic v2 data models used across the system.
Every component communicates using these types.
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


# ─── ENUMS ──────────────────────────────────────────────────────


class SourceType(str, Enum):
    """Source types for content ingestion."""
    HACKERNEWS = "hackernews"
    GITHUB = "github"
    ARXIV = "arxiv"
    PATENT = "patent"
    SEC_FILING = "sec_filing"
    REDDIT = "reddit"
    TWITTER = "twitter"
    RSS = "rss"
    WEB_CRAWL = "web_crawl"
    PRODUCT_HUNT = "product_hunt"
    CRUNCHBASE = "crunchbase"
    CT_MONITOR = "ct_monitor"
    GOOGLE_ALERTS = "google_alerts"
    COMMON_CRAWL = "common_crawl"
    CUSTOM = "custom"


class ContentType(str, Enum):
    """Types of content extracted from sources."""
    ARTICLE = "article"
    PAPER = "paper"
    CODE_REPO = "code_repo"
    PATENT_DOC = "patent_doc"
    FILING = "filing"
    DISCUSSION = "discussion"
    SOCIAL_POST = "social_post"
    PRODUCT_LAUNCH = "product_launch"
    PRESS_RELEASE = "press_release"
    JOB_POSTING = "job_posting"
    OTHER = "other"


class CrawlStatus(str, Enum):
    """Status of a URL in the crawl frontier."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    BLOCKED = "blocked"
    ROBOTS_DENIED = "robots_denied"
    DUPLICATE = "duplicate"


class AlertPriority(str, Enum):
    """Priority levels for signals and alerts."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class SignalType(str, Enum):
    """Types of detected signals."""
    ANOMALY = "anomaly"
    BURST = "burst"
    CASCADE = "cascade"
    NOVELTY = "novelty"
    SENTIMENT_SHIFT = "sentiment_shift"
    CROSS_CORRELATION = "cross_correlation"
    SEMANTIC_CHANGE = "semantic_change"
    IMMUNE_DISCOVERY = "immune_discovery"


class EntityType(str, Enum):
    """Types of named entities."""
    ORGANIZATION = "organization"
    PERSON = "person"
    TECHNOLOGY = "technology"
    PRODUCT = "product"
    LOCATION = "location"
    EVENT = "event"
    CONCEPT = "concept"
    FUNDING_ROUND = "funding_round"
    REGULATION = "regulation"


# ─── SOURCE MODELS ──────────────────────────────────────────────


class Source(BaseModel):
    """Registered data source."""
    id: UUID = Field(default_factory=uuid4)
    name: str
    source_type: SourceType
    url: Optional[str] = None
    config: dict = Field(default_factory=dict)
    enabled: bool = True
    priority: float = Field(default=1.0, ge=0.0, le=10.0)
    last_checked: Optional[datetime] = None
    check_interval_seconds: int = 3600
    total_items_collected: int = 0
    avg_signal_yield: float = 0.0
    consecutive_failures: int = 0
    created_at: datetime = Field(default_factory=datetime.utcnow)


# ─── CRAWL MODELS ───────────────────────────────────────────────


class CrawlJob(BaseModel):
    """Single URL to crawl."""
    id: UUID = Field(default_factory=uuid4)
    url: str
    source_id: Optional[UUID] = None
    priority: float = Field(default=1.0, ge=0.0)
    depth: int = Field(default=0, ge=0)
    parent_url: Optional[str] = None
    status: CrawlStatus = CrawlStatus.PENDING
    attempt_count: int = 0
    max_attempts: int = 3
    created_at: datetime = Field(default_factory=datetime.utcnow)
    scheduled_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    stealth_profile: Optional[str] = None


class CrawlResult(BaseModel):
    """Result of crawling a single URL."""
    job_id: UUID
    url: str
    status_code: int
    content_type: str
    content_hash: str
    raw_html_path: Optional[str] = None
    cleaned_text: Optional[str] = None
    text_length: int = 0
    title: Optional[str] = None
    language: Optional[str] = None
    outgoing_links: list[str] = Field(default_factory=list)
    download_time_ms: int = 0
    proxy_used: Optional[str] = None
    blocked: bool = False
    fetched_at: datetime = Field(default_factory=datetime.utcnow)


# ─── CONTENT MODELS ─────────────────────────────────────────────


class ExtractedEntity(BaseModel):
    """Named entity extracted from content."""
    text: str
    entity_type: EntityType
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    canonical_name: Optional[str] = None
    graph_node_id: Optional[str] = None
    metadata: dict = Field(default_factory=dict)


class ExtractedContent(BaseModel):
    """Structured content extracted from a crawl result."""
    id: UUID = Field(default_factory=uuid4)
    crawl_job_id: UUID
    url: str
    source_type: SourceType
    content_type: ContentType
    title: str
    summary: Optional[str] = None
    full_text: str
    structured_data: dict = Field(default_factory=dict)
    entities: list[ExtractedEntity] = Field(default_factory=list)
    topics: list[str] = Field(default_factory=list)
    sentiment_score: Optional[float] = Field(default=None, ge=-1.0, le=1.0)
    relevance_score: float = Field(default=0.0, ge=0.0, le=1.0)
    novelty_score: float = Field(default=0.0, ge=0.0, le=1.0)
    embedding: Optional[list[float]] = None
    language: str = "en"
    word_count: int = 0
    extracted_at: datetime = Field(default_factory=datetime.utcnow)
    published_at: Optional[datetime] = None


# ─── KNOWLEDGE GRAPH MODELS ─────────────────────────────────────


class GraphNode(BaseModel):
    """Entity node in the knowledge graph."""
    id: str
    canonical_name: str
    entity_type: EntityType
    aliases: list[str] = Field(default_factory=list)
    description: Optional[str] = None
    properties: dict = Field(default_factory=dict)
    first_seen: datetime = Field(default_factory=datetime.utcnow)
    last_seen: datetime = Field(default_factory=datetime.utcnow)
    mention_count: int = 1
    source_types: list[SourceType] = Field(default_factory=list)
    community_id: Optional[int] = None
    embedding: Optional[list[float]] = None


class GraphEdge(BaseModel):
    """Relationship edge in the knowledge graph."""
    source_node_id: str
    target_node_id: str
    relationship_type: str
    weight: float = 1.0
    properties: dict = Field(default_factory=dict)
    first_seen: datetime = Field(default_factory=datetime.utcnow)
    last_seen: datetime = Field(default_factory=datetime.utcnow)
    evidence_urls: list[str] = Field(default_factory=list)


# ─── SIGNAL MODELS ──────────────────────────────────────────────


class Signal(BaseModel):
    """Detected signal from any detection method."""
    id: UUID = Field(default_factory=uuid4)
    signal_type: SignalType
    priority: AlertPriority = AlertPriority.MEDIUM
    title: str
    description: str
    entities: list[str] = Field(default_factory=list)
    source_types: list[SourceType] = Field(default_factory=list)
    evidence_urls: list[str] = Field(default_factory=list)
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    metadata: dict = Field(default_factory=dict)
    detected_at: datetime = Field(default_factory=datetime.utcnow)
    acknowledged: bool = False
    useful: Optional[bool] = None


class CascadeEvent(BaseModel):
    """Single event within a cascade."""
    source_type: SourceType
    url: str
    title: str
    timestamp: datetime
    summary: Optional[str] = None


class CascadePattern(BaseModel):
    """Detected cross-domain cascade."""
    id: UUID = Field(default_factory=uuid4)
    entity_name: str
    events: list[CascadeEvent] = Field(default_factory=list)
    span_hours: float
    source_count: int
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    detected_at: datetime = Field(default_factory=datetime.utcnow)


# ─── INTELLIGENCE MODELS ────────────────────────────────────────


class CrawlStrategy(BaseModel):
    """Current crawl strategy configuration (mutable by intelligence layer)."""
    id: UUID = Field(default_factory=uuid4)
    source_weights: dict[str, float] = Field(default_factory=dict)
    topic_priorities: list[str] = Field(default_factory=list)
    relevance_threshold: float = 0.3
    novelty_threshold: float = 0.7
    exploration_rate: float = 0.15
    active_since: datetime = Field(default_factory=datetime.utcnow)
    performance_score: float = 0.0


class Experiment(BaseModel):
    """Single optimization experiment in the AutoResearch loop."""
    id: UUID = Field(default_factory=uuid4)
    strategy_id: UUID
    hypothesis: str
    parameter_changes: dict
    started_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    baseline_score: float
    result_score: Optional[float] = None
    improvement: Optional[float] = None
    accepted: Optional[bool] = None


# ─── ACQUISITION MODELS ─────────────────────────────────────────


class AcquisitionResult(BaseModel):
    """Result of multi-strategy content acquisition."""
    success: bool
    strategy_used: Optional[str] = None
    strategies_attempted: int = 0
    content: Optional[str] = None
    is_reconstructed: bool = False
    reconstruction_confidence: float = 1.0
    fragments_used: list[str] = Field(default_factory=list)
    acquisition_time_ms: int = 0
    domain_profile_updated: bool = False


class ExplorationVector(BaseModel):
    """Immune system exploration vector."""
    id: int
    vector: list[float]
    generation: int = 0
    parent_id: Optional[int] = None
    total_activations: int = 0
    useful_activations: int = 0
    reward: float = 0.0
    created_at: datetime = Field(default_factory=datetime.utcnow)
    last_activated: Optional[datetime] = None


class PredictionAction(BaseModel):
    """Action generated from a prediction (e.g., a search query to execute)."""
    action_id: str
    prediction_id: str
    query: str
    source_type: SourceType = SourceType.GOOGLE_ALERTS
    executed_at: Optional[datetime] = None
    discovered_urls: int = 0


class Prediction(BaseModel):
    """Predictive crawler forecast of a future event."""
    prediction_id: str
    event_description: str
    probability: float = Field(default=0.5, ge=0.0, le=1.0)
    timeframe_days: int = 14
    actions: list[PredictionAction] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    status: str = "PENDING"
    resolution_score: Optional[float] = None


class ChallengeResult(BaseModel):
    """Result of red team adversarial challenge on a signal."""
    original_signal_id: UUID
    adjusted_confidence: float
    challenge_score: float = 0.0
    counterevidence_score: float = 0.0
    alternative_explanation_score: float = 0.0
    base_rate_score: float = 0.0
    false_positive_score: float = 0.0
    source_reliability_score: float = 0.0
    survived: bool = True
    kill_reason: Optional[str] = None
    details: dict = Field(default_factory=dict)


class Verdict(BaseModel):
    """Hypothesis court verdict for a signal."""
    id: UUID = Field(default_factory=uuid4)
    signal_id: UUID
    hypothesis: str = "Debated Signal"
    approved: bool = False
    reasoning: str = ""
    advocate_argument: str = ""
    skeptic_argument: str = ""
    final_confidence: float = 0.5
    created_at: datetime = Field(default_factory=datetime.utcnow)


# ─── OPERATIONAL MODELS ─────────────────────────────────────────


class SystemHealth(BaseModel):
    """System health snapshot."""
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    crawl_workers_active: int = 0
    crawl_queue_size: int = 0
    pages_crawled_last_hour: int = 0
    extraction_queue_size: int = 0
    signals_detected_last_24h: int = 0
    graph_node_count: int = 0
    graph_edge_count: int = 0
    lance_table_row_count: int = 0
    redis_memory_mb: float = 0.0
    disk_usage_gb: float = 0.0
    cpu_percent: float = 0.0
    memory_percent: float = 0.0
    errors_last_hour: int = 0


class SourceHealth(BaseModel):
    """Per-source health metrics."""
    source_id: UUID
    source_name: str
    source_type: SourceType
    status: str = "healthy"
    last_successful_check: Optional[datetime] = None
    consecutive_failures: int = 0
    avg_items_per_check: float = 0.0
    avg_signal_yield: float = 0.0
    total_items_collected: int = 0
    blocked: bool = False
