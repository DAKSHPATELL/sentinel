"""
SENTINEL system-wide constants.
"""
from __future__ import annotations

# ─── DEFAULT TOPICS ─────────────────────────────────────────────
# Used for relevance scoring (PRD Section 8.4)

DEFAULT_TOPICS: list[str] = [
    "Breakthrough in artificial intelligence or machine learning technology",
    "New startup founding, product launch, or significant pivot",
    "Major funding round, IPO filing, or acquisition announcement",
    "Regulatory change affecting technology companies in EU, US, or Asia",
    "Scientific discovery with near-term commercial applications",
    "Open-source project gaining rapid community adoption",
    "Patent filing revealing novel technology or business method",
    "Geopolitical event affecting global technology supply chains",
    "Energy technology breakthrough in solar, battery, fusion, or grid",
    "Biotechnology or pharmaceutical breakthrough in development",
    "Cryptocurrency or DeFi protocol gaining unusual traction",
    "Cybersecurity vulnerability, breach, or defense technology",
    "Climate technology or carbon removal innovation",
    "Quantum computing milestone or commercial development",
    "Space technology or satellite constellation deployment",
    "Manufacturing automation or robotics advancement",
    "New government policy on AI, data, or digital infrastructure",
    "Significant leadership change at major technology company",
    "Infrastructure failure or outage revealing systemic risk",
    "Emerging market trend visible only in alternative data",
]

# ─── SEED RSS FEEDS ─────────────────────────────────────────────
# PRD Section 8.1

SEED_RSS_FEEDS: list[str] = [
    # Tech/AI
    "https://news.ycombinator.com/rss",
    "https://www.techmeme.com/feed.xml",
    "https://techcrunch.com/feed/",
    "https://feeds.arstechnica.com/arstechnica/technology-lab",
    "https://www.wired.com/feed/rss",
    "https://www.theverge.com/rss/index.xml",
    "https://blog.google/rss/",
    "https://openai.com/blog/rss.xml",
    "https://www.anthropic.com/feed.xml",
    "https://ai.meta.com/blog/rss/",
    "https://deepmind.google/blog/rss.xml",
    "https://blogs.microsoft.com/ai/feed/",
    # Science
    "https://www.nature.com/nature.rss",
    "https://www.science.org/action/showFeed?type=etoc&feed=rss&jc=science",
    "https://phys.org/rss-feed/",
    "https://newatlas.com/index.rss",
    # Finance / Markets
    "https://feeds.bloomberg.com/markets/news.rss",
    # Crypto
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cointelegraph.com/rss",
    "https://decrypt.co/feed",
    # Energy / Climate
    "https://www.greentechmedia.com/feed",
    "https://cleantechnica.com/feed/",
    "https://www.rechargenews.com/rss",
    # Biotech / Health
    "https://www.fiercebiotech.com/rss",
    "https://www.statnews.com/feed/",
    "https://www.biopharmadive.com/feeds/news/",
    # Startups / VC
    "https://www.saastr.com/feed/",
    "https://www.cbinsights.com/research/feed/",
    "https://a16z.com/feed/",
    "https://www.ycombinator.com/blog/rss/",
    "https://www.nfx.com/essays/rss.xml",
    # Policy / Regulation
    "https://www.eff.org/rss/updates.xml",
    "https://www.euractiv.com/sections/digital/feed/",
    # Space
    "https://spacenews.com/feed/",
    "https://www.nasaspaceflight.com/feed/",
    # Manufacturing / Robotics
    "https://www.therobotreport.com/feed/",
    "https://www.automationworld.com/rss.xml",
]

# ─── RELATIONSHIP TYPES ────────────────────────────────────────

RELATIONSHIP_TYPES: list[str] = [
    "FOUNDED_BY",
    "FOUNDED",
    "ACQUIRED",
    "ACQUIRED_BY",
    "COMPETES_WITH",
    "PARTNERS_WITH",
    "USES_TECHNOLOGY",
    "PRODUCES",
    "FILED_PATENT",
    "RAISED_FUNDING",
    "WORKS_AT",
    "EMPLOYS",
    "INVENTED",
    "AUTHORED",
    "INVESTS_IN",
    "SUBSIDIARY_OF",
    "PARENT_OF",
    "LOCATED_IN",
    "CAUSED_BY",
    "CAUSES",
    "RELATED_TO",
    "ENABLES",
    "SUPERSEDES",
    "IMPLEMENTS",
    "MENTIONS",
    "PARTICIPATES_IN",
]

# ─── ENTITY SUFFIX STRIP LIST ──────────────────────────────────

ENTITY_SUFFIXES: list[str] = [
    "Inc.", "Inc", "Ltd.", "Ltd", "GmbH", "Corp.", "Corp",
    "LLC", "AG", "SE", "Co.", "Co", "Plc", "PLC",
    "S.A.", "S.A", "N.V.", "B.V.",
]

# ─── AD-RELATED CSS PATTERNS ───────────────────────────────────
# Used by html_cleaner to identify ad/junk elements

AD_PATTERNS: list[str] = [
    "ad", "advertisement", "sponsor", "cookie", "consent",
    "popup", "modal", "sidebar", "menu", "nav",
    "banner", "promo", "social-share", "newsletter",
    "subscribe", "signup", "sign-up",
]

# ─── BLOOM FILTER SETTINGS ─────────────────────────────────────

BLOOM_FILTER_CAPACITY: int = 10_000_000
BLOOM_FILTER_ERROR_RATE: float = 0.01
