"""
SENTINEL DOM pruning / HTML cleaner.
Removes navigation, ads, scripts; preserves article content.
"""
from __future__ import annotations

import re
from typing import Optional

import structlog
from bs4 import BeautifulSoup, Comment

from sentinel.config import get_config
from sentinel.constants import AD_PATTERNS

logger = structlog.get_logger(__name__)

# Tags to remove entirely
REMOVE_TAGS = {"nav", "footer", "header", "aside", "script", "style", "noscript", "iframe", "svg", "form"}

# Tags to preserve (content containers)
PRESERVE_TAGS = {"article", "main", "section", "p", "h1", "h2", "h3", "h4", "h5", "h6",
                 "table", "ul", "ol", "li", "blockquote", "pre", "code", "figure", "figcaption"}


def _matches_ad_pattern(element: any) -> bool:
    """Check if an element's class/id matches ad patterns."""
    if not hasattr(element, 'attrs') or element.attrs is None:
        return False
    classes = " ".join(element.get("class", []))
    elem_id = element.get("id", "")
    combined = f"{classes} {elem_id}".lower()
    return any(pattern in combined for pattern in AD_PATTERNS)


def clean(html: str, max_text_length: Optional[int] = None) -> str:
    """
    Clean HTML by removing non-content elements.

    Removes nav, footer, ads, scripts, styles.
    Preserves article, main, section, paragraphs, headings.
    Extracts text with paragraph structure preserved.

    Args:
        html: Raw HTML string.
        max_text_length: Maximum character length (default from config).

    Returns:
        Cleaned text with paragraph structure.
    """
    config = get_config()
    if max_text_length is None:
        max_text_length = config.extraction.dom_pruning.max_text_length

    before_len = len(html)

    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")

    # Remove comments
    for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
        comment.extract()

    # Remove unwanted tags
    for tag_name in REMOVE_TAGS:
        for tag in soup.find_all(tag_name):
            tag.decompose()

    # Remove ad-related elements by class/id
    for element in soup.find_all(True):
        if _matches_ad_pattern(element):
            element.decompose()

    # Try to find main content area
    main_content = (
        soup.find("article")
        or soup.find("main")
        or soup.find("div", {"role": "main"})
        or soup.find("div", class_=re.compile(r"(content|article|post|entry|body)", re.I))
        or soup.body
        or soup
    )

    # Extract text with structure
    blocks = []
    for element in main_content.find_all(
        ["p", "h1", "h2", "h3", "h4", "h5", "h6", "li", "blockquote", "pre", "td", "th"]
    ):
        text = element.get_text(separator=" ", strip=True)
        if text and len(text) > 10:  # Skip very short fragments
            blocks.append(text)

    # If no structured blocks found, fall back to all text
    if not blocks:
        text = main_content.get_text(separator="\n", strip=True)
        blocks = [line.strip() for line in text.split("\n") if line.strip() and len(line.strip()) > 10]

    # Filter out binary/garbage content from blocks
    clean_blocks = []
    for block in blocks:
        # Skip blocks that are mostly non-alphanumeric (binary garbage)
        alnum_ratio = sum(1 for c in block if c.isalnum() or c.isspace()) / max(len(block), 1)
        if alnum_ratio < 0.6:
            continue
        # Skip base64-like content
        if re.search(r'[A-Za-z0-9+/=]{50,}', block):
            continue
        # Skip blocks with high ratio of non-printable/replacement chars
        if any(ord(c) > 0xFFF0 or (0x80 <= ord(c) <= 0x9F) for c in block[:200]):
            continue
        clean_blocks.append(block)

    result = "\n\n".join(clean_blocks)

    # Truncate if needed
    if len(result) > max_text_length:
        result = result[:max_text_length]

    after_len = len(result)
    reduction = round((1 - after_len / max(before_len, 1)) * 100, 1)

    logger.debug(
        "dom_pruning_completed",
        before_chars=before_len,
        after_chars=after_len,
        reduction_pct=reduction,
        blocks=len(blocks),
    )

    return result


def extract_title(html: str) -> Optional[str]:
    """Extract title from HTML."""
    try:
        soup = BeautifulSoup(html[:10000], "lxml")
        # Try og:title first
        og_title = soup.find("meta", property="og:title")
        if og_title and og_title.get("content"):
            return og_title["content"].strip()[:500]
        # Fall back to <title>
        title_tag = soup.find("title")
        if title_tag:
            return title_tag.get_text(strip=True)[:500]
    except Exception:
        pass
    return None
