"""
SENTINEL LLM extraction.
Uses Ollama (OpenAI-compatible API) for structured content extraction.
"""
from __future__ import annotations

import hashlib
from typing import Any, Optional

import httpx
import orjson
import structlog

from sentinel.config import get_config

logger = structlog.get_logger(__name__)

SYSTEM_PROMPT = (
    "You are a precise information extractor. Given web page content, "
    "extract structured data. Respond ONLY with valid JSON, no other text."
)

USER_PROMPT_TEMPLATE = """Extract the following from this web page content:
{{
  "title": "Page title",
  "summary": "2-3 sentence summary of the key information",
  "topics": ["list of 1-5 topic categories"],
  "key_facts": ["list of important factual claims"],
  "entities": [{{"name": "entity name", "type": "organization|person|technology|product|location|event|concept", "context": "brief context"}}],
  "published_date": "ISO date if found, null otherwise",
  "sentiment": "positive|negative|neutral|mixed"
}}

Content:
{content}"""


class LLMExtractor:
    """
    LLM-based structured content extractor.

    Uses Ollama's OpenAI-compatible API for extraction.
    Caches results by content SHA-256 hash.
    Falls back to rule-based extraction on failure.
    """

    def __init__(self) -> None:
        """Initialize LLM extractor."""
        self._config = get_config()
        self._cache: dict[str, dict] = {}

    def _get_api_url(self) -> str:
        """Get the Ollama API URL."""
        base = self._config.extraction.llm_base_url.rstrip("/")
        return f"{base}/v1/chat/completions"

    async def extract(self, text: str, url: str = "") -> dict[str, Any]:
        """
        Extract structured data from text using LLM.

        Args:
            text: Cleaned text content.
            url: Source URL (for logging).

        Returns:
            Extracted structured data dictionary.
        """
        if not text or len(text.strip()) < 50:
            return self._empty_result()

        # Check cache
        content_hash = hashlib.sha256(text.encode()).hexdigest()
        if content_hash in self._cache:
            logger.debug("llm_cache_hit", url=url[:80])
            return self._cache[content_hash]

        # Truncate to ~4000 chars for LLM context window
        truncated = text[:4000]

        # Attempt LLM extraction
        result = await self._call_llm(truncated, url)

        if result is None:
            # Retry with explicit JSON instruction
            result = await self._call_llm(
                truncated, url,
                extra_instruction="You MUST respond ONLY with valid JSON. No markdown, no explanations."
            )

        if result is None:
            # Fall back to rule-based
            logger.warning("llm_extraction_failed_fallback", url=url[:80])
            result = self._rule_based_extract(text)

        # Cache result
        self._cache[content_hash] = result

        # Trim cache if too large
        if len(self._cache) > 10000:
            # Remove oldest half
            keys = list(self._cache.keys())
            for k in keys[: len(keys) // 2]:
                del self._cache[k]

        return result

    async def _call_llm(
        self, text: str, url: str, extra_instruction: str = ""
    ) -> Optional[dict[str, Any]]:
        """Call the LLM API and parse JSON response."""
        config = self._config.extraction
        prompt = USER_PROMPT_TEMPLATE.format(content=text)
        if extra_instruction:
            prompt = f"{extra_instruction}\n\n{prompt}"

        try:
            async with httpx.AsyncClient(timeout=config.llm_timeout_seconds) as client:
                resp = await client.post(
                    self._get_api_url(),
                    json={
                        "model": config.llm_model,
                        "messages": [
                            {"role": "system", "content": SYSTEM_PROMPT},
                            {"role": "user", "content": prompt},
                        ],
                        "temperature": config.llm_temperature,
                        "max_tokens": config.llm_max_tokens,
                    },
                )
                resp.raise_for_status()
                data = resp.json()

                content = data["choices"][0]["message"]["content"]

                # Try to parse JSON from response
                return self._parse_json_response(content)

        except httpx.TimeoutException:
            logger.warning("llm_timeout", url=url[:80])
            return None
        except Exception as e:
            logger.error("llm_call_failed", url=url[:80], error=str(e))
            return None

    def _parse_json_response(self, content: str) -> Optional[dict]:
        """Parse JSON from LLM response, handling markdown wrapping."""
        # Strip markdown code fences if present
        content = content.strip()
        if content.startswith("```"):
            lines = content.split("\n")
            content = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])

        try:
            return orjson.loads(content)
        except (orjson.JSONDecodeError, ValueError):
            # Try to find JSON in the response
            import re
            json_match = re.search(r"\{.*\}", content, re.DOTALL)
            if json_match:
                try:
                    return orjson.loads(json_match.group())
                except (orjson.JSONDecodeError, ValueError):
                    pass
        return None

    def _rule_based_extract(self, text: str) -> dict[str, Any]:
        """Fallback rule-based extraction."""
        lines = text.split("\n")
        title = lines[0][:200] if lines else ""
        summary = " ".join(lines[:3])[:500] if lines else ""

        return {
            "title": title,
            "summary": summary,
            "topics": [],
            "key_facts": [],
            "entities": [],
            "published_date": None,
            "sentiment": "neutral",
        }

    def _empty_result(self) -> dict[str, Any]:
        """Return empty extraction result."""
        return {
            "title": "",
            "summary": "",
            "topics": [],
            "key_facts": [],
            "entities": [],
            "published_date": None,
            "sentiment": "neutral",
        }
