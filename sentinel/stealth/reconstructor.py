"""
SENTINEL Content Reconstructor.
Merge partial fragments from multiple sources via LLM when no strategy gets full content.
"""
from __future__ import annotations

from typing import Optional

import httpx
import orjson
import structlog

from sentinel.config import get_config
from sentinel.models import AcquisitionResult

logger = structlog.get_logger(__name__)

RECONSTRUCTION_PROMPT = """You are given fragments from different sources about the same web page. 
These fragments come from: Google Cache snippets, Wayback Machine summaries, RSS descriptions, social media quotes, and news aggregators.

Reconstruct the likely full content of the original page. For each paragraph you write, rate your confidence 0.0-1.0 based on how well-supported it is by fragments.

Respond ONLY with valid JSON:
{{
  "title": "reconstructed title",
  "content": "the reconstructed full text content",
  "paragraph_confidences": [0.8, 0.6, ...],
  "overall_confidence": 0.7,
  "sources_used": ["google_cache", "wayback", ...]
}}

Fragments:
{fragments}"""


class ContentReconstructor:
    """
    Reconstruct content from partial fragments when no direct acquisition succeeds.

    Uses LLM to merge 3+ fragments from different sources into coherent content.
    Marks result as reconstructed with confidence scoring.
    """

    def __init__(self) -> None:
        self._config = get_config()

    async def reconstruct(
        self, url: str, fragments: dict[str, str]
    ) -> Optional[AcquisitionResult]:
        """
        Attempt content reconstruction from partial fragments.

        Args:
            url: Original URL.
            fragments: Dict of {source_name: fragment_text}.

        Returns:
            AcquisitionResult with reconstructed content, or None if insufficient fragments.
        """
        config = self._config.stealth.annihilator
        min_fragments = config.reconstruction_min_fragments
        min_confidence = config.reconstruction_min_confidence

        # Filter out empty/tiny fragments
        valid_fragments = {
            source: text for source, text in fragments.items()
            if text and len(text.strip()) > 30
        }

        if len(valid_fragments) < min_fragments:
            logger.debug(
                "reconstruction_insufficient_fragments",
                url=url[:80],
                fragment_count=len(valid_fragments),
                required=min_fragments,
            )
            return None

        # Build fragment text for LLM
        fragment_text = "\n\n".join(
            f"--- Source: {source} ---\n{text[:1000]}"
            for source, text in valid_fragments.items()
        )

        prompt = RECONSTRUCTION_PROMPT.format(fragments=fragment_text)

        try:
            result = await self._call_llm(prompt)
            if result is None:
                return None

            confidence = result.get("overall_confidence", 0.0)
            content = result.get("content", "")

            if confidence < min_confidence:
                logger.debug(
                    "reconstruction_low_confidence",
                    url=url[:80],
                    confidence=confidence,
                    threshold=min_confidence,
                )
                return None

            if not content or len(content) < 100:
                return None

            title = result.get("title", "")
            sources_used = result.get("sources_used", list(valid_fragments.keys()))

            logger.info(
                "content_reconstructed",
                url=url[:80],
                confidence=round(confidence, 2),
                fragments_used=len(valid_fragments),
                content_length=len(content),
            )

            return AcquisitionResult(
                success=True,
                strategy_used="reconstruction",
                strategies_attempted=len(valid_fragments),
                content=f"{title}\n\n{content}" if title else content,
                is_reconstructed=True,
                reconstruction_confidence=confidence,
                fragments_used=sources_used,
                domain_profile_updated=False,
            )

        except Exception as e:
            logger.error("reconstruction_failed", url=url[:80], error=str(e))
            return None

    async def _call_llm(self, prompt: str) -> Optional[dict]:
        """Call Ollama LLM for reconstruction."""
        config = self._config.extraction
        api_url = f"{config.llm_base_url.rstrip('/')}/v1/chat/completions"

        try:
            async with httpx.AsyncClient(timeout=config.llm_timeout_seconds) as client:
                resp = await client.post(
                    api_url,
                    json={
                        "model": config.llm_model,
                        "messages": [
                            {"role": "system", "content": "You are a precise content reconstructor. Respond ONLY with valid JSON."},
                            {"role": "user", "content": prompt},
                        ],
                        "temperature": 0.3,
                        "max_tokens": 4096,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                content = data["choices"][0]["message"]["content"]

                # Parse JSON
                content = content.strip()
                if content.startswith("```"):
                    lines = content.split("\n")
                    content = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])

                return orjson.loads(content)

        except Exception as e:
            logger.error("reconstruction_llm_failed", error=str(e))
            return None
