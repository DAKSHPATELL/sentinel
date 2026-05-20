"""
SENTINEL HyDE Query Engine.
Hypothetical Document Embeddings for semantic search enhancement.
"""
from __future__ import annotations

import httpx
import structlog

from sentinel.config import get_config
from sentinel.extraction.embedder import Embedder

logger = structlog.get_logger(__name__)

HYDE_PROMPT = """You are an expert analyst. 
Please write a highly detailed, hypothetical web page or news article that directly answers or provides evidence for the following query.
Do NOT explain that it is hypothetical. Write exactly as if it were a real, factual document containing the target information.
Include relevant keywords, entities, and specialized terminology that would likely appear in such a document.

Query: {query}"""


class HydeEngine:
    """
    Implements Hypothetical Document Embeddings (HyDE) to bridge 
    the vocabulary gap between short queries and long documents.
    """

    def __init__(self, embedder: Embedder) -> None:
        self._embedder = embedder
        self._config = get_config()

    async def generate_hypothetical_document(self, query: str) -> str:
        """
        Generate a hypothetical document answering the query using LLM.

        Args:
            query: The search query or signal description.

        Returns:
            Generated hypothetical document text.
        """
        config = self._config.extraction
        try:
            async with httpx.AsyncClient(timeout=config.llm_timeout_seconds) as client:
                resp = await client.post(
                    f"{config.llm_base_url.rstrip('/')}/v1/chat/completions",
                    json={
                        "model": config.llm_model,
                        "messages": [
                            {"role": "system", "content": "You are a specialized content generator."},
                            {"role": "user", "content": HYDE_PROMPT.format(query=query)},
                        ],
                        "temperature": 0.5,
                        "max_tokens": 512,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                hypothetical_doc = data["choices"][0]["message"]["content"].strip()
                return hypothetical_doc
        except Exception as e:
            logger.error("hyde_generation_failed", error=str(e))
            return query  # Fallback to the original query

    async def embed_query(self, query: str) -> list[float]:
        """
        Generate a HyDE embedding block.

        1. Generate a hypothetical document.
        2. Embed the hypothetical document.
        3. Average with original query embedding (optional, we'll just embed the hypo doc).

        Args:
            query: Short query or concept.

        Returns:
            Embedding vector.
        """
        hypo_doc = await self.generate_hypothetical_document(query)
        logger.debug("hyde_generated_doc", query_len=len(query), doc_len=len(hypo_doc))
        return self._embedder.embed_text(hypo_doc)
