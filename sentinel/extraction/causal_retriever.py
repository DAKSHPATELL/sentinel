"""
SENTINEL Causal Chain Retrieval Engine.
Multi-hop graph traversal over LanceDB relationship and paragraph vectors.
"""
from __future__ import annotations

import httpx
import orjson
import structlog
from typing import Optional

from sentinel.config import get_config
from sentinel.core.lancedb_client import LanceDBClient
from sentinel.extraction.embedder import Embedder
from sentinel.models import Signal

logger = structlog.get_logger(__name__)

CAUSAL_PROMPT = """You are analyzing relationships to form a causal chain.
Identify if the provided relationships and paragraphs form a coherent causal chain that explains the root cause of the given signal.

If they do form a chain, generate a brief 'causal explanation' and rate its 'causal strength' (0.0 to 1.0).
If they don't logically connect to explain the signal, set causal strength to 0.

Respond ONLY with valid JSON:
{{
  "chain_valid": true,
  "causal_explanation": "Entity A caused Event B, which led to the observed Signal C.",
  "causal_strength": 0.85
}}

Signal:
{signal}

Retrieved Graph Relationships:
{relationships}

Retrieved Context Paragraphs:
{paragraphs}"""


class CausalRetriever:
    """
    Performs multi-hop retrieval to explain a signal's root cause.

    1. Starts with signal entities.
    2. Queries LanceDB relationship_embeddings for multi-hop neighborhood.
    3. Queries LanceDB paragraph_embeddings for contextual evidence.
    4. Passes graph sub-graph to LLM to evaluate causal chain strength.
    """

    def __init__(self, lancedb_client: LanceDBClient, embedder: Embedder) -> None:
        self._lancedb = lancedb_client
        self._embedder = embedder
        self._config = get_config()

    async def retrieve_causal_chain(self, signal: Signal, max_hops: int = 2) -> Optional[dict]:
        """
        Find a causal explanation for a signal.

        Args:
            signal: The signal to explain.
            max_hops: Max traversal depth in relationship graph.

        Returns:
            Dict containing explanation and strength, or None.
        """
        if not signal.entities:
            return None

        # 1. Multi-hop relationship traversal
        visited_nodes = set(signal.entities)
        current_frontier = set(signal.entities)
        all_relationships = []

        for hop in range(max_hops):
            next_frontier = set()
            for entity in current_frontier:
                # Target relationships where entity is source or target
                try:
                    # Search by vector similarity to entity name
                    entity_vec = self._embedder.embed_text(entity)
                    rels = await self._lancedb.search(
                        "relationship_embeddings",
                        query_vector=entity_vec,
                        limit=5,
                    )
                    
                    for r in rels:
                        # Only accept strong matches
                        if (1 - r.get("_distance", 1.0)) > 0.6:
                            text = r.get("text", "")
                            if text and text not in [ar["text"] for ar in all_relationships]:
                                all_relationships.append(r)
                                
                                src = r.get("source_id", "")
                                tgt = r.get("target_id", "")
                                if src and src not in visited_nodes:
                                    next_frontier.add(src)
                                    visited_nodes.add(src)
                                if tgt and tgt not in visited_nodes:
                                    next_frontier.add(tgt)
                                    visited_nodes.add(tgt)
                except Exception as e:
                    logger.debug("causal_hop_failed", entity=entity, error=str(e))

            if not next_frontier:
                break
            current_frontier = next_frontier

        # 2. Paragraph context retrieval
        signal_vec = self._embedder.embed_text(signal.description)
        paragraphs = []
        try:
            paras = await self._lancedb.search(
                "paragraph_embeddings",
                query_vector=signal_vec,
                limit=5,
            )
            for p in paras:
                if (1 - p.get("_distance", 1.0)) > 0.6:
                    paragraphs.append(p.get("text", ""))
        except Exception as e:
            logger.debug("causal_paragraph_search_failed", error=str(e))

        if not all_relationships and not paragraphs:
            return None

        # 3. LLM Causal Evaluation
        signal_desc = f"{signal.signal_type.value}: {signal.description}"
        rel_text = "\n".join(f"- {r.get('text', '')}" for r in all_relationships)
        para_text = "\n".join(f"- {p}" for p in paragraphs)

        prompt = CAUSAL_PROMPT.format(
            signal=signal_desc,
            relationships=rel_text or "None found.",
            paragraphs=para_text or "None found.",
        )

        try:
            config = self._config.extraction
            async with httpx.AsyncClient(timeout=config.llm_timeout_seconds) as client:
                resp = await client.post(
                    f"{config.llm_base_url.rstrip('/')}/v1/chat/completions",
                    json={
                        "model": config.llm_model,
                        "messages": [
                            {"role": "system", "content": "You are a causal reasoning engine. Respond ONLY with valid JSON."},
                            {"role": "user", "content": prompt},
                        ],
                        "temperature": 0.3,
                        "max_tokens": 512,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                content = data["choices"][0]["message"]["content"].strip()

                if content.startswith("```"):
                    lines = content.split("\n")
                    content = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])

                result = orjson.loads(content)

                if result.get("chain_valid") and result.get("causal_strength", 0.0) > 0.6:
                    logger.info(
                        "causal_chain_found",
                        strength=result.get("causal_strength"),
                        signal_id=str(signal.id),
                    )
                    return {
                        "explanation": result.get("causal_explanation"),
                        "strength": result.get("causal_strength"),
                        "hops": len(all_relationships),
                        "evidence": paragraphs,
                    }

        except Exception as e:
            logger.error("causal_evaluation_failed", error=str(e))

        return None
