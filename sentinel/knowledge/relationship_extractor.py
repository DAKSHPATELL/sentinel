"""
SENTINEL Relationship Extractor.
Uses LLM to extract typed relationships between entities from text.
Builds the edges of the knowledge graph.

Enhanced with:
- Entity-aware prompting (tells LLM which entities to look for)
- Relationship type guidance to reduce bias
- Lenient entity matching with canonical ID resolution
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

import httpx
import orjson
import structlog

from sentinel.config import get_config
from sentinel.constants import RELATIONSHIP_TYPES
from sentinel.models import GraphEdge

logger = structlog.get_logger(__name__)

RELATIONSHIP_PROMPT = """Extract knowledge graph relationships from text.

ENTITIES (use these exact names):
{entity_list}

RELATIONSHIP TYPES (ordered by priority — prefer types at top):
  PRODUCES: org/person CREATED or DEVELOPED a product/technology (e.g. "Google developed TensorFlow")
  COMPETES_WITH: direct competitors in same market
  FOUNDED_BY: person founded organization
  ACQUIRED: org acquired/bought another org
  PARTNERS_WITH: formal collaboration, alliance, integration
  WORKS_AT: person employed at org
  EMPLOYS: org employs person
  INVESTS_IN: investor funds org/startup
  RAISED_FUNDING: org raised money/funding round
  AUTHORED: person wrote paper/article/book
  INVENTED: person/org created a NEW technology or concept
  FILED_PATENT: entity filed a patent
  SUBSIDIARY_OF: child company of parent
  PARENT_OF: parent company of child
  LOCATED_IN: entity headquartered/based in location
  ENABLES: technology makes something possible (tech → capability)
  SUPERSEDES: replaces/succeeds older version
  IMPLEMENTS: implements standard/protocol/specification
  CAUSED_BY: effect caused by cause
  PARTICIPATES_IN: entity participates in event
  USES_TECHNOLOGY: ONLY when org is explicitly described as a USER/CUSTOMER of a technology (NOT the creator)

CRITICAL RULES:
1. NEVER use RELATED_TO — always pick a specific type above
2. USES_TECHNOLOGY is ONLY for user/customer relationships. If the org BUILT it, use PRODUCES. If they COMPETE, use COMPETES_WITH. Maximum 1 USES_TECHNOLOGY per extraction.
3. Source and target MUST be different entities
4. Use entity names EXACTLY as listed above
5. Only extract relationships clearly stated in the text
6. Prefer PRODUCES over USES_TECHNOLOGY — "X built Y" or "X released Y" = PRODUCES
7. Prefer COMPETES_WITH when two similar products/orgs are mentioned together

TEXT:
{text}

Return JSON only:
{{
  "relationships": [
    {{"source": "Entity A", "target": "Entity B", "relationship": "TYPE", "evidence": "quote"}}
  ]
}}"""


class RelationshipExtractor:
    """
    Extracts typed relationships between entities using LLM.

    Given a text containing multiple entities, asks the LLM to identify
    relationships like ACQUIRES, EMPLOYS, COMPETES_WITH, etc.
    """

    def __init__(self) -> None:
        self._config = get_config().extraction
        self._kg_config = get_config().knowledge
        self._max_rels = self._kg_config.max_relationships_per_extraction

    async def extract(
        self,
        text: str,
        entities: list[str],
        url: str = "",
        entity_id_map: dict[str, str] | None = None,
    ) -> list[GraphEdge]:
        """
        Extract relationships from text.

        Args:
            text: Content text to analyze.
            entities: Known entities found in the text.
            url: Source URL for evidence tracking.
            entity_id_map: Mapping from entity name (lowercase) to canonical node ID.

        Returns:
            List of GraphEdge objects representing relationships.
        """
        if len(entities) < 2:
            return []

        # Truncate text to keep within LLM context
        text_truncated = text[:6000]

        # Build entity list for prompt (show the LLM exactly which entities to use)
        entity_list = "\n".join(f"  - {e}" for e in entities[:30])

        prompt = RELATIONSHIP_PROMPT.format(
            entity_list=entity_list,
            text=text_truncated,
        )

        try:
            async with httpx.AsyncClient(timeout=self._config.llm_timeout_seconds) as client:
                resp = await client.post(
                    f"{self._config.llm_base_url.rstrip('/')}/v1/chat/completions",
                    json={
                        "model": self._config.llm_model,
                        "messages": [
                            {"role": "system", "content": "You are a precise knowledge graph extraction engine. Output ONLY valid JSON."},
                            {"role": "user", "content": prompt},
                        ],
                        "temperature": 0.1,
                        "max_tokens": 3000,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                content = data["choices"][0]["message"]["content"].strip()

                # Strip qwen3 thinking tags
                import re
                content = re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL).strip()

                # Handle markdown code blocks
                if content.startswith("```"):
                    lines = content.split("\n")
                    content = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])

                # Extract JSON object even if there's trailing garbage
                # Find the outermost { ... } block
                brace_start = content.find("{")
                if brace_start >= 0:
                    depth = 0
                    brace_end = brace_start
                    for i in range(brace_start, len(content)):
                        if content[i] == "{":
                            depth += 1
                        elif content[i] == "}":
                            depth -= 1
                            if depth == 0:
                                brace_end = i + 1
                                break
                    content = content[brace_start:brace_end]

                result = orjson.loads(content)
                relationships = result.get("relationships", [])
                logger.info("relationship_llm_response",
                            rel_count=len(relationships),
                            types=[r.get("relationship", "?") for r in relationships[:10]],
                            url=url[:60])

        except Exception as e:
            logger.error("relationship_extraction_failed",
                         error=str(e), url=url[:80])
            return []

        # Convert to GraphEdge objects
        edges: list[GraphEdge] = []
        # Build lookup sets for matching
        entity_set = set(e.lower() for e in entities)
        # Also build the id_map lookup for lenient matching
        id_map = entity_id_map or {}

        for rel in relationships[: self._max_rels]:
            source = rel.get("source", "").strip()
            target = rel.get("target", "").strip()
            rel_type = rel.get("relationship", "").strip().upper()

            if not source or not target or source.lower() == target.lower():
                continue
            # Reject placeholder/example entity names from the LLM
            placeholder = {"entity a", "entity b", "entity name", "entity_a", "entity_b",
                           "source", "target", "example", "company a", "company b"}
            if source.lower() in placeholder or target.lower() in placeholder:
                continue

            # Validate relationship type — reject RELATED_TO, force specific types
            if not rel_type or rel_type not in RELATIONSHIP_TYPES or rel_type == "RELATED_TO":
                # Try to infer from common patterns
                evidence = rel.get("evidence", "").lower()
                if any(w in evidence for w in ("founded", "started", "created by")):
                    rel_type = "FOUNDED_BY"
                elif any(w in evidence for w in ("acquired", "bought", "merger")):
                    rel_type = "ACQUIRED"
                elif any(w in evidence for w in ("compet", "rival")):
                    rel_type = "COMPETES_WITH"
                elif any(w in evidence for w in ("partner", "collaborat", "alliance")):
                    rel_type = "PARTNERS_WITH"
                elif any(w in evidence for w in ("works at", "employed", "joined", "hired")):
                    rel_type = "WORKS_AT"
                elif any(w in evidence for w in ("located", "based in", "headquarter")):
                    rel_type = "LOCATED_IN"
                elif any(w in evidence for w in ("produces", "makes", "develops", "builds")):
                    rel_type = "PRODUCES"
                elif any(w in evidence for w in ("invest", "funded", "raised", "funding")):
                    rel_type = "RAISED_FUNDING"
                else:
                    # Still can't determine — skip this edge rather than add noise
                    continue

            # Resolve source and target to canonical node IDs
            source_id = self._resolve_entity(source, entity_set, id_map)
            target_id = self._resolve_entity(target, entity_set, id_map)

            if not source_id or not target_id:
                continue

            edge = GraphEdge(
                source_node_id=source_id,
                target_node_id=target_id,
                relationship_type=rel_type,
                evidence_urls=[url] if url else [],
                properties={"evidence": rel.get("evidence", "")},
            )
            edges.append(edge)

        # Post-processing: hard cap on USES_TECHNOLOGY (max 1 per extraction)
        uses_tech_count = sum(1 for e in edges if e.relationship_type == "USES_TECHNOLOGY")
        if uses_tech_count > 1:
            kept = 0
            filtered = []
            for e in edges:
                if e.relationship_type == "USES_TECHNOLOGY":
                    if kept < 1:
                        kept += 1
                        filtered.append(e)
                    # else: drop excess USES_TECHNOLOGY
                else:
                    filtered.append(e)
            edges = filtered

        logger.info("relationships_extracted",
                    count=len(edges), entities=len(entities),
                    types=[e.relationship_type for e in edges],
                    url=url[:60])
        return edges

    def _resolve_entity(self, name: str, entity_set: set[str], id_map: dict[str, str]) -> str | None:
        """
        Resolve an entity name to its canonical node ID.

        Matching strategy (in order):
        1. Exact match in id_map
        2. Exact match in entity_set → generate ID
        3. Substring match in id_map keys
        4. Substring match in entity_set
        """
        name_lower = name.lower().strip()

        # 1. Direct id_map lookup
        if name_lower in id_map:
            return id_map[name_lower]

        # 2. Direct entity_set match
        if name_lower in entity_set:
            return f"entity:{name_lower.replace(' ', '_')}"

        # 3. Substring match against id_map keys (lenient)
        best_match = None
        best_len = 0
        for key, node_id in id_map.items():
            if name_lower in key or key in name_lower:
                if len(key) > best_len:  # Prefer longer (more specific) matches
                    best_match = node_id
                    best_len = len(key)
        if best_match:
            return best_match

        # 4. Substring match against entity_set
        for e in entity_set:
            if name_lower in e or e in name_lower:
                if e in id_map:
                    return id_map[e]
                return f"entity:{e.replace(' ', '_')}"

        # 5. No match — create an ad-hoc node ID from the name
        # This ensures we don't lose edges just because NER missed an entity
        # The graph builder will create the node automatically on upsert
        return f"entity:{name_lower.replace(' ', '_')}"
