"""
SENTINEL Causal Intervention & Counterfactual Simulation Engine.
Implements Pearl's Causal Framework (Association, Intervention, Counterfactuals)
on top of the TPE-KG Graph and LanceDB Context Store.
"""
from __future__ import annotations

import httpx
import orjson
import sqlite3
import structlog
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Dict, List

from sentinel.config import get_config
from sentinel.core.lancedb_client import LanceDBClient
from sentinel.extraction.embedder import Embedder

logger = structlog.get_logger(__name__)

SIMULATE_PROMPT = """You are the Causal Simulation Engine of SENTINEL, applying Pearl's Causal Framework.
We are analyzing a detected Signal and simulating an intervention to evaluate counterfactual scenarios.

[Causal Model]
Observed Signal: {signal_title}
Signal Description: {signal_desc}
Observed Cause/Entity: {treatment_node}
Target Outcome/Entity: {outcome_node}

[Structural Graph Context (Nodes & Edges)]
{graph_context}

[Exogenous Background Evidence (LanceDB Context)]
{exogenous_evidence}

[Reasoning Task]
We want to simulate the intervention: do({treatment_node} = {intervention_val})
In this simulated universe, we alter the structural equations by forcing {treatment_node} to be {intervention_val}.

Perform the three steps of Counterfactual Reasoning:
1. ABDUCTION: Use the observed signal and exogenous context to update the background conditions.
2. ACTION: Apply the intervention do({treatment_node} = {intervention_val}). Cut all incoming links to {treatment_node}.
3. PREDICTION: Trace the propagation of this change through the graph. Compute if the target outcome '{outcome_node}' would still occur, and estimate its probability/strength.

Respond ONLY with valid JSON matching this schema:
{{
  "abduction_notes": "Explanation of background conditions and latent factors.",
  "action_adjustments": "Explanation of how the graph equations/relations are altered by cutting incoming paths to the treatment node.",
  "prediction_outcome": "Would the outcome still occur? What is the counterfactual state of the target?",
  "estimated_counterfactual_probability": 0.35, // Float between 0.0 and 1.0
  "causal_necessity": 0.85, // P(Y_x' = y' | X=x, Y=y) - Probability that outcome would not have occurred without the cause.
  "causal_sufficiency": 0.65, // P(Y_x = y | X=x', Y=y') - Probability that introducing cause alone produces outcome.
  "detailed_explanation": "A complete, premium summary explaining the counterfactual simulation result."
}}"""

class CausalSimulator:
    """
    Pearl Causal Simulation Engine.
    Executes do-calculus and counterfactual simulations.
    """

    def __init__(self, lancedb_client: Optional[LanceDBClient] = None, embedder: Optional[Embedder] = None) -> None:
        self._lancedb = lancedb_client
        self._embedder = embedder
        self._config = get_config()
        self._db_path = Path(self._config.system.data_dir) / "knowledge_graph.db"

    def _get_db(self):
        if not self._db_path.exists():
            return None
        return sqlite3.connect(str(self._db_path))

    async def get_local_subgraph(self, entities: List[str]) -> Dict[str, Any]:
        """Fetch the local subgraph surrounding the target entities from SQLite."""
        db = self._get_db()
        if not db:
            return {"nodes": [], "edges": []}

        try:
            nodes = []
            edges = []
            node_ids = set()

            # Map inputs to canonical nodes in db
            placeholders = ",".join("?" for _ in entities)
            cursor = db.execute(
                f"SELECT id, canonical_name, entity_type, mention_count FROM nodes WHERE canonical_name IN ({placeholders})",
                entities
            )
            for r in cursor:
                nodes.append({
                    "id": r[0],
                    "name": r[1],
                    "type": r[2],
                    "mentions": r[3]
                })
                node_ids.add(r[0])

            if not node_ids:
                db.close()
                return {"nodes": [], "edges": []}

            # Fetch edges connected to these nodes (1-hop)
            edge_placeholders = ",".join("?" for _ in node_ids)
            cursor = db.execute(
                f"""SELECT source_id, target_id, relationship_type, confidence, weight 
                   FROM edges WHERE source_id IN ({edge_placeholders}) OR target_id IN ({edge_placeholders})""",
                list(node_ids) * 2
            )
            for r in cursor:
                edges.append({
                    "source": r[0],
                    "target": r[1],
                    "type": r[2],
                    "confidence": r[3],
                    "weight": r[4]
                })
                # Add neighbor nodes
                node_ids.add(r[0])
                node_ids.add(r[1])

            # Get full node objects for all node_ids
            all_node_placeholders = ",".join("?" for _ in node_ids)
            cursor = db.execute(
                f"SELECT id, canonical_name, entity_type, mention_count, COALESCE(pagerank, 0.0) FROM nodes WHERE id IN ({all_node_placeholders})",
                list(node_ids)
            )
            nodes = []
            for r in cursor:
                nodes.append({
                    "id": r[0],
                    "name": r[1],
                    "type": r[2],
                    "mentions": r[3],
                    "pagerank": r[4]
                })

            db.close()
            return {"nodes": nodes, "edges": edges}
        except Exception as e:
            logger.error("subgraph_fetch_failed", error=str(e))
            if db:
                db.close()
            return {"nodes": [], "edges": []}

    async def simulate_counterfactual(
        self,
        signal_title: str,
        signal_desc: str,
        treatment_node: str,
        outcome_node: str,
        intervention_val: str = "inactive",
    ) -> Dict[str, Any]:
        """
        Run the Pearl-based Counterfactual Simulation.
        
        Args:
            signal_title: The name of the observed signal.
            signal_desc: Description of the signal.
            treatment_node: The name of the entity we are intervening on.
            outcome_node: The name of the outcome entity.
            intervention_val: "active" or "inactive".
            
        Returns:
            Dict containing the counterfactual verdict, probabilities, and path analysis.
        """
        logger.info(
            "starting_causal_simulation",
            treatment=treatment_node,
            outcome=outcome_node,
            intervention=intervention_val
        )

        # 1. Fetch graph context
        subgraph = await self.get_local_subgraph([treatment_node, outcome_node])
        nodes_desc = "\n".join(f"- Node: {n['name']} (Type: {n['type']}, Mentions: {n['mentions']})" for n in subgraph["nodes"])
        edges_desc = "\n".join(f"- Edge: {e['source']} --[{e['type']}(conf={e['confidence']})]--> {e['target']}" for e in subgraph["edges"])
        graph_context = f"Nodes:\n{nodes_desc}\n\nEdges:\n{edges_desc}"

        # 2. Fetch exogenous background evidence from LanceDB (using embeddings)
        exogenous_evidence = "No additional context found."
        if self._lancedb and self._embedder:
            try:
                query_text = f"{treatment_node} {outcome_node} {signal_desc}"
                query_vector = self._embedder.embed_text(query_text)
                paras = await self._lancedb.search("paragraph_embeddings", query_vector=query_vector, limit=4)
                if paras:
                    exogenous_evidence = "\n".join(f"- Context: {p.get('text', '')}" for p in paras if (1.0 - p.get("_distance", 1.0)) > 0.55)
            except Exception as e:
                logger.debug("exogenous_evidence_search_failed", error=str(e))

        # 3. Formulate Prompt
        prompt = SIMULATE_PROMPT.format(
            signal_title=signal_title,
            signal_desc=signal_desc,
            treatment_node=treatment_node,
            outcome_node=outcome_node,
            graph_context=graph_context,
            exogenous_evidence=exogenous_evidence,
            intervention_val=intervention_val
        )

        # 4. Invoke LLM for structural equation simulation
        try:
            config = self._config.extraction
            async with httpx.AsyncClient(timeout=config.llm_timeout_seconds) as client:
                resp = await client.post(
                    f"{config.llm_base_url.rstrip('/')}/v1/chat/completions",
                    json={
                        "model": config.llm_model,
                        "messages": [
                            {"role": "system", "content": "You are a causal reasoning and counterfactual simulation system. Respond ONLY in valid JSON."},
                            {"role": "user", "content": prompt},
                        ],
                        "temperature": 0.2,
                        "max_tokens": 800,
                    },
                )
                resp.raise_for_status()
                content = resp.json()["choices"][0]["message"]["content"].strip()

                if content.startswith("```"):
                    lines = content.split("\n")
                    content = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])

                result = orjson.loads(content)
                result["status"] = "success"
                result["treatment"] = treatment_node
                result["outcome"] = outcome_node
                result["intervention"] = intervention_val
                
                logger.info(
                    "causal_simulation_completed",
                    probability=result.get("estimated_counterfactual_probability"),
                    necessity=result.get("causal_necessity")
                )
                return result

        except Exception as e:
            logger.error("causal_simulation_failed", error=str(e))
            return {
                "status": "error",
                "error": str(e),
                "detailed_explanation": f"Failed to compute counterfactual simulation: {e}"
            }
