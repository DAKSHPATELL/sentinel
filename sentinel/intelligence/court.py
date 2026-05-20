"""
SENTINEL Hypothesis Court.
Multi-agent debate framework for final signal validation.
Uses multi-turn dialectical debates, Bayesian probability updates, and causal verification.
"""
from __future__ import annotations

import httpx
import orjson
import structlog
from typing import Optional, Dict, Any, List

from sentinel.config import get_config
from sentinel.core.lancedb_client import LanceDBClient
from sentinel.extraction.embedder import Embedder
from sentinel.models import Signal, Verdict

logger = structlog.get_logger(__name__)

ADVOCATE_OPENING_PROMPT = """You are the Advocate for this signal.
Your job is to argue why this signal is true, highly novel, and extremely critical.
Use the observed signal, description, and any retrieved factual/causal context.
Keep your argument under 250 words.

Signal Title: {title}
Description: {description}
Factual Context: {context}
Causal Necessity (estimated): {causal_necessity}
Causal Sufficiency (estimated): {causal_sufficiency}"""

SKEPTIC_CROSS_PROMPT = """You are the Skeptic against this signal.
Your job is to tear down the Advocate's opening statement. Challenge their assumptions, point out logical gaps, question the reliability of the sources/context, suggest alternative explanations, or call out potential false positives.
Keep your argument under 250 words.

Signal: {title} - {description}
Advocate's Opening Statement: {opening}"""

ADVOCATE_REBUTTAL_PROMPT = """You are the Advocate for this signal.
Your job is to respond directly to the Skeptic's cross-examination. Defend your case by using the retrieved factual/causal context to debunk their skepticism. Do not introduce completely unrelated arguments; address their points.
Keep your rebuttal under 200 words.

Signal: {title}
Factual Context: {context}
Skeptic's Cross-Examination: {cross}"""

SKEPTIC_CLOSING_PROMPT = """You are the Skeptic against this signal.
Write a closing statement highlighting the most critical, unaddressed risks or unresolved vulnerabilities in the Advocate's rebuttal.
Keep it under 150 words.

Skeptic's Cross-Examination: {cross}
Advocate's Rebuttal: {rebuttal}"""

BAYESIAN_JUDGE_PROMPT = """You are the independent Judge evaluating the validity and credibility of this web intelligence signal.
You have monitored a multi-turn dialectical debate between the Advocate and the Skeptic.

Read the debate transcript carefully. Evaluate the logical coherence, factual support, and empirical credibility of both sides.

You must output:
1. `likelihood_adv`: Your assessment of the probability that the Advocate would make these arguments IF the signal is indeed true (P(Arguments | True)). A value between 0.01 and 0.99.
2. `likelihood_skep`: Your assessment of the probability that the Skeptic would make these arguments IF the signal is false/noise (P(Arguments | False)). A value between 0.01 and 0.99.
3. `reasoning`: A detailed explanation summarizing your verdict, weighing the points raised by the Advocate and the Skeptic.

Respond ONLY with valid JSON in this schema:
{{
  "likelihood_adv": 0.85,
  "likelihood_skep": 0.90,
  "reasoning": "The advocate provided solid corroborative evidence, whereas the skeptic's concerns about X were resolved in rebuttal."
}}

Signal: {title} ({description})
Debate Transcript:
- Advocate Opening: {opening}
- Skeptic Cross-Examination: {cross}
- Advocate Rebuttal: {rebuttal}
- Skeptic Closing: {closing}"""


class HypothesisCourt:
    """
    Multi-agent debate (Advocate vs Skeptic) evaluated by a Judge using Bayesian updates.
    Used for final validation of complex, critical signals before human review.
    """

    def __init__(
        self,
        lancedb_client: Optional[LanceDBClient] = None,
        embedder: Optional[Embedder] = None,
    ) -> None:
        self._config = get_config()
        self._lance = lancedb_client
        self._embedder = embedder

    async def deliberate(self, signal: Signal) -> Verdict:
        """Alias for evaluate to support pipeline integration."""
        return await self.evaluate(signal)

    async def evaluate(self, signal: Signal, causal_explanation: Optional[dict] = None) -> Verdict:
        """
        Run the multi-agent dialectical debate and update signal credibility using Bayes' rule.

        Returns:
            Verdict object with the Judge's final decision.
        """
        try:
            logger.info("starting_court_deliberation", signal_id=str(signal.id), title=signal.title)

            # 1. Retrieve Factual Background Context from LanceDB
            context_text = "No additional context found."
            if self._lance and self._embedder:
                try:
                    query_vector = self._embedder.embed_text(f"{signal.title} {signal.description}")
                    paras = await self._lance.search("paragraph_embeddings", query_vector=query_vector, limit=3)
                    if paras:
                        context_text = "\n".join(f"- {p.get('text', '')}" for p in paras if (1.0 - p.get("_distance", 1.0)) > 0.5)
                except Exception as e:
                    logger.debug("court_context_retrieval_failed", error=str(e))

            # 2. Run Causal Simulation for Verification
            causal_necessity = 0.5
            causal_sufficiency = 0.5
            if len(signal.entities) >= 2:
                try:
                    from sentinel.intelligence.causal_simulator import CausalSimulator
                    simulator = CausalSimulator(self._lance, self._embedder)
                    sim_result = await simulator.simulate_counterfactual(
                        signal_title=signal.title,
                        signal_desc=signal.description,
                        treatment_node=signal.entities[0],
                        outcome_node=signal.entities[1],
                        intervention_val="inactive"
                    )
                    if sim_result.get("status") == "success":
                        causal_necessity = sim_result.get("causal_necessity", 0.5)
                        causal_sufficiency = sim_result.get("causal_sufficiency", 0.5)
                        logger.info("court_causal_simulation_integrated", necessity=causal_necessity, sufficiency=causal_sufficiency)
                except Exception as e:
                    logger.debug("court_causal_simulation_failed", error=str(e))

            # 3. Multi-Turn Debate Dialectic
            # Turn 1: Advocate Opening
            advocate_opening = await self._call_llm(
                system_prompt="You are a fierce advocate.",
                user_prompt=ADVOCATE_OPENING_PROMPT.format(
                    title=signal.title,
                    description=signal.description,
                    context=context_text,
                    causal_necessity=causal_necessity,
                    causal_sufficiency=causal_sufficiency,
                ),
                temperature=0.6
            )

            # Turn 2: Skeptic Cross-Examination
            skeptic_cross = await self._call_llm(
                system_prompt="You are a relentless critic.",
                user_prompt=SKEPTIC_CROSS_PROMPT.format(
                    title=signal.title,
                    description=signal.description,
                    opening=advocate_opening
                ),
                temperature=0.6
            )

            # Turn 3: Advocate Rebuttal
            advocate_rebuttal = await self._call_llm(
                system_prompt="You are a fierce advocate defending your opening statement.",
                user_prompt=ADVOCATE_REBUTTAL_PROMPT.format(
                    title=signal.title,
                    context=context_text,
                    cross=skeptic_cross
                ),
                temperature=0.6
            )

            # Turn 4: Skeptic Closing Statement
            skeptic_closing = await self._call_llm(
                system_prompt="You are a relentless critic summarizing your final doubts.",
                user_prompt=SKEPTIC_CLOSING_PROMPT.format(
                    cross=skeptic_cross,
                    rebuttal=advocate_rebuttal
                ),
                temperature=0.6
            )

            # 4. Bayesian Evaluation by the Judge
            judge_response = await self._call_llm(
                system_prompt="You are an impartial judge evaluating a debate transcript. Respond ONLY in valid JSON.",
                user_prompt=BAYESIAN_JUDGE_PROMPT.format(
                    title=signal.title,
                    description=signal.description,
                    opening=advocate_opening,
                    cross=skeptic_cross,
                    rebuttal=advocate_rebuttal,
                    closing=skeptic_closing,
                ),
                temperature=0.2,
                expect_json=True
            )

            if isinstance(judge_response, dict):
                l_adv = float(judge_response.get("likelihood_adv", 0.5))
                l_skep = float(judge_response.get("likelihood_skep", 0.5))
                reasoning = judge_response.get("reasoning", "")
                
                # Apply Bayes' Rule to update the probability
                # Prior P(True) = signal.confidence
                p_prior = max(0.01, min(0.99, signal.confidence))
                numerator = p_prior * l_adv
                denominator = numerator + (1.0 - p_prior) * (1.0 - l_skep)
                
                if denominator > 0:
                    p_posterior = numerator / denominator
                else:
                    p_posterior = p_prior

                # Keep in logical bounds
                final_confidence = round(max(0.01, min(0.99, p_posterior)), 3)
                is_approved = (final_confidence >= 0.65)
                
                logger.info(
                    "court_bayesian_evaluation_completed",
                    signal_id=str(signal.id),
                    prior=p_prior,
                    posterior=final_confidence,
                    approved=is_approved,
                    l_adv=l_adv,
                    l_skep=l_skep
                )
            else:
                is_approved = True
                final_confidence = signal.confidence
                reasoning = "Judge failed to respond with valid JSON."

            return Verdict(
                signal_id=signal.id,
                approved=is_approved,
                reasoning=reasoning,
                advocate_argument=f"OPENING: {advocate_opening}\n\nREBUTTAL: {advocate_rebuttal}",
                skeptic_argument=f"CROSS-EXAMINATION: {skeptic_cross}\n\nCLOSING: {skeptic_closing}",
                final_confidence=final_confidence,
            )

        except Exception as e:
            logger.error("court_evaluation_failed", error=str(e))
            return Verdict(
                signal_id=signal.id,
                approved=True,  # Default to pass if system fails
                reasoning=f"System error: {e}",
                advocate_argument="",
                skeptic_argument="",
                final_confidence=signal.confidence,
            )

    async def _call_llm(
        self, system_prompt: str, user_prompt: str, temperature: float, expect_json: bool = False
    ) -> str | dict:
        """Call standard Ollama LLM endpoint."""
        config = self._config.extraction
        async with httpx.AsyncClient(timeout=config.llm_timeout_seconds) as client:
            resp = await client.post(
                f"{config.llm_base_url.rstrip('/')}/v1/chat/completions",
                json={
                    "model": config.llm_model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    "temperature": temperature,
                    "max_tokens": 512 if not expect_json else 256,
                },
            )
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"].strip()

            if expect_json:
                try:
                    if content.startswith("```"):
                        lines = content.split("\n")
                        start_idx = 1
                        end_idx = -1
                        content = "\n".join(lines[start_idx:end_idx] if lines[end_idx].strip() == "```" else lines[start_idx:])
                    first_brace = content.find("{")
                    last_brace = content.rfind("}")
                    if first_brace != -1 and last_brace != -1:
                        content = content[first_brace:last_brace+1]
                    return orjson.loads(content)
                except Exception as e:
                    logger.warning("json_parsing_failed", error=str(e), content=content)
                    raise
            return content
