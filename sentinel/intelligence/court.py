"""
SENTINEL Hypothesis Court.
Multi-agent debate framework for final signal validation.
"""
from __future__ import annotations

import httpx
import orjson
import structlog
from typing import Optional

from sentinel.config import get_config
from sentinel.models import Signal, Verdict

logger = structlog.get_logger(__name__)

ADVOCATE_PROMPT = """You are the Advocate for this signal.
Your job is to argue why this signal is true, highly novel, and extremely critical.
Use the provided evidence and causal explanation to build the strongest possible case.
Keep your argument under 200 words.

Signal: {signal}
Causal Explanation: {causal}"""

SKEPTIC_PROMPT = """You are the Skeptic against this signal.
Your job is to tear down the Advocate's argument. Point out logical fallacies, weak evidence, alternative explanations, and why this signal might be a false positive or completely irrelevant.
Keep your argument under 200 words.

Signal: {signal}
Advocate Argument: {advocate}"""

JUDGE_PROMPT = """You are the independent Judge.
You have heard the Advocate and the Skeptic debate the validity and importance of this signal.
Evaluate both arguments impartially. Then, deliver your final verdict.

Respond ONLY with valid JSON:
{{
  "decision": "APPROVED", // or "REJECTED"
  "reasoning": "The skeptic made a good point about X, but the advocate's evidence regarding Y outweighs it.",
  "final_confidence": 0.85
}}

Signal: {signal}
Advocate: {advocate}
Skeptic: {skeptic}"""


class HypothesisCourt:
    """
    Multi-agent debate (Advocate vs Skeptic) evaluated by a Judge.
    Used for final validation of complex, critical signals before human review.
    """

    def __init__(self) -> None:
        self._config = get_config()

    async def evaluate(self, signal: Signal, causal_explanation: Optional[dict] = None) -> Verdict:
        """
        Run the multi-agent debate.

        Returns:
            Verdict object with the Judge's final decision.
        """
        try:
            signal_text = f"{signal.signal_type.value}: {signal.description}"
            causal_text = causal_explanation.get("explanation", "None provided.") if causal_explanation else "None provided."

            # 1. Advocate argues for the signal
            advocate_arg = await self._call_llm(
                system_prompt="You are a fierce advocate.",
                user_prompt=ADVOCATE_PROMPT.format(signal=signal_text, causal=causal_text),
                temperature=0.7
            )

            # 2. Skeptic tries to destroy the argument
            skeptic_arg = await self._call_llm(
                system_prompt="You are a relentless skeptic.",
                user_prompt=SKEPTIC_PROMPT.format(signal=signal_text, advocate=advocate_arg),
                temperature=0.7
            )

            # 3. Judge evaluates
            judge_response = await self._call_llm(
                system_prompt="You are an impartial judge. Respond ONLY in valid JSON.",
                user_prompt=JUDGE_PROMPT.format(signal=signal_text, advocate=advocate_arg, skeptic=skeptic_arg),
                temperature=0.3,
                expect_json=True
            )

            if isinstance(judge_response, dict):
                decision_str = judge_response.get("decision", "REJECTED")
                is_approved = (decision_str.upper() == "APPROVED")
                confidence = float(judge_response.get("final_confidence", signal.confidence))
                reasoning = judge_response.get("reasoning", "")
            else:
                is_approved = True
                confidence = signal.confidence
                reasoning = "Judge failed to respond with valid JSON."

            logger.info(
                "court_evaluation_completed",
                signal_id=str(signal.id),
                approved=is_approved,
                confidence=round(confidence, 3)
            )

            return Verdict(
                signal_id=signal.id,
                approved=is_approved,
                reasoning=reasoning,
                advocate_argument=advocate_arg,
                skeptic_argument=skeptic_arg,
                final_confidence=confidence,
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
                if content.startswith("```"):
                    lines = content.split("\n")
                    content = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
                return orjson.loads(content)
            return content
