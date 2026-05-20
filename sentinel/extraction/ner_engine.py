"""
SENTINEL NER engine.
spaCy-based Named Entity Recognition with EntityType mapping.
"""
from __future__ import annotations

from typing import Optional

import structlog

from sentinel.config import get_config
from sentinel.models import EntityType, ExtractedEntity

logger = structlog.get_logger(__name__)

# spaCy label to EntityType mapping
LABEL_MAP: dict[str, EntityType] = {
    "ORG": EntityType.ORGANIZATION,
    "PERSON": EntityType.PERSON,
    "GPE": EntityType.LOCATION,
    "LOC": EntityType.LOCATION,
    "PRODUCT": EntityType.PRODUCT,
    "EVENT": EntityType.EVENT,
    "MONEY": EntityType.FUNDING_ROUND,
    "WORK_OF_ART": EntityType.CONCEPT,
    "LAW": EntityType.REGULATION,
    "NORP": EntityType.CONCEPT,
    "FAC": EntityType.LOCATION,
}

# Common stop words to filter
STOP_ENTITIES = {
    "the", "a", "an", "this", "that", "it", "he", "she", "they",
    "we", "you", "i", "me", "us", "him", "her", "them",
}

# Entities that spaCy often misclassifies — force correct type
ENTITY_TYPE_OVERRIDES: dict[str, str] = {
    **{name: "location" for name in (
        "asia", "europe", "africa", "north america", "south america", "oceania",
        "antarctica", "china", "india", "japan", "australia", "canada", "brazil",
        "russia", "germany", "france", "italy", "spain", "mexico", "korea",
        "uk", "usa", "iran", "iraq", "israel", "turkey", "egypt", "argentina",
        "colombia", "peru", "chile", "venezuela", "cuba", "sweden", "norway",
        "finland", "denmark", "poland", "ukraine", "switzerland", "austria",
        "netherlands", "belgium", "portugal", "greece", "thailand", "vietnam",
        "indonesia", "malaysia", "philippines", "singapore", "taiwan",
        "hong kong", "new zealand", "south africa", "nigeria", "kenya",
        "saudi arabia", "pakistan", "bangladesh", "sri lanka", "nepal",
    )},
    # Technology products spaCy misclassifies as PERSON/ORG
    **{name: "product" for name in (
        "kafka", "git", "docker", "kubernetes", "redis", "postgres", "postgresql",
        "mongodb", "elasticsearch", "grafana", "prometheus", "terraform", "ansible",
        "jenkins", "nginx", "apache", "linux", "ubuntu", "debian", "fedora",
        "rust", "golang", "pytorch", "tensorflow", "numpy", "pandas",
        "claude code", "codeberg", "gitea", "forgejo", "vim", "neovim", "emacs",
        "skype", "telegram", "whatsapp", "signal", "discord", "slack",
        "grapheneos", "imagemagick", "ffmpeg", "wget", "curl",
        "llama", "llama 2", "llama 3", "mistral", "qwen", "gemma", "phi",
    )},
}


class NEREngine:
    """
    Named Entity Recognition engine using spaCy.

    Lazy-loads the transformer model on first use.
    Maps spaCy labels to SENTINEL EntityType enum.
    Deduplicates and filters low-confidence entities.
    """

    def __init__(self) -> None:
        """Initialize NER engine (model loaded on first call)."""
        self._nlp = None
        self._config = get_config()

    def _load_model(self) -> None:
        """Lazy-load spaCy model."""
        if self._nlp is not None:
            return

        model_name = self._config.extraction.ner.spacy_model
        try:
            import spacy
            self._nlp = spacy.load(model_name)
            logger.info("ner_model_loaded", model=model_name)
        except OSError:
            # Fall back to smaller model
            try:
                import spacy
                self._nlp = spacy.load("en_core_web_sm")
                logger.warning("ner_fallback_model", model="en_core_web_sm")
            except OSError:
                logger.error("ner_no_model_available")
                self._nlp = None

    def extract_entities(self, text: str) -> list[ExtractedEntity]:
        """
        Extract named entities from text.

        Args:
            text: Input text to extract entities from.

        Returns:
            List of ExtractedEntity objects.
        """
        self._load_model()
        if self._nlp is None:
            return []

        # Truncate for performance
        if len(text) > 100_000:
            text = text[:100_000]

        try:
            doc = self._nlp(text)
        except Exception as e:
            logger.error("ner_processing_failed", error=str(e))
            return []

        # Extract and map entities
        entity_map: dict[str, ExtractedEntity] = {}

        for ent in doc.ents:
            # Skip single character and stop word entities
            name = ent.text.strip()

            # Strip possessive suffixes ("Trump's" → "Trump", "Trump's" → "Trump")
            import re as _re
            name = _re.sub(r"['\u2019]s$", "", name).strip()

            # Strip leading articles ("the Men's Restroom" → "Men Restroom")
            if name.lower().startswith("the "):
                name = name[4:].strip()

            if len(name) <= 1 or name.lower() in STOP_ENTITIES:
                continue

            # Skip garbage: URLs, binary content, too-long names, multi-line
            if len(name) > 50 or "\n" in name or "\r" in name:
                continue
            if "http" in name or "//" in name or "href=" in name:
                continue
            # Reject HTML fragments
            if "<" in name and ">" in name:
                continue
            # Reject entities with too many words (likely sentences/phrases)
            words = name.split()
            if len(words) > 7:
                continue
            # Must be mostly alphanumeric/spaces (reject binary garbage)
            text_chars = sum(1 for c in name if c.isalnum() or c in " -.'&")
            if text_chars / max(len(name), 1) < 0.85:
                continue
            # Reject mostly non-ASCII (e.g. Chinese text, emoji strings)
            ascii_chars = sum(1 for c in name if ord(c) < 128)
            if ascii_chars / max(len(name), 1) < 0.7:
                continue

            # Map label — check overrides first
            override_type = ENTITY_TYPE_OVERRIDES.get(name.lower())
            if override_type:
                entity_type = EntityType(override_type)
            else:
                entity_type = LABEL_MAP.get(ent.label_)
                if entity_type is None:
                    continue

            # Use KB ID confidence if available, otherwise use a default
            confidence = 0.7  # Default confidence for transformer model

            # Skip low-confidence entities
            if confidence < 0.5:
                continue

            # Deduplicate: keep highest confidence version
            key = name.lower()
            if key in entity_map:
                if confidence > entity_map[key].confidence:
                    entity_map[key] = ExtractedEntity(
                        text=name,
                        entity_type=entity_type,
                        confidence=confidence,
                    )
            else:
                entity_map[key] = ExtractedEntity(
                    text=name,
                    entity_type=entity_type,
                    confidence=confidence,
                )

        entities = list(entity_map.values())
        logger.debug("ner_extraction_completed", entity_count=len(entities))
        return entities
