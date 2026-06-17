"""Text extraction helpers."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List
import re

try:  # pragma: no cover - optional deps
    import dateparser  # type: ignore[import-untyped]
except Exception:  # pragma: no cover
    dateparser = None  # type: ignore
from dateutil import parser as dateutil_parser  # type: ignore[import-untyped]
try:  # pragma: no cover
    import spacy  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    class _DummyNLP:
        def __call__(self, _text: str):
            class _Doc:
                ents: list = []

            return _Doc()

    class _SpacyModule:
        def blank(self, _lang: str):  # noqa: D401
            return _DummyNLP()

    spacy = _SpacyModule()  # type: ignore


_nlp: spacy.language.Language | None = None


@dataclass
class Candidate:
    text: str


def load_spacy() -> spacy.language.Language:
    """Load spaCy model with graceful fallback."""
    global _nlp
    if _nlp is None:
        try:
            if spacy is None:
                raise ImportError
            _nlp = spacy.load("en_core_web_sm")
        except Exception:  # pragma: no cover - model not installed
            _nlp = spacy.blank("en")
    return _nlp


def extract_candidates(text: str, title: str = "") -> List[Candidate]:
    nlp = load_spacy()
    doc = nlp(f"{title}\n{text}")
    ents = getattr(doc, "ents", [])
    if not ents:  # very simple fallback
        return [Candidate(m) for m in re.findall(r"[A-Z][a-z]+", f"{title} {text}")]
    return [Candidate(ent.text) for ent in ents if getattr(ent, "label_", "") in {"GPE", "LOC"}]


def _ensure_aware(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def extract_event_time(meta: str | datetime | None) -> datetime:
    if isinstance(meta, datetime):
        return _ensure_aware(meta)
    if isinstance(meta, str):
        dt = dateparser.parse(meta, settings={"RETURN_AS_TIMEZONE_AWARE": True}) if dateparser else None
        if not dt:
            try:
                dt = dateutil_parser.parse(meta, fuzzy=True)
            except Exception:
                dt = None
        if dt:
            return _ensure_aware(dt)
    return datetime.now(timezone.utc)


KEYWORDS = [
    "robbery",
    "assault",
    "burglary",
    "shooting",
    "fire",
    "crash",
    "arrest",
]


def classify_event_type(text: str) -> str:
    lower = text.lower()
    for word in KEYWORDS:
        if word in lower:
            return word
    return "other"
