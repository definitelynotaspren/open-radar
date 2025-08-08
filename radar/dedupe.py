"""Duplicate detection using simhash."""
from __future__ import annotations

from collections import deque
from datetime import datetime, timedelta
from typing import Deque, Tuple

try:  # pragma: no cover - dependency optional
    from simhash import Simhash  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    class Simhash:  # type: ignore[no-redef]
        def __init__(self, text: str):
            self.value = hash(text)

_seen: Deque[Tuple[int, datetime]] = deque()


def simhash_of(text: str) -> int:
    return Simhash(text).value


def is_dupe(simhash: int, window_hours: int = 24) -> bool:
    cutoff = datetime.utcnow() - timedelta(hours=window_hours)
    while _seen and _seen[0][1] < cutoff:
        _seen.popleft()
    for h, _ in _seen:
        if h == simhash:
            return True
    _seen.append((simhash, datetime.utcnow()))
    return False
