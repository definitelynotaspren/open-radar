"""Source fetching utilities."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List

import feedparser  # type: ignore[import-untyped]
import requests  # type: ignore[import-untyped]


@dataclass
class Item:
    """Normalized feed item."""

    source: str
    title: str
    link: str
    summary: str
    published: datetime | None


def fetch_rss(urls: List[str]) -> List[Item]:
    """Fetch RSS/Atom feeds and return normalized items."""
    items: List[Item] = []
    for url in urls:
        feed = feedparser.parse(url)
        for entry in feed.entries:
            published = None
            dt = entry.get("published") or entry.get("updated")
            if dt:
                try:
                    published = datetime(*entry.published_parsed[:6])
                except Exception:
                    try:
                        published = datetime(*entry.updated_parsed[:6])
                    except Exception:  # pragma: no cover - best effort
                        published = None
            items.append(
                Item(
                    source=url,
                    title=entry.get("title", ""),
                    link=entry.get("link", ""),
                    summary=entry.get("summary", ""),
                    published=published,
                )
            )
    return items


def fetch_json(url: str) -> List[Item]:
    """Fetch JSON feed with list of items."""
    items: List[Item] = []
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return items
    for obj in data if isinstance(data, list) else data.get("items", []):
        items.append(
            Item(
                source=url,
                title=obj.get("title", ""),
                link=obj.get("link", ""),
                summary=obj.get("summary", ""),
                published=None,
            )
        )
    return items


def fetch_article_html(url: str) -> str:
    """Fetch article HTML, trying trafilatura then newspaper3k."""
    try:  # pragma: no cover - network heavy
        import trafilatura  # type: ignore[import-not-found]

        downloaded = trafilatura.fetch_url(url)
        text = trafilatura.extract(downloaded or "")
        if text:
            return text
    except Exception:
        pass
    try:  # pragma: no cover - slow
        from newspaper import Article  # type: ignore[import-not-found]

        art = Article(url)
        art.download()
        art.parse()
        return art.text
    except Exception:
        return ""
