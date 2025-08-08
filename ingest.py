"""Ingestion pipeline for Open Radar."""
from __future__ import annotations

import argparse
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd  # type: ignore[import-untyped]
import yaml  # type: ignore[import-untyped]

from radar import sources, extract, geocode, dedupe, store, export


def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def pull_sources(cfg: Dict[str, Any]) -> List[sources.Item]:
    items: List[sources.Item] = []
    items.extend(sources.fetch_rss(cfg.get("sources", {}).get("rss", [])))
    for url in cfg.get("sources", {}).get("json", []):
        items.extend(sources.fetch_json(url))
    return items


def process_items(items: List[sources.Item], cfg: Dict[str, Any], since: datetime | None) -> List[Dict[str, Any]]:
    geocoder = geocode.GeoCoder(cfg.get("geocode_cache", "geocode_cache.sqlite"))
    events: List[Dict[str, Any]] = []
    for item in items:
        text = sources.fetch_article_html(item.link) or item.summary
        candidates = extract.extract_candidates(text, item.title)
        location_text = candidates[0].text if candidates else ""
        event_time = extract.extract_event_time(item.published or item.summary)
        if since and event_time < since:
            continue
        event_type = extract.classify_event_type(f"{item.title} {item.summary}")
        simhash = dedupe.simhash_of(item.title + item.summary)
        if dedupe.is_dupe(simhash, window_hours=24):
            continue
        lat, lon, _ = geocoder.geocode(location_text) if location_text else (None, None, None)
        events.append(
            {
                "source": item.source,
                "title": item.title,
                "link": item.link,
                "summary": item.summary,
                "event_time": event_time,
                "lat": lat,
                "lon": lon,
                "event_type": event_type,
                "city": None,
                "state": None,
                "country": None,
                "simhash": simhash,
            }
        )
    return events


def run_pipeline(cfg: Dict[str, Any], dry_run: bool, since: datetime | None) -> None:
    items = pull_sources(cfg)
    events = process_items(items, cfg, since)
    if dry_run:
        print(f"Pulled {len(items)} items, {len(events)} events")
        return
    duck = store.connect_duckdb(cfg["duckdb_path"])
    sqlite_conn = store.connect_sqlite(cfg["sqlite_path"])
    ids = store.insert_events(duck, events)
    for id_, ev in zip(ids, events):
        store.upsert_article(sqlite_conn, id_, ev["title"], ev["summary"])
        if cfg.get("vault_path"):
            export.to_obsidian_note(pd.Series(ev | {"id": id_}), cfg["vault_path"])
    df = duck.execute("SELECT * FROM events").fetchdf()
    export.to_geojson(df, cfg["geojson_output"])
    export.to_csv(df, cfg["csv_output"])


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="config.yaml")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--since")
    p.add_argument("--update", action="store_true", help="Run update pipeline")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config(args.config)
    since = datetime.fromisoformat(args.since) if args.since else None
    run_pipeline(cfg, args.dry_run, since)


if __name__ == "__main__":
    main()
