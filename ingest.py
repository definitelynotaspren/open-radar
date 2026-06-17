"""Ingestion pipeline for Open Radar."""
from __future__ import annotations

import argparse
import hashlib
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

import pandas as pd  # type: ignore[import-untyped]
import yaml  # type: ignore[import-untyped]

from radar import sources, extract, geocode, dedupe, store, export


def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    # Allow env-var overrides for secrets
    for key in ("postgis_dsn", "flight_data_url", "permit_data_url"):
        env_val = os.environ.get(key.upper())
        if env_val:
            raw[key] = env_val
    return raw


def _event_uid(item: sources.Item) -> str:
    basis = (item.link or f"{item.title}|{item.published}|{item.source}").strip().lower()
    return hashlib.sha256(basis.encode()).hexdigest()[:32]


def pull_sources(cfg: Dict[str, Any]) -> List[sources.Item]:
    items: List[sources.Item] = []
    rss_urls = cfg.get("sources", {}).get("rss", [])
    # Allow runtime URL injection from env
    flight_url = cfg.get("flight_data_url")
    permit_url = cfg.get("permit_data_url")
    items.extend(sources.fetch_rss(rss_urls))
    for url in cfg.get("sources", {}).get("json", []):
        items.extend(sources.fetch_json(url))
    if flight_url:
        items.extend(sources.fetch_json(flight_url))
    if permit_url:
        items.extend(sources.fetch_json(permit_url))
    return items


def process_items(
    items: List[sources.Item], cfg: Dict[str, Any], since: datetime | None
) -> List[Dict[str, Any]]:
    geocoder = geocode.GeoCoder(cfg.get("geocode_cache", "data/geocode_cache.sqlite"))
    events: List[Dict[str, Any]] = []
    for item in items:
        text = sources.fetch_article_html(item.link) or item.summary
        candidates = extract.extract_candidates(text, item.title)
        location_text = candidates[0].text if candidates else ""
        event_time = extract.extract_event_time(item.published or item.summary)
        if since and event_time < since:
            continue
        event_type = extract.classify_event_type(f"{item.title} {item.summary}")
        simhash_val = dedupe.simhash_of(item.title + item.summary)
        if dedupe.is_dupe(simhash_val, window_hours=24):
            continue
        lat, lon, confidence = geocoder.geocode(location_text) if location_text else (None, None, None)
        events.append(
            {
                "event_uid": _event_uid(item),
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
                "confidence": confidence,
                "simhash": simhash_val,
            }
        )
    return events


def run_pipeline(cfg: Dict[str, Any], dry_run: bool, since: datetime | None) -> None:
    items = pull_sources(cfg)
    events = process_items(items, cfg, since)
    if dry_run:
        print(f"Pulled {len(items)} items, {len(events)} events (dry run — nothing written)")
        return

    postgis_dsn = cfg.get("postgis_dsn")
    if postgis_dsn:
        from radar import pg_store
        pg_conn = pg_store.connect(postgis_dsn)
        pg_store.ensure_schema(pg_conn)
        written = pg_store.upsert_events(pg_conn, events)
        pg_conn.close()
        print(f"Upserted {written} events into PostGIS")
    else:
        duck = store.connect_duckdb(cfg["duckdb_path"])
        sqlite_conn = store.connect_sqlite(cfg["sqlite_path"])
        uids = store.insert_events(duck, events)
        for uid, ev in zip(uids, events):
            store.upsert_article(sqlite_conn, uid, ev["title"], ev["summary"])
            if cfg.get("vault_path"):
                export.to_obsidian_note(pd.Series(ev | {"id": uid}), cfg["vault_path"])
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
    since: datetime | None = None
    if args.since:
        since = datetime.fromisoformat(args.since)
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
    run_pipeline(cfg, args.dry_run, since)


if __name__ == "__main__":
    main()
