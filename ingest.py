"""Ingest RSS, flight data, and permit data; output GeoJSON and flagged zones."""
import json
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any

import duckdb
import feedparser
import pandas as pd
import requests
import yaml
from dateutil import parser as date_parser

CONFIG_FILE = 'config.yaml'

def _resolve_env(obj: Any) -> Any:
    """Recursively expand environment variables in strings."""
    if isinstance(obj, dict):
        return {k: _resolve_env(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolve_env(v) for v in obj]
    if isinstance(obj, str):
        return os.path.expandvars(obj)
    return obj


def load_config(path: str) -> Dict:
    with open(path, 'r') as f:
        data = yaml.safe_load(f)
    return _resolve_env(data)


def init_db(db_path: str) -> duckdb.DuckDBPyConnection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = duckdb.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id BIGINT,
            source_type TEXT,
            title TEXT,
            link TEXT,
            description TEXT,
            event_time TIMESTAMP,
            latitude DOUBLE,
            longitude DOUBLE
        )
        """
    )
    return conn


def parse_rss(url: str) -> List[Dict]:
    entries = []
    feed = feedparser.parse(url)
    for entry in feed.entries:
        lat = entry.get('geo_lat') or entry.get('lat')
        lon = entry.get('geo_long') or entry.get('lon')
        if lat and lon:
            point = (float(lat), float(lon))
        else:
            point = (None, None)
        dt = entry.get('published') or entry.get('updated')
        try:
            dt_parsed = date_parser.parse(dt) if dt else datetime.utcnow()
        except Exception:
            dt_parsed = datetime.utcnow()
        entries.append({
            'source_type': 'rss',
            'title': entry.get('title'),
            'link': entry.get('link'),
            'description': entry.get('summary'),
            'event_time': dt_parsed,
            'latitude': point[0],
            'longitude': point[1]
        })
    return entries


def fetch_json(url: str) -> List[Dict]:
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception:
        return []


def parse_flights(url: str) -> List[Dict]:
    data = fetch_json(url)
    entries = []
    for item in data if isinstance(data, list) else data.get('flights', []):
        lat = item.get('lat') or item.get('latitude')
        lon = item.get('lon') or item.get('longitude')
        if lat and lon:
            dt = item.get('timestamp') or item.get('time')
            try:
                dt_parsed = date_parser.parse(dt) if isinstance(dt, str) else datetime.utcfromtimestamp(dt)
            except Exception:
                dt_parsed = datetime.utcnow()
            entries.append({
                'source_type': 'flight',
                'title': item.get('ident') or 'Flight',
                'link': '',
                'description': json.dumps(item),
                'event_time': dt_parsed,
                'latitude': float(lat),
                'longitude': float(lon)
            })
    return entries


def parse_permits(url: str) -> List[Dict]:
    data = fetch_json(url)
    entries = []
    for item in data if isinstance(data, list) else data.get('permits', []):
        lat = item.get('lat') or item.get('latitude')
        lon = item.get('lon') or item.get('longitude')
        if lat and lon:
            dt = item.get('date') or item.get('timestamp')
            try:
                dt_parsed = date_parser.parse(dt) if isinstance(dt, str) else datetime.utcfromtimestamp(dt)
            except Exception:
                dt_parsed = datetime.utcnow()
            entries.append({
                'source_type': 'permit',
                'title': item.get('name') or 'Permit',
                'link': '',
                'description': json.dumps(item),
                'event_time': dt_parsed,
                'latitude': float(lat),
                'longitude': float(lon)
            })
    return entries


def insert_events(conn: duckdb.DuckDBPyConnection, rows: List[Dict]):
    if not rows:
        return
    df = pd.DataFrame(rows)
    start_id = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM events").fetchone()[0]
    df.insert(0, 'id', range(start_id, start_id + len(df)))
    conn.execute("INSERT INTO events VALUES", df)


def load_data(cfg: Dict, conn: duckdb.DuckDBPyConnection):
    for feed in cfg['sources'].get('rss', []):
        insert_events(conn, parse_rss(feed))
    flight_url = cfg['sources'].get('flight_data')
    if flight_url:
        insert_events(conn, parse_flights(flight_url))
    permit_url = cfg['sources'].get('permit_data')
    if permit_url:
        insert_events(conn, parse_permits(permit_url))


def export_geojson(conn: duckdb.DuckDBPyConnection, out_path: str):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    rows = conn.execute("SELECT * FROM events").fetchdf()
    features = []
    for _, row in rows.iterrows():
        if row['latitude'] is None or row['longitude'] is None:
            continue
        geom = {
            'type': 'Point',
            'coordinates': [row['longitude'], row['latitude']]
        }
        props = {
            'id': int(row['id']),
            'source_type': row['source_type'],
            'title': row['title'],
            'description': row['description'],
            'link': row['link'],
            'event_time': row['event_time'].isoformat()
        }
        features.append({'type': 'Feature', 'geometry': geom, 'properties': props})
    with open(out_path, 'w') as f:
        json.dump({'type': 'FeatureCollection', 'features': features}, f)


def export_csv(conn: duckdb.DuckDBPyConnection, out_path: str):
    """Export events to a simple CSV for easy import in tools like GRASS."""
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df = conn.execute("SELECT * FROM events").fetchdf()
    if df.empty:
        df.to_csv(out_path, index=False)
        return
    df.to_csv(out_path, index=False)


def export_flagged(conn: duckdb.DuckDBPyConnection, cfg: Dict, docs_dir: str):
    temporal_window = cfg.get('temporal_window_hours', 48)
    window_start = datetime.utcnow() - timedelta(hours=temporal_window)
    query = f"""
        SELECT round(latitude, 3) AS lat_round,
               round(longitude, 3) AS lon_round,
               list(distinct source_type) as sources,
               count(*) as count,
               min(event_time) as first_event_time
        FROM events
        WHERE event_time >= '{window_start.isoformat()}'
          AND latitude IS NOT NULL AND longitude IS NOT NULL
        GROUP BY lat_round, lon_round
        HAVING count(distinct source_type) > 2
    """
    df = conn.execute(query).fetchdf()
    features = []
    for _, row in df.iterrows():
        geom = {
            'type': 'Point',
            'coordinates': [float(row['lon_round']), float(row['lat_round'])]
        }
        props = {
            'sources': row['sources'],
            'count': int(row['count']),
            'first_event_time': row['first_event_time'].isoformat()
        }
        features.append({'type': 'Feature', 'geometry': geom, 'properties': props})

    os.makedirs(docs_dir, exist_ok=True)
    flagged_path = os.path.join(docs_dir, 'flagged_zones.geojson')
    with open(flagged_path, 'w') as f:
        json.dump({'type': 'FeatureCollection', 'features': features}, f)

    # Export last 7 days of flagged entries
    seven_days_ago = datetime.utcnow() - timedelta(days=7)
    df7 = df[df['first_event_time'] >= seven_days_ago]
    features7 = []
    for _, row in df7.iterrows():
        geom = {
            'type': 'Point',
            'coordinates': [float(row['lon_round']), float(row['lat_round'])]
        }
        props = {
            'sources': row['sources'],
            'count': int(row['count']),
            'first_event_time': row['first_event_time'].isoformat()
        }
        features7.append({'type': 'Feature', 'geometry': geom, 'properties': props})
    out7 = os.path.join(docs_dir, 'flagged_last_7_days.geojson')
    with open(out7, 'w') as f:
        json.dump({'type': 'FeatureCollection', 'features': features7}, f)


def main():
    cfg = load_config(CONFIG_FILE)
    conn = init_db(cfg['duckdb_path'])
    load_data(cfg, conn)
    export_geojson(conn, cfg['geojson_output'])
    if cfg.get('csv_output'):
        export_csv(conn, cfg['csv_output'])
    export_flagged(conn, cfg, docs_dir='docs')
    print('Ingestion complete.')


if __name__ == '__main__':
    main()
