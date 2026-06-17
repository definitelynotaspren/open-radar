"""PostGIS storage backend for idempotent event ingestion."""
from __future__ import annotations

from typing import List, Dict, Any

try:
    import psycopg2  # type: ignore[import-not-found]
    import psycopg2.extras  # type: ignore[import-not-found]
except ImportError:
    psycopg2 = None  # type: ignore


SCHEMA_SQL = """
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS events (
    event_uid   TEXT PRIMARY KEY,
    source      TEXT,
    title       TEXT,
    link        TEXT,
    summary     TEXT,
    event_time  TIMESTAMPTZ,
    event_type  TEXT,
    city        TEXT,
    state       TEXT,
    country     TEXT,
    confidence  DOUBLE PRECISION,
    simhash     NUMERIC(20),
    first_seen  TIMESTAMPTZ DEFAULT now(),
    last_seen   TIMESTAMPTZ DEFAULT now(),
    geom        GEOMETRY(Point, 4326)
);

CREATE INDEX IF NOT EXISTS events_geom_idx ON events USING GIST (geom);
CREATE INDEX IF NOT EXISTS events_time_idx ON events (event_time);
"""

_UPSERT = """
INSERT INTO events (event_uid, source, title, link, summary, event_time,
                    event_type, city, state, country, confidence, simhash, geom)
VALUES (%(event_uid)s, %(source)s, %(title)s, %(link)s, %(summary)s, %(event_time)s,
        %(event_type)s, %(city)s, %(state)s, %(country)s, %(confidence)s, %(simhash)s,
        CASE WHEN %(lon)s IS NOT NULL AND %(lat)s IS NOT NULL
             THEN ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)
             ELSE NULL END)
ON CONFLICT (event_uid) DO UPDATE SET
    last_seen  = now(),
    summary    = EXCLUDED.summary,
    event_time = COALESCE(EXCLUDED.event_time, events.event_time);
"""


def connect(dsn: str):
    """Return a psycopg2 connection. Raises ImportError if psycopg2 unavailable."""
    if psycopg2 is None:
        raise ImportError("psycopg2 is required for PostGIS support. Install psycopg2-binary.")
    conn = psycopg2.connect(dsn)
    return conn


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()


def upsert_events(conn, rows: List[Dict[str, Any]]) -> int:
    """Upsert a list of event dicts. Returns count of rows processed."""
    if not rows:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, _UPSERT, rows)
    conn.commit()
    return len(rows)
