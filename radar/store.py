"""Storage helpers for DuckDB and SQLite FTS."""
from __future__ import annotations

from pathlib import Path
from typing import List, Dict
import sqlite3

import duckdb  # type: ignore[import-not-found]
import pandas as pd  # type: ignore[import-untyped]

EVENT_COLUMNS = [
    "event_uid",
    "source",
    "title",
    "link",
    "summary",
    "event_time",
    "lat",
    "lon",
    "event_type",
    "city",
    "state",
    "country",
    "simhash",
]


def connect_duckdb(path: str) -> duckdb.DuckDBPyConnection:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            event_uid   TEXT PRIMARY KEY,
            source      TEXT,
            title       TEXT,
            link        TEXT,
            summary     TEXT,
            event_time  TIMESTAMPTZ,
            lat         DOUBLE,
            lon         DOUBLE,
            event_type  TEXT,
            city        TEXT,
            state       TEXT,
            country     TEXT,
            simhash     HUGEINT,
            first_seen  TIMESTAMPTZ DEFAULT now(),
            last_seen   TIMESTAMPTZ DEFAULT now()
        )
        """
    )
    return conn


def insert_events(conn: duckdb.DuckDBPyConnection, rows: List[Dict]) -> List[str]:
    """Upsert events by event_uid; returns list of uids."""
    if not rows:
        return []
    uids = []
    for row in rows:
        uid = row["event_uid"]
        conn.execute(
            """
            INSERT INTO events (event_uid, source, title, link, summary, event_time,
                                lat, lon, event_type, city, state, country, simhash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (event_uid) DO UPDATE SET
                last_seen  = now(),
                summary    = EXCLUDED.summary,
                event_time = COALESCE(EXCLUDED.event_time, events.event_time)
            """,
            [
                uid,
                row.get("source"),
                row.get("title"),
                row.get("link"),
                row.get("summary"),
                row.get("event_time"),
                row.get("lat"),
                row.get("lon"),
                row.get("event_type"),
                row.get("city"),
                row.get("state"),
                row.get("country"),
                row.get("simhash"),
            ],
        )
        uids.append(uid)
    return uids


def connect_sqlite(path: str) -> sqlite3.Connection:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS articles (id TEXT PRIMARY KEY, title TEXT, summary TEXT)"
    )
    cur.execute(
        "CREATE VIRTUAL TABLE IF NOT EXISTS articles_fts USING fts5(title, summary, content='articles', content_rowid='rowid')"
    )
    conn.commit()
    return conn


def upsert_article(conn: sqlite3.Connection, uid: str, title: str, summary: str) -> None:
    cur = conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO articles(id, title, summary) VALUES(?,?,?)",
        (uid, title, summary),
    )
    conn.commit()


def search_articles(conn: sqlite3.Connection, query: str) -> List[str]:
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT id FROM articles WHERE id IN (SELECT rowid FROM articles_fts WHERE articles_fts MATCH ?)",
        (query,),
    ).fetchall()
    return [r[0] for r in rows]
