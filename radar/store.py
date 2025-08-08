"""Storage helpers for DuckDB and SQLite FTS."""
from __future__ import annotations

from typing import List, Dict
import sqlite3

import duckdb  # type: ignore[import-not-found]
import pandas as pd  # type: ignore[import-untyped]

EVENT_COLUMNS = [
    "id",
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
    conn = duckdb.connect(path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id BIGINT,
            source TEXT,
            title TEXT,
            link TEXT,
            summary TEXT,
            event_time TIMESTAMP,
            lat DOUBLE,
            lon DOUBLE,
            event_type TEXT,
            city TEXT,
            state TEXT,
            country TEXT,
            simhash BIGINT
        )
        """
    )
    return conn


def insert_events(conn: duckdb.DuckDBPyConnection, rows: List[Dict]) -> List[int]:
    if not rows:
        return []
    df = pd.DataFrame(rows, columns=EVENT_COLUMNS[1:])
    row = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM events").fetchone()
    start_id = int(row[0]) if row else 1
    df.insert(0, "id", range(start_id, start_id + len(df)))
    conn.executemany(
        "INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        df.values.tolist(),
    )
    return df["id"].tolist()


def connect_sqlite(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS articles (id INTEGER PRIMARY KEY, title TEXT, summary TEXT)"
    )
    cur.execute(
        "CREATE VIRTUAL TABLE IF NOT EXISTS articles_fts USING fts5(title, summary, content='articles', content_rowid='id')"
    )
    conn.commit()
    return conn


def upsert_article(conn: sqlite3.Connection, id_: int, title: str, summary: str) -> None:
    cur = conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO articles(id, title, summary) VALUES(?,?,?)",
        (id_, title, summary),
    )
    cur.execute(
        "INSERT OR REPLACE INTO articles_fts(rowid, title, summary) VALUES(?,?,?)",
        (id_, title, summary),
    )
    conn.commit()


def search_articles(conn: sqlite3.Connection, query: str) -> List[int]:
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT rowid FROM articles_fts WHERE articles_fts MATCH ?", (query,)
    ).fetchall()
    return [r[0] for r in rows]
