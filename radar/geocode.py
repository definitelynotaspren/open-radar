"""Geocoding with SQLite cache."""
from __future__ import annotations

from datetime import datetime
import sqlite3
import time
from typing import Tuple

try:  # pragma: no cover - optional
    from geopy.geocoders import Nominatim  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    Nominatim = None  # type: ignore


class GeoCoder:
    def __init__(self, cache_sqlite_path: str, user_agent: str = "open-radar"):
        self.conn = sqlite3.connect(cache_sqlite_path)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS geocache (
                query TEXT PRIMARY KEY,
                lat REAL,
                lon REAL,
                accuracy REAL,
                ts TIMESTAMP
            )
            """
        )
        self.conn.commit()
        if Nominatim is None:
            class _Dummy:
                def geocode(self, _query: str):
                    return None

            self.geocoder = _Dummy()
        else:
            self.geocoder = Nominatim(user_agent=user_agent)

    def geocode(self, text: str) -> Tuple[float | None, float | None, float | None]:
        cur = self.conn.execute("SELECT lat, lon, accuracy FROM geocache WHERE query=?", (text,))
        row = cur.fetchone()
        if row:
            return row
        time.sleep(1)
        try:
            loc = self.geocoder.geocode(text)
        except Exception:
            loc = None
        if loc:
            lat = loc.latitude
            lon = loc.longitude
            accuracy = loc.raw.get("importance") if hasattr(loc, "raw") else None
        else:
            lat = lon = accuracy = None
        self.conn.execute(
            "INSERT OR REPLACE INTO geocache(query, lat, lon, accuracy, ts) VALUES(?,?,?,?,?)",
            (text, lat, lon, accuracy, datetime.utcnow()),
        )
        self.conn.commit()
        return lat, lon, accuracy
