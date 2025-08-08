"""Export helpers."""
from __future__ import annotations

from pathlib import Path
import json
import pandas as pd  # type: ignore[import-untyped]


def to_geojson(df: pd.DataFrame, path: str) -> None:
    features = []
    for _, row in df.iterrows():
        if pd.isna(row.get("lat")) or pd.isna(row.get("lon")):
            continue
        props = row.to_dict()
        if isinstance(props.get("event_time"), pd.Timestamp):
            props["event_time"] = str(props["event_time"])
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [row["lon"], row["lat"]],
                },
                "properties": props,
            }
        )
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump({"type": "FeatureCollection", "features": features}, f)


def to_csv(df: pd.DataFrame, path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def to_obsidian_note(row: pd.Series, vault_path: str) -> Path:
    date = pd.to_datetime(row["event_time"]).date()
    directory = Path(vault_path) / "News" / "Events" / str(date)
    directory.mkdir(parents=True, exist_ok=True)
    slug = "".join(c for c in row["title"] if c.isalnum() or c in (" ", "-"))[:50].strip().replace(" ", "-")
    note_path = directory / f"{slug}.md"
    frontmatter = {
        "title": row["title"],
        "source": row["source"],
        "link": row["link"],
        "event_time": str(row["event_time"]),
        "lat": row.get("lat"),
        "lon": row.get("lon"),
        "event_type": row.get("event_type"),
        "city": row.get("city"),
        "state": row.get("state"),
        "country": row.get("country"),
    }
    with open(note_path, "w") as f:
        f.write("---\n")
        for k, v in frontmatter.items():
            f.write(f"{k}: {v}\n")
        f.write("---\n\n")
        f.write(str(row.get("summary", "")))
    return note_path
