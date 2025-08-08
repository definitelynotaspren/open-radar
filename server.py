"""Streamlit UI for exploring events."""
from __future__ import annotations

import subprocess
from datetime import datetime
from pathlib import Path

import duckdb  # type: ignore[import-not-found]
import pandas as pd  # type: ignore[import-untyped]
import streamlit as st  # type: ignore[import-not-found]
import yaml  # type: ignore[import-untyped]
import pydeck as pdk  # type: ignore[import-not-found]

from radar import export, store


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_events(duck_path: str) -> pd.DataFrame:
    return duckdb.connect(duck_path).execute("SELECT * FROM events").fetchdf()


def main() -> None:
    st.set_page_config(layout="wide")
    cfg_path = st.sidebar.text_input("Config path", "config.yaml")
    cfg = load_config(cfg_path)
    df = load_events(cfg["duckdb_path"])

    # Filters
    min_date, max_date = df["event_time"].min(), df["event_time"].max()
    start, end = st.sidebar.date_input("Date range", [min_date, max_date])
    sources = st.sidebar.multiselect("Source", df["source"].unique())
    types = st.sidebar.multiselect("Event type", df["event_type"].unique())
    query = st.sidebar.text_input("Text search")
    if query:
        ids = store.search_articles(store.connect_sqlite(cfg["sqlite_path"]), query)
        df = df[df["id"].isin(ids)]
    mask = (df["event_time"].dt.date >= start) & (df["event_time"].dt.date <= end)
    if sources:
        mask &= df["source"].isin(sources)
    if types:
        mask &= df["event_type"].isin(types)
    df = df[mask]

    # Map
    st.pydeck_chart(
        pdk.Deck(
            map_style="mapbox://styles/mapbox/light-v9",
            initial_view_state=pdk.ViewState(latitude=0, longitude=0, zoom=2),
            layers=[
                pdk.Layer(
                    "ScatterplotLayer",
                    data=df,
                    get_position="[lon, lat]",
                    get_color="[200, 30, 0, 160]",
                    get_radius=200,
                )
            ],
        )
    )

    st.dataframe(df)

    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("Export GeoJSON"):
            export.to_geojson(df, cfg.get("geojson_output", "events.geojson"))
    with col2:
        if st.button("Export CSV"):
            export.to_csv(df, cfg.get("csv_output", "events.csv"))
    with col3:
        selected = st.multiselect("Select IDs", df["id"].tolist())
        if st.button("Export to Obsidian") and cfg.get("vault_path"):
            for _, row in df[df["id"].isin(selected)].iterrows():
                export.to_obsidian_note(row, cfg["vault_path"])

    st.sidebar.write("Last run:", datetime.fromtimestamp(Path(cfg["duckdb_path"]).stat().st_mtime))
    if st.sidebar.button("Run update now"):
        subprocess.run(["python", "ingest.py", "--config", cfg_path, "--update"], check=False)
        st.experimental_rerun()


if __name__ == "__main__":
    main()
