import json
from pathlib import Path

from radar import sources, geocode
import ingest


def test_ingest_smoke(tmp_path, monkeypatch):
    rss1 = (Path(__file__).parent / "fixtures/rss1.xml").as_uri()
    rss2 = (Path(__file__).parent / "fixtures/rss2.xml").as_uri()
    cfg = {
        "sources": {"rss": [rss1, rss2], "json": []},
        "duckdb_path": str(tmp_path / "events.db"),
        "sqlite_path": str(tmp_path / "articles.db"),
        "vault_path": str(tmp_path / "vault"),
        "geojson_output": str(tmp_path / "events.geojson"),
        "csv_output": str(tmp_path / "events.csv"),
    }
    monkeypatch.setattr(sources, "fetch_article_html", lambda url: "")
    monkeypatch.setattr(geocode.GeoCoder, "geocode", lambda self, text: (0.0, 0.0, 1.0))
    ingest.run_pipeline(cfg, dry_run=False, since=None)
    data = json.loads(Path(cfg["geojson_output"]).read_text())
    assert data["features"]
