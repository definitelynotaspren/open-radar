# Open Radar

Open Radar ingests news feeds and exports geocoded events for mapping or note taking.

## Setup (Windows/macOS)

1. Install **Python 3.11**.
2. Create a virtual environment and activate it:
   - **Windows:**
     ```powershell
     python -m venv .venv
     .\.venv\Scripts\Activate.ps1
     ```
   - **macOS:**
     ```bash
     python -m venv .venv
     source .venv/bin/activate
     ```
3. Install dependencies and download the spaCy model:
   ```bash
   pip install -r requirements.txt
   python -m spacy download en_core_web_sm
   ```

## Configuration

Edit `config.yaml` to set:

```yaml
sources:
  rss: ["https://example.com/feed1.xml"]
  json: []
duckdb_path: data/events.db
sqlite_path: data/articles.db
vault_path: /path/to/ObsidianVault
geojson_output: data/events.geojson
csv_output: data/events.csv
```

`vault_path` is optional but enables export of events as Obsidian notes.

## Ingesting

Fetch, geocode and store events:

```bash
python ingest.py --update
```

Additional flags:

- `--dry-run` – process but do not write to databases.
- `--since YYYY-MM-DD` – only process recent items.

## Streamlit dashboard

Launch the interactive dashboard:

```bash
streamlit run server.py
```

Use the sidebar to filter by date, source, event type, or full‑text search (powered by SQLite FTS). Export filtered data to GeoJSON, CSV, or Obsidian.

## Obsidian usage

Events exported to Obsidian appear under `News/Events/YYYY-MM-DD/`. Example Dataview query:

```dataview
table title, event_time
from "News/Events"
```

## Scheduling

Automate ingestion with **Task Scheduler** (Windows) or **cron** (macOS): schedule `python ingest.py --update` at your desired frequency.

## Geocoding policy & cache

Geocoding uses **Nominatim** via `geopy` and stores results in a local SQLite cache to reduce repeated lookups. Please respect the [Nominatim usage policy](https://operations.osmfoundation.org/policies/nominatim/).
