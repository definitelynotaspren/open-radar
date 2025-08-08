python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m spacy download en_core_web_sm

@"
sources:
  rss:
    - https://news.un.org/feed/subscribe/en/news/all/rss.xml
  json: []
duckdb_path: data/events.db
sqlite_path: data/articles.db
vault_path: vault
geojson_output: data/events.geojson
csv_output: data/events.csv
"@ | Out-File -Encoding UTF8 config.yaml

python ingest.py --update
