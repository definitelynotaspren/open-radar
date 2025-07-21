## Test Log for open-radar

### Setup
- Installed requirements with `pip install -r requirements.txt`. Packages installed successfully.

### Running ingest.py
- Executed `python ingest.py`.
- No data was downloaded because sources in `config.yaml` point to placeholder URLs. Network requests to `example.com` were blocked by environment policy.
- The script created empty output files under `data/` and printed "Ingestion complete.". Deprecation warnings were shown for `datetime.utcnow()`.

### Running API server
- Started the FastAPI app using `uvicorn server:app`.
- Requested `/download/public` which returned a 500 error because `/tmp/public.geojson` does not exist. Log excerpt:

```
RuntimeError: File at path /tmp/public.geojson does not exist.
```

### Observations
- `server.py` expects files under `/tmp/` that are never written by `ingest.py`, causing the download endpoints to fail.
- Using `datetime.utcnow()` triggers deprecation warnings; consider timezone-aware datetimes.
- Network requests to placeholder endpoints are blocked, so example data must be provided locally or via allowed URLs for testing.
