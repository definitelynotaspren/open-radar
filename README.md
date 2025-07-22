# Open Radar
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/definitelynotaspren/open-radar)

This project ingests event data from multiple sources and exports both GeoJSON and CSV artifacts. The resulting files can be visualized in the provided web map or imported into external GIS tools.

## Running

Install requirements with `pip install -r requirements.txt` then run `python ingest.py` to generate GeoJSON and CSV outputs.
You can also start the FastAPI server for interactive ingestion using `uvicorn server:app`.

## Output

- `data/geojson/merged_events.geojson` – combined events
- `data/events.csv` – CSV export compatible with tools like GRASS GIS

## Public data sources for testing

The default configuration uses placeholder URLs. Below are freely available data sets that can be used during testing:

- **US state boundaries** – `https://raw.githubusercontent.com/PublicaMundi/MappingAPI/master/data/geojson/us-states.json`
- **Airports** – `https://raw.githubusercontent.com/datasets/airport-codes/master/data/airport-codes.csv`
- **Example flight paths** – `https://raw.githubusercontent.com/plotly/datasets/master/2011_february_aa_flight_paths.csv`

These data sets can populate the RSS, flight, or permit fields by placing the files under a local web server or adjusting `config.yaml` to point directly at the files.

### Using with GRASS GIS

The exported `events.csv` file contains longitude and latitude columns named `longitude` and `latitude`. Import it using:

```bash
v.in.csv input=data/events.csv x=longitude y=latitude output=events
```

GeoJSON outputs can be imported with `v.import` if the GDAL library is available.

## Automated ingestion with GitHub Actions

The repository includes a workflow that runs `ingest.py` on a schedule. The
workflow expects two secrets to be defined in the repository settings:

* `FLIGHT_DATA_URL` – private endpoint for flight data
* `PERMIT_DATA_URL` – private endpoint for permit data

These secrets are exposed as environment variables during the run so that the
placeholders in `config.yaml` can be resolved. The workflow installs the Python
dependencies, executes the ingestion script, and commits any updated GeoJSON or
CSV files back to the repository.

You can also trigger the job manually from the "Actions" tab.


The private dataset is available to authorized users. For access requests please email [woolnthorn@gmail.com](mailto:woolnthorn@gmail.com).
