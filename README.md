# IDS-DRR Flood Risk Pipeline

A Dagster-based data pipeline for automating multi-state flood risk data collection, transformation, and risk score calculation for Indian states.

## States Covered

- Himachal Pradesh
- Assam
- Odisha
- Bihar
- Uttar Pradesh

## Architecture

The pipeline uses Dagster's asset-based architecture with:
- **Multi-dimensional partitions**: State × Month combinations using `MultiPartitionsDefinition`
- **Config-driven design**: State-specific configurations in YAML files
- **Layered scheduling**: Daily/weekly extraction feeding into monthly aggregation

### Risk Model Components

1. **Hazard Factor**: 
2. **Vulnerability Factor**:
3. **Exposure Factor**: 
4. **Government Response**: 

## Quick Start

### Local Development

```bash
# Install dependencies
pip install .

# Seed reference data + historical variables (required before first run)
python scripts/seed_data.py

# Start Dagster UI
dagster dev -m pipeline.definitions

# Or use Docker Compose
docker-compose -f docker-compose.dev.yaml up
```

### Data areas (not committed)

Nothing under `data/reference/`, `staging/`, or `inbox/` lives in git.
`scripts/seed_data.py` materialises all of it from the public
flood-data-ecosystem and risk-score-model repositories (pinned revisions):

- `data/reference/<state>/` — admin boundaries, gazetteers, one-time
  variables (elevation, drainage density, Antyodaya, …) and the golden
  fixtures used by the parity tests.
- `staging/variables/<state>/<source>/<var>/<var>_YYYY_MM.csv` — the
  per-variable monthly contract every extraction asset writes to;
  the seed script backfills 2021-04 → present history.
- `inbox/<state>/<source>/` — drop folders for manual sources (NRSC
  runoff, HPSDMA losses, tender exports). Files named
  `<variable>_YYYY_MM.csv` are validated, promoted into staging, and
  moved to `processed/` by the inbox sensor.

### Running Tests

```bash
pytest tests/ -v
```

### Integration Tests for a Specific State

```bash
pytest tests/integration --state himachal_pradesh
```

## Project Structure

```
flood_risk_pipeline/
├── config/
│   ├── sources.yaml          # Shared data source definitions
│   └── states/               # State-specific configurations
├── assets/
│   ├── extraction/           # Data collection assets
│   ├── transformation/       # Risk factor computation
│   └── outputs/              # Final outputs
├── resources/
│   ├── apis.py               # API connections
│   └── storage.py            # S3/GCS configuration
└── definitions.py            # Dagster entry point
```

## Adding a New State

1. Create a new YAML config in `config/states/`
2. Add the state to `partitions.py` STATE_SOURCES
3. (Optional) Create custom extraction in `assets/extraction/custom/`

## Schedules

- **Daily Collection** (2 AM): Weather and satellite data
- **Weekly Collection** (Sundays 2 AM): Flood maps, reservoir levels
- **Monthly Aggregation** (1st of month 2 AM): Risk model execution

## License

MIT
