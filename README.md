# Google Workspace Automation

Automated workflows for Google Workspace using Python. Handles data ingestion from Google Drive, cleaning, and analysis.

## Structure

- **Types** - Python data pipeline (uv package manager)
- **`AGENTS.md`** - Development guidelines and setup instructions

## Setup

### Prerequisites

- Python 3.14+
- Google Account with Drive access

### Installation

```bash
# Setup Python environment
uv sync
```

## Python Data Pipeline

Orchestrates a three-stage data pipeline: **ingest** → **clean** → **combine**

### Quick Start

```bash
# Run full pipeline
uv run python main.py

# Test mode (download 1 of each file type)
uv run python pipeline.py --test

# Clear old raw data and ingest fresh
uv run python pipeline.py --clean-up
```

### Pipeline Stages

1. **Ingest** - Downloads CSV files from Google Drive to `data/raw/`
2. **Clean** - Processes and validates data from `data/raw/` → `data/interim/`
3. **Combine** - Merges cleaned data by type into `data/final/`

### Individual Commands

```bash
# Ingest only
uv run python ingest.py

# Skip ingestion, clean existing data
uv run python pipeline.py --skip-ingest

# Skip combine step
uv run python pipeline.py --skip-combine
```

### Data Structure

```
data/
├── raw/       # Raw CSV files from Google Drive
├── interim/   # Cleaned and validated CSV files
└── final/     # Combined/aggregated CSV files
```

### Supported File Types

- `CT.NHAP` - Import data
- `CT.XUAT` - Export data
- `XNT` - Other data type

Files are named: `{YEAR}_{MONTH}_{TYPE}.csv` (e.g., `2023_5_CT.NHAP.csv`)

### Configuration

Dataset cleaning rules defined in `cleaning_configs.py`:

```python
CONFIGS = {
    'CT.NHAP': {
        'header_rows': [3, 4],
        'numeric_cols': [...],
        'date_cols': {...},
        'key_col': 'mã_hh',
    },
}
```

To add new dataset types:
1. Add config to `CONFIGS` in `cleaning_configs.py`
2. Name raw files with the matching suffix
3. Pipeline auto-routes by suffix match

## Dependencies

- `google-api-python-client` - Google Drive API
- `google-auth-oauthlib` - Google authentication
- `pandas` - Data manipulation

## License

ISC

## Author

Lê Quang Phú
