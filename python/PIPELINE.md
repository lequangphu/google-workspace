# Data Pipeline

The pipeline orchestrates three stages: **ingest** → **clean** → **combine**

## Files

- **`main.py`** - Entry point (calls `pipeline.py`)
- **`pipeline.py`** - Orchestrates the full workflow
- **`ingest.py`** - Downloads Google Sheets to `data/raw/`
- **`clean_all_data.py`** - Batch cleaning: `data/raw/` → `data/interim/`
- **`data_cleaner.py`** - Generic CSV cleaner class
- **`cleaning_configs.py`** - Dataset-type configurations
- **`data_cleaning_utils.py`** - Shared utility functions

## Data Directories

```
data/
├── raw/        # Raw CSV files from Google Drive
├── interim/    # Cleaned CSV files
└── final/      # Combined/aggregated CSV files
```

## Usage

### Run Full Pipeline
```bash
# Ingest from Drive, clean, and combine
uv run python main.py

# Test mode (download 1 of each file type)
uv run python pipeline.py --test

# Clear old raw data and ingest fresh
uv run python pipeline.py --clean-up
```

### Run Individual Steps
```bash
# Ingest only
uv run python ingest.py

# Clean only (with existing raw data)
uv run python pipeline.py --skip-ingest

# Clean and combine (skip ingestion)
uv run python pipeline.py --skip-ingest

# Skip combining
uv run python pipeline.py --skip-combine
```

## Pipeline Steps

### 1. Ingest (`raw`)
- Downloads Google Sheets from Drive
- Exports to `data/raw/*.csv`
- Filename format: `{YEAR}_{MONTH}_{TYPE}.csv` (e.g., `2023_5_CT.NHAP.csv`)
- Supported types: `CT.NHAP`, `CT.XUAT`, `XNT`

### 2. Clean (`interim`)
- Reads all CSVs from `data/raw/`
- Routes by suffix to appropriate config
- Cleans headers, cells, converts types
- Drops empty rows/columns
- Saves to `data/interim/*.csv`

### 3. Combine (`final`)
- Placeholder for future implementation
- Will merge interim files by type
- Output to `data/final/*.csv`

## Configuration

Dataset configs in `cleaning_configs.py`:

```python
CONFIGS = {
    'CT.NHAP': {
        'header_rows': [3, 4],
        'numeric_cols': [...],
        'date_cols': {...},
        'key_col': 'mã_hh',
    },
    # ... more configs
}
```

Add new dataset types by:
1. Adding suffix pattern to `CONFIGS`
2. Naming raw files: `{YEAR}_{MONTH}_{SUFFIX}.csv`
3. Pipeline auto-routes by suffix match
