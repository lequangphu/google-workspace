# AGENTS.md (/python)

## Overview
Python scripts for data processing and analysis in Google Workspace workflows.

## Requirements
- Python 3.10+
- uv (install: `curl -LsSf https://astral.sh/uv/install.sh | sh`)

## Python Tooling

Always use `uv` for package management:
- `uv add <package>` - Add dependencies to pyproject.toml
- `uv sync` - Install from pyproject.toml
- `uv run <script.py>` - Run scripts in the project environment
- `uv venv` - Create virtual environments

Do NOT use `pip install` or `poetry`.

## Setup
```bash
uv sync  # Install project dependencies
```

## Running Scripts
**Always use `uv run` to execute any Python script.** Never use `python` directly.

```bash
uv run script_name.py
```

## Scripts

### ingest.py
Downloads data from Google Sheets to `/data/raw/` directory.
- Uses Google Drive API to find and download spreadsheets
- Extracts specified sheet tabs (CT.NHAP, CT.XUAT, XNT)
- Compares remote modified times to skip unchanged files
- Run: `uv run pipeline.py --step ingest`

### clean_chung_tu_nhap.py
Cleans import receipt (Chứng từ nhập) data.
- Combines multi-level headers
- Parses and validates dates
- Standardizes columns and data types
- Outputs to `/data/final/`
- Run: `uv run clean_chung_tu_nhap.py`

### clean_chung_tu_xuat.py
Cleans export receipt (Chứng từ xuất) data.
- Handles different header patterns
- Processes dates and year/month validation
- Standardizes columns and data types
- Outputs to `/data/final/`
- Run: `uv run clean_chung_tu_xuat.py`

### clean_xuat_nhap_ton.py
Cleans inventory (Xuất nhập tồn) data.
- Processes inventory movement data
- Calculates profit margins
- Drops empty inventory rows
- Outputs to `/data/final/`
- Run: `uv run clean_xuat_nhap_ton.py`

### pipeline.py
Orchestrates the full data pipeline with conditional execution.

**Full Pipeline** (ingest → clean → upload):
```bash
uv run pipeline.py --full
```
Or simply: `uv run pipeline.py`

**Individual Steps**:
- Ingest only: `uv run pipeline.py --step ingest`
- Clean only: `uv run pipeline.py --step clean`
- Upload only: `uv run pipeline.py --step upload`

**Workflow**:
1. **Ingest**: Downloads from Google Sheets to `/data/raw/`
2. **Clean**: Runs all three cleaners IF:
   - `/data/final/` doesn't exist, OR
   - Raw files are newer than final files
3. **Upload**: Uploads final files to Google Drive IF clean succeeded
   - Replaces existing files in destination folder
   - Upload destination: https://drive.google.com/drive/folders/1crSQgdCZdrI5EwE1zf7soYX4xgjTM6G5
