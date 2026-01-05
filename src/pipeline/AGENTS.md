# src/pipeline - Pipeline Orchestration

**Purpose**: Central controller for 4-stage data pipeline flow with CLI interface.

## STRUCTURE
```
src/pipeline/
├── orchestrator.py    # Main entry point: CLI, pipeline steps, Google Drive integration
└── __init__.py
```

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Run full pipeline | `orchestrator.py` | CLI: `uv run src/pipeline/orchestrator.py [--step transform] [--period 2025_01]` |
| Pipeline stages | `orchestrator.py` | 4 stages: ingest → transform → validate → export |
| CLI argument parsing | `orchestrator.py:main()` | Handles --step, --period, --skip-download, --skip-upload |
| Legacy script fallback | `orchestrator.py` | Falls back to legacy scripts if migrated versions fail |

## CONVENTIONS (Pipeline-Specific)

### Pipeline Flow
Strict 4-stage invariant:
1. **Ingest** → `data/00-raw/` (Google Drive CSV downloads)
2. **Transform** → `data/01-staging/` (cleaned, standardized)
3. **Validate** → `data/02-validated/` (master data extraction)
4. **Export** → `data/03-erp-export/` (KiotViet XLSX only)

### CLI Interface
```bash
# Run full pipeline
uv run src/pipeline/orchestrator.py

# Specific step only
uv run src/pipeline/orchestrator.py --step transform

# Specific period
uv run src/pipeline/orchestrator.py --period 2025_01

# Skip download/upload
uv run src/pipeline/orchestrator.py --skip-download --skip-upload
```

### mtime-Based Skipping
- Files skipped if Google Drive `modifiedTime` unchanged
- Checks `data/00-raw/` timestamps before re-downloading
- Improves performance for large datasets

## ANTI-PATTERNS (Pipeline-Specific)

### ❌ Hardcoded Configuration (Violates ADR-1)
- **Don't**: Hardcode Google Drive folder IDs or spreadsheet IDs in `orchestrator.py`
- **Do**: Load all IDs from `pipeline.toml`
- **Current Issue**: Lines 128-130 hardcode `GOOGLE_DRIVE_FOLDER_ID` and `GOOGLE_SHEETS_ID_CLEANED`

### ❌ Direct Export Directory Writes
- **Don't**: Write directly to `data/03-erp-export/` from modules
- **Do**: Always go through staging → validate → promote flow
- **Exception**: Legacy scripts may violate (marked for removal)

### ❌ Skipping Validation
- **Don't**: Export XLSX without `ERPTemplateRegistry.validate_dataframe()`
- **Do**: Validate all exports before writing to `data/03-erp-export/`

### ❌ Mixed Responsibilities
- **Don't**: Combine CLI parsing, pipeline logic, and Google Drive integration in one file
- **Do**: Split into separate modules (planned refactoring)

## NOTES

### File Complexity Warning
- `orchestrator.py` (996 lines) violates 300-line limit
- Contains CLI, pipeline logic, and Google Drive integration
- **Planned Refactor**: Split into `cli.py`, `pipeline.py`, `drive_integration.py`

### Legacy Fallback
- Lines 455-485: Falls back to legacy scripts if migrated versions fail
- Ensures backward compatibility during migration
- Legacy scripts in `./legacy/` directory are deprecated
