# Agent Guidelines

This file provides build commands, testing instructions, and code style guidelines for AI agents working on this codebase.

## Build / Lint / Test Commands

```bash
# Run all tests
pytest

# Run single test file
pytest tests/test_ingest.py

# Run specific test
pytest tests/test_ingest.py::TestParseFileMetadata::test_valid_metadata

# Run tests matching pattern
pytest tests/test_ingest.py -k "parse"

# Run with verbose output
pytest -v

# Lint code
ruff check .

# Format code
ruff format .

# Run orchestrator (full pipeline)
uv run src/pipeline/orchestrator.py

# Run orchestrator with specific modules
uv run src/pipeline/orchestrator.py -m ier

# Run specific steps
uv run src/pipeline/orchestrator.py -s ingest,transform
uv run src/pipeline/orchestrator.py -t  # transform only
uv run src/pipeline/orchestrator.py -e  # export only

# Run module scripts directly
uv run src/modules/ingest.py
uv run src/modules/receivable/generate_customers_xlsx.py
```

## Code Style Guidelines

### File Headers
- Files with Vietnamese text must include: `# -*- coding: utf-8 -*-`
- Start with module-level docstring explaining purpose
- Include source data references where applicable

### Imports
```python
# Standard library first
import logging
import sys
from pathlib import Path
from typing import Optional, List, Dict

# Third-party imports next
import pandas as pd
from openpyxl import Workbook

# Local imports last (src.*)
from src.modules.google_api import connect_to_drive
from src.utils.staging_cache import StagingCache
```

### Type Hints
- Use type hints for function signatures: `def process(data: pd.DataFrame) -> Optional[Path]:`
- Import from `typing`: `Optional`, `List`, `Dict`, `Any`, `TypedDict`
- Use `from pathlib import Path` for file paths

### Naming Conventions
- Functions/variables: `snake_case`
- Classes: `PascalCase`
- Constants: `UPPER_SNAKE_CASE`
- Module-level configs: `_CONFIG` (private), `CONFIG` (public)

### Docstrings
```python
def process_data(source: pd.DataFrame) -> Optional[dict]:
    """Process data from source.

    Args:
        source: Input DataFrame with columns 'code', 'name'.

    Returns:
        Dict with 'processed' DataFrame and 'errors' list, or None on failure.
    """
```

### Error Handling
- Use `try/except` blocks with logging
- Catch specific exceptions where possible
- Return `None` or empty results on non-critical failures
- Raise on critical failures that should stop execution
- Use `logger.error()` for errors, `logger.warning()` for warnings

### Logging
- Initialize at module level: `logger = logging.getLogger(__name__)`
- Use structured logging with context
- Log before API calls, after successful operations, on errors

### Configuration
- Load from `pipeline.toml` using `tomllib.load()`
- Store config in module-level variables: `_CONFIG = load_pipeline_config()`
- Provide defaults when config missing: `_CONFIG.get("key", "default")`

### Path Handling
- Use `Path` objects from `pathlib`
- Use `get_workspace_root()` from `src.utils` for project root
- Use `Path.cwd()` when running scripts directly
- **IMPORTANT:** Never hardcode directory names (e.g., "import_export")
- Use `PathConfig` from `src.utils.path_config` for all directory paths

### PathConfig Usage
`PathConfig` provides centralized path configuration from `pipeline.toml`, eliminating hardcoded paths and tight coupling between modules.

**Import and initialize:**
```python
from src.utils.path_config import PathConfig

# Initialize once at module level or in __init__
path_config = PathConfig()
```

**Available methods:**

| Method | Returns | Description |
|--------|---------|-------------|
| `get_raw_output_dir(source_key)` | Path | Raw directory for a source (e.g., import_export_receipts) |
| `get_staging_output_dir(source_key)` | Path | Staging directory for a source |
| `import_export_staging_dir()` | Path | Staging/import_export (convenience method) |
| `import_export_raw_dir()` | Path | Raw/import_export (convenience method) |
| `receivable_raw_dir()` | Path | Raw/receivable (convenience method) |
| `payable_raw_dir()` | Path | Raw/payable (convenience method) |
| `cashflow_raw_dir()` | Path | Raw/cashflow (convenience method) |

**Usage examples:**
```python
# Get import_export raw directory
raw_dir = path_config.import_export_raw_dir()
csv_path = raw_dir / f"{year}_{month}_{tab}.csv"

# Get any source's raw directory
raw_dir = path_config.get_raw_output_dir("receivable")
csv_path = raw_dir / "receivable_summary.csv"

# Access base directories
path_config.raw_data_dir      # data/00-raw
path_config.staging_data_dir   # data/01-staging
path_config.validated_data_dir # data/02-validated
path_config.erp_export_dir     # data/03-erp-export
```

**Why use PathConfig?**
- Single source of truth for all directory paths
- Eliminates tight coupling between modules (resolves TODO #008)
- Changing directory structure requires only updating `pipeline.toml`
- Prevents path duplication across the codebase
- Improves testability (mock PathConfig instead of multiple hardcoded paths)

**ANTI-PATTERN: Hardcoded paths (DO NOT DO THIS):**
```python
# BAD: Hardcoded directory name
csv_path = STAGING_DATA_DIR / "import_export" / f"{year}_{month}_{tab}.csv"

# GOOD: Use PathConfig
csv_path = path_config.import_export_raw_dir() / f"{year}_{month}_{tab}.csv"
```

### Data Flow for Import/Export Receipts (TODO #003 Resolved)
**Status:** Resolved - All sources now follow consistent raw→staging→export flow

**Historical Context (Pre-TODO #003):**
- Import/export receipts wrote directly to `data/01-staging/import_export/` (bypassing raw layer)
- This eliminated forensic audit trail and created architectural inconsistency
- Documented as ADR-001 with risks and migration plan

**Current Architecture (Post-TODO #003):**
- All sources (import_export_receipts, receivable, payable, cashflow) write to `data/00-raw/`
- Downstream scripts read from raw directory for processing
- Consistent data flow: `Google Sheets → data/00-raw/ → transform/export → data/03-erp-export/`

**Benefits:**
- **Security:** Maintains forensic audit trail for compliance (SOX, GDPR)
- **Consistency:** All sources follow same pipeline architecture
- **Recoverability:** Raw backup exists if Google Sheets are modified/deleted
- **Simplified:** No special case handling for import_export in codebase

**Implementation Details:**
- `ingest.py`: Writes import_export data to `data/00-raw/import_export/` via `path_config.get_raw_output_dir("import_export_receipts")`
- `data_loader.py`: Reads import_export from `path_config.import_export_raw_dir()`
- `generate_products_xlsx.py`: Reads from `CONFIG["raw_dir"]` (defaults to `data/00-raw/import_export/`)

**Testing:**
- Tests verify import_export data flows correctly through pipeline
- All existing tests updated to use raw directory path
- Schema validation still applies to import_export raw files

### Testing
- Use pytest with `@pytest.fixture` for common setup
- Mock Google Drive API to prevent actual calls
- Use descriptive test names
- Group tests in classes by functionality
- Mock external dependencies: `@patch("src.modules.google_api.connect_to_drive")`

### Pandas / Data Processing
- Use `pd.to_numeric(..., errors="coerce")` for safe numeric conversion
- Handle missing values: `df.fillna("")` for strings, `df.fillna(0)` for numeric
- Use `df.astype(str).str.strip()` for text cleaning
- Use `df.drop_duplicates()` to deduplicate

### Google API
- Rate limit: respect `API_CALL_DELAY = 0.5` between calls
- Use `@retry_api_call` decorator for retry logic
- Handle `HttpError` with appropriate logging
- Use batch operations for multiple files where possible

### Module Patterns
- Scripts should have a `process()` function that accepts optional parameters
- Return `Path` to output file on success, `None` on failure
- Use `write_to_sheets=False` parameter to control Google Sheets upload
- Include `staging_dir: Optional[Path]` parameter for custom staging paths

### StagingCache Usage
- `StagingCache` provides LRU caching for staging CSV files
- Cache size limited to 50 files (configurable via `CACHE_MAXSIZE` constant)
- Returns reference to cached DataFrame (caller must copy if modifying)
- Use `StagingCache.invalidate()` to clear cache
- Use `StagingCache.get_cache_info()` for metrics (hit rate, cached files, etc.)
- No modification time tracking - cache invalidated by LRU eviction or manual clear

```python
from src.utils.staging_cache import StagingCache

# Get DataFrame (cached with LRU eviction)
df = StagingCache.get_dataframe(staging_path)

# If you need to modify the data, make a copy first
df_modified = StagingCache.get_dataframe(staging_path).copy()

# Clear entire cache
StagingCache.invalidate()

# Check cache metrics
info = StagingCache.get_cache_info()
# Returns: {'cached_files': 42, 'maxsize': 50, 'hits': 1234, 'misses': 56, 'hit_rate': 0.96}
```

### XLSX Generation
- Use `src.utils.xlsx_formatting.XLSXFormatter` for Excel output
- Follow KiotViet templates in `src.erp.templates` module
- Apply styling: headers with bold font, gray background, centered alignment
- Write to `data/03-erp-export/` directory

### Architecture Decision Records (ADRs)
- ADRs document significant architectural decisions with context and rationale
- Located in `docs/adr/` directory with numbered filenames (e.g., `001-import-export-direct-staging.md`)
- Consult relevant ADRs before making architectural changes
- When encountering architectural deviations in code (TODO comments with ADR references),
  read the ADR to understand the decision and migration plan
- Current ADRs:
  - **ADR-001:** Import/Export Receipts Direct Staging (docs/adr/001-import-export-direct-staging.md)
    - Historical ADR documenting why import_export previously wrote directly to staging
    - **RESOLVED by TODO #003:** Now all sources follow consistent raw→staging→export flow
    - Migration plan completed: ingest.py writes to raw, downstream scripts read from raw
    - Reference this ADR for historical context on architectural decision
  - **ADR-005:** Source Type Configuration (TODO #005 - RESOLVED)
    - Implements declarative source_type field in pipeline.toml for all sources
    - "preprocessed": Data already clean, skip transform (e.g., import_export_receipts)
    - "raw": Data needs transformation to staging (e.g., receivable, payable, cashflow)
    - All sources now write to raw directory consistently (no special cases in ingest.py)
    - orchestrator.py checks source_type and skips transform for "preprocessed" sources
    - data_loader.py reads from correct location based on source_type (raw or staging)
    - Resolves SOLID violations (Single Responsibility, Open/Closed) for source handling
    - New sources can be added without modifying ingest.py code
    - Tests in tests/test_source_type_config.py verify source_type behavior

### Path Security and Validation
- All file paths constructed from external data must be validated before use
- Use `_validate_year_month()` in ingest.py to validate year (2020-2030) and month (1-12) ranges
- Use `.resolve()` to normalize paths and remove `..` segments
- Verify resolved paths are within expected directories using prefix checks
- Year/month validation protects against path traversal attacks (CWE-22)
- Always validate before constructing paths from user-controlled or external data
- Example pattern:
  ```python
  _validate_year_month(year_num, month)
  csv_path = (STAGING_DATA_DIR / "import_export" / f"{year_num}_{month}_{tab}.csv").resolve()
  if not str(csv_path).startswith(str(STAGING_DATA_DIR.resolve())):
      raise ValueError(f"Invalid path: {csv_path} escapes staging directory")
  ```

### Schema Validation
- Schema validation ensures CSV files from Google Sheets meet expected structure before entering staging
- All import_export_receipts tabs validated after export in `ingest.py`
- Invalid files are deleted before reaching staging (fail-fast behavior)
- Quarantine directory for rejected files: `data/00-rejected/`

**Tab Types and Required Columns:**

| Tab Type | Required Columns | Numeric Columns |
|----------|------------------|-----------------|
| Chi tiết nhập | Ngày, Mã hàng, Tên hàng, Số lượng, Đơn giá, Thành tiền | Số lượng, Đơn giá, Thành tiền |
| Chi tiết xuất | Ngày, Mã hàng, Tên hàng, Số lượng, Đơn giá, Thành tiền | Số lượng, Đơn giá, Thành tiền |
| Xuất nhập tồn | Mã hàng, Tên hàng, Tồn cuối kỳ, Giá trị cuối kỳ | Tồn cuối kỳ, Giá trị cuối kỳ |
| Chi tiết chi phí | Mã hàng, Tên hàng, Số tiền, Diễn giải | Số tiền |

**Validation Rules:**
- Required columns must exist in CSV
- Numeric columns must contain valid numeric data (not all NaN)
- Forbidden values are rejected: `0`, `None`, `-1`, `-999` in numeric columns
- Empty CSV files (no data rows) are rejected
- Validation errors are logged with specific missing columns or values

**Usage:**
```python
from src.pipeline.validation import validate_schema

if not validate_schema(csv_path, tab_name):
    logger.error(f"Schema validation failed: {csv_path}")
    csv_path.unlink()  # Delete invalid file
```

**Schema Definitions:** Located in `src/pipeline/validation.py` under `EXPECTED_SCHEMAS`

**Testing:** Tests in `tests/test_validation.py` verify all validation rules and quarantine behavior
