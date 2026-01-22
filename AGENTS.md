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

### XLSX Generation
- Use `src.utils.xlsx_formatting.XLSXFormatter` for Excel output
- Follow KiotViet templates in `src.erp.templates` module
- Apply styling: headers with bold font, gray background, centered alignment
- Write to `data/03-erp-export/` directory
