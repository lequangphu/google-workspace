# Ingest CLI Guide

Enhanced command-line interface for `src/modules/ingest.py` with source filtering.

## Quick Start

### Run all sources (default)
```bash
uv run src/modules/ingest.py
```

### Run specific sources only
```bash
# Single source
uv run src/modules/ingest.py --only receivable

# Multiple sources
uv run src/modules/ingest.py --only receivable,payable

# With spaces (auto-stripped)
uv run src/modules/ingest.py --only "receivable, payable, cashflow"
```

### Skip sources
```bash
# Skip one
uv run src/modules/ingest.py --skip import_export_receipts

# Skip multiple
uv run src/modules/ingest.py --skip import_export_receipts,payable
```

---

## Available Sources

```
cashflow
import_export_receipts
payable
receivable
```

---

## All Options

```bash
uv run src/modules/ingest.py --help
```

### Flags

| Flag | Type | Description | Example |
|------|------|-------------|---------|
| `--only` | string | Run only these sources (comma-separated) | `--only receivable,payable` |
| `--skip` | string | Skip these sources (comma-separated) | `--skip import_export_receipts` |
| `--clear-cache` | flag | Clear manifest cache before ingestion | `--clear-cache` |
| `--test-mode` | flag | Stop after downloading one of each tab type | `--test-mode` |
| `--cleanup` | flag | Remove existing data/00-raw/ before starting | `--cleanup` |

---

## Usage Examples

### Common Workflows

**Ingest only customer data**
```bash
uv run src/modules/ingest.py --only receivable
```

**Ingest supplier and customer data (skip transactions)**
```bash
uv run src/modules/ingest.py --only receivable,payable
```

**Ingest everything except transactions**
```bash
uv run src/modules/ingest.py --skip import_export_receipts
```

**Fresh ingest with cache clear**
```bash
uv run src/modules/ingest.py --clear-cache
```

**Test run: download one sample of each tab type**
```bash
uv run src/modules/ingest.py --test-mode
```

**Full refresh: clear data and re-download all**
```bash
uv run src/modules/ingest.py --cleanup --clear-cache
```

**Clear cache, then ingest only customers**
```bash
uv run src/modules/ingest.py --clear-cache --only receivable
```

### Development & Debugging

**Test ingest of receivable only**
```bash
uv run src/modules/ingest.py --only receivable --test-mode
```

**Ingest payable with fresh cache**
```bash
uv run src/modules/ingest.py --only payable --clear-cache
```

**Ingest cashflow and payable with cleanup**
```bash
uv run src/modules/ingest.py --only cashflow,payable --cleanup
```

---

## Validation

### Error Handling

**Invalid source name**
```bash
$ uv run src/modules/ingest.py --only invalid_source
ERROR    Invalid sources: ['invalid_source']. Available: cashflow, import_export_receipts, payable, receivable
```

**Conflicting flags**
```bash
$ uv run src/modules/ingest.py --only receivable --skip payable
ERROR    Cannot use both --only and --skip simultaneously
```

**Invalid skip source**
```bash
$ uv run src/modules/ingest.py --skip invalid_source
ERROR    Invalid sources to skip: ['invalid_source']. Available: ...
```

---

## Source Details

### import_export_receipts
- **Type**: Folder-based (7 shared folders + year folders)
- **Tabs**: CT.NHAP, CT.XUAT, XNT
- **Output**: `data/00-raw/import_export/`
- **Notes**: Downloads by year/month, always re-ingests current month

### receivable
- **Type**: Direct spreadsheet (CONG NO HANG NGAY - MỚI)
- **Sheets**: Multiple customer-related sheets
- **Output**: `data/00-raw/receivable/`

### payable
- **Type**: Direct spreadsheet (BC CÔNG NỢ NCC)
- **Sheets**: Multiple supplier-related sheets
- **Output**: `data/00-raw/payable/`

### cashflow
- **Type**: Direct spreadsheet (SỔ QUỸ TIỀN MẶT + NGÂN HÀNG - 2025)
- **Sheets**: Banking and cash flow data
- **Output**: `data/00-raw/cashflow/`

---

## Performance Tips

### Optimize ingestion time

**Ingest only what changed**
```bash
# Skip expensive transaction folder scan
uv run src/modules/ingest.py --skip import_export_receipts
```

**Use manifest cache** (default)
```bash
# Manifest is cached for 24 hours by default
# No need to specify --clear-cache unless data is stale
uv run src/modules/ingest.py
```

**Force fresh cache**
```bash
uv run src/modules/ingest.py --clear-cache
```

### Avoid full scans

**Current month is always re-ingested** (import_export_receipts)
```bash
# This will re-download current month files:
uv run src/modules/ingest.py --only import_export_receipts
```

**If cache is > 24 hours old, folder re-scanned automatically**
```bash
# Manifest cache TTL: 24 hours
# See docs/manifest-caching.md for details
uv run src/modules/ingest.py
```

---

## Testing

Run CLI tests:
```bash
uv run pytest tests/test_ingest_cli.py -v
```

Test coverage:
- ✅ Help output and examples
- ✅ Single and multiple source selection
- ✅ Space handling in comma-separated lists
- ✅ Invalid source detection
- ✅ Flag conflict detection
- ✅ All flag combinations

---

## Troubleshooting

### "Invalid sources" error
Check available sources:
```bash
uv run src/modules/ingest.py --help
```

### "Cannot use both --only and --skip"
Choose one method:
```bash
# Use --only
uv run src/modules/ingest.py --only receivable

# OR use --skip
uv run src/modules/ingest.py --skip payable
```

### Ingestion slower than expected
Check manifest age:
```bash
# Clear stale cache
uv run src/modules/ingest.py --clear-cache

# Then run normally
uv run src/modules/ingest.py
```

---

## Related Documentation

- **Architecture**: docs/architecture-decisions.md#adr-6 (manifest caching)
- **Manifest caching**: docs/manifest-caching.md
- **Pipeline I/O**: docs/pipeline-io.md
- **Development workflow**: docs/development-workflow.md

---

## API Usage (Python)

If calling from code:

```python
from src.modules.ingest import ingest_from_drive

# Ingest only receivable and payable
files = ingest_from_drive(
    sources=["receivable", "payable"],
    test_mode=False,
    clean_up=False
)

# Ingest with cache clear (manual)
from src.modules.google_api import clear_manifest
clear_manifest()
files = ingest_from_drive()
```

---

## Changelog

**Version 1.0** (Current)
- ✅ `--only` flag for selective ingestion
- ✅ `--skip` flag for exclusion-based filtering
- ✅ `--clear-cache` flag for manifest management
- ✅ `--test-mode` flag for sample downloads
- ✅ `--cleanup` flag for directory reset
- ✅ Comprehensive CLI validation
- ✅ 16 CLI tests with full coverage
