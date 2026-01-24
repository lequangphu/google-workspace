---
title: Review Project Architecture
type: review
date: 2026-01-23
---

# Review Project Architecture

## Overview

Comprehensive architectural review of the tire-shop-erp-migration ETL pipeline to assess current state, identify gaps, and propose improvements for production readiness.

## Executive Summary

This project implements an ETL pipeline for migrating tire shop data from Google Sheets to KiotViet ERP. The architecture follows a 4-stage pipeline pattern (Ingest → Transform → Export → Upload) with module-specific processing for inventory, receivables, and payables.

**Current Grade: C+ (6/10)** - Functional but with significant gaps in error handling, state management, and consistency that should be addressed before production use.

## Problem Statement

The codebase works correctly but has accumulated architectural debt that affects:
- **Maintainability**: Mixed configuration patterns (hardcoded + TOML)
- **Reliability**: No pipeline-level error handling or recovery
- **Observability**: No execution summaries or progress tracking
- **Consistency**: Modules use varying data source patterns

## Current Architecture

### Directory Structure

```
tire-shop-erp-migration/
├── src/
│   ├── modules/              # Processing modules organized by data domain
│   │   ├── ingest.py         # Single ingest module for all sources
│   │   ├── google_api.py     # Google Drive/Sheets API utilities
│   │   ├── import_export_receipts/  # Inventory/product processing
│   │   ├── receivable/       # Customer/debt data processing
│   │   └── payable/          # Supplier data processing
│   ├── pipeline/
│   │   ├── orchestrator.py   # Main CLI pipeline runner
│   │   └── data_loader.py    # Shared data loading utilities
│   ├── erp/
│   │   ├── templates.py      # KiotViet import template specs
│   │   └── exporter.py       # XLSX export utilities
│   └── utils/
│       ├── staging_cache.py  # File-based cache with mtime tracking
│       ├── data_cleaning.py  # Shared cleaning utilities
│       └── xlsx_formatting.py # Excel formatting
├── data/
│   ├── 00-raw/               # Raw CSV downloads from Google Sheets
│   ├── 01-staging/           # Cleaned/transformed data
│   ├── 02-validated/         # Validated outputs (if used)
│   ├── 03-erp-export/        # Final KiotViet XLSX files
│   └── templates/            # KiotViet import template files
├── tests/                    # Pytest test suite
├── pipeline.toml             # Central configuration (ADR-1)
├── AGENTS.md                 # Agent guidelines and code style
└── project_description.md    # Business context documentation
```

### Data Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    ORCHESTRATOR (orchestrator.py)               │
│  CLI: uv run src/pipeline/orchestrator.py                        │
│       -m ier|rec|pay      # Select modules                       │
│       -s ingest,transform # Select steps                         │
│       -i|-t|-e|-u         # Single step shortcuts                │
└────────────────────┬────────────────────────────────────────────┘
                     │
        ┌────────────┼────────────┬────────────┐
        ▼            ▼            ▼            ▼
   ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐
   │ INGEST  │ │TRANSFORM│ │ EXPORT  │ │ UPLOAD  │
   │ Step 1  │ │ Step 2  │ │ Step 3  │ │ Step 4  │
   └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘
        │           │           │           │
        ▼           ▼           ▼           ▼
   data/00-raw  data/01-   data/03-    Google Drive
   (CSV)       staging    erp-export  (disabled)
                (CSV)     (XLSX)
```

### Data Sources & Modules

| Module | Source | Output | Status |
|--------|--------|--------|--------|
| **import_export_receipts** | Google Drive folder (Xuất Nhập Tồn) | Products.xlsx, PriceBook | Active |
| **receivable** | Google Sheets (TỔNG CÔNG NỢ, Thong tin KH) | Customers.xlsx | Active |
| **payable** | Google Sheets (MÃ CTY, TỔNG HỢP) | Suppliers.xlsx | Partial |
| **cashflow** | Google Sheets (Tiền gửi, Tien mat) | - | Not Implemented |

## Architecture Health Assessment

| Aspect | Score | Notes |
|--------|-------|-------|
| **Separation of Concerns** | 6/10 | Clear stages, but module patterns inconsistent |
| **Configuration Management** | 5/10 | Mixed hardcoded + TOML, needs cleanup |
| **Error Handling** | 4/10 | Script-level retry exists, pipeline-level missing |
| **Testability** | 6/10 | Some unit tests exist, no integration tests |
| **Observability** | 3/10 | Logging exists, no metrics/summaries |
| **Security** | 5/10 | OAuth handled, no credential validation |
| **Documentation** | 7/10 | AGENTS.md comprehensive, inline docs good |
| **Maintainability** | 4/10 | Mixed patterns, dead code (cashflow), bypass patterns |

## Identified Gaps

### Critical Issues

| # | Issue | Location | Impact |
|---|-------|----------|--------|
| C1 | **Dual configuration** | `orchestrator.py:15-21` + `pipeline.toml` | Configuration drift risk |
| C2 | **No pipeline state tracking** | All modules | Can't audit or resume runs |
| C3 | **No validation gate between steps** | `orchestrator.py` | Invalid outputs if earlier steps fail |
| C4 | **Mixed source patterns** | `generate_customers_xlsx.py` vs others | Inconsistent data flow |

### Error Handling Gaps

| # | Gap | Description | Impact |
|---|-----|-------------|--------|
| E1 | No retry at pipeline level | Transform scripts can fail mid-way with no resume/rollback | Data inconsistency |
| E2 | Ingest partial failure | 3 of 4 sources succeed → user gets partial data silently | Silent data loss |
| E3 | No step dependencies | Export can run without Transform having run first | Empty or stale output |
| E4 | No execution summary | After pipeline completes, no summary of what was done | User unsure of results |

### State Management Gaps

| # | Gap | Description | Impact |
|---|-----|-------------|--------|
| S1 | Hardcoded paths | Multiple modules use `Path.cwd() / "data" / "00-raw"` | Fragile behavior |
| S2 | No manifest | No record of which files were processed, when, by which script | No audit trail |
| S3 | Cashflow dead code | Configured but no transform scripts exist | Confusion |

### Data Quality Gaps

| # | Gap | Description | Impact |
|---|-----|-------------|--------|
| D1 | No data profiling | No summary statistics (row counts, nulls, duplicates) | Can't verify quality |
| D2 | No schema enforcement | Validation only at export step, not between stages | Errors found too late |
| D3 | No referential integrity | Customer codes generated without checking source | Duplicate codes |
| D4 | Date range gaps | Missing months not flagged | Hidden data gaps |

### Observability Gaps

| # | Gap | Description | Impact |
|---|-----|-------------|--------|
| O1 | No log aggregation | Each script logs independently, no unified trace ID | Hard to correlate events |
| O2 | No progress indicators | 60+ monthly files, no progress bar during ingest | Poor UX |
| O3 | No metrics | No row counts, processing times, error rates tracked | No performance insight |

## Critical Questions Requiring Resolution

### 🔴 Critical (Blocks Implementation)

**Q1: Error Handling Strategy**
What happens if a module's transform script fails halfway through?
- Should the pipeline abort entirely, continue with other modules, or retry?
- Example: `generate_products_xlsx.py` crashes after 50% of products

**Q2: Canonical Data Source**
What is the canonical data source for each module?
- `generate_customers_xlsx.py` reads directly from Google Sheets, bypassing staging
- Should all modules read from `data/01-staging/`?

**Q3: KiotViet Validation**
What validation rules exist for KiotViet import?
- Column length limits, character restrictions, format requirements
- What happens if exported data fails validation?

**Q4: Missing Data Behavior**
What happens when `--year 2024 --month 2` is specified but no file exists?
- Fail silently? Warn and continue? Error out?

### 🟡 Important (Significantly Affects UX/Maintainability)

**Q5: Cashflow Module**
Should `cashflow` module be implemented or removed from configuration?

**Q6: Incremental Runs**
Should the pipeline support incremental runs (only process new/updated files)?

**Q7: Opening Balance Feature**
What is the expected output format for the opening balance feature?

**Q8: Upload Step**
Should the upload step be re-enabled, or is manual upload permanent?

### 🟢 Nice-to-Have

**Q9: Dry-run mode** - Preview pipeline actions without execution
**Q10: Retention policy** - Keep raw/staging data forever? Delete after X days?
**Q11: Data quality report** - Summary of row counts, nulls, duplicates

## Proposed Improvements

### Phase 1: Foundation (Immediate)

#### F1: Unify Configuration
**File**: `src/pipeline/orchestrator.py:15-21`

Remove hardcoded `TRANSFORM_MODULES_LEGACY`, load all configuration from `pipeline.toml`.

```python
# Before
TRANSFORM_MODULES_LEGACY = {
    "import_export_receipts": "src/modules/import_export_receipts",
    ...
}

# After
_CONFIG = load_pipeline_config()
```

**Effort**: 1 day
**Risk**: Low
**Benefit**: Eliminates configuration drift

#### F2: Add Validation Gate
**File**: `src/pipeline/orchestrator.py`

Add check before Export step to verify Transform outputs exist.

```python
def step_export():
    if not _CONFIG.get("transform_complete"):
        logger.error("Transform step must run before export")
        return False
```

**Effort**: 0.5 day
**Risk**: Low
**Benefit**: Prevents invalid outputs

#### F3: Remove or Implement Cashflow
Decision required:
- **Option A**: Remove `cashflow` from `pipeline.toml` and module options
- **Option B**: Implement missing transform/export scripts

**Effort**: 0.5-2 days
**Risk**: Low
**Benefit**: Removes dead code confusion

### Phase 2: Reliability (Short-Term)

#### R1: Pipeline Manifest
**New File**: `src/pipeline/manifest.py`

Track processed files, timestamps, and status for audit and incremental runs.

```python
class PipelineManifest:
    def __init__(self, path: Path):
        self.path = path
        self.entries = []

    def record(self, file: Path, stage: str, status: str):
        self.entries.append({
            "file": str(file),
            "stage": stage,
            "status": status,
            "timestamp": datetime.now().isoformat()
        })
```

**Effort**: 2 days
**Risk**: Low
**Benefit**: Enables audit, resume, incremental runs

#### R2: Execution Summary
**File**: `src/pipeline/orchestrator.py`

Print summary at pipeline completion.

```python
def run_pipeline():
    summary = {"files": 0, "rows": 0, "errors": 0}
    # ... processing ...
    print(f"Processed {summary['files']} files, {summary['rows']} rows, {summary['errors']} errors")
```

**Effort**: 0.5 day
**Risk**: Low
**Benefit**: User confidence in results

#### R3: Standardize Module Patterns
**Files**: `src/modules/receivable/generate_customers_xlsx.py`

Refactor to read from staging instead of directly from Google Sheets.

```python
# Before
def process() -> Path:
    df = gspread.from_spreadsheet("Customer Data")

# After
def process(staging_dir: Optional[Path] = None) -> Optional[Path]:
    df = StagingCache.get_dataframe(staging_dir / "receivable" / "customers.csv")
```

**Effort**: 1 day
**Risk**: Medium
**Benefit**: Consistent data flow, testability

### Phase 3: Observability (Medium-Term)

#### O1: Progress Indicators
**File**: `src/modules/ingest.py`

Add progress bar for file downloads.

```python
from tqdm import tqdm

for file in tqdm(source_files, desc="Downloading"):
    download_file(file)
```

**Effort**: 0.5 day
**Risk**: Low
**Benefit**: Better UX for long operations

#### O2: Structured Logging
**File**: `src/pipeline/orchestrator.py`

Add trace ID to correlate logs across modules.

```python
trace_id = str(uuid.uuid4())
logger = logging.getLogger(__name__).addHandler(
    logging.Handler(extra={"trace_id": trace_id})
)
```

**Effort**: 1 day
**Risk**: Low
**Benefit**: Debugging assistance

#### O3: Data Quality Report
**New File**: `src/utils/data_profiling.py`

Generate summary statistics after transform stage.

```python
def profile_dataframe(df: pd.DataFrame) -> dict:
    return {
        "rows": len(df),
        "nulls": df.isnull().sum().to_dict(),
        "duplicates": df.duplicated().sum(),
        "columns": list(df.columns)
    }
```

**Effort**: 1.5 days
**Risk**: Low
**Benefit**: Early detection of data issues

### Phase 4: Enhancement (Long-Term)

#### E1: Incremental Ingest
**File**: `src/modules/ingest.py`

Compare Drive modifiedTime vs local files, only download changed.

```python
def should_download(remote: DriveFile, local: Path) -> bool:
    if not local.exists():
        return True
    return remote.modifiedTime > local.stat().st_mtime
```

**Effort**: 2 days
**Risk**: Medium
**Benefit**: Faster runs, reduced API calls

#### E2: Dry-Run Mode
**File**: `src/pipeline/orchestrator.py`

Preview pipeline actions without execution.

```python
def run_pipeline(dry_run: bool = False):
    if dry_run:
        print("Would download: [files...]")
        print("Would transform: [files...]")
        return
    # ... actual execution ...
```

**Effort**: 1 day
**Risk**: Low
**Benefit**: Safe validation before production runs

#### E3: Pipeline-Level Retry
**File**: `src/pipeline/orchestrator.py`

Retry failed modules, skip successfully completed ones.

```python
def run_pipeline(max_retries: int = 3):
    for attempt in range(max_retries):
        failed = execute_all_modules()
        if not failed:
            break
        logger.warning(f"Retry {attempt + 1}/{max_retries} for failed modules")
```

**Effort**: 2 days
**Risk**: Medium
**Benefit**: Improved reliability

## Implementation Phases Summary

| Phase | Focus | Effort | Files Modified | New Files |
|-------|-------|--------|----------------|-----------|
| 1: Foundation | Config cleanup, validation | 2 days | 2 | 0 |
| 2: Reliability | Manifest, summary, patterns | 4.5 days | 2 | 1 |
| 3: Observability | Progress, logging, profiling | 3 days | 2 | 1 |
| 4: Enhancement | Incremental, dry-run, retry | 5 days | 2 | 0 |

**Total**: ~14.5 days

## Acceptance Criteria

### Functional Requirements

- [ ] All configuration loaded from `pipeline.toml`
- [ ] Export step blocked if Transform step not completed
- [ ] Pipeline prints execution summary (files, rows, errors)
- [ ] All modules read from staging, not mixed sources
- [ ] Cashflow module either implemented or removed

### Non-Functional Requirements

- [ ] No new hardcoded paths added to modules
- [ ] All logging includes trace ID for correlation
- [ ] Progress indicator for operations processing >5 files
- [ ] Data profiling report generated for each transform

### Quality Gates

- [ ] `pytest` passes with >80% coverage on new code
- [ ] `ruff check .` passes with no warnings
- [ ] Documentation updated for changed interfaces
- [ ] Integration test covers full pipeline run

## Dependencies & Risks

### Dependencies

| Dependency | Description | Blocked By |
|------------|-------------|------------|
| Q1-Q4 resolution | Critical questions must be answered | Implementation |
| User approval | Phase priorities may change | Implementation |

### Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| Configuration change breaks existing runs | High | Test with dry-run, rollback plan |
| Refactoring breaks Google API calls | High | Mock-based testing |
| New dependencies increase CI time | Low | Run tests in parallel |

## References & Research

### Internal References

- Architecture patterns: `src/pipeline/orchestrator.py:1-50`
- Module registry: `src/pipeline/orchestrator.py:15-21`
- Staging cache: `src/utils/staging_cache.py:1-80`
- Template patterns: `src/erp/templates.py:1-100`
- Code conventions: `AGENTS.md`

### External References

- Python ETL best practices: https://docs.python.org/3/library/pipes.html
- pandas DataFrame profiling: https://pandas-profiling.ydata.ai/
- tqdm progress bars: https://tqdm.github.io/

### Related Work

- Previous ETL architecture discussions: None documented
- Similar implementations: None referenced

## Success Metrics

| Metric | Current | Target | Measurement |
|--------|---------|--------|-------------|
| Configuration consistency | 5/10 | 10/10 | Count hardcoded vs TOML config |
| Error handling coverage | 4/10 | 8/10 | % of failure modes with recovery |
| Execution visibility | 3/10 | 8/10 | Information provided to user |
| Module pattern consistency | 5/10 | 10/10 | % of modules using standard patterns |

## Documentation Plan

- [ ] Update `AGENTS.md` with new module signature requirements
- [ ] Add `ARCHITECTURE.md` documenting pipeline flow and phases
- [ ] Document ADR decisions in `docs/adr/` if created
- [ ] Update `README.md` with current CLI options

## Questions for Stakeholders

1. Which Phase 1 items should be prioritized?
2. Is incremental ingest a priority, or is full re-run acceptable?
3. Should the upload step be re-enabled in this iteration?
4. What is the expected timeline for addressing Q1-Q4?

---

*Plan created: 2026-01-23*
*Architecture Review Type: ETL Pipeline Assessment*
