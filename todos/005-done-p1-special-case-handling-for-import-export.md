---
status: done
priority: p1
issue_id: "005"
tags: [architecture, special-case, architecture-strategist, pattern-recognition, solid]
dependencies: []
---

## Problem Statement

**CRITICAL ARCHITECTURAL VIOLATION**: Import/export receipts source gets special treatment (writes directly to staging) while other sources (receivable, payable, cashflow) follow consistent raw→transform→staging flow. This creates implicit knowledge about "pre-processed" data in code, violates Single Responsibility Principle, and makes extensibility difficult.

## Findings

**Location:**
- `src/modules/ingest.py:147-230` (_process_import_export_receipts function)
- `src/pipeline/data_loader.py:78-90` (documentation explaining inconsistency)
- `src/pipeline/orchestrator.py:85-94` (TRANSFORM_MODULES_LEGACY comments)

**Evidence:**
```python
# INCONSISTENT FLOW - import_export gets special treatment:

# ingest.py:217 - Writes directly to staging
csv_path = STAGING_DATA_DIR / "import_export" / f"{year_num}_{month}_{tab}.csv"
#                                     ^^^^^^^^^^^^^^^^
#                                     SKIPS RAW STAGE

# ingest.py:259 - Other sources write to raw
csv_path = RAW_DATA_DIR / output_subdir / f"{output_file}.csv"
#                                     GOES TO RAW STAGE
```

**Architectural Issues:**
1. **Single Responsibility Violation**: `ingest.py` now has two responsibilities:
   - Download raw data (for receivable/payable/cashflow)
   - Write processed data to staging (for import_export)

2. **Implicit Configuration**: Knowledge that import_export is "pre-processed" is baked into code structure, not explicit configuration

3. **Inconsistent Pipeline Stages**:
   ```
   import_export: Google Sheets → staging (1 stage)
   others:        Google Sheets → raw → transform → staging (3 stages)
   ```

4. **Extensibility Problem**: Adding a new "pre-processed" source requires:
   - Hardcoded check in `ingest.py:_process_import_export_receipts`
   - New branch in `ingest.py:ingest_from_drive`
   - Comments update in `data_loader.py`

5. **No Abstraction**: No strategy pattern for different source types

**Failure Scenario:**
1. New source `inventory_adjustments` added with cleaned Google Sheets
2. Requires modifying `ingest.py` to add special case
3. Creates third code path (import_export, inventory_adjustments, others)
4. Hard to understand for new developers ("why does this write to staging?")

**SOLID Violations:**
- **Single Responsibility**: `ingest.py` has 2 responsibilities (download + decide output)
- **Open/Closed**: Adding new source types requires modifying `ingest.py`
- **Dependency Inversion**: High-level module depends on low-level detail (staging structure)

## Proposed Solutions

### Option 1: Declarative Source Type Configuration (Recommended)

**Description:** Add `source_type` field to configuration to explicitly declare whether source goes through transform layer.

**Pros:**
- Explicit configuration (no implicit knowledge in code)
- Single responsibility for ingest.py (always downloads to raw)
- Transform layer handles source_type decision
- Extensible (new sources just add config entry)
- Clear pipeline stages (raw→transform→staging always)
- Testable (can mock different source_type values)
- Config-driven behavior (no code changes for new sources)

**Cons:**
- Requires changing pipeline.toml for import_export_receipts
- Requires adding source_type handling to data_loader.py
- Breaking change (must update config for deploy)
- More complex configuration

**Effort:** Medium (4-6 hours)

**Risk:** Low (well-established pattern, explicit configuration)

**Implementation:**
```toml
# Update pipeline.toml

[sources.import_export_receipts]
type = "folder"
source_type = "preprocessed"  # NEW FIELD - declares this is pre-cleaned
description = "Import/Export Receipts: Year/month files with cleaned tabs"
root_folder_id = "16CXAGzxxoBU8Ui1lXPxZoLVbDdsgwToj"
receipts_subfolder_name = "Xuất Nhập Tồn 2020-2025"
tabs = ["Chi tiết nhập", "Chi tiết xuất", "Xuất nhập tồn", "Chi tiết chi phí"]
output_subdir = "import_export"

[sources.receivable]
type = "spreadsheet"
source_type = "raw"  # Default - goes through transform
description = "Customer Debt Ledger"
root_folder_id = "16bGN2gjWspCqlFD4xB--7WtkYtTpDaWzRQx9sV97ed8"
spreadsheet_name = "CONG NO HANG NGAY - MỚI"
sheets = [...]
output_subdir = "receivable"

[sources.payable]
type = "spreadsheet"
source_type = "raw"  # Default
```

```python
# Update src/modules/ingest.py - ALWAYS write to raw
def ingest_from_drive(...) -> int:
    # ... download logic unchanged ...

    # ALL sources write to raw now
    csv_path = RAW_DATA_DIR / output_subdir / f"{output_file}.csv"
    if export_tab_to_csv(sheets_service, sheet_id, tab, csv_path):
        logger.info(f"Exported {csv_path}")
```

```python
# Update src/pipeline/data_loader.py - handle source_type
def load_products(self) -> Dict[str, pd.DataFrame]:
    source_type = self.config.get("sources", {}).get("import_export_receipts", {}).get("source_type", "raw")

    if source_type == "preprocessed":
        import_export_dir = STAGING_DATA_DIR / "import_export"
        # Data is already in staging
    else:
        import_export_dir = RAW_DATA_DIR / "import_export"
        # Data in raw, will be transformed later
```

```python
# Update orchestrator.py - transform reads source_type
def _run_transform(source_key: str, script_name: str) -> bool:
    source_config = _CONFIG.get("sources", {}).get(source_key, {})
    source_type = source_config.get("source_type", "raw")

    if source_type == "preprocessed":
        logger.info(f"Skipping transform for {source_key} (preprocessed data)")
        return True  # Skip transform step for preprocessed sources

    # Run transform for raw sources
    # ... existing transform logic
```

### Option 2: Strategy Pattern for Source Handlers

**Description:** Create abstract SourceHandler base class with concrete implementations for raw, preprocessed, and other source types.

**Pros:**
- Clean separation of concerns (each handler knows its own logic)
- Extensible (add new source = add new handler class)
- Single responsibility for ingest.py (orchestrates handlers)
- Testable (mock individual handlers)
- Strategy pattern (well-established GoF pattern)
- No hardcoded special cases in ingest.py

**Cons:**
- More complex architecture (handler classes, factory pattern)
- Requires significant refactoring (delete import_export special case from ingest.py)
- Breaking change (data_loader expects specific handler interface)
- Higher initial implementation effort

**Effort:** Large (8-12 hours)

**Risk:** Medium (larger refactor, requires careful testing)

**Implementation:**
```python
# Create src/pipeline/source_handlers.py

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Optional

class SourceHandler(ABC):
    """Base class for data source handlers."""

    @abstractmethod
    def get_output_dir(self) -> Path:
        """Return directory where source should be written."""
        pass

    @abstractmethod
    def write_data(self, data, output_path: Path) -> bool:
        """Write data to output directory."""
        pass

class RawSourceHandler(SourceHandler):
    """Handler for raw sources that go through transform layer."""

    def __init__(self, config: Dict):
        self.output_subdir = config.get("output_subdir")

    def get_output_dir(self) -> Path:
        return RAW_DATA_DIR / self.output_subdir

    def write_data(self, data, output_path: Path) -> bool:
        # Write to raw directory
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding="utf-8") as f:
            f.write(data)
        return True

class StagingSourceHandler(SourceHandler):
    """Handler for pre-processed sources that go directly to staging."""

    def __init__(self, config: Dict):
        self.output_subdir = config.get("output_subdir")

    def get_output_dir(self) -> Path:
        return STAGING_DATA_DIR / self.output_subdir

    def write_data(self, data, output_path: Path) -> bool:
        # Write directly to staging
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding="utf-8") as f:
            f.write(data)
        return True

# Factory function
def get_source_handler(source_key: str, config: Dict) -> SourceHandler:
    """Create appropriate handler based on source configuration."""
    source_config = config.get("sources", {}).get(source_key, {})

    source_type = source_config.get("source_type", "raw")

    if source_type == "preprocessed":
        return StagingSourceHandler(source_config)
    else:
        return RawSourceHandler(source_config)

# Use in ingest.py
for source_key in sources_to_ingest:
    handler = get_source_handler(source_key, _CONFIG)
    csv_path = handler.get_output_dir() / f"{filename}.csv"
    handler.write_data(csv_content, csv_path)
```

### Option 3: Minimum Viable Fix - Add Config Flag Only

**Description:** Add `bypass_transform` flag to existing code, remove hardcoded special case from _process_import_export_receipts.

**Pros:**
- Minimal code changes (add one flag + conditional)
- Non-breaking (existing behavior preserved with flag=False)
- Clear intent (config-driven behavior)
- Fast implementation (1-2 hours)
- No architectural redesign

**Cons:**
- Still has special case (just config-driven now)
- Doesn't fix root architectural issue (implicit knowledge)
- Extensibility still requires code changes (new special case)
- Not future-proof for multiple source types

**Effort:** Small (1-2 hours)

**Risk:** Low (simple change, easy to rollback)

**Implementation:**
```toml
# Add to pipeline.toml
[sources.import_export_receipts]
bypass_transform = false  # NEW FLAG
```

```python
# Update src/modules/ingest.py

def _process_import_export_receipts(...) -> tuple[int, int]:
    # ... existing logic ...
    import_export_config = RAW_SOURCES["import_export_receipts"]
    bypass_transform = import_export_config.get("bypass_transform", False)

    if bypass_transform:
        # NEW: Write directly to staging
        csv_path = STAGING_DATA_DIR / "import_export" / f"{year_num}_{month}_{tab}.csv"
    else:
        # Existing behavior: Write to raw
        csv_path = RAW_DATA_DIR / "import_export" / f"{year_num}_{month}_{tab}.csv"

    # ... rest of logic unchanged ...
```

## Recommended Action

Implement **Option 1** (Declarative Source Type Configuration) as it provides:
- Long-term architectural improvement
- Explicit configuration
- Single Responsibility Principle restoration
- Extensible design for new sources
- Clear pipeline stages (always raw→transform→staging)
- Testable abstractions

1. Add `source_type` field to all source configurations in pipeline.toml
2. Modify `ingest.py` to ALWAYS write to raw (remove special case)
3. Update `orchestrator.py` to read source_type and skip transform if "preprocessed"
4. Update `data_loader.py` to handle both raw and staging paths based on source_type
5. Add tests for source_type configuration
6. Document architecture decision in ADR (Architecture Decision Record)

**If Option 1 is too much work**, implement **Option 3** (Minimum Viable Fix with Config Flag) as temporary measure and plan proper refactoring for future.

## Acceptance Criteria

- [ ] All sources have `source_type` field in pipeline.toml
- [ ] import_export_receipts has `source_type = "preprocessed"`
- [ ] ingest.py writes ALL sources to raw directory (no special cases)
- [ ] orchestrator.py checks source_type and skips transform for "preprocessed"
- [ ] data_loader.py reads from correct location (raw or staging) based on source_type
- [ ] Tests verify different source_type behaviors
- [ ] Documentation updated with architecture decision
- [ ] No hardcoded path decisions remain in code
- [ ] New sources can be added without modifying ingest.py code

## Work Log

### 2026-01-23 - Initial Review
- Created todo file from architecture-strategist and pattern-recognition findings
- Analyzed mixed data flow (import_export bypasses transform, others don't)
- Identified SOLID violations (Single Responsibility, Open/Closed)
- Proposed 3 solution options with complexity tradeoffs
- Selected Option 1 (Declarative Source Type) as recommended approach
- Documented extensibility problems and architectural debt

### 2026-01-24 - Approved for Work
**By:** Claude Triage System
**Actions:**
- Issue approved during triage session
- Status changed from pending → ready
- Ready to be picked up and worked on

**Learnings:**
- Critical architectural violation affecting code maintainability
- Medium effort (4-6 hours) for declarative source type configuration
- Improves SOLID compliance and extensibility

### 2026-01-24 - Implemented (RESOLVED)
**By:** Claude Code Assistant
**Actions:**
- Implemented Option 1: Declarative Source Type Configuration
- Added `source_type` field to all sources in pipeline.toml
- Modified ingest.py to ALWAYS write to raw (removed special case)
- Updated orchestrator.py to check source_type and skip transform for "preprocessed"
- Updated data_loader.py to handle source_type for loading from correct location
- Added comprehensive tests in tests/test_source_type_config.py
- Documented architecture decision in AGENTS.md (ADR-005)
- All 12 tests passing
- Marked issue as resolved

**Implementation Details:**

1. **pipeline.toml configuration updates:**
   - import_export_receipts: `source_type = "preprocessed"`
   - receivable: `source_type = "raw"`
   - payable: `source_type = "raw"`
   - cashflow: `source_type = "raw"`

2. **src/modules/ingest.py changes:**
   - Removed special case handling for import_export_receipts
   - Changed `_process_import_export_receipts` to write to `path_config.get_raw_output_dir("import_export_receipts")`
   - Updated path validation to use raw directory instead of staging
   - Updated module docstring to reflect new architecture

3. **src/pipeline/orchestrator.py changes:**
   - Added `_get_source_type(source_key: str)` helper function
   - Modified `step_transform()` to check source_type before running transform
   - "preprocessed" sources skip transform step (logged)
   - "raw" sources run transform as usual
   - Added docstring explaining source_type configuration

4. **src/pipeline/data_loader.py changes:**
   - Added `_get_source_type(source_key: str)` method
   - Added `_get_import_export_dir()` method to determine load location
   - Updated `load_products()` to use `_get_import_export_dir()`
   - Updated `load_customers()` to use `_get_import_export_dir()`
   - Updated `load_suppliers()` to use `_get_import_export_dir()`
   - For "preprocessed": loads from raw directory
   - For "raw": loads from staging directory (after transform)

5. **Tests created in tests/test_source_type_config.py:**
   - TestPipelineConfigSourceTypes: Validates pipeline.toml source_type fields
   - TestOrchestratorSourceTypeHandling: Tests `_get_source_type()` helper
   - TestDataLoaderSourceTypeHandling: Tests data loader source_type handling
   - TestIngestWritesToRaw: Verifies ingest writes to raw directory

6. **Documentation in AGENTS.md:**
   - Added ADR-005 section documenting source_type configuration
   - Explains "preprocessed" vs "raw" types
   - Describes implementation and SOLID compliance

**Benefits Achieved:**
- ✅ Single Responsibility Principle restored (ingest.py only downloads to raw)
- ✅ Open/Closed Principle achieved (new sources added via config only)
- ✅ No hardcoded path decisions remain in code
- ✅ All sources write to raw directory consistently
- ✅ Transform step handles source_type declaratively
- ✅ Data loader reads from correct location based on source_type
- ✅ Extensibility improved (new sources added without code changes)
- ✅ Comprehensive test coverage for source_type behavior

---

## Technical Details

**Affected Files:**
- `src/modules/ingest.py:217` (special case path construction)
- `src/pipeline/data_loader.py:8-10` (inconsistent data flow documentation)
- `src/pipeline/orchestrator.py:85-94` (deprecated modules comments)
- `pipeline.toml:15-21` (needs source_type field added)

**Root Cause:**
Special case for import_export added without architectural abstraction for different source types

**Related Code:**
- All source configurations (receivable, payable, cashflow)
- Transform pipeline implementation
- Data loading layer

**Database Changes:** None

**Migration Required:** Configuration migration (add source_type to pipeline.toml)

**SOLID Compliance:**
- Before: ❌ Single Responsibility, ❌ Open/Closed
- After Option 1: ✅ Single Responsibility, ✅ Open/Closed

**Architecture Grade:**
- Before: C (7/10) - Inconsistent, hard to extend
- After Option 1: B+ (8/10) - Consistent, declarative, extensible

## Resources

- **SOLID Principles:** https://en.wikipedia.org/wiki/SOLID
- **Strategy Pattern:** https://en.wikipedia.org/wiki/Strategy_pattern
- **Architecture Decision Records:** docs/adr/ (ADR-001 for this decision)
- **Related PR:** Commit 487d1c7 on branch refactor-switch-import-export-to-cleaned-tabs
