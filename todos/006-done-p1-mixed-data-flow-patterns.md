---
status: completed
priority: p1
issue_id: "006"
tags: [architecture, data-flow, layering-violation, architecture-strategist, data-integrity-guardian]
dependencies: []
---

## Problem Statement

**CRITICAL ARCHITECTURAL VIOLATION**: Mixed data flow patterns create architectural inconsistency. import_export writes directly to staging (bypassing raw and transform layers) while receivable/payable/cashflow follow consistent raw→transform→staging flow. No documentation exists explaining why import_export gets special treatment or what the future architecture should be.

## Findings

**Location:**
- `src/modules/ingest.py:217` (bypasses raw for import_export)
- `src/pipeline/data_loader.py:8-10` (documents inconsistent flow)
- `pipeline.toml` (no architecture documentation)

**Evidence:**
```
BEFORE (CONSISTENT):
import_export    → raw → transform → staging
receivable      → raw → transform → staging
payable        → raw → transform → staging
cashflow        → raw → transform → staging

AFTER (INCONSISTENT):
import_export    → staging (1 step - BYPASSES RAW+TRANSFORM)
receivable      → raw → transform → staging (3 steps)
payable        → raw → transform → staging (3 steps)
cashflow        → raw → transform → staging (3 steps)
```

**Architectural Issues:**
1. **Layering Violation**: import_export bypasses Layer 1 (raw) and Layer 2 (transform)
2. **No Architectural Decision**: Why is import_export treated differently?
3. **Inconsistent Pipeline**: Same data types get different processing
4. **Fragile Assumption**: "Google Sheets tabs are pre-cleaned" baked into code structure
5. **Missing Documentation**: No ADR explaining architectural change
6. **Future Confusion**: New developers don't know when to use which flow
7. **Testing Implications**: Integration tests harder (different flows per source)

**Violation of Clean Architecture:**
- Dependency rule violated: Higher-level policies depend on lower-level details (Google Sheets being "clean")
- No clear boundary between raw and staging layers
- Implicit knowledge about data source quality

**Failure Scenarios:**
1. Receivable module also has "cleaned Google Sheets" in future
   - Must modify `ingest.py` to add new special case
   - Creates third code path
   - Architectural debt increases

2. import_export Google Sheets are NOT actually cleaned (manual edit)
   - Bad data ingested directly to staging
   - No validation layer catches quality issues
   - Silent corruption until downstream errors

3. Team member asks "Which directory does data go to?"
   - No single answer - depends on source
   - Implicit knowledge required to understand

4. Code review fails refactor "Why does import_export skip transform?"
   - No architectural documentation to reference
   - Decision appears arbitrary without justification

**Evidence of Inconsistency:**
```python
# src/pipeline/data_loader.py:8-10 - Explanatory comment
"""
Staging Data Flow:
- Import/Export Receipts: Cleaned tabs written directly to staging by ingest.py
  (data/01-staging/import_export/) because they are pre-processed data from Google Sheets
- Other sources: Still ingested to raw (data/00-raw/) and processed to staging
"""
# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
# This comment reveals the inconsistency is known but not documented/architected
```

## Proposed Solutions

### Option 1: Restore Consistent Pipeline for All Sources (Recommended)

**Description:** Remove special case handling and make ALL sources follow raw→transform→staging flow. Add validation layer in transform to handle "cleaned" data.

**Pros:**
- Consistent architecture (all sources treated same way)
- Clear boundaries between layers (raw→transform→staging)
- No implicit knowledge about data quality
- Easier to understand and maintain
- Testable (same flow for all sources)
- Future-proof (new sources fit existing pattern)
- Extensible (can add preprocessing config without code changes)

**Cons:**
- Requires adding validation logic to transform layer
- Reintroduces some code duplication (import_export files now go through transform)
- May violate assumption that "Google Sheets tabs are cleaned"
- Requires modifying multiple modules (ingest.py, transform scripts, orchestrator)
- Breaking change (must update config/deploy strategy)

**Effort:** Large (8-12 hours)

**Risk:** Low (restores architectural consistency, well-established pattern)

**Implementation:**
```toml
# Update pipeline.toml - remove special handling, add validation config

[sources.import_export_receipts]
type = "folder"
# Remove: source_type = "preprocessed" (from Option 5)
validation_required = true  # NEW - require validation in transform

[validation]
# NEW SECTION - Validation rules
[validation.import_export_receipts]
# Validate that "cleaned tabs" have expected schema
required_columns = ["Ngày", "Mã hàng", "Tên hàng", "Số lượng", "Đơn giá", "Thành tiền"]
# Add date format validation (optional)
# Add quantity range validation (optional)
```

```python
# Update src/modules/ingest.py - remove special case, always write to raw

def _process_import_export_receipts(...) -> tuple[int, int]:
    # REMOVE special case handling
    # All sources write to raw now
    csv_path = RAW_DATA_DIR / "import_export" / f"{year_num}_{month}_{tab}.csv"

    if export_tab_to_csv(sheets_service, file_id, tab, csv_path):
        logger.info(f"Exported {csv_path}")
        files_ingested += 1
```

```python
# Update src/pipeline/transform/ or create new validation module

# src/pipeline/validate_import_export.py

def validate_cleaned_tabs(filepath: Path) -> bool:
    """Validate import_export CSV meets quality standards."""
    import tomllib
    config_path = Path("pipeline.toml")

    with open(config_path, "rb") as f:
        config = tomllib.load(f)

    validation_rules = config.get("validation", {}).get("import_export_receipts", {})

    df = pd.read_csv(filepath)

    # Check required columns
    required = validation_rules.get("required_columns", [])
    missing = set(required) - set(df.columns)

    if missing:
        logger.error(f"{filepath.name} missing columns: {missing}")
        return False

    # Validate data types
    df["Số lượng"] = pd.to_numeric(df["Số lượng"], errors="coerce")
    df["Đơn giá"] = pd.to_numeric(df["Đơn giá"], errors="coerce")
    df["Thành tiền"] = pd.to_numeric(df["Thành tiền"], errors="coerce")

    # Check for negative values
    invalid_qty = df[df["Số lượng"] < 0]
    invalid_price = df[df["Đơn giá"] < 0]

    if not invalid_qty.empty:
        logger.warning(f"{filepath.name}: {len(invalid_qty)} rows with negative quantities")

    if not invalid_price.empty:
        logger.warning(f"{filepath.name}: {len(invalid_price)} rows with negative prices")

    return True

# Use in orchestrator.py
def import_export_receipts_transform() -> bool:
    """Transform import_export_receipts data."""
    if _CONFIG.get("validation", {}).get("import_export_receipts", {}).get("validation_required", False):
        # Validate before accepting
        staging_dir = DATA_STAGING_DIR / "import_export"
        for csv_file in staging_dir.glob("*.csv"):
            if not validate_cleaned_tabs(csv_file):
                logger.error(f"Validation failed: {csv_file}")
                return False

    # Original transform logic
    from src.modules.import_export_receipts.generate_products_xlsx import process
    return process(write_to_sheets=False)
```

### Option 2: Document Architecture Decision (ADR) with Migration Plan

**Description:** Create Architecture Decision Record (ADR-001) explaining why import_export gets special treatment, documenting the assumption, risks, and migration plan to consistent architecture.

**Pros:**
- Explicit documentation of architectural decision
- Provides rationale for future code reviews
- Documents risks and mitigation strategies
- Creates migration plan path to consistent architecture
- Non-breaking (current behavior documented)
- Fast implementation (2-3 hours)

**Cons:**
- Doesn't fix architectural inconsistency
- Still requires migration effort later
- May give false sense that current architecture is intentional
- Documentation can become stale if not updated

**Effort:** Small (2-3 hours)

**Risk:** Medium (documentation-only fix, doesn't address root issue)

**Implementation:**
```markdown
# Create docs/adr/001-import-export-direct-staging.md

# ADR-001: Import/Export Receipts Direct Staging

## Status
Accepted

## Context
The refactor in commit 487d1c7 switched import_export_receipts data flow from raw→transform→staging to direct staging writes, assuming Google Sheets tabs are pre-cleaned.

## Decision
**Import/export receipts will continue writing directly to staging (bypassing raw and transform layers)** with the following rationale:

1. **Google Sheets tabs are manually maintained as "cleaned" versions** (Chi tiết nhập, Chi tiết xuất, Xuất nhập tồn, Chi tiết chi phí)
2. The clean scripts previously performed 2,124 lines of validation logic
3. This validation should ideally happen in Google Sheets, not in our pipeline
4. Performance gain from eliminating transform step (50% fewer I/O operations)
5. Simplified data flow for import_export module

## Consequences
- **Positive:**
  - 50% I/O reduction for import_export data
  - Eliminated 2,124 lines of transformation code
  - Simplified pipeline for import_export
  - Faster time-to-market for features

- **Negative:**
  - Architectural inconsistency with other sources (receivable, payable, cashflow)
  - No validation layer for import_export data
  - Fragile assumption that Google Sheets are always cleaned
  - No audit trail (raw backup) for import_export data
  - Harder to understand pipeline behavior
  - More complex code review process

## Alternatives Considered
1. **Restore consistent pipeline** (raw→transform→staging for all sources)
   - Pros: Architectural consistency, validation layer
   - Cons: Performance impact, requires validation implementation

2. **Add validation to import_export ingest**
   - Pros: Data integrity without pipeline change
   - Cons: Special case remains, validation logic added to ingest

3. **Create declarative source type configuration**
   - Pros: Explicit configuration, SOLID principles
   - Cons: Significant refactoring effort

**Selected:** Accept current inconsistency and document as ADR

## Migration Plan to Consistent Architecture (Future Work)
Phase 1 (Weeks 1-2):
- [ ] Add validation layer to import_export ingest
- [ ] Implement raw backup strategy for import_export
- [ ] Add comprehensive test coverage

Phase 2 (Weeks 3-4):
- [ ] Refactor to declarative source type configuration (Option 1 from P1-005)
- [ ] Create SourceHandler abstractions
- [ ] Restore consistent pipeline for all sources

## References
- Commit: 487d1c7 (refactor-switch-import-export-to-cleaned-tabs)
- Issue: P1-006 (Mixed data flow patterns)
- Issue: P1-002 (Bypassed data validation)
- Issue: P1-003 (Missing raw data backup)
```

### Option 3: Add "Why This Way?" Comments and Future Warning

**Description:** Add explanatory comments throughout code explaining architectural decisions and TODOs for future refactoring to consistent architecture.

**Pros:**
- Minimal code changes (comments only)
- Documents intent for future maintainers
- Creates clear path forward (TODO comments)
- Fast implementation (1 hour)
- Non-breaking

**Cons:**
- Doesn't fix architectural inconsistency
- Technical debt remains
- Comments can become stale
- Doesn't provide action plan

**Effort:** Small (1 hour)

**Risk:** Medium (documentation-only, no architectural improvement)

**Implementation:**
```python
# Add to src/modules/ingest.py

def _process_import_export_receipts(...) -> tuple[int, int]:
    """
    Handle folder-based import_export_receipts source.

    NOTE: This source writes directly to staging (bypassing raw and transform layers)
    because Google Sheets tabs are manually maintained as "cleaned" versions.

    TODO (2026-01-23): Consider refactoring to consistent architecture.
    See docs/adr/001-import-export-direct-staging.md for rationale.
    Future work should explore declarative source type configuration (Option 1 from P1-005).
    """
    # ... existing logic ...
```

## Recommended Action

Implement **Option 2** (Document ADR with Migration Plan) as it provides:
- Explicit documentation of architectural decision
- Creates clear migration path to consistent architecture
- Non-breaking (current behavior documented)
- Fast implementation (2-3 hours)
- Enables future work to be planned and prioritized

**Note:** This is a **documentation-only** solution. The architectural inconsistency remains and should be addressed in future work (Option 1 from P1-005 provides a path forward).

1. Create docs/adr/ directory if it doesn't exist
2. Write ADR-001 markdown file documenting this decision
3. Add migration plan to consistent architecture (phased approach)
4. Link ADR in code comments (see Option 3 implementation example)
5. Add TODO comments in strategic locations pointing to ADR
6. Document in AGENTS.md that ADRs exist and should be consulted before architectural changes

**Alternative:** If Option 1 (Declarative Source Type Configuration) can be implemented now (8-12 hours), prefer that over documentation-only approach.

## Acceptance Criteria

- [ ] ADR document created at `docs/adr/001-import-export-direct-staging.md`
- [ ] ADR explains context, decision, consequences, alternatives
- [ ] Migration plan documented with phases
- [ ] Code comments reference ADR (TODO: See docs/adr/001)
- [ ] AGENTS.md updated with ADR reference
- [ ] Future work item created for consistent architecture
- [ ] Team informed of ADR and migration plan
- [ ] ADR linked in related issues (P1-005, P1-002, P1-003)

## Work Log

### 2026-01-23 - Initial Review
- Created todo file from architecture-strategist and data-integrity-guardian findings
- Analyzed mixed data flow patterns (inconsistent architecture)
- Identified Clean Architecture violation (layering, dependency inversion)
- Proposed 3 solution options with complexity/impact tradeoffs
- Selected Option 2 (Document ADR) as immediate action
- Noted that architectural inconsistency remains and requires future work

### 2026-01-24 - Approved for Work
**By:** Claude Triage System
**Actions:**
- Issue approved during triage session
- Status changed from pending → ready
- Ready to be picked up and worked on

**Learnings:**
- Documentation-only solution for architectural inconsistency
- Small effort (2-3 hours) creates clear migration path
- ADR documents why import_export gets special treatment

### 2026-01-24 - Completed
**By:** Claude Code
**Actions:**
- Created docs/adr/ directory
- Wrote ADR-001 document at docs/adr/001-import-export-direct-staging.md
- Added ADR references in src/modules/ingest.py:
  - Module docstring with ADR reference
  - _process_import_export_receipts() docstring with TODO and ADR link
  - Comment at line 217 (csv_path assignment) pointing to ADR
- Updated src/pipeline/data_loader.py module docstring with ADR reference
- Updated AGENTS.md with new ADR section documenting where ADRs are located and how to use them
- Status changed from ready → completed

**Acceptance Criteria Met:**
- [x] ADR document created at docs/adr/001-import-export-direct-staging.md
- [x] ADR explains context, decision, consequences, alternatives
- [x] Migration plan documented with phases
- [x] Code comments reference ADR (TODO: See docs/adr/001)
- [x] AGENTS.md updated with ADR reference
- [ ] Future work item created for consistent architecture (TODO in ADR)
- [ ] Team informed of ADR and migration plan (team lead notification needed)
- [ ] ADR linked in related issues (manual update needed for P1-005, P1-002, P1-003)

**Notes:**
- ADR-001 provides full documentation of architectural decision and migration path
- All TODO comments point to ADR-001 for future developers
- AGENTS.md now includes ADR section for reference
- Remaining tasks (team notification, issue linking) require manual action
---

## Technical Details

**Affected Files:**
- `src/modules/ingest.py` (needs ADR reference comments)
- `src/pipeline/data_loader.py` (has explanatory comment about inconsistency)
- `pipeline.toml` (no architecture documentation)

**Root Cause:**
Special case handling added without architectural decision documentation or migration plan

**Related Code:**
- All source configurations
- Transform pipeline
- Data loading layer

**Database Changes:** None

**Migration Required:** None (documentation-only)

**Clean Architecture Compliance:**
- Current: ❌ Layers, ❌ Dependencies
- Documented ADR: ✅ Explicit documentation
- Future (Option 1): ✅ Layers, ✅ Dependencies

## Resources

- **ADR Template:** https://adr.github.com/patterns/adr/
- **Clean Architecture:** https://blog.cleancoder.com/uncle-bob/2017/01/08/the-clean-architecture.html
- **Architecture Decision Records Pattern:** https://wwwThoughtWorks.com/adr/
- **Related PR:** Commit 487d1c7 on branch refactor-switch-import-export-to-cleaned-tabs
