# ADR-001: Import/Export Receipts Direct Staging

## Status
Accepted

## Context
The refactor in commit 487d1c7 switched `import_export_receipts` data flow from `raw→transform→staging` to direct staging writes, assuming Google Sheets tabs are pre-cleaned.

Before this change, all data sources followed a consistent pipeline:
- All sources: `data/00-raw/` → `transform scripts` → `data/01-staging/`

After the refactor, the pipeline became inconsistent:
- `import_export_receipts`: Direct to `data/01-staging/import_export/` (1 step)
- `receivable`, `payable`, `cashflow`: `data/00-raw/` → `transform` → `data/01-staging/` (3 steps)

This change eliminated 2,124 lines of transformation code that was previously used to clean import/export data, based on the assumption that the Google Sheets tabs (Chi tiết nhập, Chi tiết xuất, Xuất nhập tồn, Chi tiết chi phí) are manually maintained as "cleaned" versions.

## Decision
**Import/export receipts will continue writing directly to staging (bypassing raw and transform layers)** with the following rationale:

1. **Google Sheets tabs are manually maintained as "cleaned" versions** (Chi tiết nhập, Chi tiết xuất, Xuất nhập tồn, Chi tiết chi phí)
2. The clean scripts previously performed 2,124 lines of validation logic
3. This validation should ideally happen in Google Sheets, not in our pipeline
4. Performance gain from eliminating transform step (50% fewer I/O operations)
5. Simplified data flow for import_export module

## Consequences

### Positive
- 50% I/O reduction for import_export data
- Eliminated 2,124 lines of transformation code
- Simplified pipeline for import_export
- Faster time-to-market for features

### Negative
- **Architectural inconsistency** with other sources (receivable, payable, cashflow)
- **No validation layer** for import_export data
- **Fragile assumption** that Google Sheets are always cleaned
- **No audit trail** (raw backup) for import_export data
- **Harder to understand** pipeline behavior
- **More complex code review** process

### Risks
1. **Google Sheets are NOT actually cleaned** (manual edit)
   - Bad data ingested directly to staging
   - No validation layer catches quality issues
   - Silent corruption until downstream errors

2. **Receivable module also has "cleaned Google Sheets" in future**
   - Must modify `ingest.py` to add new special case
   - Creates third code path
   - Architectural debt increases

3. **Code review fails** refactor "Why does import_export skip transform?"
   - This ADR provides the justification

4. **Team member asks** "Which directory does data go to?"
   - No single answer - depends on source
   - Implicit knowledge required to understand

## Alternatives Considered

### 1. Restore Consistent Pipeline for All Sources
**Description:** Remove special case handling and make ALL sources follow `raw→transform→staging` flow. Add validation layer in transform to handle "cleaned" data.

**Pros:**
- Consistent architecture (all sources treated same way)
- Clear boundaries between layers
- No implicit knowledge about data quality
- Easier to understand and maintain
- Testable (same flow for all sources)
- Future-proof (new sources fit existing pattern)
- Extensible (can add preprocessing config without code changes)

**Cons:**
- Requires adding validation logic to transform layer
- Reintroduces some code duplication
- May violate assumption that "Google Sheets tabs are cleaned"
- Requires modifying multiple modules
- Breaking change

**Effort:** Large (8-12 hours)
**Risk:** Low (restores architectural consistency)

### 2. Add Validation to Import/Export Ingest
**Description:** Keep direct staging writes but add validation to `ingest.py` for import_export data.

**Pros:**
- Data integrity without pipeline change
- Catches bad data before staging
- Minimal architectural change

**Cons:**
- Special case remains
- Validation logic added to ingest layer (not ideal location)
- Still architectural inconsistency

**Effort:** Medium (4-6 hours)
**Risk:** Medium (validation logic in wrong layer)

### 3. Create Declarative Source Type Configuration
**Description:** Create `source_type` field in `pipeline.toml` to define how each source flows through the pipeline.

**Pros:**
- Explicit configuration
- SOLID principles (Open/Closed)
- Easier to add new source types
- No code changes needed for new sources

**Cons:**
- Significant refactoring effort
- Requires `SourceHandler` abstractions
- More complex configuration

**Effort:** Large (12-16 hours)
**Risk:** Medium (significant architectural change)

**Selected:** Accept current inconsistency and document as ADR (Option 2 from P1-006)

## Migration Plan to Consistent Architecture (Future Work)

### Phase 1 (Weeks 1-2): Add Safety Layer
- [ ] Add validation layer to `import_export_receipts` ingest
- [ ] Implement raw backup strategy for `import_export_receipts`
- [ ] Add comprehensive test coverage for import_export data validation
- [ ] Document validation rules in `pipeline.toml`

### Phase 2 (Weeks 3-4): Declarative Source Type Configuration
- [ ] Refactor to declarative source type configuration (Option 1 from P1-005)
- [ ] Create `SourceHandler` abstractions
- [ ] Define `source_type` values: `preprocessed`, `standard`, `custom`
- [ ] Update `pipeline.toml` with source_type for each source

### Phase 3 (Weeks 5-6): Restore Consistent Pipeline
- [ ] Restore consistent pipeline for all sources (`raw→transform→staging`)
- [ ] Move import_export validation to transform layer
- [ ] Update all documentation to reflect consistent architecture
- [ ] Remove special case handling from `ingest.py`
- [ ] Create integration tests for full pipeline

## Implementation Notes

### Current Behavior (src/modules/ingest.py:147-229)
```python
def _process_import_export_receipts(...) -> tuple[int, int]:
    """Handle folder-based import_export_receipts source.

    Writes cleaned tabs directly to staging (data/01-staging/import_export/)
    because they are pre-processed data from Google Sheets.
    """
    # ... code writes directly to STAGING_DATA_DIR / "import_export"
```

### Future Migration (Phase 3)
```python
def _process_import_export_receipts(...) -> tuple[int, int]:
    """Handle folder-based import_export_receipts source.

    Writes to raw (data/00-raw/import_export/) following standard pipeline.
    Transform layer will validate and move to staging.
    """
    # ... code writes to RAW_DATA_DIR / "import_export"
```

## References
- **Commit:** 487d1c7 (refactor-switch-import-export-to-cleaned-tabs)
- **Plan:** docs/plans/2026-01-23-refactor-switch-import-export-to-cleaned-tabs-plan.md
- **Issue:** P1-006 (Mixed data flow patterns)
- **Issue:** P1-002 (Bypassed data validation)
- **Issue:** P1-003 (Missing raw data backup)
- **Issue:** P1-005 (Hardcoded import_export special case)
- **ADR Template:** https://adr.github.com/patterns/adr/
- **Clean Architecture:** https://blog.cleancoder.com/uncle-bob/2017/01/08/the-clean-architecture.html

## Revision History
- **2026-01-24:** Initial ADR created documenting import_export direct staging decision
