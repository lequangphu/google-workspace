---
status: done
priority: p1
issue_id: "007"
tags: [data-integrity, fragility, assumptions, architecture-strategist, data-integrity-guardian]
dependencies: []
---

## Problem Statement

**CRITICAL DATA INTEGRITY RISK**: The assumption "Google Sheets tabs are pre-cleaned" is fragile and unverified. If the "cleaned tabs" in Google Sheets have wrong schema, missing columns, or bad data quality, the pipeline silently corrupts staging data without detection. No version tracking exists to compare Sheets snapshots to ingested data.

## Findings

**Location:**
- `src/modules/ingest.py:4-9` (module docstring)
- `src/pipeline/data_loader.py:78-90` (comment about cleaned tabs)
- `src/modules/import_export_receipts/` (all clean scripts deleted)

**Evidence:**
```python
# ingest.py:4-9 - Unverified assumption in docstring
"""
1. Import/Export Receipts: Year/month files with cleaned tabs (Chi tiết nhập, Chi tiết xuất, Xuất nhập tồn, Chi tiết chi phí) → data/01-staging/import_export/
   because they are pre-processed data from Google Sheets.
"""
#                                 ^^^^^^^^^^^^^^^^^^^^^^^^
#                          FRAGILE ASSUMPTION - NEVER VERIFIED

# data_loader.py:78-90 - Comment reveals uncertainty
"""
Staging Data Flow:
- Import/Export Receipts: Cleaned tabs written directly to staging by ingest.py
  (data/01-staging/import_export/) because they are pre-processed data from Google Sheets
"""
#                                        ^^^^^^^^^^^^^^^
#                           UNCERTAINTY EXPOSED
```

**Failure Modes:**

| Scenario | Impact | Detection | Recovery |
|---------|--------|-----------|----------|
| User edits "Chi tiết nhập" tab - adds column with wrong name | Silent | Manual fix in Sheets, re-ingest |
| User deletes critical column (e.g., "Ngày") | Pipeline crash on KeyError | Manual fix, re-ingest |
| Tab renamed from "Chi tiết nhập" to "Chi tiết nhap" | ingest.py fails to find file | Manual config update |
| Sheets formula in cell corrupts data | Silent data corruption | Restore from history (if available) |
| Manual edit adds 10K bad rows | Downstream calculations wrong | Manual data cleanup, re-ingest |
| Missing month's "Chi tiết chi phí" tab | Incomplete financial data | Manual missing data entry |

**Evidence of Fragility:**
```python
# Test changes already caught column name changes:
# tests/test_import_export_receipts_generate_products_xlsx.py:127-128
-        assert product_a["Số lượng cuối kỳ"].iloc[0] == 80
+        assert product_a["Tồn cuối kỳ"].iloc[0] == 80

# This proves column name changed and downstream broke - BUT NO VALIDATION CAUGHT IT
```

**Root Causes:**
1. No schema validation at ingest time
2. No data quality checks before accepting to staging
3. No version tracking of Google Sheets format
4. Manual maintenance of "cleaned" tabs (not automated)
5. No rollback mechanism if bad data ingested
6. No comparison between Sheets and staging data

## Proposed Solutions

### Option 1: Schema Validation at Ingest Time (Recommended)

**Description:** Add schema validation that checks required columns, data types, and business rules before accepting CSV files into staging.

**Pros:**
- Catches schema issues immediately
- Fail-fast behavior prevents bad data entering pipeline
- Clear error messages about what's wrong
- Enables safe rollback (bad data rejected before staging)
- Validates assumption that data is actually "cleaned"
- Well-established pattern (input validation)

**Cons:**
- Requires defining expected schemas for all tab types
- Adds ~50-100 lines of validation code
- May reject legitimate data (false positives)
- Doesn't validate data values (only schema)
- Breaking change if bad data currently passes

**Effort:** Medium (3-5 hours)

**Risk:** Low (standard validation pattern)

**Implementation:**
```python
# Add to src/pipeline/validation.py

EXPECTED_SCHEMAS = {
    "Chi tiết nhập": {
        "required_columns": ["Ngày", "Mã hàng", "Tên hàng", "Số lượng", "Đơn giá", "Thành tiền"],
        "date_columns": ["Ngày"],
        "numeric_columns": ["Số lượng", "Đơn giá", "Thành tiền"],
        "forbidden_values": {
            "Số lượng": (0, None),
            "Đơn giá": (0, None, -1, -999),
            "Thành tiền": (0, None, -1, -999),
        },
    },
    "Chi tiết xuất": {
        "required_columns": ["Ngày", "Mã hàng", "Tên hàng", "Số lượng", "Đơn giá", "Thành tiền"],
        "date_columns": ["Ngày"],
        "numeric_columns": ["Số lượng", "Đơn giá", "Thành tiền"],
        "forbidden_values": {
            "Số lượng": (0, None),
            "Đơn giá": (0, None, -1, -999),
            "Thành tiền": (0, None, -1, -999),
        },
    },
    "Xuất nhập tồn": {
        "required_columns": ["Mã hàng", "Tên hàng", "Tồn cuối kỳ", "Giá trị cuối kỳ"],
        "numeric_columns": ["Tồn cuối kỳ", "Giá trị cuối kỳ"],
        "forbidden_values": {
            "Tồn cuối kỳ": None,  # Not a valid inventory concept
        },
    },
    "Chi tiết chi phí": {
        "required_columns": ["Mã hàng", "Tên hàng", "Số tiền", "Diễn giải"],
        "numeric_columns": ["Số tiền"],
        "forbidden_values": {
            "Số tiền": (0, None, -1, -999),
        },
    },
}

def validate_schema(csv_path: Path, tab_name: str) -> bool:
    """Validate CSV file meets expected schema."""
    if tab_name not in EXPECTED_SCHEMAS:
        raise ValueError(f"Unknown tab type: {tab_name}")

    schema = EXPECTED_SCHEMAS[tab_name]
    df = pd.read_csv(csv_path, nrows=1)

    # Check required columns
    required = schema["required_columns"]
    missing = [col for col in required if col not in df.columns]

    if missing:
        logger.error(f"{csv_path.name} missing required columns: {missing}")
        return False

    # Check numeric columns
    numeric_cols = schema.get("numeric_columns", [])
    for col in numeric_cols:
        if col in df.columns:
            # Validate no NaN in numeric columns
            if df[col].isna().any():
                logger.error(f"{csv_path.name}: Column {col} has NaN values")
                return False

    return True
```

```python
# Use in src/modules/ingest.py

from src.pipeline.validation import validate_schema

for tab in tabs_to_process:
    csv_path = STAGING_DATA_DIR / "import_export" / f"{year_num}_{month}_{tab}.csv"

    if export_tab_to_csv(sheets_service, file_id, tab, csv_path):
        # Validate schema before accepting
        if not validate_schema(csv_path, tab):
            logger.error(f"Schema validation failed: {csv_path}")
            # Option A: Delete file
            csv_path.unlink(missing_ok=True)
            # Option B: Move to quarantine directory
            continue

        logger.info(f"Exported {csv_path}")
```

### Option 2: Google Sheets Version Tracking

**Description:** Capture Google Sheets revision ID or last modified timestamp during ingest to detect if source data changed after last ingest.

**Pros:**
- Detects when Sheets modified (new revision)
- Can warn if schema drift detected
- Enables forensic comparison (what was ingested vs what's in Sheets now)
- No schema definitions required
- Simple implementation (capture revision ID)

**Cons:**
- Requires adding revision tracking to ingest process
- Doesn't validate data quality (only detects changes)
- No protection against initial bad data (first ingest)
- Adds complexity to error messages

**Effort:** Medium (3-4 hours)

**Risk:** Medium (only detects changes, doesn't prevent issues)

**Implementation:**
```python
# Add to src/modules/google_api.py

def export_tab_to_csv_with_revision(sheets_service, sheet_id, tab, csv_path) -> bool:
    """Export tab to CSV and capture revision metadata."""
    # Get spreadsheet metadata
    spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=sheet_id)

    # Extract revision info
    revision_id = spreadsheet.get("spreadsheetId", "").get("revisionId", None)
    last_modified = spreadsheet.get("lastModifiedTime", None)

    # Export data
    success = export_tab_to_csv(sheets_service, sheet_id, tab, csv_path)

    if success:
        # Write revision metadata alongside CSV
        metadata_file = csv_path.with_suffix(".metadata.json")
        metadata = {
            "spreadsheet_id": sheet_id,
            "tab": tab,
            "revision_id": revision_id,
            "last_modified": last_modified.isoformat() if last_modified else None,
            "ingested_at": datetime.now().isoformat(),
        }

        with open(metadata_file, 'w', encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)

        logger.info(f"Exported {csv_path} (revision: {revision_id})")

    return success
```

### Option 3: Automated Schema Enforcement in Google Sheets

**Description:** Use Google Apps Script or Data Validation feature to enforce schema constraints at the source, preventing invalid data from being entered.

**Pros:**
- Prevents invalid data at source
- Real-time validation (user gets immediate feedback)
- No validation code needed in pipeline
- Best practice (validate at boundary, not in pipeline)
- Reduces downstream errors

**Cons:**
- Requires Google Apps Script setup and deployment
- Need permissions to deploy scripts to Sheets
- Can't validate existing data (only new entries)
- More complex infrastructure (Sheets script + pipeline coordination)
- Vendor lock-in (Google Apps Script platform)

**Effort:** Large (8-16 hours)

**Risk:** Medium (vendor lock-in, infrastructure complexity)

**Implementation:**
```javascript
// Google Apps Script: validate_schema.gs

function onEdit(e) {
  const sheet = e.range.getSheet();
  const tabName = sheet.getName();

  if (tabName === 'Chi tiết nhập' || tabName === 'Chi tiết xuất') {
    // Get first row (headers)
    const headers = sheet.getRange(1, 1, sheet.getLastColumn()).getValues()[0];

    // Validate required columns
    const required = ['Ngày', 'Mã hàng', 'Tên hàng', 'Số lượng', 'Đơn giá', 'Thành tiền'];
    const missing = required.filter(col => !headers.includes(col));

    if (missing.length > 0) {
      e.source.toast(`Missing required columns: ${missing.join(', ')}`, {timeout: 5});
      // Prevent edit
      e.range.clear();
    }

    // Validate numeric columns are positive
    const quantityCol = headers.indexOf('Số lượng');
    const priceCol = headers.indexOf('Đơn giá');
    const lastRow = sheet.getLastRow();

    if (quantityCol >= 0) {
      const quantities = sheet.getRange(2, quantityCol, lastRow).getValues().flat();
      const invalid = quantities.filter(q => q <= 0);

      if (invalid.length > 0) {
        e.source.toast(`Found ${invalid.length} rows with negative quantities`, {timeout: 5});
        // Prevent edit or highlight in red
        e.range.getRange(2, quantityCol, invalid.length + 1, lastRow).setBackground('#ffcccc');
      }
    }
  }
}
```

## Recommended Action

Implement **Option 1** (Schema Validation at Ingest Time) as it provides:
- Immediate detection of schema issues
- Fail-fast behavior prevents bad data entering pipeline
- Clear error messages
- Well-established pattern (input validation)
- Enables safe rollback (bad data rejected before staging)
- Minimal infrastructure changes (no Google Sheets modifications)

1. Create `src/pipeline/validation.py` with `validate_schema()` function
2. Define expected schemas for all 4 tab types
3. Call validation in `ingest.py` after each export
4. Add quarantine directory for rejected files (data/00-rejected/)
5. Add tests for all schema validation rules
6. Document validation in AGENTS.md
7. Consider adding Option 2 (Revision Tracking) in future work

**Alternative:** If Google Sheets automation is possible, implement Option 3 (Automated Schema Enforcement) for best protection.

## Acceptance Criteria

- [ ] `src/pipeline/validation.py` created with `validate_schema()` function
- [ ] Expected schemas defined for all 4 tab types
- [ ] Validation checks required columns, numeric types, forbidden values
- [ ] Validation called in `ingest.py` after each export
- [ ] Schema failures logged with specific missing columns
- [ ] Invalid files rejected (deleted or quarantined)
- [ ] Quarantine directory exists at `data/00-rejected/`
- [ ] Tests verify schema validation works correctly
- [ ] Tests verify invalid files are rejected
- [ ] Documentation updated in AGENTS.md

## Work Log

### 2026-01-23 - Initial Review
- Created todo file from data-integrity-guardian and architecture-strategist findings
- Analyzed "cleaned Google Sheets" assumption in code
- Found test changes proving column name fragility (test failures)
- Identified failure modes (schema changes, missing columns, bad data)
- Proposed 3 solution options (validation, revision tracking, automation)
- Selected Option 1 (Schema Validation at Ingest Time) as recommended approach

### 2026-01-24 - Approved for Work
**By:** Claude Triage System
**Actions:**
- Issue approved during triage session
- Status changed from pending → ready
- Ready to be picked up and worked on

**Learnings:**
- Critical data integrity risk from unverified assumptions
- Medium effort (3-5 hours) provides robust schema validation
- Fail-fast behavior prevents bad data entering pipeline

---

## Technical Details

**Affected Files:**
- `src/modules/ingest.py:4-9` (docstring with unverified assumption)
- `src/pipeline/data_loader.py:78-90` (comment about cleaned tabs)

**Root Cause:**
Assumption that Google Sheets tabs are pre-cleaned without validation mechanism

**Related Code:**
- Deprecated clean scripts (had validation logic)
- Test fixtures (proved schema fragility)
- Google Sheets API integration

**Database Changes:** None

**Migration Required:** No

**Data Integrity Impact:**
- **Current**: Fragile assumption, no validation, silent failures
- **After Fix**: Robust schema validation, fail-fast, clear error messages

**Risk Mitigation:**
- Reduces probability of data corruption from HIGH to LOW
- Enables safe rollback (bad data never reaches staging)
- Provides detection of schema drift

## Resources

- **Input Validation Best Practices:** https://owasp.org/www-community/attacks/Input_Validation_Cheat_Sheet
- **Schema Validation Patterns:** https://json-schema.org/understanding-json-schema/
- **Google Apps Script:** https://developers.google.com/apps-script
- **Data Validation Patterns:** https://en.wikipedia.org/wiki/Data_validation
- **Related PR:** Commit 487d1c7 on branch refactor-switch-import-export-to-cleaned-tabs
