---
status: done
priority: p1
issue_id: "003"
tags: [security, data-integrity, audit-trail, security-sentinel, data-integrity-guardian]
dependencies: []
---

## Problem Statement

**CRITICAL SECURITY RISK**: Bypassing `data/00-raw/` layer eliminates forensic audit trail. No backup exists when Google Sheets are modified. Undetectable fraud, data loss from destructive edits, compliance violations (SOX, GDPR audit requirements), and incident response failure are now impossible to investigate.

## Findings

**Location:** Architecture + `src/modules/ingest.py:217`

**Evidence:**
```
OLD FLOW:
Google Sheets → data/00-raw/ (archive) → clean scripts → data/01-staging/
                    ^^^^^^^^^^^^^^^^^^^
                    FORENSIC BACKUP HERE

NEW FLOW:
Google Sheets → data/01-staging/ (NO BACKUP)
```

**Impact:**
- No raw data archive for forensic investigation
- If Google Sheets are accidentally edited/deleted, no way to recover
- Compliance violations (SOX, GDPR, industry regulations require audit trails)
- Cannot investigate data corruption after the fact
- No way to verify what was ingested vs what's in Sheets
- Loss of versioning (raw captures Sheets snapshot at ingest time)

**Compliance Requirements at Risk:**
- **SOX (Sarbanes-Oxley):** Section 404 requires audit trails for financial data
- **GDPR Article 30:** Right to be informed of data processing
- **Industry Best Practices:** Always maintain raw backup of source data

**Failure Scenarios:**
1. User accidentally deletes "Chi tiết nhập" tab from Google Sheets
   - Old flow: `data/00-raw/2025_01_*.csv` has backup
   - New flow: No backup exists, data permanently lost

2. Malicious actor modifies data in Google Sheets
   - Old flow: Can compare raw backup to detect changes
   - New flow: No reference point, modification undetectable

3. Google Sheets API bug corrupts data during export
   - Old flow: Raw capture may have different data than current Sheets
   - New flow: No way to detect discrepancy

4. Data quality issue discovered weeks later
   - Old flow: Can audit raw backup to find when issue started
   - New flow: No historical snapshot to analyze

## Proposed Solutions

### Option 1: Dual-Write with Raw Archive (Recommended)

**Description:** Write to both raw (archive) and staging directories simultaneously, maintaining forensic trail while using cleaned data for processing.

**Pros:**
- Maintains full audit trail compliance
- Provides forensic investigation capability
- Enables rollback to raw backup if needed
- Minimal code changes (add shutil.copy2)
- Keeps data flow clear (raw = archive only, staging = processed data)

**Cons:**
- Doubles disk space usage (raw + staging copies)
- Adds ~50% more I/O operations
- Requires cleanup policy for old raw backups
- May confuse new developers (why two copies?)

**Effort:** Small (2-3 hours)

**Risk:** Low (standard dual-write pattern, well-understood)

**Implementation:**
```python
# Add to src/modules/ingest.py
import shutil

RAW_BACKUP_DIR = RAW_DATA_DIR / "import_export" / "raw_backups"
RAW_BACKUP_DIR.mkdir(parents=True, exist_ok=True)

for tab in tabs_to_process:
    # Write to staging for processing
    csv_path = STAGING_DATA_DIR / "import_export" / f"{year_num}_{month}_{tab}.csv"
    if export_tab_to_csv(sheets_service, file_id, tab, csv_path):
        logger.info(f"Exported {csv_path}")

        # Archive to raw for forensic trail
        backup_path = RAW_BACKUP_DIR / f"{year_num}_{month}_{tab}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        shutil.copy2(csv_path, backup_path)
        logger.info(f"Archived to raw: {backup_path}")
```

### Option 2: Conditional Raw Backup

**Description:** Write to raw only for import_export module, not other sources. Adds configuration flag to enable/disable backup.

**Pros:**
- Maintains audit trail for import_export (high-risk module)
- Doesn't waste disk space on low-risk sources (receivable, payable)
- Configurable (can disable if compliance not required)
- Clear separation (raw = backup, staging = processed)

**Cons:**
- Inconsistent backup policy (import_export has backup, others don't)
- More complex configuration
- Adds conditional logic to codebase
- Requires documentation of backup policy

**Effort:** Medium (3-4 hours)

**Risk:** Low (configurable, can disable if not needed)

**Implementation:**
```toml
# Add to pipeline.toml
[sources.import_export_receipts]
enable_raw_backup = true  # New flag
```

```python
# Add to src/modules/ingest.py
enable_backup = import_export_config.get("enable_raw_backup", True)

if enable_backup:
    # Write to raw backup
    backup_path = RAW_DATA_DIR / "import_export" / f"{year_num}_{month}_{tab}.csv"
    if export_tab_to_csv(sheets_service, file_id, tab, backup_path):
        # Write to staging from backup (copy2 instead of fresh download)
        staging_path = STAGING_DATA_DIR / "import_export" / f"{year_num}_{month}_{tab}.csv"
        shutil.copy2(backup_path, staging_path)
        logger.info(f"Exported {staging_path} (from raw backup)")
else:
    # Current flow - staging only
    csv_path = STAGING_DATA_DIR / "import_export" / f"{year_num}_{month}_{tab}.csv"
    if export_tab_to_csv(sheets_service, file_id, tab, csv_path):
        logger.info(f"Exported {csv_path}")
```

### Option 3: Git-Based Raw Archive

**Description:** Commit raw backups to Git repository instead of disk, leveraging version control for audit trail.

**Pros:**
- Git provides built-in versioning and history
- No extra disk space (Git deduplicates)
- Easy to compare versions and detect changes
- Integrated with existing development workflow
- Can use GitHub/GitLab/GitBite for collaboration

**Cons:**
- Large Git repository if not configured for large file storage (LFS needed)
- Slow for frequent ingestions (Git commit overhead)
- Requires access control for Git repository
- May require Git LFS for large CSV files

**Effort:** Medium (3-5 hours)

**Risk:** Medium (requires Git infrastructure setup, repository size growth)

**Implementation:**
```python
# Add to src/modules/ingest.py
import subprocess

def commit_raw_backup(filepath: Path) -> None:
    """Commit raw backup to Git repository."""
    subprocess.run(
        ["git", "add", str(filepath)],
        cwd=WORKSPACE_ROOT,
        check=True
    )
    subprocess.run(
        ["git", "commit", "-m", f"Raw backup: {filepath.name}"],
        cwd=WORKSPACE_ROOT,
        check=True
    )

# Use after export
if enable_raw_backup:
    raw_path = RAW_DATA_DIR / "import_export" / f"{year_num}_{month}_{tab}.csv"
    if export_tab_to_csv(sheets_service, file_id, tab, raw_path):
        commit_raw_backup(raw_path)
        # Copy to staging
        staging_path = STAGING_DATA_DIR / "import_export" / f"{year_num}_{month}_{tab}.csv"
        shutil.copy2(raw_path, staging_path)
```

## Recommended Action

**Custom approach:** Make ingest.py write import_export data to `data/00-raw/` (consistent with other sources) and update downstream scripts (data_loader.py, transform scripts) to handle this change.

**Implementation Plan:**

1. **Update ingest.py**: Change path construction from STAGING_DATA_DIR to RAW_DATA_DIR
   ```python
   # OLD:
   csv_path = STAGING_DATA_DIR / "import_export" / f"{year_num}_{month}_{tab}.csv"

   # NEW:
   csv_path = RAW_DATA_DIR / "import_export" / f"{year_num}_{month}_{tab}.csv"
   ```

2. **Update data_loader.py**: Change to read from RAW_DATA_DIR instead of STAGING_DATA_DIR
   ```python
   # OLD:
   import_export_dir = self.staging_dir / "import_export"

   # NEW:
   import_export_dir = self.raw_dir / "import_export"
   ```

3. **Update transform/export scripts**: Ensure they read from raw data location

4. **Benefits:**
   - Consistent architecture (all sources write to raw)
   - Automatic forensic backup (raw layer preserved)
   - Simpler pipeline (no special case handling)
   - Clear data lineage (raw → transform → staging)

5. **Effort:** Medium (3-4 hours) - requires updating ingest.py, data_loader.py, and testing downstream scripts

## Acceptance Criteria

- [ ] ingest.py writes import_export data to `data/00-raw/import_export/`
- [ ] data_loader.py reads import_export from raw directory
- [ ] Downstream scripts (transform, export) updated to read from raw
- [ ] All sources now follow consistent raw→transform→staging flow
- [ ] Tests verify import_export data flows correctly through pipeline
- [ ] Documentation updated in AGENTS.md
- [ ] No special case handling remains for import_export in ingest.py

## Work Log

### 2026-01-23 - Initial Review
- Created todo file from security-sentinel and data-integrity-guardian findings
- Analyzed architecture change removing raw layer
- Identified compliance risks (SOX, GDPR audit requirements)
- Proposed 3 solution options with tradeoffs
- Selected Option 1 (Dual-Write) as recommended approach

### 2026-01-24 - Custom Modification During Triage
**By:** Claude Triage System
**Changes:**
- Modified recommended action to simpler approach
- Changed from "Dual-Write with Raw Archive" to "Write to Raw + Update Downstream Scripts"
- Rationale: Simpler, more consistent architecture (all sources write to raw)
- Benefit: No special case handling, clear data lineage

**New Implementation Plan:**
- Make ingest.py write to data/00-raw/ (like other sources)
- Update data_loader.py to read from raw directory
- Update downstream transform/export scripts accordingly

### 2026-01-24 - Approved for Work
**By:** Claude Triage System
**Actions:**
- Issue approved during triage session
- Status changed from pending → ready
- Ready to be picked up and worked on
- Custom modification applied: Write to raw + update downstream scripts

**Learnings:**
- Simpler architecture preferred over dual-write
- Consistent data flow (raw → transform → staging) for all sources
- Medium effort (3-4 hours) with high compliance benefit

---

## Technical Details

**Affected Files:**
- `src/modules/ingest.py:217` (csv_path construction, needs backup logic)
- `src/pipeline/data_loader.py:78-90` (reads from staging, assumes raw backup exists elsewhere)

**Root Cause:**
Architectural decision to bypass raw layer for import_export without adding alternative audit mechanism

**Related Code:**
- Git repository structure
- Backup policies (if any exist for other modules)
- Compliance documentation
- Pipeline orchestrator error handling

**Database Changes:** None

**Migration Required:** No

**Compliance Impact:**
- **SOX Section 404:** Audit trails not maintained
- **GDPR Article 32:** No way to verify data processing accuracy
- **Industry Standards:** Deviation from data lineage best practices

**Financial Impact:**
- Risk of undetectable fraud
- No forensic investigation capability
- Potential regulatory fines if audited
- Loss of trust in data accuracy

## Resources

- **SOX Compliance Guide:** https://www.sox-online.org/
- **GDPR Documentation:** https://gdpr-info.eu/
- **Data Lineage Best Practices:** https://damg.org/data-lineage/
- **Git LFS:** https://git-lfs.github.com/
- **Related PR:** Commit 487d1c7 on branch refactor-switch-import-export-to-cleaned-tabs
