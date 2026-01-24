---
title: Repository Cleanup
type: refactor
date: 2026-01-24
---

# Repository Cleanup

Remove outdated, redundant files and folders to improve repository maintainability and reduce clutter.

## Overview

The repository has accumulated debris that needs cleanup:
- Empty directories serving no purpose
- Root-level test files that should be in `tests/`
- Backup files from configuration changes
- Intermediate development data
- macOS metadata files

## Problem Statement

Current state creates confusion:
- Empty directories (`src/services/`, `src/modules/transform/`, etc.) add noise
- Test files scattered in wrong locations
- Backup files and intermediate data clutter the repository
- Potential for accidental commits of build artifacts

## Proposed Solution

Execute cleanup in phases:
1. Delete empty directories
2. Move test files to proper location
3. Delete backup and intermediate data files
4. Verify `.gitignore` completeness

## Technical Approach

### Phase 1: Delete Empty Directories

```bash
rm -rf src/services/
rm -rf src/modules/transform/
rm -rf .github/agents/
rm -rf data/reports/
```

### Phase 2: Move Test Files

```bash
# From root to tests/
mv test_regex.py tests/
mv test_region_regex.py tests/
mv test_steps.py tests/

# From source to tests/
mv src/modules/import_export_receipts/test_product_extraction.py tests/
```

### Phase 3: Delete Backup and Intermediate Data

```bash
# Delete backup file
rm pipeline.toml.backup

# Delete intermediate data directory
rm -rf data/cleaned/

# Delete macOS metadata
rm .DS_Store
rm data/.DS_Store
```

### Phase 4: Verify `.gitignore`

Verify these entries exist:
- `.venv/`
- `__pycache__/`
- `*.pyc`
- `.pytest_cache/`
- `.ruff_cache/`

## Acceptance Criteria

- [x] Empty directories removed: `src/services/`, `src/modules/transform/`, `.github/agents/`, `data/reports/`
- [x] Test files relocated: `test_regex.py`, `test_region_regex.py`, `test_steps.py`, `test_product_extraction.py`
- [x] Backup files deleted: `pipeline.toml.backup`
- [x] Intermediate data deleted: `data/cleaned/`
- [x] macOS metadata deleted: `.DS_Store` files
- [x] `.gitignore` contains all build artifacts
- [x] Repository structure navigable and clean

## Success Metrics

- Reduced directory count by 4 empty directories
- All test files consolidated in `tests/`
- Zero backup or intermediate data files in repository
- Clean `git status` output (no untracked noise)

## Dependencies & Risks

### Dependencies
- None - this is a self-contained cleanup task

### Risks
- Low: Changes are easily reversible via git
- Medium: If any file has uncommitted changes, they would be lost

### Mitigation
- Review `git status` before executing cleanup
- Consider committing before cleanup for easy rollback

## Implementation Order

| Step | Action | Safety |
|------|--------|--------|
| 1 | Review `git status` | Safe - read only |
| 2 | Stage changes | Safe |
| 3 | Commit | Reversible |
| 4 | Delete files/directories | After commit |

## Future Considerations

- Add `.gitignore` check to pre-commit hooks
- Create a `scripts/cleanup.py` for recurring maintenance
- Consider adding a `Makefile` target for standard cleanup tasks

## References

- Brainstorm: `docs/brainstorms/2026-01-24-repository-cleanup-brainstorm.md`
- Project structure: `AGENTS.md`
- Test patterns: `tests/test_ingest.py`, `tests/test_validation.py`
