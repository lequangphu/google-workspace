---
title: Repository Cleanup
date: 2026-01-24
status: complete
participants: User, Claude
tags: [cleanup, maintenance, repository]
---

# Repository Cleanup Brainstorm

## What We're Building

A cleanup plan to remove outdated, redundant files and folders from the tire-shop-erp-migration repository to improve maintainability and reduce clutter.

## Why This Approach

The repository has accumulated several categories of debris over time:
- **Empty directories** that serve no purpose but add noise to the codebase
- **Root-level test files** that should live alongside other tests in `tests/`
- **Backup files** left over from configuration changes
- **Intermediate data** that was generated during development but is no longer needed
- **macOS metadata files** that should be gitignored
- **Build artifacts** (virtual environment, caches) that shouldn't be in version control

This cleanup will:
- Reduce cognitive load when navigating the codebase
- Eliminate potential confusion from duplicate/overlapping files
- Ensure the repository follows clean project structure conventions
- Remove artifacts that shouldn't be committed

## Key Decisions

| Decision | Rationale |
|----------|-----------|
| Delete empty directories: `src/services/`, `src/modules/transform/`, `.github/agents/`, `data/reports/` | These directories serve no purpose and add noise |
| Move root test files to `tests/`: `test_regex.py`, `test_region_regex.py`, `test_steps.py` | Consistent location for all tests; follows project conventions |
| Delete `pipeline.toml.backup` | Backup file from Jan 11, no longer needed after migration complete |
| Delete `data/cleaned/` directory | Intermediate data from Jan 13 (10MB CSV), no longer needed |
| Keep both `generate_product_master.py` and `generate_products_xlsx.py` | Serve different purposes; distinct functionality |
| Keep `todos/` directory | Useful reference for completed items; separate from ADRs |
| Move root test file `test_product_extraction.py` to `tests/` | Test files should not be mixed with source code |
| Verify `.gitignore` includes all build artifacts | Prevent future accumulation of cache/virtual env files |

## Cleanup Targets

### High Priority (Safe Deletions)

| Item | Type | Reason |
|------|------|--------|
| `src/services/` | directory | Empty (only `__pycache__`) |
| `src/modules/transform/` | directory | Empty (only `__pycache__`) |
| `.github/agents/` | directory | Empty |
| `data/reports/` | directory | Empty |
| `test_regex.py` | file | Root-level test, should be in `tests/` |
| `test_region_regex.py` | file | Root-level test, should be in `tests/` |
| `test_steps.py` | file | Root-level test, should be in `tests/` |
| `pipeline.toml.backup` | file | Backup from Jan 11, obsolete |
| `.DS_Store` (root) | file | macOS metadata |
| `data/.DS_Store` | file | macOS metadata |

### Medium Priority (Review & Act)

| Item | Action |
|------|--------|
| `test_product_extraction.py` | Move from `src/modules/import_export_receipts/` to `tests/` |
| `data/cleaned/` | Delete - intermediate data |
| `.venv/` | Verify in `.gitignore` |
| `__pycache__` directories | Verify in `.gitignore` |
| `.pytest_cache/` | Verify in `.gitignore` |
| `.ruff_cache/` | Verify in `.gitignore` |

### Low Priority (Deferred)

- Review `docs/` structure for potential consolidation (brainstorms/plans/solutions overlap)
- No immediate action needed

## Open Questions

1. Should `test_product_extraction.py` be renamed to follow test naming conventions (e.g., `test_product_extraction.py` → `test_import_export_receipts_product_extraction.py`)?

## Success Criteria

- [ ] All empty directories removed
- [ ] All root-level test files relocated to `tests/`
- [ ] Backup files and intermediate data deleted
- [ ] `.gitignore` complete with all build artifacts
- [ ] Repository structure clean and navigable
- [ ] No duplicate/overlapping functionality confusion

## Next Steps

1. Review and validate this brainstorm document
2. Run `/workflows:plan` to create implementation plan
3. Execute cleanup in a single commit for easy rollback if needed
