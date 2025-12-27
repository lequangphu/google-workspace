# Development Workflow – Git, Commits & Pre-commit Checklist

## Git Versioning & Commits

### Commit Message Format

Follow conventional commits:

```
<type>(<scope>): <subject>

<body>

<footer>
```

**Types**:
- `feat`: New feature (refactor, new transformer)
- `fix`: Bug fix
- `test`: Test additions/changes
- `docs`: Documentation updates
- `refactor`: Code restructuring (no behavior change)
- `chore`: Dependencies, tooling

**Scope**: Module affected (e.g., `templates`, `import_export_receipts`, `receivable`)

**Examples**:
```
feat(templates): add PRODUCT, CUSTOMER, SUPPLIER templates for KiotViet

Implements ERPTemplateRegistry with 4 standard KiotViet templates.
Adds validation for required columns and data types.

Closes #42
```

```
refactor(import_export_receipts): migrate clean_chung_tu_nhap.py to clean_receipts_purchase.py

Consolidates legacy cleaning scripts into raw-source-based modules.
Maintains 100% backward compatibility with output format.
```

```
test(templates): add 26 tests for template validation

Covers happy path, missing columns, type checking, and all 4 template types.
```

### Branch Strategy

**Main branches**:
- `main` - Production-ready (all tests passing)
- `develop` - Integration branch (staging area)

**Feature branches**:
```bash
git checkout -b feat/refactor-documents
git checkout -b feat/master-data-extraction
git checkout -b test/template-validation
```

**Merge to develop**:
```bash
git checkout develop
git pull
git merge --no-ff feat/refactor-documents
git push
```

**Merge to main** (after code review):
```bash
git checkout main
git pull
git merge --no-ff develop
git tag -a v1.2.0 -m "Release version 1.2.0"
git push origin main --tags
```

### Before Committing

```bash
# Run all tests
uv run pytest tests/ -v

# Format code
uv run ruff format src/ tests/

# Check types
uv run mypy src/

# Lint
uv run ruff check src/
```

### Refactoring Commits (Legacy → New Modules)

When refactoring scripts to modules:

1. **Create feature branch**:
   ```bash
   git checkout -b refactor/migrate-documents-py
   ```

2. **Implement module** with tests passing

3. **Keep old script** temporarily (for fallback), mark as deprecated:
   ```python
   # clean_chung_tu_nhap.py
   """DEPRECATED: Use src.modules.import_export_receipts.clean_receipts_purchase.clean_purchase_receipts()"""
   ```

4. **Commit with clear message**:
   ```bash
   git commit -m "refactor(import_export_receipts): migrate clean_chung_tu_nhap.py to clean_receipts_purchase.py

   - Consolidate legacy cleaning scripts into raw-source-based modules
   - Maintain 100% backward compatibility with output
   - Add 15+ tests for happy path and edge cases
   - Legacy script remains in legacy/ folder (deprecated)
   
   Related: #15"
   ```

5. **Once verified**, move old script to legacy folder in separate commit:
   ```bash
   git commit -m "chore(cleanup): move deprecated clean_chung_tu_nhap.py to legacy/

   Migration complete to src/modules/import_export_receipts/clean_receipts_purchase.py.
   All functionality covered by tests in test_import_export_receipts_clean_receipts_purchase.py."
   ```

## Development Workflow

### Adding a New Script by Raw Source

1. **Determine raw source** (import_export_receipts, receivable, payable, or cashflow)
2. **Create script** under appropriate raw source folder: `src/modules/<raw_source>/<script_name>.py`
3. **Keep script <300 lines** for AI context window
4. **Add tests** in `tests/test_<raw_source>_<script_name>.py`
5. **Document in docstring** (inputs, outputs, assumptions)
6. **Update docs/refactoring-roadmap.md** with status

Example: Adding supplier extraction
```
src/modules/payable/extract_suppliers.py  →  tests/test_payable_extract_suppliers.py
```

### Running the Pipeline

```bash
# Full pipeline
uv run src/cli.py

# Specific step only
uv run src/cli.py --step transform

# With options
uv run src/cli.py --period 2025_02 --bundle

# Tests
uv run pytest tests/ -v

# Test specific module
uv run pytest tests/test_import_export_receipts_clean_receipts_purchase.py -v
```

## Code Style

- Type hints required for function signatures
- Docstrings for all public methods (Google format)
- Max line length: 100 characters
- Use `pathlib.Path` not strings for file paths
- Use `.format()` or f-strings for logging (no `%` operator)
