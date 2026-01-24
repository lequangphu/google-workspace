---
status: done
priority: p1
issue_id: "008"
tags: [architecture, coupling, dependency-inversion, architecture-strategist, solid]
dependencies: []
---

## Problem Statement

**CRITICAL ARCHITECTURAL VIOLATION**: Tight coupling between `ingest.py` and `data_loader.py` via hardcoded paths. Ingest module depends on low-level implementation details (staging directory structure and subdirectory naming). Adding a new source or changing paths requires modifying both ingest.py and data_loader.py. Violates Dependency Inversion Principle.

## Findings

**Location:**
- `src/modules/ingest.py:217` (hardcoded staging path construction)
- `src/pipeline/data_loader.py:87-90` (hardcoded import_export subdirectory)
- `src/pipeline/orchestrator.py:85-94` (config structure used by both modules)

**Evidence:**
```python
# ingest.py:217 - Tight coupling to staging structure
csv_path = STAGING_DATA_DIR / "import_export" / f"{year_num}_{month}_{tab}.csv"
#                                     ^^^^^^^^^^^^^^^^
#                                     HARDCODED SUBDIR NAME

# data_loader.py:87-90 - Tight coupling to path naming
import_export_dir = self.staging_dir / "import_export"
#                                     ^^^^^^^^^^^^^^^^
#                                     HARDCODED SUBDIR NAME
```

**Architectural Issues:**
1. **Dependency Inversion Violation**: High-level module (ingest.py) depends on low-level detail (staging subdirectory name)
2. **No Abstraction**: No interface or protocol for data writing
3. **Tight Coupling**: Changing directory name requires updating both modules
4. **Hardcoded Values**: Subdirectory name "import_export" repeated in 2 places
5. **Configuration Duplication**: Path logic not centralized
6. **Testing Issues**: Mocking requires patching both ingest.py and data_loader.py

**Failure Scenarios:**
1. Change staging subdirectory from "import_export" to "receipts"
   - Must update `ingest.py:217` (path construction)
   - Must update `data_loader.py:87` (subdirectory name)
   - No compile-time error (both modules compile, fail at runtime)

2. Add new source that needs different path structure
   - Must add special case to ingest.py
   - Must add new subdirectory to data_loader.py
   - More scattered code changes

3. Refactor to use configuration for path structure
   - Requires touching multiple files
   - Breaking change if config not updated
   - High coordination effort

**Evidence of Tight Coupling:**
```python
# Configuration defined in pipeline.toml but used differently:

# pipeline.toml:20
[sources.import_export_receipts]
output_subdir = "import_export"  # Configured

# But used hardcoded in code:
ingest.py:217:
    csv_path = STAGING_DATA_DIR / "import_export" / ...
    #                                     ^^^^^^^^^^^^^^^^
    # NOT FROM CONFIG

data_loader.py:87-90:
    import_export_dir = self.staging_dir / "import_export"
    #                                     ^^^^^^^^^^^^^^^^
    # NOT FROM CONFIG
```

## Proposed Solutions

### Option 1: Centralized Path Configuration (Recommended)

**Description:** Extract all path construction logic into a shared module that reads from configuration, eliminating hardcoded paths and tight coupling.

**Pros:**
- Single source of truth for paths
- Changing paths requires only updating config
- Modules depend on abstraction, not concrete values
- Clear separation of concerns
- Testable (can inject mock path module)
- Follows Dependency Inversion Principle
- Removes code duplication

**Cons:**
- Requires creating new path configuration module
- Breaking change (modules import new path module)
- Refactoring effort (touch 5-6 files)
- Configuration becomes more complex

**Effort:** Medium (4-6 hours)

**Risk:** Low (well-established pattern, clear separation of concerns)

**Implementation:**
```python
# Create src/utils/path_config.py

from pathlib import Path
import tomllib
from typing import Dict

logger = logging.getLogger(__name__)

class PathConfig:
    """Centralized path configuration.

    Reads paths from pipeline.toml and provides consistent
    directory structure for all modules.
    """

    def __init__(self, config_path: Path = None):
        if config_path is None:
            config_path = Path(__file__).parent.parent.parent / "pipeline.toml"

        with open(config_path, "rb") as f:
            self._config = tomllib.load(f)

        # Read base directories from config
        self.raw_data_dir = Path(self._config["dirs"]["raw_data"])
        self.staging_data_dir = Path(self._config["dirs"]["staging"])
        self.validated_data_dir = Path(self._config["dirs"]["validated"])
        self.erp_export_dir = Path(self._config["dirs"]["erp_export"])

    def get_raw_output_dir(self, source_key: str, subdir: str) -> Path:
        """Get raw output directory for a source."""
        return self.raw_data_dir / subdir

    def get_staging_output_dir(self, source_key: str, subdir: str) -> Path:
        """Get staging output directory for a source."""
        return self.staging_data_dir / subdir

    def import_export_staging_dir(self) -> Path:
        """Get staging directory for import_export receipts."""
        return self.staging_data_dir / "import_export"
```

```python
# Update src/modules/ingest.py

from src.utils.path_config import PathConfig

# Initialize once
path_config = PathConfig()

def _process_import_export_receipts(...) -> tuple[int, int]:
    # ... existing logic ...

    # Use centralized path configuration
    csv_path = path_config.import_export_staging_dir()
```

```python
# Update src/pipeline/data_loader.py

from src.utils.path_config import PathConfig

class DataLoader:
    def __init__(self, config: Dict = None):
        # ... existing logic ...
        self.path_config = PathConfig(config)

    def load_products(self) -> Dict[str, pd.DataFrame]:
        # Use centralized path configuration
        import_export_dir = self.path_config.import_export_staging_dir()
```

### Option 2: Configuration-Driven Subdirectory Names

**Description:** Add subdirectory name to pipeline.toml configuration and use that value instead of hardcoded "import_export".

**Pros:**
- Removes hardcoded value from code
- Single source of truth (config)
- Easy to change (update config only)
- Non-breaking (default "import_export" maintained)
- Clear intent (configurable)
- Modules read from config instead of assuming

**Cons:**
- Still has some coupling (modules know which subdirectory to use)
- Doesn't create full abstraction (still hardcoded pattern)
- Changing multiple subdirectories requires updating config (more complex)
- Doesn't solve all path coupling issues

**Effort:** Small (1-2 hours)

**Risk:** Low (simple change, easy to rollback)

**Implementation:**
```toml
# Update pipeline.toml

[sources.import_export_receipts]
output_subdir = "import_export"  # Already exists, make sure it's the source of truth

[sources.receivable]
output_subdir = "receivable"  # Make sure this exists for consistency

# Add new config for subdirectory names
[path_config.subdirectories]
import_export = "import_export"
receivable = "receivable"
payable = "payable"
cashflow = "cashflow"
```

```python
# Update src/modules/ingest.py

# Read subdirectory name from config
import_export_config = RAW_SOURCES["import_export_receipts"]
subdir_name = import_export_config.get("subdir_name", "import_export")

csv_path = STAGING_DATA_DIR / subdir_name / f"{year_num}_{month}_{tab}.csv"
```

```python
# Update src/pipeline/data_loader.py

class DataLoader:
    def __init__(self, config: Dict = None):
        # ... existing logic ...
        self.path_config = PathConfig(config)

    def load_products(self) -> Dict[str, pd.DataFrame]:
        # Read subdirectory name from config
        import_export_config = self.config.get("sources", {}).get("import_export_receipts", {})
        subdir_name = import_export_config.get("subdir_name", "import_export")

        import_export_dir = self.staging_dir / subdir_name
```

### Option 3: Strategy Pattern with PathProvider Interface

**Description:** Create abstract PathProvider interface with concrete implementations for different source types.

**Pros:**
- Clean separation of concerns (path logic isolated)
- Extensible (add new path strategy = add new provider)
- Dependency Inversion (modules depend on interface, not concrete)
- Testable (mock PathProvider)
- Flexible (can change strategy without touching module code)

**Cons:**
- Most complex solution (requires interface design, multiple providers)
- Over-engineering for simple path configuration
- Higher initial implementation effort (8-12 hours)
- May confuse team (why so complex for paths?)

**Effort:** Large (8-12 hours)

**Risk:** Low (well-established pattern, but high complexity for simple need)

**Implementation:**
```python
# Create src/pipeline/path_provider.py

from abc import ABC, abstractmethod
from pathlib import Path

class PathProvider(ABC):
    """Interface for data path providers."""

    @abstractmethod
    def get_output_path(self, source_key: str, filename: str) -> Path:
        """Get output path for a file."""
        pass

class StagingPathProvider(PathProvider):
    """Provider for sources that write to staging directly."""

    def __init__(self, config: Dict):
        self.staging_dir = Path(config["dirs"]["staging"])
        self.subdir_map = {
            source_key: config.get("subdir_name", "default")
            for source_key, config in config.get("sources", {}).items()
        }

    def get_output_path(self, source_key: str, filename: str) -> Path:
        subdir = self.subdir_map.get(source_key, "default")
        return self.staging_dir / subdir / filename

# Use in modules
path_provider = StagingPathProvider(_CONFIG)
csv_path = path_provider.get_output_path("import_export_receipts", f"{year_num}_{month}_{tab}.csv")
```

## Recommended Action

Implement **Option 1** (Centralized Path Configuration) as it provides:
- Single source of truth for all paths
- Easy to change (update config only)
- Follows Dependency Inversion Principle
- Removes code duplication (path logic centralized)
- Testable abstraction (can mock PathConfig)
- Well-established pattern (configuration module)

1. Create `src/utils/path_config.py` with PathConfig class
2. Define path getters for all directory types (raw, staging, validated, export)
3. Update `pipeline.toml` to ensure consistent structure
4. Refactor `ingest.py` to use PathConfig instead of hardcoded paths
5. Refactor `data_loader.py` to use PathConfig
6. Add tests for PathConfig class
7. Update AGENTS.md with path configuration guidance
8. Remove hardcoded "import_export" strings from code

**Alternative:** If Option 1 is too much work, implement **Option 2** (Configuration-Driven Subdirectory Names) as simpler interim solution.

## Acceptance Criteria

- [x] `src/utils/path_config.py` created with PathConfig class
- [x] PathConfig reads from pipeline.toml
- [x] All path getters defined (raw_output_dir, staging_output_dir, etc.)
- [x] Hardcoded "import_export" strings removed from ingest.py
- [x] Hardcoded "import_export" strings removed from data_loader.py
- [x] Both modules use PathConfig for all paths
- [x] PathConfig tests cover all directory types
- [x] Tests verify path construction uses config
- [x] Documentation updated in AGENTS.md
- [x] No path duplication remains in codebase

## Work Log

### 2026-01-23 - Initial Review
- Created todo file from architecture-strategist and pattern-recognition findings
- Analyzed tight coupling between ingest.py and data_loader.py
- Identified hardcoded paths duplicated in 2+ locations

### 2026-01-24 - Implementation Complete
- Created `src/utils/path_config.py` with PathConfig class
- Updated `ingest.py` to use PathConfig instead of hardcoded paths
- Updated `data_loader.py` to use PathConfig
- Created comprehensive tests in `tests/test_path_config.py`
- Updated AGENTS.md with path configuration guidance
- All 13 PathConfig tests pass
- No hardcoded "import_export" strings remain in modified files
- Tight coupling eliminated - modules depend on abstraction
- Found Dependency Inversion violation
- Proposed 3 solution options with complexity tradeoffs
- Selected Option 1 (Centralized Path Configuration) as recommended approach
- Documented refactoring effort and architectural benefits

### 2026-01-24 - Approved for Work
**By:** Claude Triage System
**Actions:**
- Issue approved during triage session
- Status changed from pending → ready
- Ready to be picked up and worked on

**Learnings:**
- Critical architectural violation affecting maintainability
- Medium effort (4-6 hours) improves SOLID compliance
- Centralized path configuration eliminates tight coupling

---

## Technical Details

**Affected Files:**
- `src/modules/ingest.py:217` (hardcoded staging path)
- `src/pipeline/data_loader.py:87-90` (hardcoded import_export subdirectory)
- `pipeline.toml:20` (config structure)

**Root Cause:**
No abstraction for path configuration, hardcoded values in multiple places

**Related Code:**
- All file I/O operations across codebase
- Configuration loading (duplicated in 4+ files)
- Data loading module

**Database Changes:** None

**Migration Required:** None

**SOLID Compliance:**
- Before: ❌ Dependency Inversion, ❌ Single Responsibility (path logic scattered)
- After Option 1: ✅ Dependency Inversion, ✅ Single Responsibility

**Architecture Grade:**
- Before: C (7/10) - Tight coupling, hard to extend
- After Option 1: B+ (8/10) - Loose coupling, configurable, testable

## Resources

- **Dependency Inversion Principle:** https://en.wikipedia.org/wiki/Dependency_inversion_principle
- **Configuration Patterns:** https://12factor.net/best-practices/configuration/
- **Related PR:** Commit 487d1c7 on branch refactor-switch-import-export-to-cleaned-tabs
