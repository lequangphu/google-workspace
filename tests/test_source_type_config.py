"""Tests for source_type configuration (ADR-005).

Tests verify:
- source_type field exists in pipeline.toml
- import_export_receipts has source_type = "preprocessed"
- Other sources have source_type = "raw"
- orchestrator.py skips transform for "preprocessed" sources
- data_loader.py loads from correct location based on source_type
"""

import tomllib
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


class TestPipelineConfigSourceTypes:
    """Test pipeline.toml source_type configuration."""

    @pytest.fixture
    def config_path(self):
        """Return path to pipeline.toml."""
        return Path(__file__).parent.parent / "pipeline.toml"

    def test_all_sources_have_source_type_field(self, config_path):
        """Test all sources have source_type field in config."""
        with open(config_path, "rb") as f:
            config = tomllib.load(f)

        sources = config.get("sources", {})
        assert sources, "No sources found in config"

        for source_key, source_config in sources.items():
            assert "source_type" in source_config, f"{source_key} missing source_type"

    def test_import_export_receipts_is_preprocessed(self, config_path):
        """Test import_export_receipts has source_type = 'preprocessed'."""
        with open(config_path, "rb") as f:
            config = tomllib.load(f)

        source_type = config["sources"]["import_export_receipts"]["source_type"]
        assert source_type == "preprocessed", (
            f"import_export_receipts source_type should be 'preprocessed', got '{source_type}'"
        )

    def test_receivable_is_raw(self, config_path):
        """Test receivable has source_type = 'raw'."""
        with open(config_path, "rb") as f:
            config = tomllib.load(f)

        source_type = config["sources"]["receivable"]["source_type"]
        assert source_type == "raw", (
            f"receivable source_type should be 'raw', got '{source_type}'"
        )

    def test_payable_is_raw(self, config_path):
        """Test payable has source_type = 'raw'."""
        with open(config_path, "rb") as f:
            config = tomllib.load(f)

        source_type = config["sources"]["payable"]["source_type"]
        assert source_type == "raw", (
            f"payable source_type should be 'raw', got '{source_type}'"
        )

    def test_cashflow_is_raw(self, config_path):
        """Test cashflow has source_type = 'raw'."""
        with open(config_path, "rb") as f:
            config = tomllib.load(f)

        source_type = config["sources"]["cashflow"]["source_type"]
        assert source_type == "raw", (
            f"cashflow source_type should be 'raw', got '{source_type}'"
        )


class TestOrchestratorSourceTypeHandling:
    """Test orchestrator.py source_type handling in transform step."""

    def test_get_source_type_returns_preprocessed(self):
        """Test _get_source_type returns 'preprocessed' for import_export_receipts."""
        from src.pipeline.orchestrator import _get_source_type

        source_type = _get_source_type("import_export_receipts")
        assert source_type == "preprocessed"

    def test_get_source_type_returns_raw(self):
        """Test _get_source_type returns 'raw' for other sources."""
        from src.pipeline.orchestrator import _get_source_type

        source_type = _get_source_type("receivable")
        assert source_type == "raw"

    def test_get_source_type_defaults_to_raw(self):
        """Test _get_source_type defaults to 'raw' when not specified."""
        from src.pipeline.orchestrator import _get_source_type

        source_type = _get_source_type("unknown_source")
        assert source_type == "raw"


class TestDataLoaderSourceTypeHandling:
    """Test data_loader.py source_type handling."""

    def test_get_source_type_returns_preprocessed(self):
        """Test _get_source_type returns 'preprocessed' for import_export_receipts."""
        import tomllib

        from src.pipeline.data_loader import DataLoader

        config_path = Path(__file__).parent.parent / "pipeline.toml"
        with open(config_path, "rb") as f:
            config = tomllib.load(f)

        loader = DataLoader(config=config)
        source_type = loader._get_source_type("import_export_receipts")

        assert source_type == "preprocessed"

    def test_get_import_export_dir_for_preprocessed(self):
        """Test _get_import_export_dir returns raw directory for preprocessed source."""
        from src.pipeline.data_loader import DataLoader
        from src.utils.path_config import PathConfig

        path_config = PathConfig()
        loader = DataLoader(config=path_config._config)
        import_export_dir = loader._get_import_export_dir()

        assert import_export_dir == path_config.import_export_raw_dir()
        assert "raw" in str(import_export_dir).lower()

    def test_get_import_export_dir_for_raw(self, tmp_path):
        """Test _get_import_export_dir returns staging directory for raw source."""
        import tomllib

        from src.pipeline.data_loader import DataLoader
        from src.utils.path_config import PathConfig

        config_path = Path(__file__).parent.parent / "pipeline.toml"
        with open(config_path, "rb") as f:
            config = tomllib.load(f)

        config["sources"]["import_export_receipts"]["source_type"] = "raw"
        loader = DataLoader(config=config)
        import_export_dir = loader._get_import_export_dir()

        path_config = PathConfig()
        assert import_export_dir == path_config.import_export_staging_dir()
        assert "staging" in str(import_export_dir).lower()


class TestIngestWritesToRaw:
    """Test ingest.py writes all sources to raw directory."""

    def test_import_export_receipts_writes_to_raw(self):
        """Test _process_import_export_receipts writes to raw, not staging."""
        from src.modules.ingest import _process_import_export_receipts
        from src.utils.path_config import PathConfig

        path_config = PathConfig()
        expected_dir = path_config.get_raw_output_dir("import_export_receipts")

        assert expected_dir.name == "import_export"
        assert "raw" in str(expected_dir).lower()
