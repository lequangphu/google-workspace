# -*- coding: utf-8 -*-
"""Tests for src/utils/path_config.py."""

import logging
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from src.utils.path_config import PathConfig

logger = logging.getLogger(__name__)


class TestPathConfig:
    """Test PathConfig class."""

    @pytest.fixture
    def temp_config_dir(self):
        """Create a temporary directory with test pipeline.toml."""
        with TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "pipeline.toml"
            config_content = """
[dirs]
raw_data = "data/00-raw"
staging = "data/01-staging"
validated = "data/02-validated"
erp_export = "data/03-erp-export"

[sources.import_export_receipts]
type = "folder"
description = "Test source"
root_folder_id = "test_id"
receipts_subfolder_name = "Test"
tabs = ["Chi tiết nhập", "Chi tiết xuất"]
output_subdir = "import_export"

[sources.receivable]
type = "spreadsheet"
description = "Receivable source"
root_folder_id = "test_id"
spreadsheet_name = "Test.xlsx"
spreadsheet_id = "test_spreadsheet_id"
output_subdir = "receivable"

[sources.payable]
type = "spreadsheet"
description = "Payable source"
root_folder_id = "test_id"
spreadsheet_name = "Test.xlsx"
spreadsheet_id = "test_spreadsheet_id"
output_subdir = "payable"

[sources.cashflow]
type = "spreadsheet"
description = "Cashflow source"
root_folder_id = "test_id"
spreadsheet_name = "Test.xlsx"
output_subdir = "cashflow"
"""
            config_path.write_text(config_content)
            yield tmpdir, config_path

    def test_init_with_default_config(self, temp_config_dir):
        """Initialize PathConfig with default pipeline.toml location."""
        tmpdir, config_path = temp_config_dir

        path_config = PathConfig(config_path)

        assert path_config.raw_data_dir == Path("data/00-raw")
        assert path_config.staging_data_dir == Path("data/01-staging")
        assert path_config.validated_data_dir == Path("data/02-validated")
        assert path_config.erp_export_dir == Path("data/03-erp-export")

    def test_init_with_custom_config(self, temp_config_dir):
        """Initialize PathConfig with custom config path."""
        tmpdir, config_path = temp_config_dir

        path_config = PathConfig(config_path)

        assert path_config.raw_data_dir == Path("data/00-raw")
        assert path_config.staging_data_dir == Path("data/01-staging")

    def test_init_with_missing_config(self):
        """Raise FileNotFoundError when config file doesn't exist."""
        with pytest.raises(FileNotFoundError, match="Config file not found"):
            PathConfig(Path("nonexistent/pipeline.toml"))

    def test_get_raw_output_dir(self, temp_config_dir):
        """Get raw output directory for a source."""
        tmpdir, config_path = temp_config_dir

        path_config = PathConfig(config_path)

        import_export_dir = path_config.get_raw_output_dir("import_export_receipts")
        assert import_export_dir == Path("data/00-raw") / "import_export"

        receivable_dir = path_config.get_raw_output_dir("receivable")
        assert receivable_dir == Path("data/00-raw") / "receivable"

    def test_get_staging_output_dir(self, temp_config_dir):
        """Get staging output directory for a source."""
        tmpdir, config_path = temp_config_dir

        path_config = PathConfig(config_path)

        import_export_dir = path_config.get_staging_output_dir("import_export_receipts")
        assert import_export_dir == Path("data/01-staging") / "import_export"

        receivable_dir = path_config.get_staging_output_dir("receivable")
        assert receivable_dir == Path("data/01-staging") / "receivable"

    def test_get_staging_output_dir_invalid_key(self, temp_config_dir):
        """Raise KeyError for invalid source key."""
        tmpdir, config_path = temp_config_dir

        path_config = PathConfig(config_path)

        with pytest.raises(KeyError):
            path_config.get_staging_output_dir("invalid_source")

    def test_import_export_staging_dir(self, temp_config_dir):
        """Get import_export staging directory (convenience method)."""
        tmpdir, config_path = temp_config_dir

        path_config = PathConfig(config_path)

        import_export_dir = path_config.import_export_staging_dir()
        assert import_export_dir == Path("data/01-staging") / "import_export"

    def test_import_export_raw_dir(self, temp_config_dir):
        """Get import_export raw directory (convenience method)."""
        tmpdir, config_path = temp_config_dir

        path_config = PathConfig(config_path)

        import_export_dir = path_config.import_export_raw_dir()
        assert import_export_dir == Path("data/00-raw") / "import_export"

    def test_receivable_raw_dir(self, temp_config_dir):
        """Get receivable raw directory (convenience method)."""
        tmpdir, config_path = temp_config_dir

        path_config = PathConfig(config_path)

        receivable_dir = path_config.receivable_raw_dir()
        assert receivable_dir == Path("data/00-raw") / "receivable"

    def test_payable_raw_dir(self, temp_config_dir):
        """Get payable raw directory (convenience method)."""
        tmpdir, config_path = temp_config_dir

        path_config = PathConfig(config_path)

        payable_dir = path_config.payable_raw_dir()
        assert payable_dir == Path("data/00-raw") / "payable"

    def test_cashflow_raw_dir(self, temp_config_dir):
        """Get cashflow raw directory (convenience method)."""
        tmpdir, config_path = temp_config_dir

        path_config = PathConfig(config_path)

        cashflow_dir = path_config.cashflow_raw_dir()
        assert cashflow_dir == Path("data/00-raw") / "cashflow"

    def test_path_uses_config_not_hardcoded(self, temp_config_dir):
        """Verify paths come from config, not hardcoded values."""
        tmpdir, config_path = temp_config_dir

        path_config = PathConfig(config_path)

        import_export_staging = path_config.import_export_staging_dir()

        assert "import_export" in str(import_export_staging)
        assert "data/01-staging" in str(import_export_staging)

    def test_all_directory_types_defined(self, temp_config_dir):
        """All directory types are defined in PathConfig."""
        tmpdir, config_path = temp_config_dir

        path_config = PathConfig(config_path)

        assert hasattr(path_config, "raw_data_dir")
        assert hasattr(path_config, "staging_data_dir")
        assert hasattr(path_config, "validated_data_dir")
        assert hasattr(path_config, "erp_export_dir")

    def test_all_convenience_methods_defined(self, temp_config_dir):
        """All convenience methods are defined and working."""
        tmpdir, config_path = temp_config_dir

        path_config = PathConfig(config_path)

        assert callable(path_config.import_export_staging_dir)
        assert callable(path_config.import_export_raw_dir)
        assert callable(path_config.receivable_raw_dir)
        assert callable(path_config.payable_raw_dir)
        assert callable(path_config.cashflow_raw_dir)

        import_export_staging_dir = path_config.import_export_staging_dir()
        import_export_raw_dir = path_config.import_export_raw_dir()
        receivable_dir = path_config.receivable_raw_dir()
        payable_dir = path_config.payable_raw_dir()
        cashflow_dir = path_config.cashflow_raw_dir()

        assert import_export_staging_dir == Path("data/01-staging") / "import_export"
        assert import_export_raw_dir == Path("data/00-raw") / "import_export"
        assert receivable_dir == Path("data/00-raw") / "receivable"
        assert payable_dir == Path("data/00-raw") / "payable"
        assert cashflow_dir == Path("data/00-raw") / "cashflow"
