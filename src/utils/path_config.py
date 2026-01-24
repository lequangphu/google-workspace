# -*- coding: utf-8 -*-
"""Centralized path configuration for all data directories.

Reads paths from pipeline.toml and provides consistent directory structure
for all modules. Eliminates hardcoded paths and tight coupling between modules.

This implements Option 1 from TODO #008 to resolve tight coupling between
ingest.py and data_loader.py via hardcoded path strings.
"""

import logging
from pathlib import Path
from typing import Optional

import tomllib

logger = logging.getLogger(__name__)


class PathConfig:
    """Centralized path configuration.

    Reads paths from pipeline.toml and provides consistent
    directory structure for all modules.

    Usage:
        path_config = PathConfig()
        staging_dir = path_config.get_staging_output_dir("import_export_receipts")
        staging_dir = path_config.import_export_staging_dir()  # convenience method
    """

    def __init__(self, config_path: Optional[Path] = None):
        """Initialize PathConfig from pipeline.toml.

        Args:
            config_path: Path to pipeline.toml. If None, uses default location.

        Raises:
            FileNotFoundError: If config file not found.
        """
        if config_path is None:
            config_path = Path(__file__).parent.parent.parent / "pipeline.toml"

        if not config_path.exists():
            raise FileNotFoundError(
                f"Config file not found: {config_path}. "
                "See AGENTS.md for setup instructions."
            )

        with open(config_path, "rb") as f:
            self._config = tomllib.load(f)

        self.raw_data_dir = Path(self._config["dirs"]["raw_data"])
        self.staging_data_dir = Path(self._config["dirs"]["staging"])
        self.validated_data_dir = Path(self._config["dirs"]["validated"])
        self.erp_export_dir = Path(self._config["dirs"]["erp_export"])

    def get_raw_output_dir(self, source_key: str) -> Path:
        """Get raw output directory for a source.

        Args:
            source_key: Source key from pipeline.toml (e.g., "import_export_receipts").

        Returns:
            Path to raw output directory.

        Raises:
            KeyError: If source_key not found in config.
        """
        subdir = self._config["sources"][source_key]["output_subdir"]
        return self.raw_data_dir / subdir

    def get_staging_output_dir(self, source_key: str) -> Path:
        """Get staging output directory for a source.

        Args:
            source_key: Source key from pipeline.toml (e.g., "import_export_receipts").

        Returns:
            Path to staging output directory.

        Raises:
            KeyError: If source_key not found in config.
        """
        subdir = self._config["sources"][source_key]["output_subdir"]
        return self.staging_data_dir / subdir

    def import_export_staging_dir(self) -> Path:
        """Get staging directory for import_export receipts.

        Convenience method for import_export_receipts source.

        Returns:
            Path to staging/import_export directory.
        """
        return self.staging_data_dir / "import_export"

    def receivable_raw_dir(self) -> Path:
        """Get raw directory for receivable source.

        Convenience method for receivable source.

        Returns:
            Path to raw/receivable directory.
        """
        return self.raw_data_dir / "receivable"

    def payable_raw_dir(self) -> Path:
        """Get raw directory for payable source.

        Convenience method for payable source.

        Returns:
            Path to raw/payable directory.
        """
        return self.raw_data_dir / "payable"

    def cashflow_raw_dir(self) -> Path:
        """Get raw directory for cashflow source.

        Convenience method for cashflow source.

        Returns:
            Path to raw/cashflow directory.
        """
        return self.raw_data_dir / "cashflow"

    def import_export_raw_dir(self) -> Path:
        """Get raw directory for import_export_receipts source.

        Convenience method for import_export_receipts source.

        Returns:
            Path to raw/import_export directory.
        """
        return self.raw_data_dir / "import_export"
