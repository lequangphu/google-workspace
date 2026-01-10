"""Path utilities for the tire-shop-erp-migration project.

This module provides centralized path utilities to avoid duplication across
the codebase (ADR-1: Configuration-Driven Pipeline).
"""

from pathlib import Path


def get_workspace_root() -> Path:
    """Get the project workspace root directory.

    The workspace root is the parent directory of the src/ directory.
    This is calculated from the location of this file to work correctly
    regardless of where the module is imported from.

    Returns:
        Path: The workspace root directory.
    """
    return Path(__file__).parent.parent.parent


def ensure_dir(path: Path) -> None:
    """Ensure a directory exists, creating it if necessary.

    Args:
        path: Path to the directory to ensure.

    Returns:
        None
    """
    path.mkdir(parents=True, exist_ok=True)


def list_csv_files(dirpath: Path) -> list[Path]:
    """List all CSV files in a directory and subdirectories.

    Args:
        dirpath: Path to the directory.

    Returns:
        List of Path objects for CSV files, sorted alphabetically.
    """
    if not dirpath.exists():
        return []
    return sorted(dirpath.rglob("*.csv"))
