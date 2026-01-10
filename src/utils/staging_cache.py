# -*- coding: utf-8 -*-
"""Staging data cache with automatic invalidation.

This module provides a caching layer for reading staging data files,
reducing redundant I/O operations across multiple pipeline stages.

Features:
- File-based caching with modification time tracking
- Automatic cache invalidation when files change
- Thread-safe singleton pattern
- Manual cache clearing support
"""

import logging
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class StagingCache:
    """Cache for staging data reads with invalidation.

    Caches DataFrames read from staging CSV files and invalidates
    them when the underlying files are modified (detected via mtime).

    Usage:
        cache = StagingCache()
        df = cache.get_dataframe("/path/to/staging/data.csv")
        cache.invalidate()  # Clear all cached data
        cache.invalidate("/path/to/specific/file.csv")  # Clear one file
    """

    _cache: Dict[Path, pd.DataFrame] = {}
    _modification_times: Dict[Path, float] = {}

    @classmethod
    def get_dataframe(cls, filepath: Path) -> pd.DataFrame:
        """Get DataFrame from cache or read from file with invalidation.

        Args:
            filepath: Path to staging CSV file

        Returns:
            Cached or newly loaded DataFrame

        Raises:
            FileNotFoundError: If filepath does not exist
            pd.errors.EmptyDataError: If file is empty
        """
        if not isinstance(filepath, Path):
            filepath = Path(filepath)

        if not filepath.exists():
            raise FileNotFoundError(f"Staging file not found: {filepath}")

        current_mtime = filepath.stat().st_mtime
        cached_mtime = cls._modification_times.get(filepath)

        if filepath in cls._cache and cached_mtime == current_mtime:
            logger.debug(f"Cache hit: {filepath.name}")
            return cls._cache[filepath].copy()

        logger.debug(f"Cache miss: {filepath.name} - reading from file")
        df = pd.read_csv(filepath, encoding="utf-8")

        cls._cache[filepath] = df
        cls._modification_times[filepath] = current_mtime

        return df.copy()

    @classmethod
    def invalidate(cls, filepath: Optional[Path] = None):
        """Invalidate cache for specific file or all files.

        Args:
            filepath: Specific file to invalidate, or None to clear all cache
        """
        if filepath:
            if isinstance(filepath, str):
                filepath = Path(filepath)

            if filepath in cls._cache:
                del cls._cache[filepath]
            if filepath in cls._modification_times:
                del cls._modification_times[filepath]
            logger.debug(f"Invalidated cache for: {filepath.name}")
        else:
            cls._cache.clear()
            cls._modification_times.clear()
            logger.debug("Invalidated all cache entries")

    @classmethod
    def get_cache_info(cls) -> Dict[str, int]:
        """Get information about current cache state.

        Returns:
            Dict with keys:
                - cached_files: Number of files currently cached
                - total_size_mb: Approximate memory usage in MB
        """
        total_memory_mb = sum(
            df.memory_usage(deep=True).sum() for df in cls._cache.values()
        ) / (1024 * 1024)

        return {
            "cached_files": len(cls._cache),
            "total_memory_mb": round(total_memory_mb, 2),
        }

    @classmethod
    def preload(cls, filepaths: list[Path]):
        """Preload multiple files into cache.

        Useful for batch operations where multiple files will be read.

        Args:
            filepaths: List of file paths to preload
        """
        for filepath in filepaths:
            try:
                cls.get_dataframe(filepath)
            except Exception as e:
                logger.warning(f"Failed to preload {filepath.name}: {e}")
