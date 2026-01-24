# -*- coding: utf-8 -*-
"""Staging data cache with LRU eviction.

This module provides a caching layer for reading staging data files,
reducing redundant I/O operations across multiple pipeline stages.

Features:
- LRU cache with configurable size limit (prevents unbounded memory growth)
- Automatic eviction of least recently used files
- Returns reference to cached DataFrame (caller must copy if modifying)
- Manual cache clearing support
- Cache hit/miss metrics

Configuration:
- CACHE_MAXSIZE: Maximum number of files to cache (default: 50)
"""

import logging
from pathlib import Path
from typing import Dict

from functools import lru_cache

import pandas as pd

logger = logging.getLogger(__name__)

CACHE_MAXSIZE = 50


class StagingCache:
    """Cache for staging data reads with LRU eviction.

    Caches DataFrames read from staging CSV files using LRU policy.
    Automatically evicts least recently used files when cache size limit reached.

    Usage:
        df = StagingCache.get_dataframe("/path/to/staging/data.csv")
        StagingCache.invalidate()  # Clear all cached data

    Note: Returns reference to cached DataFrame. If you need to modify the data,
    call df.copy() after retrieving.
    """

    @staticmethod
    @lru_cache(maxsize=CACHE_MAXSIZE)
    def _read_csv(filepath: str) -> pd.DataFrame:
        """Read CSV file - cached by LRU decorator.

        Args:
            filepath: String path to CSV file

        Returns:
            DataFrame loaded from file

        Raises:
            FileNotFoundError: If filepath does not exist
            pd.errors.EmptyDataError: If file is empty
        """
        path = Path(filepath)
        logger.debug(f"Cache miss: {path.name} - reading from file")
        return pd.read_csv(path, encoding="utf-8")

    @classmethod
    def get_dataframe(cls, filepath: Path) -> pd.DataFrame:
        """Get DataFrame from LRU cache or read from file.

        Args:
            filepath: Path to staging CSV file

        Returns:
            Cached or newly loaded DataFrame (reference to cached data)

        Raises:
            FileNotFoundError: If filepath does not exist
            pd.errors.EmptyDataError: If file is empty
        """
        if not isinstance(filepath, Path):
            filepath = Path(filepath)

        if not filepath.exists():
            raise FileNotFoundError(f"Staging file not found: {filepath}")

        df = cls._read_csv(str(filepath))
        logger.debug(f"Cache hit: {filepath.name}")
        return df

    @classmethod
    def invalidate(cls) -> None:
        """Clear all cached data.

        Invalidates entire LRU cache, forcing reload on next access.
        """
        cls._read_csv.cache_clear()
        logger.debug("Invalidated all cache entries")

    @classmethod
    def get_cache_info(cls) -> Dict[str, int]:
        """Get information about current cache state.

        Returns:
            Dict with keys:
                - cached_files: Number of files currently cached
                - maxsize: Maximum cache size
                - hits: Number of cache hits
                - misses: Number of cache misses
        """
        info = cls._read_csv.cache_info()
        return {
            "cached_files": info.currsize,
            "maxsize": info.maxsize,
            "hits": info.hits,
            "misses": info.misses,
            "hit_rate": round(info.hits / max(info.hits + info.misses, 1), 2)
            if info.hits + info.misses > 0
            else 0.0,
        }

    @classmethod
    def preload(cls, filepaths: list[Path]) -> None:
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
