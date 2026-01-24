# -*- coding: utf-8 -*-
"""Tests for StagingCache with LRU eviction."""

import logging
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import pytest

from src.utils.staging_cache import StagingCache, CACHE_MAXSIZE

logger = logging.getLogger(__name__)


@pytest.fixture
def temp_dir():
    """Create temporary directory for test CSV files."""
    with TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_csv(temp_dir):
    """Create a sample CSV file."""
    filepath = temp_dir / "test.csv"
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    df.to_csv(filepath, index=False)
    return filepath


@pytest.fixture
def multiple_csvs(temp_dir):
    """Create multiple CSV files for LRU eviction testing."""
    filepaths = []
    for i in range(100):
        filepath = temp_dir / f"test_{i}.csv"
        df = pd.DataFrame(
            {"col1": [i, i + 1, i + 2], "col2": [f"a{i}", f"b{i}", f"c{i}"]}
        )
        df.to_csv(filepath, index=False)
        filepaths.append(filepath)
    return filepaths


class TestStagingCacheBasic:
    """Test basic StagingCache functionality."""

    def test_get_dataframe_reads_file(self, sample_csv):
        """Test that get_dataframe reads file from disk."""
        StagingCache.invalidate()

        df = StagingCache.get_dataframe(sample_csv)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert list(df.columns) == ["col1", "col2"]

    def test_get_dataframe_cache_hit(self, sample_csv, caplog):
        """Test that subsequent calls hit the cache."""
        StagingCache.invalidate()

        with caplog.at_level(logging.DEBUG):
            df1 = StagingCache.get_dataframe(sample_csv)
            df2 = StagingCache.get_dataframe(sample_csv)

        assert df1 is df2

        debug_messages = [msg for msg in caplog.messages if "Cache" in msg]
        assert any("Cache miss" in msg for msg in debug_messages)
        assert any("Cache hit" in msg for msg in debug_messages)

    def test_get_dataframe_file_not_found(self, temp_dir):
        """Test that FileNotFoundError is raised for missing file."""
        filepath = temp_dir / "nonexistent.csv"

        with pytest.raises(FileNotFoundError, match="Staging file not found"):
            StagingCache.get_dataframe(filepath)

    def test_get_dataframe_accepts_string_path(self, sample_csv):
        """Test that get_dataframe accepts string path."""
        StagingCache.invalidate()

        df = StagingCache.get_dataframe(str(sample_csv))

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3

    def test_invalidate_clears_cache(self, sample_csv):
        """Test that invalidate clears all cache entries."""
        StagingCache.invalidate()

        StagingCache.get_dataframe(sample_csv)
        info1 = StagingCache.get_cache_info()
        assert info1["cached_files"] >= 1

        StagingCache.invalidate()

        info2 = StagingCache.get_cache_info()
        assert info2["cached_files"] == 0

        StagingCache.get_dataframe(sample_csv)
        info3 = StagingCache.get_cache_info()
        assert info3["cached_files"] >= 1


class TestStagingCacheLRUEviction:
    """Test LRU eviction behavior."""

    def test_lru_eviction_with_many_files(self, multiple_csvs):
        """Test that old entries are evicted when cache is full."""
        StagingCache.invalidate()

        filepaths = multiple_csvs

        for i, filepath in enumerate(filepaths):
            StagingCache.get_dataframe(filepath)

        info = StagingCache.get_cache_info()

        assert info["cached_files"] <= CACHE_MAXSIZE
        assert info["maxsize"] == CACHE_MAXSIZE

    def test_lru_evicted_files_reload(self, multiple_csvs):
        """Test that evicted files are reloaded from disk."""
        StagingCache.invalidate()

        filepaths = multiple_csvs

        for filepath in filepaths:
            StagingCache.get_dataframe(filepath)

        info_after_load = StagingCache.get_cache_info()

        first_file = filepaths[0]
        df1 = StagingCache.get_dataframe(first_file)
        info_after_reload = StagingCache.get_cache_info()

        assert df1 is not None
        assert info_after_reload["misses"] > info_after_load["misses"]

    def test_most_recently_used_keeps_file(self, multiple_csvs):
        """Test that frequently accessed files stay in cache."""
        StagingCache.invalidate()

        filepaths = multiple_csvs

        StagingCache.get_dataframe(filepaths[0])

        for filepath in filepaths[1:CACHE_MAXSIZE]:
            StagingCache.get_dataframe(filepath)

        StagingCache.get_dataframe(filepaths[0])

        for filepath in filepaths[CACHE_MAXSIZE : CACHE_MAXSIZE + 10]:
            StagingCache.get_dataframe(filepath)

        info_after = StagingCache.get_cache_info()

        assert info_after["cached_files"] <= CACHE_MAXSIZE


class TestStagingCacheMetrics:
    """Test cache metrics and info reporting."""

    def test_cache_info_returns_expected_fields(self, sample_csv):
        """Test that get_cache_info returns all expected fields."""
        StagingCache.invalidate()

        StagingCache.get_dataframe(sample_csv)

        info = StagingCache.get_cache_info()

        assert "cached_files" in info
        assert "maxsize" in info
        assert "hits" in info
        assert "misses" in info
        assert "hit_rate" in info

    def test_cache_info_tracks_hits_and_misses(self, sample_csv):
        """Test that cache info correctly tracks hits and misses."""
        StagingCache.invalidate()

        info_before = StagingCache.get_cache_info()
        assert info_before["hits"] == 0
        assert info_before["misses"] == 0

        StagingCache.get_dataframe(sample_csv)

        info_after_miss = StagingCache.get_cache_info()
        assert info_after_miss["misses"] == 1
        assert info_after_miss["hits"] == 0

        StagingCache.get_dataframe(sample_csv)

        info_after_hit = StagingCache.get_cache_info()
        assert info_after_hit["misses"] == 1
        assert info_after_hit["hits"] == 1

    def test_cache_hit_rate_calculation(self, sample_csv):
        """Test that hit rate is calculated correctly."""
        StagingCache.invalidate()

        for _ in range(5):
            StagingCache.get_dataframe(sample_csv)

        info = StagingCache.get_cache_info()

        expected_rate = info["hits"] / (info["hits"] + info["misses"])
        assert info["hit_rate"] == expected_rate


class TestStagingCachePreload:
    """Test preload functionality."""

    def test_preload_loads_multiple_files(self, temp_dir):
        """Test that preload loads multiple files into cache."""
        StagingCache.invalidate()

        filepaths = []
        for i in range(10):
            filepath = temp_dir / f"test_{i}.csv"
            df = pd.DataFrame({"col1": [i]})
            df.to_csv(filepath, index=False)
            filepaths.append(filepath)

        StagingCache.preload(filepaths)

        info = StagingCache.get_cache_info()
        assert info["cached_files"] == 10

    def test_preload_handles_nonexistent_files(self, temp_dir, caplog):
        """Test that preload gracefully handles missing files."""
        StagingCache.invalidate()

        filepath = temp_dir / "test.csv"
        df = pd.DataFrame({"col1": [1]})
        df.to_csv(filepath, index=False)

        missing_filepath = temp_dir / "missing.csv"
        filepaths = [filepath, missing_filepath]

        with caplog.at_level(logging.WARNING):
            StagingCache.preload(filepaths)

        assert any("Failed to preload missing.csv" in msg for msg in caplog.messages)

        info = StagingCache.get_cache_info()
        assert info["cached_files"] >= 1


class TestStagingCacheMemory:
    """Test memory usage and bounded behavior."""

    def test_cache_size_remains_bounded(self, multiple_csvs):
        """Test that cache size never exceeds maxsize."""
        StagingCache.invalidate()

        filepaths = multiple_csvs

        for filepath in filepaths:
            StagingCache.get_dataframe(filepath)
            info = StagingCache.get_cache_info()
            assert info["cached_files"] <= CACHE_MAXSIZE

    def test_cache_returns_reference_not_copy(self, sample_csv):
        """Test that get_dataframe returns reference to cached data."""
        StagingCache.invalidate()

        df1 = StagingCache.get_dataframe(sample_csv)
        df2 = StagingCache.get_dataframe(sample_csv)

        assert df1 is df2

    def test_caller_must_copy_if_needed(self, sample_csv):
        """Test that caller must explicitly copy if modifying data."""
        StagingCache.invalidate()

        df1 = StagingCache.get_dataframe(sample_csv)

        df1.loc[0, "col1"] = 999

        df2 = StagingCache.get_dataframe(sample_csv)

        assert df2.loc[0, "col1"] == 999


class TestStagingCacheConfiguration:
    """Test cache configuration."""

    def test_cache_maxsize_constant_exists(self):
        """Test that CACHE_MAXSIZE constant is defined."""
        assert isinstance(CACHE_MAXSIZE, int)
        assert CACHE_MAXSIZE > 0
        assert CACHE_MAXSIZE == 50

    def test_cache_maxsize_is_respected(self, multiple_csvs):
        """Test that actual cache respects CACHE_MAXSIZE."""
        StagingCache.invalidate()

        filepaths = multiple_csvs[: CACHE_MAXSIZE + 20]

        for filepath in filepaths:
            StagingCache.get_dataframe(filepath)

        info = StagingCache.get_cache_info()

        assert info["cached_files"] == CACHE_MAXSIZE
        assert info["maxsize"] == CACHE_MAXSIZE
