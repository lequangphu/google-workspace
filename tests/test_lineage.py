# -*- coding: utf-8 -*-
"""Test data lineage tracking module.

Tests the DataLineage class for audit trail generation.
"""

import tempfile
from pathlib import Path

import pytest

from src.modules.lineage import DataLineage


class TestDataLineageInitialization:
    """Test DataLineage initialization."""

    def test_init_creates_output_dir(self):
        """Test that initialization creates output directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "lineage_test"
            lineage = DataLineage(output_dir)
            assert output_dir.exists()

    def test_init_with_existing_dir(self):
        """Test initialization with existing directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            lineage = DataLineage(output_dir)
            assert output_dir.exists()


class TestDataLineageTracking:
    """Test lineage tracking functionality."""

    def test_track_success(self):
        """Test tracking successful row processing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            lineage = DataLineage(Path(tmpdir))
            lineage.track(
                source_file="test.csv",
                source_row=0,
                output_row=0,
                operation="test_op",
                status="success",
            )
            assert len(lineage.entries) == 1
            assert lineage.entries[0]["status"] == "success"

    def test_track_rejection(self):
        """Test tracking rejected row processing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            lineage = DataLineage(Path(tmpdir))
            lineage.track(
                source_file="test.csv",
                source_row=1,
                output_row=None,
                operation="test_op",
                status="rejected: invalid format",
            )
            assert len(lineage.entries) == 1
            assert "rejected" in lineage.entries[0]["status"]
            assert lineage.entries[0]["output_row"] == "REJECTED"

    def test_track_multiple_rows(self):
        """Test tracking multiple rows."""
        with tempfile.TemporaryDirectory() as tmpdir:
            lineage = DataLineage(Path(tmpdir))
            for i in range(5):
                lineage.track(
                    source_file="test.csv",
                    source_row=i,
                    output_row=i,
                    operation="test_op",
                    status="success",
                )
            assert len(lineage.entries) == 5


class TestDataLineageSummary:
    """Test lineage summary statistics."""

    def test_summary_all_success(self):
        """Test summary with all successful rows."""
        with tempfile.TemporaryDirectory() as tmpdir:
            lineage = DataLineage(Path(tmpdir))
            for i in range(10):
                lineage.track(
                    source_file="test.csv",
                    source_row=i,
                    output_row=i,
                    operation="test_op",
                    status="success",
                )
            summary = lineage.summary()
            assert summary["total"] == 10
            assert summary["success"] == 10
            assert summary["rejected"] == 0
            assert summary["success_rate"] == 100.0

    def test_summary_mixed_success_rejection(self):
        """Test summary with mixed success and rejection."""
        with tempfile.TemporaryDirectory() as tmpdir:
            lineage = DataLineage(Path(tmpdir))
            for i in range(10):
                if i % 2 == 0:
                    lineage.track(
                        source_file="test.csv",
                        source_row=i,
                        output_row=i // 2,
                        operation="test_op",
                        status="success",
                    )
                else:
                    lineage.track(
                        source_file="test.csv",
                        source_row=i,
                        output_row=None,
                        operation="test_op",
                        status="rejected: test",
                    )
            summary = lineage.summary()
            assert summary["total"] == 10
            assert summary["success"] == 5
            assert summary["rejected"] == 5
            assert summary["success_rate"] == 50.0

    def test_summary_empty(self):
        """Test summary with no entries."""
        with tempfile.TemporaryDirectory() as tmpdir:
            lineage = DataLineage(Path(tmpdir))
            summary = lineage.summary()
            assert summary["total"] == 0
            assert summary["success"] == 0
            assert summary["rejected"] == 0
            assert summary["success_rate"] == 0


class TestDataLineageSave:
    """Test lineage CSV saving."""

    def test_save_creates_file(self):
        """Test that save creates a CSV file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            lineage = DataLineage(output_dir)
            lineage.track(
                source_file="test.csv",
                source_row=0,
                output_row=0,
                operation="test_op",
                status="success",
            )
            filepath = lineage.save()
            assert filepath is not None
            assert filepath.exists()
            assert filepath.suffix == ".csv"

    def test_save_file_content(self):
        """Test saved CSV file content."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            lineage = DataLineage(output_dir)
            lineage.track(
                source_file="test.csv",
                source_row=0,
                output_row=0,
                operation="test_op",
                status="success",
            )
            filepath = lineage.save()
            with open(filepath, "r", encoding="utf-8") as f:
                content = f.read()
                assert "source_file" in content
                assert "test.csv" in content
                assert "success" in content

    def test_save_empty_lineage(self):
        """Test save with no entries."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            lineage = DataLineage(output_dir)
            filepath = lineage.save()
            assert filepath is None
