"""Tests for ingest.py CLI argument parsing and source selection."""

import subprocess
import sys
from pathlib import Path


class TestIngestCLI:
    """Test command-line interface for ingest.py."""

    @staticmethod
    def run_ingest_help():
        """Get help output from ingest.py."""
        result = subprocess.run(
            [sys.executable, "-m", "src.modules.ingest", "--help"],
            cwd=Path(__file__).parent.parent,
            capture_output=True,
            text=True,
        )
        return result.returncode, result.stdout, result.stderr

    @staticmethod
    def run_ingest_with_args(args):
        """Run ingest.py with specific arguments."""
        cmd = [sys.executable, "-m", "src.modules.ingest"] + args
        result = subprocess.run(
            cmd,
            cwd=Path(__file__).parent.parent,
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.returncode, result.stdout, result.stderr

    def test_help_shows_options(self):
        """Help should show all CLI options."""
        returncode, stdout, stderr = self.run_ingest_help()
        assert returncode == 0
        assert "--only" in stdout
        assert "--skip" in stdout
        assert "--year" in stdout
        assert "--month" in stdout

    def test_help_lists_available_sources(self):
        """Help should list all available sources."""
        returncode, stdout, stderr = self.run_ingest_help()
        assert returncode == 0
        assert "cashflow" in stdout
        assert "import_export_receipts" in stdout
        assert "payable" in stdout
        assert "receivable" in stdout

    def test_help_shows_examples(self):
        """Help should show usage examples."""
        returncode, stdout, stderr = self.run_ingest_help()
        assert returncode == 0
        assert "--only receivable,payable" in stdout
        assert "--skip import_export_receipts" in stdout
        assert "--year 2024" in stdout
        assert "--month 1" in stdout

    def test_only_single_source(self):
        """--only with single source should work."""
        returncode, stdout, stderr = self.run_ingest_with_args(["--only", "receivable"])
        assert returncode == 0
        assert "Ingesting only: receivable" in stdout

    def test_only_multiple_sources(self):
        """--only with comma-separated sources should parse correctly."""
        returncode, stdout, stderr = self.run_ingest_with_args(
            ["--only", "receivable,payable"]
        )
        assert returncode == 0
        assert "Ingesting only: receivable, payable" in stdout

    def test_only_with_spaces(self):
        """--only should handle spaces after commas."""
        returncode, stdout, stderr = self.run_ingest_with_args(
            ["--only", "receivable, payable"]
        )
        assert returncode == 0
        assert "Ingesting only: receivable, payable" in stdout

    def test_skip_single_source(self):
        """--skip with single source should exclude it."""
        returncode, stdout, stderr = self.run_ingest_with_args(
            ["--skip", "import_export_receipts"]
        )
        assert returncode == 0
        assert "Skipping: import_export_receipts" in stdout
        assert (
            "Ingesting: receivable, cashflow, payable" in stdout
            or ("Ingesting: receivable, payable, cashflow" in stdout)
            or ("Ingesting: payable, receivable, cashflow" in stdout)
        )

    def test_skip_multiple_sources(self):
        """--skip with multiple sources should exclude all."""
        returncode, stdout, stderr = self.run_ingest_with_args(
            ["--skip", "import_export_receipts,payable"]
        )
        assert returncode == 0
        assert "Skipping: import_export_receipts, payable" in stdout
        assert "Ingesting: receivable, cashflow" in stdout or (
            "Ingesting: cashflow, receivable" in stdout
        )

    def test_invalid_only_source_fails(self):
        """--only with invalid source should fail."""
        returncode, stdout, stderr = self.run_ingest_with_args(
            ["--only", "invalid_source"]
        )
        assert returncode == 1
        assert "Invalid sources: ['invalid_source']" in stdout

    def test_invalid_skip_source_fails(self):
        """--skip with invalid source should fail."""
        returncode, stdout, stderr = self.run_ingest_with_args(
            ["--skip", "invalid_source"]
        )
        assert returncode == 1
        assert "Invalid sources to skip: ['invalid_source']" in stdout

    def test_only_and_skip_conflict(self):
        """Using both --only and --skip should fail."""
        returncode, stdout, stderr = self.run_ingest_with_args(
            ["--only", "receivable", "--skip", "payable"]
        )
        assert returncode == 1
        assert "Cannot use both --only and --skip simultaneously" in stdout


class TestSourceFiltering:
    """Test that source filtering produces correct subset."""

    def test_all_sources_available(self):
        """All raw sources should be defined."""
        returncode, stdout, stderr = self.run_help()
        sources = ["cashflow", "import_export_receipts", "payable", "receivable"]
        for source in sources:
            assert source in stdout, f"Source '{source}' not in help"

    @staticmethod
    def run_help():
        """Get help output."""
        result = subprocess.run(
            [sys.executable, "-m", "src.modules.ingest", "--help"],
            cwd=Path(__file__).parent.parent,
            capture_output=True,
            text=True,
        )
        return result.returncode, result.stdout, result.stderr
