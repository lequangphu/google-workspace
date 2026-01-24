"""Tests for src/modules/ingest.py and google_api.py."""

import pytest
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.modules.ingest import RAW_SOURCES, _validate_year_month
from src.modules.google_api import (
    parse_file_metadata,
    find_year_folders,
    find_sheets_in_folder,
    get_sheet_tabs,
)


@pytest.fixture(autouse=True)
def mock_google_drive_connection():
    """Mock Google Drive connection to prevent actual API calls during tests."""
    with patch("src.modules.ingest.connect_to_drive") as mock_connect:
        mock_connect.return_value = (MagicMock(), MagicMock())
        yield


class TestValidateYearMonth:
    """Test _validate_year_month function for path traversal protection."""

    def test_valid_year_month(self):
        """Valid year and month pass validation."""
        _validate_year_month(2025, 1)
        _validate_year_month(2020, 12)
        _validate_year_month(2030, 6)

    def test_invalid_year_too_low(self):
        """Year below 2020 raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            _validate_year_month(2019, 1)
        assert "Invalid year: 2019 (must be 2020-2030)" in str(exc_info.value)

    def test_invalid_year_too_high(self):
        """Year above 2030 raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            _validate_year_month(2031, 1)
        assert "Invalid year: 2031 (must be 2020-2030)" in str(exc_info.value)

    def test_invalid_month_too_low(self):
        """Month below 1 raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            _validate_year_month(2025, 0)
        assert "Invalid month: 0 (must be 1-12)" in str(exc_info.value)

    def test_invalid_month_too_high(self):
        """Month above 12 raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            _validate_year_month(2025, 13)
        assert "Invalid month: 13 (must be 1-12)" in str(exc_info.value)

    def test_edge_cases(self):
        """Test boundary values."""
        _validate_year_month(2020, 1)  # Lower boundary
        _validate_year_month(2030, 12)  # Upper boundary

    def test_negative_values(self):
        """Negative year and month raise ValueError."""
        with pytest.raises(ValueError):
            _validate_year_month(-1, 1)
        with pytest.raises(ValueError):
            _validate_year_month(2025, -1)

    def test_extreme_values(self):
        """Extremely large year raises ValueError."""
        with pytest.raises(ValueError):
            _validate_year_month(9999, 1)


class TestParseFileMetadata:
    """Test parse_file_metadata function."""

    def test_valid_metadata(self):
        """Parse valid filename."""
        year, month = parse_file_metadata("XUẤT NHẬP TỒN TỔNG T01.23")
        assert year == 2023
        assert month == 1

    def test_valid_metadata_december(self):
        """Parse December (month 12)."""
        year, month = parse_file_metadata("XUẤT NHẬP TỒN TỔNG T12.24")
        assert year == 2024
        assert month == 12

    def test_invalid_metadata(self):
        """Invalid filename returns None, None."""
        year, month = parse_file_metadata("invalid_file.csv")
        assert year is None
        assert month is None

    def test_empty_string(self):
        """Empty string returns None, None."""
        year, month = parse_file_metadata("")
        assert year is None
        assert month is None


class TestFindYearFolders:
    """Test find_year_folders function."""

    def test_find_year_folders(self):
        """Mock Google Drive API call."""
        mock_service = MagicMock()
        mock_service.files().list().execute.return_value = {
            "files": [
                {"id": "folder_1", "name": "TỔNG HỢP 2023"},
                {"id": "folder_2", "name": "TỔNG HỢP 2024"},
            ]
        }

        result = find_year_folders(mock_service)
        assert result == {
            "TỔNG HỢP 2023": "folder_1",
            "TỔNG HỢP 2024": "folder_2",
        }

    def test_find_year_folders_empty(self):
        """No folders found."""
        mock_service = MagicMock()
        mock_service.files().list().execute.return_value = {"files": []}

        result = find_year_folders(mock_service)
        assert result == {}


class TestFindSheetsInFolder:
    """Test find_sheets_in_folder function."""

    def test_find_sheets(self):
        """Mock Google Drive API call."""
        mock_service = MagicMock()
        mock_service.files().list().execute.return_value = {
            "files": [
                {
                    "id": "sheet_1",
                    "name": "XUẤT NHẬP TỒN TỔNG T01.23",
                    "modifiedTime": "2025-01-01T10:00:00.000Z",
                },
            ]
        }

        result = find_sheets_in_folder(mock_service, "folder_id")
        assert len(result) == 1
        assert result[0]["name"] == "XUẤT NHẬP TỒN TỔNG T01.23"

    def test_find_sheets_error(self):
        """Handle HTTP error gracefully."""
        from googleapiclient.errors import HttpError

        mock_service = MagicMock()
        mock_service.files().list().execute.side_effect = HttpError(
            MagicMock(status=403), b"Forbidden"
        )

        result = find_sheets_in_folder(mock_service, "folder_id")
        assert result == []


class TestGetSheetTabs:
    """Test get_sheet_tabs function."""

    def test_get_tabs(self):
        """Mock Google Sheets API call."""
        mock_service = MagicMock()
        mock_service.spreadsheets().get().execute.return_value = {
            "sheets": [
                {"properties": {"title": "CT.NHAP"}},
                {"properties": {"title": "CT.XUAT"}},
                {"properties": {"title": "XNT"}},
            ]
        }

        result = get_sheet_tabs(mock_service, "spreadsheet_id")
        assert result == ["CT.NHAP", "CT.XUAT", "XNT"]

    def test_get_tabs_error(self):
        """Handle HTTP error gracefully - should raise HttpError on 404."""
        from googleapiclient.errors import HttpError

        mock_service = MagicMock()
        mock_service.spreadsheets().get().execute.side_effect = HttpError(
            MagicMock(status=404), b"Not Found"
        )

        # 404 is a permanent error, no retry, should raise immediately
        with pytest.raises(HttpError):
            result = get_sheet_tabs(mock_service, "spreadsheet_id")


class TestRawSourcesConfiguration:
    """Test RAW_SOURCES configuration."""

    def test_raw_sources_defined(self):
        """RAW_SOURCES includes all 4 raw sources."""
        assert "import_export_receipts" in RAW_SOURCES
        assert "receivable" in RAW_SOURCES
        assert "payable" in RAW_SOURCES
        assert "cashflow" in RAW_SOURCES

    def test_import_export_receipts_config(self):
        """Verify import_export_receipts configuration."""
        config = RAW_SOURCES["import_export_receipts"]
        assert config["type"] == "folder"
        assert config["root_folder_id"] == "16CXAGzxxoBU8Ui1lXPxZoLVbDdsgwToj"
        assert config["receipts_subfolder_name"] == "Import Export Receipts"
        assert config["tabs"] == ["CT.NHAP", "CT.XUAT", "XNT"]
        assert config["output_subdir"] == "import_export"

    def test_receivable_config(self):
        """Verify receivable configuration."""
        config = RAW_SOURCES["receivable"]
        assert config["type"] == "spreadsheet"
        assert config["root_folder_id"] == "16CXAGzxxoBU8Ui1lXPxZoLVbDdsgwToj"
        assert config["spreadsheet_name"] == "Receivable.xlsx"
        assert "sheets" in config
        assert isinstance(config["sheets"], list)
        assert len(config["sheets"]) == 2
        sheet_names = [s["name"] for s in config["sheets"]]
        assert "TỔNG CÔNG NỢ" in sheet_names
        assert "Thong tin KH" in sheet_names

    def test_payable_config(self):
        """Verify payable configuration."""
        config = RAW_SOURCES["payable"]
        assert config["type"] == "spreadsheet"
        assert config["root_folder_id"] == "16CXAGzxxoBU8Ui1lXPxZoLVbDdsgwToj"
        assert config["spreadsheet_name"] == "Payable.xlsx"
        assert "sheets" in config
        assert isinstance(config["sheets"], list)
        assert len(config["sheets"]) == 2
        sheet_names = [s["name"] for s in config["sheets"]]
        assert "MÃ CTY" in sheet_names
        assert "TỔNG HỢP" in sheet_names

    def test_cashflow_config(self):
        """Verify cashflow configuration."""
        config = RAW_SOURCES["cashflow"]
        assert config["type"] == "spreadsheet"
        assert config["root_folder_id"] == "16CXAGzxxoBU8Ui1lXPxZoLVbDdsgwToj"
        assert config["spreadsheet_name"] == "Cashflow.xlsx"
        assert "sheets" in config
        assert isinstance(config["sheets"], list)
        assert len(config["sheets"]) == 2
        sheet_names = [s["name"] for s in config["sheets"]]
        assert "Tiền gửi" in sheet_names
        assert "Tien mat" in sheet_names


class TestIngestErrorHandling:
    """Test error handling in ingest_from_drive."""

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

    def test_invalid_source_error(self):
        """Invalid source should exit with code 1."""
        returncode, stdout, stderr = self.run_ingest_with_args(
            ["--only", "invalid_source"]
        )
        assert returncode == 1
        assert "Invalid sources: ['invalid_source']" in stdout

    def test_invalid_skip_source_error(self):
        """Invalid source to skip should exit with code 1."""
        returncode, stdout, stderr = self.run_ingest_with_args(
            ["--skip", "invalid_source"]
        )
        assert returncode == 1
        assert "Invalid sources to skip: ['invalid_source']" in stdout

    def test_only_and_skip_conflict_error(self):
        """Using both --only and --skip should exit with code 1."""
        returncode, stdout, stderr = self.run_ingest_with_args(
            ["--only", "receivable", "--skip", "payable"]
        )
        assert returncode == 1
        assert "Cannot use both --only and --skip simultaneously" in stdout
