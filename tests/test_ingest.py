"""Tests for src/modules/ingest.py and google_api.py."""

from pathlib import Path
from unittest.mock import MagicMock

from src.modules.ingest import RAW_SOURCES
from src.modules.google_api import (
    parse_file_metadata,
    should_ingest_file,
    find_year_folders,
    find_sheets_in_folder,
    get_sheet_tabs,
)


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


class TestShouldIngestFile:
    """Test should_ingest_file function."""

    def test_file_does_not_exist(self):
        """Should ingest if file doesn't exist."""
        csv_path = Path("/tmp/nonexistent_file.csv")
        remote_time = "2025-01-01T10:00:00.000Z"
        assert should_ingest_file(csv_path, remote_time) is True

    def test_remote_newer_than_local(self, tmp_path):
        """Should ingest if remote is newer."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text("old data")

        # Set local file to 1 hour ago
        import time
        import os

        one_hour_ago = time.time() - 3600
        os.utime(csv_path, (one_hour_ago, one_hour_ago))

        # Remote time in the future
        remote_time = "2099-01-01T10:00:00.000Z"
        assert should_ingest_file(csv_path, remote_time) is True

    def test_local_newer_than_remote(self, tmp_path):
        """Should not ingest if local is newer."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text("new data")

        # Remote time in past
        remote_time = "2020-01-01T10:00:00.000Z"
        assert should_ingest_file(csv_path, remote_time) is False


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
        """Handle HTTP error gracefully."""
        from googleapiclient.errors import HttpError

        mock_service = MagicMock()
        mock_service.spreadsheets().get().execute.side_effect = HttpError(
            MagicMock(status=404), b"Not Found"
        )

        result = get_sheet_tabs(mock_service, "spreadsheet_id")
        assert result == []


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
        assert isinstance(config["folder_ids"], list)
        assert len(config["folder_ids"]) == 7
        assert "16CXAGzxxoBU8Ui1lXPxZoLVbDdsgwToj" in config["folder_ids"]
        assert config["tabs"] == ["CT.NHAP", "CT.XUAT", "XNT"]
        assert config["output_subdir"] == "import_export"

    def test_receivable_config(self):
        """Verify receivable configuration."""
        config = RAW_SOURCES["receivable"]
        assert config["type"] == "spreadsheet"
        assert (
            config["spreadsheet_id"] == "1kouZwJy8P_zZhjjn49Lfbp3KN81mhHADV7VKDhv5xkM"
        )
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
        assert (
            config["spreadsheet_id"] == "1b4LWWyfddfiMZWnFreTyC-epo17IR4lcbUnPpLW8X00"
        )
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
        assert (
            config["spreadsheet_id"] == "1OZ0cdEob37H8z0lGEI4gCet10ox5DgjO6u4wsQL29Ag"
        )
        assert "sheets" in config
        assert isinstance(config["sheets"], list)
        assert len(config["sheets"]) == 2
        sheet_names = [s["name"] for s in config["sheets"]]
        assert "Tiền gửi" in sheet_names
        assert "Tien mat" in sheet_names
