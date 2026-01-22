"""Tests for google_api.py improvements (retry decorator)."""

import socket
import ssl
from unittest.mock import MagicMock, patch

from googleapiclient.errors import HttpError

from src.modules.google_api import (
    SheetMetadata,
    find_year_folders,
    find_sheets_in_folder,
    get_sheet_tabs,
    parse_file_metadata,
)


class TestSheetMetadataTypedDict:
    """Test SheetMetadata TypedDict definition."""

    def test_sheet_metadata_structure(self):
        """Test SheetMetadata has required fields."""
        sheet: SheetMetadata = {
            "id": "sheet_123",
            "name": "CT.NHAP",
            "modifiedTime": "2025-01-15T10:30:00Z",
        }
        assert sheet["id"] == "sheet_123"
        assert sheet["name"] == "CT.NHAP"
        assert sheet["modifiedTime"] == "2025-01-15T10:30:00Z"


class TestRetryApiCall:
    """Test @retry_api_call decorator."""

    def test_retry_on_429_error(self):
        """Retries on rate limit (429) error."""
        call_count = 0

        def mock_function():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise HttpError(MagicMock(status=429), b"Rate limit exceeded")
            return "success"

        from src.modules.google_api import retry_api_call

        decorated = retry_api_call(mock_function)

        with patch("src.modules.google_api.time.sleep") as mock_sleep:
            result = decorated()

        assert result == "success"
        assert call_count == 2  # Initial call + 1 retry
        mock_sleep.assert_called_once_with(1)  # Exponential backoff: 2^0 = 1s

    def test_retry_on_500_error(self):
        """Retries on server error (500)."""
        call_count = 0

        def mock_function():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise HttpError(MagicMock(status=500), b"Internal Server Error")
            return "success"

        from src.modules.google_api import retry_api_call

        decorated = retry_api_call(mock_function)

        with patch("src.modules.google_api.time.sleep") as mock_sleep:
            result = decorated()

        assert result == "success"
        assert call_count == 2
        mock_sleep.assert_called_once_with(1)

    def test_no_retry_on_401_error(self):
        """Does not retry on authentication error (401)."""

        def mock_function():
            raise HttpError(MagicMock(status=401), b"Unauthorized")

        from src.modules.google_api import retry_api_call

        decorated = retry_api_call(mock_function)

        with patch("src.modules.google_api.time.sleep") as mock_sleep:
            try:
                decorated()
                assert False, "Should have raised HttpError"
            except HttpError as e:
                assert e.status_code == 401

        mock_sleep.assert_not_called()

    def test_no_retry_on_403_error(self):
        """Does not retry on forbidden error (403)."""

        def mock_function():
            raise HttpError(MagicMock(status=403), b"Forbidden")

        from src.modules.google_api import retry_api_call

        decorated = retry_api_call(mock_function)

        with patch("src.modules.google_api.time.sleep") as mock_sleep:
            try:
                decorated()
                assert False, "Should have raised HttpError"
            except HttpError as e:
                assert e.status_code == 403

        mock_sleep.assert_not_called()

    def test_no_retry_on_400_error(self):
        """Does not retry on bad request error (400)."""

        def mock_function():
            raise HttpError(MagicMock(status=400), b"Bad Request")

        from src.modules.google_api import retry_api_call

        decorated = retry_api_call(mock_function)

        with patch("src.modules.google_api.time.sleep") as mock_sleep:
            try:
                decorated()
                assert False, "Should have raised HttpError"
            except HttpError as e:
                assert e.status_code == 400

        mock_sleep.assert_not_called()

    def test_no_retry_on_404_error(self):
        """Does not retry on not found error (404)."""

        def mock_function():
            raise HttpError(MagicMock(status=404), b"Not Found")

        from src.modules.google_api import retry_api_call

        decorated = retry_api_call(mock_function)

        with patch("src.modules.google_api.time.sleep") as mock_sleep:
            try:
                decorated()
                assert False, "Should have raised HttpError"
            except HttpError as e:
                assert e.status_code == 404

        mock_sleep.assert_not_called()

    def test_exponential_backoff(self):
        """Verifies exponential backoff: 1s, 2s for two retries."""
        call_count = 0
        sleep_times = []

        def mock_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:  # Fail first 2 times (initial + 1 retry)
                raise HttpError(MagicMock(status=503), b"Service Unavailable")
            return "success"

        from src.modules.google_api import retry_api_call

        decorated = retry_api_call(mock_function)

        def capture_sleep(seconds):
            sleep_times.append(seconds)

        with patch("src.modules.google_api.time.sleep", side_effect=capture_sleep):
            result = decorated()

        assert result == "success"
        assert call_count == 3  # Initial + 2 retries
        assert sleep_times == [1, 2]  # Exponential backoff: 2^0, 2^1

    def test_retry_on_timeout_error(self):
        """Retries on TimeoutError."""
        call_count = 0

        def mock_function():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise TimeoutError("Request timeout")
            return "success"

        from src.modules.google_api import retry_api_call

        decorated = retry_api_call(mock_function)

        with patch("src.modules.google_api.time.sleep") as mock_sleep:
            result = decorated()

        assert result == "success"
        assert call_count == 2
        mock_sleep.assert_called_once_with(1)

    def test_retry_on_socket_timeout(self):
        """Retries on socket.timeout."""
        call_count = 0

        def mock_function():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise socket.timeout("Socket timeout")
            return "success"

        from src.modules.google_api import retry_api_call

        decorated = retry_api_call(mock_function)

        with patch("src.modules.google_api.time.sleep") as mock_sleep:
            result = decorated()

        assert result == "success"
        assert call_count == 2
        mock_sleep.assert_called_once_with(1)

    def test_retry_on_ssl_error(self):
        """Retries on ssl.SSLError."""
        call_count = 0

        def mock_function():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ssl.SSLError("SSL error")
            return "success"

        from src.modules.google_api import retry_api_call

        decorated = retry_api_call(mock_function)

        with patch("src.modules.google_api.time.sleep") as mock_sleep:
            result = decorated()

        assert result == "success"
        assert call_count == 2
        mock_sleep.assert_called_once_with(1)

    def test_retry_on_connection_reset(self):
        """Retries on ConnectionResetError."""
        call_count = 0

        def mock_function():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionResetError("Connection reset")
            return "success"

        from src.modules.google_api import retry_api_call

        decorated = retry_api_call(mock_function)

        with patch("src.modules.google_api.time.sleep") as mock_sleep:
            result = decorated()

        assert result == "success"
        assert call_count == 2
        mock_sleep.assert_called_once_with(1)


class TestGoogleApiFunctions:
    """Test core Google API functions."""

    @patch("src.modules.google_api.time.sleep")
    def test_find_year_folders(self, mock_sleep):
        """Test find_year_folders function."""
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
        mock_sleep.assert_called_once_with(0.5)  # API_CALL_DELAY

    @patch("src.modules.google_api.time.sleep")
    def test_find_sheets_in_folder(self, mock_sleep):
        """Test find_sheets_in_folder function."""
        mock_service = MagicMock()
        mock_service.files().list().execute.return_value = {
            "files": [
                {
                    "id": "sheet_1",
                    "name": "XUẤT NHẬP TỒN TỔNG T01.23",
                    "modifiedTime": "2025-01-01T10:00:00.000Z",
                }
            ]
        }

        result = find_sheets_in_folder(mock_service, "folder_id")
        assert len(result) == 1
        assert result[0]["name"] == "XUẤT NHẬP TỒN TỔNG T01.23"
        mock_sleep.assert_called_once_with(0.5)

    @patch("src.modules.google_api.time.sleep")
    def test_get_sheet_tabs(self, mock_sleep):
        """Test get_sheet_tabs function."""
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
        mock_sleep.assert_called_once_with(0.5)


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
