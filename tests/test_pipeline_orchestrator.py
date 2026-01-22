"""Tests for pipeline orchestrator."""

from unittest.mock import MagicMock, patch


from src.pipeline.orchestrator import (
    execute_pipeline,
    should_run_transform,
    should_run_upload,
    find_file_in_drive,
)


class TestOrchestrationLogic:
    """Test orchestration decision logic."""

    def test_should_run_transform_always_true(self):
        """Test should_run_transform always returns True."""
        assert should_run_transform() is True

    def test_should_run_upload_always_false(self):
        """Test should_run_upload always returns False."""
        assert should_run_upload() is False


class TestMockFunctions:
    """Test mocked Google Drive functions."""

    def test_find_file_in_drive_not_found(self):
        """Test finding file that doesn't exist."""
        from src.pipeline.orchestrator import find_file_in_drive

        mock_service = MagicMock()
        mock_service.files().list().execute.return_value = {"files": []}

        result = find_file_in_drive(mock_service, "test.csv", "folder_id")
        assert result is None

    def test_find_file_in_drive_error(self):
        """Test finding file with error."""
        from src.pipeline.orchestrator import find_file_in_drive
        from googleapiclient.errors import HttpError

        mock_service = MagicMock()
        mock_service.files().list.side_effect = HttpError(MagicMock(), b"error")

        with patch("src.pipeline.orchestrator.logger"):
            result = find_file_in_drive(mock_service, "test.csv", "folder_id")
            assert result is None


class TestMainEntry:
    """Test main entry point."""

    @patch("src.pipeline.orchestrator.execute_pipeline")
    @patch("sys.exit")
    def test_main_default_full_pipeline(self, mock_exit, mock_execute):
        """Test main runs full pipeline by default."""
        from src.pipeline.orchestrator import main

        mock_execute.return_value = True

        with patch("sys.argv", ["orchestrator.py"]):
            main()
        mock_execute.assert_called_once()
        mock_exit.assert_called_once_with(0)

    @patch("src.pipeline.orchestrator.step_ingest")
    @patch("sys.exit")
    def test_main_ingest_step(self, mock_exit, mock_ingest):
        """Test main runs ingest step."""
        from src.pipeline.orchestrator import main

        mock_ingest.return_value = True

        with patch("sys.argv", ["orchestrator.py", "--step", "ingest"]):
            main()
        mock_ingest.assert_called_once()
        mock_exit.assert_called_once_with(0)

    @patch("src.pipeline.orchestrator.step_transform")
    @patch("sys.exit")
    def test_main_transform_step(self, mock_exit, mock_transform):
        """Test main runs transform step."""
        from src.pipeline.orchestrator import main

        mock_transform.return_value = True

        with patch("sys.argv", ["orchestrator.py", "--step", "transform"]):
            main()
        mock_transform.assert_called_once()
        mock_exit.assert_called_once_with(0)
