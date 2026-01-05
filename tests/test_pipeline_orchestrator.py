"""Tests for pipeline orchestrator."""

from unittest.mock import MagicMock, patch


from src.pipeline.orchestrator import (
    execute_pipeline,
    get_directory_mtime,
    get_file_mtime,
    list_csv_files,
    run_command,
    should_run_transform,
    should_run_upload,
)


class TestHelperFunctions:
    """Test helper utility functions."""

    def test_get_file_mtime_existing_file(self, tmp_path):
        """Test getting mtime of existing file."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("test")

        mtime = get_file_mtime(test_file)
        assert mtime is not None
        assert isinstance(mtime, float)

    def test_get_file_mtime_nonexistent_file(self, tmp_path):
        """Test getting mtime of nonexistent file."""
        nonexistent = tmp_path / "nonexistent.txt"
        assert get_file_mtime(nonexistent) is None

    def test_get_directory_mtime_with_files(self, tmp_path):
        """Test getting mtime of directory with files."""
        (tmp_path / "file1.csv").write_text("a")
        (tmp_path / "file2.csv").write_text("b")

        mtime = get_directory_mtime(tmp_path)
        assert mtime is not None
        assert isinstance(mtime, float)

    def test_get_directory_mtime_empty_directory(self, tmp_path):
        """Test getting mtime of empty directory."""
        mtime = get_directory_mtime(tmp_path)
        assert mtime is None

    def test_get_directory_mtime_nonexistent(self, tmp_path):
        """Test getting mtime of nonexistent directory."""
        nonexistent = tmp_path / "nonexistent"
        assert get_directory_mtime(nonexistent) is None

    def test_list_csv_files(self, tmp_path):
        """Test listing CSV files."""
        (tmp_path / "file1.csv").write_text("a")
        (tmp_path / "file2.csv").write_text("b")
        (tmp_path / "file3.txt").write_text("c")

        files = list_csv_files(tmp_path)
        assert len(files) == 2
        assert all(f.suffix == ".csv" for f in files)

    def test_list_csv_files_empty_directory(self, tmp_path):
        """Test listing CSV files in empty directory."""
        files = list_csv_files(tmp_path)
        assert files == []

    def test_list_csv_files_nonexistent_directory(self, tmp_path):
        """Test listing CSV files in nonexistent directory."""
        nonexistent = tmp_path / "nonexistent"
        files = list_csv_files(nonexistent)
        assert files == []

    def test_run_command_success(self):
        """Test running successful command."""
        returncode, stdout, stderr = run_command(["echo", "hello"])
        assert returncode == 0
        assert "hello" in stdout

    def test_run_command_failure(self):
        """Test running failing command."""
        returncode, stdout, stderr = run_command(["false"])
        assert returncode != 0

    def test_run_command_with_cwd(self, tmp_path):
        """Test running command in specific directory."""
        (tmp_path / "test.txt").write_text("test")
        returncode, stdout, stderr = run_command(["ls", "test.txt"], cwd=tmp_path)
        assert returncode == 0


class TestOrchestrationLogic:
    """Test orchestration decision logic."""

    def test_should_run_transform_missing_directory(self, tmp_path):
        """Test should_run_transform when directory missing."""
        with patch(
            "src.pipeline.orchestrator.DATA_STAGING_DIR", tmp_path / "nonexistent"
        ):
            assert should_run_transform() is True

    def test_should_run_transform_missing_staging_files(self, tmp_path):
        """Test should_run_transform when staging files missing but raw exist."""
        raw_dir = tmp_path / "raw"
        staging_dir = tmp_path / "staging"
        raw_dir.mkdir()
        staging_dir.mkdir()
        (raw_dir / "test.csv").write_text("data")

        with (
            patch("src.pipeline.orchestrator.DATA_RAW_DIR", raw_dir),
            patch("src.pipeline.orchestrator.DATA_STAGING_DIR", staging_dir),
        ):
            assert should_run_transform() is True

    def test_should_run_transform_up_to_date(self, tmp_path):
        """Test should_run_transform when staging is up-to-date."""
        import time

        raw_dir = tmp_path / "raw"
        staging_dir = tmp_path / "staging"
        raw_dir.mkdir()
        staging_dir.mkdir()

        # Create raw file first
        (raw_dir / "test.csv").write_text("data")
        time.sleep(0.1)

        # Create staging file after
        (staging_dir / "test.csv").write_text("data")

        with (
            patch("src.pipeline.orchestrator.DATA_RAW_DIR", raw_dir),
            patch("src.pipeline.orchestrator.DATA_STAGING_DIR", staging_dir),
        ):
            assert should_run_transform() is False

    def test_should_run_transform_raw_modified_later(self, tmp_path):
        """Test should_run_transform when raw modified after staging."""
        import time

        raw_dir = tmp_path / "raw"
        staging_dir = tmp_path / "staging"
        raw_dir.mkdir()
        staging_dir.mkdir()

        # Create staging file first
        (staging_dir / "test.csv").write_text("data")
        time.sleep(0.1)

        # Create raw file after (newer)
        (raw_dir / "test.csv").write_text("data")

        with (
            patch("src.pipeline.orchestrator.DATA_RAW_DIR", raw_dir),
            patch("src.pipeline.orchestrator.DATA_STAGING_DIR", staging_dir),
        ):
            assert should_run_transform() is True

    def test_should_run_upload_failed_transform(self):
        """Test should_run_upload with failed transform."""
        assert should_run_upload(False) is False

    def test_should_run_upload_no_files(self, tmp_path):
        """Test should_run_upload with no staging files."""
        staging_dir = tmp_path / "staging"
        staging_dir.mkdir()

        with (
            patch("src.pipeline.orchestrator.DATA_STAGING_DIR", staging_dir),
            patch("src.pipeline.orchestrator.DATA_FINAL_DIR", tmp_path / "final"),
        ):
            assert should_run_upload(True) is False

    def test_should_run_upload_success(self, tmp_path):
        """Test should_run_upload with staging files."""
        staging_dir = tmp_path / "staging"
        staging_dir.mkdir()
        (staging_dir / "test.csv").write_text("data")

        with patch("src.pipeline.orchestrator.DATA_STAGING_DIR", staging_dir):
            assert should_run_upload(True) is True


class TestMockFunctions:
    """Test mocked Google Drive functions."""

    @patch("src.pipeline.orchestrator.Credentials.from_authorized_user_file")
    def test_authenticate_google_drive_from_token(self, mock_creds):
        """Test authentication from existing token."""
        from src.pipeline.orchestrator import authenticate_google_drive

        mock_creds_obj = MagicMock()
        mock_creds_obj.valid = True
        mock_creds.return_value = mock_creds_obj

        with patch("os.path.exists", return_value=True):
            result = authenticate_google_drive()
            assert result is not None

    @patch("src.pipeline.orchestrator.build")
    def test_find_file_in_drive(self, mock_build):
        """Test finding file in Google Drive."""
        from src.pipeline.orchestrator import find_file_in_drive

        mock_service = MagicMock()
        mock_service.files().list().execute.return_value = {
            "files": [{"id": "test_id", "name": "test.csv"}]
        }

        result = find_file_in_drive(mock_service, "test.csv", "folder_id")
        assert result == "test_id"

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
