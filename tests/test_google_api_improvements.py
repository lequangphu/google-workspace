"""Tests for google_api.py improvements (manifest caching wrapper, type hints)."""

from datetime import datetime, timezone
from unittest.mock import Mock, patch

from src.modules.google_api import (
    FolderEntry,
    Manifest,
    SheetMetadata,
    get_sheets_for_folder,
    load_manifest,
    save_manifest,
    update_manifest_for_folder,
)


class TestManifestTypeHints:
    """Test TypedDict definitions for Manifest, FolderEntry, SheetMetadata."""

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

    def test_folder_entry_structure(self):
        """Test FolderEntry has required fields."""
        entry: FolderEntry = {
            "scanned_at": datetime.now(timezone.utc).isoformat(),
            "sheets": [
                {
                    "id": "sheet_1",
                    "name": "Tab1",
                    "modifiedTime": "2025-01-15T10:30:00Z",
                }
            ],
        }
        assert "scanned_at" in entry
        assert isinstance(entry["sheets"], list)

    def test_manifest_structure(self):
        """Test Manifest has required fields."""
        manifest: Manifest = {
            "version": 1,
            "folders": {
                "folder_123": {
                    "scanned_at": datetime.now(timezone.utc).isoformat(),
                    "sheets": [],
                }
            },
        }
        assert manifest["version"] == 1
        assert isinstance(manifest["folders"], dict)


class TestGetSheetsForFolderWrapper:
    """Test new get_sheets_for_folder() wrapper function."""

    @patch("src.modules.google_api.find_sheets_in_folder")
    def test_cache_hit_returns_cached_sheets(self, mock_find_sheets):
        """When cache is fresh, return cached sheets and report 1 API call saved."""
        # Setup manifest with fresh cache
        now_iso = datetime.now(timezone.utc).isoformat()
        manifest: Manifest = {
            "version": 1,
            "folders": {
                "folder_123": {
                    "scanned_at": now_iso,
                    "sheets": [
                        {
                            "id": "sheet_1",
                            "name": "CT.NHAP",
                            "modifiedTime": "2025-01-15T10:30:00Z",
                        }
                    ],
                }
            },
        }

        drive_service = Mock()
        sheets, calls_saved = get_sheets_for_folder(
            manifest, drive_service, "folder_123"
        )

        # Verify cached sheets returned
        assert len(sheets) == 1
        assert sheets[0]["name"] == "CT.NHAP"
        assert calls_saved == 1  # Cache hit saved 1 API call

        # Verify find_sheets_in_folder was NOT called
        mock_find_sheets.assert_not_called()

    @patch("src.modules.google_api.find_sheets_in_folder")
    def test_cache_miss_fetches_from_drive(self, mock_find_sheets):
        """When cache is stale or missing, fetch from Drive and update manifest."""
        mock_find_sheets.return_value = [
            {
                "id": "sheet_1",
                "name": "CT.XUAT",
                "modifiedTime": "2025-01-15T10:30:00Z",
            }
        ]

        # Empty manifest (no cache)
        manifest: Manifest = {"version": 1, "folders": {}}

        drive_service = Mock()
        sheets, calls_saved = get_sheets_for_folder(
            manifest, drive_service, "folder_456"
        )

        # Verify Drive API was called
        mock_find_sheets.assert_called_once_with(drive_service, "folder_456")

        # Verify sheets returned
        assert len(sheets) == 1
        assert sheets[0]["name"] == "CT.XUAT"
        assert calls_saved == 0  # API call made, no save

        # Verify manifest was updated
        assert "folder_456" in manifest["folders"]
        assert len(manifest["folders"]["folder_456"]["sheets"]) == 1

    @patch("src.modules.google_api.find_sheets_in_folder")
    def test_cache_stale_refetches_from_drive(self, mock_find_sheets):
        """When cache is older than TTL, re-fetch from Drive."""
        # Create stale timestamp (25 hours ago)
        from datetime import timedelta

        stale_time = datetime.now(timezone.utc) - timedelta(hours=25)
        manifest: Manifest = {
            "version": 1,
            "folders": {
                "folder_789": {
                    "scanned_at": stale_time.isoformat(),
                    "sheets": [
                        {
                            "id": "old_sheet",
                            "name": "OldTab",
                            "modifiedTime": "2025-01-10T10:00:00Z",
                        }
                    ],
                }
            },
        }

        mock_find_sheets.return_value = [
            {
                "id": "new_sheet",
                "name": "NewTab",
                "modifiedTime": "2025-01-16T10:00:00Z",
            }
        ]

        drive_service = Mock()
        sheets, calls_saved = get_sheets_for_folder(
            manifest, drive_service, "folder_789"
        )

        # Verify Drive API was called
        mock_find_sheets.assert_called_once()

        # Verify new sheets returned
        assert len(sheets) == 1
        assert sheets[0]["name"] == "NewTab"
        assert calls_saved == 0

        # Verify manifest was updated with new time
        new_time = manifest["folders"]["folder_789"]["scanned_at"]
        old_time = stale_time.isoformat()
        assert new_time > old_time

    @patch("src.modules.google_api.find_sheets_in_folder")
    def test_api_call_tracking_with_multiple_folders(self, mock_find_sheets):
        """Test API call savings accumulate across multiple folders."""
        now_iso = datetime.now(timezone.utc).isoformat()
        manifest: Manifest = {
            "version": 1,
            "folders": {
                "folder_1": {
                    "scanned_at": now_iso,
                    "sheets": [
                        {
                            "id": "s1",
                            "name": "Tab1",
                            "modifiedTime": "2025-01-15T10:30:00Z",
                        }
                    ],
                }
            },
        }

        drive_service = Mock()
        total_saved = 0

        # First call: cache hit
        _, calls_saved = get_sheets_for_folder(manifest, drive_service, "folder_1")
        total_saved += calls_saved
        assert total_saved == 1

        # Second call: cache miss
        mock_find_sheets.return_value = [
            {
                "id": "s2",
                "name": "Tab2",
                "modifiedTime": "2025-01-15T10:30:00Z",
            }
        ]
        _, calls_saved = get_sheets_for_folder(manifest, drive_service, "folder_2")
        total_saved += calls_saved
        assert total_saved == 1

        # Verify correct call count
        mock_find_sheets.assert_called_once()  # Only folder_2, not folder_1


class TestManifestPersistence:
    """Test loading/saving manifest with TypedDict."""

    def test_load_manifest_creates_default(self, tmp_path):
        """load_manifest returns empty manifest if file doesn't exist."""
        with patch("src.modules.google_api.MANIFEST_PATH", tmp_path / "missing.json"):
            manifest = load_manifest()
            assert manifest["version"] == 1
            assert manifest["folders"] == {}

    def test_save_and_load_manifest(self, tmp_path):
        """save_manifest and load_manifest roundtrip correctly."""
        manifest_path = tmp_path / "manifest.json"

        original_manifest: Manifest = {
            "version": 1,
            "folders": {
                "folder_1": {
                    "scanned_at": "2025-01-15T10:30:00+00:00",
                    "sheets": [
                        {
                            "id": "sheet_1",
                            "name": "Tab1",
                            "modifiedTime": "2025-01-15T10:30:00Z",
                        }
                    ],
                }
            },
        }

        with patch("src.modules.google_api.MANIFEST_PATH", manifest_path):
            save_manifest(original_manifest)
            loaded_manifest = load_manifest()

        assert loaded_manifest == original_manifest
        assert loaded_manifest["folders"]["folder_1"]["sheets"][0]["name"] == "Tab1"

    def test_update_manifest_for_folder_merges(self):
        """update_manifest_for_folder merges with existing folders."""
        manifest: Manifest = {
            "version": 1,
            "folders": {
                "folder_1": {
                    "scanned_at": "2025-01-15T10:00:00+00:00",
                    "sheets": [
                        {
                            "id": "s1",
                            "name": "Tab1",
                            "modifiedTime": "2025-01-15T10:00:00Z",
                        }
                    ],
                }
            },
        }

        new_sheets: list[SheetMetadata] = [
            {
                "id": "s2",
                "name": "Tab2",
                "modifiedTime": "2025-01-16T10:00:00Z",
            }
        ]

        update_manifest_for_folder(manifest, "folder_2", new_sheets)

        # Original folder untouched
        assert "folder_1" in manifest["folders"]
        assert len(manifest["folders"]["folder_1"]["sheets"]) == 1

        # New folder added
        assert "folder_2" in manifest["folders"]
        assert manifest["folders"]["folder_2"]["sheets"][0]["name"] == "Tab2"
