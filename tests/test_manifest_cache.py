"""Tests for Drive manifest caching system."""

import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch


from src.modules.google_api import (
    MANIFEST_CACHE_TTL_HOURS,
    clear_manifest,
    get_cached_sheets_for_folder,
    is_manifest_stale,
    load_manifest,
    save_manifest,
    update_manifest_for_folder,
)


class TestManifestCache:
    """Test manifest caching functions."""

    def test_load_manifest_nonexistent(self):
        """Loading non-existent manifest returns empty structure."""
        with patch("src.modules.google_api.MANIFEST_PATH") as mock_path:
            mock_path.exists.return_value = False
            result = load_manifest()
            assert result == {"version": 1, "folders": {}}

    def test_load_manifest_existing(self):
        """Loading existing manifest returns correct data."""
        test_data = {
            "version": 1,
            "folders": {
                "folder_1": {
                    "scanned_at": datetime.now(timezone.utc).isoformat(),
                    "sheets": [{"id": "sheet_1", "name": "Test"}],
                }
            },
        }
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
            json.dump(test_data, f)
            temp_path = Path(f.name)

        try:
            with patch("src.modules.google_api.MANIFEST_PATH", temp_path):
                result = load_manifest()
                assert result == test_data
                assert "folder_1" in result["folders"]
        finally:
            temp_path.unlink()

    def test_save_manifest(self):
        """Saving manifest creates file with correct structure."""
        test_data = {"version": 1, "folders": {"folder_1": {"sheets": []}}}
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_path = Path(tmpdir) / "manifest.json"
            with patch("src.modules.google_api.MANIFEST_PATH", temp_path):
                save_manifest(test_data)
                assert temp_path.exists()
                with open(temp_path) as f:
                    saved_data = json.load(f)
                assert saved_data == test_data

    def test_is_manifest_stale_fresh(self):
        """Fresh manifest (within TTL) is not stale."""
        now = datetime.now(timezone.utc)
        recent_ts = (now - timedelta(hours=1)).isoformat()
        assert not is_manifest_stale(recent_ts)

    def test_is_manifest_stale_old(self):
        """Old manifest (past TTL) is stale."""
        now = datetime.now(timezone.utc)
        old_ts = (now - timedelta(hours=MANIFEST_CACHE_TTL_HOURS + 1)).isoformat()
        assert is_manifest_stale(old_ts)

    def test_is_manifest_stale_invalid(self):
        """Invalid timestamp is treated as stale."""
        assert is_manifest_stale("invalid_iso_date")

    def test_update_manifest_for_folder(self):
        """Update manifest adds/updates folder entry with timestamp."""
        manifest = {"version": 1, "folders": {}}
        sheets = [
            {"id": "sheet_1", "name": "Test1"},
            {"id": "sheet_2", "name": "Test2"},
        ]

        before = datetime.now(timezone.utc)
        update_manifest_for_folder(manifest, "folder_1", sheets)
        after = datetime.now(timezone.utc)

        assert "folder_1" in manifest["folders"]
        entry = manifest["folders"]["folder_1"]
        assert entry["sheets"] == sheets
        assert "scanned_at" in entry

        scanned_at = datetime.fromisoformat(entry["scanned_at"])
        assert before <= scanned_at <= after

    def test_get_cached_sheets_not_cached(self):
        """Getting sheets for non-cached folder returns None, False."""
        manifest = {"version": 1, "folders": {}}
        sheets, is_fresh = get_cached_sheets_for_folder(manifest, "missing_folder")
        assert sheets is None
        assert not is_fresh

    def test_get_cached_sheets_cached_fresh(self):
        """Getting fresh cached sheets returns data, True."""
        now = datetime.now(timezone.utc)
        manifest = {
            "version": 1,
            "folders": {
                "folder_1": {
                    "scanned_at": now.isoformat(),
                    "sheets": [{"id": "sheet_1", "name": "Test"}],
                }
            },
        }
        sheets, is_fresh = get_cached_sheets_for_folder(manifest, "folder_1")
        assert sheets == [{"id": "sheet_1", "name": "Test"}]
        assert is_fresh

    def test_get_cached_sheets_cached_stale(self):
        """Getting stale cached sheets returns None, False."""
        old_time = datetime.now(timezone.utc) - timedelta(
            hours=MANIFEST_CACHE_TTL_HOURS + 1
        )
        manifest = {
            "version": 1,
            "folders": {
                "folder_1": {
                    "scanned_at": old_time.isoformat(),
                    "sheets": [{"id": "sheet_1", "name": "Test"}],
                }
            },
        }
        sheets, is_fresh = get_cached_sheets_for_folder(manifest, "folder_1")
        assert sheets is None
        assert not is_fresh

    def test_clear_manifest(self):
        """Clear manifest removes cache file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_path = Path(tmpdir) / "manifest.json"
            temp_path.write_text('{"version": 1, "folders": {}}')
            assert temp_path.exists()

            with patch("src.modules.google_api.MANIFEST_PATH", temp_path):
                clear_manifest()
                assert not temp_path.exists()
