# Manifest Caching System

## Overview

The manifest caching system dramatically speeds up ingestion by avoiding redundant Google Drive API calls. Instead of scanning all 7 shared folders + year folders every run, it caches folder→sheets metadata locally and only refreshes stale entries.

## How It Works

### Cache File

- **Location**: `data/.drive_manifest.json`
- **Structure**: JSON with timestamp-keyed folder entries
- **Example**:
  ```json
  {
    "version": 1,
    "folders": {
      "1RbkY2dd1IaqSHhnivjh6iejKv9mpkSJ_": {
        "scanned_at": "2025-12-28T14:30:00+00:00",
        "sheets": [
          {"id": "sheet_1", "name": "XUẤT NHẬP TỒN TỔNG T01.25", "modifiedTime": "..."}
        ]
      }
    }
  }
  ```

### TTL (Time-To-Live)

- **Default**: 24 hours (`MANIFEST_CACHE_TTL_HOURS = 24`)
- Folders scanned more recently than 24 hours are served from cache
- Older folders trigger a fresh API scan
- Modify in `src/modules/google_api.py` if needed

### API Call Reduction

**Before caching**: 
- Each run scans 7 shared folders + N year folders = 7+ API calls per folder

**After caching** (on repeated runs):
- 0 folder scan calls (all served from cache)
- Only tab + data export calls (same as before)
- **~90% reduction in API calls** on typical workflows

### Manifest Functions

In `src/modules/google_api.py`:

| Function | Purpose |
|----------|---------|
| `load_manifest()` | Load cache from `data/.drive_manifest.json` |
| `save_manifest(manifest)` | Save cache back to disk |
| `get_cached_sheets_for_folder(manifest, folder_id)` | Retrieve sheets if fresh, else None |
| `update_manifest_for_folder(manifest, folder_id, sheets)` | Store newly scanned sheets with timestamp |
| `is_manifest_stale(iso_timestamp)` | Check if scan is past TTL |
| `clear_manifest()` | Force-delete cache (admin use) |

## Usage

### Normal Ingestion (automatic caching)

```bash
uv run src/modules/ingest.py
```

Logs will show cache efficiency:
```
Cache efficiency: Saved 5 API calls using manifest
```

### Force Full Refresh

If you suspect stale data or want to re-scan all folders:

```bash
uv run src/modules/ingest.py --clear-cache
```

This deletes the manifest cache, forcing a fresh scan on the next run.

### Programmatic Usage

```python
from src.modules.ingest import ingest_from_drive

# Normal run with automatic caching
files = ingest_from_drive()

# Force clear cache before ingestion
from src.modules.google_api import clear_manifest
clear_manifest()
files = ingest_from_drive()  # Will do full scan
```

## Implementation Details

### Integration with `ingest_from_drive()`

1. Load manifest at start
2. For each folder, check `get_cached_sheets_for_folder()`
3. If cached and fresh → use it (save 1 API call)
4. If stale/missing → query Drive API, update manifest
5. Save updated manifest before exit

### Thread Safety

- Manifest is single-file JSON (not thread-safe for concurrent writes)
- Pipeline runs sequentially, so no concurrency issues
- If running parallel instances, last write wins (acceptable for metadata)

## Tuning

**For faster iteration (dev mode)**: Reduce TTL
```python
# In google_api.py
MANIFEST_CACHE_TTL_HOURS = 1  # Refresh hourly instead of daily
```

**For stable production**: Keep 24h TTL (default)

**For offline testing**: Pre-populate manifest with known folder data, then operate without network

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Manifest seems stale | Run `uv run src/modules/ingest.py --clear-cache` |
| "Failed to write manifest" warning | Check `data/` directory permissions |
| Cache not being used (no "Cache efficiency" log) | Either folders are stale (expected after 24h) or manifest didn't load |
| Manifest file is corrupted | Delete `data/.drive_manifest.json` and re-run |

## Future Enhancements

- [ ] Validate sheet IDs against Drive to catch deleted files
- [ ] Add manifest inspection CLI: `--show-manifest`, `--manifest-stats`
- [ ] Async manifest updates (background refresh while ingesting)
- [ ] Per-folder TTL overrides (some folders might need 6h refresh)
