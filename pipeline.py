#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data Pipeline Orchestrator

Workflow:
1. Ingest: Download data from Google Sheets to /data/raw/
2. Clean: Process raw data to /data/final/ (only if ingest modified files OR final files missing)
3. Upload: Upload final files to Google Drive (only if clean modified files)

Each step can be run independently with --step flag, or full pipeline with --full.
"""

import argparse
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

# === CONFIGURATION ===

WORKSPACE_ROOT = Path(__file__).parent
DATA_RAW_DIR = WORKSPACE_ROOT / "data" / "raw"
DATA_FINAL_DIR = WORKSPACE_ROOT / "data" / "final"

SCRIPT_INGEST = WORKSPACE_ROOT / "ingest.py"
SCRIPT_CLEAN_NHAP = WORKSPACE_ROOT / "clean_chung_tu_nhap.py"
SCRIPT_CLEAN_XUAT = WORKSPACE_ROOT / "clean_chung_tu_xuat.py"
SCRIPT_CLEAN_XNT = WORKSPACE_ROOT / "clean_xuat_nhap_ton.py"

GOOGLE_DRIVE_FOLDER_ID = "1crSQgdCZdrI5EwE1zf7soYX4xgjTM6G5"
SCOPES = [
    "https://www.googleapis.com/auth/drive",
]

# State file to track modifications
STATE_FILE = WORKSPACE_ROOT / ".pipeline_state.json"

# === LOGGING ===

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


# === HELPER FUNCTIONS ===

def get_file_mtime(filepath: Path) -> Optional[float]:
    """Get file modification time (UTC timestamp)."""
    if filepath.exists():
        return filepath.stat().st_mtime
    return None


def get_directory_mtime(dirpath: Path) -> Optional[float]:
    """Get most recent modification time in directory (recursive)."""
    if not dirpath.exists():
        return None
    
    max_mtime = None
    for file in dirpath.rglob("*"):
        if file.is_file():
            mtime = file.stat().st_mtime
            if max_mtime is None or mtime > max_mtime:
                max_mtime = mtime
    
    return max_mtime


def files_modified_since(dirpath: Path, since_time: Optional[float]) -> bool:
    """Check if any files in directory were modified after since_time."""
    if not dirpath.exists():
        return True  # Directory doesn't exist, consider as "modified"
    
    if since_time is None:
        return True  # No baseline, consider as modified
    
    for file in dirpath.rglob("*"):
        if file.is_file() and file.stat().st_mtime > since_time:
            return True
    
    return False


def list_csv_files(dirpath: Path) -> List[Path]:
    """List all CSV files in directory."""
    if not dirpath.exists():
        return []
    return sorted(dirpath.glob("*.csv"))


def run_command(cmd: List[str], cwd: Optional[Path] = None) -> Tuple[int, str, str]:
    """Run command and capture output."""
    try:
        result = subprocess.run(
            cmd,
            cwd=cwd or WORKSPACE_ROOT,
            capture_output=True,
            text=True,
            timeout=3600
        )
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return 1, "", "Command timed out after 1 hour"
    except Exception as e:
        return 1, "", str(e)


# === GOOGLE DRIVE FUNCTIONS ===

def authenticate_google_drive():
    """Authenticate with Google Drive API."""
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as token:
            token.write(creds.to_json())
    
    return creds


def find_file_in_drive(drive_service, filename: str, parent_folder_id: str) -> Optional[str]:
    """Find file in Google Drive by name within a folder."""
    try:
        query = (
            f"'{parent_folder_id}' in parents and name='{filename}' "
            "and mimeType!='application/vnd.google-apps.folder' and trashed=false"
        )
        results = drive_service.files().list(
            q=query,
            spaces="drive",
            fields="files(id, name, modifiedTime)"
        ).execute()
        
        files = results.get("files", [])
        if files:
            return files[0]["id"]
        return None
    except HttpError as e:
        logger.error(f"Error searching for {filename} on Drive: {e}")
        return None


def upload_file_to_drive(drive_service, filepath: Path, parent_folder_id: str, replace: bool = True) -> bool:
    """Upload file to Google Drive, optionally replacing existing file."""
    try:
        file_id = find_file_in_drive(drive_service, filepath.name, parent_folder_id)
        
        file_metadata = {"name": filepath.name}
        media = MediaFileUpload(str(filepath), mimetype="text/csv")
        
        if file_id and replace:
            # Update existing file
            logger.info(f"Replacing {filepath.name} on Drive...")
            drive_service.files().update(
                fileId=file_id,
                media_body=media,
                fields="id, name, modifiedTime"
            ).execute()
            logger.info(f"Replaced {filepath.name}")
        else:
            # Create new file
            file_metadata["parents"] = [parent_folder_id]
            logger.info(f"Uploading {filepath.name}...")
            drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields="id, name, modifiedTime"
            ).execute()
            logger.info(f"Uploaded {filepath.name}")
        
        return True
    except HttpError as e:
        logger.error(f"Error uploading {filepath.name}: {e}")
        return False


# === PIPELINE STEPS ===

def step_ingest() -> bool:
    """Step 1: Ingest data from Google Sheets."""
    logger.info("=" * 70)
    logger.info("STEP 1: INGEST")
    logger.info("=" * 70)
    
    if not SCRIPT_INGEST.exists():
        logger.error(f"Ingest script not found: {SCRIPT_INGEST}")
        return False
    
    returncode, stdout, stderr = run_command(["uv", "run", str(SCRIPT_INGEST)])
    
    if stdout:
        logger.info(stdout)
    if stderr:
        logger.warning(stderr)
    
    if returncode == 0:
        logger.info("Ingest completed successfully")
        return True
    else:
        logger.error(f"Ingest failed with return code {returncode}")
        return False


def step_clean() -> bool:
    """Step 2: Clean data from raw to final."""
    logger.info("=" * 70)
    logger.info("STEP 2: CLEAN")
    logger.info("=" * 70)
    
    # Clear final directory before running cleaners
    if DATA_FINAL_DIR.exists():
        final_files = list(DATA_FINAL_DIR.glob("*.csv"))
        if final_files:
            logger.info(f"Clearing {len(final_files)} files from /data/final/")
            for filepath in final_files:
                filepath.unlink()
                logger.debug(f"Deleted {filepath.name}")
    
    clean_scripts = [
        ("CT.NHAP", SCRIPT_CLEAN_NHAP),
        ("CT.XUAT", SCRIPT_CLEAN_XUAT),
        ("XNT", SCRIPT_CLEAN_XNT),
    ]
    
    all_succeeded = True
    for script_name, script_path in clean_scripts:
        if not script_path.exists():
            logger.warning(f"Clean script not found: {script_path}")
            continue
        
        logger.info(f"Running {script_name} cleaner...")
        returncode, stdout, stderr = run_command(["uv", "run", str(script_path)])
        
        if stdout:
            logger.info(stdout)
        if stderr and returncode != 0:
            logger.warning(stderr)
        
        if returncode == 0:
            logger.info(f"{script_name} cleaning completed")
        else:
            logger.error(f"{script_name} cleaning failed with return code {returncode}")
            all_succeeded = False
    
    if all_succeeded:
        logger.info("All cleaning scripts completed successfully")
    return all_succeeded


def step_upload() -> bool:
    """Step 3: Upload cleaned files to Google Drive."""
    logger.info("=" * 70)
    logger.info("STEP 3: UPLOAD")
    logger.info("=" * 70)
    
    try:
        creds = authenticate_google_drive()
        drive_service = build("drive", "v3", credentials=creds)
    except Exception as e:
        logger.error(f"Failed to authenticate with Google Drive: {e}")
        return False
    
    final_files = list_csv_files(DATA_FINAL_DIR)
    
    if not final_files:
        logger.warning("No final CSV files found to upload")
        return True
    
    logger.info(f"Found {len(final_files)} files to upload")
    
    all_succeeded = True
    for filepath in final_files:
        if not upload_file_to_drive(drive_service, filepath, GOOGLE_DRIVE_FOLDER_ID, replace=True):
            all_succeeded = False
    
    if all_succeeded:
        logger.info("All files uploaded successfully")
    return all_succeeded


# === PIPELINE ORCHESTRATION ===

def should_run_clean() -> bool:
    """Determine if cleaning step should run."""
    # Always clean if final data directory is missing
    if not DATA_FINAL_DIR.exists():
        logger.info("Final data directory doesn't exist, running clean")
        return True
    
    # Check if any final CSV files are missing
    raw_files = list_csv_files(DATA_RAW_DIR)
    final_files = list_csv_files(DATA_FINAL_DIR)
    
    if not final_files and raw_files:
        logger.info("No final files found but raw files exist, running clean")
        return True
    
    # Check if raw directory was modified more recently than final directory
    raw_mtime = get_directory_mtime(DATA_RAW_DIR)
    final_mtime = get_directory_mtime(DATA_FINAL_DIR)
    
    if raw_mtime and final_mtime and raw_mtime > final_mtime:
        logger.info("Raw data was modified after final data, running clean")
        return True
    
    logger.info("Final data is up-to-date, skipping clean")
    return False


def should_run_upload(clean_succeeded: bool) -> bool:
    """Determine if upload step should run."""
    if not clean_succeeded:
        logger.info("Clean step did not succeed, skipping upload")
        return False
    
    final_files = list_csv_files(DATA_FINAL_DIR)
    if not final_files:
        logger.info("No final files to upload")
        return False
    
    logger.info("Final files ready for upload")
    return True


def run_full_pipeline() -> bool:
    """Run complete pipeline: ingest → clean → upload."""
    logger.info("\n" + "=" * 70)
    logger.info("STARTING FULL PIPELINE")
    logger.info("=" * 70 + "\n")
    
    # Step 1: Ingest
    if not step_ingest():
        logger.error("Ingest failed, aborting pipeline")
        return False
    
    # Step 2: Clean (conditional)
    if should_run_clean():
        if not step_clean():
            logger.error("Clean failed, aborting pipeline")
            return False
    else:
        logger.info("Skipping clean step")
    
    # Step 3: Upload (conditional)
    if should_run_upload(clean_succeeded=True):
        if not step_upload():
            logger.error("Upload failed, but data is ready")
            return False
    else:
        logger.info("Skipping upload step")
    
    logger.info("\n" + "=" * 70)
    logger.info("PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("=" * 70 + "\n")
    return True


# === MAIN ===

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Data Pipeline: Ingest → Clean → Upload"
    )
    parser.add_argument(
        "--step",
        choices=["ingest", "clean", "upload"],
        help="Run a specific step only"
    )
    parser.add_argument(
        "--full",
        action="store_true",
        default=False,
        help="Run full pipeline (default if no step specified)"
    )
    
    args = parser.parse_args()
    
    # Create directories if they don't exist
    DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
    DATA_FINAL_DIR.mkdir(parents=True, exist_ok=True)
    
    if args.step == "ingest":
        success = step_ingest()
    elif args.step == "clean":
        success = step_clean()
    elif args.step == "upload":
        success = step_upload()
    else:
        success = run_full_pipeline()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
