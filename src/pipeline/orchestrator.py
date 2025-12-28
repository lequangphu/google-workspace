#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data Pipeline Orchestrator

Workflow:
1. Ingest: Download data from Google Sheets to data/00-raw/
2. Transform: Process raw data to data/01-staging/ (only if ingest modified files OR staging files missing)
3. Upload: Upload transformed files to Google Drive (only if transform modified files)

Each step can be run independently with --step flag, or full pipeline with --full.
"""

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Optional, Tuple

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

# === CONFIGURATION ===

WORKSPACE_ROOT = Path(__file__).parent.parent.parent
DATA_RAW_DIR = WORKSPACE_ROOT / "data" / "00-raw"
DATA_STAGING_DIR = WORKSPACE_ROOT / "data" / "01-staging"
DATA_VALIDATED_DIR = WORKSPACE_ROOT / "data" / "02-validated"
DATA_EXPORT_DIR = WORKSPACE_ROOT / "data" / "03-erp-export"

# Legacy compatibility paths
DATA_FINAL_DIR = WORKSPACE_ROOT / "data" / "cleaned"
DATA_PRODUCT_DIR = WORKSPACE_ROOT / "data" / "reports"

# Migrated module paths
MODULE_INGEST = WORKSPACE_ROOT / "src" / "modules" / "ingest.py"

# Legacy script paths (for fallback)
SCRIPT_INGEST = WORKSPACE_ROOT / "ingest.py"
SCRIPT_CLEAN_NHAP = WORKSPACE_ROOT / "clean_chung_tu_nhap.py"
SCRIPT_CLEAN_XUAT = WORKSPACE_ROOT / "clean_chung_tu_xuat.py"
SCRIPT_CLEAN_XNT = WORKSPACE_ROOT / "clean_xuat_nhap_ton.py"
SCRIPT_GENERATE_PRODUCT_INFO = WORKSPACE_ROOT / "generate_product_info.py"

GOOGLE_DRIVE_FOLDER_ID = "1J-3aAf8Hco3iL9-oFnfoABgjiLNdjI7H"
GOOGLE_SHEETS_ID_CLEANED = "1KYz8S4WSL5vG2TIYsZKKIwNulKLvMc82iBMso_u49dk"
GOOGLE_SHEETS_ID_REPORTS = "11vk-p0iL9JcNH180n4uV5VTuPnhJ97lBgsLEfCnWx_k"
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

# === LOGGING ===

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
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
            cmd, cwd=cwd or WORKSPACE_ROOT, capture_output=True, text=True, timeout=3600
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


def find_file_in_drive(
    drive_service, filename: str, parent_folder_id: str
) -> Optional[str]:
    """Find file in Google Drive by name within a folder."""
    try:
        query = (
            f"'{parent_folder_id}' in parents and name='{filename}' "
            "and mimeType!='application/vnd.google-apps.folder' and trashed=false"
        )
        results = (
            drive_service.files()
            .list(q=query, spaces="drive", fields="files(id, name, modifiedTime)")
            .execute()
        )

        files = results.get("files", [])
        if files:
            return files[0]["id"]
        return None
    except HttpError as e:
        logger.error(f"Error searching for {filename} on Drive: {e}")
        return None


def add_csv_as_sheet_tab(
    sheets_service, csv_filepath: Path, spreadsheet_id: str, replace: bool = True
) -> bool:
    """Add CSV as a tab/sheet in an existing Google Spreadsheet."""
    try:
        import pandas as pd

        # Sheet name: remove .csv extension
        sheet_name = csv_filepath.stem

        # Get existing sheet metadata
        spreadsheet = (
            sheets_service.spreadsheets()
            .get(
                spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))"
            )
            .execute()
        )

        sheet_properties = spreadsheet.get("sheets", [])
        existing_sheet_id = None

        # Check if sheet already exists
        for sheet in sheet_properties:
            if sheet["properties"]["title"] == sheet_name:
                existing_sheet_id = sheet["properties"]["sheetId"]
                break

        # If sheet exists and we're replacing, clear it first
        if existing_sheet_id is not None and replace:
            logger.info(f"Clearing existing sheet: {sheet_name}")
            sheets_service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=f"'{sheet_name}'!A:Z",
            ).execute()
        elif existing_sheet_id is not None:
            logger.info(f"Sheet {sheet_name} already exists, skipping")
            return True
        else:
            # Create new sheet
            logger.info(f"Creating new sheet: {sheet_name}")
            request = {
                "addSheet": {
                    "properties": {
                        "title": sheet_name,
                    }
                }
            }
            response = (
                sheets_service.spreadsheets()
                .batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={"requests": [request]},
                )
                .execute()
            )
            response["replies"][0]["addSheet"]["properties"]["sheetId"]

        # Read CSV and prepare data
        df = pd.read_csv(csv_filepath)

        # Convert NaN to None for JSON serialization
        df_values = df.astype(object).where(pd.notna(df), None).values.tolist()
        values = [df.columns.tolist()] + df_values

        # Write data to sheet
        logger.info(f"Populating sheet: {sheet_name} ({len(df)} rows)")
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_name}'!A1",
            valueInputOption="RAW",
            body={"values": values},
        ).execute()

        logger.info(f"Successfully added/updated sheet: {sheet_name}")
        return True

    except Exception as e:
        logger.error(f"Error adding {csv_filepath.name} as sheet tab: {e}")
        return False


def upload_file_to_drive(
    drive_service, filepath: Path, parent_folder_id: str, replace: bool = True
) -> bool:
    """Upload file to Google Drive, optionally replacing existing file."""
    try:
        file_id = find_file_in_drive(drive_service, filepath.name, parent_folder_id)

        file_metadata = {"name": filepath.name}
        media = MediaFileUpload(str(filepath), mimetype="text/csv")

        if file_id and replace:
            # Update existing file
            logger.info(f"Replacing {filepath.name} on Drive...")
            drive_service.files().update(
                fileId=file_id, media_body=media, fields="id, name, modifiedTime"
            ).execute()
            logger.info(f"Replaced {filepath.name}")
        else:
            # Create new file
            file_metadata["parents"] = [parent_folder_id]
            logger.info(f"Uploading {filepath.name}...")
            drive_service.files().create(
                body=file_metadata, media_body=media, fields="id, name, modifiedTime"
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

    # Try migrated module first
    ingest_script = MODULE_INGEST if MODULE_INGEST.exists() else SCRIPT_INGEST

    if not ingest_script.exists():
        logger.error(f"Ingest script not found: {ingest_script}")
        return False

    returncode, stdout, stderr = run_command(["uv", "run", str(ingest_script)])

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


def step_transform() -> bool:
    """Step 2: Transform data from raw to staging."""
    logger.info("=" * 70)
    logger.info("STEP 2: TRANSFORM")
    logger.info("=" * 70)

    # Run all migrated transformation modules
    transform_modules = [
        ("import_export_receipts", "clean_receipts_purchase.py"),
        ("import_export_receipts", "clean_receipts_sale.py"),
        ("import_export_receipts", "clean_inventory.py"),
        ("import_export_receipts", "extract_products.py"),
        ("receivable", "clean_customers.py"),
        ("receivable", "extract_customer_ids.py"),
        ("receivable", "clean_debts.py"),
        ("payable", "extract_suppliers.py"),
    ]

    all_succeeded = True
    for module, script in transform_modules:
        script_path = WORKSPACE_ROOT / "src" / "modules" / module / script

        if not script_path.exists():
            logger.debug(f"Transform module not yet migrated: {script}")
            continue

        logger.info(f"Running {module}/{script}...")
        returncode, stdout, stderr = run_command(["uv", "run", str(script_path)])

        if stdout:
            logger.debug(stdout)
        if stderr and returncode != 0:
            logger.warning(stderr)

        if returncode == 0:
            logger.info(f"{module}/{script} completed")
        else:
            logger.error(f"{module}/{script} failed with return code {returncode}")
            all_succeeded = False

    # Fallback to legacy scripts if needed
    if not all_succeeded or not any(
        (WORKSPACE_ROOT / "src" / "modules" / m / s).exists()
        for m, s in transform_modules
    ):
        logger.info("Falling back to legacy cleaning scripts...")
        legacy_scripts = [
            ("CT.NHAP", SCRIPT_CLEAN_NHAP),
            ("CT.XUAT", SCRIPT_CLEAN_XUAT),
            ("XNT", SCRIPT_CLEAN_XNT),
        ]

        for script_name, script_path in legacy_scripts:
            if not script_path.exists():
                logger.warning(f"Legacy script not found: {script_path}")
                continue

            logger.info(f"Running {script_name} legacy cleaner...")
            returncode, stdout, stderr = run_command(["uv", "run", str(script_path)])

            if stdout:
                logger.info(stdout)
            if stderr and returncode != 0:
                logger.warning(stderr)

            if returncode == 0:
                logger.info(f"{script_name} cleaning completed")
            else:
                logger.error(
                    f"{script_name} cleaning failed with return code {returncode}"
                )
                all_succeeded = False

    if all_succeeded:
        logger.info("All transformation scripts completed successfully")
    return all_succeeded


def step_generate_product_info() -> bool:
    """Step 2.5: Generate product information from transformed data (legacy)."""
    logger.info("=" * 70)
    logger.info("STEP 2.5: GENERATE PRODUCT INFO (LEGACY)")
    logger.info("=" * 70)

    if not SCRIPT_GENERATE_PRODUCT_INFO.exists():
        logger.warning(
            f"Product generation script not found: {SCRIPT_GENERATE_PRODUCT_INFO}"
        )
        return False

    # Clear reports directory before generating
    if DATA_PRODUCT_DIR.exists():
        product_files = list(DATA_PRODUCT_DIR.glob("*.csv"))
        if product_files:
            logger.info(f"Clearing {len(product_files)} files from /data/reports/")
            for filepath in product_files:
                filepath.unlink()
                logger.debug(f"Deleted {filepath.name}")

    logger.info("Running product info generator...")
    returncode, stdout, stderr = run_command(
        ["uv", "run", str(SCRIPT_GENERATE_PRODUCT_INFO)]
    )

    if stdout:
        logger.info(stdout)
    if stderr and returncode != 0:
        logger.warning(stderr)

    if returncode == 0:
        logger.info("Product info generation completed")
        return True
    else:
        logger.error(f"Product info generation failed with return code {returncode}")
        return False


def step_upload() -> bool:
    """Step 3: Upload transformed and report files to respective target sheets."""
    logger.info("=" * 70)
    logger.info("STEP 3: UPLOAD")
    logger.info("=" * 70)

    try:
        creds = authenticate_google_drive()
        sheets_service = build("sheets", "v4", credentials=creds)
    except Exception as e:
        logger.error(f"Failed to authenticate with Google Sheets: {e}")
        return False

    # Check both legacy and new data directories
    cleaned_files = list_csv_files(DATA_FINAL_DIR) if DATA_FINAL_DIR.exists() else []
    staging_files = (
        list_csv_files(DATA_STAGING_DIR) if DATA_STAGING_DIR.exists() else []
    )
    report_files = list_csv_files(DATA_PRODUCT_DIR) if DATA_PRODUCT_DIR.exists() else []

    all_succeeded = True

    # Prefer staging files (new) over legacy cleaned files
    files_to_upload = staging_files or cleaned_files

    if files_to_upload:
        logger.info(
            f"Uploading {len(files_to_upload)} transformed files to cleaned sheet"
        )
        logger.info(
            f"Target spreadsheet: https://docs.google.com/spreadsheets/d/{GOOGLE_SHEETS_ID_CLEANED}"
        )
        for filepath in files_to_upload:
            if not add_csv_as_sheet_tab(
                sheets_service, filepath, GOOGLE_SHEETS_ID_CLEANED, replace=True
            ):
                all_succeeded = False

    # Upload report files to GOOGLE_SHEETS_ID_REPORTS
    if report_files:
        logger.info(f"Uploading {len(report_files)} report files to reports sheet")
        logger.info(
            f"Target spreadsheet: https://docs.google.com/spreadsheets/d/{GOOGLE_SHEETS_ID_REPORTS}"
        )
        for filepath in report_files:
            if not add_csv_as_sheet_tab(
                sheets_service, filepath, GOOGLE_SHEETS_ID_REPORTS, replace=True
            ):
                all_succeeded = False

    if not files_to_upload and not report_files:
        logger.warning("No transformed or report files found to upload")
        return True

    if all_succeeded:
        logger.info("All files uploaded successfully as sheet tabs")
    return all_succeeded


# === PIPELINE ORCHESTRATION ===


def should_run_transform() -> bool:
    """Determine if transformation step should run."""
    # Always transform if staging data directory is missing
    if not DATA_STAGING_DIR.exists():
        logger.info("Staging data directory doesn't exist, running transform")
        return True

    # Check if any staging CSV files are missing
    raw_files = list_csv_files(DATA_RAW_DIR)
    staging_files = list_csv_files(DATA_STAGING_DIR)

    if not staging_files and raw_files:
        logger.info("No staging files found but raw files exist, running transform")
        return True

    # Check if raw directory was modified more recently than staging directory
    raw_mtime = get_directory_mtime(DATA_RAW_DIR)
    staging_mtime = get_directory_mtime(DATA_STAGING_DIR)

    if raw_mtime and staging_mtime and raw_mtime > staging_mtime:
        logger.info("Raw data was modified after staging data, running transform")
        return True

    logger.info("Staging data is up-to-date, skipping transform")
    return False


def should_run_generate_product_info(transform_succeeded: bool) -> bool:
    """Determine if product info generation should run."""
    if not transform_succeeded:
        logger.info("Transform step did not succeed, skipping product info generation")
        return False

    staging_files = list_csv_files(DATA_STAGING_DIR)
    if not staging_files:
        logger.info("No staging files available for product generation")
        return False

    logger.info("Staging files ready for product info generation")
    return True


def should_run_upload(transform_succeeded: bool) -> bool:
    """Determine if upload step should run."""
    if not transform_succeeded:
        logger.info("Transform step did not succeed, skipping upload")
        return False

    staging_files = list_csv_files(DATA_STAGING_DIR)
    final_files = list_csv_files(DATA_FINAL_DIR) if DATA_FINAL_DIR.exists() else []
    files_to_upload = staging_files or final_files

    if not files_to_upload:
        logger.info("No transformed files to upload")
        return False

    logger.info("Transformed files ready for upload")
    return True


def run_full_pipeline() -> bool:
    """Run complete pipeline: ingest → transform → generate product info → upload."""
    logger.info("\n" + "=" * 70)
    logger.info("STARTING FULL PIPELINE")
    logger.info("=" * 70 + "\n")

    # Step 1: Ingest
    if not step_ingest():
        logger.error("Ingest failed, aborting pipeline")
        return False

    # Step 2: Transform (conditional)
    transform_succeeded = False
    if should_run_transform():
        transform_succeeded = step_transform()
        if not transform_succeeded:
            logger.error("Transform failed, aborting pipeline")
            return False
    else:
        logger.info("Skipping transform step")
        transform_succeeded = True

    # Step 2.5: Generate product info (conditional, legacy)
    if should_run_generate_product_info(transform_succeeded):
        if not step_generate_product_info():
            logger.error("Product info generation failed, but continuing to upload")
    else:
        logger.info("Skipping product info generation step")

    # Step 3: Upload (conditional)
    if should_run_upload(transform_succeeded):
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
        description="Data Pipeline: Ingest → Transform → Upload"
    )
    parser.add_argument(
        "--step",
        choices=["ingest", "transform", "upload"],
        help="Run a specific step only",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        default=False,
        help="Run full pipeline (default if no step specified)",
    )

    args = parser.parse_args()

    # Create directories if they don't exist
    DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
    DATA_STAGING_DIR.mkdir(parents=True, exist_ok=True)
    DATA_VALIDATED_DIR.mkdir(parents=True, exist_ok=True)
    DATA_EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    DATA_FINAL_DIR.mkdir(parents=True, exist_ok=True)
    DATA_PRODUCT_DIR.mkdir(parents=True, exist_ok=True)

    if args.step == "ingest":
        success = step_ingest()
    elif args.step == "transform":
        success = step_transform()
    elif args.step == "upload":
        success = step_upload()
    else:
        success = run_full_pipeline()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
