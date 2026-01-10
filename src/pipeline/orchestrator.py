#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data Pipeline Orchestrator

CLI Quick Reference:
    uv run src/pipeline/orchestrator.py                    # Full pipeline, all modules
    uv run src/pipeline/orchestrator.py -m ier             # Full pipeline, import_export_receipts only
    uv run src/pipeline/orchestrator.py -m rec,pay         # Full pipeline, receivable + payable
    uv run src/pipeline/orchestrator.py -s transform       # Transform only, all modules
    uv run src/pipeline/orchestrator.py -s transform -m ier # Transform, import_export_receipts only
    uv run src/pipeline/orchestrator.py -i                 # Ingest step only
    uv run src/pipeline/orchestrator.py -t                 # Transform step only
    uv run src/pipeline/orchestrator.py -e                 # Export step only
    uv run src/pipeline/orchestrator.py -u                 # Upload step only
    uv run src/pipeline/orchestrator.py -P                 # Products workflow (ingest + export)

Short Flags:
    -s <steps>     --steps <steps>      Steps: ingest,transform,export,upload
    -m <modules>   --modules <modules>  Modules: ier,rec,pay,cash (or full names)
    -i             ingest step only
    -t             transform step only
    -e             export step only
    -u             upload step only
    -P             products-only workflow

Module Aliases:
    ier   import_export_receipts    Products, PriceBook, Inventory
    rec   receivable                Customers, Debts
    pay   payable                   Suppliers
    cash  cashflow                  Deposits, Cash transactions

Use Cases:
    1. Full pipeline on all modules
       uv run src/pipeline/orchestrator.py

    2. Full pipeline on specific modules
       uv run src/pipeline/orchestrator.py -m ier
       uv run src/pipeline/orchestrator.py -m rec,pay

    3. Part of pipeline on all modules
       uv run src/pipeline/orchestrator.py -s ingest,transform
       uv run src/pipeline/orchestrator.py -s transform

    4. Part of pipeline on specific modules
       uv run src/pipeline/orchestrator.py -s transform -m ier
       uv run src/pipeline/orchestrator.py -s transform,export -m rec

Workflow:
1. Ingest: Download data from Google Sheets to data/00-raw/
2. Transform: Process raw data to data/01-staging/ (only if ingest modified files OR staging files missing)
3. Export: Generate ERP XLSX files from data/01-staging/ to data/03-erp-export/
4. Upload: Upload transformed files to Google Drive (only if transform modified files)
"""

import argparse
import logging
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional, Tuple

from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from src.modules.google_api import get_sheet_id_by_name, API_CALL_DELAY

from src.utils import get_workspace_root


# === CONFIGURATION ===

WORKSPACE_ROOT = get_workspace_root()

DATA_RAW_DIR = WORKSPACE_ROOT / "data" / "00-raw"
DATA_STAGING_DIR = WORKSPACE_ROOT / "data" / "01-staging"
DATA_VALIDATED_DIR = WORKSPACE_ROOT / "data" / "02-validated"
DATA_EXPORT_DIR = WORKSPACE_ROOT / "data" / "03-erp-export"

# Legacy compatibility paths
DATA_FINAL_DIR = WORKSPACE_ROOT / "data" / "cleaned"
DATA_PRODUCT_DIR = WORKSPACE_ROOT / "data" / "reports"

# Migrated module paths
MODULE_INGEST = WORKSPACE_ROOT / "src" / "modules" / "ingest.py"

# Legacy script paths (fallback when migrated versions not available)
SCRIPT_INGEST = WORKSPACE_ROOT / "legacy" / "ingest.py"
SCRIPT_CLEAN_NHAP = WORKSPACE_ROOT / "legacy" / "clean_chung_tu_nhap.py"
SCRIPT_CLEAN_XUAT = WORKSPACE_ROOT / "legacy" / "clean_chung_tu_xuat.py"
SCRIPT_CLEAN_XNT = WORKSPACE_ROOT / "legacy" / "clean_xuat_nhap_ton.py"
SCRIPT_GENERATE_PRODUCT_INFO = WORKSPACE_ROOT / "legacy" / "generate_product_info.py"

# Transform module registry: maps source to transformation scripts
TRANSFORM_MODULES = {
    "import_export_receipts": [
        "clean_inventory.py",
        "clean_receipts_purchase.py",
        "generate_opening_balance_receipts.py",
        "clean_receipts_sale.py",
        "extract_products.py",
        "extract_attributes.py",
        "reconcile_inventory.py",
    ],
    "receivable": [
        "generate_customers_xlsx.py",
    ],
    "payable": [
        "generate_suppliers_xlsx.py",
    ],
}

# Module alias shortcuts for CLI
MODULE_ALIASES = {
    "ier": "import_export_receipts",
    "rec": "receivable",
    "pay": "payable",
}


def import_export_receipts_transform() -> bool:
    """Transform import_export_receipts data."""
    from src.modules.import_export_receipts.generate_products_xlsx import process

    logger.info("Running generate_products_xlsx.process()...")
    try:
        result = process(write_to_sheets=False)
        if result is None:
            logger.error("generate_products_xlsx failed")
            return False
        return True
    except Exception as e:
        logger.error(f"generate_products_xlsx raised exception: {e}")
        return False


def receivable_transform() -> bool:
    """Transform receivable data."""
    from src.modules.receivable.generate_customers_xlsx import process

    logger.info("Running generate_customers_xlsx.process()...")
    try:
        result = process(write_to_sheets=False)
        if result is None:
            logger.error("generate_customers_xlsx failed")
            return False
        return True
    except Exception as e:
        logger.error(f"generate_customers_xlsx raised exception: {e}")
        return False


def payable_transform() -> bool:
    """Transform payable data."""
    from src.modules.payable.generate_suppliers_xlsx import process

    logger.info("Running generate_suppliers_xlsx.process()...")
    try:
        result = process(write_to_sheets=False)
        if result is None:
            logger.error("generate_suppliers_xlsx failed")
            return False
        return True
    except Exception as e:
        logger.error(f"generate_suppliers_xlsx raised exception: {e}")
        return False


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
    """List all CSV files in directory and subdirectories."""
    if not dirpath.exists():
        return []
    return sorted(dirpath.rglob("*.csv"))


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

        existing_sheet_id = get_sheet_id_by_name(
            sheets_service, spreadsheet_id, sheet_name
        )

        # If sheet exists and we're replacing, clear it first
        if existing_sheet_id is not None and replace:
            logger.info(f"Clearing existing sheet: {sheet_name}")
            sheets_service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=f"'{sheet_name}'!A:Z",
            ).execute()
            time.sleep(API_CALL_DELAY)
        elif existing_sheet_id is not None:
            logger.info(f"Sheet {sheet_name} already exists, skipping")
            return True
        else:
            # Create new sheet
            logger.info(f"Creating new sheet: {sheet_name}")
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
            valueInputOption="USER_ENTERED",
            body={"values": values},
        ).execute()
        time.sleep(API_CALL_DELAY)

        logger.info(f"Successfully added/updated sheet: {sheet_name}")  # noqa: F841
        time.sleep(API_CALL_DELAY)

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


def step_ingest(modules_filter: Optional[List[str]] = None) -> bool:
    """Step 1: Ingest data from Google Sheets.

    Args:
        modules_filter: List of modules to ingest. If None, ingest all.
    """
    logger.info("=" * 70)
    logger.info("STEP 1: INGEST")
    if modules_filter:
        logger.info(f"Modules: {', '.join(modules_filter)}")
    logger.info("=" * 70)

    # Try migrated module first
    ingest_script = MODULE_INGEST if MODULE_INGEST.exists() else SCRIPT_INGEST

    if not ingest_script.exists():
        logger.error(f"Ingest script not found: {ingest_script}")
        return False

    # Build command with optional sources filter
    cmd = ["uv", "run", str(ingest_script)]
    if modules_filter:
        cmd.extend(["--only", ",".join(modules_filter)])

    returncode, stdout, stderr = run_command(cmd)

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


def step_transform(modules_filter: Optional[List[str]] = None) -> bool:
    """Step 2: Transform data from raw to staging.

    Args:
        modules_filter: List of modules to transform. If None, transform all.
    """
    logger.info("=" * 70)
    logger.info("STEP 2: TRANSFORM")
    if modules_filter:
        logger.info(f"Modules: {', '.join(modules_filter)}")
    logger.info("=" * 70)

    # Build list of (module, script) tuples from registry
    transform_modules = []
    for module, scripts in TRANSFORM_MODULES.items():
        if modules_filter and module not in modules_filter:
            logger.debug(f"Skipping module: {module}")
            continue
        for script in scripts:
            transform_modules.append((module, script))

    if not transform_modules:
        logger.warning("No transform modules found for given filter")
        return True

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


def step_export_erp(modules_filter: Optional[List[str]] = None) -> bool:
    """Step 2.7: Export validated data to ERP XLSX files.

    Args:
        modules_filter: List of modules to export. If None, export all available.
    """
    logger.info("=" * 70)
    logger.info("STEP 2.7: EXPORT ERP")
    if modules_filter:
        logger.info(f"Modules: {', '.join(modules_filter)}")
    logger.info("=" * 70)

    all_succeeded = True

    # Export import_export_receipts (Products.xlsx)
    if not modules_filter or "import_export_receipts" in modules_filter:
        try:
            from src.modules.import_export_receipts.generate_products_xlsx import (
                process as generate_products,
            )

            staging_dir = DATA_STAGING_DIR / "import_export"
            if not staging_dir.exists():
                logger.warning(f"Staging directory not found: {staging_dir}")
            else:
                logger.info("Generating Products.xlsx from staging data...")
                output_path = generate_products(staging_dir=staging_dir)

                if output_path and output_path.exists():
                    logger.info(f"Successfully generated: {output_path}")
                else:
                    logger.error("Products.xlsx generation failed")
                    all_succeeded = False

        except ImportError as e:
            logger.error(f"Failed to import generate_products_xlsx: {e}")
            all_succeeded = False
        except Exception as e:
            logger.error(f"ERP export failed: {e}")
            import traceback

            logger.error(traceback.format_exc())
            all_succeeded = False

    # Export receivable (Customers.xlsx) - if module exists
    if not modules_filter or "receivable" in modules_filter:
        try:
            from src.modules.receivable.generate_customers_xlsx import (
                process as generate_customers,
            )

            staging_dir = DATA_STAGING_DIR / "receivable"
            if not staging_dir.exists():
                logger.debug(f"Receivable staging directory not found: {staging_dir}")
            else:
                logger.info("Generating Customers.xlsx from staging data...")
                output_path = generate_customers(staging_dir=staging_dir)

                if output_path and output_path.exists():
                    logger.info(f"Successfully generated: {output_path}")
                else:
                    logger.warning("Customers.xlsx generation failed")

        except ImportError:
            logger.debug("Receivable customers export not yet implemented")
        except Exception as e:
            logger.warning(f"Receivable export failed: {e}")

    if all_succeeded:
        logger.info("ERP export completed")
    return all_succeeded


def step_upload(modules_filter: Optional[List[str]] = None) -> bool:
    logger.info("=" * 70)
    logger.info("STEP 3: UPLOAD")
    logger.info("Upload disabled per ADR-10 - XLSX files in data/03-erp-export/")
    logger.info("Please upload XLSX files manually to ERP")
    logger.info("=" * 70)
    return True


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


def should_run_upload(transform_succeeded: bool) -> bool:
    """Determine if upload step should run."""
    logger.info("Upload step is disabled")
    return False


def execute_pipeline(
    steps: List[str], modules_filter: Optional[List[str]] = None
) -> bool:
    """Execute pipeline steps in order.

    Args:
        steps: List of steps to execute (e.g., ["ingest", "transform", "export"])
        modules_filter: Optional list of modules to filter by

    Returns:
        True if all steps succeeded, False otherwise
    """
    logger.info("\n" + "=" * 70)
    logger.info(
        f"PIPELINE: Steps={', '.join(steps)}",
    )
    if modules_filter:
        logger.info(f"Modules={', '.join(modules_filter)}")
    logger.info("=" * 70 + "\n")

    for step in steps:
        if step == "ingest":
            if not step_ingest(modules_filter=modules_filter):
                logger.error("Ingest failed, aborting pipeline")
                return False

        elif step == "transform":
            transform_succeeded = False
            if should_run_transform():
                transform_succeeded = step_transform(modules_filter=modules_filter)
                if not transform_succeeded:
                    logger.error("Transform failed, aborting pipeline")
                    return False
            else:
                logger.info("Skipping transform step (up-to-date)")
                transform_succeeded = True

        elif step == "export":
            if not step_export_erp(modules_filter=modules_filter):
                logger.error("ERP export failed, but continuing pipeline")

        elif step == "upload":
            if should_run_upload(transform_succeeded):
                if not step_upload(modules_filter=modules_filter):
                    logger.error("Upload failed, but data is ready")
                    return False
            else:
                logger.info("Skipping upload step (no files to upload)")

        else:
            logger.error(f"Unknown step: {step}")
            return False

    logger.info("\n" + "=" * 70)
    logger.info("PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("=" * 70 + "\n")
    return True


# === MAIN ===

# Set up logging for the orchestrator
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Data Pipeline: Ingest → Transform → Export → Upload",
        epilog="""
Examples:
  # Full pipeline on all modules
  uv run src/pipeline/orchestrator.py

  # Full pipeline on specific modules (short flags)
  uv run src/pipeline/orchestrator.py -m ier
  uv run src/pipeline/orchestrator.py -m rec,pay

  # Part of pipeline on all modules (short flags)
  uv run src/pipeline/orchestrator.py -s ingest,transform
  uv run src/pipeline/orchestrator.py -s transform

  # Part of pipeline on specific modules (combined short flags)
  uv run src/pipeline/orchestrator.py -s transform -m ier
  uv run src/pipeline/orchestrator.py -s transform,export -m rec

  # Single-step shortcuts
  uv run src/pipeline/orchestrator.py -i    # ingest only
  uv run src/pipeline/orchestrator.py -t    # transform only
  uv run src/pipeline/orchestrator.py -e    # export only
  uv run src/pipeline/orchestrator.py -u    # upload only

  # Products workflow shortcut
  uv run src/pipeline/orchestrator.py -P    # products-only

Module Aliases: ier=import_export_receipts, rec=receivable, pay=payable, cash=cashflow
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Steps arguments
    parser.add_argument(
        "-s",
        "--steps",
        type=str,
        help="Comma-separated list of steps to run (ingest,transform,export,upload). Default: all steps",
    )
    parser.add_argument(
        "--step",
        choices=["ingest", "transform", "upload", "export"],
        help="[LEGACY] Run a specific step only (use -i/-t/-e/-u for shortcuts)",
    )

    # Single-step shortcuts
    step_group = parser.add_mutually_exclusive_group()
    step_group.add_argument(
        "-i",
        dest="step",
        action="store_const",
        const="ingest",
        help="Run ingest step only",
    )
    step_group.add_argument(
        "-t",
        dest="step",
        action="store_const",
        const="transform",
        help="Run transform step only",
    )
    step_group.add_argument(
        "-e",
        dest="step",
        action="store_const",
        const="export",
        help="Run export step only",
    )
    step_group.add_argument(
        "-u",
        dest="step",
        action="store_const",
        const="upload",
        help="Run upload step only",
    )

    # Module arguments
    parser.add_argument(
        "-m",
        "--modules",
        type=str,
        help="Comma-separated list of modules to process (ier,rec,pay,cash or full names). "
        "Default: all modules",
    )
    parser.add_argument(
        "-P",
        "--products-only",
        action="store_true",
        default=False,
        help="Only run ingest & export steps for Products.xlsx (skip transform & upload)",
    )
    parser.add_argument(
        "--resources",
        type=str,
        help="[DEPRECATED] Use --modules instead",
    )

    args = parser.parse_args()

    # Create directories if they don't exist
    DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
    DATA_STAGING_DIR.mkdir(parents=True, exist_ok=True)
    DATA_VALIDATED_DIR.mkdir(parents=True, exist_ok=True)
    DATA_EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    DATA_FINAL_DIR.mkdir(parents=True, exist_ok=True)
    DATA_PRODUCT_DIR.mkdir(parents=True, exist_ok=True)

    # Parse modules filter
    modules_filter = None
    if args.modules:
        raw_modules = [m.strip() for m in args.modules.split(",")]
        # Resolve aliases
        modules_filter = []
        invalid_modules = []
        for m in raw_modules:
            if m in TRANSFORM_MODULES:
                modules_filter.append(m)
            elif m in MODULE_ALIASES:
                modules_filter.append(MODULE_ALIASES[m])
            else:
                invalid_modules.append(m)

        if invalid_modules:
            logger.error(f"Invalid modules: {invalid_modules}")
            logger.error(
                f"Valid modules: {', '.join(list(TRANSFORM_MODULES.keys()) + list(MODULE_ALIASES.keys()))}"
            )
            sys.exit(1)
    elif args.resources:
        # Deprecated --resources flag (map to modules for backward compat)
        logger.warning("--resources is deprecated, use --modules instead")
        modules_filter = [r.strip() for r in args.resources.split(",")]

    # Parse steps to run
    if args.products_only:
        # Legacy shortcut
        logger.info("\n" + "=" * 70)
        logger.info("PRODUCTS-ONLY MODE: Ingest → Export Products.xlsx")
        logger.info("=" * 70 + "\n")
        success = step_ingest(
            modules_filter=["import_export_receipts"]
        ) and step_export_erp(modules_filter=["import_export_receipts"])
    elif args.step:
        # Legacy single step
        logger.info(f"Running single step: {args.step}")
        modules_for_step = modules_filter
        if args.step == "ingest":
            success = step_ingest(modules_filter=modules_for_step)
        elif args.step == "transform":
            success = step_transform(modules_filter=modules_for_step)
        elif args.step == "upload":
            success = step_upload(modules_filter=modules_for_step)
        elif args.step == "export":
            success = step_export_erp(modules_filter=modules_for_step)
        else:
            success = False
    elif args.steps:
        # New multi-step execution
        steps = [s.strip() for s in args.steps.split(",")]
        # Validate step names
        valid_steps = ["ingest", "transform", "export", "upload"]
        invalid_steps = [s for s in steps if s not in valid_steps]
        if invalid_steps:
            logger.error(f"Invalid steps: {invalid_steps}")
            logger.error(f"Valid steps: {', '.join(valid_steps)}")
            sys.exit(1)
        success = execute_pipeline(steps=steps, modules_filter=modules_filter)
    else:
        # Default: full pipeline
        success = execute_pipeline(
            steps=["ingest", "transform", "export", "upload"],
            modules_filter=modules_filter,
        )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
