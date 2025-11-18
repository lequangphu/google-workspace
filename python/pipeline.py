"""Main pipeline: ingest (raw) -> clean (interim) -> combine (final)."""

import subprocess
from pathlib import Path

from ingest import ingest_from_drive


def run_command(cmd, description):
    """Run a shell command and return success status."""
    print(f"\n{'=' * 60}")
    print(f"{description}")
    print(f"{'=' * 60}\n")

    result = subprocess.run(cmd, cwd=".")
    if result.returncode != 0:
        print(f"\n✗ {description} failed")
        return False

    print(f"\n✓ {description} completed successfully")
    return True


def clean_interim():
    """Run batch cleaning: raw -> interim."""
    return run_command(
        ["uv", "run", "python", "clean_all_data.py"],
        "Cleaning (raw -> interim)"
    )


def combine_final():
    """Run combining: interim -> final."""
    # Placeholder for future combining logic
    print("✓ Combining (interim -> final) - not yet implemented")
    return True


def check_raw_files():
    """Check if raw data directory has CSV files."""
    raw_path = Path("data/raw")
    if not raw_path.exists():
        return 0
    csv_files = list(raw_path.glob("*.csv"))
    return len(csv_files)


def pipeline(test_mode=False, clean_up=False, skip_ingest=False, skip_clean=False, skip_combine=False):
    """
    Execute the full data pipeline: ingest -> clean -> combine.

    Args:
        test_mode: Download only one file of each type
        clean_up: Clear data/raw/ before ingesting
        skip_ingest: Skip ingestion step
        skip_clean: Skip cleaning step
        skip_combine: Skip combining step

    Returns:
        True if pipeline succeeded, False otherwise
    """
    print(f"\n{'=' * 60}")
    print("Data Pipeline: raw -> interim -> final")
    print(f"{'=' * 60}\n")

    # Step 1: Ingest from Google Drive
    if not skip_ingest:
        files_ingested = ingest_from_drive(test_mode=test_mode, clean_up=clean_up)
        if files_ingested == 0:
            print("No files ingested. Exiting.")
            return False
    else:
        raw_count = check_raw_files()
        if raw_count == 0:
            print("No files in data/raw/. Run with skip_ingest=False or check data/raw/.")
            return False
        print(f"Skipping ingestion. Found {raw_count} existing CSV files in data/raw/")

    # Step 2: Clean data
    if not skip_clean:
        if not clean_interim():
            return False
    else:
        print("Skipping cleaning step")

    # Step 3: Combine data
    if not skip_combine:
        if not combine_final():
            return False
    else:
        print("Skipping combining step")

    print(f"\n{'=' * 60}")
    print("✓ Pipeline completed successfully")
    print(f"{'=' * 60}\n")

    return True


if __name__ == "__main__":
    import sys

    # Parse command-line arguments
    test_mode = "--test" in sys.argv
    clean_up = "--clean-up" in sys.argv
    skip_ingest = "--skip-ingest" in sys.argv
    skip_clean = "--skip-clean" in sys.argv
    skip_combine = "--skip-combine" in sys.argv

    success = pipeline(
        test_mode=test_mode,
        clean_up=clean_up,
        skip_ingest=skip_ingest,
        skip_clean=skip_clean,
        skip_combine=skip_combine,
    )
    sys.exit(0 if success else 1)
