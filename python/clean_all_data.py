"""Batch process all CSV files from raw directory using suffix-based configuration."""

import re
from pathlib import Path

from data_cleaner import DataCleaner
from cleaning_configs import CONFIGS


def get_file_type(filename):
    """
    Extract file type from filename based on suffix patterns.

    Matches patterns like:
    - 2023_5_CT.NHAP.csv -> 'CT.NHAP'
    - 2023_5_CT.XUAT.csv -> 'CT.XUAT'
    - 2023_5_XNT.csv -> 'XNT'
    """
    # Match CT.NHAP, CT.XUAT, XNT patterns
    match = re.search(r'(CT\.NHAP|CT\.XUAT|XNT)\.csv$', filename)
    if match:
        return match.group(1)
    return None


def process_raw_files(raw_dir='data/raw', interim_dir='data/interim'):
    """
    Process all CSV files in raw directory and save to interim directory.

    Args:
        raw_dir: Path to directory containing raw CSV files
        interim_dir: Path to directory to save cleaned CSV files
    """
    raw_path = Path(raw_dir)
    interim_path = Path(interim_dir)

    # Find all CSV files in raw directory
    csv_files = sorted(raw_path.glob('*.csv'))

    if not csv_files:
        print(f"No CSV files found in {raw_path}")
        return

    print(f"Found {len(csv_files)} CSV files in {raw_path}\n")

    processed_count = 0
    skipped_count = 0

    for csv_file in csv_files:
        file_type = get_file_type(csv_file.name)

        if not file_type:
            print(f"⚠ SKIPPED: {csv_file.name} (unknown file type)")
            skipped_count += 1
            continue

        if file_type not in CONFIGS:
            print(f"⚠ SKIPPED: {csv_file.name} (no config for {file_type})")
            skipped_count += 1
            continue

        try:
            config = CONFIGS[file_type]
            cleaner = DataCleaner(config, csv_file)
            df = cleaner.clean()

            # Generate output filename with same basename as input
            output_filename = csv_file.stem + '.csv'
            output_path = interim_path / output_filename

            cleaner.save(output_path)
            processed_count += 1
            print(f"✓ SUCCESS\n")

        except Exception as e:
            print(f"✗ ERROR: {csv_file.name}")
            print(f"  {type(e).__name__}: {e}\n")
            skipped_count += 1

    print(f"\n{'='*60}")
    print(f"Processing Summary")
    print(f"{'='*60}")
    print(f"Total files:     {len(csv_files)}")
    print(f"Processed:       {processed_count}")
    print(f"Skipped/Failed:  {skipped_count}")
    print(f"{'='*60}")


if __name__ == '__main__':
    process_raw_files()
