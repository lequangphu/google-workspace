"""Generic data cleaner for CSV files with configurable schema."""

import csv
import re
from pathlib import Path

import pandas as pd

from data_cleaning_utils import (
    clean_cell_value,
    clean_and_convert_numeric,
    combine_headers,
    combine_headers_three_level,
    handle_duplicate_column_names,
    excel_date_to_datetime,
    excel_date_from_day_month_year,
)


class DataCleaner:
    """Generic CSV data cleaner that applies configuration-based cleaning and transformations."""

    def __init__(self, config, file_path):
        """
        Initialize cleaner with configuration and file path.

        Args:
            config: Dictionary with cleaning configuration
            file_path: Path to the CSV file to clean
        """
        self.config = config
        self.file_path = Path(file_path)
        self.raw_data = None
        self.df = None
        self.year_from_filename = self._extract_year_from_filename()

    def _extract_year_from_filename(self):
        """Extract year from filename (e.g., 2023_5_CT.XUAT.csv -> 2023)."""
        match = re.search(r'(\d{4})_', self.file_path.name)
        if match:
            return int(match.group(1))
        return None

    def load_raw_csv(self):
        """Load CSV data into raw_data list."""
        self.raw_data = []
        with open(self.file_path, 'r', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            for row in csv_reader:
                self.raw_data.append(row)
        print(f"Loaded {len(self.raw_data)} rows from {self.file_path.name}")
        return self.raw_data

    def build_columns(self):
        """Build column names from header rows based on configuration."""
        header_rows_indices = self.config['header_rows']
        header_type = self.config.get('header_type', 'two_level')

        if header_type == 'two_level':
            header_row_0 = self.raw_data[header_rows_indices[0]]
            header_row_1 = self.raw_data[header_rows_indices[1]]
            max_cols = max(len(row) for row in self.raw_data)
            column_names = combine_headers(header_row_0, header_row_1, max_cols)

        elif header_type == 'three_level':
            header_row_0 = self.raw_data[header_rows_indices[0]]
            header_row_1 = self.raw_data[header_rows_indices[1]]
            header_row_2 = self.raw_data[header_rows_indices[2]]
            max_cols = max(len(row) for row in self.raw_data)
            column_names = combine_headers_three_level(
                header_row_0, header_row_1, header_row_2, max_cols
            )
        else:
            raise ValueError(f"Unknown header type: {header_type}")

        # Handle duplicates and empty names
        final_columns = handle_duplicate_column_names(column_names)
        return final_columns, max_cols

    def prepare_data_rows(self, max_cols):
        """Prepare data rows with consistent column count."""
        data_start_row = self.config['data_start_row']
        prepared_data = []

        for row in self.raw_data[data_start_row:]:
            if len(row) < max_cols:
                prepared_data.append(row + [None] * (max_cols - len(row)))
            elif len(row) > max_cols:
                prepared_data.append(row[:max_cols])
            else:
                prepared_data.append(row)

        return prepared_data

    def create_dataframe(self):
        """Create DataFrame from raw data and column names."""
        columns, max_cols = self.build_columns()
        prepared_data = self.prepare_data_rows(max_cols)
        self.df = pd.DataFrame(prepared_data, columns=columns)
        print(f"Created DataFrame with shape {self.df.shape}")
        return self.df

    def clean_cells(self):
        """Apply general cell cleaning (strip, handle NaN, remove non-breaking spaces)."""
        self.df = self.df.map(clean_cell_value)
        print("Applied cell-level cleaning")

    def convert_date_columns(self):
        """Convert date columns based on configuration."""
        date_cols = self.config.get('date_cols', {})

        for col, date_type in date_cols.items():
            if col not in self.df.columns:
                continue

            if date_type == 'excel':
                self.df[col] = self.df[col].apply(excel_date_to_datetime)
            elif date_type == 'day_month_filename_year':
                # Used for CT.XUAT: combine day column with month column and filename year
                date_components = self.config.get('date_components', {}).get(col)
                if date_components and len(date_components) == 2:
                    day_col, month_col = date_components
                    self.df[col] = self.df.apply(
                        lambda row: excel_date_from_day_month_year(
                            row[day_col], row[month_col], self.year_from_filename
                        ),
                        axis=1
                    )
            elif date_type == 'numeric':
                # Just convert to numeric if needed
                self.df[col] = self.df[col].apply(clean_and_convert_numeric)

        print("Applied date column conversions")

    def convert_numeric_columns(self):
        """Convert numeric columns."""
        numeric_cols = self.config.get('numeric_cols', [])

        for col in numeric_cols:
            if col in self.df.columns:
                self.df[col] = self.df[col].apply(clean_and_convert_numeric)

        print(f"Converted {len(numeric_cols)} numeric columns")

    def drop_empty_columns(self):
        """Drop columns that are entirely empty."""
        empty_columns = [col for col in self.df.columns if self.df[col].isnull().all()]
        if empty_columns:
            self.df = self.df.drop(columns=empty_columns)
            print(f"Dropped {len(empty_columns)} empty columns: {empty_columns}")
        else:
            print("No empty columns to drop")

    def drop_rows_with_missing_key(self):
        """Drop rows where the key column is missing."""
        key_col = self.config.get('key_col')
        if key_col and key_col in self.df.columns:
            initial_rows = len(self.df)
            self.df = self.df.dropna(subset=[key_col])
            dropped = initial_rows - len(self.df)
            print(f"Dropped {dropped} rows with missing '{key_col}'")

    def clean(self):
        """Execute full cleaning pipeline."""
        print(f"\n{'='*60}")
        print(f"Processing: {self.file_path.name}")
        print(f"{'='*60}")

        self.load_raw_csv()
        self.create_dataframe()
        self.clean_cells()
        self.convert_date_columns()
        self.convert_numeric_columns()
        self.drop_empty_columns()
        self.drop_rows_with_missing_key()

        print(f"\nFinal DataFrame shape: {self.df.shape}")
        return self.df

    def save(self, output_path):
        """Save cleaned DataFrame to CSV."""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        self.df.to_csv(output_path, index=False, encoding='utf-8')
        print(f"Saved to: {output_path}")
