"""Shared data cleaning utilities for all dataset types."""

import pandas as pd


def clean_header_string(header_str):
    """Normalize header strings: strip, remove newlines/non-breaking spaces, convert to lowercase with underscores."""
    if not isinstance(header_str, str):
        return ''
    cleaned_str = header_str.strip().replace('\n', '').replace('\xa0', ' ')
    return cleaned_str.replace(' ', '_').lower()


def combine_headers(header_row_0, header_row_1, max_cols_expected):
    """Combine two header rows into a single set of column names with forward-fill logic."""
    combined_headers = []

    # Pad both header rows to max_cols_expected length
    header_row_0_padded = header_row_0 + [''] * (max_cols_expected - len(header_row_0))
    header_row_1_padded = header_row_1 + [''] * (max_cols_expected - len(header_row_1))

    # Forward-fill header_row_0
    current_primary = ""
    header_row_0_filled = []
    for h0 in header_row_0_padded:
        cleaned_h0 = clean_header_string(h0)
        if cleaned_h0:
            current_primary = cleaned_h0
        header_row_0_filled.append(current_primary)

    # Combine primary and secondary headers
    for h0_filled, h1 in zip(header_row_0_filled, header_row_1_padded):
        cleaned_h1 = clean_header_string(h1)
        if h0_filled and cleaned_h1:
            combined_headers.append(f"{h0_filled}_{cleaned_h1}")
        elif h0_filled:
            combined_headers.append(h0_filled)
        elif cleaned_h1:
            combined_headers.append(cleaned_h1)
        else:
            combined_headers.append('')
    return combined_headers


def combine_headers_three_level(header_row_0, header_row_1, header_row_2, max_cols_expected):
    """Combine three header rows (used for XNT files)."""
    # First pass: combine header_row_0 and header_row_1
    intermediate_combined = combine_headers(header_row_0, header_row_1, max_cols_expected)

    # Second pass: combine intermediate with header_row_2
    final_column_names = []
    header_row_2_padded = header_row_2 + [''] * (max_cols_expected - len(header_row_2))

    for i, h_inter in enumerate(intermediate_combined):
        cleaned_h2 = clean_header_string(header_row_2_padded[i])
        if cleaned_h2:
            # Special handling for XUẤT TRONG KỲ to get 'xuất_trong_kỳ_lẽ' and 'xuất_trong_kỳ_sỉ'
            if 'xuất_trong_kỳ_số_lượng' in h_inter and cleaned_h2 == 'lẽ':
                final_column_names.append('xuất_trong_kỳ_lẽ')
            elif 'xuất_trong_kỳ_số_lượng' in h_inter and cleaned_h2 == 'sỉ':
                final_column_names.append('xuất_trong_kỳ_sỉ')
            else:
                final_column_names.append(f"{h_inter}_{cleaned_h2}")
        else:
            final_column_names.append(h_inter)

    return final_column_names


def handle_duplicate_column_names(column_names):
    """Handle duplicate column names by appending a suffix and replacing empty strings."""
    counts = {}
    final_columns = []
    for col in column_names:
        base_name = col if col else 'unnamed_col'

        if base_name in counts:
            counts[base_name] += 1
            final_columns.append(f"{base_name}_{counts[base_name]}")
        else:
            counts[base_name] = 0
            final_columns.append(base_name)

    return final_columns


def clean_cell_value(value):
    """Clean individual cell values: handle NaN, strip whitespace, remove non-breaking spaces."""
    if pd.isna(value):
        return None
    cleaned_str = str(value).strip().replace('\xa0', ' ')
    return cleaned_str if cleaned_str != '' else None


def excel_date_to_datetime(excel_date):
    """Convert Excel serial date format to datetime object."""
    if excel_date is None:
        return pd.NaT
    try:
        if isinstance(excel_date, str):
            for fmt in ('%d-%b', '%d/%m/%Y', '%Y-%m-%d'):
                try:
                    return pd.to_datetime(excel_date, format=fmt)
                except ValueError:
                    pass
        excel_float = float(excel_date)
        if excel_float > 59:
            return pd.to_datetime(excel_float - 25569, unit='D')
        elif excel_float == 59:
            return pd.to_datetime('1900-02-28')
        else:
            return pd.to_datetime(excel_float - 25568, unit='D')
    except (ValueError, TypeError):
        return pd.NaT


def excel_date_from_day_month_year(day_str, month_str, year):
    """Convert day, month, and year values to datetime object."""
    if pd.isna(day_str) or pd.isna(month_str) or year is None:
        return pd.NaT
    try:
        day_int = int(day_str)
        month_int = int(month_str)
        return pd.to_datetime(f"{year}-{month_int}-{day_int}")
    except (ValueError, TypeError):
        return pd.NaT


def clean_and_convert_numeric(value):
    """Convert numeric strings to float, handling dot/comma decimal separators."""
    if value is None:
        return None
    try:
        cleaned_value = str(value).replace('.', '').replace(',', '.').strip()
        return pd.to_numeric(cleaned_value)
    except ValueError:
        return None
