# -*- coding: utf-8 -*-
"""Generate KiotViet supplier import XLSX and upload to Google Sheets.

This single script:
1. Reads supplier data from Google Sheets (MÃ CTY, TỔNG HỢP)
2. Reads transaction data from staged clean_receipts_purchase_*.csv
3. Merges and transforms data in-memory
4. Generates unified supplier codes (NCC000001, NCC000002, ...)
5. Maps to KiotViet 15-column template
6. Exports to data/03-erp-export/Suppliers.xlsx
7. Uploads to Google Sheet as "suppliers_to_import" tab

Raw sources:
- Google Sheets: 1b4LWWyfddfiMZWnFreTyC-epo17IR4lcbUnPpLW8X00 (MÃ CTY, TỔNG HỢP)
- Local CSVs: data/01-staging/import_export/clean_receipts_purchase_*.csv

Output:
- XLSX: data/03-erp-export/Suppliers.xlsx
- Google Sheet: 11vk-p0iL9JcNH180n4uV5VTuPnhJ97lBgsLEfCnWx_k (tab: suppliers_to_import)
"""

import logging
import re
from pathlib import Path
from typing import List, Optional

import pandas as pd
import toml
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill

from src.erp.templates import SupplierTemplate
from src.modules.google_api import (
    authenticate_google,
    read_sheet_data,
    write_sheet_data,
)

logger = logging.getLogger(__name__)


def clean_phone_number(phone: str) -> str:
    if not phone:
        return ""
    phone = str(phone).strip()
    phone = re.sub(r"[.,;:]", "", phone)
    phone = re.sub(r"\s+", "", phone)
    return phone


def split_phone_numbers(phone_str: str) -> List[str]:
    if not phone_str or pd.isna(phone_str):
        return []

    phone_str = str(phone_str).strip()
    if not phone_str or phone_str == "None":
        return []

    if "/" in phone_str or " - " in phone_str:
        phones = re.split(r"\s*[/-]\s*", phone_str)
    else:
        phones = [phone_str]

    cleaned = [clean_phone_number(p) for p in phones]
    cleaned = [p for p in cleaned if p]

    return cleaned


def parse_numeric(value: str) -> str:
    if not value or pd.isna(value):
        return "0"

    value = str(value).strip()

    if value == "-" or value == "":
        return "0"

    value = value.replace(".", "").replace(" ", "")

    if value.startswith("(") and value.endswith(")"):
        value = "-" + value[1:-1]

    return value


DATA_STAGING_DIR = Path.cwd() / "data" / "01-staging"
IMPORT_EXPORT_STAGING = DATA_STAGING_DIR / "import_export"
DATA_EXPORT_DIR = Path.cwd() / "data" / "03-erp-export"
PIPELINE_CONFIG = Path.cwd() / "pipeline.toml"

PAYABLE_SPREADSHEET_ID = "1b4LWWyfddfiMZWnFreTyC-epo17IR4lcbUnPpLW8X00"
EXPORT_SPREADSHEET_ID = "11vk-p0iL9JcNH180n4uV5VTuPnhJ97lBgsLEfCnWx_k"
EXPORT_SHEET_NAME = "suppliers_to_import"


def load_config() -> dict:
    if not PIPELINE_CONFIG.exists():
        logger.warning(f"Config file not found: {PIPELINE_CONFIG}")
        return {}

    try:
        config = toml.load(PIPELINE_CONFIG)
        return config
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        return {}


def read_google_sheet(sheets_service, spreadsheet_id: str, sheet_name: str) -> list:
    raw_data = read_sheet_data(sheets_service, spreadsheet_id, sheet_name)
    if not raw_data:
        logger.warning(f"No data from {sheet_name}")
        return []

    header_row_idx = None
    for idx, row in enumerate(raw_data):
        if row and str(row[0]).strip() == "STT":
            header_row_idx = idx
            break

    if header_row_idx is None:
        logger.warning(f"Could not find header row in {sheet_name}")
        return []

    header_row = raw_data[header_row_idx]
    data_rows = raw_data[header_row_idx + 1 :]

    max_cols = len(header_row)
    padded_rows = []
    for row in data_rows:
        if len(row) < max_cols:
            row = row + [""] * (max_cols - len(row))
        padded_rows.append(row[:max_cols])

    return [header_row] + padded_rows


def load_ma_cty(sheets_service) -> pd.DataFrame:
    logger.info("Loading MÃ CTY...")
    raw_data = read_google_sheet(sheets_service, PAYABLE_SPREADSHEET_ID, "MÃ CTY")
    if not raw_data:
        return pd.DataFrame()

    header = raw_data[0]
    rows = raw_data[1:]

    col_map = {}
    for idx, col in enumerate(header):
        col_clean = col.strip() if col else ""
        if col_clean == "MÃ NCC":
            col_map["MÃ NCC"] = idx
        elif col_clean == "TÊN NCC":
            col_map["TÊN NCC"] = idx
        elif col_clean == "SĐT":
            col_map["SĐT"] = idx
        elif col_clean in ("Địa chỉ", "Địa chỉ liên hệ"):
            col_map["Địa chỉ"] = idx
        elif col_clean in ("Email", "E-mail"):
            col_map["Email"] = idx
        elif col_clean in ("Mã số thuế", "MST"):
            col_map["Mã số thuế"] = idx
        elif col_clean in ("Ghi chú", "Ghi chú/Notes"):
            col_map["Ghi chú"] = idx

    if "TÊN NCC" not in col_map:
        logger.warning("Required column TÊN NCC not found")
        return pd.DataFrame()

    data = []
    for row in rows:
        if len(row) <= col_map.get("TÊN NCC", 0):
            continue

        name = str(row[col_map["TÊN NCC"]]).strip()
        if not name:
            continue

        if name.isdigit():
            continue

        if name.upper() in ("TỔNG CỘNG", "NL", "NGƯỜI LẬP", "TYPO"):
            continue

        ma_ncc_col = col_map.get("MÃ NCC")
        ma_ncc_val = ""
        if ma_ncc_col is not None and len(row) > ma_ncc_col:
            ma_ncc_val = str(row[ma_ncc_col]).strip()

        tel_col = col_map.get("SĐT")
        tel_raw = ""
        if tel_col is not None and len(row) > tel_col:
            tel_raw = str(row[tel_col]).strip()

        tel_list = split_phone_numbers(tel_raw)
        phone_value = tel_list[0] if tel_list else ""

        email_col = col_map.get("Email")
        email_val = ""
        if email_col is not None and len(row) > email_col:
            email_val = str(row[email_col]).strip()

        data.append(
            {
                "Mã NCC cũ": ma_ncc_val,
                "Tên nhà cung cấp": name,
                "Điện thoại": phone_value,
                "Email": email_val,
                "Địa chỉ": str(row[col_map.get("Địa chỉ", -1)]).strip()
                if col_map.get("Địa chỉ") is not None and len(row) > col_map["Địa chỉ"]
                else "",
                "Mã số thuế": str(row[col_map.get("Mã số thuế", -1)]).strip()
                if col_map.get("Mã số thuế") is not None
                and len(row) > col_map["Mã số thuế"]
                else "",
                "Ghi chú": str(row[col_map.get("Ghi chú", -1)]).strip()
                if col_map.get("Ghi chú") is not None and len(row) > col_map["Ghi chú"]
                else "",
            }
        )

    df = pd.DataFrame(data)
    logger.info(f"Loaded {len(df)} suppliers from MÃ CTY")
    return df


def load_tong_hop(sheets_service) -> pd.DataFrame:
    logger.info("Loading TỔNG HỢP...")
    raw_data = read_google_sheet(sheets_service, PAYABLE_SPREADSHEET_ID, "TỔNG HỢP")
    if not raw_data:
        return pd.DataFrame()

    header = raw_data[0]
    rows = raw_data[1:]

    col_map = {}
    for idx, col in enumerate(header):
        col_clean = col.strip() if col else ""
        if col_clean == "TÊN NHÀ CUNG CẤP":
            col_map["TÊN NHÀ CUNG CẤP"] = idx
        elif "NỢ CÒN LẠI" in col_clean:
            col_map["NỢ CÒN LẠI"] = idx

    if "TÊN NHÀ CUNG CẤP" not in col_map:
        logger.warning("Required column TÊN NHÀ CUNG CẤP not found in TỔNG HỢP")
        return pd.DataFrame()

    data = []
    for row in rows:
        if len(row) <= col_map.get("TÊN NHÀ CUNG CẤP", 0):
            continue

        name = str(row[col_map["TÊN NHÀ CUNG CẤP"]]).strip()
        if not name:
            continue

        if name.isdigit():
            continue

        if name.upper() in ("TỔNG CỘNG", "NL", "NGƯỜI LẬP", "TYPO"):
            continue

        debt_col = col_map.get("NỢ CÒN LẠI")
        debt_val = "0"
        if debt_col is not None and len(row) > debt_col:
            debt_val = parse_numeric(row[debt_col])

        data.append(
            {
                "Tên nhà cung cấp": name,
                "Nợ cần trả hiện tại": debt_val,
            }
        )

    df = pd.DataFrame(data)
    df["Nợ cần trả hiện tại"] = pd.to_numeric(
        df["Nợ cần trả hiện tại"], errors="coerce"
    ).fillna(0)
    logger.info(f"Loaded {len(df)} payable records from TỔNG HỢP")
    return df


def load_purchase_transactions() -> pd.DataFrame:
    logger.info("Loading purchase transactions from staging...")

    receipt_files = list(IMPORT_EXPORT_STAGING.glob("Chi tiết nhập*.csv"))
    if not receipt_files:
        logger.warning("No Chi tiết nhập*.csv files found")
        return pd.DataFrame()

    all_data = []
    for f in receipt_files:
        try:
            df = pd.read_csv(f, encoding="utf-8")
            all_data.append(df)
            logger.info(f"  Loaded {len(df)} rows from {f.name}")
        except Exception as e:
            logger.warning(f"Error reading {f}: {e}")

    if not all_data:
        return pd.DataFrame()

    combined = pd.concat(all_data, ignore_index=True)
    logger.info(f"Total transactions: {len(combined)}")
    return combined


def aggregate_transactions(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "Tên nhà cung cấp" not in df.columns:
        return pd.DataFrame()

    logger.info("Aggregating transactions by supplier...")

    df = df.copy()

    if "Ngày" in df.columns:
        df["Ngày"] = pd.to_datetime(df["Ngày"], errors="coerce")

    amount_col = "Thành tiền" if "Thành tiền" in df.columns else "Số lượng"

    aggregated = (
        df.groupby("Tên nhà cung cấp", dropna=False)
        .agg(
            first_date=("Ngày", "min"),
            last_date=("Ngày", "max"),
            total_amount=(amount_col, "sum"),
            transaction_count=("Ngày", "count"),
        )
        .reset_index()
    )

    aggregated = aggregated.dropna(subset=["Tên nhà cung cấp"])
    aggregated["Tên nhà cung cấp"] = aggregated["Tên nhà cung cấp"].str.strip()

    logger.info(f"Aggregated {len(aggregated)} unique suppliers")
    return aggregated


def merge_all_data(
    suppliers_df: pd.DataFrame,
    debts_df: pd.DataFrame,
    transactions_df: pd.DataFrame,
) -> pd.DataFrame:
    logger.info("Merging all data sources...")

    all_suppliers = set()

    if not suppliers_df.empty and "Tên nhà cung cấp" in suppliers_df.columns:
        all_suppliers |= set(suppliers_df["Tên nhà cung cấp"].dropna().unique())

    if not debts_df.empty and "Tên nhà cung cấp" in debts_df.columns:
        all_suppliers |= set(debts_df["Tên nhà cung cấp"].dropna().unique())

    if not transactions_df.empty and "Tên nhà cung cấp" in transactions_df.columns:
        all_suppliers |= set(transactions_df["Tên nhà cung cấp"].dropna().unique())

    all_suppliers = {c for c in all_suppliers if c and str(c).strip()}

    if not all_suppliers:
        logger.info("No suppliers found in any source")
        return pd.DataFrame()

    logger.info(f"Total unique suppliers: {len(all_suppliers)}")

    result = pd.DataFrame({"Tên nhà cung cấp": sorted(list(all_suppliers))})

    if not suppliers_df.empty and "Tên nhà cung cấp" in suppliers_df.columns:
        suppliers_df = suppliers_df.copy()
        suppliers_df["Tên nhà cung cấp"] = suppliers_df["Tên nhà cung cấp"].str.strip()
        result = result.merge(suppliers_df, on="Tên nhà cung cấp", how="left")

    if not debts_df.empty and "Tên nhà cung cấp" in debts_df.columns:
        debts_df = debts_df.copy()
        debts_df["Tên nhà cung cấp"] = debts_df["Tên nhà cung cấp"].str.strip()
        result = result.merge(debts_df, on="Tên nhà cung cấp", how="left")

    if not transactions_df.empty and "Tên nhà cung cấp" in transactions_df.columns:
        transactions_df = transactions_df.copy()
        transactions_df["Tên nhà cung cấp"] = transactions_df[
            "Tên nhà cung cấp"
        ].str.strip()
        result = result.merge(transactions_df, on="Tên nhà cung cấp", how="left")

    if "Nợ cần trả hiện tại" in result.columns:
        result["Nợ cần trả hiện tại"] = pd.to_numeric(
            result["Nợ cần trả hiện tại"], errors="coerce"
        ).fillna(0)

    result = result.fillna("")
    return result


def generate_supplier_codes(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()

    if "first_date" in df.columns:
        df["first_date"] = pd.to_datetime(df["first_date"], errors="coerce")
    else:
        df["first_date"] = pd.NaT

    if "total_amount" not in df.columns:
        df["total_amount"] = 0
    df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce").fillna(0)

    df = df.sort_values(
        by=["first_date", "total_amount", "Tên nhà cung cấp"],
        ascending=[True, False, True],
        na_position="last",
    ).reset_index(drop=True)

    df["Mã nhà cung cấp"] = df.index.map(lambda x: f"NCC{x + 1:06d}")

    logger.info(f"Generated {len(df)} supplier codes")
    return df


def map_to_kiotviet_template(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    template = SupplierTemplate()
    columns = template.get_column_names()

    result = pd.DataFrame(index=range(len(df)), columns=columns)
    result = result.fillna("")

    result.loc[: len(df) - 1, "Mã nhà cung cấp"] = df["Mã nhà cung cấp"].values
    result.loc[: len(df) - 1, "Tên nhà cung cấp"] = df["Tên nhà cung cấp"].values

    if "Điện thoại" in df.columns:
        phones = df["Điện thoại"].apply(
            lambda x: split_phone_numbers(x)[0] if split_phone_numbers(x) else ""
        )
        result.loc[: len(df) - 1, "Điện thoại"] = phones.astype(str).values

    if "Email" in df.columns:
        emails = df["Email"].fillna("").astype(str)
        # Generate dummy emails for empty values using supplier code
        empty_mask = ~emails.str.strip().astype(bool)
        emails = emails.where(~empty_mask, other=df["Mã nhà cung cấp"] + "@dummy.local")
        result.loc[: len(df) - 1, "Email"] = emails.values

    if "Địa chỉ" in df.columns:
        result.loc[: len(df) - 1, "Địa chỉ"] = df["Địa chỉ"].fillna("").values

    if "Mã số thuế" in df.columns:
        result.loc[: len(df) - 1, "Mã số thuế"] = df["Mã số thuế"].fillna("").values

    if "Ghi chú" in df.columns:
        ghi_chu = df["Ghi chú"].fillna("")
        ma_ncc_cu = df.get("Mã NCC cũ", pd.Series([""] * len(df)))
        if isinstance(ma_ncc_cu, str):
            ma_ncc_cu = pd.Series([ma_ncc_cu] * len(df))
        ma_ncc_cu = ma_ncc_cu.fillna("")
        combined = []
        for gh, mk in zip(ghi_chu, ma_ncc_cu):
            parts = []
            if gh and str(gh).strip():
                parts.append(str(gh))
            if mk and str(mk).strip():
                parts.append(f"Mã cũ: {mk}")
            combined.append("\n".join(parts))
        result.loc[: len(df) - 1, "Ghi chú"] = combined

    if "last_date" in df.columns:
        dates = pd.to_datetime(df["last_date"], errors="coerce")
        # Use ISO 8601 format with timezone info
        dates_str = dates.dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        dates_str = dates_str.replace("NaT", "")
        result.loc[: len(df) - 1, "Ngày giao dịch cuối"] = dates_str.values

    if "Nợ cần trả hiện tại" in df.columns:
        result.loc[: len(df) - 1, "Nợ cần trả hiện tại"] = (
            pd.to_numeric(df["Nợ cần trả hiện tại"], errors="coerce").fillna(0).values
        )

    if "total_amount" in df.columns:
        result.loc[: len(df) - 1, "Tổng mua (Không Import)"] = (
            pd.to_numeric(df["total_amount"], errors="coerce").fillna(0).values
        )

    result.loc[: len(df) - 1, "Nhóm nhà cung cấp"] = ""

    if "last_date" in df.columns and "Nợ cần trả hiện tại" in df.columns:
        from datetime import datetime, timedelta

        cutoff_date = datetime.now() - timedelta(days=90)
        last_date = pd.to_datetime(df["last_date"], errors="coerce")
        debt = pd.to_numeric(df["Nợ cần trả hiện tại"], errors="coerce").fillna(0)

        has_recent_transaction = (last_date >= cutoff_date) & last_date.notna()
        has_debt = debt > 0

        is_active = has_recent_transaction | has_debt
        result.loc[: len(df) - 1, "Trạng thái"] = is_active.apply(
            lambda x: 1 if x else 0
        ).values
    else:
        result.loc[: len(df) - 1, "Trạng thái"] = 1

    result = result.infer_objects(copy=False).fillna("")
    result["Điện thoại"] = result["Điện thoại"].astype(str)

    return result


def write_xlsx(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    template = SupplierTemplate()

    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "Nhà cung cấp"

    for col_idx, col_spec in enumerate(template.COLUMNS, start=1):
        cell = worksheet.cell(row=1, column=col_idx, value=col_spec.name)

    for idx, row in df.iterrows():
        for col_idx, col_spec in enumerate(template.COLUMNS, start=1):
            value = row[col_spec.name]
            if pd.isna(value):
                value = ""
            elif not isinstance(value, str):
                value = str(value)
            cell = worksheet.cell(row=idx + 2, column=col_idx, value=value)
            if col_spec.data_type == "text":
                cell.number_format = "@"

    header_fill = PatternFill(
        start_color="4472C4", end_color="4472C4", fill_type="solid"
    )
    header_font = Font(color="FFFFFF", bold=True)

    for col_idx in range(1, len(template.COLUMNS) + 1):
        cell = worksheet.cell(row=1, column=col_idx)
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center", vertical="center")

    for col_idx, col_spec in enumerate(template.COLUMNS, start=1):
        letter = worksheet.cell(row=1, column=col_idx).column_letter
        worksheet.column_dimensions[letter].width = 20

        if col_spec.format_code and col_spec.data_type == "number":
            for row in range(2, len(df) + 2):
                cell = worksheet.cell(row=row, column=col_idx)
                cell.number_format = col_spec.format_code
                cell.alignment = Alignment(horizontal="right")

    workbook.save(output_path)
    logger.info(f"Wrote XLSX: {output_path}")


def validate_data(df: pd.DataFrame) -> tuple:
    template = SupplierTemplate()
    is_valid, errors = template.validate_dataframe(df)

    if not is_valid:
        for error in errors:
            logger.error(f"Validation error: {error}")

    return is_valid, errors


def format_value(value, data_type: str) -> any:
    if value is None or value == "" or (isinstance(value, float) and pd.isna(value)):
        return ""

    if data_type == "number":
        if isinstance(value, (int, float)):
            return value
        try:
            cleaned = str(value).replace(",", "").replace(".", "").replace(" ", "")
            if cleaned.startswith("(") and cleaned.endswith(")"):
                cleaned = "-" + cleaned[1:-1]
            return float(cleaned)
        except (ValueError, TypeError):
            return str(value)

    elif data_type == "date":
        if isinstance(value, str):
            if "-" in value and len(value) >= 10:
                return value[:10]
            try:
                parsed = pd.to_datetime(value, errors="coerce")
                if pd.notna(parsed):
                    return parsed.strftime("%Y-%m-%d")
            except Exception:
                pass
        return str(value)

    else:
        return str(value)


def upload_to_google_sheet(df: pd.DataFrame, sheets_service) -> bool:
    logger.info(
        f"Uploading to Google Sheet: {EXPORT_SPREADSHEET_ID}/{EXPORT_SHEET_NAME}"
    )

    template = SupplierTemplate()
    col_types = {col.name: col.data_type for col in template.COLUMNS}

    header = df.columns.tolist()
    rows = []

    for _, row in df.iterrows():
        formatted_row = []
        for col in header:
            data_type = col_types.get(col, "text")
            formatted_row.append(format_value(row[col], data_type))
        rows.append(formatted_row)

    values = [header] + rows

    success = write_sheet_data(
        sheets_service, EXPORT_SPREADSHEET_ID, EXPORT_SHEET_NAME, values
    )

    if success:
        logger.info(f"Successfully uploaded {len(df)} rows to {EXPORT_SHEET_NAME}")
    else:
        logger.error("Failed to upload to Google Sheet")

    return success


def process(staging_dir: Optional[Path] = None) -> Optional[Path]:
    """Generate Suppliers XLSX from Google Sheets data.

    Args:
        staging_dir: Optional path to staging directory (unused, for API consistency).
                    This function reads from Google Sheets directly.

    Returns:
        Path to generated Suppliers.xlsx or None if failed.
    """
    logger.info("=" * 70)
    logger.info("GENERATING SUPPLIERS XLSX")
    logger.info("=" * 70)

    DATA_EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    try:
        creds = authenticate_google()
        from googleapiclient.discovery import build

        sheets_service = build("sheets", "v4", credentials=creds)

        suppliers_df = load_ma_cty(sheets_service)
        debts_df = load_tong_hop(sheets_service)
        transactions_df = aggregate_transactions(load_purchase_transactions())

        merged_df = merge_all_data(suppliers_df, debts_df, transactions_df)

        if merged_df.empty:
            logger.error("No supplier data to export")
            return None

        merged_df = generate_supplier_codes(merged_df)
        template_df = map_to_kiotviet_template(merged_df)

        is_valid, errors = validate_data(template_df)
        if not is_valid:
            logger.error(f"Data validation failed: {errors}")
            return None

        output_path = DATA_EXPORT_DIR / "Suppliers.xlsx"
        write_xlsx(template_df, output_path)

        upload_to_google_sheet(template_df, sheets_service)

        logger.info(f"Generated {len(template_df)} supplier records")
        logger.info(f"Output: {output_path}")
        logger.info("=" * 70)

        return output_path

    except Exception as e:
        logger.error(f"Failed to generate suppliers XLSX: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    process()
