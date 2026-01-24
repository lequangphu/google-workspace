# -*- coding: utf-8 -*-
"""Data loader with caching for all data sources.

This module provides a unified DataLoader class that uses StagingCache
to load data with automatic invalidation.

Data Flow by Source Type (ADR-005):
- "preprocessed": Data already clean, load from raw (e.g., import_export_receipts)
- "raw": Data needs transformation, load from staging after transform runs

Source type configuration is in pipeline.toml for each source.

Replaces scattered pd.read_csv() calls in:
- generate_products_xlsx.py
- generate_customers_xlsx.py
- generate_suppliers_xlsx.py
- exporter.py
"""

from typing import Dict, Tuple

import pandas as pd
import toml

from ..utils.staging_cache import StagingCache
from ..utils.path_config import PathConfig
from pathlib import Path

import logging

logger = logging.getLogger(__name__)


class DataLoader:
    """Unified data loader with built-in caching.

    Provides single entry point for all data reads with automatic
    cache invalidation via StagingCache.

    Source type determines load location:
    - "preprocessed": Load from raw (no transform needed)
    - "raw": Load from staging (after transform runs)

    Usage:
        loader = DataLoader()
        products = loader.load_products()
        customers = loader.load_customers(sheets_service)
        suppliers = loader.load_suppliers(sheets_service)
    """

    def __init__(self, config: Dict = None):
        """Initialize data loader with configuration.

        Args:
            config: Optional config dictionary. If None, loads from pipeline.toml
        """
        if config is None:
            self.config = self._load_default_config()
        else:
            self.config = config

        self.path_config = PathConfig()
        self.spreadsheet_ids = self._load_spreadsheet_ids()
        self.patterns = {
            "nhap": "*Chi tiết nhập*.csv",
            "xuat": "*Chi tiết xuất*.csv",
            "xnt": "Xuất nhập tồn *.csv",
            "product_info": "*product_info*.csv",
        }
        self.cache = StagingCache()

    def _load_default_config(self) -> Dict:
        workspace_root = Path(__file__).parent.parent.parent
        config_path = workspace_root / "pipeline.toml"

        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        return toml.load(config_path)

    def _load_spreadsheet_ids(self) -> Dict[str, str]:
        ids = {}
        for key in self.config:
            if key.endswith("_spreadsheet_id"):
                ids[key.replace("_spreadsheet_id", "")] = self.config[key]
        return ids

    def _get_source_type(self, source_key: str) -> str:
        """Get source_type from configuration.

        Args:
            source_key: Source key from pipeline.toml (e.g., "import_export_receipts").

        Returns:
            Source type string ("preprocessed" or "raw"). Defaults to "raw" if not specified.
        """
        sources = self.config.get("sources", {})
        if source_key not in sources:
            return "raw"

        return sources[source_key].get("source_type", "raw")

    def _get_import_export_dir(self) -> Path:
        """Get directory for import_export data based on source_type.

        Returns:
            Path to raw/import_export for "preprocessed" sources.
            Path to staging/import_export for "raw" sources (after transform).
        """
        source_type = self._get_source_type("import_export_receipts")

        if source_type == "preprocessed":
            logger.debug("Loading import_export from raw (preprocessed source)")
            return self.path_config.import_export_raw_dir()
        else:
            logger.debug("Loading import_export from staging (raw source)")
            return self.path_config.import_export_staging_dir()

    def load_products(self) -> Dict[str, pd.DataFrame]:
        import_export_dir = self._get_import_export_dir()

        nhap_files = list(
            import_export_dir.glob(self.patterns.get("nhap", "*Chi tiết nhập*.csv"))
        )
        if not nhap_files:
            raise FileNotFoundError(
                f"No Chi tiết nhập*.csv files found in {import_export_dir}"
            )

        nhap_df = self.cache.get_dataframe(nhap_files[0])
        logger.info(f"Loaded nhập data: {len(nhap_df)} rows")

        xuat_files = list(
            import_export_dir.glob(self.patterns.get("xuat", "*Chi tiết xuất*.csv"))
        )
        if not xuat_files:
            raise FileNotFoundError(
                f"No Chi tiết xuất*.csv files found in {import_export_dir}"
            )

        xuat_df = self.cache.get_dataframe(xuat_files[0])
        logger.info(f"Loaded xuất data: {len(xuat_df)} rows")

        xnt_files = list(
            import_export_dir.glob(self.patterns.get("xnt", "Xuất nhập tồn *.csv"))
        )
        xnt_files = [f for f in xnt_files if "adjustments" not in f.name.lower()]

        if not xnt_files:
            raise FileNotFoundError(
                f"No Xuất nhập tồn*.csv files found in {import_export_dir}"
            )

        inventory_df = self._get_latest_inventory(xnt_files)
        logger.info(f"Loaded inventory data: {len(inventory_df)} products")

        product_info_files = list(
            import_export_dir.glob(
                self.patterns.get("product_info", "*product_info*.csv")
            )
        )
        if product_info_files:
            lookup_df = self.cache.get_dataframe(product_info_files[0])
            logger.info(f"Loaded product lookup: {len(lookup_df)} rows")
        else:
            lookup_df = pd.DataFrame()
            logger.warning("No product_info files found")

        return {
            "purchase": nhap_df,
            "sales": xuat_df,
            "inventory": inventory_df,
            "product_info": lookup_df,
        }

    def _get_latest_inventory(self, xnt_files: list[Path]) -> pd.DataFrame:
        # Sort files by modification time, get the latest
        xnt_files_sorted = sorted(
            xnt_files, key=lambda f: f.stat().st_mtime, reverse=True
        )
        latest_file = xnt_files_sorted[0]

        df = self.cache.get_dataframe(latest_file)

        result = df[["Mã hàng", "Tồn cuối kỳ", "Giá trị cuối kỳ"]].copy()
        return result

    def load_customers(
        self, sheets_service=None
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        import_export_dir = self._get_import_export_dir()

        if sheets_service:
            master_df = self._load_thong_tin_kh(sheets_service)
            logger.info(f"Loaded {len(master_df)} customers from Thong tin KH")
        else:
            master_df = pd.DataFrame()
            logger.warning("No sheets_service provided, skipping Thong tin KH load")

        if sheets_service:
            debts_df = self._load_tong_cong_no(sheets_service)
            logger.info(f"Loaded {len(debts_df)} debt records from TỔNG CÔNG NỢ")
        else:
            debts_df = pd.DataFrame()
            logger.warning("No sheets_service provided, skipping TỔNG CÔNG NỢ load")

        transactions_df = self._load_sale_transactions(import_export_dir)
        logger.info(f"Loaded {len(transactions_df)} sale transactions")

        return master_df, debts_df, transactions_df

    def _load_thong_tin_kh(self, sheets_service) -> pd.DataFrame:
        from ..utils.data_cleaning import split_phone_numbers
        from ..modules.google_api import read_sheet_data

        logger.info("Loading Thong tin KH...")

        spreadsheet_id = self.spreadsheet_ids.get("receivable")
        raw_data = read_sheet_data(sheets_service, spreadsheet_id, "Thong tin KH")
        if not raw_data:
            return pd.DataFrame()

        header_row_idx = None
        for idx, row in enumerate(raw_data):
            if row and str(row[0]).strip() == "STT":
                header_row_idx = idx
                break

        if header_row_idx is None:
            logger.warning("Could not find header row in Thong tin KH")
            return pd.DataFrame()

        header_row = raw_data[header_row_idx]
        data_rows = raw_data[header_row_idx + 1 :]

        max_cols = len(header_row)
        padded_rows = []
        for row in data_rows:
            if len(row) < max_cols:
                row = row + [""] * (max_cols - len(row))
            padded_rows.append(row[:max_cols])

        rows = [header_row] + padded_rows

        col_map = {}
        for idx, col in enumerate(header_row):
            col_clean = col.strip() if col else ""
            if col_clean == "MÃ KH":
                col_map["MÃ KH"] = idx
            elif col_clean == "TÊN KHÁCH HÀNG":
                col_map["TÊN KHÁCH HÀNG"] = idx
            elif col_clean == "Địa chỉ":
                col_map["Địa chỉ"] = idx
            elif col_clean == "Tel":
                col_map["Tel"] = idx
            elif col_clean == "Ghi chú":
                col_map["Ghi chú"] = idx

        if "TÊN KHÁCH HÀNG" not in col_map:
            logger.warning("Required column TÊN KHÁCH HÀNG not found")
            return pd.DataFrame()

        data = []
        for row in rows[1:]:
            if len(row) <= col_map.get("TÊN KHÁCH HÀNG", 0):
                continue

            name = str(row[col_map["TÊN KHÁCH HÀNG"]]).strip()
            if not name:
                continue

            if name.isdigit():
                continue

            if name.upper() in ("TỔNG CỘNG", "NL", "NGƯỜI LẬP", "TYPO", "TRUNGKL"):
                continue

            ma_kh_col = col_map.get("MÃ KH")
            ma_kh_val = ""
            if ma_kh_col is not None and len(row) > ma_kh_col:
                ma_kh_val = str(row[ma_kh_col]).strip()

            if not ma_kh_val:
                continue

            tel_col = col_map.get("Tel")
            tel_raw = ""
            if tel_col is not None and len(row) > tel_col:
                tel_raw = str(row[tel_col]).strip()

            tel_list = split_phone_numbers(tel_raw)
            phone_value = tel_list[0] if tel_list else ""

            data.append(
                {
                    "Mã KH cũ": ma_kh_val,
                    "Tên khách hàng": name,
                    "Điện thoại": phone_value,
                    "Địa chỉ": str(row[col_map.get("Địa chỉ", -1)]).strip()
                    if col_map.get("Địa chỉ") is not None
                    and len(row) > col_map["Địa chỉ"]
                    else "",
                    "Ghi chú": str(row[col_map.get("Ghi chú", -1)]).strip()
                    if col_map.get("Ghi chú") is not None
                    and len(row) > col_map["Ghi chú"]
                    else "",
                }
            )

        df = pd.DataFrame(data)
        logger.info(f"Loaded {len(df)} customers from Thong tin KH")
        return df

    def _load_tong_cong_no(self, sheets_service) -> pd.DataFrame:
        from ..utils.data_cleaning import parse_numeric
        from ..modules.google_api import read_sheet_data

        logger.info("Loading TỔNG CÔNG NỢ...")

        spreadsheet_id = self.spreadsheet_ids.get("receivable")
        raw_data = read_sheet_data(sheets_service, spreadsheet_id, "TỔNG CÔNG NỢ")
        if not raw_data:
            return pd.DataFrame()

        header_row_idx = None
        for idx, row in enumerate(raw_data):
            if row and str(row[0]).strip() == "STT":
                header_row_idx = idx
                break

        if header_row_idx is None:
            logger.warning("Could not find header row in TỔNG CÔNG NỢ")
            return pd.DataFrame()

        header_row = raw_data[header_row_idx]
        data_rows = raw_data[header_row_idx + 1 :]

        max_cols = len(header_row)
        padded_rows = []
        for row in data_rows:
            if len(row) < max_cols:
                row = row + [""] * (max_cols - len(row))
            padded_rows.append(row[:max_cols])

        rows = [header_row] + padded_rows

        col_map = {}
        for idx, col in enumerate(header_row):
            col_clean = col.strip() if col else ""
            if col_clean == "TÊN KHÁCH HÀNG":
                col_map["TÊN KHÁCH HÀNG"] = idx
            elif "NỢ CÒN LẠI" in col_clean:
                col_map["NỢ CÒN LẠI"] = idx

        if "TÊN KHÁCH HÀNG" not in col_map:
            logger.warning("Required column TÊN KHÁCH HÀNG not found in TỔNG CÔNG NỢ")
            return pd.DataFrame()

        data = []
        for row in rows[1:]:
            if len(row) <= col_map.get("TÊN KHÁCH HÀNG", 0):
                continue

            name = str(row[col_map["TÊN KHÁCH HÀNG"]]).strip()
            if not name:
                continue

            if name.isdigit():
                continue

            if name.upper() in ("TỔNG CỘNG", "NL", "NGƯỜI LẬP", "TYPO", "TRUNGKL"):
                continue

            debt_col = col_map.get("NỢ CÒN LẠI")
            debt_val = "0"
            if debt_col is not None and len(row) > debt_col:
                debt_val = parse_numeric(row[debt_col])

            data.append(
                {
                    "Tên khách hàng": name,
                    "Nợ cần thu hiện tại": debt_val,
                }
            )

        df = pd.DataFrame(data)
        df["Nợ cần thu hiện tại"] = pd.to_numeric(
            df["Nợ cần thu hiện tại"], errors="coerce"
        ).fillna(0)
        logger.info(f"Loaded {len(df)} debt records from TỔNG CÔNG NỢ")
        return df

    def _load_sale_transactions(self, staging_dir: Path) -> pd.DataFrame:
        logger.info("Loading sale transactions from staging...")

        receipt_files = list(staging_dir.glob("Chi tiết xuất*.csv"))
        if not receipt_files:
            logger.warning("No Chi tiết xuất*.csv files found")
            return pd.DataFrame()

        all_data = []
        for f in receipt_files:
            try:
                df = self.cache.get_dataframe(f)
                all_data.append(df)
                logger.info(f"  Loaded {len(df)} rows from {f.name}")
            except Exception as e:
                logger.warning(f"Error reading {f}: {e}")

        if not all_data:
            return pd.DataFrame()

        combined = pd.concat(all_data, ignore_index=True)
        logger.info(f"Total transactions: {len(combined)}")
        return combined

    def load_suppliers(
        self, sheets_service=None
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        import_export_dir = self._get_import_export_dir()

        if sheets_service:
            master_df = self._load_ma_cty(sheets_service)
            logger.info(f"Loaded {len(master_df)} suppliers from MÃ CTY")
        else:
            master_df = pd.DataFrame()
            logger.warning("No sheets_service provided, skipping MÃ CTY load")

        if sheets_service:
            debts_df = self._load_tong_hop(sheets_service)
            logger.info(f"Loaded {len(debts_df)} debt records from TỔNG HỢP")
        else:
            debts_df = pd.DataFrame()
            logger.warning("No sheets_service provided, skipping TỔNG HỢP load")

        transactions_df = self._load_purchase_transactions(import_export_dir)
        logger.info(f"Loaded {len(transactions_df)} purchase transactions")

        return master_df, debts_df, transactions_df

    def _load_ma_cty(self, sheets_service) -> pd.DataFrame:
        from ..utils.data_cleaning import split_phone_numbers
        from ..modules.google_api import read_sheet_data

        logger.info("Loading MÃ CTY...")

        spreadsheet_id = self.spreadsheet_ids.get("payable")
        raw_data = read_sheet_data(sheets_service, spreadsheet_id, "MÃ CTY")
        if not raw_data:
            return pd.DataFrame()

        header_row_idx = None
        for idx, row in enumerate(raw_data):
            if row and str(row[0]).strip() == "STT":
                header_row_idx = idx
                break

        if header_row_idx is None:
            logger.warning("Could not find header row in MÃ CTY")
            return pd.DataFrame()

        header_row = raw_data[header_row_idx]
        data_rows = raw_data[header_row_idx + 1 :]

        max_cols = len(header_row)
        padded_rows = []
        for row in data_rows:
            if len(row) < max_cols:
                row = row + [""] * (max_cols - len(row))
            padded_rows.append(row[:max_cols])

        rows = [header_row] + padded_rows

        col_map = {}
        for idx, col in enumerate(header_row):
            col_clean = col.strip() if col else ""
            if col_clean == "MÃ NCC":
                col_map["MÃ NCC"] = idx
            elif col_clean == "TÊN NCC":
                col_map["TÊN NCC"] = idx
            elif col_clean == "SĐT":
                col_map["SĐT"] = idx
            elif col_clean == "Địa chỉ" or col_clean == "Địa chỉ liên hệ":
                col_map["Địa chỉ"] = idx
            elif col_clean == "Email" or col_clean == "E-mail":
                col_map["Email"] = idx
            elif col_clean == "Mã số thuế" or col_clean == "MST":
                col_map["Mã số thuế"] = idx
            elif col_clean == "Ghi chú" or col_clean == "Ghi chú/Notes":
                col_map["Ghi chú"] = idx

        if "TÊN NCC" not in col_map:
            logger.warning("Required column TÊN NCC not found")
            return pd.DataFrame()

        data = []
        for row in rows[1:]:
            if len(row) <= col_map.get("TÊN NCC", 0):
                continue

            name = str(row[col_map["TÊN NCC"]]).strip()
            if not name:
                continue

            if name.isdigit():
                continue

            if name.upper() in ("TỔNG CỘNG", "NL", "NGƯỜI LẬP", "TYPO", "TRUNGKL"):
                continue

            ma_ncc_col = col_map.get("MÃ NCC")
            ma_ncc_val = ""
            if ma_ncc_col is not None and len(row) > ma_ncc_col:
                ma_ncc_val = str(row[ma_ncc_col]).strip()

            if not ma_ncc_val:
                continue

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
                    if col_map.get("Địa chỉ") is not None
                    and len(row) > col_map["Địa chỉ"]
                    else "",
                    "Mã số thuế": str(row[col_map.get("Mã số thuế", -1)]).strip()
                    if col_map.get("Mã số thuế") is not None
                    and len(row) > col_map["Mã số thuế"]
                    else "",
                    "Ghi chú": str(row[col_map.get("Ghi chú", -1)]).strip()
                    if col_map.get("Ghi chú") is not None
                    and len(row) > col_map["Ghi chú"]
                    else "",
                }
            )

        df = pd.DataFrame(data)
        logger.info(f"Loaded {len(df)} suppliers from MÃ CTY")
        return df

    def _load_tong_hop(self, sheets_service) -> pd.DataFrame:
        from ..utils.data_cleaning import parse_numeric
        from ..modules.google_api import read_sheet_data

        logger.info("Loading TỔNG HỢP...")

        spreadsheet_id = self.spreadsheet_ids.get("payable")
        raw_data = read_sheet_data(sheets_service, spreadsheet_id, "TỔNG HỢP")
        if not raw_data:
            return pd.DataFrame()

        header_row_idx = None
        for idx, row in enumerate(raw_data):
            if row and str(row[0]).strip() == "STT":
                header_row_idx = idx
                break

        if header_row_idx is None:
            logger.warning("Could not find header row in TỔNG HỢP")
            return pd.DataFrame()

        header_row = raw_data[header_row_idx]
        data_rows = raw_data[header_row_idx + 1 :]

        max_cols = len(header_row)
        padded_rows = []
        for row in data_rows:
            if len(row) < max_cols:
                row = row + [""] * (max_cols - len(row))
            padded_rows.append(row[:max_cols])

        rows = [header_row] + padded_rows

        col_map = {}
        for idx, col in enumerate(header_row):
            col_clean = col.strip() if col else ""
            if col_clean == "TÊN NHÀ CUNG CẤP":
                col_map["TÊN NHÀ CUNG CẤP"] = idx
            elif "NỢ CÒN LẠI" in col_clean:
                col_map["NỢ CÒN LẠI"] = idx

        if "TÊN NHÀ CUNG CẤP" not in col_map:
            logger.warning("Required column TÊN NHÀ CUNG CẤP not found in TỔNG HỢP")
            return pd.DataFrame()

        data = []
        for row in rows[1:]:
            if len(row) <= col_map.get("TÊN NHÀ CUNG CẤP", 0):
                continue

            name = str(row[col_map["TÊN NHÀ CUNG CẤP"]]).strip()
            if not name:
                continue

            if name.isdigit():
                continue

            if name.upper() in ("TỔNG CỘNG", "NL", "NGƯỜI LẬP", "TYPO", "TRUNGKL"):
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

    def _load_purchase_transactions(self, staging_dir: Path) -> pd.DataFrame:
        logger.info("Loading purchase transactions from staging...")

        receipt_files = list(staging_dir.glob("Chi tiết nhập*.csv"))
        if not receipt_files:
            logger.warning("No Chi tiết nhập*.csv files found")
            return pd.DataFrame()

        all_data = []
        for f in receipt_files:
            try:
                df = self.cache.get_dataframe(f)
                all_data.append(df)
                logger.info(f"  Loaded {len(df)} rows from {f.name}")
            except Exception as e:
                logger.warning(f"Error reading {f}: {e}")

        if not all_data:
            return pd.DataFrame()

        combined = pd.concat(all_data, ignore_index=True)
        logger.info(f"Total transactions: {len(combined)}")
        return combined
