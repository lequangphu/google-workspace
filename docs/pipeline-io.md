---
globs:
  - 'src/modules/**/*.py'
  - 'src/pipeline/**/*.py'
  - 'src/services/**/*.py'
---

# Pipeline Input/Output Reference

Complete mapping of all scripts, their inputs, outputs, and storage locations.

## Stage 1: Ingest (Google Sheets → CSV)

### Script: `src/modules/ingest.py`

| Aspect | Details |
|--------|---------|
| **Input** | Google Sheets & Google Drive folders (4 raw sources) |
| **Process** | Download data from 7 shared folders + 3 spreadsheets |
| **Outputs** | Multiple CSV files, 1 per sheet/tab |
| **Storage** | `data/00-raw/` (by raw source subdirectories) |

**Output Structure:**
```
data/00-raw/
├── import_export/
│   ├── chi_tiet_nhap_YYYY_MM.csv          [CT.NHAP sheet]
│   ├── chi_tiet_xuat_YYYY_MM.csv          [CT.XUAT sheet]
│   └── xuat_nhap_ton_YYYY_MM.csv          [XNT sheet]
├── receivable/
│   ├── receivable_summary.csv             [TỔNG CÔNG NỢ sheet]
│   └── receivable_customers.csv           [Thong tin KH sheet]
├── payable/
│   ├── payable_master.csv                 [MÃ CTY sheet]
│   └── payable_summary.csv                [TỔNG HỢP sheet]
└── cashflow/
    ├── cashflow_deposits.csv              [Tiền gửi sheet]
    └── cashflow_cash.csv                  [Tien mat sheet]
```

**Configuration:**
- Folder IDs from: `src/modules/ingest.py` lines 33-80 (project_description.md)
- Direct Spreadsheet IDs: Receivable, Payable, CashFlow

---

## Stage 2A: Transform Receipt Data → CSV

### Script: `src/modules/import_export_receipts/clean_receipts_purchase.py`

| Aspect | Details |
|--------|---------|
| **Input** | `data/00-raw/import_export/chi_tiet_nhap_*.csv` |
| **Process** | Clean headers, parse dates, standardize columns, validate |
| **Output** | Single merged CSV with cleaned purchase data |
| **Storage** | `data/01-staging/import_export/chi_tiet_nhap_cleaned.csv` |

**Columns (standardized to KiotViet names):**
- Mã chứng từ, Ngày, Tên nhà cung cấp, Mã hàng, Tên hàng, Số lượng, Đơn giá, Thành tiền

---

### Script: `src/modules/import_export_receipts/clean_receipts_sale.py`

| Aspect | Details |
|--------|---------|
| **Input** | `data/00-raw/import_export/chi_tiet_xuat_*.csv` |
| **Process** | Clean headers, parse dates, standardize columns, validate |
| **Output** | Single merged CSV with cleaned sales data |
| **Storage** | `data/01-staging/import_export/chi_tiet_xuat_cleaned.csv` |

**Columns:**
- Mã chứng từ, Ngày, Tên khách hàng, Mã hàng, Tên hàng, Số lượng, Đơn giá, Thành tiền

---

### Script: `src/modules/import_export_receipts/clean_inventory.py`

| Aspect | Details |
|--------|---------|
| **Input** | `data/00-raw/import_export/xuat_nhap_ton_*.csv` |
| **Process** | Clean inventory data, standardize columns |
| **Output** | Single merged CSV with inventory data |
| **Storage** | `data/01-staging/import_export/xuat_nhap_ton_cleaned.csv` |

---

### Script: `src/modules/receivable/clean_customers.py`

| Aspect | Details |
|--------|---------|
| **Input** | `data/00-raw/receivable/receivable_customers.csv` |
| **Process** | Clean phone numbers, split by semicolon, standardize names |
| **Output** | Cleaned customer list |
| **Storage** | `data/01-staging/receivable/receivable_customers_cleaned.csv` |

---

### Script: `src/modules/receivable/clean_debts.py`

| Aspect | Details |
|--------|---------|
| **Input** | `data/00-raw/receivable/receivable_summary.csv` |
| **Process** | Clean debt amounts, remove empty rows, join with customer IDs |
| **Output** | Cleaned debt summary |
| **Storage** | `data/01-staging/receivable/receivable_debts_cleaned.csv` |

---

## Stage 2B: Extract & Calculate Master Data → CSV

### Script: `src/modules/import_export_receipts/extract_products.py`

| Aspect | Details |
|--------|---------|
| **Inputs** | 1. `data/01-staging/import_export/chi_tiet_nhap_cleaned.csv` |
|  | 2. `data/01-staging/import_export/chi_tiet_xuat_cleaned.csv` |
|  | (Reads raw data for FIFO calculations) |
| **Process** | Group by product, assign sequential codes, calculate inventory/prices/profit |
| **Outputs** | 7 CSV files with different aggregations |
| **Storage** | `data/02-validated/` |

**Output Files:**
```
data/02-validated/
├── product_info.csv              [Mã hàng mới, Mã hàng, Tên hàng]
├── inventory.csv                 [Mã hàng mới, Tồn số lượng, Giá vốn FIFO]
├── summary_purchase.csv          [Mã hàng mới, Tổng số lượng nhập, Tổng thành tiền nhập]
├── summary_sale.csv              [Mã hàng mới, Tổng số lượng xuất, Tổng thành tiền xuất]
├── price_purchase.csv            [Mã hàng mới, Giá nhập đầu, Giá nhập cuối, Ngày...]
├── price_sale.csv                [Mã hàng mới, Giá xuất đầu, Giá xuất cuối (max), Ngày...]
└── gross_profit.csv              [Mã hàng mới, Tổng doanh thu, Giá vốn FIFO, Lãi gộp, Biên lãi %]
```

**Key Columns Mapping:**
- `Mã hàng mới` — Generated sequential code (HH000001, HH000002...)
- `Mã hàng` — Original product code from CT.NHAP
- `Giá xuất cuối` — Max selling price (used as "Giá bán" in template)
- `Giá vốn FIFO` — FIFO-weighted unit cost (used as "Giá vốn" in template)
- `Tồn số lượng` — Current stock = Total Purchased − Total Sold

---

### Script: `src/modules/receivable/extract_customer_ids.py`

| Aspect | Details |
|--------|---------|
| **Input** | `data/01-staging/import_export/chi_tiet_xuat_cleaned.csv` |
| **Process** | Group by customer, rank by date + amount, assign sequential IDs |
| **Output** | Customer list with generated IDs |
| **Storage** | `data/01-staging/receivable/` |

**Columns:**
- Mã khách hàng (KH000001...), Tên khách hàng, Ngày giao dịch đầu, Ngày giao dịch cuối, Tổng tiền bán

---

## Stage 3: Enrich & Map to Templates

### (Missing) Script: `src/erp/exporter.py` ❌

| Aspect | Details |
|--------|---------|
| **Inputs** | 1. `data/02-validated/product_info.csv` |
|  | 2. `data/02-validated/inventory.csv` |
|  | 3. `data/02-validated/price_sale.csv` |
|  | 4. Google Sheet: Product Enrichment (Nhóm hàng, Thương hiệu) |
|  | 5. `data/templates/MauFileSanPham.xlsx` (template) |
| **Process** | Merge CSVs, enrich from Google Sheet, map to 9 template columns |
| **Output** | XLSX file with 9 columns filled, 18 empty |
| **Storage** | `data/03-erp-export/Products.xlsx` |

**Template Columns Filled (9 of 27):**
1. Loại hàng ← "Hàng hóa" (constant)
2. Nhóm hàng(3 Cấp) ← Google Sheet enrichment
3. Mã hàng ← product_info.csv (HH000001...)
4. Mã vạch ← product_info.csv (original code)
5. Tên hàng ← product_info.csv
6. Thương hiệu ← Google Sheet enrichment
7. Giá bán ← price_sale.csv (max selling price)
8. Giá vốn ← inventory.csv (FIFO unit cost)
9. Tồn kho ← inventory.csv (current stock)

**Columns Left Empty (18):**
- Tồn nhỏ nhất, Tồn lớn nhất, ĐVT, Mã ĐVT Cơ bản, Quy đổi, Thuộc tính, Mã HH Liên quan, Hình ảnh, Sử dụng Imei, Trọng lượng, Đang kinh doanh, Được bán trực tiếp, Mô tả, Mẫu ghi chú, Vị trí, Hàng thành phần, Bảo hành, Bảo trì định kỳ

---

## Data Directory Structure (Complete)

```
data/
├── 00-raw/                      [Ingest output]
│   ├── import_export/           
│   │   ├── chi_tiet_nhap_2024_01.csv
│   │   ├── chi_tiet_xuat_2024_01.csv
│   │   └── xuat_nhap_ton_2024_01.csv
│   ├── receivable/
│   │   ├── receivable_summary.csv
│   │   └── receivable_customers.csv
│   ├── payable/
│   │   ├── payable_master.csv
│   │   └── payable_summary.csv
│   └── cashflow/
│       ├── cashflow_deposits.csv
│       └── cashflow_cash.csv
│
├── 01-staging/                  [Transform output]
│   ├── import_export/
│   │   ├── chi_tiet_nhap_cleaned.csv
│   │   ├── chi_tiet_xuat_cleaned.csv
│   │   └── xuat_nhap_ton_cleaned.csv
│   └── receivable/
│       ├── receivable_customers_cleaned.csv
│       ├── receivable_debts_cleaned.csv
│       └── extract_customer_ids.csv
│
├── 02-validated/                [Extract output - products only]
│   ├── product_info.csv
│   ├── inventory.csv
│   ├── summary_purchase.csv
│   ├── summary_sale.csv
│   ├── price_purchase.csv
│   ├── price_sale.csv
│   └── gross_profit.csv
│
├── 03-erp-export/               [Final XLSX exports]
│   ├── Products.xlsx            ← extract_products.py → exporter.py
│   ├── Customers.xlsx
│   ├── Suppliers.xlsx
│   └── PriceBook.xlsx
│
├── templates/
│   ├── MauFileSanPham.xlsx      [27-column Products template]
│   ├── MauFileKhachHang.xlsx    [20-column Customers template]
│   ├── MauFileNhaCungCap.xlsx   [15-column Suppliers template]
│   └── MauFileBangGia.xlsx      [5-column PriceBook template]
│
└── .drive_manifest.json        [Google Drive folder→sheet cache]
```

---

## External Data Sources (Enrichment)

### Product Enrichment Sheet
- **URL:** https://docs.google.com/spreadsheets/d/16bGN2gjWspCqlFD4xB--7WtkYtTpDaWzRQx9sV97ed8
- **Spreadsheet ID:** `16bGN2gjWspCqlFD4xB--7WtkYtTpDaWzRQx9sV97ed8`
- **Sheet gid:** `23224859`
- **Columns:** Mã hàng, Nhóm hàng(3 Cấp), Thương hiệu
- **Used by:** `exporter.py` (enrich product_info.csv with brand & category)

---

## Pipeline Flow Summary

```
INGEST (Google Sheets → CSV)
    ↓
data/00-raw/
    ├── import_export/chi_tiet_nhap_*.csv
    ├── import_export/chi_tiet_xuat_*.csv
    └── [receivable, payable, cashflow]
    
TRANSFORM (CSV → Clean CSV)
    ↓
data/01-staging/
    ├── import_export/chi_tiet_nhap_cleaned.csv
    ├── import_export/chi_tiet_xuat_cleaned.csv
    └── receivable/extract_customer_ids.csv
    
EXTRACT (Aggregate → Data Lake)
    ↓
data/02-validated/
    ├── product_info.csv
    ├── inventory.csv
    ├── price_sale.csv
    └── [+ 4 more reports]
    
ENRICH + EXPORT (CSV + Template → XLSX)
    ↓
data/03-erp-export/
    └── Products.xlsx  ← Ready for KiotViet import
```

---

## Testing Data

All scripts tested on **real CSV files** from `data/00-raw/` (not mock data).

**Test Commands:**
```bash
# Run all tests
uv run pytest tests/ -v

# Run specific module tests
uv run pytest tests/test_import_export_receipts_*.py -v
uv run pytest tests/test_receivable_*.py -v

# Run specific script directly
uv run src/modules/import_export_receipts/extract_products.py
```

**Test Coverage:** 153 tests passing, 1 skipped (see refactoring-roadmap.md)
