# Import/Export Receipts Module - AI Agent Knowledge Base

**Module**: `src/modules/import_export_receipts/`
**Focus**: Products, PriceBook, inventory reconciliation, FIFO costing

## OVERVIEW
Handles purchase (CT.NHAP) and sale (CT.XUAT) receipts, inventory movement (XNT), and Products.xlsx generation with multi-month consolidation and Vietnamese column handling.

## STRUCTURE
```
import_export_receipts/
 ├── clean_inventory.py                   # XNT multi-month consolidation, profit margins
 ├── clean_receipts_purchase.py          # CT.NHAP parsing, multi-warehouse handling
 ├── clean_receipts_sale.py               # CT.XUAT parsing, quantity aggregation
 ├── extract_attributes.py                # Product name → dimension/tire type
 ├── generate_products_xlsx.py          # Products.xlsx, Google Sheets enrichment
 ├── upload_cleaned_to_sheets.py        # Upload staging files back to Google Sheets
 └── extract_products.py                  # DEPRECATED (use generate_products_xlsx.py)
```

## WHERE TO LOOK
| Task | File | Notes |
|------|------|-------|
| Inventory consolidation | `clean_inventory.py` | Groups XNT files by header, `combine_headers()` handles 2-level Vietnamese columns |
| Purchase receipts | `clean_receipts_purchase.py` | 5-warehouse columns (Kho 1-5), keeps Kho 1 only (~9.6% loss expected) |
| Sale receipts | `clean_receipts_sale.py` | Combines Bán lẻ + Bán sì quantities |
| FIFO costing | `generate_products_xlsx.py:get_latest_inventory()` | Uses Đơn giá cuối kỳ as Giá vốn (latest month per product) |
| Vietnamese columns | Any `*_clean_*.py` | Multi-level headers: `combine_headers()` in each file |

## CONVENTIONS

### FIFO Costing
```python
# From get_latest_inventory() - latest month per product
latest = combined.loc[combined.groupby("Mã hàng")["Ngày"].idxmax()]
# Uses: Đơn giá cuối kỳ (latest) as Giá vốn
```

### Multi-Month Consolidation
- XNT files: `YYYY_M_XNT.csv` → consolidated by header structure (`group_files_by_headers()`)
- Receipts: `YYYY_M_CT.NHAP.csv` → merged, sorted by date
- Inventory: `xuat_nhap_ton_YYYY_MM_YYYY_MM.csv` (min/max months in filename)

### Reconciliation Checkpoints
Every `*_clean_*.py` generates `reconciliation_report_<script>.json`:
- Input vs output quantities
- File-by-file dropout breakdown
- Alerts if dropout > threshold (5-20% depending on script)

### Vietnamese Column Handling
```python
# Pattern in clean_inventory.py, clean_receipts_purchase.py, clean_receipts_sale.py
def combine_headers(header_row_1, header_row_2):
    # Combines parent-child headers (e.g., "TỒN_ĐẦU_KỲ_S_LƯỢNG")
    # Preserves Vietnamese, replaces spaces with underscores
```

## ANTI-PATTERNS

### Data Loss
- ❌ Ignore reconciliation dropout warnings → Check `reconciliation_report_*.json`
- ❌ Expect 100% quantity retention → ~9.6% loss from multi-warehouse filtering (Kho 1 only)

### Column Mapping
- ❌ Hardcode column indices → Read from `HEADER_COLUMN_MAP` (purchase) or detect dynamically
- ❌ Assume single header row → Files have 2-level headers requiring `combine_headers()`

### Product Code
- ❌ Use extract_products.py (deprecated) → Use `generate_products_xlsx.py`
- ❌ Generate HH##### codes → Modern code: `SPC` + original code (see `generate_products_xlsx.py`)
