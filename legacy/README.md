# Legacy Scripts

These scripts have been migrated to the new modular structure in `src/modules/` and `src/pipeline/`.

## Migration Status

| Legacy Script | New Location | Status |
|---|---|---|
| `ingest.py` | `src/modules/ingest.py` + `src/modules/google_api.py` | ✅ Migrated |
| `clean_chung_tu_nhap.py` | `src/modules/import_export_receipts/clean_receipts_purchase.py` | Pending |
| `clean_chung_tu_xuat.py` | `src/modules/import_export_receipts/clean_receipts_sale.py` | Pending |
| `clean_xuat_nhap_ton.py` | `src/modules/import_export_receipts/clean_inventory.py` | Pending |
| `generate_product_info.py` | `src/modules/import_export_receipts/extract_products.py` | Pending |
| `clean_thong_tin_khach_hang.py` | `src/modules/receivable/clean_customers.py` | Pending |
| `generate_new_customer_id.py` | `src/modules/receivable/extract_customer_ids.py` | Pending |
| `clean_tong_no.py` | `src/modules/receivable/` (deprecated, analyze usage) | Pending |
| `pipeline.py` | `src/pipeline/orchestrator.py` | Pending |

## Using Migrated Scripts

Instead of:
```python
# Old way (legacy/)
from ingest import ingest_from_drive
```

Use:
```python
# New way (src/modules/)
from src.modules.ingest import ingest_from_drive
```

See `docs/refactoring-roadmap.md` for details.
