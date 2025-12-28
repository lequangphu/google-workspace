# Legacy Scripts

These scripts have been migrated to the new modular structure in `src/modules/` and `src/pipeline/`.

## Migration Status

| Legacy Script | New Location | Status |
|---|---|---|
| `ingest.py` | `src/modules/ingest.py` + `src/modules/google_api.py` | ✅ Migrated |
| `clean_chung_tu_nhap.py` | `src/modules/import_export_receipts/clean_receipts_purchase.py` | ✅ Migrated (T-019b5f16) |
| `clean_chung_tu_xuat.py` | `src/modules/import_export_receipts/clean_receipts_sale.py` | ✅ Migrated (T-019b5f24) |
| `clean_xuat_nhap_ton.py` | `src/modules/import_export_receipts/clean_inventory.py` | ✅ Migrated (T-019b5f24) |
| `generate_product_info.py` | `src/modules/import_export_receipts/extract_products.py` | ✅ Migrated (T-019b5f24) |
| `clean_thong_tin_khach_hang.py` | `src/modules/receivable/clean_customers.py` | ✅ Migrated (T-019b5f24) |
| `generate_new_customer_id.py` | `src/modules/receivable/extract_customer_ids.py` | ⏳ Pending Migration |
| `clean_tong_no.py` | `src/modules/receivable/` (deprecated, analyze usage) | ⏳ Pending Analysis |
| `pipeline.py` | `src/pipeline/orchestrator.py` | ⏳ Pending Migration |

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
