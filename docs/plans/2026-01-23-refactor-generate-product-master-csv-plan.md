---
title: Create Master Product CSV with Revenue and Inventory Values
type: refactor
date: 2026-01-23
---

# Create Master Product CSV with Revenue and Inventory Values

## Overview

Refactor `src/modules/import_export_receipts/refine_product_master.py` to create a standalone master product CSV that combines unique Mã hàng from all 3 sources (Chi tiết nhập, Chi tiết xuất, Xuất nhập tồn), enriches with aggregated metrics (2025 revenue and ending inventory value), and saves to a dedicated folder under `data/01-staging/`.

## Problem Statement

The current `refine_product_master.py` extracts and cleans product data but:
- Does not produce a standalone master product CSV file
- Does not include aggregated metrics (revenue, inventory values)
- Does not provide a clean reference dataset for downstream processing

## Proposed Solution

Create a new script `generate_product_master.py` in `src/modules/import_export_receipts/` that:

1. **Load 3 source files** from `data/01-staging/import_export/`:
   - `Chi tiết nhập YYYY-MM_YYYY-MM.csv`
   - `Chi tiết xuất YYYY-MM_YYYY-MM.csv`
   - `Xuất nhập tồn YYYY_MM_YYYY_MM.csv`

2. **Extract unique products** (Mã hàng, Tên hàng) from union of all 3 sources

3. **Calculate metrics**:
   - `Doanh thu 2025` = sum of `Thành tiền` from Chi tiết xuất where `Năm` == 2025
   - `Giá trị cuối kỳ 2025` = `Giá trị cuối kỳ` from Xuất nhập tồn December 2025 row

4. **Apply cleaning** on the master product data (existing cleaning functions)

5. **Sort** by `Doanh thu 2025` DESC, then `Giá trị cuối kỳ 2025` DESC

6. **Save to** `data/01-staging/master_products/master_products.csv`

7. **Integrate** into orchestrator pipeline with `-t -m ier` flag

## Technical Approach

### New Script: `src/modules/import_export_receipts/generate_product_master.py`

```python
# -*- coding: utf-8 -*-
"""Generate master product CSV with aggregated metrics.

Creates a standalone master product reference file with:
- Unique Mã hàng from all 3 sources (nhap, xuat, inventory)
- Cleaned Tên hàng
- Doanh thu 2025 (sum of Thành tiền from Chi tiết xuất 2025)
- Giá trị cuối kỳ 2025 (from Xuất nhập tồn Dec 2025)
"""

import pandas as pd
from pathlib import Path

from src.utils.staging_cache import StagingCache

def load_source_files(staging_dir: Path) -> dict:
    """Load Chi tiết nhập, Chi tiết xuất, Xuất nhập tồn."""
    nhap_file = staging_dir / "Chi tiết nhập 2020-04_2025-12.csv"
    xuat_file = staging_dir / "Chi tiết xuất 2020-04_2025-12.csv"
    xnt_file = staging_dir / "Xuất nhập tồn 2020_04_2025_12.csv"
    
    return {
        "nhap": StagingCache.get_dataframe(nhap_file),
        "xuat": StagingCache.get_dataframe(xuat_file),
        "xnt": StagingCache.get_dataframe(xnt_file),
    }

def extract_unique_products(files: dict) -> pd.DataFrame:
    """Extract unique (Mã hàng, Tên hàng) from all 3 sources."""
    product_refs = []
    
    for source_name, df in files.items():
        if "Mã hàng" in df.columns:
            temp = df[["Mã hàng", "Tên hàng"]].drop_duplicates()
            temp["_sources"] = source_name
            product_refs.append(temp)
    
    master = pd.concat(product_refs, ignore_index=True)
    
    # Deduplicate by Mã hàng, keeping first Tên hàng
    master = master.drop_duplicates(subset=["Mã hàng"], keep="first")
    
    return master[["Mã hàng", "Tên hàng", "_sources"]]

def calculate_2025_revenue(xuat_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate Doanh thu 2025 per product."""
    xuat_2025 = xuat_df[xuat_df["Năm"] == 2025]
    
    revenue = (
        xuat_2025.groupby("Mã hàng", dropna=False)["Thành tiền"]
        .sum()
        .reset_index()
    )
    revenue.columns = ["Mã hàng", "Doanh thu 2025"]
    
    return revenue

def get_december_2025_inventory_value(xnt_df: pd.DataFrame) -> pd.DataFrame:
    """Get Giá trị cuối kỳ 2025 from December row."""
    dec_2025 = xnt_df[(xnt_df["Năm"] == 2025) & (xnt_df["Tháng"] == 12)]
    
    if dec_2025.empty:
        return pd.DataFrame(columns=["Mã hàng", "Giá trị cuối kỳ 2025"])
    
    result = dec_2025[["Mã hàng", "Giá trị cuối kỳ 2025"]].copy()
    return result

def process(staging_dir: Optional[Path] = None) -> Path:
    """Generate master product CSV.
    
    Returns:
        Path to generated master CSV
    """
    if staging_dir is None:
        staging_dir = Path("data/01-staging/import_export")
    
    # Step 1: Load source files
    files = load_source_files(staging_dir)
    
    # Step 2: Extract unique products
    master = extract_unique_products(files)
    
    # Step 3: Calculate Doanh thu 2025
    revenue = calculate_2025_revenue(files["xuat"])
    master = master.merge(revenue, on="Mã hàng", how="left")
    
    # Step 4: Get Giá trị cuối kỳ 2025
    inventory_value = get_december_2025_inventory_value(files["xnt"])
    master = master.merge(inventory_value, on="Mã hàng", how="left")
    
    # Step 5: Apply cleaning (reuse existing functions)
    from .refine_product_master import clean_product_names_in_master
    master = clean_product_names_in_master(master)
    
    # Step 6: Sort by revenue and inventory value descending
    master = master.sort_values(
        by=["Doanh thu 2025", "Giá trị cuối kỳ 2025"],
        ascending=[False, False]
    )
    
    # Step 7: Save to master_products directory
    output_dir = Path("data/01-staging/master_products")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_path = output_dir / "master_products.csv"
    master.to_csv(output_path, index=False, encoding="utf-8")
    
    return output_path
```

### Modified File: `src/pipeline/orchestrator.py`

Add `generate_product_master.py` to TRANSFORM_MODULES_LEGACY:

```python
TRANSFORM_MODULES_LEGACY = {
    "import_export_receipts": [
        "clean_inventory.py",
        "clean_receipts_purchase.py",
        "clean_receipts_sale.py",
        "refine_product_master.py",
        "generate_product_master.py",  # NEW
    ],
    # ...
}
```

### New Directory Structure

```
data/01-staging/
├── import_export/
│   ├── Chi tiết nhập 2020-04_2025-12.csv
│   ├── Chi tiết xuất 2020-04_2025-12.csv
│   ├── Xuất nhập tồn 2020_04_2025_12.csv
│   └── master_products/         # NEW
│       └── master_products.csv
```

## Acceptance Criteria

- [ ] Script `generate_product_master.py` exists in `src/modules/import_export_receipts/`
- [ ] Master CSV has columns: `Mã hàng`, `Tên hàng`, `_sources`, `Doanh thu 2025`, `Giá trị cuối kỳ 2025`
- [ ] `Doanh thu 2025` correctly sums `Thành tiền` from Chi tiết xuất 2025
- [ ] `Giá trị cuối kỳ 2025` correctly sourced from Xuất nhập tồn December 2025
- [ ] Data sorted by revenue DESC, inventory value DESC
- [ ] Output saved to `data/01-staging/master_products/master_products.csv`
- [ ] Script runs successfully with `uv run src/pipeline/orchestrator.py -t -m ier`
- [ ] Master products count matches overlap analysis (1,289 unique products)

## Dependencies & Risks

| Dependency | Status |
|------------|--------|
| `refine_product_master.py` cleaning functions | Existing, reused |
| `StagingCache` for file reading | Existing, reused |
| Source files exist in staging | Confirmed from overlap analysis |

| Risk | Mitigation |
|------|------------|
| December 2025 row missing in Xuất nhập tồn | Handle empty result with NaN |
| Products with no 2025 revenue | Left join preserves all products |
| Duplicate Mã hàng with different names | Keep first (most recent) name |

## Implementation Phases

### Phase 1: Core Script
- Create `generate_product_master.py`
- Implement file loading and product extraction
- Add revenue and inventory aggregations

### Phase 2: Integration
- Update `orchestrator.py` TRANSFORM_MODULES_LEGACY
- Test with `-t -m ier` flag
- Verify output file and data quality

### Phase 3: Polish
- Add logging
- Generate summary statistics
- Add unit tests

## Success Metrics

| Metric | Target |
|--------|--------|
| Unique products in master CSV | 1,289 (from overlap analysis) |
| Products with 2025 revenue | Count > 0 |
| Script execution time | < 10 seconds |
| File size | < 2 MB |

## References & Research

### Internal References
- `src/modules/import_export_receipts/refine_product_master.py:94-134` - extract_product_references pattern
- `src/modules/import_export_receipts/refine_product_master.py:137-170` - clean_product_names_in_master
- `src/pipeline/orchestrator.py:86-99` - TRANSFORM_MODULES_LEGACY structure

### External References
- Aggregation patterns: `generate_customers_xlsx.py:275-304`, `generate_suppliers_xlsx.py:277-305`
- DataFrame merge: `pandas.DataFrame.merge()` with left join for preservation

## Future Considerations

- Add incremental update capability for new data periods
- Include historical revenue tracking (2024, 2023, etc.)
- Add product categorization from ERP templates
- Generate product hierarchy (brand, model, size)
