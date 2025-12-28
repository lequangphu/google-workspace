"""Import/Export Receipts module.

Raw source: XUẤT NHẬP TỒN TỔNG T* (Transaction records from Google Drive)
Output modules: Products, PriceBook

Processes:
- CT.NHAP (purchase receipts) → clean_receipts_purchase.py
- CT.XUAT (sales receipts) → clean_receipts_sale.py
- XNT (inventory) → clean_inventory.py
- Extract products → extract_products.py
"""
