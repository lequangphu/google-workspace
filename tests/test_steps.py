from src.modules.import_export_receipts.clean_product_names import (
    standardize_product_type,
)

# Test each step individually
name = "70/90-17 TT (N)"
print(f"Input: {repr(name)}")

# Step 1: T/L normalization
name = re.sub(r"\bT\s+L\b", "T/L", name, flags=re.IGNORECASE)
name = re.sub(r"\bTL\b", "T/L", name, flags=re.IGNORECASE)
print(f"After T/L: {repr(name)}")

# Step 2: TT to T/T
name = re.sub(r"\bTT\b(?!\w)", "T/T", name)
print(f"After TT: {repr(name)}")

# Step 3: PR spacing
name = re.sub(r"(\d+)\s+PR\b", r"\1PR", name)
print(f"After PR: {repr(name)}")

# Step 4: Region codes
name = re.sub(
    r"\((\s*[A-ZÀ-Ỹ]+(?:,\s*[A-ZÀ-Ỹ]+)\s*)\)",
    lambda m: f"-{m.group(1).replace(', ', '/').replace(',', '/').replace(' ', '')}",
    name,
)
print(f"After region: {repr(name)}")
print(f"Final: {repr(name)}")
print()
