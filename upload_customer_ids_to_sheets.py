# -*- coding: utf-8 -*-
"""Upload customer IDs to Google Sheets in chunks."""

import csv
import json
from pathlib import Path

# Read all data from CSV
csv_file = Path.cwd() / "data" / "final" / "Mã khách hàng mới.csv"
with open(csv_file, "r", encoding="utf-8") as f:
    reader = csv.reader(f)
    rows = list(reader)

# Convert all to strings
data = [[str(cell) for cell in row] for row in rows]

print(f"Total rows: {len(data)} (including header)")
print(f"Rows to process: {len(data) - 1} (excluding header)")

# Output chunk information
chunk_size = 100
for i in range(1, 13):  # Chunks 1-12 (0 already uploaded)
    start_idx = i * chunk_size
    end_idx = min((i + 1) * chunk_size, len(data))
    
    if start_idx >= len(data):
        break
    
    chunk = data[start_idx:end_idx]
    start_row = start_idx + 1
    end_row = end_idx
    
    print(f"\nChunk {i}:")
    print(f"  Range: A{start_row}:F{end_row} (rows {start_row}-{end_row})")
    print(f"  Rows: {len(chunk)}")
    print(f"  First: {chunk[0][0]} - {chunk[0][1]}")
    print(f"  Last: {chunk[-1][0]} - {chunk[-1][1]}")
    
    # Save as JSON for reference
    output_file = Path("/tmp") / f"chunk_{i}_info.txt"
    with open(output_file, "w") as f:
        f.write(f"Range: Mã khách hàng mới!A{start_row}:F{end_row}\n")
        f.write(f"Rows: {len(chunk)}\n")
        f.write(f"First customer: {chunk[0][0]} - {chunk[0][1]}\n")
        f.write(f"Last customer: {chunk[-1][0]} - {chunk[-1][1]}\n")
EOF
