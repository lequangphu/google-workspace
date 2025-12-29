# -*- coding: utf-8 -*-
"""Data lineage tracking for audit trail.

Module: lineage
Purpose: Track every row through the pipeline (success or rejection)
Implementation: ADR-5 (Data Lineage Tracking - Mandatory)

This module provides the DataLineage class for tracking:
- Source file and row index
- Output row index (or None if rejected)
- Operation performed
- Status (success or rejection reason)
- Timestamp for audit trail
"""

import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class DataLineage:
    """Track row-level transformations for audit trail.

    Every row processed must be tracked with:
    - Source file and row number
    - Output row number (or None if rejected)
    - Operation name
    - Status (success or rejection reason)
    """

    def __init__(self, output_dir: Path):
        """Initialize lineage tracker.

        Args:
            output_dir: Directory where lineage CSV will be saved
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.entries: List[Dict] = []
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    def track(
        self,
        source_file: str,
        source_row: int,
        output_row: Optional[int],
        operation: str,
        status: str,
    ) -> None:
        """Track a single row transformation.

        Args:
            source_file: Name of source CSV file
            source_row: Row index in source file (0-based)
            output_row: Row index in output dataframe (or None if rejected)
            operation: Name of operation (e.g., "clean_receipts_purchase")
            status: "success" or "rejected: <reason>"
        """
        entry = {
            "source_file": source_file,
            "source_row": source_row,
            "output_row": output_row if output_row is not None else "REJECTED",
            "operation": operation,
            "status": status,
            "timestamp": datetime.now().isoformat(),
        }
        self.entries.append(entry)

    def save(self) -> Path:
        """Save lineage entries to CSV file.

        Returns:
            Path: Path to saved lineage CSV file
        """
        if not self.entries:
            logger.warning("No lineage entries to save")
            return None

        lineage_filename = f"lineage_{self.timestamp}.csv"
        lineage_filepath = self.output_dir / lineage_filename

        try:
            with open(lineage_filepath, "w", newline="", encoding="utf-8") as f:
                fieldnames = [
                    "source_file",
                    "source_row",
                    "output_row",
                    "operation",
                    "status",
                    "timestamp",
                ]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(self.entries)

            logger.info(f"Lineage saved to: {lineage_filepath}")
            return lineage_filepath
        except Exception as e:
            logger.error(f"Failed to save lineage: {e}")
            raise

    def summary(self) -> Dict:
        """Get summary statistics of lineage.

        Returns:
            Dict with keys: total, success, rejected
        """
        total = len(self.entries)
        success = sum(
            1 for entry in self.entries if entry["status"] == "success"
        )
        rejected = total - success

        return {
            "total": total,
            "success": success,
            "rejected": rejected,
            "success_rate": (success / total * 100) if total > 0 else 0,
        }
