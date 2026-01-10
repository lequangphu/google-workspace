# -*- coding: utf-8 -*-
"""Tests for src/modules/import_export_receipts/product_disambiguation.py"""

import pandas as pd
import pytest
from datetime import datetime

from src.modules.import_export_receipts.product_disambiguation import (
    BRAND_UNIFICATION,
    SIMILARITY_THRESHOLD,
    unify_brand_name,
    get_similarity,
    check_pairwise_similarity,
    group_similar_names,
    disambiguate_product_codes,
)


class test_unify_brand_name:
    """Test brand name unification function."""

    def test_globe_from_lobe(self):
        """LOBE should be unified to GLOBE."""
        assert unify_brand_name("BÌNH LOBE DREAM L1") == "GLOBE"

    def test_globe_from_globe(self):
        """GLOBE should remain GLOBE."""
        assert unify_brand_name("BÌNH GLOBE DREAM L1") == "GLOBE"

    def test_inoue_from_inu(self):
        """INU should be unified to INOUE."""
        assert unify_brand_name("LỐP INU 90/90-14") == "INOUE"

    def test_inoue_from_inou(self):
        """INOU should be unified to INOUE."""
        assert unify_brand_name("LỐP INOU 90/90-14") == "INOUE"

    def test_chengshin_from_chengsin(self):
        """CHENGSIN should be unified to CHENGSHIN."""
        assert unify_brand_name("VỎ CHENGSIN 100/90-18") == "CHENGSHIN"

    def test_chengshin_from_chengsin(self):
        """CHENGSIN should be unified to CHENGSHIN."""
        assert unify_brand_name("VỎ CHENGSIN 100/90-18") == "CHENGSHIN"

    def test_camel_from_cheetah(self):
        """CHEETAH should be unified to CAMEL."""
        assert unify_brand_name("LỐP CHEETAH 120/70-14") == "CAMEL"

    def test_no_brand(self):
        """Empty string for names without known brand."""
        assert unify_brand_name("LỐP XE MÁY 100/90-10") == ""

    def test_case_insensitive(self):
        """Brand matching should be case insensitive."""
        assert unify_brand_name("lobe") == "GLOBE"
        assert unify_brand_name("LOBE") == "GLOBE"


class TestGetSimilarity:
    """Test similarity calculation function."""

    def test_identical_names(self):
        """Identical names should have similarity 1.0."""
        assert get_similarity("LỐP A", "LỐP A") == 1.0

    def test_similar_names(self):
        """Similar names should have high similarity."""
        assert (
            get_similarity("BÌNH DREAM L1", "BÌNH LOBE DREAM L1")
            >= SIMILARITY_THRESHOLD
        )

    def test_different_brands(self):
        """Different brands should have lower similarity."""
        name1 = "BÌNH XE ĐIỆN DURAMOTO 12V-15AH"
        name2 = "BÌNH XE ĐIỆN VTOKY 12V-14AH"
        assert get_similarity(name1, name2) < SIMILARITY_THRESHOLD

    def test_similarity_symmetric(self):
        """Similarity should be symmetric."""
        assert get_similarity("LỐP A", "LỐP B") == get_similarity("LỐP B", "LỐP A")

    def test_empty_names(self):
        """Empty names should have similarity 0.0."""
        assert (
            get_similarity("", "") == 1.0
        )  # SequenceMatcher returns 1.0 for identical
        assert get_similarity("a", "") < 1.0


class TestCheckPairwiseSimilarity:
    """Test pairwise similarity matrix generation."""

    def test_two_names(self):
        """Test with two names."""
        names = ["LỐP A", "LỐP B"]
        matrix = check_pairwise_similarity(names)
        assert (0, 1) in matrix
        assert isinstance(matrix[(0, 1)], float)
        assert 0 <= matrix[(0, 1)] <= 1

    def test_three_names(self):
        """Test with three names."""
        names = ["LỐP A", "LỐP B", "LỐP C"]
        matrix = check_pairwise_similarity(names)
        assert len(matrix) == 3  # C(3,2)
        assert isinstance(matrix[(0, 1)], float)
        assert isinstance(matrix[(1, 2)], float)

    def test_single_name(self):
        """Test with single name."""
        names = ["LỐP A"]
        matrix = check_pairwise_similarity(names)
        assert len(matrix) == 0

    def test_empty_list(self):
        """Test with empty list."""
        matrix = check_pairwise_similarity([])
        assert len(matrix) == 0


class TestGroupSimilarNames:
    """Test grouping similar names."""

    def test_all_similar(self):
        """All similar names should be in one group."""
        names = [
            "BÌNH DREAM L1",
            "BÌNH LOBE DREAM L1",
            "BÌNH GLOBE DREAM L1",
        ]
        groups = group_similar_names(names)
        assert len(groups) == 1
        assert len(groups[0]) == 3

    def test_all_different(self):
        """Different names should be in separate groups."""
        names = ["LỐP A", "LỐP B"]
        groups = group_similar_names(names)
        assert len(groups) == 2
        assert len(groups[0]) == 1
        assert len(groups[1]) == 1

    def test_mixed_similarity(self):
        """Test with mixed similarity."""
        names = [
            "LỐP A",  # Similar to B
            "LỐP B",  # Similar to A
            "LỐP C",  # Different from A,B
        ]
        groups = group_similar_names(names, SIMILARITY_THRESHOLD)
        assert len(groups) >= 2

    def test_single_name_grouping(self):
        """Single name should return one group with index 0."""
        names = ["LỐP A"]
        groups = group_similar_names(names)
        assert len(groups) == 1
        assert groups[0] == [0]

    def test_empty_list(self):
        """Empty list should return empty list."""
        groups = group_similar_names([])
        assert groups == []


class TestDisambiguateProductCodes:
    """Test main disambiguation function."""

    def test_empty_dataframe(self):
        """Empty DataFrame should return unchanged."""
        df = pd.DataFrame()
        result, stats = disambiguate_product_codes(df)
        assert result.empty
        assert stats["codes_processed"] == 0
        assert stats["normalized"] == 0
        assert stats["suffixed"] == 0

    def test_single_name_per_code(self):
        """Single name per code should not change."""
        df = pd.DataFrame(
            {
                "Mã hàng": ["A", "B"],
                "Tên hàng": ["LỐP A", "LỐP B"],
                "Số lượng": [10, 20],
            }
        )
        result, stats = disambiguate_product_codes(df)
        assert len(result) == 2
        assert stats["codes_processed"] == 0
        assert stats["normalized"] == 0
        assert stats["suffixed"] == 0

    def test_normalize_similar_names(self):
        """Similar names should be normalized to newest name."""
        df = pd.DataFrame(
            {
                "Mã hàng": ["06136A", "06136A", "06136A"],
                "Tên hàng": [
                    "BÌNH DREAM L1 (WP5S-3BP)",
                    "BÌNH LOBE DREAM L1 (WP5S-3BP)",
                    "BÌNH GLOBE DREAM L1 (WP5S-3BP) (10b/THÙNG)",
                ],
                "Ngày": ["2024-01-05", "2024-02-10", "2024-03-15"],
                "Số lượng": [5, 10, 15],
            }
        )
        result, stats = disambiguate_product_codes(df)
        assert len(result) == 1
        assert stats["codes_processed"] == 1
        assert stats["normalized"] == 3
        # All 3 records normalized to newest name
        assert all(
            name == "BÌNH GLOBE DREAM L1 (WP5S-3BP) (10b/THÙNG)"
            for name in result["Tên hàng"]
        )

    def test_suffix_different_products(self):
        """Different products should be suffixed."""
        df = pd.DataFrame(
            {
                "Mã hàng": ["0612D", "0612D"],
                "Tên hàng": [
                    "BÌNH XE ĐIỆN DURAMOTO 12V-15AH",
                    "BÌNH XE ĐIỆN VTOKY 12V-14AH",
                ],
                "Ngày": ["2024-01-01", "2024-01-15"],
                "Số lượng": [5, 10],
            }
        )
        result, stats = disambiguate_product_codes(df)
        assert len(result) == 2
        assert stats["codes_processed"] == 1
        assert stats["suffixed"] == 2
        # Two different products → suffix applied
        assert "0612D-01" in result["Mã hàng"].values
        assert "0612D" in result["Mã hàng"].values

    def test_preserves_other_columns(self):
        """Other columns should be preserved."""
        df = pd.DataFrame(
            {
                "Mã hàng": ["A"],
                "Tên hàng": ["LỐP A", "LỐP B"],
                "Số lượng": [10, 20],
                "Đơn giá": [100, 200],
            }
        )
        result, stats = disambiguate_product_codes(df)
        assert "Số lượng" in result.columns
        assert "Đơn giá" in result.columns

    def test_missing_columns(self):
        """Missing required columns should return unchanged."""
        df = pd.DataFrame(
            {
                "Mã hàng": ["A", "A"],
                "Số lượng": [10, 20],
            }
        )
        result, stats = disambiguate_product_codes(df)
        assert result.equals(df)  # No columns, no changes
        assert stats["codes_processed"] == 0
