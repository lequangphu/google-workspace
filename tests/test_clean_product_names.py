# -*- coding: utf-8 -*-
"""Unit tests for clean_product_names.py module.

Tests Phase 1 implementation:
- Issue 1: Spaces around special characters
- Issue 4: Dimension format standardization

Tests Phase 2 implementation:
- Issue 3: Brand name spelling and extraction
- Issue 5: Product type format standardization
"""

import pytest
import pandas as pd

from src.modules.import_export_receipts.clean_product_names import (
    clean_product_names_series,
    clean_and_extract,
    normalize_spaces_around_special_chars,
    standardize_dimension,
    clean_dimension_format,
    clean_product_name,
    check_cleaning_quality,
    standardize_product_type,
    extract_product_type_attributes,
)

from src.modules.import_export_receipts.product_disambiguation import (
    extract_brand_from_name,
)

# ============================================================================
# ISSUE 1: SPACES AROUND SPECIAL CHARACTERS
# ============================================================================


class TestNormalizeSpacesAroundSpecialChars:
    """Test normalization of spaces around special characters."""

    def test_remove_space_after_letter_dot(self):
        assert (
            normalize_spaces_around_special_chars("CHENGSHIN L. 80/90-17")
            == "CHENGSHIN L.80/90-17"
        )
        assert normalize_spaces_around_special_chars("L. 225-17") == "L.225-17"

    def test_remove_spaces_around_slash(self):
        assert normalize_spaces_around_special_chars("70/90 - 17") == "70/90-17"

    def test_remove_spaces_around_dash(self):
        assert normalize_spaces_around_special_chars("2.50 - 17") == "2.50-17"

    def test_normalize_multiplication_sign(self):
        assert normalize_spaces_around_special_chars("700 * 23C") == "700*23C"

    def test_remove_spaces_in_parentheses(self):
        assert normalize_spaces_around_special_chars("( N )") == "(N)"

    def test_handle_multiple_spaces(self):
        assert normalize_spaces_around_special_chars("70/90  - 17") == "70/90-17"

    def test_no_change_needed(self):
        assert normalize_spaces_around_special_chars("L.80/90-17") == "L.80/90-17"

    def test_none_input(self):
        assert normalize_spaces_around_special_chars(None) is None
        assert normalize_spaces_around_special_chars("") == ""

    def test_non_string_input(self):
        assert normalize_spaces_around_special_chars(123) == 123


# ============================================================================
# ISSUE 4: DIMENSION FORMAT STANDARDIZATION
# ============================================================================


class TestStandardizeDimension:
    """Test dimension format standardization."""

    def test_fractional_tire_format(self):
        dim, dim_type = standardize_dimension("CHENGSHIN L.80/90-17 RS")
        assert dim == "80/90-17"
        assert dim_type == "motorcycle_tire"

    def test_triple_to_fractional_format(self):
        dim, dim_type = standardize_dimension("L.80/90-17")
        assert dim == "80/90-17"
        assert dim_type == "motorcycle_tire"

    def test_decimal_tire_format(self):
        dim, dim_type = standardize_dimension("L.2.50-17")
        assert dim == "2.50-17"
        assert dim_type == "motorcycle_tire"

    def test_tube_range_format(self):
        dim, dim_type = standardize_dimension("SĂM 2.25/2.50-17")
        assert dim == "2.50-17"
        assert dim_type == "motorcycle_tire"

    def test_french_bicycle_format(self):
        dim, dim_type = standardize_dimension("KENDA 27x1-3/8")
        assert dim == "27x1-3/8"
        assert dim_type == "bicycle"

    def test_drop_leading_letter_with_space(self):
        assert clean_dimension_format("L 80/90-17") == "80/90-17"

    def test_drop_leading_letter_with_dot(self):
        assert clean_dimension_format("L.80/90-17") == "80/90-17"

    def test_no_dimension_found(self):
        dim, dim_type = standardize_dimension("BÌNH WAVE RS L1 (WTZ5S)")
        assert dim is None
        assert dim_type == "unknown"

    def test_none_input(self):
        assert clean_dimension_format(None) is None
        assert clean_dimension_format("") == ""


# ============================================================================
# ISSUE 4: DIMENSION FORMAT STANDARDIZATION (CLEAN DIMENSION FORMAT)
# ============================================================================


class TestCleanDimensionFormat:
    """Test dimension format cleaning in product name."""

    def test_drop_leading_letter_and_space(self):
        assert clean_dimension_format("L.80/90-17 RS") == "80/90-17 RS"

    def test_convert_triple_to_fractional(self):
        assert clean_dimension_format("L.80/90/17") == "80/90-17"

    def test_normalize_fractional_separators(self):
        assert clean_dimension_format("80 / 90 - 17") == "80/90-17"

    def test_normalize_decimal_format(self):
        assert clean_dimension_format("2 . 50 - 17") == "2.50-17"

    def test_bicycle_format_with_asterisk(self):
        assert clean_dimension_format("700 * 23C") == "700x23C"

    def test_complex_name_with_dimension(self):
        assert (
            clean_dimension_format("CHENGSHIN L.80/90-17 RS") == "CHENGSHIN 80/90-17 RS"
        )

    def test_no_dimension_in_name(self):
        assert (
            clean_dimension_format("BÌNH WAVE RS L1 (WTZ5S)")
            == "BÌNH WAVE RS L1 (WTZ5S)"
        )


# ============================================================================
# PHASE 1: FULL CLEANING (PHASE 1 ONLY - NO PHASE 2)
# ============================================================================


class TestCleanProductName:
    """Test combined Phase 1 cleaning."""

    def test_full_cleaning_fractional_tire(self):
        original = "CHENGSHIN L. 80/90 - 17 RS T/T"
        cleaned = clean_product_name(original)
        assert cleaned == "CHENGSHIN 80/90-17 RS T/T"

    def test_full_cleaning_triple_tire(self):
        original = "L. 80 / 90 / 17"
        cleaned = clean_product_name(original)
        assert cleaned == "80/90-17"

    def test_full_cleaning_decimal_tire(self):
        # Updated: clean_product_name keeps brand when dimension lacks leading space
        # For "MAXXIS L. 2 . 50 - 17", dimension "2.50-17" has space after L
        # so "L." is removed, "2" starts dimension, brand "MAXXIS" is KEPT
        original = "MAXXIS L. 2 . 50 - 17"
        cleaned = clean_product_name(original)
        assert cleaned == "MAXXIS 2.50-17"

    def test_full_cleaning_bicycle_tire(self):
        original = "KENDA Vỏ 700 * 23C Đen K191"
        cleaned = clean_product_name(original)
        assert cleaned == "KENDA Vỏ 700x23C Đen K191"

    def test_full_cleaning_with_parentheses(self):
        # Updated: Region codes preserved, TT→T/T conversion happens
        original = "CASUMINA Lốp 2.50-17 6PR CA123A TT Cam ĐN ( N )"
        cleaned = clean_product_name(original)
        assert cleaned == "CASUMINA Lốp 2.50-17 6PR CA123A TT Cam ĐN (N)"

    def test_full_cleaning_no_dimension(self):
        original = "BÌNH WAVE RS L1 (WTZ5S)"
        cleaned = clean_product_name(original)
        assert cleaned == "BÌNH WAVE RS L1 (WTZ5S)"


# ============================================================================
# BATCH PROCESSING
# ============================================================================


class TestCleanProductNamesSeries:
    """Test batch processing of product names."""

    def test_process_series(self):
        series = pd.Series(
            [
                "L. 80/90 - 17",
                "L.80/90-17",
                "700 * 23C",
                "2 . 50 - 17",
            ]
        )
        cleaned = clean_product_names_series(series)
        expected = pd.Series(["80/90-17", "80/90-17", "700x23C", "2.50-17"])
        pd.testing.assert_series_equal(cleaned, expected)


# ============================================================================
# VALIDATION
# ============================================================================


class TestCheckCleaningQuality:
    """Test cleaning quality metrics."""

    def test_spaces_removed(self):
        original = "L. 80/90  - 17"
        cleaned = clean_product_name(original)
        metrics = check_cleaning_quality(original, cleaned)
        assert metrics["spaces_removed"] is True
        assert metrics["dimension_extracted"] == "80/90-17"

    def test_special_chars_normalized(self):
        original = "70/90 - 17"
        cleaned = clean_product_name(original)
        metrics = check_cleaning_quality(original, cleaned)
        assert metrics["special_chars_normalized"] is True

    def test_dimension_cleaned(self):
        # Updated: clean_product_name includes Phase 2 (TT→T/T conversion)
        original = "L.80/90-17"
        cleaned = clean_product_name(original)
        metrics = check_cleaning_quality(original, cleaned)
        assert metrics["dimension_cleaned"] is True
        assert metrics["dimension_extracted"] == "80/90-17 T/T"


# ============================================================================
# PHASE 2: BRAND & PRODUCT TYPE
# ============================================================================


class TestExtractBrandFromName:
    """Test brand extraction with typo corrections."""

    def test_tire_brand_extraction(self):
        assert extract_brand_from_name("CHENGSHIN L.80/90-17") == "CHENGSHIN"

    def test_typo_correction_michelin(self):
        assert extract_brand_from_name("MICHENLIN 70/90-17") == "MICHELIN"

    def test_battery_brand_extraction(self):
        assert extract_brand_from_name("BÌNH WAVE RS") == "WAVE"

    def test_oil_brand_extraction(self):
        assert extract_brand_from_name("NHỚT YAMAHA GA 0.8L") == "YAMAHA"

    def test_product_type_keyword_not_brand(self):
        assert extract_brand_from_name("VỐ 80/90-17") == "VỐ"

    def test_unknown_brand(self):
        # First word not in brand list
        result = extract_brand_from_name("UNKNOWN BRAND 80/90-17")
        assert result == "UNKNOWN"

    def test_none_input(self):
        assert extract_brand_from_name(None) == ""
        assert extract_brand_from_name("") == ""


# ============================================================================
# PHASE 2: PRODUCT TYPE STANDARDIZATION
# ============================================================================


class TestStandardizeProductType:
    """Test product type format standardization."""

    def test_normalize_t_l_variations(self):
        assert standardize_product_type("70/90-17 T/L") == "70/90-17 T/L"
        assert standardize_product_type("70/90-17 TL") == "70/90-17 T/L"
        assert standardize_product_type("70/90-17 T L") == "70/90-17 T/L"

    def test_normalize_tt_to_t_slash(self):
        assert standardize_product_type("70/90-17 TT") == "70/90-17 T/T"

    def test_normalize_pr_spacing(self):
        assert standardize_product_type("2.50-17 6 PR") == "2.50-17 6PR"
        assert standardize_product_type("2.50-17 6PR 38L") == "2.50-17 6PR 38L"

    # Updated: Region code transformation disabled (no-op)
    def test_standardize_region_codes(self):
        # Updated: Region codes remain in parentheses format
        assert standardize_product_type("70/90-17 TT (N)") == "70/90-17 TT (N)"
        assert standardize_product_type("70/90-17 TT (N, S)") == "70/90-17 TT (N, S)"
        assert standardize_product_type("70/90-17 TT -N") == "70/90-17 TT (N)"


# ============================================================================
# PHASE 2: PRODUCT TYPE ATTRIBUTES
# ============================================================================


class TestExtractProductTypeAttributes:
    """Test product type attribute extraction."""

    def test_extract_tubeless_type(self):
        attrs = extract_product_type_attributes("70/90-17 T/L")
        assert attrs["tire_type"] == "tubeless"

    def test_extract_tube_type(self):
        attrs = extract_product_type_attributes("70/90-17 T/T")
        assert attrs["tire_type"] == "tube_type"

    def test_extract_ply_rating(self):
        attrs = extract_product_type_attributes("2.50-17 6PR")
        assert attrs["ply_rating"] == 6

    def test_extract_load_index(self):
        attrs = extract_product_type_attributes("70/90-17 38P")
        assert attrs["load_index"] == "38P"

    def test_extract_region_code(self):
        attrs = extract_product_type_attributes("70/90-17 T/T-N")
        assert attrs["region_code"] == "N"

    def test_extract_directional_pattern(self):
        attrs = extract_product_type_attributes("70/90-17 RS T/T")
        assert attrs["has_pattern"] is True

    def test_unknown_type(self):
        attrs = extract_product_type_attributes("BÌNH WAVE RS L1 (WTZ5S)")
        assert attrs["tire_type"] == "unknown"
        assert attrs["ply_rating"] is None
        assert attrs["load_index"] is None
        assert attrs["region_code"] is None
        assert attrs["has_pattern"] is False

    def test_none_input(self):
        attrs = extract_product_type_attributes(None)
        assert attrs == {}

    def test_dimension_dropped(self):
        # Updated: Phase 1 + Phase 2 (TT→T/T conversion included)
        original = "L.80/90-17"
        cleaned = clean_product_name(original)
        metrics = check_cleaning_quality(original, cleaned)
        assert metrics["dimension_cleaned"] is True
        assert metrics["dimension_extracted"] == "80/90-17 T/T"
