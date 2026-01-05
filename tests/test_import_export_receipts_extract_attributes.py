# -*- coding: utf-8 -*-
"""Tests for extract_attributes module."""

from src.modules.import_export_receipts.extract_attributes import extract_attributes


class TestExtractAttributes:
    """Test attribute extraction from product names."""

    def test_fractional_tire_with_type(self):
        """Extract fractional tire size and type: W/H-D + T/L (Không ruột) or TT (Có ruột)."""
        assert (
            extract_attributes("CAMEL 110/70-12 CMI 547 T/L")
            == "Kích thước:110/70-12|Loại vỏ:Không ruột"
        )
        assert (
            extract_attributes("MAXXIS 100/90-17 M6029 TT")
            == "Kích thước:100/90-17|Loại vỏ:Có ruột"
        )
        assert (
            extract_attributes("KENDA Vỏ 80/90-14 K270 TL")
            == "Kích thước:80/90-14|Loại vỏ:Không ruột"
        )
        assert (
            extract_attributes("CHENGSHIN C922 120/80-14 T/L")
            == "Kích thước:120/80-14|Loại vỏ:Không ruột"
        )

    def test_decimal_tire_with_type(self):
        """Extract decimal tire size and type."""
        assert extract_attributes("KENDA Vỏ 2.50-18 K203") == "Kích thước:2.50-18"
        assert (
            extract_attributes("INU 2.25-17 4PR 33L NF3 TT")
            == "Kích thước:2.25-17|Loại vỏ:Có ruột"
        )
        assert (
            extract_attributes("Vỏ 3.00-18 Michelin T/L")
            == "Kích thước:3.00-18|Loại vỏ:Không ruột"
        )

    def test_tube_range_size(self):
        """Extract tube size with range."""
        assert (
            extract_attributes("CHENGSHIN SĂM 2.25/2.50-17")
            == "Kích thước:2.25/2.50-17"
        )
        assert (
            extract_attributes("CAMEL TUBE 2.00/2.50-18") == "Kích thước:2.00/2.50-18"
        )

    def test_simple_tube_size(self):
        """Extract simple tube size with tube keywords."""
        assert extract_attributes("CHENGSHIN SĂM 225-17") == "Kích thước:225-17"
        assert extract_attributes("CAMEL TUBE 2.00-17") == "Kích thước:2.00-17"
        assert extract_attributes("CASUMINA Săm 27x1.3/8 AV28") == "Kích thước:27x1.3/8"

    def test_french_bicycle_size(self):
        """Extract French/bicycle size."""
        assert (
            extract_attributes("CASUMINA Săm 27x1.3/8 AV28 HM") == "Kích thước:27x1.3/8"
        )
        assert (
            extract_attributes("Lốp 20x1.50/1.75 AV28 HM") == "Kích thước:20x1.50/1.75"
        )

    def test_belt_size(self):
        """Extract belt/curoa size."""
        assert extract_attributes("DÂY CUROA XE SH 150") == "Kích thước:150"
        assert extract_attributes("DÂY CUROA XE LEAD 125") == "Kích thước:125"
        assert extract_attributes("Curoa Xe Airblade 110") == "Kích thước:110"

    def test_oil_size(self):
        """Extract oil/volume size."""
        assert extract_attributes("NHỚT YAMAHA GA 0.8L") == "Kích thước:0.8L"
        assert extract_attributes("Dầu nhớt MOTUL 1000ML") == "Kích thước:1000ML"
        assert extract_attributes("NHỚT 1.0 L") == "Kích thước:1.0L"

    def test_no_attributes(self):
        """Return empty string when no pattern matched."""
        assert extract_attributes("Sản phẩm không có thuộc tính") == ""
        assert extract_attributes("") == ""
        assert extract_attributes("   ") == ""
        assert extract_attributes(None) == ""

    def test_partial_matches(self):
        """Handle partial matches correctly."""
        assert extract_attributes("Vỏ 110/70-12 không rõ") == "Kích thước:110/70-12"
        assert extract_attributes("Săm 225-17 cũ") == "Kích thước:225-17"

    def test_case_insensitivity(self):
        """Pattern matching should be case insensitive."""
        assert extract_attributes("dây curoa xe sh 150") == "Kích thước:150"
        assert extract_attributes("NHỚT YAMAHA 0.8L") == "Kích thước:0.8L"
        assert (
            extract_attributes("vỏ 110/70-12 t/l")
            == "Kích thước:110/70-12|Loại vỏ:Không ruột"
        )
        assert (
            extract_attributes("VỎ 100/90-17 TT")
            == "Kích thước:100/90-17|Loại vỏ:Có ruột"
        )

    def test_priority_order(self):
        """Ensure correct priority for overlapping patterns."""
        name = "CAMEL 110/70-12 T/L"
        result = extract_attributes(name)
        assert "Kích thước:110/70-12" in result
        assert "Loại vỏ:Không ruột" in result

    def test_invalid_product_names(self):
        """Handle edge cases gracefully."""
        assert extract_attributes("SẢN PHẨM ABC") == ""
        assert extract_attributes("123 456 789") == ""
        assert extract_attributes("Vỏ / - ") == ""

    def test_tire_type_detection(self):
        """Test tire type (loại vỏ) detection for different products."""
        assert (
            extract_attributes("PIRELLI ANGEL VỎ 90/80-17 TL")
            == "Kích thước:90/80-17|Loại vỏ:Không ruột"
        )
        assert (
            extract_attributes("MAXXIS L.70/90-17 TT M6230")
            == "Kích thước:70/90-17|Loại vỏ:Có ruột"
        )
        assert (
            extract_attributes("KENDA Vỏ 70/90-17 K6010 4P TL")
            == "Kích thước:70/90-17|Loại vỏ:Không ruột"
        )
        assert (
            extract_attributes("DUNLOP 100/70/17 TL")
            == "Kích thước:100/70/17|Loại vỏ:Không ruột"
        )
        assert (
            extract_attributes("MiCHENLIN 110/70-16 TL/TT City Grip 2")
            == "Kích thước:110/70-16|Loại vỏ:Không ruột"
        )

    def test_non_tire_products_no_type(self):
        """Non-tire products should not have Loại vỏ attribute."""
        assert extract_attributes("CHENGSHIN SĂM 225-17") == "Kích thước:225-17"
        assert extract_attributes("NHỚT YAMAHA GA 0.8L") == "Kích thước:0.8L"
        assert extract_attributes("DÂY CUROA XE SH 150") == "Kích thước:150"
