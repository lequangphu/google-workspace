"""Configuration for data cleaning by file type (suffix-based)."""

CONFIGS = {
    'CT.NHAP': {
        'header_rows': [3, 4],
        'header_type': 'two_level',
        'data_start_row': 5,
        'numeric_cols': [
            'mã_hh',
            'số_lượng_kho_1',
            'đơn_giá_nhập',
            'thành_tiền'
        ],
        'date_cols': {
            'chứng_từ_nhập_ngày': 'excel'
        },
        'key_col': 'mã_hh',
    },
    'CT.XUAT': {
        'header_rows': [3, 4],
        'header_type': 'two_level',
        'data_start_row': 5,
        'numeric_cols': [
            'số_lượng_bán_lẻ',
            'ghi_chú_giá_bán',
            'ghi_chú_thành_tiền'
        ],
        'date_cols': {
            'chứng_từ_xuất_ngày': 'day_month_filename_year'
        },
        'date_components': {
            'chứng_từ_xuất_ngày': ['chứng_từ_xuất_ngày', 'tháng']
        },
        'key_col': 'mã_số_mã_số',
    },
    'XNT': {
        'header_rows': [2, 3, 4],
        'header_type': 'three_level',
        'data_start_row': 5,
        'numeric_cols': [
            'tồn_đầu_kỳ_s.lượng',
            'tồn_đầu_kỳ_đ.giá',
            'tồn_đầu_kỳ_thành_tiền',
            'nhập_trong_kỳ_s.lượng',
            'nhập_trong_kỳ_đ.giá',
            'nhập_trong_kỳ_thành_tiền',
            'xuất_trong_kỳ_lẽ',
            'xuất_trong_kỳ_sỉ',
            'xuất_trong_kỳ_đ.giá',
            'xuất_trong_kỳ_thành_tiền',
            'tồn_cuối_kỳ_s._lượng',
            'tồn_cuối_kỳ_đ.giá',
            'tồn_cuối_kỳ_thành_tiền',
            'tồn_cuối_kỳ_doanh_thu',
            'tồn_cuối_kỳ_lãi_gộp',
            'chi_phí_tiền',
            'lãi_0'
        ],
        'date_cols': {
            'chi_phí_ngày': 'excel',
            'stt': 'numeric'
        },
        'key_col': 'mã_sp',
    },
}
