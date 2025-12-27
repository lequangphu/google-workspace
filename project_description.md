# Migrating data from Google Sheets to KiotViet
Data of my business, a tire shop named 'Nhan Thanh Tam'.

## Migration steps

### Ingest: read raw data from Google Sheets
Raw data is stored in Google Drive folders shared with me, url of the folders:
1. https://drive.google.com/drive/folders/1RbkY2dd1IaqSHhnivjh6iejKv9mpkSJ_?usp=drive_link
2. https://drive.google.com/drive/folders/1ZJY7aJ-eRdoqYA1NE9ByfHeiYnxq1cFZ?usp=drive_link
3. https://drive.google.com/drive/folders/1p4XXU0nOsJc2Rr2_vJa3YluqmjPtzWRo?usp=drive_link
4. https://drive.google.com/drive/folders/1SYlk8Uztzd8asEZp6SK1yfF-EL0-t_sc?usp=drive_link
5. https://drive.google.com/drive/folders/1Q2-P4aeJfKEVuT69akFHcgrxMsB2mrrU?usp=drive_link
6. https://drive.google.com/drive/folders/1QIP6LCr6lANzzRnAZPmVg7wIWdUmhs6q?usp=drive_link
7. https://drive.google.com/drive/folders/16CXAGzxxoBU8Ui1lXPxZoLVbDdsgwToj?usp=drive_link

Category and spreadsheets name, url of raw sources are:
1. Import/Export receipts, inventory: 'XUẤT NHẬP TỒN TỔNG T*'
2. Receivable: 'CONG NO HANG NGAY - MỚI', url: https://docs.google.com/spreadsheets/d/1kouZwJy8P_zZhjjn49Lfbp3KN81mhHADV7VKDhv5xkM/edit?usp=drive_link
3. Payable: 'BC CÔNG NỢ NCC', url: https://docs.google.com/spreadsheets/d/1b4LWWyfddfiMZWnFreTyC-epo17IR4lcbUnPpLW8X00/edit?usp=drive_link
4. CashFlow: 'SỔ QUỸ TIỀN MẶT + NGÂN HÀNG - 2025', url: https://docs.google.com/spreadsheets/d/1OZ0cdEob37H8z0lGEI4gCet10ox5DgjO6u4wsQL29Ag/edit?usp=drive_link

Import/Export receipts, inventory data is in all urls.
Receivable, payable, and cash flow data is url 7.

Raw data of Import/Export receipts, inventory is separated by year, month. Each 'XUẤT NHẬP TỒN TỔNG T*' file is data of a month in a year.
Raw data of receivable and payable is total and show current balance.
Raw data of cash flow is of year 2025.

### Transform: clean raw data by raw sources
Clean raw data including:
- Rename columns to match columns of KiotViet templates,
- Drop unnecessary columns and rows,
- Clean text and numbers,
- Convert data types to match KiotViet templates,
- Add missing data if necessary.

#### Raw Sources to KiotViet Modules Mapping
Based on existing scripts.

| Raw Source | Category | Processing Scripts | Output Files | KiotViet Module |
|---|---|---|---|---|
| XUẤT NHẬP TỒN TỔNG T* | Import/Export Receipts | clean_chung_tu_nhap.py, clean_chung_tu_xuat.py, clean_xuat_nhap_ton.py, generate_product_info.py | Chi tiết nhập*.csv, Chi tiết xuất*.csv, Xuất nhập tồn*.csv, Thông tin sản phẩm.csv, Tổng nhập.csv, Tổng xuất.csv, Tổng tồn.csv, Giá nhập.csv, Giá xuất.csv, Lãi gộp.csv | **Products**, **PriceBook** |
| CONG NO HANG NGAY | Receivable | clean_thong_tin_khach_hang.py, clean_tong_no.py, generate_new_customer_id.py | Thông tin khách hàng.csv, Tổng nợ.csv, Mã khách hàng mới.csv | **Customers** |
| BC CÔNG NỢ NCC | Payable | (planned) | — | **Suppliers** |
| SỔ QUỸ TIỀN MẶT | CashFlow | (planned) | — | (n/a) |

### Export: write transformed data to Excel (.xlsx) files
Restructure and write transformed data to Excel (.xlsx) files in /data/02-transformed/. The files structure must strictly follow the templates of KiotViet modules stored in /data/templates/.

Modules and templates path:
1. Products: /Users/phu/repos/google-workspace/data/templates/MauFileSanPham.xlsx
2. PriceBook: /Users/phu/repos/google-workspace/data/templates/MauFileBangGia.xlsx
3. Suppliers: /Users/phu/repos/google-workspace/data/templates/MauFileNhaCungCap.xlsx
4. Customers: /Users/phu/repos/google-workspace/data/templates/MauFileKhachHang.xlsx