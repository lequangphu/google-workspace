---
globs:
  - 'src/erp/**/*.py'
  - 'src/modules/**/*.py'
  - 'tests/test_templates.py'
  - 'tests/test_exporter.py'
---

# ERP Mapping – KiotViet Templates & Column Details

Reference for all columns required in each KiotViet import template. Use for validation and field mapping.

## Products (Sản phẩm)

**File**: `data/templates/MauFileSanPham.xlsx` (27 columns)

| # | Column | Type | Format | Notes |
|---|--------|------|--------|-------|
| 1 | Loại hàng | Text | — | "Hàng hóa", "Dịch vụ" |
| 2 | Nhóm hàng(3 Cấp) | Text | — | Hierarchy: "Loại > Nhóm > Chi tiết" |
| 3 | Mã hàng | Text | — | **Primary Key**. Format: "HH000026" |
| 4 | Mã vạch | Text | — | Barcode or product code |
| 5 | Tên hàng | Text | — | Product name (Title Case) |
| 6 | Thương hiệu | Text | — | Brand name |
| 7 | Giá bán | Number | #,0.#0 | Selling price (2 decimals) |
| 8 | Giá vốn | Number | #,0.#0 | Cost price (2 decimals) |
| 9 | Tồn kho | Number | #,0.##0 | Integer quantity, 1000s separator |
| 10 | Tồn nhỏ nhất | Number | #,0.##0 | Min stock (integer) |
| 11 | Tồn lớn nhất | Number | #,0.##0 | Max stock (integer) |
| 12 | ĐVT | Text | — | Unit of measurement: "Hộp", "Cái", "Bộ" |
| 13 | Mã ĐVT Cơ bản | Text | — | Base UOM code |
| 14 | Quy đổi | Number | #,0.##0 | Conversion factor (integer) |
| 15 | Thuộc tính | Text | — | Product attributes (pipe-separated) |
| 16 | Mã HH Liên quan | Text | — | Related product IDs |
| 17 | Hình ảnh (url1,url2...) | Text | — | Comma-separated URLs |
| 18 | Sử dụng Imei | Number | — | Boolean: 0 or 1 |
| 19 | Trọng lượng | Number | #,0.##0 | Weight in kg (integer) |
| 20 | Đang kinh doanh | Number | — | Boolean: 0=inactive, 1=active |
| 21 | Được bán trực tiếp | Number | — | Boolean: 0 or 1 |
| 22 | Mô tả | Text | — | Product description |
| 23 | Mẫu ghi chú | Text | — | Note template |
| 24 | Vị trí | Text | — | Storage location: "Dãy 1", "Kệ A" |
| 25 | Hàng thành phần | Text | — | Component products (pipe-separated) |
| 26 | Bảo hành | Text | — | Warranty info: "Toàn bộ:1 Tháng\|Thành phần:20 Ngày" |
| 27 | Bảo trì định kỳ | Text | — | Maintenance: "Toàn bộ:1 Tháng" |

## PriceBook (Bảng giá)

**File**: `data/templates/MauFileBangGia.xlsx` (5 columns)

| # | Column | Type | Format | Notes |
|---|--------|------|--------|-------|
| 1 | Mã hàng | Text | — | **Primary Key** |
| 2 | Tên hàng | Text | — | Product name |
| 3 | Tên bảng giá 1 | Number | #,0.#0 | Price with 2 decimals |
| 4 | Tên bảng giá 2 | Number | #,0.#0 | Price with 2 decimals |
| 5 | Tên bảng giá 3 | Number | #,0.#0 | Price with 2 decimals |

## Customers (Khách hàng)

**File**: `data/templates/MauFileKhachHang.xlsx` (20 columns)

| # | Column | Type | Format | Notes |
|---|--------|------|--------|-------|
| 1 | Loại khách | Text | — | "Cá nhân", "Công ty" |
| 2 | Mã khách hàng | Text | — | **Primary Key**. Format: "KH000008" |
| 3 | Tên khách hàng | Text | — | Title Case: "Nguyễn Hoàng Mai" |
| 4 | Điện thoại | Text | — | Phone number |
| 5 | Địa chỉ | Text | — | Street address |
| 6 | Khu vực giao hàng | Text | — | "Hà Nội - Quận Tây Hồ" |
| 7 | Phường/Xã | Text | — | "Phường Thụy Khuê" |
| 8 | Công ty | Text | — | Company name (if applicable) |
| 9 | Mã số thuế | Text | — | Tax ID number |
| 10 | Số CMND/CCCD | Text | — | ID/Passport number |
| 11 | Ngày sinh | Date | dd/MM/yyyy | Birth date |
| 12 | Giới tính | Text | — | "Nam", "Nữ" |
| 13 | Email | Text | — | Email address |
| 14 | Facebook | Text | — | Facebook URL |
| 15 | Nhóm khách hàng | Text | — | Pipe-separated: "Nhóm1\|Nhóm2" |
| 16 | Ghi chú | Text | — | Notes |
| 17 | Ngày giao dịch cuối | Date | dd/MM/yyyy | Read-only |
| 18 | Nợ cần thu hiện tại | Number | #,##0 | Integer, read-only |
| 19 | Tổng bán (Không import) | Number | #,##0 | Integer, no import |
| 20 | Trạng thái | Number | — | Boolean: 0 or 1 |

## Suppliers (Nhà cung cấp)

**File**: `data/templates/MauFileNhaCungCap.xlsx` (15 columns)

| # | Column | Type | Format | Notes |
|---|--------|------|--------|-------|
| 1 | Mã nhà cung cấp | Text | — | **Primary Key**. Format: "NCC000004" |
| 2 | Tên nhà cung cấp | Text | — | Supplier name |
| 3 | Email | Text | — | Email address |
| 4 | Điện thoại | Text | — | Phone number |
| 5 | Địa chỉ | Text | — | Street address |
| 6 | Khu vực | Text | — | "Hà Nội - Quận Đống Đa" |
| 7 | Phường/Xã | Text | — | "Phường Quốc Tử Giám" |
| 8 | Tổng mua (Không Import) | Number | #,##0 | Integer, no import |
| 9 | Nợ cần trả hiện tại | Number | #,##0 | Integer, read-only |
| 10 | Mã số thuế | Text | — | Tax ID: "0400123456-002" |
| 11 | Ghi chú | Text | — | Notes |
| 12 | Nhóm nhà cung cấp | Text | — | Supplier group name |
| 13 | Trạng thái | Number | — | Boolean: 0 or 1 |
| 14 | Tổng mua trừ trả hàng | Number | #,##0 | Integer |
| 15 | Công ty | Text | — | Company name |
