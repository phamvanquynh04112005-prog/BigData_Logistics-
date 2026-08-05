import pandas as pd
from pathlib import Path
from datetime import datetime

FILES = {
    "Dữ liệu đơn hàng/vận chuyển gốc": "data/raw/DataCoSupplyChainDataset.csv",
    "Mô tả cột (kèm theo dataset gốc từ Kaggle)": "data/raw/DescriptionDataCoSupplyChain.csv",
    "Access logs (bonus, chưa dùng trong đồ án)": "data/raw/tokenized_access_logs.csv",
    "Kho hàng (mô phỏng)": "data/simulated/Dim_Warehouse.csv",
    "Hãng vận chuyển (mô phỏng)": "data/simulated/Dim_Carrier.csv",
}

lines = []
lines.append("# Data Catalog - Đồ án 8: Chuỗi cung ứng & Logistics")
lines.append("")
lines.append(f"_Tự động sinh lúc {datetime.now().strftime('%Y-%m-%d %H:%M')}_")
lines.append("")

for label, path_str in FILES.items():
    path = Path(path_str)
    lines.append(f"## {label}")
    if not path.exists():
        lines.append(f"⚠️ Không tìm thấy file: `{path_str}`")
        lines.append("")
        continue

    df = pd.read_csv(path, encoding="latin-1")
    lines.append(f"- Đường dẫn: `{path_str}`")
    lines.append(f"- Số dòng: {len(df):,} | Số cột: {len(df.columns)}")
    lines.append("")
    lines.append("| Cột | Kiểu dữ liệu | Ví dụ giá trị |")
    lines.append("|---|---|---|")
    for col in df.columns:
        non_null = df[col].dropna()
        sample = str(non_null.iloc[0]) if len(non_null) > 0 else ""
        sample = sample.replace("|", "/")[:50]
        lines.append(f"| {col} | {df[col].dtype} | {sample} |")
    lines.append("")
    lines.append("---")
    lines.append("")


lines.append("## Ghi chú chất lượng dữ liệu (Data Quality Notes)")
lines.append("")
lines.append("Phát hiện khi khảo sát ban đầu — cần lưu ý khi làm sạch ở bước PySpark (Vai trò 2):")
lines.append("")
lines.append("1. **`Product Description`** — rỗng gần như 100% dòng. Cân nhắc loại bỏ khi làm sạch.")
lines.append("2. **`order date (DateOrders)`**, **`shipping date (DateOrders)`** — tên cột viết thường không đồng nhất với các cột khác (đa số viết hoa chữ đầu mỗi từ), dễ gây `KeyError` nếu gõ nhầm. Cả 2 đang ở kiểu `str`, cần parse sang `datetime` trước khi tính lead time/delay.")
lines.append("3. **`Customer Zipcode`**, **`Order Zipcode`** — đang ở kiểu `float64` nên mất số 0 ở đầu (VD: `00725` bị đọc thành `725.0`). Cần ép về kiểu chuỗi + đệm số 0 nếu dùng để join/group theo khu vực.")
lines.append("4. **`region`** trong `Dim_Warehouse` — 3 giá trị dính khoảng trắng thừa cuối chuỗi: `South of USA `, `US Center `, `West of USA `. Cần `.strip()` cả 2 phía trước khi join Fact với Dim.")
lines.append("")
output_path = Path("DATA_CATALOG.md")
output_path.write_text("\n".join(lines), encoding="utf-8")
print(f"Đã tạo {output_path} với thông tin của {len(FILES)} file")