import pandas as pd
from pathlib import Path

RAW_PATH = Path("data/raw/DataCoSupplyChainDataset.csv")
OUT_DIR = Path("data/simulated")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ---- 1. Đọc dữ liệu gốc, chỉ lấy các cột cần thiết ----
df = pd.read_csv(
    RAW_PATH,
    encoding="latin-1",
    usecols=["Market", "Order Region"],
)

# Chuẩn hóa khoảng trắng: strip 2 đầu + gộp khoảng trắng kép về 1
# (phát hiện thực tế: "South of  USA" có 2 khoảng trắng giữa "of" và "USA")
for col in ["Market", "Order Region"]:
    df[col] = df[col].str.strip().str.replace(r"\s+", " ", regex=True)

# ---- 2. Sinh Dim_Route: mỗi cặp (Market, Order Region) là 1 route ----
# Lưu ý thiết kế: đã thử nghiệm thêm route_type (Domestic/International) dựa trên
# so sánh Customer Country vs Order Country, nhưng bỏ vì vô nghĩa ở dataset này —
# Customer Country trong dữ liệu gốc chỉ có 2 giá trị cố định ("EE. UU.", "Puerto Rico"),
# không bao giờ khớp chuỗi với Order Country nên 100% route luôn ra "International".
dim_route = (
    df[["Market", "Order Region"]]
    .drop_duplicates()
    .sort_values(["Market", "Order Region"])
    .reset_index(drop=True)
)

print(f"Tìm thấy {len(dim_route)} cặp (Market, Order Region) duy nhất:")
print(dim_route)

dim_route.insert(0, "route_id", [f"RT{i:03d}" for i in range(1, len(dim_route) + 1)])
dim_route = dim_route.rename(columns={
    "Market": "origin_market",
    "Order Region": "destination_region",
})

dim_route.to_csv(OUT_DIR / "Dim_Route.csv", index=False)
print(f"\nĐã tạo Dim_Route.csv với {len(dim_route)} route")
print(dim_route.head(10))