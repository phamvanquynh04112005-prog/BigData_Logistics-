import pandas as pd
from pathlib import Path

RAW_PATH = Path("data/raw/DataCoSupplyChainDataset.csv")
OUT_DIR = Path("data/simulated")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ---- 1. Đọc 2 cột ngày, xác định khoảng thời gian thực tế cần cover ----
df = pd.read_csv(
    RAW_PATH,
    encoding="latin-1",
    usecols=["order date (DateOrders)", "shipping date (DateOrders)"],
)

order_dates = pd.to_datetime(df["order date (DateOrders)"], errors="coerce")
ship_dates = pd.to_datetime(df["shipping date (DateOrders)"], errors="coerce")

min_date = min(order_dates.min(), ship_dates.min())
max_date = max(order_dates.max(), ship_dates.max())
print(f"Khoảng ngày thực tế trong dữ liệu: {min_date.date()} -> {max_date.date()}")

# ---- 2. Sinh dải ngày đầy đủ, có buffer 1 tháng mỗi đầu để an toàn khi join ----
start = (min_date - pd.DateOffset(months=1)).replace(day=1)
end = (max_date + pd.DateOffset(months=1))

date_range = pd.date_range(start=start, end=end, freq="D")

dim_date = pd.DataFrame({"full_date": date_range})
dim_date["date_key"] = dim_date["full_date"].dt.strftime("%Y%m%d").astype(int)
dim_date["year"] = dim_date["full_date"].dt.year
dim_date["month"] = dim_date["full_date"].dt.month
dim_date["day"] = dim_date["full_date"].dt.day
dim_date["quarter"] = dim_date["full_date"].dt.quarter
dim_date["day_of_week"] = dim_date["full_date"].dt.day_name()
dim_date["is_weekend"] = dim_date["full_date"].dt.dayofweek.isin([5, 6])

dim_date = dim_date[
    ["date_key", "full_date", "day", "month", "quarter", "year", "day_of_week", "is_weekend"]
]

dim_date.to_csv(OUT_DIR / "Dim_Date.csv", index=False)
print(f"Đã tạo Dim_Date.csv với {len(dim_date)} ngày")
print(dim_date.head())
print(dim_date.tail())