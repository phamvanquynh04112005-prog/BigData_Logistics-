import pandas as pd
import numpy as np
from pathlib import Path

RAW_PATH = Path("data/raw/DataCoSupplyChainDataset.csv")
OUT_DIR = Path("data/simulated")
OUT_DIR.mkdir(parents=True, exist_ok=True)

np.random.seed(42)  # để lần nào chạy cũng ra kết quả giống nhau, tiện demo lại

# ---- 1. Đọc dữ liệu gốc, lấy các vùng (region) đang thực sự tồn tại ----
df = pd.read_csv(RAW_PATH, encoding="latin-1")

regions = sorted(df["Order Region"].dropna().unique().tolist())
print(f"Tìm thấy {len(regions)} vùng (Order Region) trong dataset:")
print(regions)

# ---- 2. Sinh Dim_Warehouse: mỗi vùng có 1 kho ----
warehouse_rows = []
for i, region in enumerate(regions, start=1):
    warehouse_rows.append({
        "warehouse_id": f"WH{i:03d}",
        "warehouse_name": f"Warehouse {region}",
        "region": region,
        "capacity_units": np.random.randint(5000, 20000)
    })

dim_warehouse = pd.DataFrame(warehouse_rows)
dim_warehouse.to_csv(OUT_DIR / "Dim_Warehouse.csv", index=False)
print(f"Đã tạo Dim_Warehouse.csv với {len(dim_warehouse)} kho")

# ---- 3. Sinh Dim_Carrier: danh sách hãng vận chuyển cố định ----
carrier_data = [
    ("CARR001", "FastLine Express", "Express"),
    ("CARR002", "GlobalCargo Logistics", "Standard"),
    ("CARR003", "SwiftShip Co.", "Express"),
    ("CARR004", "OceanBridge Freight", "Standard"),
    ("CARR005", "SkyRoute Delivery", "Same Day"),
    ("CARR006", "TransContinental Movers", "Standard"),
]
dim_carrier = pd.DataFrame(carrier_data, columns=["carrier_id", "carrier_name", "service_type"])
dim_carrier.to_csv(OUT_DIR / "Dim_Carrier.csv", index=False)
print(f"Đã tạo Dim_Carrier.csv với {len(dim_carrier)} hãng vận chuyển")