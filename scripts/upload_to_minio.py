import boto3
from pathlib import Path

# --- Cấu hình kết nối tới MinIO local ---
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin123",
    region_name="us-east-1",
)

BUCKET = "raw"

# Tự động tạo bucket nếu chưa có
existing_buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
if BUCKET not in existing_buckets:
    s3.create_bucket(Bucket=BUCKET)
    print(f"Đã tạo bucket '{BUCKET}'")
else:
    print(f"Bucket '{BUCKET}' đã tồn tại")

# Ánh xạ: file local -> vị trí (key) trên MinIO, tổ chức theo từng nguồn dữ liệu
FILES_TO_UPLOAD = {
    "data/raw/DataCoSupplyChainDataset.csv": "orders/DataCoSupplyChainDataset.csv",
    "data/raw/DescriptionDataCoSupplyChain.csv": "orders/DescriptionDataCoSupplyChain.csv",
    "data/raw/tokenized_access_logs.csv": "access_logs/tokenized_access_logs.csv",
    "data/simulated/Dim_Warehouse.csv": "dim_warehouse/Dim_Warehouse.csv",
    "data/simulated/Dim_Carrier.csv": "dim_carrier/Dim_Carrier.csv",
}

for local_path, s3_key in FILES_TO_UPLOAD.items():
    path = Path(local_path)
    if not path.exists():
        print(f"⚠️ Bỏ qua (không tìm thấy): {local_path}")
        continue
    s3.upload_file(str(path), BUCKET, s3_key)
    size_mb = path.stat().st_size / (1024 * 1024)
    print(f"Đã upload: {local_path} -> s3://{BUCKET}/{s3_key} ({size_mb:.2f} MB)")

print("\nDanh sách object trong bucket:")
response = s3.list_objects_v2(Bucket=BUCKET)
for obj in response.get("Contents", []):
    print(f"  - {obj['Key']} ({obj['Size']} bytes)")