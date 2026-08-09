import pandas as pd
import random
import json
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

# --- Cấu hình ---
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "shipment-tracking-events"
SLEEP_SECONDS = 1  # khoảng cách giữa 2 sự kiện

# --- Đọc dữ liệu tham chiếu ---
orders_df = pd.read_csv(
    "data/raw/DataCoSupplyChainDataset.csv",
    encoding="latin-1",
    usecols=["Order Id", "Order Region"],
)
orders_df["Order Region"] = orders_df["Order Region"].str.strip()

warehouse_df = pd.read_csv("data/simulated/Dim_Warehouse.csv")
warehouse_df["region_clean"] = warehouse_df["region"].str.strip()
region_to_warehouse = dict(zip(warehouse_df["region_clean"], warehouse_df["warehouse_id"]))

carrier_df = pd.read_csv("data/simulated/Dim_Carrier.csv")
carrier_ids = carrier_df["carrier_id"].tolist()

# Lấy mẫu 2000 đơn hàng để mô phỏng (không cần load hết 180k dòng mỗi lần sinh sự kiện)
sample_orders = orders_df.sample(n=min(2000, len(orders_df)), random_state=42).to_dict("records")

EVENT_TYPES = ["SCAN", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED", "DELAYED"]


def generate_event():
    order = random.choice(sample_orders)
    region = order["Order Region"]
    warehouse_id = region_to_warehouse.get(region, "WH000")
    return {
        "event_id": str(uuid.uuid4()),
        "shipment_id": int(order["Order Id"]),
        "carrier_id": random.choice(carrier_ids),
        "warehouse_id": warehouse_id,
        "event_type": random.choice(EVENT_TYPES),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "region": region,
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print(f"Đang gửi sự kiện vào topic '{TOPIC_NAME}'... (Ctrl+C để dừng)\n")

    count = 0
    try:
        while True:
            event = generate_event()
            producer.send(TOPIC_NAME, value=event)
            count += 1
            print(f"[{count}] {event}")
            time.sleep(SLEEP_SECONDS)
    except KeyboardInterrupt:
        print(f"\nĐã dừng. Tổng cộng gửi {count} sự kiện.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()