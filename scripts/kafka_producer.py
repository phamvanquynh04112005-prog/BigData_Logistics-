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
SLEEP_SECONDS = 1
SCHEMA_VERSION = "1.1"  # tăng version khi đổi cấu trúc message, để consumer biết

STATE_SEQUENCE = ["SCAN", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED"]
DELAY_PROBABILITY = 0.15  # xác suất chèn 1 sự kiện DELAYED, chỉ sau khi đã SCAN

# --- Đọc dữ liệu tham chiếu ---
orders_df = pd.read_csv(
    "data/raw/DataCoSupplyChainDataset.csv",
    encoding="latin-1",
    usecols=["Order Item Id", "Order Id", "Order Region"],
)
orders_df["Order Region"] = orders_df["Order Region"].str.strip()

warehouse_df = pd.read_csv("data/simulated/Dim_Warehouse.csv")
warehouse_df["region_clean"] = warehouse_df["region"].str.strip()
region_to_warehouse = dict(zip(warehouse_df["region_clean"], warehouse_df["warehouse_id"]))

carrier_df = pd.read_csv("data/simulated/Dim_Carrier.csv")
carrier_ids = carrier_df["carrier_id"].tolist()

# Lấy mẫu shipment, MỖI shipment gán CỐ ĐỊNH 1 carrier ngay từ đầu (không đổi giữa các event)
random.seed(42)
sample = orders_df.sample(n=min(500, len(orders_df)), random_state=42).to_dict("records")

shipments = {}
for row in sample:
    shipment_id = int(row["Order Item Id"])  # grain đúng chuẩn: 1 shipment = 1 order item
    region = row["Order Region"]
    shipments[shipment_id] = {
        "order_key": int(row["Order Id"]),
        "region": region,
        "warehouse_id": region_to_warehouse.get(region, "WH000"),
        "carrier_id": random.choice(carrier_ids),  # gán 1 LẦN, giữ nguyên suốt vòng đời shipment
        "state_index": 0,
        "done": False,
    }

active_ids = list(shipments.keys())


def generate_event():
    pending = [sid for sid in active_ids if not shipments[sid]["done"]]
    if not pending:
        # Hết shipment "sống" -> reset để demo tiếp tục dài hạn
        for sid in active_ids:
            shipments[sid]["state_index"] = 0
            shipments[sid]["done"] = False
        pending = active_ids

    shipment_id = random.choice(pending)
    s = shipments[shipment_id]

    # DELAYED chỉ có thể xảy ra SAU khi đã có ít nhất 1 trạng thái thật (đã SCAN)
    if s["state_index"] > 0 and s["state_index"] < len(STATE_SEQUENCE) - 1 and random.random() < DELAY_PROBABILITY:
        event_type = "DELAYED"
    else:
        event_type = STATE_SEQUENCE[s["state_index"]]
        s["state_index"] = min(s["state_index"] + 1, len(STATE_SEQUENCE) - 1)
        if event_type == "DELIVERED":
            s["done"] = True

    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": str(uuid.uuid4()),
        "shipment_id": shipment_id,      # = Order Item Id
        "order_key": s["order_key"],     # = Order Id
        "carrier_id": s["carrier_id"],
        "warehouse_id": s["warehouse_id"],
        "event_type": event_type,
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "region": s["region"],
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print(f"Đang gửi sự kiện vào topic '{TOPIC_NAME}' (schema v{SCHEMA_VERSION})... (Ctrl+C để dừng)\n")

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