"""Đẩy các bảng cần cho dashboard từ logistics.duckdb sang Postgres cho Metabase đọc."""
import duckdb
from sqlalchemy import create_engine

DUCKDB_PATH = "logistics.duckdb"
PG_URI = "postgresql+psycopg2://analytics:analytics123@localhost:5433/analytics"

REQUIRED_TABLES = [
    "Fact_Shipment",
    "Dim_Carrier",
    "Dim_Warehouse",
    "Dim_Route",
    "Dim_Date",
    "sla_monthly",
    "carrier_performance",
    "route_performance",
    "shipment_risk_predictions",
    "route_recommendations",
]

# These tables are created only after the Kafka/Spark realtime demo runs.
# Skipping them keeps the dashboard export usable before that optional step.
OPTIONAL_REALTIME_TABLES = [
    "shipment_realtime_alert",
    "shipment_risk_realtime_alert",
    "shipment_proactive_risk_alert",
]


def main():
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    engine = create_engine(PG_URI)
    available = {
        row[0]
        for row in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
    }
    for table in REQUIRED_TABLES:
        df = con.execute(f"SELECT * FROM {table}").fetchdf()
        df.to_sql(table.lower(), engine, if_exists="replace", index=False)
        print(f"Exported {table} -> {table.lower()} ({len(df)} rows)")
    for table in OPTIONAL_REALTIME_TABLES:
        if table not in available:
            print(f"Skipped {table}: run the realtime stream and evaluator first")
            continue
        df = con.execute(f"SELECT * FROM {table}").fetchdf()
        df.to_sql(table.lower(), engine, if_exists="replace", index=False)
        print(f"Exported {table} -> {table.lower()} ({len(df)} rows)")
    con.close()


if __name__ == "__main__":
    main()
