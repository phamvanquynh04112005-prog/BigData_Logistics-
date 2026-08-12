"""Đẩy các bảng cần cho dashboard từ logistics.duckdb sang Postgres cho Metabase đọc."""
import duckdb
from sqlalchemy import create_engine

DUCKDB_PATH = "logistics.duckdb"
PG_URI = "postgresql+psycopg2://analytics:analytics123@localhost:5433/analytics"

TABLES = [
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


def main():
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    engine = create_engine(PG_URI)
    for table in TABLES:
        df = con.execute(f"SELECT * FROM {table}").fetchdf()
        df.to_sql(table.lower(), engine, if_exists="replace", index=False)
        print(f"Exported {table} -> {table.lower()} ({len(df)} rows)")
    con.close()


if __name__ == "__main__":
    main()