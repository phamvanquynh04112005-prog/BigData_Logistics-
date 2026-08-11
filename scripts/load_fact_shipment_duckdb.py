r"""Load Fact_Shipment into the local DuckDB warehouse from project CSV data.

Run from the repository root:
    .venv\Scripts\python.exe scripts\load_fact_shipment_duckdb.py

The load is idempotent: Fact_Shipment is rebuilt in a transaction and is only
committed after row-count, primary-key, and foreign-key checks pass.
"""

from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "logistics.duckdb"
ORDERS_PATH = ROOT / "data" / "raw" / "DataCoSupplyChainDataset.csv"


def main() -> None:
    if not ORDERS_PATH.exists():
        raise FileNotFoundError(f"Missing source file: {ORDERS_PATH}")

    con = duckdb.connect(str(DB_PATH))
    try:
        con.execute("BEGIN TRANSACTION")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS Fact_Shipment (
                shipment_id VARCHAR PRIMARY KEY,
                order_key INTEGER,
                carrier_key VARCHAR REFERENCES Dim_Carrier(carrier_id),
                warehouse_key VARCHAR REFERENCES Dim_Warehouse(warehouse_id),
                route_key VARCHAR REFERENCES Dim_Route(route_id),
                date_key INTEGER REFERENCES Dim_Date(date_key),
                lead_time INTEGER,
                scheduled_time INTEGER,
                delay_hours INTEGER,
                on_time BOOLEAN,
                sales DOUBLE,
                profit DOUBLE
            )
            """
        )
        con.execute("DROP TABLE IF EXISTS Fact_Shipment_new")
        con.execute(
            """
            CREATE TABLE Fact_Shipment_new AS
            WITH orders AS (
                SELECT
                    CAST("Order Item Id" AS VARCHAR) AS shipment_id,
                    CAST("Order Id" AS INTEGER) AS order_key,
                    regexp_replace(trim("Order Region"), '\\s+', ' ', 'g') AS order_region,
                    regexp_replace(trim("Market"), '\\s+', ' ', 'g') AS market,
                    CAST(strptime("order date (DateOrders)", '%m/%d/%Y %H:%M') AS DATE) AS order_date,
                    CAST("Days for shipping (real)" AS INTEGER) AS lead_time,
                    CAST("Days for shipment (scheduled)" AS INTEGER) AS scheduled_time,
                    CAST("Late_delivery_risk" AS INTEGER) AS late_delivery_risk,
                    CAST("Sales" AS DOUBLE) AS sales,
                    CAST("Order Profit Per Order" AS DOUBLE) AS profit
                FROM read_csv(
                    ?, header = true, encoding = 'utf-8',
                    all_varchar = true, ignore_errors = true
                )
            ),
            carriers AS (
                SELECT
                    carrier_id,
                    row_number() OVER (ORDER BY carrier_id) - 1 AS carrier_index,
                    count(*) OVER () AS carrier_count
                FROM Dim_Carrier
            )
            SELECT
                o.shipment_id,
                o.order_key,
                c.carrier_id AS carrier_key,
                w.warehouse_id AS warehouse_key,
                r.route_id AS route_key,
                d.date_key,
                o.lead_time,
                o.scheduled_time,
                (o.lead_time - o.scheduled_time) * 24 AS delay_hours,
                o.late_delivery_risk = 0 AS on_time,
                o.sales,
                o.profit
            FROM orders o
            JOIN Dim_Warehouse w
              ON regexp_replace(trim(w.region), '\\s+', ' ', 'g') = o.order_region
            JOIN Dim_Route r
              ON regexp_replace(trim(r.origin_market), '\\s+', ' ', 'g') = o.market
             AND regexp_replace(trim(r.destination_region), '\\s+', ' ', 'g') = o.order_region
            JOIN Dim_Date d ON d.full_date = o.order_date
            JOIN carriers c
              ON c.carrier_index = CAST(hash(o.shipment_id) % c.carrier_count AS BIGINT)
            """,
            [str(ORDERS_PATH)],
        )

        row_count, distinct_ids = con.execute(
            "SELECT count(*), count(DISTINCT shipment_id) FROM Fact_Shipment_new"
        ).fetchone()
        if row_count != 180_519 or distinct_ids != row_count:
            raise RuntimeError(
                f"Fact validation failed: rows={row_count}, distinct shipment_id={distinct_ids}"
            )

        null_fk_count = con.execute(
            """
            SELECT count(*) FROM Fact_Shipment_new
            WHERE carrier_key IS NULL OR warehouse_key IS NULL
               OR route_key IS NULL OR date_key IS NULL
            """
        ).fetchone()[0]
        if null_fk_count:
            raise RuntimeError(f"Fact validation failed: {null_fk_count} rows have null foreign keys")

        # Preserve the table object so dbt views that depend on it remain valid.
        con.execute("DELETE FROM Fact_Shipment")
        con.execute("INSERT INTO Fact_Shipment SELECT * FROM Fact_Shipment_new")
        con.execute("DROP TABLE Fact_Shipment_new")
        con.execute("COMMIT")
        print(f"Loaded Fact_Shipment successfully: {row_count:,} rows")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
