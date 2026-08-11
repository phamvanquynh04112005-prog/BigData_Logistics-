r"""Create and load the logistics star schema into BigQuery.

Prerequisites:
    pip install google-cloud-bigquery
    gcloud auth application-default login

Example:
    .venv\Scripts\python.exe scripts\load_bigquery.py \
        --project my-gcp-project --dataset logistics

The local DuckDB warehouse must already contain all five source tables. Loads
use WRITE_TRUNCATE, wait for every BigQuery job, and verify final row counts.
"""

import argparse
import csv
import tempfile
from pathlib import Path

import duckdb

try:
    from google.cloud import bigquery
except ImportError as error:
    raise SystemExit(
        "Missing dependency: install it with "
        "`.venv\\Scripts\\python.exe -m pip install google-cloud-bigquery`."
    ) from error


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "logistics.duckdb"
DDL_PATH = ROOT / "sql" / "ddl_bigquery.sql"

TABLES = {
    "Dim_Carrier": [
        bigquery.SchemaField("carrier_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("carrier_name", "STRING"),
        bigquery.SchemaField("service_type", "STRING"),
    ],
    "Dim_Warehouse": [
        bigquery.SchemaField("warehouse_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("warehouse_name", "STRING"),
        bigquery.SchemaField("region", "STRING"),
        bigquery.SchemaField("capacity_units", "INTEGER"),
    ],
    "Dim_Route": [
        bigquery.SchemaField("route_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("origin_market", "STRING"),
        bigquery.SchemaField("destination_region", "STRING"),
    ],
    "Dim_Date": [
        bigquery.SchemaField("date_key", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("full_date", "DATE"),
        bigquery.SchemaField("day", "INTEGER"),
        bigquery.SchemaField("month", "INTEGER"),
        bigquery.SchemaField("quarter", "INTEGER"),
        bigquery.SchemaField("year", "INTEGER"),
        bigquery.SchemaField("day_of_week", "STRING"),
        bigquery.SchemaField("is_weekend", "BOOLEAN"),
    ],
    "Fact_Shipment": [
        bigquery.SchemaField("shipment_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("order_key", "INTEGER"),
        bigquery.SchemaField("carrier_key", "STRING"),
        bigquery.SchemaField("warehouse_key", "STRING"),
        bigquery.SchemaField("route_key", "STRING"),
        bigquery.SchemaField("date_key", "INTEGER"),
        bigquery.SchemaField("lead_time", "INTEGER"),
        bigquery.SchemaField("scheduled_time", "INTEGER"),
        bigquery.SchemaField("delay_hours", "INTEGER"),
        bigquery.SchemaField("on_time", "BOOLEAN"),
        bigquery.SchemaField("sales", "FLOAT"),
        bigquery.SchemaField("profit", "FLOAT"),
        bigquery.SchemaField("shipment_date", "DATE", mode="REQUIRED"),
    ],
}


def export_csv(con: duckdb.DuckDBPyConnection, table: str, path: Path) -> int:
    if table == "Fact_Shipment":
        query = """
            SELECT f.*, d.full_date AS shipment_date
            FROM Fact_Shipment f JOIN Dim_Date d USING (date_key)
        """
    else:
        query = f'SELECT * FROM "{table}"'
    rows = con.execute(f"SELECT count(*) FROM ({query})").fetchone()[0]
    con.execute(f"COPY ({query}) TO ? (HEADER, DELIMITER ',')", [str(path)])
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True, help="Google Cloud project ID")
    parser.add_argument("--dataset", required=True, help="BigQuery dataset ID")
    parser.add_argument("--location", default="US", help="BigQuery location (default: US)")
    args = parser.parse_args()

    client = bigquery.Client(project=args.project, location=args.location)
    dataset_ref = f"{args.project}.{args.dataset}"
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = args.location
    client.create_dataset(dataset, exists_ok=True)

    ddl = DDL_PATH.read_text(encoding="utf-8")
    ddl = ddl.replace("<project_id>", args.project).replace("<dataset_id>", args.dataset)
    client.query(ddl).result()

    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        with tempfile.TemporaryDirectory(prefix="logistics_bigquery_") as temp_dir:
            for table, schema in TABLES.items():
                csv_path = Path(temp_dir) / f"{table}.csv"
                local_rows = export_csv(con, table, csv_path)
                config = bigquery.LoadJobConfig(
                    schema=schema,
                    source_format=bigquery.SourceFormat.CSV,
                    skip_leading_rows=1,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                )
                with csv_path.open("rb") as source:
                    client.load_table_from_file(
                        source, f"{dataset_ref}.{table}", job_config=config
                    ).result()
                remote_rows = client.get_table(f"{dataset_ref}.{table}").num_rows
                if remote_rows != local_rows:
                    raise RuntimeError(
                        f"{table}: local rows={local_rows}, BigQuery rows={remote_rows}"
                    )
                print(f"Loaded {table}: {remote_rows:,} rows")
    finally:
        con.close()


if __name__ == "__main__":
    main()
