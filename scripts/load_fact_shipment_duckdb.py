r"""Load the PySpark-curated Fact_Shipment Parquet from MinIO into DuckDB.

This loader does not rebuild business fields from the raw CSV. PySpark owns
all cleansing, key generation, and metric calculations; this warehouse step
only transfers its curated output into the star schema.

Run from the repository root after MinIO is available:
    .venv\Scripts\python.exe scripts\load_fact_shipment_duckdb.py

For an offline copy of the same Parquet dataset:
    .venv\Scripts\python.exe scripts\load_fact_shipment_duckdb.py \
        --parquet-dir path\to\fact_shipment
"""

import argparse
import os
import tempfile
from pathlib import Path

import boto3
import duckdb
from botocore.exceptions import BotoCoreError, ClientError


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "logistics.duckdb"
EXPECTED_ROWS = 180_519
FACT_COLUMNS = (
    "shipment_id",
    "order_key",
    "carrier_key",
    "warehouse_key",
    "route_key",
    "date_key",
    "lead_time",
    "scheduled_time",
    "delay_hours",
    "on_time",
    "sales",
    "profit",
)


def minio_client(endpoint: str, access_key: str, secret_key: str):
    """Create an S3-compatible client for the local MinIO service."""
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )


def download_curated_parquet(
    destination: Path,
    endpoint: str,
    access_key: str,
    secret_key: str,
    bucket: str,
    prefix: str,
) -> int:
    """Download every curated Parquet object while retaining Hive folders."""
    client = minio_client(endpoint, access_key, secret_key)
    normalised_prefix = prefix.strip("/") + "/"
    file_count = 0

    try:
        pages = client.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=normalised_prefix
        )
        for page in pages:
            for item in page.get("Contents", []):
                key = item["Key"]
                if not key.lower().endswith(".parquet"):
                    continue
                relative = Path(key[len(normalised_prefix) :])
                local_path = destination / relative
                local_path.parent.mkdir(parents=True, exist_ok=True)
                client.download_file(bucket, key, str(local_path))
                file_count += 1
    except (BotoCoreError, ClientError) as error:
        raise RuntimeError(
            f"Cannot read MinIO path s3a://{bucket}/{normalised_prefix}. "
            "Start MinIO and ensure Khang's curated Parquet has been written."
        ) from error

    if not file_count:
        raise FileNotFoundError(
            f"No Parquet files found at s3a://{bucket}/{normalised_prefix}"
        )
    return file_count


def parquet_glob(parquet_dir: Path) -> str:
    """Return a DuckDB-compatible recursive glob after checking local input."""
    files = list(parquet_dir.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No Parquet files found under {parquet_dir}")
    return (parquet_dir / "**" / "*.parquet").as_posix()


def create_staging_fact(con: duckdb.DuckDBPyConnection, source_glob: str) -> None:
    """Project the Spark output into the warehouse's exact 12-column schema."""
    escaped_glob = source_glob.replace("'", "''")
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW curated_fact_parquet AS
        SELECT * FROM read_parquet(
            '{escaped_glob}', hive_partitioning=true, union_by_name=true
        )
        """
    )
    available = {
        row[0]
        for row in con.execute("DESCRIBE curated_fact_parquet").fetchall()
    }
    missing = sorted(set(FACT_COLUMNS) - available)
    if missing:
        raise RuntimeError(f"Curated Parquet is missing Fact columns: {missing}")

    con.execute("DROP TABLE IF EXISTS Fact_Shipment_new")
    con.execute(
        """
        CREATE TABLE Fact_Shipment_new AS
        SELECT
            CAST(shipment_id AS VARCHAR) AS shipment_id,
            CAST(order_key AS INTEGER) AS order_key,
            CAST(carrier_key AS VARCHAR) AS carrier_key,
            CAST(warehouse_key AS VARCHAR) AS warehouse_key,
            CAST(route_key AS VARCHAR) AS route_key,
            CAST(date_key AS INTEGER) AS date_key,
            CAST(lead_time AS INTEGER) AS lead_time,
            CAST(scheduled_time AS INTEGER) AS scheduled_time,
            CAST(delay_hours AS INTEGER) AS delay_hours,
            CAST(on_time AS BOOLEAN) AS on_time,
            CAST(sales AS DOUBLE) AS sales,
            CAST(profit AS DOUBLE) AS profit
        FROM curated_fact_parquet
        """,
    )


def validate_staging_fact(con: duckdb.DuckDBPyConnection, expected_rows: int) -> int:
    """Validate count, primary key, required values, and all dimension links."""
    row_count, distinct_ids = con.execute(
        "SELECT count(*), count(DISTINCT shipment_id) FROM Fact_Shipment_new"
    ).fetchone()
    if row_count != expected_rows or distinct_ids != row_count:
        raise RuntimeError(
            "Fact validation failed: "
            f"expected={expected_rows}, rows={row_count}, distinct shipment_id={distinct_ids}"
        )

    required = ("shipment_id", "carrier_key", "warehouse_key", "route_key", "date_key")
    null_predicate = " OR ".join(f"{column} IS NULL" for column in required)
    null_count = con.execute(
        f"SELECT count(*) FROM Fact_Shipment_new WHERE {null_predicate}"
    ).fetchone()[0]
    if null_count:
        raise RuntimeError(f"Fact validation failed: {null_count} rows have null keys")

    relationships = (
        ("carrier_key", "Dim_Carrier", "carrier_id"),
        ("warehouse_key", "Dim_Warehouse", "warehouse_id"),
        ("route_key", "Dim_Route", "route_id"),
        ("date_key", "Dim_Date", "date_key"),
    )
    for fact_key, dimension, dimension_key in relationships:
        orphan_count = con.execute(
            f"""
            SELECT count(*)
            FROM Fact_Shipment_new f
            LEFT JOIN {dimension} d ON f.{fact_key} = d.{dimension_key}
            WHERE d.{dimension_key} IS NULL
            """
        ).fetchone()[0]
        if orphan_count:
            raise RuntimeError(
                f"Fact validation failed: {orphan_count} orphan values in {fact_key}"
            )
    return row_count


def load_fact(parquet_dir: Path, expected_rows: int) -> int:
    """Atomically replace Fact rows without breaking dependent dbt views."""
    source_glob = parquet_glob(parquet_dir)
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
        create_staging_fact(con, source_glob)
        row_count = validate_staging_fact(con, expected_rows)
        con.execute("DELETE FROM Fact_Shipment")
        con.execute("INSERT INTO Fact_Shipment SELECT * FROM Fact_Shipment_new")
        con.execute("DROP TABLE Fact_Shipment_new")
        con.execute("COMMIT")
        return row_count
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--parquet-dir",
        type=Path,
        help="offline curated Parquet directory; default downloads from MinIO",
    )
    parser.add_argument("--endpoint", default=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"))
    parser.add_argument("--access-key", default=os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
    parser.add_argument("--secret-key", default=os.getenv("MINIO_SECRET_KEY", "minioadmin123"))
    parser.add_argument("--bucket", default="curated")
    parser.add_argument("--prefix", default="fact_shipment")
    parser.add_argument("--expected-rows", type=int, default=EXPECTED_ROWS)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.parquet_dir:
        row_count = load_fact(args.parquet_dir.resolve(), args.expected_rows)
        print(f"Loaded Fact_Shipment from curated Parquet: {row_count:,} rows")
        return

    with tempfile.TemporaryDirectory(prefix="fact_shipment_minio_") as temp_dir:
        local_dir = Path(temp_dir)
        file_count = download_curated_parquet(
            local_dir,
            args.endpoint,
            args.access_key,
            args.secret_key,
            args.bucket,
            args.prefix,
        )
        print(f"Downloaded {file_count} curated Parquet file(s) from MinIO")
        row_count = load_fact(local_dir, args.expected_rows)
    print(f"Loaded Fact_Shipment from curated Parquet: {row_count:,} rows")


if __name__ == "__main__":
    main()
