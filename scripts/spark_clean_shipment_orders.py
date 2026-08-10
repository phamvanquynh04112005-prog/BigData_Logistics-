"""Clean the shipment-order source for the Task 2 Spark batch stage.

The source stays immutable in MinIO.  This job only prepares the small set of
fields needed by the later Fact_Shipment transformations; it does not join any
dimensions, generate keys, or write an output zone.

Run from the repository root after the raw object has been uploaded to MinIO:
    venv\\Scripts\\python scripts\\spark_clean_shipment_orders.py --verify
"""

import argparse
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession, functions as F

from spark_test_connection import create_spark_session


ORDERS_PATH = "s3a://raw/orders/DataCoSupplyChainDataset.csv"
EXPECTED_SOURCE_ROWS = 180_519
EXPECTED_SOURCE_COLUMNS = 53
DATE_FORMAT = "M/d/yyyy H:mm"

# Keep this mapping explicit: these are exactly the raw fields Task 2 needs.
SOURCE_TO_CLEAN = {
    "Order Item Id": "order_item_id",
    "Order Id": "order_id",
    "order date (DateOrders)": "order_date_raw",
    "Days for shipping (real)": "days_for_shipping_real",
    "Days for shipment (scheduled)": "days_for_shipment_scheduled",
    "Late_delivery_risk": "late_delivery_risk",
    "Sales": "sales",
    "Order Profit Per Order": "order_profit_per_order",
    "Order Region": "order_region",
    "Market": "market",
}


def read_orders(spark: SparkSession) -> DataFrame:
    """Read the raw CSV through S3A while preserving Latin-1 characters."""
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        # Spark 4.2 accepts the canonical Java charset name, not the
        # ``latin1`` alias used by pandas.
        .option("encoding", "iso-8859-1")
        .csv(ORDERS_PATH)
    )


def clean_orders(source: DataFrame) -> DataFrame:
    """Rename Task 2 fields to snake_case and parse the order timestamp."""
    missing_columns = sorted(set(SOURCE_TO_CLEAN) - set(source.columns))
    if missing_columns:
        raise ValueError(f"Raw orders source is missing required columns: {missing_columns}")

    selected = source.select(
        *(F.col(source_name).alias(clean_name) for source_name, clean_name in SOURCE_TO_CLEAN.items())
    )
    return selected.withColumn("order_date", F.to_timestamp("order_date_raw", DATE_FORMAT))


def null_counts(frame: DataFrame, columns: Iterable[str]) -> dict[str, int]:
    """Return one null count per column with one Spark aggregation job."""
    aggregates = [
        F.sum(F.when(F.col(column).isNull(), F.lit(1)).otherwise(F.lit(0))).alias(column)
        for column in columns
    ]
    result = frame.agg(*aggregates).first().asDict()
    return {column: int(result[column] or 0) for column in columns}


def log_data_quality(cleaned: DataFrame) -> int:
    """Log nulls for every cleansed field and return date parse failures."""
    counts = null_counts(cleaned, cleaned.columns)
    print("Null counts by cleansed column:")
    for column, count in counts.items():
        print(f"  {column}: {count}")

    parse_failures = cleaned.filter(
        F.col("order_date_raw").isNotNull()
        & (F.trim(F.col("order_date_raw")) != "")
        & F.col("order_date").isNull()
    ).count()
    print(f"Order-date parse failures: {parse_failures}")
    return parse_failures


def verify_source(source: DataFrame, parse_failures: int) -> None:
    """Validate the known source shape and that parsing produced no new nulls."""
    source_rows = source.count()
    source_columns = len(source.columns)
    print(f"Source rows: {source_rows}; source columns: {source_columns}")

    failures = []
    if source_rows != EXPECTED_SOURCE_ROWS:
        failures.append(f"expected {EXPECTED_SOURCE_ROWS} source rows, got {source_rows}")
    if source_columns != EXPECTED_SOURCE_COLUMNS:
        failures.append(f"expected {EXPECTED_SOURCE_COLUMNS} source columns, got {source_columns}")
    if parse_failures:
        failures.append(f"found {parse_failures} order_date parse failures")
    if failures:
        raise RuntimeError("Task 2 verification failed: " + "; ".join(failures))

    print("Task 2 verification passed.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--verify",
        action="store_true",
        help="assert the known raw row/column counts and zero date parse failures",
    )
    args = parser.parse_args()

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        source = read_orders(spark)
        cleaned = clean_orders(source).cache()
        print("Cleansed schema:")
        cleaned.printSchema()
        print(f"Cleansed rows: {cleaned.count()}")
        parse_failures = log_data_quality(cleaned)

        if args.verify:
            verify_source(source, parse_failures)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
