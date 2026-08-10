"""Generate the four Fact_Shipment foreign keys required by Task 3.

This job deliberately stops after enriching the cleansed Task 2 DataFrame: it
does not calculate Fact_Shipment business fields and does not write Parquet.

Run from the repository root after the raw source and both simulated
dimensions have been uploaded to MinIO:
    venv\\Scripts\\python scripts\\spark_generate_shipment_foreign_keys.py --verify
"""

import argparse
from pathlib import Path
from typing import Sequence

from pyspark.sql import DataFrame, SparkSession, functions as F

from spark_clean_shipment_orders import clean_orders, read_orders
from spark_test_connection import create_spark_session


WAREHOUSES_PATH = "s3a://raw/dim_warehouse/Dim_Warehouse.csv"
CARRIERS_PATH = "s3a://raw/dim_carrier/Dim_Carrier.csv"
ROUTES_PATH = "data/simulated/Dim_Route.csv"
DATES_PATH = "data/simulated/Dim_Date.csv"


# Đọc bốn dimension từ các vị trí S3A và local đã quy định.
def read_dimensions(spark: SparkSession) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    """Read Task 3 dimensions from their specified S3A/local locations."""
    csv_reader = spark.read.option("header", "true").option("inferSchema", "true")
    warehouses = csv_reader.option("encoding", "iso-8859-1").csv(WAREHOUSES_PATH)
    carriers = csv_reader.option("encoding", "iso-8859-1").csv(CARRIERS_PATH)
    routes = csv_reader.csv(ROUTES_PATH)
    dates = csv_reader.csv(DATES_PATH)
    return warehouses, carriers, routes, dates


# Kiểm tra DataFrame có đủ các cột bắt buộc trước khi xử lý.
def _require_columns(frame: DataFrame, required: Sequence[str], name: str) -> None:
    missing = sorted(set(required) - set(frame.columns))
    if missing:
        raise ValueError(f"{name} is missing required columns: {missing}")


# Chuẩn hoá text join bằng cách trim và gộp khoảng trắng liên tiếp.
def _normalise_text(column: str) -> F.Column:
    """Trim and collapse internal whitespace so logically equal keys join."""
    return F.regexp_replace(F.trim(F.col(column)), r"\s+", " ")


# Chặn dimension có khóa join trùng để tránh nhân bản shipment khi join.
def _assert_unique_key(frame: DataFrame, key_columns: Sequence[str], name: str) -> None:
    """Fail before a join that could multiply shipment rows."""
    duplicates = frame.groupBy(*key_columns).count().filter(F.col("count") > 1).limit(1).count()
    if duplicates:
        raise ValueError(f"{name} has duplicate join keys: {list(key_columns)}")


# Log và xác nhận số dòng không đổi qua từng phép join.
def _log_join_count(stage: str, before: int, after: int) -> None:
    print(f"{stage}: rows before={before}, rows after={after}")
    if before != after:
        raise RuntimeError(f"{stage} changed row count from {before} to {after}")


# Bổ sung warehouse, route, date và carrier key cho dữ liệu Task 2.
def add_foreign_keys(
    cleaned: DataFrame,
    warehouses: DataFrame,
    carriers: DataFrame,
    routes: DataFrame,
    dates: DataFrame,
) -> DataFrame:
    """Enrich Task 2 records with warehouse, route, date and carrier keys.

    Carrier assignment is deterministic: ``xxhash64(order_item_id)`` is
    reduced modulo the number of carriers and selects from carrier IDs sorted
    lexicographically.  Thus the same shipment always maps to the same carrier
    on a rerun; no non-deterministic random function is used.  ``order_item_id``
    is the Task 2 name for the shipment identifier and is renamed to
    ``shipment_id`` in Task 4.
    """
    _require_columns(cleaned, ["order_item_id", "order_region", "market", "order_date"], "Task 2 DataFrame")
    _require_columns(warehouses, ["warehouse_id", "region"], "Dim_Warehouse")
    _require_columns(carriers, ["carrier_id"], "Dim_Carrier")
    _require_columns(routes, ["route_id", "origin_market", "destination_region"], "Dim_Route")
    _require_columns(dates, ["date_key"], "Dim_Date")

    warehouse_lookup = warehouses.select(
        _normalise_text("region").alias("warehouse_region_join"),
        F.col("warehouse_id").cast("string").alias("warehouse_key"),
    )
    route_lookup = routes.select(
        _normalise_text("origin_market").alias("route_market_join"),
        _normalise_text("destination_region").alias("route_region_join"),
        F.col("route_id").cast("string").alias("route_key"),
    )
    date_lookup = dates.select(F.col("date_key").cast("int").alias("date_key_lookup"))
    carrier_ids = [row.carrier_id for row in carriers.select(F.col("carrier_id").cast("string").alias("carrier_id")).distinct().orderBy("carrier_id").collect()]
    if not carrier_ids:
        raise ValueError("Dim_Carrier has no carrier_id values")

    _assert_unique_key(warehouse_lookup, ["warehouse_region_join"], "Dim_Warehouse")
    _assert_unique_key(route_lookup, ["route_market_join", "route_region_join"], "Dim_Route")
    _assert_unique_key(date_lookup, ["date_key_lookup"], "Dim_Date")

    enriched = cleaned.withColumn("warehouse_region_join", _normalise_text("order_region"))
    before = enriched.count()
    enriched = enriched.join(warehouse_lookup, "warehouse_region_join", "left").drop("warehouse_region_join")
    _log_join_count("Warehouse join", before, enriched.count())

    enriched = (
        enriched.withColumn("route_market_join", _normalise_text("market"))
        .withColumn("route_region_join", _normalise_text("order_region"))
    )
    before = enriched.count()
    enriched = (
        enriched.join(route_lookup, ["route_market_join", "route_region_join"], "left")
        .drop("route_market_join", "route_region_join")
    )
    _log_join_count("Route join", before, enriched.count())

    enriched = enriched.withColumn("date_key", F.date_format("order_date", "yyyyMMdd").cast("int"))
    before = enriched.count()
    enriched = enriched.join(
        date_lookup,
        enriched.date_key == date_lookup.date_key_lookup,
        "left",
    ).drop("date_key_lookup")
    _log_join_count("Date validation join", before, enriched.count())

    # ``pmod`` keeps the index non-negative even when xxhash64 returns a
    # negative 64-bit value. element_at uses Spark's one-based array indexes.
    carrier_array = F.array(*[F.lit(carrier_id) for carrier_id in carrier_ids])
    enriched = enriched.withColumn(
        "carrier_key",
        F.element_at(
            carrier_array,
            (
                F.pmod(F.xxhash64(F.col("order_item_id").cast("string")), F.lit(len(carrier_ids)))
                + F.lit(1)
            ).cast("int"),
        ),
    )
    return enriched


# Kiểm tra bốn khóa ngoại đều có giá trị sau khi enrich.
def verify_foreign_keys(enriched: DataFrame) -> None:
    """Validate lookup coverage after all Task 3 operations."""
    nulls = enriched.agg(
        *[
            F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias(column)
            for column in ("warehouse_key", "route_key", "date_key", "carrier_key")
        ]
    ).first().asDict()
    print("Foreign-key null counts:")
    for column, count in nulls.items():
        print(f"  {column}: {int(count or 0)}")

    failures = [column for column, count in nulls.items() if count]
    if failures:
        raise RuntimeError(f"Task 3 lookup coverage failed for: {', '.join(failures)}")
    print("Task 3 verification passed.")


# Điều phối job Task 3 và tuỳ chọn kiểm tra coverage khóa ngoại.
def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify", action="store_true", help="fail if any generated foreign key is null")
    args = parser.parse_args()

    for path in (ROUTES_PATH, DATES_PATH):
        if not Path(path).is_file():
            raise FileNotFoundError(f"Required local dimension is missing: {path}")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        cleaned = clean_orders(read_orders(spark))
        enriched = add_foreign_keys(cleaned, *read_dimensions(spark)).cache()
        print("Task 3 schema:")
        enriched.printSchema()
        print(f"Task 3 rows: {enriched.count()}")
        if args.verify:
            verify_foreign_keys(enriched)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
