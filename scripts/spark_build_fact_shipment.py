"""Build the Task 4 Fact_Shipment DataFrame from the Task 3 output.

This job only performs the business calculations and schema projection for
Fact_Shipment. It deliberately does not write Parquet (Task 5).

Run from the repository root after Task 2/3 inputs are available:
    venv\\Scripts\\python scripts\\spark_build_fact_shipment.py --verify
"""

import argparse
from typing import Sequence

from pyspark.sql import DataFrame, functions as F

from spark_clean_shipment_orders import clean_orders, read_orders
from spark_generate_shipment_foreign_keys import add_foreign_keys, read_dimensions
from spark_test_connection import create_spark_session


# Column order and data types match Fact_Shipment in sql/ddl_duckdb_postgres.sql.
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

REQUIRED_TASK3_COLUMNS = (
    "order_item_id",
    "order_id",
    "carrier_key",
    "warehouse_key",
    "route_key",
    "date_key",
    "days_for_shipping_real",
    "days_for_shipment_scheduled",
    "late_delivery_risk",
    "sales",
    "order_profit_per_order",
)


def _require_columns(frame: DataFrame, required: Sequence[str]) -> None:
    missing = sorted(set(required) - set(frame.columns))
    if missing:
        raise ValueError(f"Task 3 DataFrame is missing required columns: {missing}")


def build_fact_shipment(enriched: DataFrame) -> DataFrame:
    """Calculate Task 4 fields and project the DDL's 12-column schema.

    ``delay_hours`` intentionally retains negative values: they represent early
    deliveries and are useful for later SLA analysis.
    """
    _require_columns(enriched, REQUIRED_TASK3_COLUMNS)

    lead_time = F.col("days_for_shipping_real").cast("int")
    scheduled_time = F.col("days_for_shipment_scheduled").cast("int")

    return enriched.select(
        F.col("order_item_id").cast("string").alias("shipment_id"),
        F.col("order_id").cast("int").alias("order_key"),
        F.col("carrier_key").cast("string").alias("carrier_key"),
        F.col("warehouse_key").cast("string").alias("warehouse_key"),
        F.col("route_key").cast("string").alias("route_key"),
        F.col("date_key").cast("int").alias("date_key"),
        lead_time.alias("lead_time"),
        scheduled_time.alias("scheduled_time"),
        ((lead_time - scheduled_time) * F.lit(24)).cast("int").alias("delay_hours"),
        (F.col("late_delivery_risk").cast("int") == F.lit(0)).alias("on_time"),
        F.col("sales").cast("double").alias("sales"),
        F.col("order_profit_per_order").cast("double").alias("profit"),
    )


def log_duplicate_shipment_ids(fact: DataFrame) -> int:
    """Log duplicate shipment IDs for the team without silently dropping rows."""
    duplicate_groups = fact.groupBy("shipment_id").count().filter(F.col("count") > 1)
    duplicate_group_count = duplicate_groups.count()
    if duplicate_group_count:
        print(f"WARNING: found {duplicate_group_count} duplicate shipment_id group(s).")
        duplicate_groups.orderBy(F.desc("count"), "shipment_id").show(20, truncate=False)
    else:
        print("shipment_id uniqueness check passed: no duplicates found.")
    return duplicate_group_count


def verify_fact_shipment(fact: DataFrame) -> None:
    """Assert the Task 4 projection has the DDL column order and unique PK."""
    if tuple(fact.columns) != FACT_COLUMNS:
        raise RuntimeError(f"Fact_Shipment columns do not match DDL order: {fact.columns}")
    if log_duplicate_shipment_ids(fact):
        raise RuntimeError("Task 4 verification failed: shipment_id is not unique")
    print("Task 4 verification passed.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--verify",
        action="store_true",
        help="fail when shipment_id duplicates are found",
    )
    args = parser.parse_args()

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        task3_output = add_foreign_keys(
            clean_orders(read_orders(spark)),
            *read_dimensions(spark),
        )
        fact = build_fact_shipment(task3_output).cache()
        print("Fact_Shipment schema:")
        fact.printSchema()
        print(f"Fact_Shipment rows: {fact.count()}")

        if args.verify:
            verify_fact_shipment(fact)
        else:
            log_duplicate_shipment_ids(fact)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
