"""Write the Task 2 and Task 4 shipment outputs to MinIO Parquet zones.

This is the Task 5 storage job.  It creates the ``cleansed`` and ``curated``
buckets when needed, writes the cleansed shipment orders and the completed
Fact_Shipment, then reads both outputs back and checks that no rows were lost.

Run from the repository root after the raw source and dimensions are available:
    venv\\Scripts\\python scripts\\spark_write_shipment_parquet.py --verify
"""

import argparse
from pathlib import Path
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError
from pyspark.sql import DataFrame, SparkSession, functions as F

from spark_build_fact_shipment import build_fact_shipment
from spark_clean_shipment_orders import clean_orders, read_orders
from spark_generate_shipment_foreign_keys import add_foreign_keys, read_dimensions
from spark_test_connection import (
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    create_spark_session,
)


CLEANSED_BUCKET = "cleansed"
CURATED_BUCKET = "curated"
CLEANSED_PATH = "s3a://cleansed/shipment_orders/"
CURATED_PATH = "s3a://curated/fact_shipment/"

# The cleansed dataset has only 180k rows, so a small fixed number of files is
# preferable to Spark's default number of shuffle partitions.  Fact_Shipment is
# repartitioned by its partition keys below, which gives one writer per actual
# month/warehouse partition instead of arbitrary tiny files.
CLEANSED_OUTPUT_PARTITIONS = 8


def configure_s3a_write_buffer(spark: SparkSession) -> None:
    """Use a project-local temporary directory for S3A's buffered writes.

    Hadoop's Windows default can resolve to an unavailable ``/tmp`` directory.
    The byte-buffer uploader avoids its native Windows disk-access dependency;
    the directory setting remains a safe fallback for any disk-backed writer.
    """
    buffer_dir = Path(__file__).resolve().parents[1] / ".spark-s3a-buffer"
    buffer_dir.mkdir(exist_ok=True)
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.buffer.dir", str(buffer_dir))
    hadoop_conf.set("fs.s3a.fast.upload.buffer", "bytebuffer")


def create_bucket_if_missing(bucket: str) -> None:
    """Create a MinIO bucket idempotently through its S3-compatible API."""
    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )
    try:
        client.head_bucket(Bucket=bucket)
        print(f"Bucket '{bucket}' already exists.")
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code", "")
        if error_code not in {"404", "NoSuchBucket", "NotFound"}:
            raise
        client.create_bucket(Bucket=bucket)
        print(f"Created bucket '{bucket}'.")


def create_output_buckets() -> None:
    """Ensure the Task 5 cleansed and curated zones exist before writing."""
    create_bucket_if_missing(CLEANSED_BUCKET)
    create_bucket_if_missing(CURATED_BUCKET)


def build_task_outputs(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    """Build the existing Task 2 and Task 4 DataFrames without changing them."""
    cleansed = clean_orders(read_orders(spark))
    enriched = add_foreign_keys(cleansed, *read_dimensions(spark))
    return cleansed, build_fact_shipment(enriched)


def write_cleansed_parquet(cleansed: DataFrame, mode: str) -> None:
    """Persist the Task 2 output as a compact set of Parquet files."""
    (
        cleansed.repartition(CLEANSED_OUTPUT_PARTITIONS)
        .write.mode(mode)
        .parquet(CLEANSED_PATH)
    )


def write_curated_parquet(fact: DataFrame, mode: str) -> None:
    """Persist Fact_Shipment partitioned by month and warehouse.

    ``shipment_month`` is derived only to make storage partitions practical:
    daily ``date_key`` partitions would create a large number of tiny files.
    Spark recovers this partition column automatically when the Parquet path is
    read.  The 12 Fact_Shipment business columns themselves remain unchanged.
    """
    partitioned_fact = fact.withColumn(
        "shipment_month",
        F.substring(F.col("date_key").cast("string"), 1, 6),
    )
    (
        partitioned_fact.repartition("shipment_month", "warehouse_key")
        .write.mode(mode)
        .partitionBy("shipment_month", "warehouse_key")
        .parquet(CURATED_PATH)
    )


def minio_client():
    """Return a client for inspecting Task 5 outputs after Spark writes them."""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def output_storage_counts(path: str) -> tuple[int, int]:
    """Count Parquet objects and Fact partition directories in MinIO."""
    parsed = urlparse(path)
    bucket, prefix = parsed.netloc, parsed.path.lstrip("/")
    file_count = 0
    partitions: set[str] = set()
    for page in minio_client().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            if not key.endswith(".parquet"):
                continue
            file_count += 1
            if "shipment_month=" in key and "warehouse_key=" in key:
                partitions.add(key.rsplit("/", 1)[0])
    return file_count, len(partitions)


def verify_round_trip(source: DataFrame, path: str, label: str, spark: SparkSession) -> None:
    """Read a written dataset back and assert its row count is unchanged."""
    source_count = source.count()
    written_count = spark.read.parquet(path).count()
    if source_count != written_count:
        raise RuntimeError(
            f"{label} Parquet round-trip lost rows: source={source_count}, read_back={written_count}"
        )
    print(f"{label} round-trip passed: {written_count} rows.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=("overwrite", "errorifexists"),
        default="overwrite",
        help="write mode for both output paths (default: overwrite for idempotent reruns)",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="read both Parquet outputs back and assert their row counts",
    )
    args = parser.parse_args()

    create_output_buckets()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        configure_s3a_write_buffer(spark)
        cleansed, fact = build_task_outputs(spark)
        write_cleansed_parquet(cleansed, args.mode)
        write_curated_parquet(fact, args.mode)

        if args.verify:
            verify_round_trip(cleansed, CLEANSED_PATH, "Cleansed shipment_orders", spark)
            verify_round_trip(fact, CURATED_PATH, "Curated Fact_Shipment", spark)

        cleansed_files, _ = output_storage_counts(CLEANSED_PATH)
        curated_files, curated_partitions = output_storage_counts(CURATED_PATH)
        print(f"Cleansed Parquet files: {cleansed_files}")
        print(f"Curated Parquet files: {curated_files}; partitions: {curated_partitions}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
