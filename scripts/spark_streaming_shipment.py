"""Aggregate shipment-tracking events from Kafka with Spark Structured Streaming.

Run from the repository root in two terminals:
    venv\\Scripts\\python scripts\\kafka_producer.py
    venv\\Scripts\\python scripts\\spark_streaming_shipment.py

The default console sink is for the Task 6 demo.  Persist completed windows to
MinIO Parquet instead with:
    venv\\Scripts\\python scripts\\spark_streaming_shipment.py --sink parquet
"""

import argparse

import boto3
from botocore.exceptions import ClientError
# Importing this module first applies its Windows winutils setup before
# PySpark starts a JVM.  This is required for local checkpoint directories.
from spark_test_connection import (
    HADOOP_AWS_VERSION,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
)
from pyspark.sql import SparkSession, functions as F, types as T


APP_NAME = "shipment-tracking-stream"
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "shipment-tracking-events"
CURATED_BUCKET = "curated"
PARQUET_PATH = "s3a://curated/streaming_shipment_status/"
# Keep streaming checkpoints in MinIO too.  It avoids Hadoop NativeIO issues
# with local checkpoint folders on Windows and preserves Kafka offsets across
# restarts.
CHECKPOINT_ROOT = "s3a://curated/_checkpoints/shipment-tracking-stream"
KAFKA_CONNECTOR = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"

EVENT_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType(), nullable=True),
        T.StructField("shipment_id", T.IntegerType(), nullable=True),
        T.StructField("carrier_id", T.StringType(), nullable=True),
        T.StructField("warehouse_id", T.StringType(), nullable=True),
        T.StructField("event_type", T.StringType(), nullable=True),
        T.StructField("event_timestamp", T.StringType(), nullable=True),
        T.StructField("region", T.StringType(), nullable=True),
    ]
)


# Khởi tạo Spark với đồng thời connector Kafka và cấu hình S3A MinIO.
def create_streaming_spark_session() -> SparkSession:
    """Create one session with both the MinIO and Kafka connector packages."""
    s3a_package = f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}"
    # Both packages must be configured before SparkContext starts; changing
    # spark.jars.packages afterward does not add the Kafka source to its JVM.
    return (
        SparkSession.builder.appName(APP_NAME)
        .master("local[*]")
        .config("spark.jars.packages", f"{s3a_package},{KAFKA_CONNECTOR}")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Avoid Hadoop's disk-backed S3A buffer, which calls NativeIO on
        # Windows while writing checkpoint metadata.
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
        .getOrCreate()
    )


# Đảm bảo bucket curated tồn tại trước khi streaming ghi output/checkpoint.
def ensure_curated_bucket() -> None:
    """Create the Task 6 destination bucket when it does not yet exist."""
    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )
    try:
        client.head_bucket(Bucket=CURATED_BUCKET)
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") not in {"404", "NoSuchBucket", "NotFound"}:
            raise
        client.create_bucket(Bucket=CURATED_BUCKET)
        print(f"Created bucket '{CURATED_BUCKET}'.")


# Parse sự kiện Kafka và tổng hợp event type theo kho trong cửa sổ một phút.
def build_windowed_events(spark: SparkSession):
    """Parse Kafka JSON and count event types by warehouse in one-minute windows."""
    kafka_events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )
    parsed = (
        kafka_events.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event"))
        .select("event.*")
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .filter(F.col("event_timestamp").isNotNull())
    )
    return (
        parsed.withWatermark("event_timestamp", "5 minutes")
        .groupBy(
            F.window("event_timestamp", "1 minute"),
            F.col("event_type"),
            F.col("warehouse_id"),
        )
        .count()
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "event_type",
            "warehouse_id",
            "count",
        )
    )


# Khởi chạy sink console để demo hoặc Parquet để lưu kết quả cửa sổ hoàn tất.
def start_query(windowed_events, sink: str):
    """Start the requested debug or persistent output sink."""
    ensure_curated_bucket()
    writer = windowed_events.writeStream.option("checkpointLocation", f"{CHECKPOINT_ROOT}/{sink}")
    if sink == "console":
        return writer.outputMode("update").format("console").option("truncate", "false").start()

    # File sinks do not support update mode.  Append writes each finalized
    # event-time window after its watermark has passed it.
    return writer.outputMode("append").format("parquet").option("path", PARQUET_PATH).start()


# Điều phối Structured Streaming và dừng query/Spark an toàn khi ngắt.
def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sink", choices=("console", "parquet"), default="console")
    args = parser.parse_args()

    spark = create_streaming_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        query = start_query(build_windowed_events(spark), args.sink)
        print(f"Streaming from '{TOPIC}' to {args.sink}. Press Ctrl+C to stop.")
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping streaming query.")
    finally:
        for query in spark.streams.active:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
