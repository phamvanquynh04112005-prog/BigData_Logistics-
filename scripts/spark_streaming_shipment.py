"""Persist real-time shipment status and delay alerts from Kafka.

Run from the repository root after Docker services are available:

    docker compose up -d minio zookeeper kafka
    venv\\Scripts\\python scripts\\spark_streaming_shipment.py --sink duckdb
    venv\\Scripts\\python scripts\\kafka_producer.py

The ``duckdb`` sink is the production/demo path.  It records every valid event
by ``event_id``, upserts the latest status for every ``shipment_id``, and writes
one durable alert for every ``DELAYED`` event.  The checkpoint plus idempotent
primary keys make a retried micro-batch safe: it cannot create duplicate alerts
or replace a newer shipment state with an older event.
"""

import argparse
from functools import partial
from pathlib import Path

import boto3
import duckdb
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession, functions as F, types as T

# Importing this module applies the project's Windows winutils setup before
# PySpark starts a JVM.
from spark_test_connection import (
    HADOOP_AWS_VERSION,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
)


APP_NAME = "shipment-tracking-stream"
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "shipment-tracking-events"
CURATED_BUCKET = "curated"
PARQUET_PATH = "s3a://curated/shipment_tracking_events/"
CHECKPOINT_ROOT = "s3a://curated/_checkpoints/shipment-tracking-stream-v2"
KAFKA_CONNECTOR = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
DEFAULT_DB_PATH = Path(__file__).resolve().parents[1] / "logistics.duckdb"
DELAYED_EVENT = "DELAYED"

EVENT_SCHEMA = T.StructType(
    [
        T.StructField("schema_version", T.StringType(), nullable=True),
        T.StructField("event_id", T.StringType(), nullable=True),
        T.StructField("shipment_id", T.LongType(), nullable=True),
        T.StructField("order_key", T.IntegerType(), nullable=True),
        T.StructField("carrier_id", T.StringType(), nullable=True),
        T.StructField("warehouse_id", T.StringType(), nullable=True),
        T.StructField("event_type", T.StringType(), nullable=True),
        T.StructField("event_timestamp", T.StringType(), nullable=True),
        T.StructField("region", T.StringType(), nullable=True),
    ]
)

TRACKING_DDL = """
CREATE TABLE IF NOT EXISTS shipment_tracking_event (
    event_id VARCHAR PRIMARY KEY,
    shipment_id VARCHAR NOT NULL,
    order_key INTEGER,
    carrier_id VARCHAR,
    warehouse_id VARCHAR NOT NULL,
    event_type VARCHAR NOT NULL,
    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    region VARCHAR,
    kafka_topic VARCHAR NOT NULL,
    kafka_partition BIGINT NOT NULL,
    kafka_offset BIGINT NOT NULL,
    kafka_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS latest_shipment_tracking (
    shipment_id VARCHAR PRIMARY KEY,
    event_id VARCHAR NOT NULL UNIQUE,
    order_key INTEGER,
    carrier_id VARCHAR,
    warehouse_id VARCHAR NOT NULL,
    event_type VARCHAR NOT NULL,
    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    region VARCHAR,
    kafka_topic VARCHAR NOT NULL,
    kafka_partition BIGINT NOT NULL,
    kafka_offset BIGINT NOT NULL,
    kafka_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS shipment_realtime_alert (
    event_id VARCHAR PRIMARY KEY,
    shipment_id VARCHAR NOT NULL,
    carrier_id VARCHAR,
    warehouse_id VARCHAR NOT NULL,
    event_type VARCHAR NOT NULL CHECK (event_type = 'DELAYED'),
    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    alert_status VARCHAR NOT NULL DEFAULT 'OPEN',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp
);

CREATE INDEX IF NOT EXISTS idx_tracking_event_shipment_time
    ON shipment_tracking_event (shipment_id, event_timestamp);
CREATE INDEX IF NOT EXISTS idx_realtime_alert_status_time
    ON shipment_realtime_alert (alert_status, event_timestamp);
"""


def create_streaming_spark_session() -> SparkSession:
    """Create one Spark session with the MinIO and Kafka connectors."""
    s3a_package = f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}"
    return (
        SparkSession.builder.appName(APP_NAME)
        .master("local[*]")
        .config("spark.jars.packages", f"{s3a_package},{KAFKA_CONNECTOR}")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
        .getOrCreate()
    )


def ensure_curated_bucket() -> None:
    """Create the MinIO bucket used by the optional Parquet sink/checkpoint."""
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


def ensure_tracking_tables(database_path: str | Path) -> None:
    """Create the durable event, latest-status, and alert tables if needed."""
    connection = duckdb.connect(str(database_path))
    try:
        connection.execute(TRACKING_DDL)
    finally:
        connection.close()


def build_tracking_events(spark: SparkSession, starting_offsets: str):
    """Read Kafka and retain the shipment key plus Kafka audit metadata.

    Invalid events are deliberately excluded before persistence.  A valid event
    must have a stable ``event_id``, a ``shipment_id``, an event type, warehouse,
    and a parseable event timestamp; this makes the downstream primary-key
    idempotency guarantees meaningful.
    """
    kafka_events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", starting_offsets)
        .load()
    )
    return (
        kafka_events.select(
            F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").cast("long").alias("kafka_partition"),
            F.col("offset").cast("long").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .select("event.*", "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp")
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .filter(
            F.col("event_id").isNotNull()
            & F.col("shipment_id").isNotNull()
            & F.col("warehouse_id").isNotNull()
            & F.col("event_type").isNotNull()
            & F.col("event_timestamp").isNotNull()
        )
        .select(
            "event_id",
            F.col("shipment_id").cast("string").alias("shipment_id"),
            "order_key",
            "carrier_id",
            "warehouse_id",
            "event_type",
            "event_timestamp",
            "region",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
        )
    )


def persist_realtime_batch(batch_df, batch_id: int, database_path: str) -> None:
    """Idempotently store one Spark micro-batch and emit durable delay alerts."""
    if batch_df.isEmpty():
        return

    events = batch_df.toPandas()
    connection = duckdb.connect(database_path)
    try:
        connection.register("microbatch_events", events)
        connection.execute("BEGIN TRANSACTION")
        connection.execute(
            """
            INSERT OR IGNORE INTO shipment_tracking_event (
                event_id, shipment_id, order_key, carrier_id, warehouse_id,
                event_type, event_timestamp, region, kafka_topic,
                kafka_partition, kafka_offset, kafka_timestamp
            )
            SELECT
                event_id, shipment_id, order_key, carrier_id, warehouse_id,
                event_type, event_timestamp, region, kafka_topic,
                kafka_partition, kafka_offset, kafka_timestamp
            FROM microbatch_events
            """
        )
        new_alerts = connection.execute(
            """
            SELECT DISTINCT
                incoming.event_id,
                incoming.shipment_id,
                incoming.warehouse_id,
                incoming.event_timestamp
            FROM microbatch_events incoming
            LEFT JOIN shipment_realtime_alert existing
                ON incoming.event_id = existing.event_id
            WHERE incoming.event_type = ?
              AND existing.event_id IS NULL
            """,
            [DELAYED_EVENT],
        ).fetchdf()
        connection.execute(
            """
            INSERT OR IGNORE INTO shipment_realtime_alert (
                event_id, shipment_id, carrier_id, warehouse_id,
                event_type, event_timestamp
            )
            SELECT
                event_id, shipment_id, carrier_id, warehouse_id,
                event_type, event_timestamp
            FROM microbatch_events
            WHERE event_type = ?
            """,
            [DELAYED_EVENT],
        )
        connection.execute(
            """
            MERGE INTO latest_shipment_tracking AS target
            USING (
                SELECT * EXCLUDE (row_number)
                FROM (
                    SELECT
                        *,
                        row_number() OVER (
                            PARTITION BY shipment_id
                            ORDER BY event_timestamp DESC, kafka_timestamp DESC,
                                     kafka_partition DESC, kafka_offset DESC, event_id DESC
                        ) AS row_number
                    FROM microbatch_events
                )
                WHERE row_number = 1
            ) AS source
            ON target.shipment_id = source.shipment_id
            WHEN MATCHED AND (
                source.event_timestamp > target.event_timestamp
                OR (
                    source.event_timestamp = target.event_timestamp
                    AND (
                        source.kafka_timestamp > target.kafka_timestamp
                        OR (
                            source.kafka_timestamp = target.kafka_timestamp
                            AND (
                                source.kafka_partition > target.kafka_partition
                                OR (
                                    source.kafka_partition = target.kafka_partition
                                    AND (
                                        source.kafka_offset > target.kafka_offset
                                        OR (
                                            source.kafka_offset = target.kafka_offset
                                            AND source.event_id > target.event_id
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            ) THEN UPDATE SET
                event_id = source.event_id,
                order_key = source.order_key,
                carrier_id = source.carrier_id,
                warehouse_id = source.warehouse_id,
                event_type = source.event_type,
                event_timestamp = source.event_timestamp,
                region = source.region,
                kafka_topic = source.kafka_topic,
                kafka_partition = source.kafka_partition,
                kafka_offset = source.kafka_offset,
                kafka_timestamp = source.kafka_timestamp,
                updated_at = current_timestamp
            WHEN NOT MATCHED THEN INSERT (
                shipment_id, event_id, order_key, carrier_id, warehouse_id,
                event_type, event_timestamp, region, kafka_topic,
                kafka_partition, kafka_offset, kafka_timestamp
            ) VALUES (
                source.shipment_id, source.event_id, source.order_key, source.carrier_id,
                source.warehouse_id, source.event_type, source.event_timestamp,
                source.region, source.kafka_topic, source.kafka_partition,
                source.kafka_offset, source.kafka_timestamp
            )
            """
        )
        connection.execute("COMMIT")
        delayed_count = len(new_alerts)
        print(
            f"Committed micro-batch {batch_id}: {len(events)} valid event(s), "
            f"{delayed_count} delay alert(s)."
        )
        if delayed_count:
            print("REALTIME ALERT")
            print(new_alerts[["shipment_id", "warehouse_id", "event_timestamp"]].to_string(index=False))
    except Exception:
        try:
            connection.execute("ROLLBACK")
        except duckdb.Error:
            pass
        raise
    finally:
        connection.close()


def start_query(tracking_events, sink: str, database_path: Path, checkpoint_id: str):
    """Start the requested streaming sink with a sink-specific checkpoint."""
    ensure_curated_bucket()
    checkpoint = f"{CHECKPOINT_ROOT}/{checkpoint_id}"
    if sink == "duckdb":
        ensure_tracking_tables(database_path)
        writer = (
            tracking_events.writeStream.foreachBatch(
                partial(persist_realtime_batch, database_path=str(database_path.resolve()))
            )
            .option("checkpointLocation", checkpoint)
            .outputMode("append")
        )
        return writer.start()
    if sink == "console":
        return (
            tracking_events.writeStream.option("checkpointLocation", checkpoint)
            .outputMode("append")
            .format("console")
            .option("truncate", "false")
            .start()
        )
    return (
        tracking_events.writeStream.option("checkpointLocation", checkpoint)
        .outputMode("append")
        .format("parquet")
        .option("path", PARQUET_PATH)
        .start()
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sink", choices=("duckdb", "console", "parquet"), default="duckdb")
    parser.add_argument(
        "--starting-offsets",
        choices=("latest", "earliest"),
        default="latest",
        help="Kafka offsets for a new checkpoint; use earliest only to rebuild history",
    )
    parser.add_argument(
        "--checkpoint-id",
        help="checkpoint subdirectory; defaults to the selected sink",
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="DuckDB file for --sink duckdb (default: logistics.duckdb)",
    )
    args = parser.parse_args()

    spark = create_streaming_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        query = start_query(
            build_tracking_events(spark, args.starting_offsets),
            args.sink,
            args.database,
            args.checkpoint_id or args.sink,
        )
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
