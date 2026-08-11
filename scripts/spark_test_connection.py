"""Verify that local PySpark can read the project data from MinIO via S3A.

Run from the repository root after starting the local services:
    docker compose up -d
    venv\\Scripts\\python scripts\\spark_test_connection.py

The Hadoop 3.5.0 artifact matches the Hadoop runtime bundled with PySpark
4.2.0. Spark/Ivy downloads it and its compatible AWS SDK bundle to its local
cache on the first run; they are then reused by subsequent runs.
"""

import os
import sys
from pathlib import Path

# PySpark on Windows invokes winutils.exe while distributing Maven-resolved
# JARs. The setup below is ignored on Linux/macOS and lets this repository use
# a project-local runtime helper rather than a machine-wide Hadoop install.
WINUTILS_HOME = Path(__file__).resolve().parents[1] / "venv" / "hadoop"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
IVY_CACHE_DIR = PROJECT_ROOT / ".ivy2"
if (WINUTILS_HOME / "bin" / "winutils.exe").exists():
    os.environ.setdefault("HADOOP_HOME", str(WINUTILS_HOME))
    os.environ["PATH"] = f"{WINUTILS_HOME / 'bin'}{os.pathsep}{os.environ['PATH']}"

# Keep the driver and workers on the active virtual environment. PySpark's
# Windows launcher otherwise searches for a Unix-style ``python3`` executable.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession


APP_NAME = "minio-s3a-connection-test"
HADOOP_AWS_VERSION = "3.5.0"
# Hadoop 3.5 resolves its compatible AWS SDK v2 bundle transitively. Adding
# the older v1 aws-java-sdk-bundle explicitly would create a classpath conflict.
S3A_PACKAGES = f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}"


def _cached_s3a_classpath() -> str | None:
    """Use already-resolved S3A jars directly on Windows when available.

    Adding Maven packages through ``spark.jars.packages`` makes Spark copy the
    jars via Hadoop's local filesystem, which unnecessarily requires
    ``winutils.exe``. In local mode, putting the resolved jars directly on both
    classpaths avoids that Windows-only dependency.
    """
    patterns = (
        "org.apache.hadoop_hadoop-aws-*.jar",
        "software.amazon.awssdk_bundle-*.jar",
        "software.amazon.s3.analyticsaccelerator_analyticsaccelerator-s3-*.jar",
        "org.wildfly.openssl_wildfly-openssl-*.jar",
    )
    jars = []
    for pattern in patterns:
        matches = sorted((IVY_CACHE_DIR / "jars").glob(pattern))
        if not matches:
            return None
        jars.append(matches[-1])
    return os.pathsep.join(str(jar) for jar in jars)

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"

ORDERS_PATH = "s3a://raw/orders/DataCoSupplyChainDataset.csv"
WAREHOUSES_PATH = "s3a://raw/dim_warehouse/Dim_Warehouse.csv"
CARRIERS_PATH = "s3a://raw/dim_carrier/Dim_Carrier.csv"


# Khởi tạo Spark cục bộ với cấu hình S3A để truy cập MinIO.
def create_spark_session() -> SparkSession:
    """Create a local Spark session configured for MinIO's S3-compatible API."""
    builder = (
        SparkSession.builder.appName(APP_NAME)
        .master("local[*]")
        .config("spark.jars.ivy", str(IVY_CACHE_DIR))
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )
    cached_classpath = _cached_s3a_classpath()
    if cached_classpath:
        builder = builder.config("spark.driver.extraClassPath", cached_classpath).config(
            "spark.executor.extraClassPath", cached_classpath
        )
    else:
        # Resolve hadoop-aws and the AWS SDK bundle on the first execution.
        builder = builder.config("spark.jars.packages", S3A_PACKAGES)
    return builder.getOrCreate()


# Đọc CSV có header và giữ nguyên các ký tự Latin-1 của dữ liệu nguồn.
def read_csv(spark: SparkSession, path: str):
    """Read a headered CSV while retaining the source's Latin-1 characters."""
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("encoding", "iso-8859-1")
        .csv(path)
    )


# Chạy kiểm tra kết nối và khả năng đọc ba nguồn raw thiết yếu.
def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        print(f"Spark version: {spark.version}")
        print(f"Reading orders: {ORDERS_PATH}")
        orders = read_csv(spark, ORDERS_PATH)
        orders.printSchema()
        print(f"Orders row count: {orders.count()}")

        print(f"Reading warehouses: {WAREHOUSES_PATH}")
        warehouses = read_csv(spark, WAREHOUSES_PATH)
        warehouses.printSchema()

        print(f"Reading carriers: {CARRIERS_PATH}")
        carriers = read_csv(spark, CARRIERS_PATH)
        carriers.printSchema()

        print("MinIO S3A connection test completed successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()


# Troubleshooting log (2026-08-10)
# - On Windows, Spark can fail before creating SparkContext with
#   "HADOOP_HOME and hadoop.home.dir are unset". Put winutils.exe in
#   venv/hadoop/bin (or set HADOOP_HOME to a Hadoop directory containing bin/
#   winutils.exe); the project-local setup above applies it automatically.
# - Spark 4.2.0 bundles Hadoop 3.5.0. Use hadoop-aws:3.5.0 only: its compatible
#   AWS SDK v2 bundle is resolved transitively. Adding the old v1
#   aws-java-sdk-bundle can cause classpath conflicts.
# - A successful S3A configuration still requires bucket raw and these objects:
#   orders/DataCoSupplyChainDataset.csv,
#   dim_warehouse/Dim_Warehouse.csv, and dim_carrier/Dim_Carrier.csv. Run
#   scripts/upload_to_minio.py with PYTHONUTF8=1 after restoring their local
#   source files if MinIO reports a missing bucket/object. On 2026-08-10, a
#   fresh local MinIO instance returned PATH_NOT_FOUND because all five local
#   input CSV files were absent, so the upload script correctly skipped them.
