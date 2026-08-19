r"""Chạy toàn bộ pipeline Data Warehouse và dbt của Ngọc Huy.

Luồng thực hiện:
    Raw CSV -> MinIO raw -> PySpark -> curated Parquet
    -> DuckDB dimensions/fact -> dbt staging/marts/tests -> validation

Chạy đầy đủ từ thư mục gốc repository:
    .venv\Scripts\python.exe scripts\run_ngochuy_pipeline.py

Bỏ qua PySpark khi curated Parquet đã có sẵn:
    .venv\Scripts\python.exe scripts\run_ngochuy_pipeline.py --skip-spark

Lưu ý: setup_warehouse_duckdb.py sẽ xóa logistics.duckdb cũ và dựng lại.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

import duckdb

from load_fact_shipment_duckdb import (
    FACT_COLUMNS,
    download_curated_parquet,
    parquet_glob,
)

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
DBT_PROJECT = ROOT / "dbt_logistics"
MINIO_HEALTH_URL = "http://localhost:9000/minio/health/live"

REQUIRED_FILES = (
    ROOT / "data" / "raw" / "DataCoSupplyChainDataset.csv",
    ROOT / "data" / "simulated" / "Dim_Carrier.csv",
    ROOT / "data" / "simulated" / "Dim_Warehouse.csv",
    ROOT / "data" / "simulated" / "Dim_Route.csv",
    ROOT / "data" / "simulated" / "Dim_Date.csv",
)

EXPECTED_COUNTS = {
    "Dim_Carrier": 6,
    "Dim_Warehouse": 23,
    "Dim_Route": 23,
    "Dim_Date": 1_192,
    "Fact_Shipment": 180_519,
    "stg_carrier": 6,
    "stg_warehouse": 23,
    "stg_route": 23,
    "stg_date": 1_192,
    "stg_shipment": 180_519,
    "carrier_performance": 6,
    "route_performance": 23,
    "sla_monthly": 37,
}


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


def heading(number: int, title: str) -> None:
    print("\n" + "=" * 76)
    print(f"BƯỚC {number}: {title}")
    print("=" * 76)


def run(command: list[str], label: str, env: dict[str, str] | None = None) -> None:
    """Chạy lệnh, stream output và dừng pipeline ngay khi có lỗi."""
    print(f"\n> {subprocess.list2cmdline(command)}")
    result = subprocess.run(
        command,
        cwd=ROOT,
        env=env,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"{label} thất bại với exit code {result.returncode}")
    print(f"[OK] {label}")


def minio_is_healthy() -> bool:
    try:
        with urllib.request.urlopen(MINIO_HEALTH_URL, timeout=5) as response:
            return response.status == 200
    except (urllib.error.URLError, TimeoutError):
        return False


def ensure_minio(start_docker: bool) -> None:
    """Kiểm tra MinIO; có thể gọi Docker Compose để khởi động service."""
    if minio_is_healthy():
        print("[PASS] MinIO health check: HTTP 200")
        return

    if not start_docker:
        raise RuntimeError(
            "MinIO chưa chạy tại localhost:9000. "
            "Hãy chạy `docker compose up -d minio`."
        )

    run(["docker", "compose", "up", "-d", "minio"], "Khởi động MinIO")
    for _ in range(30):
        if minio_is_healthy():
            print("[PASS] MinIO health check: HTTP 200")
            return
        time.sleep(1)
    raise RuntimeError("MinIO không healthy sau 30 giây")


def check_inputs() -> None:
    missing = [str(path.relative_to(ROOT)) for path in REQUIRED_FILES if not path.exists()]
    if missing:
        raise FileNotFoundError("Thiếu file đầu vào:\n  - " + "\n  - ".join(missing))

    for path in REQUIRED_FILES:
        print(f"[PASS] {path.relative_to(ROOT)} ({path.stat().st_size:,} bytes)")


class ValidationReport:
    def __init__(self) -> None:
        self.passed = 0
        self.failed = 0

    def check(self, condition: bool, message: str, detail: str = "") -> None:
        if condition:
            self.passed += 1
            print(f"[PASS] {message}")
        else:
            self.failed += 1
            suffix = f" — {detail}" if detail else ""
            print(f"[FAIL] {message}{suffix}")


def scalar(con: duckdb.DuckDBPyConnection, sql: str):
    return con.execute(sql).fetchone()[0]


def validate_warehouse() -> tuple[int, int]:
    """Kiểm tra schema, dữ liệu, relationships, marts và MinIO consistency."""
    report = ValidationReport()
    con = duckdb.connect(str(ROOT / "logistics.duckdb"), read_only=True)
    try:
        existing = {
            row[0]
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
        }
        for table in EXPECTED_COUNTS:
            report.check(table in existing, f"Tồn tại bảng/view {table}")

        actual_columns = tuple(
            row[0] for row in con.execute("DESCRIBE Fact_Shipment").fetchall()
        )
        report.check(
            actual_columns == FACT_COLUMNS,
            "Fact_Shipment có đúng 12 cột và đúng thứ tự",
            f"actual={actual_columns}",
        )

        for table, expected in EXPECTED_COUNTS.items():
            actual = scalar(con, f'SELECT count(*) FROM "{table}"')
            report.check(
                actual == expected,
                f"{table}: {actual:,} dòng",
                f"expected={expected:,}",
            )

        total, distinct_ids = con.execute(
            "SELECT count(*), count(DISTINCT shipment_id) FROM Fact_Shipment"
        ).fetchone()
        report.check(total == distinct_ids, "shipment_id không trùng")

        null_keys = scalar(
            con,
            """
            SELECT count(*) FROM Fact_Shipment
            WHERE shipment_id IS NULL OR carrier_key IS NULL
               OR warehouse_key IS NULL OR route_key IS NULL OR date_key IS NULL
            """,
        )
        report.check(null_keys == 0, "Không có khóa null", f"rows={null_keys}")

        invalid_delay = scalar(
            con,
            """
            SELECT count(*) FROM Fact_Shipment
            WHERE delay_hours IS DISTINCT FROM (lead_time-scheduled_time)*24
            """,
        )
        report.check(
            invalid_delay == 0,
            "Công thức delay_hours chính xác",
            f"invalid={invalid_delay}",
        )
        report.check(
            scalar(con, "SELECT count(*) FROM Fact_Shipment WHERE on_time IS NULL") == 0,
            "on_time là boolean và không null",
        )

        relationships = (
            ("carrier_key", "Dim_Carrier", "carrier_id"),
            ("warehouse_key", "Dim_Warehouse", "warehouse_id"),
            ("route_key", "Dim_Route", "route_id"),
            ("date_key", "Dim_Date", "date_key"),
        )
        for fact_key, dimension, dimension_key in relationships:
            orphans = scalar(
                con,
                f"""
                SELECT count(*) FROM Fact_Shipment f
                LEFT JOIN {dimension} d ON f.{fact_key}=d.{dimension_key}
                WHERE d.{dimension_key} IS NULL
                """,
            )
            report.check(
                orphans == 0,
                f"{fact_key} liên kết hợp lệ với {dimension}",
                f"orphans={orphans}",
            )

        sla_type = con.execute(
            """
            SELECT table_type FROM information_schema.tables
            WHERE table_schema='main' AND table_name='sla_monthly'
            """
        ).fetchone()[0]
        report.check(sla_type == "VIEW", "sla_monthly là VIEW", f"actual={sla_type}")
        duplicate_months = scalar(
            con,
            """
            SELECT count(*) FROM (
                SELECT year, month FROM sla_monthly
                GROUP BY year, month HAVING count(*)>1
            )
            """,
        )
        report.check(duplicate_months == 0, "Mỗi tháng chỉ có một dòng SLA")
        invalid_sla = scalar(
            con,
            """
            SELECT count(*) FROM sla_monthly
            WHERE year IS NULL OR month IS NULL OR on_time_rate NOT BETWEEN 0 AND 1
               OR on_time_shipments > total_shipments
            """,
        )
        report.check(invalid_sla == 0, "Các chỉ số SLA hợp lệ")

        with tempfile.TemporaryDirectory(prefix="verify_fact_") as temp_dir:
            parquet_dir = Path(temp_dir)
            file_count = download_curated_parquet(
                parquet_dir,
                "http://localhost:9000",
                "minioadmin",
                "minioadmin123",
                "curated",
                "fact_shipment",
            )
            source = parquet_glob(parquet_dir).replace("'", "''")
            con.execute(
                f"""
                CREATE OR REPLACE TEMP VIEW parquet_fact AS
                SELECT * FROM read_parquet(
                    '{source}', hive_partitioning=true, union_by_name=true
                )
                """
            )
            comparisons = " OR ".join(
                f"f.{column} IS DISTINCT FROM p.{column}"
                for column in FACT_COLUMNS
                if column != "shipment_id"
            )
            mismatches = scalar(
                con,
                f"""
                SELECT count(*) FROM Fact_Shipment f
                FULL JOIN parquet_fact p USING (shipment_id)
                WHERE f.shipment_id IS NULL OR p.shipment_id IS NULL
                   OR {comparisons}
                """,
            )
            carrier_mismatches = scalar(
                con,
                """
                SELECT count(*) FROM Fact_Shipment f
                JOIN parquet_fact p USING (shipment_id)
                WHERE f.carrier_key IS DISTINCT FROM p.carrier_key
                """,
            )
            report.check(file_count == 210, f"MinIO có {file_count} curated files")
            report.check(mismatches == 0, "12 cột DuckDB khớp curated Parquet")
            report.check(carrier_mismatches == 0, "carrier_key khớp PySpark")
    finally:
        con.close()

    print("\n" + "=" * 76)
    print(
        f"VALIDATION: {report.passed}/{report.passed + report.failed} PASS; "
        f"{report.failed} FAIL"
    )
    print("=" * 76)
    return report.passed, report.failed


def show_sla_monthly() -> None:
    """In 37 tháng SLA và các chỉ số tổng hợp phục vụ báo cáo."""
    con = duckdb.connect(str(ROOT / "logistics.duckdb"), read_only=True)
    try:
        rows = con.execute(
            """
            SELECT year, month, total_shipments, on_time_shipments,
                   round(on_time_rate*100, 2), round(avg_delay_hours, 2)
            FROM sla_monthly ORDER BY year, month
            """
        ).fetchall()
        print(
            f"{'Năm':<7}{'Tháng':<8}{'Tổng đơn':>14}{'Đúng hạn':>14}"
            f"{'SLA (%)':>12}{'Trễ TB (giờ)':>16}"
        )
        print("-" * 71)
        for year, month, total, on_time, sla, delay in rows:
            print(
                f"{year:<7}{month:<8}{total:>14,}{on_time:>14,}"
                f"{float(sla):>12.2f}{float(delay):>16.2f}"
            )
        summary = con.execute(
            """
            SELECT count(*), sum(total_shipments), sum(on_time_shipments),
                   round(sum(on_time_shipments)*100.0/sum(total_shipments),2),
                   round(sum(avg_delay_hours*total_shipments)/sum(total_shipments),2),
                   min(year), max(year)
            FROM sla_monthly
            """
        ).fetchone()
        months, total, on_time, sla, delay, min_year, max_year = summary
        print("-" * 71)
        print(f"Giai đoạn: {min_year}–{max_year}; Số tháng: {months}")
        print(f"Tổng shipment: {total:,}; Đúng hạn: {on_time:,}")
        print(f"SLA toàn kỳ: {float(sla):.2f}%; Trễ TB: {float(delay):.2f} giờ")
    finally:
        con.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="không upload lại raw/dimensions lên MinIO",
    )
    parser.add_argument(
        "--skip-spark",
        action="store_true",
        help="không chạy lại PySpark; dùng curated Parquet đang có trên MinIO",
    )
    parser.add_argument(
        "--no-start-docker",
        action="store_true",
        help="chỉ kiểm tra MinIO, không tự gọi docker compose up",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    python = sys.executable
    dbt = ROOT / ".venv" / "Scripts" / "dbt.exe"

    print("=" * 76)
    print("CHẠY TOÀN BỘ PIPELINE DATA WAREHOUSE & DBT — NGỌC HUY")
    print("Raw -> PySpark -> MinIO Parquet -> DuckDB -> dbt -> Validation")
    print("=" * 76)

    try:
        heading(1, "KIỂM TRA DỮ LIỆU ĐẦU VÀO")
        check_inputs()

        heading(2, "KHỞI ĐỘNG VÀ KIỂM TRA MINIO")
        ensure_minio(start_docker=not args.no_start_docker)

        if not args.skip_upload:
            heading(3, "UPLOAD RAW VÀ DIMENSIONS LÊN MINIO")
            upload_env = os.environ.copy()
            upload_env["PYTHONUTF8"] = "1"
            run(
                [python, str(SCRIPTS / "upload_to_minio.py")],
                "Upload dữ liệu lên MinIO",
                env=upload_env,
            )
        else:
            heading(3, "BỎ QUA UPLOAD THEO THAM SỐ --skip-upload")

        if not args.skip_spark:
            heading(4, "PYSPARK XỬ LÝ VÀ GHI CURATED PARQUET")
            spark_env = os.environ.copy()
            spark_env["PYTHONUTF8"] = "1"
            # Tránh một HADOOP_HOME hệ thống sai ghi đè cấu hình project.
            spark_env.pop("HADOOP_HOME", None)
            run(
                [
                    python,
                    str(SCRIPTS / "spark_write_shipment_parquet.py"),
                    "--verify",
                ],
                "PySpark curated Parquet và round-trip verification",
                env=spark_env,
            )
        else:
            heading(4, "BỎ QUA PYSPARK THEO THAM SỐ --skip-spark")

        heading(5, "DỰNG LẠI DUCKDB VÀ NẠP 4 DIMENSIONS")
        setup_env = os.environ.copy()
        setup_env["PYTHONUTF8"] = "1"
        run(
            [python, str(SCRIPTS / "setup_warehouse_duckdb.py")],
            "Dựng warehouse DuckDB",
            env=setup_env,
        )

        heading(6, "NẠP FACT_SHIPMENT TỪ CURATED PARQUET")
        run(
            [python, str(SCRIPTS / "load_fact_shipment_duckdb.py")],
            "Nạp Fact_Shipment từ MinIO",
        )

        heading(7, "CHẠY TOÀN BỘ DBT MODELS VÀ TESTS")
        if not dbt.exists():
            raise FileNotFoundError(f"Không tìm thấy dbt.exe: {dbt}")
        run(
            [
                str(dbt),
                "build",
                "--project-dir",
                str(DBT_PROJECT),
                "--profiles-dir",
                str(DBT_PROJECT),
            ],
            "dbt build",
        )

        heading(8, "KIỂM TRA TOÀN BỘ KẾT QUẢ VÀ ĐỐI CHIẾU MINIO")
        passed, failed = validate_warehouse()
        if failed:
            raise RuntimeError(f"Validation có {failed} test FAIL")

        heading(9, "KẾT QUẢ PHÂN TÍCH SLA GIAO HÀNG THEO THÁNG")
        show_sla_monthly()

    except (FileNotFoundError, RuntimeError) as error:
        print("\n" + "!" * 76)
        print(f"PIPELINE THẤT BẠI: {error}")
        print("!" * 76)
        return 1

    print("\n" + "=" * 76)
    print("PIPELINE HOÀN THÀNH THÀNH CÔNG")
    print(f"Kết quả: PySpark 180,519 dòng; dbt 35/35; validation {passed}/{passed} PASS")
    print("=" * 76)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
