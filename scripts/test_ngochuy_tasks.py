r"""Kiểm tra toàn bộ phần Data Warehouse và dbt của Ngọc Huy.

Chạy kiểm tra DuckDB:
    .venv\Scripts\python.exe scripts\test_ngochuy_tasks.py

Chạy cả dbt build trước khi kiểm tra:
    .venv\Scripts\python.exe scripts\test_ngochuy_tasks.py --run-dbt

Kiểm tra thêm DuckDB khớp tuyệt đối với curated Parquet trên MinIO:
    .venv\Scripts\python.exe scripts\test_ngochuy_tasks.py --check-minio

Chạy đầy đủ nhất:
    .venv\Scripts\python.exe scripts\test_ngochuy_tasks.py --run-dbt --check-minio
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "logistics.duckdb"
DBT_PROJECT = ROOT / "dbt_logistics"
EXPECTED_FACT_ROWS = 180_519

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


class TestReport:
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

    def summary(self) -> None:
        total = self.passed + self.failed
        print("\n" + "=" * 68)
        print(f"KẾT QUẢ: {self.passed}/{total} TEST PASS; {self.failed} TEST FAIL")
        print("=" * 68)


def scalar(con: duckdb.DuckDBPyConnection, sql: str):
    return con.execute(sql).fetchone()[0]


def run_dbt(report: TestReport) -> None:
    print("\n--- DBT BUILD ---")
    dbt_exe = ROOT / ".venv" / "Scripts" / "dbt.exe"
    if not dbt_exe.exists():
        report.check(False, "Tìm thấy dbt.exe", str(dbt_exe))
        return

    result = subprocess.run(
        [
            str(dbt_exe),
            "build",
            "--project-dir",
            str(DBT_PROJECT),
            "--profiles-dir",
            str(DBT_PROJECT),
        ],
        cwd=ROOT,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        check=False,
    )
    print(result.stdout)
    if result.stderr:
        print(result.stderr)
    report.check(
        result.returncode == 0 and "PASS=35" in result.stdout,
        "dbt build đạt 35/35 PASS",
        f"exit_code={result.returncode}",
    )


def test_schema(con: duckdb.DuckDBPyConnection, report: TestReport) -> None:
    print("\n--- 1. STAR SCHEMA ---")
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


def test_counts(con: duckdb.DuckDBPyConnection, report: TestReport) -> None:
    print("\n--- 2. SỐ DÒNG ---")
    for table, expected in EXPECTED_COUNTS.items():
        actual = scalar(con, f'SELECT count(*) FROM "{table}"')
        report.check(
            actual == expected,
            f"{table}: {actual:,} dòng",
            f"expected={expected:,}",
        )


def test_fact_quality(con: duckdb.DuckDBPyConnection, report: TestReport) -> None:
    print("\n--- 3. CHẤT LƯỢNG FACT_SHIPMENT ---")
    total, distinct_ids = con.execute(
        "SELECT count(*), count(DISTINCT shipment_id) FROM Fact_Shipment"
    ).fetchone()
    report.check(
        total == EXPECTED_FACT_ROWS,
        f"Fact_Shipment có {total:,} dòng",
        f"expected={EXPECTED_FACT_ROWS:,}",
    )
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
        WHERE delay_hours IS DISTINCT FROM (lead_time - scheduled_time) * 24
        """,
    )
    report.check(
        invalid_delay == 0,
        "Công thức delay_hours chính xác",
        f"invalid_rows={invalid_delay}",
    )

    invalid_on_time = scalar(
        con,
        "SELECT count(*) FROM Fact_Shipment WHERE on_time IS NULL",
    )
    report.check(
        invalid_on_time == 0,
        "on_time chỉ chứa boolean, không null",
        f"invalid_rows={invalid_on_time}",
    )

    total_shipments, on_time_shipments = con.execute(
        """
        SELECT count(*), count(*) FILTER (WHERE on_time)
        FROM Fact_Shipment
        """
    ).fetchone()
    rate = on_time_shipments / total_shipments
    report.check(
        0 <= rate <= 1,
        f"Tỷ lệ đúng hạn hợp lệ: {rate:.2%}",
    )


def test_relationships(con: duckdb.DuckDBPyConnection, report: TestReport) -> None:
    print("\n--- 4. KHÓA NGOẠI ---")
    relationships = (
        ("carrier_key", "Dim_Carrier", "carrier_id"),
        ("warehouse_key", "Dim_Warehouse", "warehouse_id"),
        ("route_key", "Dim_Route", "route_id"),
        ("date_key", "Dim_Date", "date_key"),
    )
    for fact_key, dimension, dimension_key in relationships:
        orphan_count = scalar(
            con,
            f"""
            SELECT count(*)
            FROM Fact_Shipment f
            LEFT JOIN {dimension} d ON f.{fact_key}=d.{dimension_key}
            WHERE d.{dimension_key} IS NULL
            """,
        )
        report.check(
            orphan_count == 0,
            f"{fact_key} liên kết hợp lệ với {dimension}",
            f"orphans={orphan_count}",
        )


def test_marts(con: duckdb.DuckDBPyConnection, report: TestReport) -> None:
    print("\n--- 5. MARTS VÀ SLA ---")
    sla_type_rows = con.execute(
        """
        SELECT table_type FROM information_schema.tables
        WHERE table_schema='main' AND table_name='sla_monthly'
        """
    ).fetchall()
    sla_type = sla_type_rows[0][0] if sla_type_rows else None
    report.check(sla_type == "VIEW", "sla_monthly là VIEW", f"actual={sla_type}")

    duplicate_months = scalar(
        con,
        """
        SELECT count(*) FROM (
            SELECT year, month FROM sla_monthly
            GROUP BY year, month HAVING count(*) > 1
        )
        """,
    )
    report.check(
        duplicate_months == 0,
        "Mỗi tháng chỉ có một dòng SLA",
        f"duplicates={duplicate_months}",
    )

    invalid_sla = scalar(
        con,
        """
        SELECT count(*) FROM sla_monthly
        WHERE year IS NULL OR month IS NULL
           OR on_time_rate NOT BETWEEN 0 AND 1
           OR on_time_shipments > total_shipments
        """,
    )
    report.check(invalid_sla == 0, "Các chỉ số SLA hợp lệ", f"invalid={invalid_sla}")

    min_year, max_year = con.execute(
        "SELECT min(year), max(year) FROM sla_monthly"
    ).fetchone()
    report.check(
        (min_year, max_year) == (2015, 2018),
        "SLA bao phủ dữ liệu 2015–2018",
        f"actual={min_year}–{max_year}",
    )


def test_minio(con: duckdb.DuckDBPyConnection, report: TestReport) -> None:
    print("\n--- 6. ĐỐI CHIẾU MINIO PARQUET ↔ DUCKDB ---")
    try:
        from load_fact_shipment_duckdb import (
            download_curated_parquet,
            parquet_glob,
        )

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
            source_glob = parquet_glob(parquet_dir).replace("'", "''")
            con.execute(
                f"""
                CREATE OR REPLACE TEMP VIEW parquet_fact AS
                SELECT * FROM read_parquet(
                    '{source_glob}', hive_partitioning=true, union_by_name=true
                )
                """
            )
            comparisons = " OR ".join(
                f"f.{column} IS DISTINCT FROM p.{column}"
                for column in FACT_COLUMNS
                if column != "shipment_id"
            )
            mismatch_count = scalar(
                con,
                f"""
                SELECT count(*)
                FROM Fact_Shipment f FULL JOIN parquet_fact p USING (shipment_id)
                WHERE f.shipment_id IS NULL OR p.shipment_id IS NULL
                   OR {comparisons}
                """,
            )
            carrier_mismatch = scalar(
                con,
                """
                SELECT count(*)
                FROM Fact_Shipment f JOIN parquet_fact p USING (shipment_id)
                WHERE f.carrier_key IS DISTINCT FROM p.carrier_key
                """,
            )
            report.check(file_count == 210, f"MinIO có {file_count} curated Parquet files")
            report.check(
                mismatch_count == 0,
                "Toàn bộ 12 cột DuckDB khớp curated Parquet",
                f"mismatches={mismatch_count}",
            )
            report.check(
                carrier_mismatch == 0,
                "carrier_key khớp tuyệt đối với PySpark",
                f"mismatches={carrier_mismatch}",
            )
    except Exception as error:
        report.check(False, "Kết nối/đối chiếu MinIO", str(error))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dbt", action="store_true", help="chạy dbt build trước")
    parser.add_argument(
        "--check-minio",
        action="store_true",
        help="tải curated Parquet và đối chiếu toàn bộ Fact",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = TestReport()

    print("=" * 68)
    print("KIỂM TRA TOÀN BỘ NHIỆM VỤ DATA WAREHOUSE & DBT — NGỌC HUY")
    print("=" * 68)

    if args.run_dbt:
        run_dbt(report)

    if not DB_PATH.exists():
        report.check(False, "Tồn tại logistics.duckdb", str(DB_PATH))
        report.summary()
        return 1

    try:
        con = duckdb.connect(str(DB_PATH), read_only=True)
        try:
            test_schema(con, report)
            test_counts(con, report)
            test_fact_quality(con, report)
            test_relationships(con, report)
            test_marts(con, report)
            if args.check_minio:
                test_minio(con, report)
        finally:
            con.close()
    except Exception as error:
        report.check(False, "Mở và kiểm tra DuckDB", repr(error))

    report.summary()
    return 1 if report.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
