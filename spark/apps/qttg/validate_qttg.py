"""
Validate layer - Kiểm tra tính toàn vẹn dữ liệu sau khi chạy Bronze, Silver, Gold.

Kiểm tra:
    1. Detail không mồ côi (mọi MASTER_ID đều có master tương ứng tại Silver).
    2. Mỗi người chỉ còn một master tại Silver.
    3. TU_THANG <= DEN_THANG trong detail Silver.
    4. Gold không trùng tháng.
    5. Nếu có lỗi sẽ raise exception để task Airflow chuyển sang failed.
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ERRORS = []


def check_no_orphan_detail(spark, lake_dir):
    """Detail không mồ côi: mọi MASTER_ID phải tồn tại trong master Silver."""
    master_df = spark.read.parquet(f"{lake_dir}/silver/RAW_QTTG_BHXH").select("ID", "NLD_ID")
    detail_df = spark.read.parquet(f"{lake_dir}/silver/RAW_QTTG_BHXH_DETAIL").select(
        "MASTER_ID", "NLD_ID"
    )

    orphan_df = detail_df.join(master_df, detail_df["MASTER_ID"] == master_df["ID"], "left_anti")
    orphan_count = orphan_df.count()
    if orphan_count > 0:
        ERRORS.append(f"Detail mo coi (khong co master): {orphan_count} dong")

    # Kiểm tra NLD_ID khớp giữa master và detail
    joined_df = detail_df.join(
        master_df, detail_df["MASTER_ID"] == master_df["ID"], "inner"
    ).filter(detail_df["NLD_ID"] != master_df["NLD_ID"])
    mismatch_count = joined_df.count()
    if mismatch_count > 0:
        ERRORS.append(f"Detail co NLD_ID khong khop master: {mismatch_count} dong")

    print(f"VALIDATE orphan_detail={orphan_count} nld_mismatch={mismatch_count}")


def check_unique_person(spark, lake_dir):
    """Mỗi người (SO_SO_BHXH) chỉ còn một master tại Silver."""
    master_df = spark.read.parquet(f"{lake_dir}/silver/RAW_QTTG_BHXH").select("SO_SO_BHXH")

    dup_df = (
        master_df.groupBy("SO_SO_BHXH")
        .agg(F.count("*").alias("CNT"))
        .filter(F.col("CNT") > 1)
    )
    dup_count = dup_df.count()
    if dup_count > 0:
        ERRORS.append(f"So nguoi (SO_SO_BHXH) bi trung tai Silver: {dup_count}")

    print(f"VALIDATE duplicate_person={dup_count}")


def check_month_range(spark, lake_dir):
    """TU_THANG <= DEN_THANG trong detail Silver."""
    detail_df = spark.read.parquet(f"{lake_dir}/silver/RAW_QTTG_BHXH_DETAIL").select(
        "TU_THANG", "DEN_THANG"
    )

    invalid_df = detail_df.filter(
        (F.col("TU_THANG").isNotNull())
        & (F.col("DEN_THANG").isNotNull())
        & (F.col("TU_THANG") > F.col("DEN_THANG"))
    )
    invalid_count = invalid_df.count()
    if invalid_count > 0:
        ERRORS.append(f"Detail co TU_THANG > DEN_THANG: {invalid_count} dong")

    print(f"VALIDATE invalid_month_range={invalid_count}")


def check_gold_unique_month(spark, lake_dir):
    """Gold không trùng tháng (THANG_ID unique)."""
    gold_df = spark.read.parquet(f"{lake_dir}/gold/BAO_CAO_BHXH_THANG").select("THANG_ID")

    dup_df = (
        gold_df.groupBy("THANG_ID")
        .agg(F.count("*").alias("CNT"))
        .filter(F.col("CNT") > 1)
    )
    dup_count = dup_df.count()
    if dup_count > 0:
        ERRORS.append(f"Gold bi trung thang: {dup_count} THANG_ID")

    print(f"VALIDATE duplicate_gold_month={dup_count}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate QTTG data lake")
    parser.add_argument("--lake-dir", required=True, help="dir chua bronze/silver/gold")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("QTTG_Validate")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    check_no_orphan_detail(spark, args.lake_dir)
    check_unique_person(spark, args.lake_dir)
    check_month_range(spark, args.lake_dir)
    check_gold_unique_month(spark, args.lake_dir)

    error_count = len(ERRORS)
    if error_count > 0:
        for err in ERRORS:
            print(f"VALIDATE ERROR: {err}")
        raise SystemExit(f"Validate that bai: {error_count} loi")

    print("LAYER=VALIDATE STATUS=SUCCESS ERROR_ROWS=0")

    spark.stop()


if __name__ == "__main__":
    main()