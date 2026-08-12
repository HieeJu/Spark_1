import argparse

from pyspark.sql import SparkSession # type: ignore
from pyspark.sql import functions as F # type: ignore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Gold layer QTTG")
    parser.add_argument("--input-dir", required=True, help="dir silver")
    parser.add_argument("--output-dir", required=True, help="dir ghi gold")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("QTTG_Gold")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    master_df = spark.read.parquet(f"{args.input_dir}/RAW_QTTG_BHXH")
    detail_df = spark.read.parquet(f"{args.input_dir}/RAW_QTTG_BHXH_DETAIL")

    # Tạo DIM_THANG từ khoảng TU_THANG - DEN_THANG của detail
    # TU_THANG/DEN_THANG có dạng YYYYMM (ví dụ: 200804)
    month_range_df = (
        detail_df.select(
            F.col("MASTER_ID"),
            F.col("MA_DON_VI"),
            F.col("MUC_LUONG"),
            F.col("TU_THANG").cast("int").alias("TU_THANG_INT"),
            F.col("DEN_THANG").cast("int").alias("DEN_THANG_INT"),
        )
    )

    # Join với master để lấy SO_SO_BHXH
    month_range_df = month_range_df.join(
        master_df.select("ID", "SO_SO_BHXH"),
        month_range_df["MASTER_ID"] == master_df["ID"],
        "inner",
    ).select(
        F.col("SO_SO_BHXH"),
        F.col("MA_DON_VI"),
        F.col("MUC_LUONG"),
        F.col("TU_THANG_INT"),
        F.col("DEN_THANG_INT"),
    )

    # Bung khoảng tháng thành từng tháng báo cáo
    # Tạo DIM_THANG chứa tất cả các tháng YYYYMM hợp lệ từ min đến max
    min_month = month_range_df.agg(F.min("TU_THANG_INT")).collect()[0][0]
    max_month = month_range_df.agg(F.max("DEN_THANG_INT")).collect()[0][0]

    min_year = min_month // 100
    max_year = max_month // 100

    # Tạo tất cả các tháng YYYYMM từ min_year*100+01 đến max_year*100+12
    dim_thang = (
        spark.range(min_year * 100 + 1, max_year * 100 + 13)
        .toDF("THANG_SEQ")
        .withColumn("YEAR", F.floor(F.col("THANG_SEQ") / 100))
        .withColumn("MONTH", F.col("THANG_SEQ") % 100)
        .filter((F.col("MONTH") >= 1) & (F.col("MONTH") <= 12))
        .withColumn("THANG_ID", F.col("YEAR") * 100 + F.col("MONTH"))
        .select("THANG_ID")
        .distinct()
    )

    # Join: tháng nằm trong khoảng TU_THANG..DEN_THANG
    exploded_df = (
        month_range_df.join(
            dim_thang,
            (dim_thang["THANG_ID"] >= month_range_df["TU_THANG_INT"])
            & (dim_thang["THANG_ID"] <= month_range_df["DEN_THANG_INT"]),
            "inner",
        )
        .select(
            F.col("THANG_ID"),
            F.col("SO_SO_BHXH"),
            F.col("MA_DON_VI"),
            F.col("MUC_LUONG"),
        )
    )

    # Báo cáo tổng hợp theo tháng
    report_df = (
        exploded_df.groupBy("THANG_ID")
        .agg(
            F.countDistinct("SO_SO_BHXH").alias("SO_NGUOI_THAM_GIA"),
            F.countDistinct("MA_DON_VI").alias("SO_DON_VI"),
            F.sum("MUC_LUONG").alias("TONG_QUY_LUONG"),
            F.avg(F.when(F.col("MUC_LUONG") > 0, F.col("MUC_LUONG"))).alias("LUONG_BINH_QUAN"),
            F.countDistinct(F.when(F.col("MUC_LUONG") == 0, F.col("SO_SO_BHXH"))).alias(
                "SO_NGUOI_LUONG_0"
            ),
        )
        .orderBy("THANG_ID")
    )

    report_count = report_df.count()

    report_df.write.mode("overwrite").parquet(f"{args.output_dir}/BAO_CAO_BHXH_THANG")

    print(f"LAYER=GOLD STATUS=SUCCESS REPORT_ROWS={report_count}")

    # In mẫu dữ liệu
    report_df.orderBy("THANG_ID").show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()