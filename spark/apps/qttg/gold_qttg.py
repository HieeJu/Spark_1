import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, expr, sequence, to_date, date_format, explode,
    countDistinct, sum as _sum, avg, count, when
)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("Gold_QTTG_Monthly_Report") \
        .getOrCreate()

    master_df = spark.read.parquet(f"{args.input_dir}/master")
    detail_df = spark.read.parquet(f"{args.input_dir}/detail")

    # Bung khoảng TU_THANG - DEN_THANG thành các tháng báo cáo YYYYMM
    detail_expanded = detail_df \
        .filter(col("TU_THANG").isNotNull() & col("DEN_THANG").isNotNull()) \
        .withColumn("start_date", to_date(col("TU_THANG"), "yyyyMM")) \
        .withColumn("end_date", to_date(col("DEN_THANG"), "yyyyMM")) \
        .withColumn("date_array", expr("sequence(start_date, end_date, interval 1 month)")) \
        .withColumn("month_date", explode(col("date_array"))) \
        .withColumn("THANG_ID", date_format(col("month_date"), "yyyyMM"))

    # Join với Master để lấy thông tin SO_SO_BHXH
    joined_df = detail_expanded.join(
        master_df.select("ID", "SO_SO_BHXH"),
        detail_expanded["MASTER_ID"] == master_df["ID"],
        "inner"
    )

    # Tổng hợp chỉ số Gold theo THANG_ID
    gold_df = joined_df.groupBy("THANG_ID").agg(
        countDistinct("SO_SO_BHXH").alias("SO_NGUOI_THAM_GIA"),
        countDistinct("MA_DON_VI").alias("SO_DON_VI"),
        _sum("MUC_LUONG").alias("TONG_QUY_LUONG"),
        avg(when(col("MUC_LUONG") > 0, col("MUC_LUONG"))).alias("LUONG_BINH_QUAN"),
        countDistinct(when(col("MUC_LUONG") == 0, col("SO_SO_BHXH"))).alias("SO_NGUOI_LUONG_0")
    )

    report_count = gold_df.count()
    print(f"LAYER=GOLD STATUS=SUCCESS REPORT_ROWS={report_count}")
    gold_df.orderBy("THANG_ID").show(20, truncate=False)

    # Ghi đè Gold
    gold_df.write.mode("overwrite").parquet(f"{args.output_dir}/report_monthly")

    spark.stop()

if __name__ == "__main__":
    main()