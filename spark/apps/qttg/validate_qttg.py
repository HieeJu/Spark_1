import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--lake-dir", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("Validate_QTTG_Data_Lake") \
        .getOrCreate()

    # Read Silver Data
    silver_master = spark.read.parquet(f"{args.lake_dir}/silver/master")
    silver_detail = spark.read.parquet(f"{args.lake_dir}/silver/detail")
    gold_report = spark.read.parquet(f"{args.lake_dir}/gold/report_monthly")

    # 1. Check duplicate person in Silver Master
    dup_person = silver_master.groupBy("SO_SO_BHXH").count().filter(col("count") > 1).count()

    # 2. Check orphan details in Silver
    orphan_detail = silver_detail.join(
        silver_master,
        silver_detail["MASTER_ID"] == silver_master["ID"],
        "left_anti"
    ).count()

    # 3. Check duplicate THANG_ID in Gold
    dup_gold_month = gold_report.groupBy("THANG_ID").count().filter(col("count") > 1).count()

    total_errors = dup_person + orphan_detail + dup_gold_month

    print(f"LAYER=VALIDATE STATUS=SUCCESS ERROR_ROWS={total_errors}")

    if total_errors > 0:
        raise ValueError(f"Validation failed with {total_errors} errors! Check duplicates or orphan records.")

    spark.stop()

if __name__ == "__main__":
    main()