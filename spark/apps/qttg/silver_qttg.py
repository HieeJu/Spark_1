import argparse
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("Silver_QTTG_Deduplicate") \
        .getOrCreate()

    master_df = spark.read.parquet(f"{args.input_dir}/master")
    detail_df = spark.read.parquet(f"{args.input_dir}/detail")

    # Lấy bản ghi cha mới nhất cho mỗi người theo CREATED_AT DESC, ID DESC
    window_spec = Window.partitionBy("SO_SO_BHXH").orderBy(col("CREATED_AT").desc(), col("ID").desc())
    latest_master_df = master_df.withColumn("rn", row_number().over(window_spec)) \
                                .filter(col("rn") == 1) \
                                .drop("rn")

    # Lọc Detail thuộc các MASTER_ID vừa chọn
    latest_detail_df = detail_df.join(
        latest_master_df.select("ID").withColumnRenamed("ID", "MATCH_MASTER_ID"),
        detail_df["MASTER_ID"] == col("MATCH_MASTER_ID"),
        "inner"
    ).drop("MATCH_MASTER_ID")

    person_count = latest_master_df.count()
    detail_count = latest_detail_df.count()

    print(f"LAYER=SILVER STATUS=SUCCESS PERSON_ROWS={person_count} DETAIL_ROWS={detail_count}")

    # Ghi đè Silver
    latest_master_df.write.mode("overwrite").parquet(f"{args.output_dir}/master")
    latest_detail_df.write.mode("overwrite").parquet(f"{args.output_dir}/detail")

    spark.stop()

if __name__ == "__main__":
    main()