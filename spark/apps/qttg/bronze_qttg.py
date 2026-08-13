import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType
)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("Bronze_QTTG_Ingest") \
        .getOrCreate()

    # Schema Master
    master_schema = StructType([
        StructField("ID", LongType(), True),
        StructField("NLD_ID", LongType(), True),
        StructField("SO_SO_BHXH", StringType(), True),
        StructField("THANG_BD", StringType(), True),
        StructField("THANG_KT", StringType(), True),
        StructField("TT_TG_BHXH", DoubleType(), True),
        StructField("DT_TG_BHXH", DoubleType(), True),
        StructField("NAM_TG_BHXH", DoubleType(), True),
        StructField("THANG_TG_BHXH", DoubleType(), True),
        StructField("NAM_TG_BHXH_BB", DoubleType(), True),
        StructField("THANG_TG_BHXH_BB", DoubleType(), True),
        StructField("RAW_RESPONSE", StringType(), True),
        StructField("CREATED_AT", StringType(), True)
    ])

    # Schema Detail
    detail_schema = StructType([
        StructField("ID", LongType(), True),
        StructField("MASTER_ID", LongType(), True),
        StructField("NLD_ID", LongType(), True),
        StructField("TU_THANG", StringType(), True),
        StructField("DEN_THANG", StringType(), True),
        StructField("DOT_PHAT_SINH", StringType(), True),
        StructField("MA_DON_VI", StringType(), True),
        StructField("TEN_DON_VI", StringType(), True),
        StructField("CHUC_DANH_CV", StringType(), True),
        StructField("NOI_LAM_VIEC", StringType(), True),
        StructField("MUC_LUONG", DoubleType(), True),
        StructField("HS_LUONG", DoubleType(), True),
        StructField("TY_LE_DONG", DoubleType(), True),
        StructField("CREATED_AT", StringType(), True)
    ])

    # Đọc CSV
    master_df = spark.read.csv(f"{args.input_dir}/RAW_QTTG_BHXH.csv", header=True, schema=master_schema, mode="PERMISSIVE")
    detail_df = spark.read.csv(f"{args.input_dir}/RAW_QTTG_BHXH_DETAIL.csv", header=True, schema=detail_schema, mode="PERMISSIVE")

    master_count = master_df.count()
    detail_count = detail_df.count()

    print(f"LAYER=BRONZE STATUS=SUCCESS MASTER_ROWS={master_count} DETAIL_ROWS={detail_count}")

    # Ghi ra Bronze Lake
    master_df.write.mode("overwrite").parquet(f"{args.output_dir}/master")
    detail_df.write.mode("overwrite").parquet(f"{args.output_dir}/detail")

    spark.stop()

if __name__ == "__main__":
    main()