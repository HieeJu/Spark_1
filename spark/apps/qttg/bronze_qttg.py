"""
Bronze layer - Ingest CSV files into the data lake.

Reads:
    file:///opt/spark/data/raw_qttg_1m/RAW_QTTG_BHXH.csv
    file:///opt/spark/data/raw_qttg_1m/RAW_QTTG_BHXH_DETAIL.csv

Writes (overwrite):
    file:///opt/spark/data/lake/bronze/RAW_QTTG_BHXH
    file:///opt/spark/data/lake/bronze/RAW_QTTG_BHXH_DETAIL
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

MASTER_SCHEMA = StructType(
    [
        StructField("ID", LongType(), True),
        StructField("NLD_ID", LongType(), True),
        StructField("SO_SO_BHXH", StringType(), True),
        StructField("THANG_BD", StringType(), True),
        StructField("THANG_KT", StringType(), True),
        StructField("TT_TG_BHXH", StringType(), True),
        StructField("DT_TG_BHXH", StringType(), True),
        StructField("NAM_TG_BHXH", IntegerType(), True),
        StructField("THANG_TG_BHXH", IntegerType(), True),
        StructField("NAM_TG_BHXH_BB", IntegerType(), True),
        StructField("THANG_TG_BHXH_BB", IntegerType(), True),
        StructField("TT_TG_BHTN", StringType(), True),
        StructField("DT_TG_BHTN", StringType(), True),
        StructField("NAM_TG_BHTN", IntegerType(), True),
        StructField("THANG_TG_BHTN", IntegerType(), True),
        StructField("TT_TG_BHYT", StringType(), True),
        StructField("DT_TG_BHYT", StringType(), True),
        StructField("NAM_TG_BHYT", IntegerType(), True),
        StructField("THANG_TG_BHYT", IntegerType(), True),
        StructField("NAM_NO_BHXH", IntegerType(), True),
        StructField("THANG_NO_BHXH", IntegerType(), True),
        StructField("NAM_NO_BHTN", IntegerType(), True),
        StructField("THANG_NO_BHTN", IntegerType(), True),
        StructField("TT_TG_BH", StringType(), True),
        StructField("DT_TG_BH", StringType(), True),
        StructField("TU_THANG_DVI", StringType(), True),
        StructField("DEN_THANG_DVI", StringType(), True),
        StructField("DEN_THANG_HTTT", StringType(), True),
        StructField("DEN_THANG_BHTN", StringType(), True),
        StructField("THANG_BD_LT", StringType(), True),
        StructField("THANG_KT_LT", StringType(), True),
        StructField("SO_THANG_LT", IntegerType(), True),
        StructField("IS_ERRORS", IntegerType(), True),
        StructField("NGHI_VIEC", IntegerType(), True),
        StructField("IS_CONTINUE", IntegerType(), True),
        StructField("TRUY_DONG", IntegerType(), True),
        StructField("DEN_NGAY", StringType(), True),
        StructField("MA_CD", StringType(), True),
        StructField("MA_NHH", StringType(), True),
        StructField("DD_MA_DON_VI", StringType(), True),
        StructField("DD_THANG_DONG_DEN_XH", StringType(), True),
        StructField("DD_TY_LE_NO_BHXH", DoubleType(), True),
        StructField("DD_THANG_DONG_DEN_YT", StringType(), True),
        StructField("DD_TY_LE_NO_BHYT", DoubleType(), True),
        StructField("DD_THANG_DONG_DEN_TN", StringType(), True),
        StructField("DD_TY_LE_NO_BHTN", DoubleType(), True),
        StructField("DD_THANG_DONG_DEN_TNLD", StringType(), True),
        StructField("DD_TY_LE_NO_TNLD", DoubleType(), True),
        StructField("RAW_RESPONSE", StringType(), True),
        StructField("CREATED_AT", TimestampType(), True),
    ]
)

DETAIL_SCHEMA = StructType(
    [
        StructField("ID", LongType(), True),
        StructField("MASTER_ID", LongType(), True),
        StructField("NLD_ID", LongType(), True),
        StructField("DOT_PHAT_SINH", StringType(), True),
        StructField("TU_THANG", StringType(), True),
        StructField("DEN_THANG", StringType(), True),
        StructField("MA_DON_VI", StringType(), True),
        StructField("TEN_DON_VI", StringType(), True),
        StructField("LOAI_DT", StringType(), True),
        StructField("LOAI", IntegerType(), True),
        StructField("PA", StringType(), True),
        StructField("DON_VI_TINH", StringType(), True),
        StructField("MA_NT", StringType(), True),
        StructField("CHUC_DANH_CV", StringType(), True),
        StructField("CHUC_DANH_CV_PRE", StringType(), True),
        StructField("NOI_LAM_VIEC", StringType(), True),
        StructField("NOI_DUNG", StringType(), True),
        StructField("MUC_LUONG", DoubleType(), True),
        StructField("MUC_LUONG_TN", DoubleType(), True),
        StructField("MUC_LUONG_BHYT", DoubleType(), True),
        StructField("MUC_LUONG_PC", DoubleType(), True),
        StructField("MUC_LUONG_BS", DoubleType(), True),
        StructField("MUC_LUONG_NLD", DoubleType(), True),
        StructField("MUC_LUONG_NSNN", DoubleType(), True),
        StructField("MUC_LUONG_HS", DoubleType(), True),
        StructField("MUC_LUONG_TT", DoubleType(), True),
        StructField("HS_LUONG", DoubleType(), True),
        StructField("PC_CHUC_VU", DoubleType(), True),
        StructField("PC_THAM_NIEN", DoubleType(), True),
        StructField("PC_NGHE", DoubleType(), True),
        StructField("PC_KHU_VUC", DoubleType(), True),
        StructField("PC_KHAC", DoubleType(), True),
        StructField("PC_TAI_CU", DoubleType(), True),
        StructField("HS_TN", DoubleType(), True),
        StructField("HS_NG", DoubleType(), True),
        StructField("HS_TC", DoubleType(), True),
        StructField("TYLE_BHXH", DoubleType(), True),
        StructField("TYLE_BHYT", DoubleType(), True),
        StructField("TYLE_BHTN", DoubleType(), True),
        StructField("TYLE_TUDV", DoubleType(), True),
        StructField("TYLE_HTTT", DoubleType(), True),
        StructField("TYLE_ODTS", DoubleType(), True),
        StructField("TYLE_TNLD", DoubleType(), True),
        StructField("TYLE_NSNN", DoubleType(), True),
        StructField("DK1", IntegerType(), True),
        StructField("DK2", IntegerType(), True),
        StructField("DK3", IntegerType(), True),
        StructField("DK4", IntegerType(), True),
        StructField("DK5", IntegerType(), True),
        StructField("DK6", IntegerType(), True),
        StructField("IS_BHXH", IntegerType(), True),
        StructField("IS_BHXH_BB", IntegerType(), True),
        StructField("IS_BHTN", IntegerType(), True),
        StructField("IS_BHYT", IntegerType(), True),
        StructField("IS_BHXH2", IntegerType(), True),
        StructField("IS_BHTN2", IntegerType(), True),
        StructField("IS_ERROR", IntegerType(), True),
        StructField("IS_TR", IntegerType(), True),
        StructField("IS_BONUS", IntegerType(), True),
        StructField("ML_TC", IntegerType(), True),
        StructField("GHI_CHU", StringType(), True),
        StructField("KIEM_TRA", IntegerType(), True),
        StructField("SO_THANG", IntegerType(), True),
        StructField("MA_KHOI_TK", StringType(), True),
        StructField("TY_LE_DONG", DoubleType(), True),
        StructField("MUC_DONG", DoubleType(), True),
        StructField("LUONG_CHINH", DoubleType(), True),
        StructField("CHUC_DANH_NLV", StringType(), True),
        StructField("PHUONG_THUC", StringType(), True),
        StructField("PHUONG_THUC_DONG", StringType(), True),
        StructField("MUC_LUONG_PRE", DoubleType(), True),
        StructField("MUC_LUONG_PC_PRE", DoubleType(), True),
        StructField("MUC_LUONG_BS_PRE", DoubleType(), True),
        StructField("HS_LUONG_PRE", DoubleType(), True),
        StructField("PC_CHUC_VU_PRE", DoubleType(), True),
        StructField("PC_THAM_NIEN_PRE", DoubleType(), True),
        StructField("PC_NGHE_PRE", DoubleType(), True),
        StructField("PC_KHU_VUC_PRE", DoubleType(), True),
        StructField("PC_KHAC_PRE", DoubleType(), True),
        StructField("PC_TAI_CU_PRE", DoubleType(), True),
        StructField("LUONG_CHINH_PRE", DoubleType(), True),
        StructField("CREATED_AT", TimestampType(), True),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bronze ingest QTTG CSV")
    parser.add_argument("--input-dir", required=True, help="dir chua 2 CSV")
    parser.add_argument("--output-dir", required=True, help="dir ghi bronze")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("QTTG_Bronze")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    master_path = f"{args.input_dir}/RAW_QTTG_BHXH.csv"
    detail_path = f"{args.input_dir}/RAW_QTTG_BHXH_DETAIL.csv"

    master_df = (
        spark.read.option("header", "true")
        .option("encoding", "UTF-8")
        .option("mode", "PERMISSIVE")
        .schema(MASTER_SCHEMA)
        .csv(master_path)
    )

    detail_df = (
        spark.read.option("header", "true")
        .option("encoding", "UTF-8")
        .option("mode", "PERMISSIVE")
        .schema(DETAIL_SCHEMA)
        .csv(detail_path)
    )

    master_count = master_df.count()
    detail_count = detail_df.count()

    master_df.write.mode("overwrite").parquet(f"{args.output_dir}/RAW_QTTG_BHXH")
    detail_df.write.mode("overwrite").parquet(f"{args.output_dir}/RAW_QTTG_BHXH_DETAIL")

    print(f"LAYER=BRONZE STATUS=SUCCESS MASTER_ROWS={master_count} DETAIL_ROWS={detail_count}")

    if master_count != 142857:
        raise SystemExit(f"Bronze master count mismatch: expected 142857, got {master_count}")
    if detail_count != 1000000:
        raise SystemExit(f"Bronze detail count mismatch: expected 1000000, got {detail_count}")

    spark.stop()


if __name__ == "__main__":
    main()