import argparse

from pyspark.sql import SparkSession, Window # type: ignore
from pyspark.sql import functions as F # type: ignore
from pyspark.sql.types import StringType # type: ignore

MASTER_COLUMNS = [
    "ID",
    "NLD_ID",
    "SO_SO_BHXH",
    "THANG_BD",
    "THANG_KT",
    "TT_TG_BHXH",
    "DT_TG_BHXH",
    "NAM_TG_BHXH",
    "THANG_TG_BHXH",
    "NAM_TG_BHXH_BB",
    "THANG_TG_BHXH_BB",
    "TT_TG_BHTN",
    "DT_TG_BHTN",
    "NAM_TG_BHTN",
    "THANG_TG_BHTN",
    "TT_TG_BHYT",
    "DT_TG_BHYT",
    "NAM_TG_BHYT",
    "THANG_TG_BHYT",
    "NAM_NO_BHXH",
    "THANG_NO_BHXH",
    "NAM_NO_BHTN",
    "THANG_NO_BHTN",
    "TT_TG_BH",
    "DT_TG_BH",
    "TU_THANG_DVI",
    "DEN_THANG_DVI",
    "DEN_THANG_HTTT",
    "DEN_THANG_BHTN",
    "THANG_BD_LT",
    "THANG_KT_LT",
    "SO_THANG_LT",
    "IS_ERRORS",
    "NGHI_VIEC",
    "IS_CONTINUE",
    "TRUY_DONG",
    "DEN_NGAY",
    "MA_CD",
    "MA_NHH",
    "DD_MA_DON_VI",
    "DD_THANG_DONG_DEN_XH",
    "DD_TY_LE_NO_BHXH",
    "DD_THANG_DONG_DEN_YT",
    "DD_TY_LE_NO_BHYT",
    "DD_THANG_DONG_DEN_TN",
    "DD_TY_LE_NO_BHTN",
    "DD_THANG_DONG_DEN_TNLD",
    "DD_TY_LE_NO_TNLD",
    "RAW_RESPONSE",
    "CREATED_AT",
]

DETAIL_COLUMNS = [
    "ID",
    "MASTER_ID",
    "NLD_ID",
    "DOT_PHAT_SINH",
    "TU_THANG",
    "DEN_THANG",
    "MA_DON_VI",
    "TEN_DON_VI",
    "LOAI_DT",
    "LOAI",
    "PA",
    "DON_VI_TINH",
    "MA_NT",
    "CHUC_DANH_CV",
    "CHUC_DANH_CV_PRE",
    "NOI_LAM_VIEC",
    "NOI_DUNG",
    "MUC_LUONG",
    "MUC_LUONG_TN",
    "MUC_LUONG_BHYT",
    "MUC_LUONG_PC",
    "MUC_LUONG_BS",
    "MUC_LUONG_NLD",
    "MUC_LUONG_NSNN",
    "MUC_LUONG_HS",
    "MUC_LUONG_TT",
    "HS_LUONG",
    "PC_CHUC_VU",
    "PC_THAM_NIEN",
    "PC_NGHE",
    "PC_KHU_VUC",
    "PC_KHAC",
    "PC_TAI_CU",
    "HS_TN",
    "HS_NG",
    "HS_TC",
    "TYLE_BHXH",
    "TYLE_BHYT",
    "TYLE_BHTN",
    "TYLE_TUDV",
    "TYLE_HTTT",
    "TYLE_ODTS",
    "TYLE_TNLD",
    "TYLE_NSNN",
    "DK1",
    "DK2",
    "DK3",
    "DK4",
    "DK5",
    "DK6",
    "IS_BHXH",
    "IS_BHXH_BB",
    "IS_BHTN",
    "IS_BHYT",
    "IS_BHXH2",
    "IS_BHTN2",
    "IS_ERROR",
    "IS_TR",
    "IS_BONUS",
    "ML_TC",
    "GHI_CHU",
    "KIEM_TRA",
    "SO_THANG",
    "MA_KHOI_TK",
    "TY_LE_DONG",
    "MUC_DONG",
    "LUONG_CHINH",
    "CHUC_DANH_NLV",
    "PHUONG_THUC",
    "PHUONG_THUC_DONG",
    "MUC_LUONG_PRE",
    "MUC_LUONG_PC_PRE",
    "MUC_LUONG_BS_PRE",
    "HS_LUONG_PRE",
    "PC_CHUC_VU_PRE",
    "PC_THAM_NIEN_PRE",
    "PC_NGHE_PRE",
    "PC_KHU_VUC_PRE",
    "PC_KHAC_PRE",
    "PC_TAI_CU_PRE",
    "LUONG_CHINH_PRE",
    "CREATED_AT",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Silver layer QTTG")
    parser.add_argument("--input-dir", required=True, help="dir bronze")
    parser.add_argument("--output-dir", required=True, help="dir ghi silver")
    return parser.parse_args()


def normalize_master(df):
    """Chuẩn hóa master: trim chuỗi, null path, lấy tháng chuẩn."""
    return df.select(
        *[
            F.trim(F.col(c)).alias(c) if isinstance(df.schema[c].dataType, StringType) else F.col(c)
            for c in MASTER_COLUMNS
        ]
    )


def normalize_detail(df):
    """Chuẩn hóa detail: trim chuỗi, null path."""
    return df.select(
        *[
            F.trim(F.col(c)).alias(c) if isinstance(df.schema[c].dataType, StringType) else F.col(c)
            for c in DETAIL_COLUMNS
        ]
    )


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("QTTG_Silver")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    master_df = spark.read.parquet(f"{args.input_dir}/RAW_QTTG_BHXH")
    detail_df = spark.read.parquet(f"{args.input_dir}/RAW_QTTG_BHXH_DETAIL")

    master_df = normalize_master(master_df)
    detail_df = normalize_detail(detail_df)

    # Chọn phiên bản mới nhất cho mỗi người (SO_SO_BHXH)
    window_spec = Window.partitionBy("SO_SO_BHXH").orderBy(F.col("CREATED_AT").desc(), F.col("ID").desc())
    latest_master_df = (
        master_df.withColumn("RN", F.row_number().over(window_spec))
        .filter(F.col("RN") == 1)
        .drop("RN")
    )

    # Chỉ lấy detail có MASTER_ID thuộc các master vừa chọn
    latest_master_ids = latest_master_df.select(F.col("ID").alias("MASTER_ID"))
    latest_detail_df = detail_df.join(latest_master_ids, on="MASTER_ID", how="inner")

    person_count = latest_master_df.count()
    detail_count = latest_detail_df.count()

    latest_master_df.write.mode("overwrite").parquet(f"{args.output_dir}/RAW_QTTG_BHXH")
    latest_detail_df.write.mode("overwrite").parquet(f"{args.output_dir}/RAW_QTTG_BHXH_DETAIL")

    print(f"LAYER=SILVER STATUS=SUCCESS PERSON_ROWS={person_count} DETAIL_ROWS={detail_count}")

    spark.stop()


if __name__ == "__main__":
    main()