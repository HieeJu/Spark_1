import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-dir", required=True)
    parser.add_argument("--db-url", default="jdbc:postgresql://tst-postgres:5432/airflow")
    parser.add_argument("--db-user", default="airflow")
    parser.add_argument("--db-pass", default="airflow")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("Silver_To_Postgres_Loader") \
        .getOrCreate()

    print("--- Reading Silver Parquet ---")
    master_df = spark.read.parquet(f"{args.silver_dir}/master")
    detail_df = spark.read.parquet(f"{args.silver_dir}/detail")

    # Mapping cột chữ hoa sang chữ thường đúng schema DB Postgres
    master_df_pg = master_df.select(
        col("ID").alias("id"),
        col("SO_SO_BHXH").alias("so_so_bhxh"),
        col("NLD_ID").alias("nld_id"),
        col("THANG_BD").alias("thang_bd"),
        col("THANG_KT").alias("thang_kt")
    )

    detail_df_pg = detail_df.select(
        col("ID").alias("id"),
        col("MASTER_ID").alias("master_id"),
        col("NLD_ID").alias("nld_id"),
        col("TU_THANG").alias("tu_thang"),
        col("DEN_THANG").alias("den_thang"),
        col("MA_DON_VI").alias("ma_don_vi"),
        col("TEN_DON_VI").alias("ten_don_vi"),
        col("CHUC_DANH_CV").alias("chuc_danh_cv"),
        col("MUC_LUONG").alias("muc_luong")
    )

    jdbc_properties = {
        "user": args.db_user,
        "password": args.db_pass,
        "driver": "org.postgresql.Driver",
        "batchsize": "10000"
    }

    # Ghi bảng Master trước để đảm bảo Khóa chính
    print("Writing qttg_master to Postgres...")
    master_df_pg.write.mode("append").jdbc(url=args.db_url, table="qttg_master", properties=jdbc_properties)

    # Ghi bảng Detail sau để đảm bảo Khóa ngoại
    print("Writing qttg_detail to Postgres...")
    detail_df_pg.write.mode("append").jdbc(url=args.db_url, table="qttg_detail", properties=jdbc_properties)

    print("LAYER=POSTGRES STATUS=SUCCESS Load completed!")
    spark.stop()

if __name__ == "__main__":
    main()