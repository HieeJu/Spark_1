from datetime import datetime, timedelta

from airflow import DAG  # type: ignore
from airflow.operators.bash import BashOperator  # type: ignore

# Định nghĩa câu lệnh gửi trực tiếp job tới Spark Master thông qua Spark Submit REST API hoặc client nhẹ
SPARK_MASTER = "spark://spark-master:7077"
APP_DIR = "/opt/spark/apps/qttg"

default_args = {
    "owner": "student",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def run_spark_script(app_name: str, app_args: str = "") -> str:
    # Chạy script thông qua python driver đơn giản không phụ thuộc vào Docker CLI
    return f"python3 {APP_DIR}/{app_name} {app_args}"


with DAG(
    dag_id="qttg_bronze_silver_gold",
    description="Bài tập Spark QTTG chạy local bằng Docker Desktop",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["spark", "qttg", "local"],
) as dag:
    bronze = BashOperator(
        task_id="bronze_ingest_csv",
        bash_command=run_spark_script(
            "bronze_qttg.py",
            "--input-dir file:///opt/spark/data/raw_qttg_1m "
            "--output-dir file:///opt/spark/data/lake/bronze",
        ),
        execution_timeout=timedelta(hours=1),
    )

    silver = BashOperator(
        task_id="silver_latest_person",
        bash_command=run_spark_script(
            "silver_qttg.py",
            "--input-dir file:///opt/spark/data/lake/bronze "
            "--output-dir file:///opt/spark/data/lake/silver",
        ),
        execution_timeout=timedelta(hours=1),
    )

    gold = BashOperator(
        task_id="gold_monthly_report",
        bash_command=run_spark_script(
            "gold_qttg.py",
            "--input-dir file:///opt/spark/data/lake/silver "
            "--output-dir file:///opt/spark/data/lake/gold",
        ),
        execution_timeout=timedelta(hours=1),
    )

    validate = BashOperator(
        task_id="validate_results",
        bash_command=run_spark_script(
            "validate_qttg.py",
            "--lake-dir file:///opt/spark/data/lake",
        ),
        execution_timeout=timedelta(minutes=30),
    )

    bronze >> silver >> gold >> validate