from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


# Lấy đúng tên từ lệnh: docker ps --format "table {{.Names}}"
SPARK_CONTAINER = "spark-master"
SPARK_MASTER = "spark://spark-master:7077"
APP_DIR = "/opt/spark/apps/qttg"


def spark_submit(app_name: str, app_args: str = "") -> str:
    return f"""
set -e
docker exec {SPARK_CONTAINER} \
  /opt/spark/bin/spark-submit \
  --master {SPARK_MASTER} \
  --deploy-mode client \
  --driver-memory 1g \
  --executor-memory 2g \
  --executor-cores 2 \
  {APP_DIR}/{app_name} {app_args}
""".strip()


default_args = {
    "owner": "student",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


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
        bash_command=spark_submit(
            "bronze_qttg.py",
            "--input-dir file:///opt/spark/data/raw_qttg_1m "
            "--output-dir file:///opt/spark/data/lake/bronze",
        ),
        execution_timeout=timedelta(hours=1),
    )

    silver = BashOperator(
        task_id="silver_latest_person",
        bash_command=spark_submit(
            "silver_qttg.py",
            "--input-dir file:///opt/spark/data/lake/bronze "
            "--output-dir file:///opt/spark/data/lake/silver",
        ),
        execution_timeout=timedelta(hours=1),
    )

    gold = BashOperator(
        task_id="gold_monthly_report",
        bash_command=spark_submit(
            "gold_qttg.py",
            "--input-dir file:///opt/spark/data/lake/silver "
            "--output-dir file:///opt/spark/data/lake/gold",
        ),
        execution_timeout=timedelta(hours=1),
    )

    validate = BashOperator(
        task_id="validate_results",
        bash_command=spark_submit(
            "validate_qttg.py",
            "--lake-dir file:///opt/spark/data/lake",
        ),
        execution_timeout=timedelta(minutes=30),
    )

    bronze >> silver >> gold >> validate