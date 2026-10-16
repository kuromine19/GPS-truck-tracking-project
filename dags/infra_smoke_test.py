"""DAG kiểm tra Airflow -> Spark -> MinIO. Bật DAG rồi bấm Trigger trên giao diện Airflow."""
from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="infra_smoke_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["infra"],
) as dag:
    SparkSubmitOperator(
        task_id="spark_smoke_test",
        conn_id="spark_default",
        application="/opt/infra/scripts/smoke_test.py",
        conf={"spark.driver.host": "airflow-scheduler"},  # executor kết nối ngược về driver
        name="airflow-smoke-test",
    )
