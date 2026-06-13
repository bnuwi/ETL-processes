"""
Airflow DAG: запуск PySpark-задания на кластере Yandex Data Processing.

Расписание: ежедневно в 03:00 UTC.
Кластер Data Processing: dataproc102
Бакет S3: etl-mel
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreatePysparkJobOperator,
)

DATAPROC_CLUSTER_ID = "c9qxxxxxxxxxxxxxxxx"   # ID кластера dataproc102
S3_BUCKET           = "etl-mel"
SCRIPT_PATH         = f"s3a://{S3_BUCKET}/scripts/process_loans.py"
INPUT_PATH          = f"s3a://{S3_BUCKET}/input/loans_input.csv"
OUTPUT_PATH         = f"s3a://{S3_BUCKET}/output/loans_processed"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 6, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="loans_etl_pipeline",
    default_args=default_args,
    description="ETL пайплайн обработки данных по кредитам через PySpark на Yandex Data Processing",
    schedule_interval="0 3 * * *",   
    catchup=False,
    tags=["etl", "pyspark", "dataproc", "loans"],
) as dag:

    run_pyspark_job = DataprocCreatePysparkJobOperator(
        task_id="run_pyspark_loans_etl",
        cluster_id=DATAPROC_CLUSTER_ID,
        main_python_file_uri=SCRIPT_PATH,
        args=[INPUT_PATH, OUTPUT_PATH],
        properties={
            "spark.executor.memory":        "2g",
            "spark.executor.cores":         "2",
            "spark.driver.memory":          "1g",
            "spark.hadoop.fs.s3a.endpoint": "https://storage.yandexcloud.net",
        },
        name="loans-etl-pyspark",
        connection_id="yandexcloud_default",
    )