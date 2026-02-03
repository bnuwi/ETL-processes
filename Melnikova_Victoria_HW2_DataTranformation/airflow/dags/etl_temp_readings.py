import pandas as pd
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

CSV_FILE = "/opt/airflow/dags/data/temp_readings.csv"


def etl_temperature_readings():
    df = pd.read_csv(CSV_FILE)

    df = df[df["out/in"].str.strip().str.lower() == "in"].copy()

    
    df["noted_date"] = pd.to_datetime(df["noted_date"], format="%d-%m-%Y %H:%M")
    df["date"] = df["noted_date"].dt.date 

    low, high = df["temp"].quantile([0.05, 0.95])
    df = df[(df["temp"] >= low) & (df["temp"] <= high)].copy()

    daily = (
        df.groupby("date", as_index=False)
        .agg(
            avg_temp=("temp", "mean"),
            min_temp=("temp", "min"),
            max_temp=("temp", "max"),
            reading_count=("temp", "count"),
        )
        .round(2)
    )

    hottest = daily.nlargest(5, "avg_temp")
    coldest = daily.nsmallest(5, "avg_temp")
    hottest["type"] = "hottest"
    coldest["type"] = "coldest"
    extremes = pd.concat([hottest, coldest], ignore_index=True)
    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS temp_extremes (
                    id SERIAL PRIMARY KEY,
                    date DATE,
                    avg_temp DOUBLE PRECISION,
                    min_temp DOUBLE PRECISION,
                    max_temp DOUBLE PRECISION,
                    reading_count INT,
                    type VARCHAR(10)
                );
                """
            )

    extremes.to_sql("temp_extremes", engine, if_exists="replace", index=False)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
}

with DAG(
    dag_id="etl_temp_readings",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["etl", "csv", "temperature"],
) as dag:
    run_etl = PythonOperator(
        task_id="process_temp_readings",
        python_callable=etl_temperature_readings,
    )