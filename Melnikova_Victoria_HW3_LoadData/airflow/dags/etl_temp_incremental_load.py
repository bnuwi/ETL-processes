import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

CSV_FILE = "/opt/airflow/dags/data/temp_readings.csv"


def incremental_load_temp_daily(days_back: int = 7):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT max(date) FROM temp_daily;")
            row = cur.fetchone()
            max_date_in_db = row[0]  

    df = pd.read_csv(CSV_FILE)
    df = df[df["out/in"].str.strip().str.lower() == "in"].copy()
    df["noted_date"] = pd.to_datetime(df["noted_date"], format="%d-%m-%Y %H:%M")
    df["date"] = df["noted_date"].dt.date


    today = datetime.utcnow().date()
    min_date_filter = today - timedelta(days=days_back)

    if max_date_in_db is not None and max_date_in_db > min_date_filter:
        min_date_filter = max_date_in_db

    df = df[df["date"] > min_date_filter].copy()

    if df.empty:
        print("Нет новых данных для загрузки.")
        return

    low, high = df["temp"].quantile([0.05, 0.95])
    df = df[(df["temp"] >= low) & (df["temp"] <= high)].copy()

    if df.empty:
        print("Все новые данные отфильтрованы по перцентилям.")
        return

    daily_new = (
        df.groupby("date", as_index=False)
        .agg(
            avg_temp=("temp", "mean"),
            min_temp=("temp", "min"),
            max_temp=("temp", "max"),
            reading_count=("temp", "count"),
        )
        .round(2)
    )

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS temp_daily (
                    date DATE PRIMARY KEY,
                    avg_temp DOUBLE PRECISION,
                    min_temp DOUBLE PRECISION,
                    max_temp DOUBLE PRECISION,
                    reading_count INT
                );
                """
            )
            conn.commit()

        temp_table_name = "temp_daily_incremental_tmp"
        daily_new.to_sql(temp_table_name, engine, if_exists="replace", index=False)

        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO temp_daily AS t (date, avg_temp, min_temp, max_temp, reading_count)
                SELECT date, avg_temp, min_temp, max_temp, reading_count
                FROM {temp_table_name}
                ON CONFLICT (date) DO UPDATE
                SET avg_temp = EXCLUDED.avg_temp,
                    min_temp = EXCLUDED.min_temp,
                    max_temp = EXCLUDED.max_temp,
                    reading_count = EXCLUDED.reading_count;
                """
            )
            conn.commit()

    print(f"Инкрементально загружено {len(daily_new)} дат.")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
}

with DAG(
    dag_id="etl_temp_incremental_load",
    default_args=default_args,
    schedule_interval="@daily", 
    catchup=False,
    tags=["etl", "csv", "temperature", "incremental"],
) as dag:
    run_incremental = PythonOperator(
        task_id="incremental_load_temp_daily",
        python_callable=incremental_load_temp_daily,
    )
