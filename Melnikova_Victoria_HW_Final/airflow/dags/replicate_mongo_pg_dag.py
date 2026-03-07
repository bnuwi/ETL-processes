from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

from pymongo import MongoClient
import pandas as pd

MONGO_URI = "mongodb://root:root@de-mongodb:27017/"
MONGO_DB = "app_db"


def etl_sessions():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    data = list(db["user_sessions"].find())
    if not data:
        print("NO SESSIONS IN MONGO")
        return

    df = pd.DataFrame(data)
    if "_id" in df.columns:
        df = df.drop(columns=["_id"])

    df.drop_duplicates(subset=["session_id"], inplace=True)
    df["start_time"] = pd.to_datetime(df["start_time"])
    df["end_time"] = pd.to_datetime(df["end_time"])
    df["duration"] = (df["end_time"] - df["start_time"]).dt.total_seconds() / 60

    df = df[
        [
            "session_id",
            "user_id",
            "start_time",
            "end_time",
            "pages_visited",
            "device",
            "actions",
            "duration",
        ]
    ]

    print("SESSIONS ROWS:", len(df))

    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cur = conn.cursor()

    users = df[["user_id"]].drop_duplicates()
    for _, row in users.iterrows():
        cur.execute(
            """
            INSERT INTO users(user_id)
            VALUES (%s)
            ON CONFLICT (user_id) DO NOTHING;
            """,
            (row["user_id"],),
        )

    for _, r in df.iterrows():
        cur.execute(
            """
            INSERT INTO user_sessions_raw(
                session_id, user_id, start_time, end_time,
                pages_visited, device, actions, duration
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (session_id) DO NOTHING;
            """,
            (
                r["session_id"],
                r["user_id"],
                r["start_time"],
                r["end_time"],
                r["pages_visited"],
                r["device"],
                r["actions"],
                r["duration"],
            ),
        )

    conn.commit()
    cur.close()
    conn.close()


def etl_tickets():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    data = list(db["support_tickets"].find())
    if not data:
        print("NO TICKETS IN MONGO")
        return

    df = pd.DataFrame(data)
    if "_id" in df.columns:
        df = df.drop(columns=["_id"])

    df.drop_duplicates(subset=["ticket_id"], inplace=True)
    df["created_at"] = pd.to_datetime(df["created_at"])
    df["updated_at"] = pd.to_datetime(df["updated_at"])
    df["resolution_time_hours"] = (
        df["updated_at"] - df["created_at"]
    ).dt.total_seconds() / 3600

    df = df[
        [
            "ticket_id",
            "user_id",
            "status",
            "issue_type",
            "created_at",
            "updated_at",
            "resolution_time_hours",
        ]
    ]

    print("TICKETS ROWS:", len(df))

    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cur = conn.cursor()

    users = df[["user_id"]].drop_duplicates()
    for _, row in users.iterrows():
        cur.execute(
            """
            INSERT INTO users(user_id)
            VALUES (%s)
            ON CONFLICT (user_id) DO NOTHING;
            """,
            (row["user_id"],),
        )

    for _, r in df.iterrows():
        cur.execute(
            """
            INSERT INTO support_tickets(
                ticket_id, user_id, status, issue_type,
                created_at, updated_at, resolution_time_hours
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (ticket_id) DO NOTHING;
            """,
            (
                r["ticket_id"],
                r["user_id"],
                r["status"],
                r["issue_type"],
                r["created_at"],
                r["updated_at"],
                r["resolution_time_hours"],
            ),
        )

    conn.commit()
    cur.close()
    conn.close()


default_args = {"owner": "student"}

with DAG(
    dag_id="replicate_mongo_pg",
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule_interval=None,
    catchup=False,
    tags=["etl"],
) as dag:

    etl_sessions_task = PythonOperator(
        task_id="etl_sessions",
        python_callable=etl_sessions,
    )

    etl_tickets_task = PythonOperator(
        task_id="etl_tickets",
        python_callable=etl_tickets,
    )

    etl_sessions_task >> etl_tickets_task