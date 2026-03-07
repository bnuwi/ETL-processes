from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {"owner": "student"}

with DAG(
    dag_id="analytics_marts_dag",
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["analytics"],
) as dag:

    create_mart1 = PostgresOperator(
    task_id="create_user_activity_mart",
    postgres_conn_id="postgres_default",
    sql="""
    DROP MATERIALIZED VIEW IF EXISTS user_activity_mart;

    CREATE MATERIALIZED VIEW user_activity_mart AS
    WITH sessions AS (
        SELECT
            user_id,
            duration,
            start_time,
            unnest(pages_visited) AS page,
            unnest(actions)       AS action
        FROM user_sessions_raw
    ),
    agg AS (
        SELECT
            user_id,
            date_trunc('day', start_time) AS activity_date,
            COUNT(DISTINCT start_time)    AS session_count,
            AVG(duration)                 AS avg_session_min
        FROM user_sessions_raw
        GROUP BY user_id, activity_date
    ),
    top_pages AS (
        SELECT
            user_id,
            date_trunc('day', start_time) AS activity_date,
            page,
            COUNT(*) AS page_hits,
            ROW_NUMBER() OVER (
                PARTITION BY user_id, date_trunc('day', start_time)
                ORDER BY COUNT(*) DESC
            ) AS rn
        FROM sessions
        GROUP BY user_id, activity_date, page
    ),
    top_actions AS (
        SELECT
            user_id,
            date_trunc('day', start_time) AS activity_date,
            action,
            COUNT(*) AS action_hits,
            ROW_NUMBER() OVER (
                PARTITION BY user_id, date_trunc('day', start_time)
                ORDER BY COUNT(*) DESC
            ) AS rn
        FROM sessions
        GROUP BY user_id, activity_date, action
    )
    SELECT
        a.user_id,
        a.session_count,
        a.avg_session_min,
        a.activity_date,
        tp.page   AS most_popular_page,
        ta.action AS most_popular_action
    FROM agg a
    LEFT JOIN top_pages tp
      ON a.user_id = tp.user_id
     AND a.activity_date = tp.activity_date
     AND tp.rn = 1
    LEFT JOIN top_actions ta
      ON a.user_id = ta.user_id
     AND a.activity_date = ta.activity_date
     AND ta.rn = 1;
    """,
)

    create_mart2 = PostgresOperator(
        task_id="create_support_efficiency_mart",
        postgres_conn_id="postgres_default",
        sql="""
        DROP MATERIALIZED VIEW IF EXISTS support_efficiency_mart;
        CREATE MATERIALIZED VIEW support_efficiency_mart AS
        SELECT 
            issue_type,
            status,
            COUNT(*) AS ticket_count,
            AVG(resolution_time_hours) AS avg_resolution_hours,
            COUNT(*) FILTER (WHERE status = 'open') AS open_tickets
        FROM support_tickets
        GROUP BY issue_type, status;
        """,
    )

    refresh_marts = PostgresOperator(
        task_id="refresh_marts",
        postgres_conn_id="postgres_default",
        sql="""
        REFRESH MATERIALIZED VIEW user_activity_mart;
        REFRESH MATERIALIZED VIEW support_efficiency_mart;
        """,
    )

    create_mart1 >> create_mart2 >> refresh_marts
