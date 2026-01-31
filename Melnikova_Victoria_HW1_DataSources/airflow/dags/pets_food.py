import json
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

JSON_FILE = "/opt/airflow/dags/data/pets-data.json"
XML_FILE = "/opt/airflow/dags/data/nutrition.xml"


def load_pets_json():
    with open(JSON_FILE, "r") as f:
        json_data = json.load(f)

    df = pd.json_normalize(json_data, record_path=["pets"])

    df.rename(columns={"birthYear": "birth_year", "favFoods": "fav_foods"}, inplace=True)

    df["fav_foods"] = df["fav_foods"].apply(
        lambda x: ", ".join(x) if isinstance(x, list) else x
    )

    df = df[["name", "species", "birth_year", "fav_foods", "photo"]]

    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS pets_json (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100),
                    species VARCHAR(50),
                    birth_year INT,
                    fav_foods TEXT,
                    photo TEXT
                );
                """
            )

    df.to_sql("pets_json", engine, if_exists="append", index=False)


def load_nutrition_xml():
    tree = ET.parse(XML_FILE)
    root = tree.getroot()

    rows = []
    for food in root.findall("food"):
        name = food.findtext("name")
        mfr = food.findtext("mfr")

        serving_el = food.find("serving")
        serving = float(serving_el.text) if serving_el is not None and serving_el.text else None
        serving_units = serving_el.get("units") if serving_el is not None else None

        calories_el = food.find("calories")
        calories_total = calories_el.get("total") if calories_el is not None else None
        calories_fat = calories_el.get("fat") if calories_el is not None else None

        def _float_or_none(tag):
            el = food.find(tag)
            if el is not None and el.text not in (None, ""):
                return float(el.text)
            return None

        total_fat = _float_or_none("total-fat")
        saturated_fat = _float_or_none("saturated-fat")
        cholesterol = _float_or_none("cholesterol")
        sodium = _float_or_none("sodium")
        carb = _float_or_none("carb")
        fiber = _float_or_none("fiber")
        protein = _float_or_none("protein")

        vitamins_el = food.find("vitamins")
        vit_a = float(vitamins_el.findtext("a")) if vitamins_el is not None and vitamins_el.findtext("a") else None
        vit_c = float(vitamins_el.findtext("c")) if vitamins_el is not None and vitamins_el.findtext("c") else None

        minerals_el = food.find("minerals")
        min_ca = float(minerals_el.findtext("ca")) if minerals_el is not None and minerals_el.findtext("ca") else None
        min_fe = float(minerals_el.findtext("fe")) if minerals_el is not None and minerals_el.findtext("fe") else None

        rows.append(
            {
                "name": name,
                "mfr": mfr,
                "serving": serving,
                "serving_units": serving_units,
                "calories_total": float(calories_total) if calories_total else None,
                "calories_fat": float(calories_fat) if calories_fat else None,
                "total_fat": total_fat,
                "saturated_fat": saturated_fat,
                "cholesterol": cholesterol,
                "sodium": sodium,
                "carb": carb,
                "fiber": fiber,
                "protein": protein,
                "vit_a": vit_a,
                "vit_c": vit_c,
                "min_ca": min_ca,
                "min_fe": min_fe,
            }
        )

    df = pd.DataFrame(rows)

    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS nutrition_foods (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200),
                    mfr VARCHAR(100),
                    serving DOUBLE PRECISION,
                    serving_units VARCHAR(20),
                    calories_total DOUBLE PRECISION,
                    calories_fat DOUBLE PRECISION,
                    total_fat DOUBLE PRECISION,
                    saturated_fat DOUBLE PRECISION,
                    cholesterol DOUBLE PRECISION,
                    sodium DOUBLE PRECISION,
                    carb DOUBLE PRECISION,
                    fiber DOUBLE PRECISION,
                    protein DOUBLE PRECISION,
                    vit_a DOUBLE PRECISION,
                    vit_c DOUBLE PRECISION,
                    min_ca DOUBLE PRECISION,
                    min_fe DOUBLE PRECISION
                );
                """
            )

    df.to_sql("nutrition_foods", engine, if_exists="append", index=False)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 25),
}

with DAG(
    dag_id="pets_and_nutrition",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["etl", "json", "xml"],
) as dag:
    load_pets = PythonOperator(
        task_id="load_pets_json",
        python_callable=load_pets_json,
    )

    load_nutrition = PythonOperator(
        task_id="load_nutrition_xml",
        python_callable=load_nutrition_xml,
    )

    load_pets >> load_nutrition