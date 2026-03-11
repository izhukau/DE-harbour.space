from __future__ import annotations

import os
import json
import logging
from datetime import datetime, timezone

import pandas as pd
import pyarrow.parquet as pq
import psycopg2
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.operators.python import PythonOperator


DATA_DIR    = "/opt/airflow/data"
TRACKER     = "/opt/airflow/data/.processed_files.json"

DB_CONF = dict(
    host="postgres",
    port=5432,
    dbname="airflow",
    user="airflow",
    password="airflow",
)


def load_tracker() -> set:
    if os.path.exists(TRACKER):
        with open(TRACKER) as f:
            return set(json.load(f))
    return set()

def save_tracker(processed: set):
    with open(TRACKER, "w") as f:
        json.dump(list(processed), f)


def find_new_files(**ctx):
    processed = load_tracker()
    all_files = [
        f for f in os.listdir(DATA_DIR)
        if f.endswith(".parquet") and f not in processed
    ]
    all_files.sort()  
    logging.info(f"New files found: {all_files}")
    ctx["ti"].xcom_push(key="new_files", value=all_files)


def create_table_if_not_exists(**_):
    ddl = """
    CREATE TABLE IF NOT EXISTS fuel_exports (
        transaction_id  TEXT PRIMARY KEY,
        station_id      INTEGER,
        dock_bay        SMALLINT,
        dock_level      TEXT,
        ship_name       TEXT,
        franchise       TEXT,
        captain_name    TEXT,
        species         TEXT,
        fuel_type       TEXT,
        fuel_units      DOUBLE PRECISION,
        price_per_unit  NUMERIC(8,2),
        total_cost      NUMERIC(12,2),
        services        TEXT[],
        is_emergency    BOOLEAN,
        visited_at      TIMESTAMPTZ,
        arrival_date    DATE,
        coords_x        DOUBLE PRECISION,
        coords_y        DOUBLE PRECISION,
        loaded_at       TIMESTAMPTZ DEFAULT now()
    );
    """
    with psycopg2.connect(**DB_CONF) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    logging.info("Table fuel_exports is ready.")


def load_files(**ctx):
    new_files = ctx["ti"].xcom_pull(key="new_files", task_ids="find_new_files")
    if not new_files:
        logging.info("Nothing to load.")
        return

    processed = load_tracker()

    with psycopg2.connect(**DB_CONF) as conn:
        with conn.cursor() as cur:
            for fname in new_files:
                fpath = os.path.join(DATA_DIR, fname)
                try:
                    table = pq.read_table(fpath)
                    df = table.to_pandas()

                    df["dock_bay"]   = df["dock"].apply(lambda d: d.get("bay")   if isinstance(d, dict) else None)
                    df["dock_level"] = df["dock"].apply(lambda d: d.get("level") if isinstance(d, dict) else None)
                    df.drop(columns=["dock"], inplace=True)

                    df["services"] = df["services"].apply(
                        lambda s: list(s) if s is not None else []
                    )

                    if df["visited_at"].dt.tz is None:
                        df["visited_at"] = df["visited_at"].dt.tz_localize("UTC")

                    cols = [
                        "transaction_id", "station_id",
                        "dock_bay", "dock_level",
                        "ship_name", "franchise",
                        "captain_name", "species",
                        "fuel_type", "fuel_units",
                        "price_per_unit", "total_cost",
                        "services",
                        "is_emergency",
                        "visited_at", "arrival_date",
                        "coords_x", "coords_y",
                    ]
                    rows = [tuple(row) for row in df[cols].itertuples(index=False)]

                    insert_sql = f"""
                        INSERT INTO fuel_exports ({', '.join(cols)})
                        VALUES %s
                        ON CONFLICT (transaction_id) DO NOTHING
                    """
                    execute_values(cur, insert_sql, rows)
                    conn.commit()

                    processed.add(fname)
                    save_tracker(processed)
                    logging.info(f"Loaded {len(rows)} rows from {fname}")

                except Exception as e:
                    logging.error(f"Failed to process {fname}: {e}")
                    raise


with DAG(
    dag_id="fuel_exports_etl",
    description="Load fuel export Parquet files into PostgreSQL every minute",
    schedule_interval="* * * * *",      
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "fuel"],
) as dag:

    t1 = PythonOperator(task_id="find_new_files",            python_callable=find_new_files)
    t2 = PythonOperator(task_id="create_table_if_not_exists", python_callable=create_table_if_not_exists)
    t3 = PythonOperator(task_id="load_files",                python_callable=load_files)

    t1 >> t2 >> t3