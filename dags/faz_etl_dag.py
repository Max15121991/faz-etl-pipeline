"""
FAZ ETL Pipeline – Apache Airflow DAG
======================================
Stack: Azure Data Lake Storage Gen2, Azure SQL, Databricks / PySpark, Power BI
Orchestrierung: Apache Airflow 2.x

Abhängigkeiten:
  pip install apache-airflow apache-airflow-providers-microsoft-azure \
              apache-airflow-providers-databricks \
              apache-airflow-providers-http \
              pandas pyarrow sqlalchemy

Airflow-Connections (in der UI oder per CLI anlegen):
  azure_adls          → Azure Data Lake Storage Gen2
  azure_sql_source    → Operative Azure SQL DB (Quelle)
  azure_sql_dwh       → Azure SQL Data Warehouse (Ziel)
  databricks_default  → Databricks REST API
  faz_api             → Externe REST-API (Basis-URL)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

import pandas as pd
from sqlalchemy import create_engine, text

log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
# DAG-Konfiguration
# ──────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner": "data-team-faz",
    "depends_on_past": False,
    "email": ["data-alerts@faz.net"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

# Geteilte Konstanten
ADLS_CONTAINER = "raw-data"
ADLS_PATH_PREFIX = "faz/{{ ds }}"          # Airflow-Template: YYYY-MM-DD
DWH_SCHEMA = "dbo"
DATABRICKS_JOB_ID = 12345                  # ← eigene Job-ID eintragen


# ──────────────────────────────────────────────────────────────
# Task-Funktionen
# ──────────────────────────────────────────────────────────────

# ── Extract ──────────────────────────────────────────────────

def extract_from_adls(**context) -> str:
    """
    Lädt Rohdaten aus Azure Data Lake Storage Gen2 (Parquet / CSV).
    Gibt den lokalen Pfad der heruntergeladenen Datei zurück (via XCom).
    """
    ds = context["ds"]
    blob_name = f"faz/{ds}/orders.parquet"
    local_path = f"/tmp/orders_{ds}.parquet"

    hook = WasbHook(wasb_conn_id="azure_adls")
    hook.get_file(
        file_path=local_path,
        container_name=ADLS_CONTAINER,
        blob_name=blob_name,
    )
    log.info(f"[EXTRACT] ADLS → {local_path} ({blob_name})")

    df = pd.read_parquet(local_path)
    log.info(f"[EXTRACT] {len(df)} Zeilen aus ADLS geladen")

    # Pfad via XCom weitergeben
    context["ti"].xcom_push(key="adls_local_path", value=local_path)
    return local_path


def extract_from_azure_sql(**context) -> None:
    """
    Liest Kundendaten aus der operativen Azure SQL DB und
    schreibt sie als Parquet in /tmp für den nächsten Task.
    """
    from airflow.hooks.base import BaseHook

    ds = context["ds"]
    conn = BaseHook.get_connection("azure_sql_source")
    engine = create_engine(
        f"mssql+pyodbc://{conn.login}:{conn.password}"
        f"@{conn.host}/{conn.schema}?driver=ODBC+Driver+18+for+SQL+Server"
    )

    with engine.connect() as db_conn:
        df = pd.read_sql(
            text("""
                SELECT id, name, email, segment, created_at
                FROM   dbo.customers
                WHERE  active = 1
                  AND  updated_at >= :since
            """),
            db_conn,
            params={"since": ds},
        )

    output_path = f"/tmp/customers_{ds}.parquet"
    df.to_parquet(output_path, index=False)
    log.info(f"[EXTRACT] Azure SQL → {output_path}: {len(df)} Kunden")
    context["ti"].xcom_push(key="sql_local_path", value=output_path)


def extract_from_api(**context) -> None:
    """
    Ruft externe Artikel-Metadaten-API der FAZ ab.
    SimpleHttpOperator übernimmt das HTTP-Handling;
    diese Funktion verarbeitet die Antwort weiter.
    """
    import json

    ds = context["ds"]
    response_str = context["ti"].xcom_pull(task_ids="extract.api_call")
    records = json.loads(response_str)

    df = pd.json_normalize(records)
    output_path = f"/tmp/articles_{ds}.parquet"
    df.to_parquet(output_path, index=False)
    log.info(f"[EXTRACT] API → {output_path}: {len(df)} Artikel")
    context["ti"].xcom_push(key="api_local_path", value=output_path)


# ── Staging auf ADLS ─────────────────────────────────────────

def stage_to_adls(**context) -> None:
    """
    Lädt alle extrahierten Rohdateien in die ADLS Staging-Zone hoch,
    damit Databricks darauf zugreifen kann.
    """
    ds = context["ds"]
    ti = context["ti"]

    files = {
        "orders": ti.xcom_pull(task_ids="extract.adls_load", key="adls_local_path"),
        "customers": ti.xcom_pull(task_ids="extract.sql_load", key="sql_local_path"),
        "articles": ti.xcom_pull(task_ids="extract.api_parse", key="api_local_path"),
    }

    hook = WasbHook(wasb_conn_id="azure_adls")
    for name, local_path in files.items():
        if local_path:
            blob_name = f"staging/{ds}/{name}.parquet"
            hook.load_file(
                file_path=local_path,
                container_name=ADLS_CONTAINER,
                blob_name=blob_name,
                overwrite=True,
            )
            log.info(f"[STAGE] {local_path} → adls://{ADLS_CONTAINER}/{blob_name}")


# ── Load in DWH ──────────────────────────────────────────────

def load_to_dwh(**context) -> None:
    """
    Liest Databricks-Output aus ADLS und schreibt ihn in Azure SQL DWH.
    Databricks schreibt Ergebnisse in adls://processed/{ds}/.
    """
    from airflow.hooks.base import BaseHook

    ds = context["ds"]
    conn = BaseHook.get_connection("azure_sql_dwh")
    engine = create_engine(
        f"mssql+pyodbc://{conn.login}:{conn.password}"
        f"@{conn.host}/{conn.schema}?driver=ODBC+Driver+18+for+SQL+Server"
    )

    # Databricks hat Ergebnisse in ADLS abgelegt
    adls_hook = WasbHook(wasb_conn_id="azure_adls")
    tables = {
        "fact_orders": f"processed/{ds}/fact_orders.parquet",
        "dim_customers": f"processed/{ds}/dim_customers.parquet",
        "agg_daily_revenue": f"processed/{ds}/agg_daily_revenue.parquet",
    }

    with engine.connect() as db_conn:
        for table_name, blob_path in tables.items():
            local = f"/tmp/{table_name}_{ds}.parquet"
            adls_hook.get_file(local, ADLS_CONTAINER, blob_path)

            df = pd.read_parquet(local)
            if_exists = "replace" if "dim_" in table_name or "agg_" in table_name else "append"

            df.to_sql(
                name=table_name,
                con=engine,
                schema=DWH_SCHEMA,
                if_exists=if_exists,
                index=False,
                chunksize=5000,
                method="multi",
            )
            log.info(f"[LOAD] {table_name}: {len(df)} Zeilen → Azure SQL DWH")


def notify_success(**context) -> None:
    """Abschluss-Log nach erfolgreichem Lauf."""
    ds = context["ds"]
    log.info(f"[DONE] Pipeline für {ds} erfolgreich abgeschlossen.")
    # Hier könnte ein Teams-/Slack-Webhook oder Power BI Refresh ausgelöst werden


# ──────────────────────────────────────────────────────────────
# DAG Definition
# ──────────────────────────────────────────────────────────────

with DAG(
    dag_id="faz_etl_pipeline",
    description="FAZ Data Platform ETL – ADLS → Databricks → Azure SQL DWH",
    schedule_interval="0 3 * * *",      # täglich 03:00 Uhr
    start_date=datetime(2024, 1, 1),
    catchup=False,                       # keine Nachverarbeitung alter Läufe
    max_active_runs=1,                   # kein paralleler Betrieb
    default_args=DEFAULT_ARGS,
    tags=["faz", "etl", "azure", "databricks"],
) as dag:

    # ── TaskGroup: Extract ─────────────────────────────────────
    with TaskGroup("extract") as extract_group:

        adls_load = PythonOperator(
            task_id="adls_load",
            python_callable=extract_from_adls,
        )

        sql_load = PythonOperator(
            task_id="sql_load",
            python_callable=extract_from_azure_sql,
        )

        # SimpleHttpOperator für direkten API-Aufruf
        api_call = SimpleHttpOperator(
            task_id="api_call",
            http_conn_id="faz_api",
            endpoint="/v1/articles",
            method="GET",
            data={"date": "{{ ds }}"},
            headers={"Accept": "application/json"},
            response_filter=lambda resp: resp.text,
            log_response=True,
        )

        api_parse = PythonOperator(
            task_id="api_parse",
            python_callable=extract_from_api,
        )

        # API: erst aufrufen, dann parsen
        api_call >> api_parse

    # ── TaskGroup: Stage ──────────────────────────────────────
    with TaskGroup("stage") as stage_group:

        stage_raw = PythonOperator(
            task_id="upload_to_adls",
            python_callable=stage_to_adls,
        )

    # ── TaskGroup: Transform (Databricks) ─────────────────────
    with TaskGroup("transform") as transform_group:

        databricks_transform = DatabricksRunNowOperator(
            task_id="spark_transform",
            databricks_conn_id="databricks_default",
            job_id=DATABRICKS_JOB_ID,
            notebook_params={
                "run_date": "{{ ds }}",
                "adls_container": ADLS_CONTAINER,
                "input_path": "staging/{{ ds }}",
                "output_path": "processed/{{ ds }}",
            },
        )

    # ── TaskGroup: Load ────────────────────────────────────────
    with TaskGroup("load") as load_group:

        dwh_load = PythonOperator(
            task_id="to_azure_sql_dwh",
            python_callable=load_to_dwh,
        )

    # ── Abschluss-Task ────────────────────────────────────────
    done = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Abhängigkeiten (DAG-Graph) ────────────────────────────
    #
    #  extract (adls + sql + api parallel)
    #      │
    #      ▼
    #    stage
    #      │
    #      ▼
    #  transform (Databricks Spark Job)
    #      │
    #      ▼
    #    load
    #      │
    #      ▼
    #    done

    [adls_load, sql_load, api_parse] >> stage_raw
    stage_group >> databricks_transform
    transform_group >> dwh_load
    load_group >> done