"""
ETL Pipeline – Beispielimplementierung für Junior Data Engineer
================================================================
Struktur:
  1. Extract  – Daten aus CSV, PostgreSQL und REST API laden
  2. Transform – Bereinigung, Normalisierung, Aggregation
  3. Load      – In Data Warehouse (PostgreSQL) schreiben

Abhängigkeiten:
  pip install pandas requests sqlalchemy psycopg2-binary python-dotenv
"""

import logging
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# ──────────────────────────────────────────────────────────────
# Konfiguration & Logging
# ──────────────────────────────────────────────────────────────

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("etl_pipeline.log"),
    ],
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
# Datenbankverbindungen
# ──────────────────────────────────────────────────────────────

SOURCE_DB_URL = os.getenv(
    "SOURCE_DB_URL",
    "postgresql://user:password@localhost:5432/source_db",
)
TARGET_DB_URL = os.getenv(
    "TARGET_DB_URL",
    "postgresql://user:password@localhost:5432/warehouse",
)

source_engine = create_engine(SOURCE_DB_URL)
target_engine = create_engine(TARGET_DB_URL)


# ══════════════════════════════════════════════════════════════
# PHASE 1 – EXTRACT
# ══════════════════════════════════════════════════════════════

def extract_from_csv(filepath: str) -> pd.DataFrame:
    """Lädt eine CSV-Datei und gibt einen DataFrame zurück."""
    log.info(f"[EXTRACT] CSV laden: {filepath}")
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"CSV nicht gefunden: {filepath}")

    df = pd.read_csv(filepath)
    log.info(f"[EXTRACT] CSV: {len(df)} Zeilen geladen")
    return df


def extract_from_database(query: str) -> pd.DataFrame:
    """Führt eine SQL-Abfrage auf der Quelldatenbank aus."""
    log.info("[EXTRACT] Datenbankabfrage starten …")
    with source_engine.connect() as conn:
        df = pd.read_sql(text(query), conn)
    log.info(f"[EXTRACT] DB: {len(df)} Zeilen geladen")
    return df


def extract_from_api(url: str, params: dict = None, retries: int = 3) -> pd.DataFrame:
    """
    Holt JSON-Daten von einer REST API mit Retry-Logik.
    Erwartet eine Liste von Objekten oder ein Objekt mit einem 'data'-Key.
    """
    log.info(f"[EXTRACT] API aufrufen: {url}")
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            payload = response.json()

            # Flexibles Mapping: Liste oder verschachteltes Objekt
            records = payload if isinstance(payload, list) else payload.get("data", payload)
            df = pd.json_normalize(records)
            log.info(f"[EXTRACT] API: {len(df)} Datensätze erhalten")
            return df

        except requests.RequestException as exc:
            log.warning(f"[EXTRACT] API-Versuch {attempt}/{retries} fehlgeschlagen: {exc}")
            if attempt < retries:
                time.sleep(2 ** attempt)  # exponentielles Backoff
    raise RuntimeError(f"API nicht erreichbar nach {retries} Versuchen: {url}")


def extract_all() -> dict[str, pd.DataFrame]:
    """
    Orchestriert alle Extraktionsquellen.
    Gibt ein Dict mit DataFrame-Ergebnissen zurück.
    """
    log.info("═══ EXTRACT PHASE START ═══")
    data = {}

    data["csv_orders"] = extract_from_csv("data/orders.csv")

    data["db_customers"] = extract_from_database(
        "SELECT id, name, email, created_at FROM customers WHERE active = TRUE"
    )

    data["api_products"] = extract_from_api(
        url="https://fakestoreapi.com/products",
    )

    log.info("═══ EXTRACT PHASE ABGESCHLOSSEN ═══")
    return data


# ══════════════════════════════════════════════════════════════
# PHASE 2 – TRANSFORM
# ══════════════════════════════════════════════════════════════

def clean_dataframe(df: pd.DataFrame, name: str) -> pd.DataFrame:
    """Entfernt Duplikate und Zeilen mit zu vielen fehlenden Werten."""
    before = len(df)
    df = df.drop_duplicates()
    df = df.dropna(thresh=int(df.shape[1] * 0.6))  # mind. 60 % gefüllt
    log.info(f"[TRANSFORM] {name}: {before} → {len(df)} Zeilen nach Bereinigung")
    return df


def normalize_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Normalisiert den Orders-DataFrame: Typen, Datum, Beträge."""
    df = df.copy()

    # Spaltennamen vereinheitlichen
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # Datumskonvertierung
    if "order_date" in df.columns:
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

    # Numerische Spalten sicherstellen
    for col in ["amount", "quantity", "price"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Kategorisierung des Auftragswertes
    if "amount" in df.columns:
        df["amount_category"] = pd.cut(
            df["amount"],
            bins=[0, 50, 200, 1000, float("inf")],
            labels=["klein", "mittel", "groß", "enterprise"],
        )

    # ETL-Metadaten ergänzen
    df["etl_loaded_at"] = datetime.utcnow()
    log.info(f"[TRANSFORM] orders normalisiert: {len(df)} Zeilen")
    return df


def enrich_orders_with_customers(
    orders: pd.DataFrame,
    customers: pd.DataFrame,
) -> pd.DataFrame:
    """Verknüpft Orders mit Kundendaten (LEFT JOIN)."""
    if "customer_id" not in orders.columns or "id" not in customers.columns:
        log.warning("[TRANSFORM] Fehlende Schlüsselspalten – Anreicherung übersprungen")
        return orders

    enriched = orders.merge(
        customers.rename(columns={"id": "customer_id"}),
        on="customer_id",
        how="left",
        suffixes=("", "_customer"),
    )
    log.info(f"[TRANSFORM] Anreicherung: {len(enriched)} Zeilen nach Join")
    return enriched


def aggregate_daily_revenue(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregiert Tagesumsätze pro Kategorie."""
    if "order_date" not in df.columns or "amount" not in df.columns:
        log.warning("[TRANSFORM] Fehlende Spalten – Aggregation übersprungen")
        return pd.DataFrame()

    daily = (
        df.groupby([df["order_date"].dt.date, "amount_category"], observed=True)
        .agg(
            total_revenue=("amount", "sum"),
            order_count=("amount", "count"),
            avg_order_value=("amount", "mean"),
        )
        .reset_index()
        .rename(columns={"order_date": "date"})
    )
    log.info(f"[TRANSFORM] Tagesaggregation: {len(daily)} Zeilen")
    return daily


def transform_all(raw_data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Orchestriert alle Transformationsschritte."""
    log.info("═══ TRANSFORM PHASE START ═══")

    orders = clean_dataframe(raw_data["csv_orders"], "orders")
    orders = normalize_orders(orders)

    customers = clean_dataframe(raw_data["db_customers"], "customers")

    orders_enriched = enrich_orders_with_customers(orders, customers)
    daily_revenue = aggregate_daily_revenue(orders_enriched)

    result = {
        "dim_customers": customers,
        "fact_orders": orders_enriched,
        "agg_daily_revenue": daily_revenue,
    }

    log.info("═══ TRANSFORM PHASE ABGESCHLOSSEN ═══")
    return result


# ══════════════════════════════════════════════════════════════
# PHASE 3 – LOAD
# ══════════════════════════════════════════════════════════════

def load_to_warehouse(
    df: pd.DataFrame,
    table_name: str,
    if_exists: str = "append",
) -> None:
    """
    Schreibt einen DataFrame ins Data Warehouse.

    Parameters
    ----------
    df         : Zu ladender DataFrame
    table_name : Zieltabellenname
    if_exists  : 'append', 'replace' oder 'fail'
    """
    if df.empty:
        log.warning(f"[LOAD] Tabelle '{table_name}' übersprungen – leerer DataFrame")
        return

    log.info(f"[LOAD] Lade {len(df)} Zeilen → {table_name} …")
    df.to_sql(
        name=table_name,
        con=target_engine,
        if_exists=if_exists,
        index=False,
        chunksize=1000,          # Batch-Größe für große Datensätze
        method="multi",          # Schnellere Mehrfach-Inserts
    )
    log.info(f"[LOAD] '{table_name}' erfolgreich geschrieben")


def load_all(transformed_data: dict[str, pd.DataFrame]) -> None:
    """Schreibt alle transformierten Tabellen ins Warehouse."""
    log.info("═══ LOAD PHASE START ═══")

    table_strategy = {
        "dim_customers": "replace",   # Dimension: immer neu aufbauen
        "fact_orders": "append",      # Faktentabelle: inkrementell
        "agg_daily_revenue": "replace",
    }

    for table_name, df in transformed_data.items():
        strategy = table_strategy.get(table_name, "append")
        load_to_warehouse(df, table_name, if_exists=strategy)

    log.info("═══ LOAD PHASE ABGESCHLOSSEN ═══")


# ══════════════════════════════════════════════════════════════
# PIPELINE ORCHESTRIERUNG
# ══════════════════════════════════════════════════════════════

def run_pipeline() -> None:
    """
    Führt die komplette ETL-Pipeline aus und misst die Laufzeit.
    Bei Fehler wird ein vollständiger Traceback geloggt.
    """
    start_time = time.time()
    log.info("════════════════════════════════════════")
    log.info("  ETL PIPELINE START")
    log.info(f"  Zeitstempel: {datetime.utcnow().isoformat()}")
    log.info("════════════════════════════════════════")

    try:
        raw_data = extract_all()
        transformed_data = transform_all(raw_data)
        load_all(transformed_data)

        duration = time.time() - start_time
        log.info(f"✓ Pipeline erfolgreich abgeschlossen in {duration:.2f}s")

    except Exception as exc:
        log.exception(f"✗ Pipeline fehlgeschlagen: {exc}")
        raise


# ──────────────────────────────────────────────────────────────
# Einstiegspunkt
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    run_pipeline()