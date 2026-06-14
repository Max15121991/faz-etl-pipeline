"""
Unit Tests für die FAZ ETL Pipeline
Ausführen: pytest tests/ -v
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from datetime import datetime

import sys
sys.path.insert(0, ".")
from etl.etl_pipeline import (
    clean_dataframe,
    normalize_orders,
    enrich_orders_with_customers,
    aggregate_daily_revenue,
)


# ──────────────────────────────────────────────
# Fixtures – Testdaten
# ──────────────────────────────────────────────

@pytest.fixture
def sample_orders():
    return pd.DataFrame({
        "order_id":    ["O001", "O002", "O003", "O001"],   # O001 doppelt
        "customer_id": ["C1", "C2", "C3", "C1"],
        "amount":      [49.99, 199.0, 1500.0, 49.99],
        "quantity":    [1, 2, 5, 1],
        "order_date":  ["2024-01-15", "2024-01-15", "2024-01-15", "2024-01-15"],
        "channel":     ["Web", "App", "Web", "Web"],
    })


@pytest.fixture
def sample_customers():
    return pd.DataFrame({
        "id":       ["C1", "C2", "C3"],
        "name":     ["Anna Müller", "Ben Schulz", "Clara Weber"],
        "email":    ["a@faz.net", "b@faz.net", "c@faz.net"],
        "segment":  ["premium", "standard", "premium"],
    })


@pytest.fixture
def orders_with_missing():
    return pd.DataFrame({
        "order_id":    ["O001", "O002", None, "O004"],
        "customer_id": ["C1", None, None, "C4"],
        "amount":      [100.0, None, None, 50.0],
        "order_date":  ["2024-01-15"] * 4,
        "channel":     ["Web"] * 4,
    })


# ──────────────────────────────────────────────
# Tests: clean_dataframe
# ──────────────────────────────────────────────

class TestCleanDataframe:
    def test_removes_duplicates(self, sample_orders):
        result = clean_dataframe(sample_orders, "orders")
        assert len(result) == 3, "Duplikat O001 muss entfernt werden"

    def test_removes_mostly_null_rows(self, orders_with_missing):
        result = clean_dataframe(orders_with_missing, "orders")
        # Zeile mit 3/5 NULLs (60% fehlt) soll raus
        assert len(result) < len(orders_with_missing)

    def test_preserves_valid_rows(self, sample_orders):
        result = clean_dataframe(sample_orders, "orders")
        assert "O002" in result["order_id"].values
        assert "O003" in result["order_id"].values


# ──────────────────────────────────────────────
# Tests: normalize_orders
# ──────────────────────────────────────────────

class TestNormalizeOrders:
    def test_order_date_is_datetime(self, sample_orders):
        result = normalize_orders(sample_orders)
        assert pd.api.types.is_datetime64_any_dtype(result["order_date"])

    def test_amount_is_numeric(self, sample_orders):
        result = normalize_orders(sample_orders)
        assert pd.api.types.is_float_dtype(result["amount"])

    def test_amount_category_klein(self, sample_orders):
        result = normalize_orders(sample_orders)
        row = result[result["order_id"] == "O001"]
        assert row["amount_category"].values[0] == "klein"

    def test_amount_category_enterprise(self, sample_orders):
        result = normalize_orders(sample_orders)
        row = result[result["order_id"] == "O003"]
        assert row["amount_category"].values[0] == "enterprise"

    def test_etl_metadata_added(self, sample_orders):
        result = normalize_orders(sample_orders)
        assert "etl_loaded_at" in result.columns

    def test_column_names_lowercase(self, sample_orders):
        df = sample_orders.copy()
        df.columns = ["Order_ID", "Customer_ID", "Amount", "Quantity", "Order_Date", "Channel"]
        result = normalize_orders(df)
        assert all(c == c.lower() for c in result.columns if c != "etl_loaded_at")


# ──────────────────────────────────────────────
# Tests: enrich_orders_with_customers
# ──────────────────────────────────────────────

class TestEnrichOrders:
    def test_customer_segment_joined(self, sample_orders, sample_customers):
        orders = normalize_orders(sample_orders.drop_duplicates())
        result = enrich_orders_with_customers(orders, sample_customers)
        c1_row = result[result["customer_id"] == "C1"]
        assert c1_row["segment"].values[0] == "premium"

    def test_left_join_keeps_all_orders(self, sample_orders, sample_customers):
        orders = normalize_orders(sample_orders.drop_duplicates())
        # Kunde C4 existiert nicht in customers
        orders_extra = pd.concat([
            orders,
            pd.DataFrame([{
                "order_id": "O999", "customer_id": "C99",
                "amount": 10.0, "quantity": 1,
                "order_date": pd.Timestamp("2024-01-15"),
                "channel": "web", "amount_category": "klein",
                "etl_loaded_at": datetime.utcnow()
            }])
        ])
        result = enrich_orders_with_customers(orders_extra, sample_customers)
        assert len(result) == len(orders_extra), "LEFT JOIN darf keine Zeilen verlieren"

    def test_skips_when_key_missing(self, sample_orders, sample_customers):
        orders_no_key = sample_orders.drop(columns=["customer_id"])
        result = enrich_orders_with_customers(orders_no_key, sample_customers)
        # Soll original-DF zurückgeben, kein Crash
        assert result is not None


# ──────────────────────────────────────────────
# Tests: aggregate_daily_revenue
# ──────────────────────────────────────────────

class TestAggregateDailyRevenue:
    def test_revenue_sum_correct(self, sample_orders, sample_customers):
        orders = normalize_orders(clean_dataframe(sample_orders, "orders"))
        enriched = enrich_orders_with_customers(orders, sample_customers)
        result = aggregate_daily_revenue(enriched)
        assert result["total_revenue"].sum() == pytest.approx(
            49.99 + 199.0 + 1500.0, rel=1e-3
        )

    def test_output_has_required_columns(self, sample_orders, sample_customers):
        orders = normalize_orders(clean_dataframe(sample_orders, "orders"))
        enriched = enrich_orders_with_customers(orders, sample_customers)
        result = aggregate_daily_revenue(enriched)
        for col in ["total_revenue", "order_count", "avg_order_value"]:
            assert col in result.columns, f"Spalte {col} fehlt"

    def test_returns_empty_on_missing_columns(self, sample_orders):
        # Wenn order_date oder amount fehlt → leerer DataFrame, kein Crash
        bad_df = sample_orders.drop(columns=["order_date"])
        result = aggregate_daily_revenue(bad_df)
        assert isinstance(result, pd.DataFrame)