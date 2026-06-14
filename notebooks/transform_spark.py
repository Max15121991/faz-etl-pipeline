# Databricks notebook source
# MAGIC %md
# MAGIC # FAZ ETL – Transform Phase (PySpark)
# MAGIC
# MAGIC Dieses Notebook wird vom Airflow DAG via `DatabricksRunNowOperator` gestartet.
# MAGIC Parameter kommen als `notebook_params` rein: `run_date`, `adls_container`, `input_path`, `output_path`

# COMMAND ----------
# MAGIC %md ## 0. Parameter & Spark-Session

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType
)
from pyspark.sql.window import Window
from datetime import datetime
import logging

log = logging.getLogger(__name__)

# Databricks: SparkSession ist bereits vorhanden
spark = SparkSession.builder.getOrCreate()

# Airflow übergibt diese Parameter via DatabricksRunNowOperator
dbutils.widgets.text("run_date",      "2024-01-15")
dbutils.widgets.text("adls_container","raw-data")
dbutils.widgets.text("input_path",    "staging/2024-01-15")
dbutils.widgets.text("output_path",   "processed/2024-01-15")

RUN_DATE      = dbutils.widgets.get("run_date")
CONTAINER     = dbutils.widgets.get("adls_container")
INPUT_PATH    = dbutils.widgets.get("input_path")
OUTPUT_PATH   = dbutils.widgets.get("output_path")

# Azure Data Lake Storage Gen2 Pfade
ADLS_BASE     = f"abfss://{CONTAINER}@fazadls.dfs.core.windows.net"
INPUT_BASE    = f"{ADLS_BASE}/{INPUT_PATH}"
OUTPUT_BASE   = f"{ADLS_BASE}/{OUTPUT_PATH}"

print(f"Run date:    {RUN_DATE}")
print(f"Input path:  {INPUT_BASE}")
print(f"Output path: {OUTPUT_BASE}")


# COMMAND ----------
# MAGIC %md ## 1. Schemas definieren

# COMMAND ----------

orders_schema = StructType([
    StructField("order_id",     StringType(),    False),
    StructField("customer_id",  StringType(),    False),
    StructField("product_id",   StringType(),    True),
    StructField("amount",       DoubleType(),    True),
    StructField("quantity",     IntegerType(),   True),
    StructField("order_date",   TimestampType(), True),
    StructField("status",       StringType(),    True),
    StructField("channel",      StringType(),    True),
])

customers_schema = StructType([
    StructField("id",           StringType(),    False),
    StructField("name",         StringType(),    True),
    StructField("email",        StringType(),    True),
    StructField("segment",      StringType(),    True),
    StructField("created_at",   TimestampType(), True),
])

articles_schema = StructType([
    StructField("article_id",   StringType(),    False),
    StructField("title",        StringType(),    True),
    StructField("category",     StringType(),    True),
    StructField("published_at", TimestampType(), True),
    StructField("views",        IntegerType(),   True),
    StructField("author_id",    StringType(),    True),
])


# COMMAND ----------
# MAGIC %md ## 2. Extract – Rohdaten aus ADLS lesen

# COMMAND ----------

def read_parquet(path: str, schema: StructType, name: str) -> DataFrame:
    """Liest Parquet-Dateien aus ADLS mit explizitem Schema."""
    print(f"[READ] {name} ← {path}")
    df = (
        spark.read
        .schema(schema)
        .parquet(path)
    )
    print(f"[READ] {name}: {df.count()} Zeilen, {len(df.columns)} Spalten")
    return df

df_orders    = read_parquet(f"{INPUT_BASE}/orders.parquet",    orders_schema,    "orders")
df_customers = read_parquet(f"{INPUT_BASE}/customers.parquet", customers_schema, "customers")
df_articles  = read_parquet(f"{INPUT_BASE}/articles.parquet",  articles_schema,  "articles")


# COMMAND ----------
# MAGIC %md ## 3. Datenqualität prüfen

# COMMAND ----------

def quality_check(df: DataFrame, name: str, not_null_cols: list[str]) -> DataFrame:
    """
    Prüft Datenqualität:
      - Duplikate entfernen
      - Pflichtfelder auf NULL prüfen und ungültige Zeilen herausfiltern
      - Zeilenzahl vor/nach loggen
    """
    total_before = df.count()

    # Duplikate entfernen
    df = df.dropDuplicates()

    # Zeilen mit NULL in Pflichtfeldern entfernen
    for col in not_null_cols:
        null_count = df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            print(f"[QC] {name}.{col}: {null_count} NULL-Zeilen werden entfernt")
        df = df.filter(F.col(col).isNotNull())

    total_after = df.count()
    dropped = total_before - total_after
    print(f"[QC] {name}: {total_before} → {total_after} Zeilen ({dropped} entfernt)")
    return df

df_orders    = quality_check(df_orders,    "orders",    ["order_id", "customer_id", "amount"])
df_customers = quality_check(df_customers, "customers", ["id", "email"])
df_articles  = quality_check(df_articles,  "articles",  ["article_id"])


# COMMAND ----------
# MAGIC %md ## 4. Transform – Orders bereinigen & anreichern

# COMMAND ----------

def transform_orders(orders: DataFrame, customers: DataFrame) -> DataFrame:
    """
    Bereinigt Orders und reichert mit Kundendaten an:
      - Negative Beträge filtern
      - Betragskategorie berechnen
      - Kanal normalisieren
      - Mit Kundensegment joinen
      - ETL-Metadaten ergänzen
    """

    # Negative / null Beträge entfernen
    orders = orders.filter(F.col("amount") > 0)

    # Spaltennamen normalisieren
    orders = orders.withColumn(
        "channel",
        F.lower(F.trim(F.col("channel")))
    )

    # Betragskategorie
    orders = orders.withColumn(
        "amount_category",
        F.when(F.col("amount") < 50,   F.lit("klein"))
         .when(F.col("amount") < 200,  F.lit("mittel"))
         .when(F.col("amount") < 1000, F.lit("groß"))
         .otherwise(F.lit("enterprise"))
    )

    # Datumsspalten extrahieren
    orders = (
        orders
        .withColumn("order_year",  F.year("order_date"))
        .withColumn("order_month", F.month("order_date"))
        .withColumn("order_day",   F.dayofmonth("order_date"))
        .withColumn("order_week",  F.weekofyear("order_date"))
    )

    # Kunden-JOIN (LEFT): Segment für Analyse mitbringen
    customers_slim = customers.select(
        F.col("id").alias("customer_id"),
        F.col("segment").alias("customer_segment"),
        F.col("name").alias("customer_name"),
    )
    enriched = orders.join(customers_slim, on="customer_id", how="left")

    # ETL-Metadaten
    enriched = enriched.withColumn("etl_run_date", F.lit(RUN_DATE))
    enriched = enriched.withColumn("etl_loaded_at", F.current_timestamp())

    print(f"[TRANSFORM] fact_orders: {enriched.count()} Zeilen nach Anreicherung")
    return enriched


df_fact_orders = transform_orders(df_orders, df_customers)
df_fact_orders.printSchema()


# COMMAND ----------
# MAGIC %md ## 5. Transform – Kunden-Dimension

# COMMAND ----------

def transform_customers(customers: DataFrame) -> DataFrame:
    """
    Baut die Kundendimension:
      - E-Mail-Domain extrahieren
      - Kundensenioritätsstufe berechnen (Tage seit Registrierung)
      - SCD Type 1 (immer aktuelle Werte, kein History-Tracking hier)
    """
    dim = (
        customers
        .withColumn(
            "email_domain",
            F.regexp_extract(F.col("email"), r"@(.+)$", 1)
        )
        .withColumn(
            "days_since_registration",
            F.datediff(F.current_date(), F.col("created_at").cast("date"))
        )
        .withColumn(
            "customer_tier",
            F.when(F.col("days_since_registration") < 90,  F.lit("neu"))
             .when(F.col("days_since_registration") < 365, F.lit("aktiv"))
             .otherwise(F.lit("bestandskunde"))
        )
        .withColumn("etl_run_date",   F.lit(RUN_DATE))
        .withColumn("etl_loaded_at",  F.current_timestamp())
    )
    print(f"[TRANSFORM] dim_customers: {dim.count()} Zeilen")
    return dim

df_dim_customers = transform_customers(df_customers)


# COMMAND ----------
# MAGIC %md ## 6. Aggregation – Tagesumsätze

# COMMAND ----------

def aggregate_daily_revenue(orders: DataFrame) -> DataFrame:
    """
    Aggregiert Tagesumsätze je Kanal und Kundensegment.
    Wird in Power BI als Basis für Revenue-Dashboards genutzt.
    """
    agg = (
        orders
        .groupBy(
            "order_year", "order_month", "order_day", "order_week",
            "channel", "customer_segment", "amount_category"
        )
        .agg(
            F.sum("amount").alias("total_revenue"),
            F.count("order_id").alias("order_count"),
            F.avg("amount").alias("avg_order_value"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum("quantity").alias("total_units_sold"),
        )
        .withColumn("etl_run_date",  F.lit(RUN_DATE))
        .withColumn("etl_loaded_at", F.current_timestamp())
    )
    print(f"[TRANSFORM] agg_daily_revenue: {agg.count()} Zeilen")
    return agg

df_agg_revenue = aggregate_daily_revenue(df_fact_orders)
df_agg_revenue.show(5)


# COMMAND ----------
# MAGIC %md ## 7. Aggregation – Artikel-Performance

# COMMAND ----------

def aggregate_article_performance(articles: DataFrame) -> DataFrame:
    """
    7-Tage-Rollup für Artikel-Views – Basis für Editorial-Dashboards.
    Nutzt Window-Funktion für kumulierte Views.
    """
    window_7d = (
        Window
        .partitionBy("category")
        .orderBy("published_at")
        .rowsBetween(-6, 0)   # gleitendes 7-Tage-Fenster
    )

    perf = (
        articles
        .withColumn("views_7d_rolling", F.sum("views").over(window_7d))
        .withColumn("rank_in_category",
            F.dense_rank().over(
                Window.partitionBy("category", F.col("published_at").cast("date"))
                      .orderBy(F.desc("views"))
            )
        )
        .withColumn("etl_run_date",  F.lit(RUN_DATE))
        .withColumn("etl_loaded_at", F.current_timestamp())
    )
    print(f"[TRANSFORM] agg_article_performance: {perf.count()} Zeilen")
    return perf

df_agg_articles = aggregate_article_performance(df_articles)


# COMMAND ----------
# MAGIC %md ## 8. Load – Ergebnisse in ADLS schreiben (für Airflow Load-Phase)

# COMMAND ----------

def write_parquet(df: DataFrame, name: str, partition_cols: list[str] = None) -> None:
    """
    Schreibt DataFrame als Parquet in ADLS.
    Partitionierung beschleunigt spätere Abfragen im DWH.
    """
    path = f"{OUTPUT_BASE}/{name}.parquet"
    writer = df.write.mode("overwrite").format("parquet")

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.save(path)
    print(f"[WRITE] {name} → {path} ✓")

write_parquet(df_fact_orders,    "fact_orders",            partition_cols=["order_year", "order_month"])
write_parquet(df_dim_customers,  "dim_customers")
write_parquet(df_agg_revenue,    "agg_daily_revenue",      partition_cols=["order_year", "order_month"])
write_parquet(df_agg_articles,   "agg_article_performance",partition_cols=["order_year", "order_month"])

print(f"\nTransform abgeschlossen: {RUN_DATE}")


# COMMAND ----------
# MAGIC %md ## 9. Qualitäts-Summary

# COMMAND ----------

summary = spark.createDataFrame([
    ("fact_orders",             df_fact_orders.count()),
    ("dim_customers",           df_dim_customers.count()),
    ("agg_daily_revenue",       df_agg_revenue.count()),
    ("agg_article_performance", df_agg_articles.count()),
], ["tabelle", "zeilen"])

summary.show()
dbutils.notebook.exit("SUCCESS")