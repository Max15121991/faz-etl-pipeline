# FAZ ETL Pipeline

Produktionsnahe Datenpipeline auf dem Azure-Stack der Frankfurter Allgemeinen Zeitung –
gebaut als Portfolioprojekt für die Junior-Data-Engineer-Stelle.

![CI/CD](https://github.com/DEIN-NAME/faz-etl-pipeline/actions/workflows/ci-cd.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.11-blue)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9-017CEE)
![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C)
![Azure](https://img.shields.io/badge/Azure-Data%20Lake%20%7C%20SQL%20%7C%20Databricks-0078D4)

---

## Überblick

```
Datenquellen          Orchestrierung      Transform            Ziel
─────────────         ──────────────      ─────────────        ──────────────
ADLS Gen2       ─┐
Azure SQL DB    ─┼──► Airflow DAG  ──►  Databricks /   ──►  Azure SQL DWH
REST APIs       ─┘   (täglich 3 Uhr)    PySpark             Power BI
```

Die Pipeline verarbeitet täglich Bestell-, Kunden- und Artikeldaten der FAZ,
reichert sie an und stellt aggregierte Tabellen für Machine-Learning-Modelle
und Dashboards bereit.

---

## Stack

| Komponente        | Technologie                              |
|-------------------|------------------------------------------|
| Orchestrierung    | Apache Airflow 2.9 (Docker)              |
| Transform         | PySpark auf Databricks                   |
| Speicher          | Azure Data Lake Storage Gen2 (Parquet)   |
| Datenbank (Quelle)| Azure SQL Database                       |
| Data Warehouse    | Azure SQL Database (DWH-Schema)          |
| Visualisierung    | Power BI                                 |
| CI/CD             | GitHub Actions + Azure DevOps            |
| Tests             | pytest + ruff                            |

---

## Projektstruktur

```
faz-etl-pipeline/
├── dags/
│   └── faz_etl_dag.py          # Airflow DAG – Orchestrierung aller Phasen
├── etl/
│   └── etl_pipeline.py         # Extract & Load – Python / SQLAlchemy
├── notebooks/
│   └── transform_spark.py      # Transform – PySpark auf Databricks
├── tests/
│   └── test_etl_pipeline.py    # pytest Unit Tests
├── .github/
│   └── workflows/
│       └── ci-cd.yml           # GitHub Actions: Test → Validate → Deploy
├── docker-compose.yml          # Lokales Airflow via Docker
├── requirements.txt
└── GIT_WORKFLOW.md             # Branch-Modell & Commit-Konventionen
```

---

## Pipeline im Detail

### Phase 1 – Extract

Drei parallele Airflow-Tasks laufen unabhängig voneinander:

- **ADLS Gen2** – Tages-Parquet-Dateien (Orders, Artikel)
- **Azure SQL DB** – Aktive Kundendaten per inkrementeller Abfrage
- **REST API** – Externe Artikel-Metadaten mit Retry & exponentiellem Backoff

Ergebnisse landen als Parquet in der Staging-Zone des Data Lake.

### Phase 2 – Transform (PySpark / Databricks)

Das Databricks-Notebook wird vom `DatabricksRunNowOperator` gestartet
und führt folgende Schritte aus:

| Schritt | Inhalt |
|---------|--------|
| Schema-Enforcement | Explizite StructType-Definitionen, keine Schema-Inferenz |
| Datenqualität | Duplikat-Entfernung, NULL-Prüfung auf Pflichtfeldern |
| Normalisierung | Datumsextraktion, Kanal-Vereinheitlichung, Betragskategorie |
| Anreicherung | LEFT JOIN Orders ↔ Kunden (Segment, Name) |
| Aggregation | Tagesumsätze je Kanal & Segment, 7-Tage-Window auf Artikel-Views |
| Output | Partitionierte Parquet-Dateien in ADLS `processed/`-Zone |

### Phase 3 – Load

Airflow liest die Databricks-Outputs aus ADLS und schreibt sie per
SQLAlchemy in Azure SQL DWH:

| Tabelle | Strategie |
|---------|-----------|
| `fact_orders` | `append` – inkrementell je Lauf |
| `dim_customers` | `replace` – täglich neu aufgebaut |
| `agg_daily_revenue` | `replace` – Tagesaggregat wird neu berechnet |
| `agg_article_performance` | `replace` – Rollup-Werte werden ersetzt |

---

## Lokale Entwicklung

### Voraussetzungen

- Python 3.11+
- Docker Desktop
- Git

### Setup

```bash
git clone https://github.com/DEIN-NAME/faz-etl-pipeline.git
cd faz-etl-pipeline

python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### Airflow lokal starten

```bash
docker compose up -d
# Airflow UI: http://localhost:8080  (admin / admin)
```

Neue DAG-Dateien im `dags/`-Ordner werden automatisch erkannt.

### Pipeline ohne Airflow testen

```bash
python etl/etl_pipeline.py
```

### Tests ausführen

```bash
pytest tests/ -v --cov=etl
```

---

## CI/CD

Jeder Push löst automatisch drei Jobs aus:

```
push / pull_request
       │
       ▼
  [test]  ──── ruff Linting + pytest + Coverage-Upload
       │
       ▼
  [validate-dag]  ──── Airflow importiert DAG, prüft auf Fehler
       │
       ▼  (nur main-Branch)
  [deploy]  ──── Notebook → Databricks Workspace
             └── DAG → Airflow-Server (SSH)
```

GitHub Secrets die gesetzt werden müssen:

| Secret | Beschreibung |
|--------|-------------|
| `DATABRICKS_HOST` | Workspace-URL |
| `DATABRICKS_TOKEN` | Personal Access Token |
| `AIRFLOW_HOST` | Server-IP / Hostname |
| `AIRFLOW_SSH_KEY` | Privater SSH-Deploy-Key |

---

## Design-Entscheidungen

**Warum Airflow TaskGroups statt einzelner Tasks?**
TaskGroups gruppieren Extract/Transform/Load visuell im Graph und erlauben
`trigger_rule`-Konfiguration pro Gruppe – z. B. Load nur wenn Transform
vollständig erfolgreich war.

**Warum PySpark statt Pandas für Transform?**
Bestelldaten der FAZ wachsen täglich. PySpark skaliert horizontal auf
Databricks-Clustern, während Pandas in RAM limitiert ist. Window-Funktionen
für rollende Artikel-Aggregationen sind in Spark nativer als in Pandas.

**Warum Parquet als Zwischenformat?**
Parquet ist spaltenorientiert, komprimiert gut und wird von Databricks,
Azure Synapse und Power BI direkt gelesen – kein Konversionsverlust.

**Warum `replace` für Dimensionen, `append` für Fakten?**
Dimensionstabellen (Kunden) sind klein und ändern sich täglich – ein
vollständiger Rebuild ist sicherer als Delta-Updates (SCD Type 1).
Faktentabellen wachsen unbegrenzt und werden nur erweitert.

---

## Lizenz

MIT – freie Nutzung zu Lern- und Portfoliozwecken.