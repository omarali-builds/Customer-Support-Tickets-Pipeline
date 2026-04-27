# Customer-Support-Tickets-Pipeline

An incremental batch ETL pipeline built with **Apache Airflow** and **PostgreSQL** that processes customer support ticket data in chunks — extracting from CSV, transforming and enriching the data, and loading it into a relational database for downstream analytics.

---

## Architecture

```
CSV File
   │
   ▼
[Extract]  ── reads 100 rows at a time using an offset tracker
   │
   ▼
[Transform] ── cleans, deduplicates, and enriches the chunk
   │
   ▼
[Load] ────── appends to PostgreSQL staging table
   │
   ▼
PostgreSQL (customer_support_tickets)
```

---

## Project Structure

```
.
├── dags/
│   └── customer_tickets_pipeline.py   # Main DAG definition
├── data/
│   ├── customer_support_tickets.csv   # Source data
│   └── offset.json                    # Incremental load tracker
└── README.md
```

---

## Pipeline Stages

### 1. Extract
- Reads the source CSV in chunks of **100 rows** at a time
- Tracks progress using an `offset.json` file so each DAG run picks up where the last one left off
- Writes the raw chunk to a temp file and passes the path downstream via **XCom**
- Stops gracefully when no more rows remain

### 2. Transform
Applies the following cleaning and enrichment(feature engineering) steps:

| Step | Detail |
|---|---|
| Drop index column | Removes `Unnamed: 0` if present |
| Deduplicate | Drops exact duplicate rows |
| Clean `Ticket_Subject` | Keeps only the part before the first `-` |
| Clean `Ticket_Description` | Strips `"Hi Support,"` boilerplate from the start |
| Parse `Submission_Date` | Converts to datetime; invalid values become `NaT` |
| Derive `ticket_age` | `"new"` if submitted July 2025 or later, otherwise `"old"` |
| Derive `performance` | Categorizes `Resolution_Time_Hours` into `excellent / good / needs_improvement` |

### 3. Load
- Validates that all required columns are present before writing
- Appends the cleaned chunk to the `customer_support_tickets` table in PostgreSQL
- Creates the table automatically on first run if it doesn't exist

---


## Database

**Target table:** `customer_support_tickets` in the `airflow` PostgreSQL database.

The table is created automatically on first load. Schema mirrors the transformed DataFrame columns including the derived `ticket_age` and `performance` fields.

---

## Enhancements

- **Star schema DWH layer** — staging table now feeds a proper DWH with `dim_customer`, `dim_agent`, `dim_date`, and `fact_ticket` tables, enabling clean analytical queries separated from raw ingestion
- **Idempotent dimension loads** — all dimension inserts use `ON CONFLICT DO NOTHING`, making reruns safe without producing duplicates
- **Fact deduplication** — `fact_ticket` inserts filter out already-loaded `ticket_id` values, preventing double-counting on retries
- **Database-backed offset tracking** — replace `offset.json` with a state table in PostgreSQL so offset state survives container rebuilds and is consistent across workers
- **Data quality checks** — add row-level validation after transform (e.g. flag rows where `Satisfaction_Score` is out of range or `Submission_Date` is `NaT`) before committing to staging
- **dbt modeling** — replace raw SQL transforms with dbt models for version-controlled, testable, and documented DWH transformations

---

