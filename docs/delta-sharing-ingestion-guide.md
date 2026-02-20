# Delta Sharing — Data Ingestion Guide

## Introduction

[Delta Sharing](https://docs.databricks.com/en/delta-sharing/index.html) is an open protocol developed by Databricks for secure, real-time data sharing across organizations. It allows a data provider to share datasets with recipients without requiring them to use Databricks or any specific platform.

This guide walks you through consuming data shared via Delta Sharing and ingesting it into your own database. We provide a ready-to-use Python reference script that reads a shared Delta table and writes it to a PostgreSQL database.

**Who is this for?**

This guide is for customers who receive data through Delta Sharing and want to load it into a system that does not natively support the Delta format — such as PostgreSQL, MySQL, or any SQL database.

## Architecture Overview

The ingestion flow is straightforward:

```
┌──────────────────────┐       ┌────────────────────┐       ┌──────────────────────┐
│  Databricks          │       │  Python Script      │       │  Your Database       │
│  Delta Sharing       │──────▶│  (this repository)  │──────▶│  (e.g. PostgreSQL)   │
│                      │ HTTPS │                     │  SQL  │                      │
│  Shared tables are   │       │  1. Reads data via  │       │  Data is written to  │
│  accessed via a      │       │     Delta Sharing   │       │  the target table    │
│  .share profile      │       │  2. Loads into a    │       │  using standard SQL  │
│                      │       │     Pandas DataFrame│       │  (INSERT / REPLACE)  │
└──────────────────────┘       └────────────────────┘       └──────────────────────┘
```

- **No Databricks account required** on the recipient side.
- **No special drivers** — the script uses the open-source `delta-sharing` Python library.
- **Data stays in your control** — once ingested, the data lives in your database.

## Prerequisites

Before you begin, make sure you have the following:

| Requirement | Details |
|---|---|
| **Python** | Version 3.9 or higher. Check with `python --version`. |
| **Git** | To clone the repository. Check with `git --version`. |
| **Docker** (optional) | Only needed if you want to spin up a local PostgreSQL for testing. |
| **PostgreSQL** | A running instance you have write access to — either your own or the Docker setup included in the repository. |
| **Delta Sharing profile** | A `.share` file provided by your data provider (Uberall). See Step 1. |

## Step 1 — Obtain Your Delta Sharing Profile

Your data provider (Uberall) will supply you with a **Delta Sharing profile file**. This is a small JSON file with a `.share` extension that contains the connection details and authentication token.

The file looks like this:

```json
{
  "shareCredentialsVersion": 1,
  "endpoint": "https://<databricks-host>/api/2.0/delta-sharing/",
  "bearerToken": "<your-token>"
}
```

Save this file to a secure location on your machine. You will reference its path when configuring the script.

**Important:**

- This file contains a **bearer token** that grants access to the shared data. Treat it like a password.
- **Do not commit it to version control.**
- Restrict file permissions so only your user can read it:

```bash
chmod 600 /path/to/your/profile.share
```

If you have not received your profile file, contact the Uberall Data Engineering team.

## Step 2 — Clone the Repository

Open a terminal and clone the repository:

```bash
git clone https://gitlab.com/momentfeed/uberall/development/ar/data-lake/lakehouse/databricks/delta-sharing-code-examples.git
```

Navigate into the project directory:

```bash
cd delta-sharing-code-examples
```

The repository contains the following files:

| File | Description |
|---|---|
| `delta_sharing_to_postgres.py` | The main ingestion script. |
| `requirements.txt` | Python dependencies. |
| `compose.yaml` | Docker Compose file to spin up a local PostgreSQL for testing. |
| `README.md` | Quick-start guide and reference documentation. |

## Step 3 — Set Up a Python Environment

We recommend using a virtual environment to avoid conflicts with other Python projects.

**Create and activate a virtual environment:**

```bash
# Create the virtual environment
python -m venv venv

# Activate it (Linux / macOS)
source venv/bin/activate

# Activate it (Windows PowerShell)
.\venv\Scripts\Activate.ps1
```

**Install the required dependencies:**

```bash
pip install -r requirements.txt
```

This installs the following packages:

| Package | Purpose |
|---|---|
| `delta-sharing` | Official Databricks library for reading Delta Sharing tables. |
| `pandas` | Data manipulation — the shared data is loaded as a Pandas DataFrame. |
| `sqlalchemy` | Database abstraction layer for writing data to PostgreSQL (or other databases). |
| `psycopg2-binary` | PostgreSQL driver used by SQLAlchemy. |

## Step 4 — Set Up a PostgreSQL Database

You have two options:

### Option A: Use the included Docker setup (recommended for testing)

If you have Docker installed, the repository includes a `compose.yaml` that spins up a PostgreSQL 16 instance with a single command.

**Start the database:**

```bash
docker compose up -d
```

**Verify it is running:**

```bash
docker compose logs db
```

Look for `database system is ready to accept connections` in the output.

**Quick connectivity test:**

```bash
docker compose exec db psql -U postgres -d testdb -c "SELECT 1;"
```

The Docker PostgreSQL instance uses the following defaults:

| Setting | Value |
|---|---|
| Host | `localhost` |
| Port | `5432` |
| Database | `testdb` |
| User | `postgres` |
| Password | `example` |

These match the default configuration in the script, so no additional changes are needed.

**When you are done testing**, stop the database with:

```bash
# Stop containers (data is preserved)
docker compose down

# Stop and delete all data
docker compose down -v
```

### Option B: Use your own PostgreSQL instance

If you already have a PostgreSQL database, simply note your connection details (host, port, database, user, password). You will enter them in Step 5.

## Step 5 — Configure the Script

Open `delta_sharing_to_postgres.py` in any text editor. At the top of the file, you will find the **USER CONFIGURATION** section:

```python
# =============================================================================
# USER CONFIGURATION — Edit the values below to match your environment.
# =============================================================================

DELTA_SHARING_PROFILE_PATH = "/path/to/your/profile.share"
DELTA_SHARING_TABLE = "<share_name>.<schema_name>.<table_name>"

PG_HOST = "localhost"
PG_PORT = 5432
PG_DATABASE = "testdb"
PG_USER = "postgres"
PG_PASSWORD = "example"

PG_TARGET_SCHEMA = "public"
PG_TARGET_TABLE = "delta_sharing_data"

WRITE_MODE = "replace"
```

Edit the following values:

| Variable | What to set | Example |
|---|---|---|
| `DELTA_SHARING_PROFILE_PATH` | Path to the `.share` file you received in Step 1. | `"./config.share"` |
| `DELTA_SHARING_TABLE` | The fully qualified table name. Format: `<share>.<schema>.<table>`. If you are unsure about the name, see the section "How to discover available tables" below. | `"my_share.analytics.sales_data"` |
| `PG_HOST` | Your PostgreSQL host. Use `"localhost"` for Docker. | `"localhost"` |
| `PG_PORT` | Your PostgreSQL port. | `5432` |
| `PG_DATABASE` | Your target database name. | `"testdb"` |
| `PG_USER` | Your database username. | `"postgres"` |
| `PG_PASSWORD` | Your database password. | `"example"` |
| `PG_TARGET_SCHEMA` | The schema where the table will be created. | `"public"` |
| `PG_TARGET_TABLE` | The name of the table to create/write into. | `"delta_sharing_data"` |
| `WRITE_MODE` | How to handle existing data. See "Write Modes" below. | `"replace"` |

### How to Discover Available Tables

If you don't know the exact table name, you can run a quick discovery script. Create a temporary Python file or use a Python shell:

```python
import delta_sharing

client = delta_sharing.SharingClient("/path/to/your/profile.share")

for table in client.list_all_tables():
    print(f"{table.share}.{table.schema}.{table.name}")
```

This prints all tables available to you. Use the full `<share>.<schema>.<table>` string as the value for `DELTA_SHARING_TABLE`.

Alternatively, just run the main script — it logs all available tables at startup before attempting to read data.

### Write Modes

The `WRITE_MODE` setting controls how the script handles existing data in the target table:

| Mode | Behavior | Typical use case |
|---|---|---|
| `replace` | Drops and recreates the target table on every run. All previous data is deleted. | Full refresh — you always want the latest complete snapshot. |
| `append` | Inserts new rows into the existing table. No deduplication is performed. | Accumulating data over time (e.g., daily exports). |
| `fail` | The script aborts if the target table already exists. | Safety net — to avoid accidental overwrites. |

## Step 6 — Run the Script

Make sure your virtual environment is activated (see Step 3), then run:

```bash
python delta_sharing_to_postgres.py
```

### What to Expect

A successful run produces output similar to the following:

```
2026-02-20 17:20:00,000 [INFO] ============================================================
2026-02-20 17:20:00,000 [INFO] Delta Sharing to PostgreSQL — Ingestion Script
2026-02-20 17:20:00,000 [INFO] ============================================================
2026-02-20 17:20:00,001 [INFO] Loading Delta Sharing profile from: ./config.share
2026-02-20 17:20:00,002 [INFO] Available shares and tables:
2026-02-20 17:20:01,500 [INFO]   my_share.analytics.sales_data
2026-02-20 17:20:01,500 [INFO] Reading table: my_share.analytics.sales_data
2026-02-20 17:20:05,000 [INFO] Read 15000 rows and 12 columns.
2026-02-20 17:20:05,001 [INFO] Preview of the data (first 5 rows):
2026-02-20 17:20:05,002 [INFO]
   col_a  col_b  col_c ...
0  ...    ...    ...
1  ...    ...    ...
2026-02-20 17:20:05,100 [INFO] Connecting to PostgreSQL...
2026-02-20 17:20:05,200 [INFO] PostgreSQL connection successful.
2026-02-20 17:20:05,201 [INFO] Writing 15000 rows to public.delta_sharing_data (mode=replace)...
2026-02-20 17:20:08,000 [INFO] Write complete.
2026-02-20 17:20:08,100 [INFO] Validation: public.delta_sharing_data now contains 15000 rows.
2026-02-20 17:20:08,101 [INFO] ============================================================
2026-02-20 17:20:08,101 [INFO] Ingestion completed successfully.
2026-02-20 17:20:08,101 [INFO] ============================================================
```

The script:

1. Connects to Delta Sharing and lists all available tables.
2. Reads the specified table into memory.
3. Shows a preview of the first 5 rows.
4. Writes the data to your PostgreSQL instance.
5. Validates the ingestion by counting rows in the target table.

## Step 7 — Verify the Data

After a successful run, you can verify the data in PostgreSQL.

**Using the command line (Docker setup):**

```bash
docker compose exec db psql -U postgres -d testdb -c "SELECT COUNT(*) FROM public.delta_sharing_data;"
```

**Using a SQL client (DataGrip, DBeaver, pgAdmin, etc.):**

Connect to your PostgreSQL instance with the same credentials from Step 5, then run:

```sql
-- Row count
SELECT COUNT(*) FROM public.delta_sharing_data;

-- Preview rows
SELECT * FROM public.delta_sharing_data LIMIT 10;

-- Check column types
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'delta_sharing_data';
```

## Use Cases

### Full snapshot refresh

Set `WRITE_MODE = "replace"` and run the script on a schedule (e.g., daily via a cron job). Each run replaces the entire table with the latest data from the share.

```
# Example cron entry: run every day at 2:00 AM
0 2 * * * /path/to/venv/bin/python /path/to/delta_sharing_to_postgres.py
```

### Append / accumulate data over time

Set `WRITE_MODE = "append"` to keep adding rows on each run. This is useful when the shared table contains new records (e.g., daily events or logs) and you want to build up a history in your database.

**Note:** This mode does not perform deduplication. If you run the script twice with the same data, you will have duplicate rows. Consider adding deduplication logic in your database or in the script if this is a concern.

### Load into a different database

The script uses SQLAlchemy, which supports many databases. To target a different database, change the connection string in `build_pg_connection_string()` and install the appropriate driver:

| Target database | Connection string format | Driver to install |
|---|---|---|
| PostgreSQL | `postgresql+psycopg2://user:pass@host:port/db` | `psycopg2-binary` (included) |
| MySQL | `mysql+pymysql://user:pass@host:port/db` | `pip install pymysql` |
| Microsoft SQL Server | `mssql+pyodbc://user:pass@host:port/db?driver=ODBC+Driver+18+for+SQL+Server` | `pip install pyodbc` |
| SQLite (local file) | `sqlite:///path/to/database.db` | Built-in (no install needed) |

### Ingest into a data warehouse or cloud database

The same approach works with cloud-hosted databases (Amazon RDS, Google Cloud SQL, Azure Database for PostgreSQL, etc.). Simply update the connection parameters to point to your cloud instance.

## Customization Guide

The script is intentionally simple and meant to be adapted to your needs. Below are common modifications.

### Filter or transform data before loading

After reading the data, you can manipulate the Pandas DataFrame before writing it to the database. For example, to keep only specific columns:

```python
df = read_delta_sharing_table(DELTA_SHARING_PROFILE_PATH, DELTA_SHARING_TABLE)

# Keep only the columns you need
df = df[["column_a", "column_b", "column_c"]]

# Rename columns to match your schema
df = df.rename(columns={"column_a": "id", "column_b": "name"})

# Filter rows
df = df[df["status"] == "active"]

ingest_to_postgres(df, connection_string, PG_TARGET_SCHEMA, PG_TARGET_TABLE, WRITE_MODE)
```

### Incremental ingestion (load only new data)

If the shared table has a timestamp or date column, you can filter to load only new records:

```python
df = read_delta_sharing_table(DELTA_SHARING_PROFILE_PATH, DELTA_SHARING_TABLE)

# Only load data from the last 7 days
from datetime import datetime, timedelta
cutoff = datetime.now() - timedelta(days=7)
df = df[df["updated_at"] >= cutoff]

ingest_to_postgres(df, connection_string, PG_TARGET_SCHEMA, PG_TARGET_TABLE, "append")
```

### Load multiple tables

Duplicate the configuration block or loop over a list of tables:

```python
TABLES_TO_INGEST = [
    {"delta_table": "my_share.schema.table_a", "pg_table": "table_a"},
    {"delta_table": "my_share.schema.table_b", "pg_table": "table_b"},
]

for entry in TABLES_TO_INGEST:
    df = read_delta_sharing_table(DELTA_SHARING_PROFILE_PATH, entry["delta_table"])
    ingest_to_postgres(df, connection_string, PG_TARGET_SCHEMA, entry["pg_table"], WRITE_MODE)
```

### Use environment variables instead of hardcoded values

For production use, avoid hardcoding secrets in the script. Use environment variables instead:

```python
import os

DELTA_SHARING_PROFILE_PATH = os.environ["DELTA_SHARING_PROFILE_PATH"]
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PASSWORD = os.environ["PG_PASSWORD"]
```

Then set them before running:

```bash
export DELTA_SHARING_PROFILE_PATH="./config.share"
export PG_PASSWORD="your_password"
python delta_sharing_to_postgres.py
```

## Security Best Practices

| Topic | Recommendation |
|---|---|
| **Profile file (`.share`)** | Store securely. Never commit to version control. Restrict file permissions (`chmod 600`). |
| **Bearer token expiry** | Tokens may have an expiration date. If you get `401 Unauthorized` errors, request a new profile from your data provider. |
| **Database passwords** | Do not hardcode in the script for production. Use environment variables or a secrets manager. |
| **Network access** | Ensure your machine can reach both the Databricks endpoint (HTTPS) and your PostgreSQL instance. |
| **Data at rest** | Once ingested, the data is under your control. Apply your organization's data governance policies to the target database. |

## Troubleshooting

| Error | Cause | Solution |
|---|---|---|
| `FileNotFoundError` on the profile | The `.share` file path is wrong or the file doesn't exist. | Double-check `DELTA_SHARING_PROFILE_PATH`. Use an absolute path if unsure. |
| `HTTPError 401 Unauthorized` | The bearer token is invalid or expired. | Contact your data provider (Uberall) for a new profile file. |
| `HTTPError 403 Forbidden` | You don't have permission to access this share or table. | Verify the table name. Contact your data provider to check recipient permissions. |
| `HTTPError 404 Not Found` / `SHARE_DOES_NOT_EXIST` | The share name in `DELTA_SHARING_TABLE` is incorrect. | Run the table discovery step (see Step 5) to list available shares and use the exact name printed. |
| `DS_MATERIALIZATION_QUERY_FAILED` | The data provider has not configured materialization for the share. This is a server-side issue. | Contact your data provider (Uberall) and share this error message. This cannot be resolved on the recipient side. |
| `psycopg2.OperationalError: could not connect` | PostgreSQL is unreachable. | Verify host, port, and credentials. If using Docker, ensure containers are running (`docker compose ps`). Check that port 5432 is not blocked by a firewall. |
| `psycopg2.OperationalError: password authentication failed` | Wrong username or password. | Verify `PG_USER` and `PG_PASSWORD`. For the Docker setup, defaults are `postgres` / `example`. |
| Empty DataFrame (0 rows) | The shared table exists but contains no data. | Check with your data provider whether the table is expected to have data. |
| `ModuleNotFoundError: No module named 'delta_sharing'` | Dependencies are not installed or the virtual environment is not activated. | Run `pip install -r requirements.txt` and ensure your virtual environment is active. |

## Support

If you encounter issues related to:

- **The shared data, access tokens, or permissions** — contact the Uberall Data Engineering team.
- **The ingestion script** — refer to this guide and the `README.md` in the repository. The script is provided as a reference implementation; you are responsible for adapting and maintaining it for your environment.
