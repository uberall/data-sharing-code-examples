# Delta Sharing to PostgreSQL — Ingestion Script

A reference Python script that reads data from a [Databricks Delta Sharing](https://docs.databricks.com/en/delta-sharing/index.html) table and ingests it into a PostgreSQL database.

This is intended for customers who want to consume Delta Sharing data in systems that do not natively support the Delta format.

## Prerequisites

- **Python 3.9+**
- **Docker** (for local testing with the included `compose.yaml`)
- **PostgreSQL** instance with write access (or use the Docker setup below)
- **Delta Sharing profile file** (`.share`) — a JSON credential file provided by your data provider. It looks like this:

```json
{
  "shareCredentialsVersion": 1,
  "endpoint": "https://<databricks-host>/api/2.0/delta-sharing/",
  "bearerToken": "<your-token>"
}
```

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure the script

Open `delta_sharing_to_postgres.py` and edit the **USER CONFIGURATION** section at the top:

| Variable | Description |
|---|---|
| `DELTA_SHARING_PROFILE_PATH` | Path to your `.share` profile file |
| `DELTA_SHARING_TABLE` | Fully qualified table name: `<share>.<schema>.<table>` |
| `PG_HOST` | PostgreSQL host |
| `PG_PORT` | PostgreSQL port (default `5432`) |
| `PG_DATABASE` | Target database name |
| `PG_USER` | Database user |
| `PG_PASSWORD` | Database password |
| `PG_TARGET_SCHEMA` | Target schema (default `public`) |
| `PG_TARGET_TABLE` | Target table name |
| `WRITE_MODE` | `replace`, `append`, or `fail` |

### 3. Run the script

```bash
python delta_sharing_to_postgres.py
```

The script will:
1. Connect to Delta Sharing and list available tables.
2. Read the specified table into memory.
3. Print a preview of the first 5 rows.
4. Write the data to your PostgreSQL instance.
5. Validate the row count in the target table.

## Write Modes

| Mode | Behavior |
|---|---|
| `replace` | Drops and recreates the target table on every run. |
| `append` | Adds rows to the existing table (no deduplication). |
| `fail` | Aborts if the target table already exists. |

## Local Testing with Docker

A `compose.yaml` is included so you can spin up a PostgreSQL instance locally for testing.

### 1. Start the database

```bash
docker compose up -d
```

This starts two services:

| Service | Description | Access from host |
|---|---|---|
| **db** | PostgreSQL 16 | `localhost:5432` |
| **adminer** | Web-based DB admin UI | [http://localhost:8080](http://localhost:8080) |

### 2. Verify the database is ready

```bash
docker compose logs db
```

Look for `database system is ready to accept connections` in the output.

### 3. Test connectivity from the command line

```bash
docker compose exec db psql -U postgres -d testdb -c "SELECT 1;"
```

If this prints a table with `1`, the database is working.

### 4. Connect from DataGrip

Create a new **PostgreSQL** data source in DataGrip with these settings:

| Setting | Value |
|---|---|
| **Host** | `localhost` |
| **Port** | `5432` |
| **Database** | `testdb` |
| **User** | `postgres` |
| **Password** | `example` |

Click **Test Connection** — it should succeed. If it fails:

- Make sure the containers are running: `docker compose ps`
- Make sure nothing else is using port 5432: `ss -tlnp | grep 5432`
- Check the container logs: `docker compose logs db`

### 5. Connect from Adminer (alternative)

Open [http://localhost:8080](http://localhost:8080) in your browser and use:

| Setting | Value |
|---|---|
| **System** | PostgreSQL |
| **Server** | `db` (the Docker service name) |
| **Username** | `postgres` |
| **Password** | `example` |
| **Database** | `testdb` |

### 6. Run the script

The default configuration in `delta_sharing_to_postgres.py` already matches the Docker setup. Just set your `DELTA_SHARING_PROFILE_PATH` and `DELTA_SHARING_TABLE`, then run:

```bash
python delta_sharing_to_postgres.py
```

### 7. Stop and clean up

```bash
# Stop containers (data is preserved in a Docker volume)
docker compose down

# Stop and delete all data
docker compose down -v
```

## Security Considerations

- **Never commit your `.share` profile file to version control.** It contains a bearer token that grants access to shared data. Add `*.share` to your `.gitignore`.
- **Avoid hardcoding passwords** in the script for production use. Consider using environment variables or a secrets manager instead.
- **Restrict file permissions** on the `.share` file (`chmod 600 profile.share` on Linux/macOS).

## Customization

This script is a starting point. Common adaptations include:

- **Column mapping or transformation** — modify the DataFrame between the read and write steps.
- **Incremental ingestion** — filter by a timestamp column to load only new data.
- **Different target databases** — swap the SQLAlchemy connection string and driver (e.g., `mysql+pymysql://` for MySQL).
- **Scheduling** — wrap the script in a cron job, Airflow DAG, or other orchestrator.

## Troubleshooting

| Problem | Solution |
|---|---|
| `FileNotFoundError` on the profile | Verify `DELTA_SHARING_PROFILE_PATH` points to a valid `.share` file. |
| `HTTPError 401/403` | Your bearer token may be expired or invalid. Request a new profile from your data provider. |
| `psycopg2.OperationalError` | Check your PostgreSQL connection parameters and ensure the database is reachable. |
| Empty DataFrame | Verify the table name is correct by checking the logged list of available tables. |
