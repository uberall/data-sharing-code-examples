"""
Delta Sharing to PostgreSQL Ingestion Script
=============================================

This script reads data from a Databricks Delta Sharing table and ingests it
into a PostgreSQL database.

Usage:
    1. Fill in the USER CONFIGURATION section below with your own values.
    2. Install dependencies:  pip install -r requirements.txt
    3. Run the script:        python delta_sharing_to_postgres.py

Prerequisites:
    - A Delta Sharing profile file (.share) provided by your data provider.
    - A running PostgreSQL instance you have write access to.
    - Python 3.9+
"""

import sys
import logging

import delta_sharing
import pandas as pd
from sqlalchemy import create_engine, text

# =============================================================================
# USER CONFIGURATION — Edit the values below to match your environment.
# =============================================================================

# Path to your Delta Sharing profile file (.share JSON file).
# This file contains the endpoint URL and bearer token provided by your
# data provider. Keep this file secure and never commit it to version control.
DELTA_SHARING_PROFILE_PATH = "/path/to/your/profile.share"

# The fully qualified name of the shared table you want to read.
# Format: "<share_name>.<schema_name>.<table_name>"
DELTA_SHARING_TABLE = "<share_name>.<schema_name>.<table_name>"

# PostgreSQL connection parameters.
# The defaults below match the included compose.yaml for local testing.
PG_HOST = "localhost"
PG_PORT = 5432
PG_DATABASE = "testdb"
PG_USER = "postgres"
PG_PASSWORD = "example"

# Target table configuration in PostgreSQL.
PG_TARGET_SCHEMA = "public"
PG_TARGET_TABLE = "delta_sharing_data"

# Write mode: how to handle existing data in the target table.
#   "replace" — Drop and recreate the table on every run.
#   "append"  — Add new rows to the existing table (no deduplication).
#   "fail"    — Abort if the table already exists.
WRITE_MODE = "replace"

# =============================================================================
# END OF USER CONFIGURATION — No changes needed below this line.
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def read_delta_sharing_table(profile_path: str, table_name: str) -> pd.DataFrame:
    """Read a shared Delta table and return it as a Pandas DataFrame.

    Args:
        profile_path: Filesystem path to the .share profile file.
        table_name:   Fully qualified table name (<share>.<schema>.<table>).

    Returns:
        A Pandas DataFrame containing the table data.
    """
    logger.info("Loading Delta Sharing profile from: %s", profile_path)
    sharing_client = delta_sharing.SharingClient(profile_path)

    logger.info("Available shares and tables:")
    for table in sharing_client.list_all_tables():
        logger.info("  %s.%s.%s", table.share, table.schema, table.name)

    table_url = f"{profile_path}#{table_name}"
    logger.info("Reading table: %s", table_name)
    df = delta_sharing.load_as_pandas(table_url)
    logger.info("Read %d rows and %d columns.", len(df), len(df.columns))

    return df


def build_pg_connection_string(
    host: str, port: int, database: str, user: str, password: str
) -> str:
    """Build a SQLAlchemy connection string for PostgreSQL."""
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"


def ingest_to_postgres(
    df: pd.DataFrame,
    connection_string: str,
    schema: str,
    table: str,
    write_mode: str,
) -> None:
    """Write a DataFrame to a PostgreSQL table.

    Args:
        df:                The data to write.
        connection_string: SQLAlchemy connection string.
        schema:            Target PostgreSQL schema.
        table:             Target table name.
        write_mode:        One of "replace", "append", or "fail".
    """
    logger.info("Connecting to PostgreSQL...")
    engine = create_engine(connection_string)

    # Verify connectivity before attempting the write.
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    logger.info("PostgreSQL connection successful.")

    logger.info(
        "Writing %d rows to %s.%s (mode=%s)...",
        len(df), schema, table, write_mode,
    )
    df.to_sql(
        name=table,
        con=engine,
        schema=schema,
        if_exists=write_mode,
        index=False,
    )
    logger.info("Write complete.")

    # Quick validation: count rows in the target table.
    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT COUNT(*) FROM {schema}.{table}")
        )
        row_count = result.scalar()
    logger.info("Validation: %s.%s now contains %d rows.", schema, table, row_count)

    engine.dispose()


def main() -> None:
    """Run the full ingestion pipeline: Delta Sharing → PostgreSQL."""
    logger.info("=" * 60)
    logger.info("Delta Sharing to PostgreSQL — Ingestion Script")
    logger.info("=" * 60)

    # --- Step 1: Read from Delta Sharing ---
    try:
        df = read_delta_sharing_table(DELTA_SHARING_PROFILE_PATH, DELTA_SHARING_TABLE)
    except Exception:
        logger.exception("Failed to read from Delta Sharing.")
        sys.exit(1)

    if df.empty:
        logger.warning("The Delta Sharing table returned no data. Nothing to ingest.")
        sys.exit(0)

    logger.info("Preview of the data (first 5 rows):")
    logger.info("\n%s", df.head().to_string())

    # --- Step 2: Ingest into PostgreSQL ---
    connection_string = build_pg_connection_string(
        PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD,
    )

    try:
        ingest_to_postgres(df, connection_string, PG_TARGET_SCHEMA, PG_TARGET_TABLE, WRITE_MODE)
    except Exception:
        logger.exception("Failed to ingest data into PostgreSQL.")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("Ingestion completed successfully.")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
