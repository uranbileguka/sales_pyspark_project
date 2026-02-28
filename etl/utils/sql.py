from pathlib import Path
import logging
from typing import Iterable

import sqlparse
from etl.db import get_conn


# Create a module-level logger
log = logging.getLogger("sql_runner")


def run_sql_file(path: str) -> None:
    """Run all statements in a SQL file inside a single transaction."""
    sql_path = Path(path)
    sql_text = sql_path.read_text(encoding="utf-8")

    log.info(f"Running SQL file: {sql_path}")

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                statements = sqlparse.split(sql_text)

                for i, stmt in enumerate(statements, start=1):
                    stmt = stmt.strip()
                    if not stmt:
                        continue

                    log.info(f"Executing statement #{i} from {sql_path.name}")

                    try:
                        cur.execute(stmt)
                    except Exception as e:
                        # rollback happens automatically due to `with conn:`
                        log.error("=" * 70)
                        log.error(f"❌ SQL FAILED in file: {sql_path}")
                        log.error(f"❌ Statement #{i}:\n{stmt[:1200]}")
                        log.error(f"❌ Error: {e}")
                        log.error("=" * 70)
                        raise
    finally:
        conn.close()


def ensure_tables_exist(tables: Iterable[str]) -> None:
    """Verify the given fully-qualified tables (schema.table) exist in the DB.

    Raises a RuntimeError listing missing tables if any are not present.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        missing = []
        for fq in tables:
            if "." in fq:
                schema, table = fq.split(".", 1)
            else:
                # assume public schema if not provided
                schema, table = 'public', fq

            cur.execute(
                """
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
                LIMIT 1
                """,
                (schema, table),
            )
            if cur.fetchone() is None:
                missing.append(fq)

        cur.close()

        if missing:
            raise RuntimeError(f"Missing tables: {', '.join(missing)}")
    finally:
        conn.close()
