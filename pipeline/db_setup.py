"""Stage 6: prepare the RDS PostgreSQL database.

Reads connection settings from .env (see .env.example), then:
0. creates the PGDATABASE database if it does not exist yet (RDS only makes
   one if "Initial database name" was filled in when creating the instance)
1. connects as the admin user and prints the Postgres version
2. enables the PostGIS extension
3. creates (or updates the password of) a read-only login role for the
   dashboard. It may read every table in schema public, including tables
   created later, but cannot change anything.
4. connects as the read-only user to prove it can log in and cannot create tables
5. (Stage 8) creates the sync_log table and a limited login role for the
   scheduled API update (GitHub Actions): it may read neighborhoods and
   read/write the request tables and sync_log, but cannot create, drop or
   alter anything. Grants on the request tables are applied only once they
   exist (after load_db.py), so re-run this script after the first load.

Safe to re-run: every step checks before it acts.

Usage:
    .venv/Scripts/python pipeline/db_setup.py
"""

import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from psycopg import sql

ROOT = Path(__file__).resolve().parent.parent
REQUIRED = ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD", "DASHBOARD_USER", "DASHBOARD_PASSWORD",
            "PIPELINE_USER", "PIPELINE_PASSWORD"]

# Same definition the update script uses (kept here so setup can create it up front).
SYNC_LOG_DDL = """
CREATE TABLE IF NOT EXISTS sync_log (
    run_id        serial PRIMARY KEY,
    started_at    timestamptz NOT NULL,
    finished_at   timestamptz NOT NULL,
    mode          text NOT NULL,
    since         timestamptz,
    fetched       integer NOT NULL,
    inserted      integer NOT NULL,
    updated       integer NOT NULL,
    moved         integer NOT NULL
)"""

# What the scheduled update needs, and nothing more.
PIPELINE_GRANTS = {
    None: ["GRANT CONNECT, TEMPORARY ON DATABASE {db} TO {u}",   # TEMPORARY: its staging table
           "GRANT USAGE ON SCHEMA public TO {u}"],
    "neighborhoods": ["GRANT SELECT ON neighborhoods TO {u}"],
    "requests": ["GRANT SELECT, INSERT, UPDATE ON requests TO {u}"],
    "requests_no_location": ["GRANT SELECT, INSERT, UPDATE, DELETE ON requests_no_location TO {u}"],
    "sync_log": ["GRANT SELECT, INSERT ON sync_log TO {u}",
                 "GRANT USAGE ON SEQUENCE sync_log_run_id_seq TO {u}"],
}


def connect(user: str, password: str, dbname: str | None = None) -> psycopg.Connection:
    return psycopg.connect(
        host=os.environ["PGHOST"], port=os.environ["PGPORT"], dbname=dbname or os.environ["PGDATABASE"],
        user=user, password=password, sslmode="require", connect_timeout=10, autocommit=True,
    )


def ensure_database(db: str) -> None:
    # Connect to the built-in "postgres" database to create ours.
    with connect(os.environ["PGUSER"], os.environ["PGPASSWORD"], dbname="postgres") as con:
        if con.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db,)).fetchone():
            print(f"database    : {db} exists")
        else:
            con.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db)))
            print(f"database    : created {db}")


def main() -> None:
    load_dotenv(ROOT / ".env")
    missing = [k for k in REQUIRED if not os.environ.get(k)]
    if missing:
        raise SystemExit(f"missing in .env: {', '.join(missing)} (see .env.example)")

    db = os.environ["PGDATABASE"]
    ro_user, ro_pw = os.environ["DASHBOARD_USER"], os.environ["DASHBOARD_PASSWORD"]

    ensure_database(db)
    with connect(os.environ["PGUSER"], os.environ["PGPASSWORD"]) as con:
        print("connected   :", con.execute("SELECT version()").fetchone()[0])

        con.execute("CREATE EXTENSION IF NOT EXISTS postgis")
        print("postgis     :", con.execute("SELECT postgis_full_version()").fetchone()[0][:60], "...")

        exists = con.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (ro_user,)).fetchone()
        verb = "ALTER" if exists else "CREATE"
        con.execute(sql.SQL(verb + " ROLE {} LOGIN PASSWORD {}").format(sql.Identifier(ro_user), sql.Literal(ro_pw)))
        for stmt in [
            "GRANT CONNECT ON DATABASE {db} TO {u}",
            "GRANT USAGE ON SCHEMA public TO {u}",
            "GRANT SELECT ON ALL TABLES IN SCHEMA public TO {u}",
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO {u}",
        ]:
            con.execute(sql.SQL(stmt).format(db=sql.Identifier(db), u=sql.Identifier(ro_user)))
        print(f"read-only   : {'updated' if exists else 'created'} role {ro_user}")

    with connect(ro_user, ro_pw) as ro:
        print(f"login check : {ro_user} connected OK")
        try:
            ro.execute("CREATE TABLE _ro_write_test (x int)")
        except psycopg.errors.InsufficientPrivilege:
            print(f"write check : {ro_user} cannot create tables (expected)")
        else:
            ro.execute("DROP TABLE _ro_write_test")
            raise SystemExit(f"{ro_user} was able to create a table; it should be read-only")

    setup_pipeline_role(db)


def setup_pipeline_role(db: str) -> None:
    user, pw = os.environ["PIPELINE_USER"], os.environ["PIPELINE_PASSWORD"]
    with connect(os.environ["PGUSER"], os.environ["PGPASSWORD"]) as con:
        con.execute(SYNC_LOG_DDL)
        exists = con.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (user,)).fetchone()
        verb = "ALTER" if exists else "CREATE"
        con.execute(sql.SQL(verb + " ROLE {} LOGIN PASSWORD {}").format(sql.Identifier(user), sql.Literal(pw)))
        skipped = []
        for table, stmts in PIPELINE_GRANTS.items():
            if table and not con.execute("SELECT to_regclass(%s)", (f"public.{table}",)).fetchone()[0]:
                skipped.append(table)
                continue
            for stmt in stmts:
                con.execute(sql.SQL(stmt).format(db=sql.Identifier(db), u=sql.Identifier(user)))
        print(f"pipeline    : {'updated' if exists else 'created'} role {user}"
              + (f" (tables not loaded yet, re-run after load_db.py: {', '.join(skipped)})" if skipped else ""))

    with connect(user, pw) as w:
        n = w.execute("SELECT count(*) FROM sync_log").fetchone()[0]
        print(f"login check : {user} connected OK, reads sync_log ({n} runs)")
        try:
            w.execute("CREATE TABLE _pw_write_test (x int)")
        except psycopg.errors.InsufficientPrivilege:
            print(f"write check : {user} cannot create tables (expected)")
        else:
            w.execute("DROP TABLE _pw_write_test")
            raise SystemExit(f"{user} was able to create a table; it should only read/write existing tables")


if __name__ == "__main__":
    main()
