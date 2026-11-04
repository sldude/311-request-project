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
REQUIRED = ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD", "DASHBOARD_USER", "DASHBOARD_PASSWORD"]


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


if __name__ == "__main__":
    main()
