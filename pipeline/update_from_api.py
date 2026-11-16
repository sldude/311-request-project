"""Stage 8: pull new and changed 311 requests from the ArcGIS API into RDS.

Each run
  1. Fetch from the API (2,000 rows per page, paged by RowID):
       default : records created/updated since the last successful run - 3 days
                 (the overlap also covers the 2026 local-time bug, ~5 h)
       --full  : every record in the API (all of 2026, ~1.04M rows; weekly)
       --since : records since a given date (manual catch-up / testing)
  2. Clean them with the SAME SQL as the CSV pipeline:
       standardize.build_sql    (types, UTC dates, 2026 ts_suspect rule, ...)
       fill_locations.build_sql (placeholder coords -> NULL, street_key,
                                 address matching against known locations
                                 fetched from the database)
  3. In one Postgres transaction:
       - assign nsa_id by point-in-polygon against neighborhoods
       - upsert located rows into requests; remove them from
         requests_no_location (a request that gained a location moves over)
       - rows without a location: update in requests if already there (its
         earlier location is kept), else upsert into requests_no_location
       - record the run in sync_log
     Any error rolls everything back. --dry-run does all of it, prints what
     would change, then rolls back.

Prints counts only (GitHub Actions logs are public): no hosts, secrets or rows.

Usage:
    .venv/Scripts/python pipeline/update_from_api.py              # daily
    .venv/Scripts/python pipeline/update_from_api.py --full       # weekly
    .venv/Scripts/python pipeline/update_from_api.py --since 2026-11-01 --dry-run
"""

import argparse
import datetime as dt
import os
import sys
import time
from pathlib import Path

import duckdb
import pandas as pd
import psycopg
import requests
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))
import fill_locations  # noqa: E402
import standardize  # noqa: E402
from db_setup import SYNC_LOG_DDL  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
API_URL = ("https://services1.arcgis.com/UWYHeuuJISiGmgXx/arcgis/rest/services/"
           "311_Customer_Service_Requests_current/FeatureServer/0/query")
PAGE_SIZE = 2000
OVERLAP = dt.timedelta(days=3)
SOURCE_FILE = "api"

API_DATE_FIELDS = list(standardize.DATE_COLS)               # CreatedDate, StatusDate, ...
API_TEXT_FIELDS = list(standardize.TEXT_COLS) + [
    "CouncilDistrict", "PoliceDistrict", "Latitude", "Longitude", "IsDeleted"]
OUT_FIELDS = ["RowID"] + API_DATE_FIELDS + API_TEXT_FIELDS

# Columns kept from a request's earlier location when the API sends it without one.
LOCATION_COLS = {"latitude", "longitude", "location_source", "nsa_id"}



# ---------------------------------------------------------------- database

def connect() -> psycopg.Connection:
    con = psycopg.connect(
        host=os.environ["PGHOST"], port=os.environ.get("PGPORT", "5432"), dbname=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"], password=os.environ["PGPASSWORD"],
        sslmode="require", connect_timeout=15, autocommit=True,
        keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5,
    )
    con.execute("SET TimeZone = 'UTC'")
    return con


def table_columns(pg: psycopg.Connection, table: str) -> list[str]:
    rows = pg.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = %s AND is_generated = 'NEVER' "
        "ORDER BY ordinal_position", (table,)).fetchall()
    return [r[0] for r in rows]


def last_success(pg: psycopg.Connection) -> dt.datetime | None:
    return pg.execute("SELECT max(started_at) FROM sync_log").fetchone()[0]


# ---------------------------------------------------------------- fetch

def fetch(duck: duckdb.DuckDBPyConnection, where: str) -> int:
    """Page through the API into DuckDB table api_raw (all text, CSV-like layout)."""
    session = requests.Session()
    session.headers["User-Agent"] = "baltimore-311-pipeline"
    duck.execute(
        "CREATE OR REPLACE TABLE api_raw ("
        + ", ".join(["RowID BIGINT"] + [f"{f} VARCHAR" for f in API_DATE_FIELDS + API_TEXT_FIELDS])
        + ")")
    date_text = {  # epoch ms -> the 2026 CSV text format, so standardize parses it identically
        f: f"strftime(epoch_ms({f}), '%Y/%m/%d %H:%M:%S') || '+00'" for f in API_DATE_FIELDS}
    select = ", ".join(["RowID"] + [f"{date_text[f]} AS {f}" for f in API_DATE_FIELDS]
                       + [f"CAST({f} AS VARCHAR) AS {f}" for f in API_TEXT_FIELDS])

    last_id, total, start = 0, 0, time.time()
    while True:
        params = {
            "where": f"({where}) AND RowID > {last_id}", "outFields": ",".join(OUT_FIELDS),
            "orderByFields": "RowID", "resultRecordCount": PAGE_SIZE,
            "returnGeometry": "false", "f": "json",
        }
        for attempt in range(5):
            try:
                resp = session.get(API_URL, params=params, timeout=60)
                resp.raise_for_status()
                data = resp.json()
                if "error" in data:
                    raise RuntimeError(f"API error: {data['error'].get('message')}")
                break
            except (requests.RequestException, RuntimeError, ValueError) as e:
                if attempt == 4:
                    raise
                print(f"  fetch retry {attempt + 1} after: {type(e).__name__}")
                time.sleep(5 * 2 ** attempt)
        feats = data.get("features", [])
        if not feats:
            break
        page = pd.DataFrame([f["attributes"] for f in feats], columns=OUT_FIELDS)
        page = page.astype({"RowID": "Int64", **{f: "Int64" for f in API_DATE_FIELDS},
                            **{f: "string" for f in API_TEXT_FIELDS if f != "IsDeleted"}})
        page["IsDeleted"] = page["IsDeleted"].astype("Int64").astype("string")
        duck.register("page_df", page)
        duck.execute(f"INSERT INTO api_raw SELECT {select} FROM page_df")
        duck.unregister("page_df")
        last_id = int(page["RowID"].max())
        total += len(page)
        if total % 100_000 < PAGE_SIZE:
            print(f"  fetched {total:,} rows ({time.time() - start:.0f}s)")
    return total


# ---------------------------------------------------------------- clean

def clean(duck: duckdb.DuckDBPyConnection, pg: psycopg.Connection) -> None:
    """api_raw -> table batch (standardized + filled), one row per request."""
    # Same request twice in one fetch: keep the newest API row.
    duck.execute(f"""
        CREATE OR REPLACE TABLE api_src AS
        SELECT * EXCLUDE (RowID), '{SOURCE_FILE}' AS source_file
        FROM api_raw
        QUALIFY row_number() OVER (PARTITION BY ServiceRequestNum ORDER BY RowID DESC) = 1""")
    duck.execute(f"CREATE OR REPLACE TABLE std AS {standardize.build_sql('api_src')}")

    # Reference rows for address matching: known locations in the database for
    # the streets of new rows that lack coordinates (same lookup as Stage 3).
    placeholder = "(latitude = round(latitude) AND longitude = round(longitude))"
    keys = [r[0] for r in duck.execute(f"""
        SELECT DISTINCT {fill_locations.street_key_sql('address')} FROM std
        WHERE address IS NOT NULL AND (latitude IS NULL OR longitude IS NULL OR {placeholder})
        """).fetchall() if r[0]]
    ref = pd.DataFrame(columns=["service_request_num", "address", "zip_code", "latitude", "longitude", "created_date"])
    if keys:
        with pg.cursor() as cur:
            cur.execute("""
                SELECT service_request_num, address, zip_code, latitude, longitude, created_date::text
                FROM requests WHERE location_source = 'original' AND street_key = ANY(%s)""", (keys,))
            ref = pd.DataFrame(cur.fetchall(), columns=ref.columns)
    ref = ref.astype({"service_request_num": "string", "address": "string", "zip_code": "string",
                      "latitude": "float64", "longitude": "float64", "created_date": "string"})
    duck.register("ref_df", ref)
    duck.execute("""
        CREATE OR REPLACE TABLE fill_input AS
        SELECT *, false AS is_ref FROM std
        UNION ALL BY NAME
        SELECT service_request_num, address, zip_code, latitude, longitude,
               CAST(created_date AS TIMESTAMPTZ) AS created_date, true AS is_ref
        FROM ref_df WHERE service_request_num NOT IN (SELECT service_request_num FROM std)""")
    duck.unregister("ref_df")
    duck.execute(f"""
        CREATE OR REPLACE TABLE batch AS
        SELECT * EXCLUDE (is_ref) FROM ({fill_locations.build_sql('fill_input')}) WHERE NOT is_ref""")


# ---------------------------------------------------------------- merge

def stage(duck: duckdb.DuckDBPyConnection, pg: psycopg.Connection, cols: list[str]) -> None:
    """Copy table batch into a temp table `stage` shaped like requests (nsa_id added in SQL)."""
    pg.execute("DROP TABLE IF EXISTS stage")
    pg.execute(f"CREATE TEMP TABLE stage AS SELECT {', '.join(cols)} FROM requests WITH NO DATA")
    from_duck = [c for c in cols if c != "nsa_id"]
    ts_cols = {r[0] for r in duck.execute("DESCRIBE batch").fetchall() if r[1].startswith("TIMESTAMP")}
    select = ", ".join(f"CAST({c} AS VARCHAR)" if c in ts_cols else c for c in from_duck)
    result = duck.execute(f"SELECT {select} FROM batch")
    with pg.cursor() as cur, cur.copy(f"COPY stage ({', '.join(from_duck)}) FROM STDIN") as copy:
        while batch := result.fetchmany(50_000):
            for row in batch:
                copy.write_row(row)
    pg.execute("""
        UPDATE stage s SET nsa_id = (
            SELECT min(n.nsa_id) FROM neighborhoods n
            WHERE ST_Intersects(n.geom, ST_SetSRID(ST_MakePoint(s.longitude, s.latitude), 4326)))
        WHERE s.latitude IS NOT NULL""")
    pg.execute("CREATE INDEX ON stage (service_request_num)")
    pg.execute("ANALYZE stage")


def upsert_sql(table: str, cols: list[str], where: str) -> str:
    """INSERT ... ON CONFLICT DO UPDATE that only writes rows that actually changed."""
    set_cols = [c for c in cols if c != "service_request_num"]
    return f"""
        INSERT INTO {table} AS t ({', '.join(cols)})
        SELECT {', '.join(cols)} FROM stage s WHERE {where}
        ON CONFLICT (service_request_num) DO UPDATE
          SET {', '.join(f'{c} = EXCLUDED.{c}' for c in set_cols)}
          WHERE ({', '.join(f't.{c}' for c in set_cols)})
                IS DISTINCT FROM ({', '.join(f'EXCLUDED.{c}' for c in set_cols)})
        RETURNING old.service_request_num IS NULL AS inserted"""


def merge(pg: psycopg.Connection, req_cols: list[str], noloc_cols: list[str]) -> dict[str, int]:
    c = {}
    rows = pg.execute(upsert_sql("requests", req_cols, "s.latitude IS NOT NULL")).fetchall()
    c["requests_inserted"] = sum(r[0] for r in rows)
    c["requests_updated"] = len(rows) - c["requests_inserted"]
    c["moved_to_requests"] = pg.execute("""
        DELETE FROM requests_no_location n USING stage s
        WHERE s.latitude IS NOT NULL AND n.service_request_num = s.service_request_num""").rowcount

    keep = [col for col in noloc_cols if col != "service_request_num" and col not in LOCATION_COLS]
    c["requests_updated_kept_location"] = pg.execute(f"""
        UPDATE requests r SET {', '.join(f'{col} = s.{col}' for col in keep)}
        FROM stage s
        WHERE s.latitude IS NULL AND r.service_request_num = s.service_request_num
          AND ({', '.join(f'r.{col}' for col in keep)}) IS DISTINCT FROM ({', '.join(f's.{col}' for col in keep)})
        """).rowcount
    rows = pg.execute(upsert_sql(
        "requests_no_location", noloc_cols,
        "s.latitude IS NULL AND NOT EXISTS (SELECT 1 FROM requests r WHERE r.service_request_num = s.service_request_num)",
    )).fetchall()
    c["no_location_inserted"] = sum(r[0] for r in rows)
    c["no_location_updated"] = len(rows) - c["no_location_inserted"]
    return c


def compare_report(pg: psycopg.Connection, cols: list[str]) -> None:
    """Dry-run check: for requests already in the DB, how many differ per column?"""
    exprs = [f"count(*) FILTER (WHERE s.{c} IS DISTINCT FROM r.{c})" for c in cols if c != "service_request_num"]
    names = [c for c in cols if c != "service_request_num"]
    for table in ("requests", "requests_no_location"):
        tcols = set(table_columns(pg, table))
        use = [(n, e) for n, e in zip(names, exprs) if n in tcols]
        row = pg.execute(f"""
            SELECT count(*), {', '.join(e for _, e in use)}
            FROM stage s JOIN {table} r USING (service_request_num)""").fetchone()
        diffs = {n: v for (n, _), v in zip(use, row[1:]) if v}
        print(f"  vs existing {table:<21}: {row[0]:,} matched; columns that differ: "
              + (", ".join(f"{k} {v:,}" for k, v in sorted(diffs.items(), key=lambda kv: -kv[1])) or "none"))


# ---------------------------------------------------------------- main

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--full", action="store_true", help="fetch every record in the API (weekly refresh)")
    mode.add_argument("--since", type=dt.date.fromisoformat, help="fetch records since this date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="do everything, print the counts, then roll back")
    args = parser.parse_args()
    load_dotenv(ROOT / ".env")

    started = dt.datetime.now(dt.timezone.utc)
    pg = connect()
    if not pg.execute("SELECT to_regclass('public.sync_log')").fetchone()[0]:
        pg.execute(SYNC_LOG_DDL)  # normally created by db_setup.py (the limited role can't create tables)

    if args.full:
        since, where = None, "1=1"
    else:
        if args.since:
            since = dt.datetime.combine(args.since, dt.time(), dt.timezone.utc)
        else:
            prev = last_success(pg)
            if prev is None:
                raise SystemExit("no successful run in sync_log yet: run with --full first "
                                 "(the database is weeks behind the API)")
            since = prev - OVERLAP
        ts = since.strftime("%Y-%m-%d %H:%M:%S")
        where = " OR ".join(f"{f} >= TIMESTAMP '{ts}'" for f in API_DATE_FIELDS)
    print(f"mode        : {'full' if args.full else 'window since ' + since.strftime('%Y-%m-%d %H:%M UTC')}"
          f"{' (dry run)' if args.dry_run else ''}")

    duck = duckdb.connect()
    duck.execute("SET TimeZone = 'UTC'")
    t = time.time()
    fetched = fetch(duck, where)
    print(f"fetched     : {fetched:,} rows ({time.time() - t:.0f}s)")
    if fetched == 0:
        print("nothing to do")
        return

    t = time.time()
    clean(duck, pg)
    n, n_loc, n_match, n_susp, n_del = duck.execute("""
        SELECT count(*), count(latitude), count(*) FILTER (WHERE location_source = 'address_match'),
               count(*) FILTER (WHERE ts_suspect), count(*) FILTER (WHERE is_deleted) FROM batch""").fetchall()[0]
    print(f"cleaned     : {n:,} unique requests ({time.time() - t:.0f}s) | located {n_loc:,} "
          f"(address-matched {n_match:,}) | no location {n - n_loc:,} | ts_suspect {n_susp:,} | deleted {n_del:,}")

    req_cols = table_columns(pg, "requests")
    noloc_cols = table_columns(pg, "requests_no_location")
    t = time.time()
    with pg.transaction() as tx:
        stage(duck, pg, req_cols)
        n_nsa = pg.execute("SELECT count(nsa_id) FROM stage").fetchone()[0]
        print(f"staged      : {n:,} rows, {n_nsa:,} inside a neighborhood ({time.time() - t:.0f}s)")
        if args.dry_run:
            compare_report(pg, req_cols)
        c = merge(pg, req_cols, noloc_cols)
        for k, v in c.items():
            print(f"  {k:<31}: {v:,}")
        if args.dry_run:
            raise psycopg.Rollback(tx)  # leaves the block, discarding everything
        pg.execute(
            "INSERT INTO sync_log (started_at, finished_at, mode, since, fetched, inserted, updated, moved) "
            "VALUES (%s, clock_timestamp(), %s, %s, %s, %s, %s, %s)",  # now() = transaction start
            (started, "full" if args.full else "window", since, fetched,
             c["requests_inserted"] + c["no_location_inserted"],
             c["requests_updated"] + c["requests_updated_kept_location"] + c["no_location_updated"],
             c["moved_to_requests"]))
    print(f"{'rolled back (dry run)' if args.dry_run else 'committed'} in {time.time() - t:.0f}s")


if __name__ == "__main__":
    main()
