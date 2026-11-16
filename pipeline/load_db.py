"""Stage 7: create the tables in RDS PostgreSQL and bulk-load the parquet outputs.

Tables
  neighborhoods          279 NSA polygons + demographics (from neighborhoods.parquet)
  requests               requests with a location (from requests_final.parquet)
  requests_no_location   requests without a location (from requests_no_location.parquet)

Steps
  1. Safety check: if any of the tables already holds rows, print the counts and
     stop, unless --replace is given (then drop and reload all three).
  2. Create the tables without keys or indexes (bulk loading is much faster
     without them).
  3. Load neighborhoods (small; plain INSERTs, geometry passed as WKB).
  4. Stream requests / requests_no_location from DuckDB into Postgres COPY in
     chunks, committing each chunk, so memory stays low on both machines.
     requests.geom is a generated column: Postgres builds the point from
     longitude/latitude itself, during COPY and on every later insert/update.
  5. Add primary keys, the foreign key requests.nsa_id -> neighborhoods, and
     indexes (created_date, sr_type, nsa_id, GIST on geom).
  6. VACUUM ANALYZE so the query planner has fresh statistics.
  7. Verify: row counts match the parquet files, every request has a geom,
     and the read-only dashboard role can read the tables.

Usage:
    .venv/Scripts/python pipeline/load_db.py            # first load
    .venv/Scripts/python pipeline/load_db.py --replace  # drop and reload everything
    .venv/Scripts/python pipeline/load_db.py --finish   # data already loaded: only add missing
                                                        # keys/indexes, vacuum and verify
"""

import argparse
import os
import time
from pathlib import Path

import duckdb
import geopandas as gpd
import psycopg
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
INTERIM = ROOT / "data" / "interim"
CHUNK_ROWS = 500_000   # rows per COPY transaction
FETCH_ROWS = 50_000    # rows pulled from DuckDB at a time

TABLES = ["requests", "requests_no_location", "neighborhoods"]

# Columns shared by both request tables, in load order, with their Postgres types.
REQUEST_COLUMNS = [
    ("service_request_num", "text NOT NULL"),
    ("sr_record_id", "text"),
    ("sr_type", "text"),
    ("method_received", "text"),
    ("created_date", "timestamptz"),
    ("status_date", "timestamptz"),
    ("due_date", "timestamptz"),
    ("close_date", "timestamptz"),
    ("last_activity_date", "timestamptz"),
    ("sr_status", "text"),
    ("agency", "text"),
    ("last_activity", "text"),
    ("outcome", "text"),
    ("address", "text"),
    ("zip_code", "text"),
    ("neighborhood", "text"),
    ("council_district", "smallint"),
    ("police_district", "text"),
    ("police_post", "text"),
    ("is_deleted", "boolean NOT NULL"),
    ("ts_suspect", "boolean NOT NULL"),
    ("source_file", "text NOT NULL"),
    ("street_key", "text"),
]
LOCATED_EXTRA = [
    ("latitude", "double precision NOT NULL"),
    ("longitude", "double precision NOT NULL"),
    ("location_source", "text NOT NULL"),
    ("nsa_id", "smallint"),
]
TIMESTAMP_COLS = {c for c, t in REQUEST_COLUMNS if t == "timestamptz"}

LOADS = {
    # table -> (parquet file, columns)
    "requests": ("requests_final.parquet", REQUEST_COLUMNS + LOCATED_EXTRA),
    "requests_no_location": ("requests_no_location.parquet", REQUEST_COLUMNS),
}

# (constraint name, table, SQL) - constraints are skipped if they already exist,
# so these steps can be re-run with --finish after an interruption.
CONSTRAINTS = [
    ("neighborhoods_pkey", "ALTER TABLE neighborhoods ADD CONSTRAINT neighborhoods_pkey PRIMARY KEY (nsa_id)"),
    ("requests_pkey", "ALTER TABLE requests ADD CONSTRAINT requests_pkey PRIMARY KEY (service_request_num)"),
    ("requests_no_location_pkey",
     "ALTER TABLE requests_no_location ADD CONSTRAINT requests_no_location_pkey PRIMARY KEY (service_request_num)"),
    ("requests_nsa_id_fkey",
     "ALTER TABLE requests ADD CONSTRAINT requests_nsa_id_fkey FOREIGN KEY (nsa_id) REFERENCES neighborhoods (nsa_id)"),
]
INDEXES = [
    "CREATE INDEX IF NOT EXISTS requests_nsa_id_idx ON requests (nsa_id)",
    "CREATE INDEX IF NOT EXISTS requests_created_date_idx ON requests (created_date)",
    "CREATE INDEX IF NOT EXISTS requests_sr_type_idx ON requests (sr_type)",
    "CREATE INDEX IF NOT EXISTS requests_geom_idx ON requests USING gist (geom)",
    "CREATE INDEX IF NOT EXISTS neighborhoods_geom_idx ON neighborhoods USING gist (geom)",
]


def connect(user: str | None = None, password: str | None = None) -> psycopg.Connection:
    return psycopg.connect(
        host=os.environ["PGHOST"], port=os.environ["PGPORT"], dbname=os.environ["PGDATABASE"],
        user=user or os.environ["PGUSER"], password=password or os.environ["PGPASSWORD"],
        sslmode="require", connect_timeout=10, autocommit=True,
        # Index builds run minutes with no traffic; without keepalives a home
        # router can silently drop the idle connection and the script hangs.
        keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5,
    )


def existing_counts(con: psycopg.Connection) -> dict[str, int]:
    counts = {}
    for t in TABLES:
        if con.execute("SELECT to_regclass(%s)", (f"public.{t}",)).fetchone()[0]:
            counts[t] = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
    return counts


def create_tables(con: psycopg.Connection, nsa_cols: list[str]) -> None:
    con.execute("DROP TABLE IF EXISTS requests, requests_no_location, neighborhoods CASCADE")

    nsa_defs = ",\n  ".join(
        ["nsa_id smallint NOT NULL", "name text NOT NULL", "globalid text"]
        + [f"{c} double precision" for c in nsa_cols]
        + ["geom geometry(MultiPolygon, 4326) NOT NULL"]
    )
    con.execute(f"CREATE TABLE neighborhoods (\n  {nsa_defs}\n)")

    for table, (_, cols) in LOADS.items():
        defs = [f"{c} {t}" for c, t in cols]
        if table == "requests":
            defs.append(
                "geom geometry(Point, 4326) GENERATED ALWAYS AS "
                "(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)) STORED"
            )
        con.execute(f"CREATE TABLE {table} (\n  " + ",\n  ".join(defs) + "\n)")
    print("tables      : created neighborhoods, requests, requests_no_location")


def load_neighborhoods(con: psycopg.Connection) -> int:
    gdf = gpd.read_parquet(INTERIM / "neighborhoods.parquet")
    assert gdf.crs.to_epsg() == 4326, gdf.crs
    gdf.columns = [c if c == "geometry" else c.lower() for c in gdf.columns]
    attr_cols = [c for c in gdf.columns if c != "geometry"]
    placeholders = ", ".join(["%s"] * len(attr_cols))
    sql = (
        f"INSERT INTO neighborhoods ({', '.join(attr_cols)}, geom) "
        f"VALUES ({placeholders}, ST_Multi(ST_GeomFromWKB(%s, 4326)))"
    )
    rows = []
    for rec, geom in zip(gdf[attr_cols].itertuples(index=False), gdf.geometry):
        vals = [None if v != v else v for v in rec]  # NaN -> NULL
        vals[0] = int(vals[0])                       # nsa_id
        rows.append(vals + [geom.wkb])
    with con.transaction(), con.cursor() as cur:
        cur.executemany(sql, rows)
    return len(rows)


def nsa_demographic_columns() -> list[str]:
    cols = gpd.read_parquet(INTERIM / "neighborhoods.parquet").columns
    return [c.lower() for c in cols if c not in ("nsa_id", "Name", "GlobalID", "geometry")]


def copy_table(pg: psycopg.Connection, table: str) -> int:
    parquet, cols = LOADS[table]
    names = [c for c, _ in cols]
    # Timestamps go over as text in UTC ('2020-01-01 05:00:00+00'); this also
    # avoids DuckDB needing pytz to hand timezone-aware datetimes to Python.
    select = ", ".join(f"CAST({c} AS VARCHAR) AS {c}" if c in TIMESTAMP_COLS else c for c in names)

    duck = duckdb.connect()
    duck.execute("SET TimeZone = 'UTC'")
    total = duck.execute(f"SELECT count(*) FROM '{(INTERIM / parquet).as_posix()}'").fetchall()[0][0]
    result = duck.execute(f"SELECT {select} FROM '{(INTERIM / parquet).as_posix()}'")

    copy_sql = f"COPY {table} ({', '.join(names)}) FROM STDIN"
    loaded, start, done = 0, time.time(), False
    while not done:
        in_chunk = 0
        with pg.transaction(), pg.cursor() as cur, cur.copy(copy_sql) as copy:
            while in_chunk < CHUNK_ROWS:
                batch = result.fetchmany(FETCH_ROWS)
                if not batch:
                    done = True
                    break
                for row in batch:
                    copy.write_row(row)
                in_chunk += len(batch)
        loaded += in_chunk
        rate = loaded / max(time.time() - start, 1e-9)
        eta = (total - loaded) / rate if rate else 0
        print(f"  {table:<21}: {loaded:>11,} / {total:,} rows  ({rate:,.0f} rows/s, ~{eta / 60:.0f} min left)")
    duck.close()
    return loaded


def parquet_count(name: str) -> int:
    return duckdb.sql(f"SELECT count(*) FROM '{(INTERIM / name).as_posix()}'").fetchall()[0][0]


def verify(con: psycopg.Connection) -> None:
    expected = {
        "neighborhoods": parquet_count("neighborhoods.parquet"),
        "requests": parquet_count("requests_final.parquet"),
        "requests_no_location": parquet_count("requests_no_location.parquet"),
    }
    ok = True
    for t, exp in expected.items():
        got = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        flag = "OK" if got == exp else "MISMATCH"
        ok &= got == exp
        print(f"  {t:<21}: {got:>11,} rows (parquet {exp:,}) {flag}")

    no_geom, nsa_null, bad_nsa = con.execute(
        "SELECT count(*) FILTER (WHERE geom IS NULL), count(*) FILTER (WHERE nsa_id IS NULL), "
        "count(*) FILTER (WHERE nsa_id IS NOT NULL AND NOT ST_Intersects(geom, "
        "(SELECT n.geom FROM neighborhoods n WHERE n.nsa_id = r.nsa_id))) "
        "FROM requests r TABLESAMPLE SYSTEM (1)"
    ).fetchone()
    print(f"  requests (1% sample)  : {no_geom} without geom, {nsa_null:,} with NULL nsa_id, "
          f"{bad_nsa} points outside their assigned NSA")
    ok &= no_geom == 0 and bad_nsa == 0

    ro = connect(os.environ["DASHBOARD_USER"], os.environ["DASHBOARD_PASSWORD"])
    n = ro.execute("SELECT count(*) FROM neighborhoods").fetchone()[0]
    ro.close()
    print(f"  {os.environ['DASHBOARD_USER']} read check : SELECT on neighborhoods returned {n} rows")
    print("verify      :", "all checks passed" if ok else "SOME CHECKS FAILED")


def post_load(con: psycopg.Connection) -> None:
    con.execute("SET maintenance_work_mem = '256MB'")
    for name, stmt in CONSTRAINTS:
        if con.execute("SELECT 1 FROM pg_constraint WHERE conname = %s", (name,)).fetchone():
            print(f"  exists   {name}")
            continue
        s = time.time()
        con.execute(stmt)
        print(f"  {time.time() - s:6.1f}s  {stmt}")
    for stmt in INDEXES:
        s = time.time()
        con.execute(stmt)
        print(f"  {time.time() - s:6.1f}s  {stmt}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--replace", action="store_true", help="drop and reload tables that already contain data")
    parser.add_argument("--finish", action="store_true",
                        help="skip loading; only add missing keys/indexes, vacuum and verify (after an interruption)")
    args = parser.parse_args()
    load_dotenv(ROOT / ".env")

    t0 = time.time()
    with connect() as con:
        if not args.finish:
            counts = existing_counts(con)
            if any(counts.values()) and not args.replace:
                for t, n in counts.items():
                    print(f"  {t:<21}: {n:,} rows")
                raise SystemExit("tables already contain data; re-run with --replace to drop and reload them, "
                                 "or --finish to only complete keys/indexes/verify")

            create_tables(con, nsa_demographic_columns())
            print(f"neighborhoods: loaded {load_neighborhoods(con)} rows")
            for table in LOADS:
                copy_table(con, table)

        post_load(con)
        for t in TABLES:
            con.execute(f"VACUUM ANALYZE {t}")
        print("vacuum      : VACUUM ANALYZE done")

        print("verify:")
        verify(con)
    print(f"done in {(time.time() - t0) / 60:.1f} min")


if __name__ == "__main__":
    main()
