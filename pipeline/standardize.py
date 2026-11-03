"""Stage 2: standardize raw 311 data into one typed table.

Input : data/interim/raw_parquet/*.parquet  (all text, three column layouts)
Output: data/interim/standardized.parquet   (one schema, snake_case, real types)

What this does
--------------
* Maps all three column layouts to one schema with snake_case names.
* Drops redundant / source-system columns (GeoLocation, x, y, RowID,
  ESRI_OID, HashedRecord, NeedsSync).
* Trims whitespace and turns empty strings into NULL.
* Parses both date formats into UTC timestamps (TIMESTAMPTZ):
    - 'M/D/YYYY h:mm:ss AM'   (2010-2025): Baltimore local time -> UTC
    - 'YYYY/MM/DD HH:MM:SS+00' (2026):    labeled UTC, BUT see below
* 2026 timestamp bug: from about 2026-08-16 the source system writes
  Baltimore local time while labeling it "+00". It happens per record (all
  dates in a record follow one convention) and no field marks it. Rule
  (chosen in Stage 2, ~90% accurate where it can be checked): if a record's
  latest date is on/after SWITCH_DATE, its labels are treated as local time
  and converted to real UTC, and ts_suspect = true.
* Normalizes police_district capitalization (NORTHERN / Northern -> Northern).

Usage:
    .venv/Scripts/python pipeline/standardize.py
"""

import time
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
IN_GLOB = (ROOT / "data" / "interim" / "raw_parquet" / "*.parquet").as_posix()
OUT_PATH = ROOT / "data" / "interim" / "standardized.parquet"

LOCAL_TZ = "America/New_York"
# From this (labeled) date on, the source writes local time labeled as UTC.
SWITCH_DATE = "2026-08-16"

OLD_FMT = "%m/%d/%Y %I:%M:%S %p"   # 3/24/2010 7:21:00 AM   (local, no tz)
NEW_FMT = "%Y/%m/%d %H:%M:%S%z"    # 2026/01/03 14:29:00+00 (labeled UTC)

DATE_COLS = {
    "CreatedDate": "created_date",
    "StatusDate": "status_date",
    "DueDate": "due_date",
    "CloseDate": "close_date",
    "LastActivityDate": "last_activity_date",
}

TEXT_COLS = {
    "ServiceRequestNum": "service_request_num",
    "SRRecordID": "sr_record_id",
    "SRType": "sr_type",
    "MethodReceived": "method_received",
    "SRStatus": "sr_status",
    "Agency": "agency",
    "LastActivity": "last_activity",
    "Outcome": "outcome",
    "Address": "address",
    "ZipCode": "zip_code",
    "Neighborhood": "neighborhood",
    "PolicePost": "police_post",  # text: some rows list several posts ("111,112")
}


def clean(col: str) -> str:
    """Trim whitespace; empty string -> NULL."""
    return f"nullif(trim({col}), '')"


def build_sql() -> str:
    # Step 1: clean text and parse dates "as labeled" (naive wall-clock values).
    parsed = []
    for raw, new in DATE_COLS.items():
        c = clean(raw)
        parsed.append(
            f"coalesce(try_strptime({c}, '{OLD_FMT}'), "
            f"try_strptime({c}, '{NEW_FMT}')::TIMESTAMP) AS {new}_wall"
        )
        # New format starts with a 4-digit year and a slash: '2026/...'.
        # (Not LIKE '____/%': '_' matches any char, so '4/28/2025' would match.)
        parsed.append(f"regexp_matches({c}, '^\\d{{4}}/') AS {new}_is_new_fmt")
    texts = [f"{clean(raw)} AS {new}" for raw, new in TEXT_COLS.items()]

    # Step 2: decide per record whether the 2026 "+00" labels are really local.
    wall = [f"{new}_wall" for new in DATE_COLS.values()]
    latest = "greatest(" + ", ".join(f"coalesce({w}, TIMESTAMP '1900-01-01')" for w in wall) + ")"

    # Step 3: convert each date to a real UTC TIMESTAMPTZ.
    #   old format            -> wall clock is local time
    #   new format, suspect   -> wall clock is local time (mislabeled)
    #   new format, fine      -> wall clock is UTC
    final_dates = []
    for new in DATE_COLS.values():
        final_dates.append(
            f"""CASE
                  WHEN {new}_wall IS NULL THEN NULL
                  WHEN NOT {new}_is_new_fmt OR ts_suspect THEN timezone('{LOCAL_TZ}', {new}_wall)
                  ELSE timezone('UTC', {new}_wall)
                END AS {new}"""
        )

    return f"""
    WITH src AS (
        SELECT * FROM read_parquet('{IN_GLOB}', union_by_name = true)
    ),
    step1 AS (
        SELECT
            {", ".join(texts)},
            {", ".join(parsed)},
            try_cast({clean('CouncilDistrict')} AS SMALLINT)            AS council_district,
            -- single words: first letter upper, rest lower (NORTHERN -> Northern)
            upper(left({clean('PoliceDistrict')}, 1))
              || lower(substr({clean('PoliceDistrict')}, 2))            AS police_district,
            try_cast({clean('Latitude')}  AS DOUBLE)                    AS latitude,
            try_cast({clean('Longitude')} AS DOUBLE)                    AS longitude,
            coalesce({clean('IsDeleted')} = '1', false)                 AS is_deleted,
            source_file
        FROM src
    ),
    step2 AS (
        SELECT *,
               (created_date_is_new_fmt AND {latest} >= TIMESTAMP '{SWITCH_DATE}') AS ts_suspect
        FROM step1
    )
    SELECT
        service_request_num, sr_record_id, sr_type, method_received,
        {", ".join(final_dates)},
        sr_status, agency, last_activity, outcome,
        address, zip_code, neighborhood, council_district, police_district, police_post,
        latitude, longitude,
        is_deleted, ts_suspect, source_file
    FROM step2
    """


def main() -> None:
    con = duckdb.connect()
    con.sql("SET TimeZone = 'UTC'")  # so nothing is shifted by this computer's time zone
    start = time.time()
    tmp = OUT_PATH.with_suffix(".parquet.tmp")
    con.sql(f"COPY ({build_sql()}) TO '{tmp.as_posix()}' (FORMAT parquet, COMPRESSION zstd)")
    tmp.replace(OUT_PATH)
    n = con.sql(f"SELECT count(*) FROM '{OUT_PATH.as_posix()}'").fetchall()[0][0]
    print(f"wrote {OUT_PATH.relative_to(ROOT)}: {n:,} rows "
          f"({OUT_PATH.stat().st_size / 1e6:,.0f} MB, {time.time() - start:.0f}s)")


if __name__ == "__main__":
    main()
