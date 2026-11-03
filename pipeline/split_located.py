"""Stage 4: split requests into those with and without a location.

Input : data/interim/located.parquet
Output: data/interim/requests_located.parquet     (has latitude/longitude)
        data/interim/requests_no_location.parquet (no location; kept so total
                                                   call volume stays countable)

The split is by whether a location exists, not by request type: most types
have a mix of located and unlocated rows (checked in Stage 4).

The no-location file drops latitude, longitude and location_source, which
would always be empty there.

Usage:
    .venv/Scripts/python pipeline/split_located.py
"""

from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
IN_PATH = ROOT / "data" / "interim" / "located.parquet"
OUT_LOCATED = ROOT / "data" / "interim" / "requests_located.parquet"
OUT_NO_LOCATION = ROOT / "data" / "interim" / "requests_no_location.parquet"


def write(con: duckdb.DuckDBPyConnection, sql: str, out: Path) -> int:
    tmp = out.with_suffix(".parquet.tmp")
    con.sql(f"COPY ({sql}) TO '{tmp.as_posix()}' (FORMAT parquet, COMPRESSION zstd)")
    tmp.replace(out)
    return con.sql(f"SELECT count(*) FROM '{out.as_posix()}'").fetchall()[0][0]


def main() -> None:
    con = duckdb.connect()
    con.sql("SET TimeZone = 'UTC'")
    src = f"'{IN_PATH.as_posix()}'"

    n_located = write(con, f"SELECT * FROM {src} WHERE latitude IS NOT NULL", OUT_LOCATED)
    n_none = write(
        con,
        f"SELECT * EXCLUDE (latitude, longitude, location_source) FROM {src} WHERE latitude IS NULL",
        OUT_NO_LOCATION,
    )
    n_total = con.sql(f"SELECT count(*) FROM {src}").fetchall()[0][0]

    print(f"located     : {n_located:,} -> {OUT_LOCATED.relative_to(ROOT)}")
    print(f"no location : {n_none:,} -> {OUT_NO_LOCATION.relative_to(ROOT)}")
    if n_located + n_none != n_total:
        raise RuntimeError(f"split lost rows: {n_located:,} + {n_none:,} != {n_total:,}")
    print(f"total       : {n_total:,} (matches input)")


if __name__ == "__main__":
    main()
