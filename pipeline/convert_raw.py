"""Convert raw 311 CSVs (data/raw) into Parquet (data/interim/raw_parquet).

Every column is kept as text (VARCHAR), exactly as in the source CSV, so no
values are lost to bad type guesses. Real types are applied in Stage 2.

A `source_file` column is added so each row can be traced back to its CSV.

Usage:
    .venv/Scripts/python pipeline/convert_raw.py           # skip years already converted
    .venv/Scripts/python pipeline/convert_raw.py --force   # reconvert everything
"""

import argparse
import time
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"
OUT_DIR = ROOT / "data" / "interim" / "raw_parquet"


def convert(csv_path: Path, out_path: Path, con: duckdb.DuckDBPyConnection) -> int:
    # Write to a temp file first, then rename, so an interrupted run never
    # leaves a half-written Parquet file that looks complete.
    tmp_path = out_path.with_suffix(".parquet.tmp")
    con.execute(
        f"""
        COPY (
            SELECT *, '{csv_path.name}' AS source_file
            FROM read_csv('{csv_path.as_posix()}',
                          header = true,
                          -- State the format explicitly: auto-detection
                          -- misread the 2010 file as a single column.
                          delim = ',',
                          quote = '"',
                          escape = '"',
                          all_varchar = true,
                          -- Some rows omit trailing empty fields (e.g. 23 of
                          -- 25 columns). Pad those with NULL. Rows with too
                          -- MANY fields still raise an error.
                          null_padding = true)
        ) TO '{tmp_path.as_posix()}' (FORMAT parquet, COMPRESSION zstd)
        """
    )
    tmp_path.replace(out_path)

    # Sanity check: a real 311 file has ~25+ columns. If parsing collapsed
    # everything into one column, fail loudly instead of continuing.
    n_cols = len(con.execute(f"DESCRIBE SELECT * FROM '{out_path.as_posix()}'").fetchall())
    if n_cols < 20:
        raise RuntimeError(f"{csv_path.name}: only {n_cols} columns parsed - check CSV format")
    return con.execute(f"SELECT count(*) FROM '{out_path.as_posix()}'").fetchone()[0]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true", help="reconvert files that already exist")
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()

    csvs = sorted(RAW_DIR.glob("*.csv"))
    if not csvs:
        raise SystemExit(f"No CSVs found in {RAW_DIR}")

    for csv_path in csvs:
        out_path = OUT_DIR / f"{csv_path.stem}.parquet"
        if out_path.exists() and not args.force:
            print(f"skip   {csv_path.name} (already converted)")
            continue
        start = time.time()
        rows = convert(csv_path, out_path, con)
        mb_in = csv_path.stat().st_size / 1e6
        mb_out = out_path.stat().st_size / 1e6
        print(f"done   {csv_path.name}: {rows:,} rows, {mb_in:,.0f} MB -> {mb_out:,.0f} MB "
              f"({time.time() - start:.0f}s)")


if __name__ == "__main__":
    main()
