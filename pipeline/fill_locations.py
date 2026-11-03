"""Stage 3: fill in missing locations by matching addresses across the dataset.

Input : data/interim/standardized.parquet
Output: data/interim/located.parquet  (same rows + location_source column)

How it works
------------
1. Build a cleaned matching key for every address:
   - keep the street part (before the first comma), uppercase,
     drop punctuation, squeeze spaces
   - standardize words (AVENUE/AV -> AVE, STREET -> ST, NORTH -> N, ...)
   - intersections: sort the two streets so "A & B" == "B & A"
   - pull out a 5-digit zip (from the address, else the zip_code column)
2. From rows that HAVE coordinates, build two lookup tables:
   - by street + zip, and by street only
   For each key: the most recent request's coordinates, and the "spread"
   (largest distance of any known point from the key's median point).
3. For rows with an address but no coordinates:
   - try street + zip first, then street only
   - only accept a key whose spread is <= MAX_SPREAD_M (200 m); widely
     spread keys (bad coordinates, vague entries like "I 83") are skipped
4. location_source = 'original' (had coordinates), 'address_match'
   (filled here), or NULL (still no location -> split off in Stage 4).

Usage:
    .venv/Scripts/python pipeline/fill_locations.py
"""

import time
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
IN_PATH = ROOT / "data" / "interim" / "standardized.parquet"
OUT_PATH = ROOT / "data" / "interim" / "located.parquet"

MAX_SPREAD_M = 200

# Whole-word replacements applied to the street key (order does not matter:
# no output word is also an input word).
WORD_MAP = {
    "AVENUE": "AVE", "AV": "AVE",
    "STREET": "ST", "STR": "ST",
    "ROAD": "RD",
    "DRIVE": "DR",
    "LANE": "LN",
    "COURT": "CT",
    "BOULEVARD": "BLVD",
    "PARKWAY": "PKWY",
    "PLACE": "PL",
    "TERRACE": "TER", "TERR": "TER",
    "HIGHWAY": "HWY",
    "CIRCLE": "CIR",
    "NORTH": "N", "SOUTH": "S", "EAST": "E", "WEST": "W",
}


def street_key_sql(col: str) -> str:
    """SQL expression turning an address into a normalized street key."""
    s = f"upper(split_part({col}, ',', 1))"
    s = f"regexp_replace({s}, '[^A-Z0-9& ]', ' ', 'g')"   # drop punctuation, keep &
    for long, short in WORD_MAP.items():
        s = f"regexp_replace({s}, '\\b{long}\\b', '{short}', 'g')"
    s = f"trim(regexp_replace({s}, '\\s+', ' ', 'g'))"     # squeeze spaces
    # Intersections: split on &, trim each part, sort, re-join.
    return (
        f"CASE WHEN {s} LIKE '%&%' "
        f"THEN array_to_string(list_sort(list_transform(string_split({s}, '&'), lambda x: trim(x))), ' & ') "
        f"ELSE {s} END"
    )


def build_sql() -> str:
    zip_from_address = r"nullif(regexp_extract(address, '(\d{5})(-\d{4})?\s*$', 1), '')"
    zip_from_column = r"CASE WHEN regexp_matches(zip_code, '^\d{5}') THEN left(zip_code, 5) END"

    # Approximate distance in meters between two nearby lat/lon points
    # (flat-earth approximation; accurate enough at city scale).
    def dist_m(lat1, lon1, lat2, lon2):
        return (f"111320 * sqrt(power({lat1} - {lat2}, 2) + "
                f"power(({lon1} - {lon2}) * cos(radians(39.3)), 2))")

    def lookup(keys: str) -> str:
        return f"""
        SELECT {keys},
               arg_max(latitude,  created_date) AS fill_lat,   -- most recent request
               arg_max(longitude, created_date) AS fill_lon,
               max({dist_m('latitude', 'longitude', 'med_lat', 'med_lon')}) AS spread_m
        FROM (
            SELECT *, median(latitude)  OVER (PARTITION BY {keys}) AS med_lat,
                      median(longitude) OVER (PARTITION BY {keys}) AS med_lon
            FROM keyed WHERE latitude IS NOT NULL AND street_key <> ''
                         {"AND zip5 IS NOT NULL" if "zip5" in keys else ""}
        )
        GROUP BY {keys}
        """

    return f"""
    WITH keyed AS (
        SELECT *,
               {street_key_sql('address')} AS street_key,
               coalesce({zip_from_address}, {zip_from_column}) AS zip5
        FROM '{IN_PATH.as_posix()}'
    ),
    by_street_zip AS ({lookup('street_key, zip5')}),
    by_street     AS ({lookup('street_key')}),
    filled AS (
        SELECT k.*,
               CASE WHEN z.spread_m <= {MAX_SPREAD_M} THEN z.fill_lat
                    WHEN s.spread_m <= {MAX_SPREAD_M} THEN s.fill_lat END AS new_lat,
               CASE WHEN z.spread_m <= {MAX_SPREAD_M} THEN z.fill_lon
                    WHEN s.spread_m <= {MAX_SPREAD_M} THEN s.fill_lon END AS new_lon
        FROM keyed k
        LEFT JOIN by_street_zip z ON k.latitude IS NULL AND z.street_key = k.street_key AND z.zip5 = k.zip5
        LEFT JOIN by_street     s ON k.latitude IS NULL AND s.street_key = k.street_key
    )
    SELECT * EXCLUDE (latitude, longitude, new_lat, new_lon, street_key, zip5),
           coalesce(latitude,  new_lat) AS latitude,
           coalesce(longitude, new_lon) AS longitude,
           CASE WHEN latitude IS NOT NULL THEN 'original'
                WHEN new_lat  IS NOT NULL THEN 'address_match' END AS location_source,
           street_key
    FROM filled
    """


def main() -> None:
    con = duckdb.connect()
    con.sql("SET TimeZone = 'UTC'")
    start = time.time()
    tmp = OUT_PATH.with_suffix(".parquet.tmp")
    con.sql(f"COPY ({build_sql()}) TO '{tmp.as_posix()}' (FORMAT parquet, COMPRESSION zstd)")
    tmp.replace(OUT_PATH)
    stats = con.sql(f"""
        SELECT count(*), count(*) FILTER (WHERE location_source = 'original'),
               count(*) FILTER (WHERE location_source = 'address_match'),
               count(*) FILTER (WHERE location_source IS NULL)
        FROM '{OUT_PATH.as_posix()}'""").fetchall()[0]
    print(f"wrote {OUT_PATH.relative_to(ROOT)} in {time.time() - start:.0f}s: "
          f"{stats[0]:,} rows | original {stats[1]:,} | filled {stats[2]:,} | still no location {stats[3]:,}")


if __name__ == "__main__":
    main()
