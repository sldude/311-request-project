"""Stage 5: assign each located request to a Neighborhood Statistical Area (NSA).

Inputs : neighborhood_gdb/...gdb (layer Neighborhood_NSA, EPSG:2248)
         data/interim/requests_located.parquet
         reference/nsa_id_map.csv (created on first run; committed to git)
Outputs: data/interim/neighborhoods.parquet  (GeoParquet, EPSG:4326, one row per NSA)
         data/interim/requests_final.parquet (requests_located + nsa_id)

nsa_id: a small integer key for each neighborhood. On the first run ids are
1..N in alphabetical Name order. They are saved to reference/nsa_id_map.csv,
so later runs (or a future boundary update) keep the same ids. A new Name gets
the next unused number, and ids are never reused.

Join rule: a point gets the NSA polygon it falls in, boundary included
(ST_Intersects). The polygons do not overlap, so a point can only match two
NSAs if it lies exactly on a shared border; then the lowest nsa_id wins.
Points outside every NSA (e.g. Baltimore County addresses) get nsa_id NULL.

Usage:
    .venv/Scripts/python pipeline/spatial_join.py
"""

from pathlib import Path

import duckdb
import geopandas as gpd
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
GDB_PATH = ROOT / "neighborhood_gdb" / "_ags_data66ACD992124244559E776E60316275B6.gdb"
GDB_LAYER = "Neighborhood_NSA"
MAP_PATH = ROOT / "reference" / "nsa_id_map.csv"
REQ_PATH = ROOT / "data" / "interim" / "requests_located.parquet"
OUT_NSA = ROOT / "data" / "interim" / "neighborhoods.parquet"
OUT_REQ = ROOT / "data" / "interim" / "requests_final.parquet"

# Rough box around Baltimore City, used to catch a bad reprojection.
BBOX = {"lon": (-76.8, -76.5), "lat": (39.15, 39.4)}


def load_neighborhoods() -> gpd.GeoDataFrame:
    gdf = gpd.read_file(GDB_PATH, layer=GDB_LAYER)
    if gdf.crs is None or gdf.crs.to_epsg() != 2248:
        raise RuntimeError(f"expected EPSG:2248, got {gdf.crs}")
    if not gdf["Name"].is_unique:
        raise RuntimeError("NSA Name is not unique")
    gdf = gdf.to_crs(epsg=4326)

    minx, miny, maxx, maxy = gdf.total_bounds
    if not (BBOX["lon"][0] < minx < maxx < BBOX["lon"][1] and BBOX["lat"][0] < miny < maxy < BBOX["lat"][1]):
        raise RuntimeError(f"reprojected bounds look wrong: {gdf.total_bounds}")
    return gdf


def assign_ids(names: pd.Series, global_ids: pd.Series) -> pd.DataFrame:
    """Return the id map (name, global_id, nsa_id), extending the saved one if needed."""
    if MAP_PATH.exists():
        id_map = pd.read_csv(MAP_PATH, dtype={"name": str, "global_id": str, "nsa_id": int})
    else:
        id_map = pd.DataFrame({"name": pd.Series(dtype=str), "global_id": pd.Series(dtype=str),
                               "nsa_id": pd.Series(dtype=int)})

    current = pd.DataFrame({"name": names, "global_id": global_ids})
    new = current[~current["name"].isin(id_map["name"])].sort_values("name")
    if len(new):
        start = int(id_map["nsa_id"].max()) + 1 if len(id_map) else 1
        new = new.assign(nsa_id=range(start, start + len(new)))
        id_map = pd.concat([id_map, new], ignore_index=True)
        MAP_PATH.parent.mkdir(exist_ok=True)
        id_map.to_csv(MAP_PATH, index=False)
        print(f"id map      : added {len(new)} new names -> {MAP_PATH.relative_to(ROOT)}")
    else:
        print(f"id map      : reused {MAP_PATH.relative_to(ROOT)} (no new names)")
    return id_map


def build_neighborhoods() -> gpd.GeoDataFrame:
    gdf = load_neighborhoods()
    id_map = assign_ids(gdf["Name"], gdf["GlobalID"])
    gdf = gdf.merge(id_map[["name", "nsa_id"]], left_on="Name", right_on="name", how="left").drop(columns="name")
    # Shape_Length/Shape_Area are in the old CRS's feet; keep them (they are real
    # measurements) but rename so their unit is clear. OBJECTID is internal to the GDB.
    gdf = gdf.drop(columns=["OBJECTID"], errors="ignore").rename(
        columns={"Shape_Length": "shape_length_ft", "Shape_Area": "shape_area_sqft"}
    )
    cols = ["nsa_id", "Name", "GlobalID"] + [c for c in gdf.columns if c not in ("nsa_id", "Name", "GlobalID")]
    gdf = gdf[cols].sort_values("nsa_id").reset_index(drop=True)

    tmp = OUT_NSA.with_suffix(".parquet.tmp")
    gdf.to_parquet(tmp, index=False)
    tmp.replace(OUT_NSA)
    print(f"neighborhoods: {len(gdf):,} -> {OUT_NSA.relative_to(ROOT)}")
    return gdf


def join_requests(gdf: gpd.GeoDataFrame) -> None:
    con = duckdb.connect()
    con.sql("SET TimeZone = 'UTC'")
    con.sql("INSTALL spatial; LOAD spatial")

    polys = pd.DataFrame({"nsa_id": gdf["nsa_id"], "wkb": gdf.geometry.to_wkb()})
    con.register("polys_df", polys)
    con.sql("CREATE TABLE polys AS SELECT nsa_id, ST_GeomFromWKB(wkb) AS geom FROM polys_df")

    # Join each distinct coordinate once (far fewer than 10.8M rows), then attach to requests.
    con.sql(f"""
        CREATE TABLE pts AS
        SELECT DISTINCT longitude, latitude, ST_Point(longitude, latitude) AS geom
        FROM '{REQ_PATH.as_posix()}'
    """)
    con.sql("""
        CREATE TABLE pt_matches AS
        SELECT p.longitude, p.latitude, n.nsa_id
        FROM pts p JOIN polys n ON ST_Intersects(n.geom, p.geom)
    """)
    n_ties = con.sql("""
        SELECT count(*) FROM (SELECT longitude, latitude FROM pt_matches GROUP BY ALL HAVING count(*) > 1)
    """).fetchall()[0][0]
    con.sql("""
        CREATE TABLE pt_nsa AS
        SELECT longitude, latitude, min(nsa_id) AS nsa_id FROM pt_matches GROUP BY ALL
    """)

    tmp = OUT_REQ.with_suffix(".parquet.tmp")
    con.sql(f"""
        COPY (
            SELECT r.*, m.nsa_id
            FROM '{REQ_PATH.as_posix()}' r
            LEFT JOIN pt_nsa m USING (longitude, latitude)
        ) TO '{tmp.as_posix()}' (FORMAT parquet, COMPRESSION zstd)
    """)
    tmp.replace(OUT_REQ)

    n_in = con.sql(f"SELECT count(*) FROM '{REQ_PATH.as_posix()}'").fetchall()[0][0]
    n_out, n_null, n_keys = con.sql(f"""
        SELECT count(*), count(*) FILTER (nsa_id IS NULL), count(DISTINCT service_request_num)
        FROM '{OUT_REQ.as_posix()}'
    """).fetchall()[0]
    n_pts = con.sql("SELECT count(*) FROM pts").fetchall()[0][0]

    print(f"distinct pts: {n_pts:,} ({n_ties:,} on a border between two NSAs -> lowest nsa_id)")
    print(f"requests    : {n_out:,} -> {OUT_REQ.relative_to(ROOT)}")
    print(f"  in an NSA : {n_out - n_null:,} ({(n_out - n_null) / n_out:.2%})")
    print(f"  no NSA    : {n_null:,}")
    if n_out != n_in or n_keys != n_out:
        raise RuntimeError(f"row count changed: in {n_in:,}, out {n_out:,}, unique keys {n_keys:,}")
    print("row count   : matches input, service_request_num still unique")


def main() -> None:
    gdf = build_neighborhoods()
    join_requests(gdf)


if __name__ == "__main__":
    main()
