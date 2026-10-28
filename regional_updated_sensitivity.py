"""Read-only sensitivity of FINAL Updated counts to geometry distance threshold.

Does not write to PostgreSQL or Neo4j. Compares common nf_id within the
selected geographic scope, using the same watched attributes as delta.py.
"""
import argparse
import os
import time
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")
THRESHOLDS = (0.001, 0.01, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--all-seoul", action="store_true", help="All Seoul (default)")
    group.add_argument("--region", help="5 digit sigungu code")
    args = parser.parse_args()
    if args.region and (len(args.region) != 5 or not args.region.isdigit()):
        parser.error("--region must be a 5-digit code")
    code = args.region or "11"
    prefix_length = 5 if args.region else 2
    print(f"[SCOPE] {'district ' + code if args.region else 'all Seoul'}", flush=True)
    print("[WATCHED] poi_nm, poi_cl_dc, adm_cd, geom distance", flush=True)
    print("[THRESHOLDS] " + ", ".join(f"{x:g}m" for x in THRESHOLDS), flush=True)

    # SQL thresholds are fixed module constants, never user supplied strings.
    parts = []
    for i, threshold in enumerate(THRESHOLDS):
        parts.append(
            f"count(*) FILTER (WHERE geo_missing OR distance_m > {threshold}) AS geom_{i},\n"
            f" count(*) FILTER (WHERE attr_changed OR geo_missing OR distance_m > {threshold}) AS updated_{i},\n"
            f" count(*) FILTER (WHERE NOT attr_changed AND (geo_missing OR distance_m > {threshold})) AS geo_only_{i}"
        )
    stats = ",\n".join(parts)
    sql = f"""
    WITH t1 AS MATERIALIZED (
      SELECT nf_id, poi_nm, poi_cl_dc, adm_cd, geom
      FROM mart.poi WHERE snapshot='2024' AND left(adm_cd,{prefix_length})=%s
    ), t2 AS MATERIALIZED (
      SELECT nf_id, poi_nm, poi_cl_dc, adm_cd, geom
      FROM mart.poi WHERE snapshot='2026' AND left(adm_cd,{prefix_length})=%s
    ), paired AS MATERIALIZED (
      SELECT
        (t1.poi_nm IS DISTINCT FROM t2.poi_nm
         OR t1.poi_cl_dc IS DISTINCT FROM t2.poi_cl_dc
         OR t1.adm_cd IS DISTINCT FROM t2.adm_cd) AS attr_changed,
        ((t1.geom IS NULL) IS DISTINCT FROM (t2.geom IS NULL)) AS geo_missing,
        CASE WHEN t1.geom IS NOT NULL AND t2.geom IS NOT NULL
          THEN ST_Distance(t1.geom, t2.geom) ELSE NULL END AS distance_m
      FROM t1 INNER JOIN t2 ON t1.nf_id=t2.nf_id
    )
    SELECT count(*) AS common,
           count(*) FILTER (WHERE attr_changed) AS attr_changed,
           {stats}
    FROM paired
    """
    started = time.perf_counter()
    with psycopg.connect(
        host=os.getenv("PG_HOST"), port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DATABASE"), user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"), row_factory=dict_row,
        options="-c default_transaction_read_only=on",
    ) as conn:
        result = conn.execute(sql, (code, code)).fetchone()
    elapsed = time.perf_counter() - started
    print(f"Common nf_id: {result['common']:,}")
    print(f"Attribute/adm_cd updated (independent of distance): {result['attr_changed']:,}")
    print(f"{'threshold':>12} {'geom changed':>15} {'geo-only':>12} {'FINAL Updated':>16} {'Updated % common':>17}")
    for i, threshold in enumerate(THRESHOLDS):
        updated = result[f"updated_{i}"]
        ratio = 100.0*updated/result["common"] if result["common"] else 0
        print(f"{threshold:>11g}m {result[f'geom_{i}']:>15,} {result[f'geo_only_{i}']:>12,} {updated:>16,} {ratio:>16.3f}%")
    print(f"Read-only analysis took {elapsed:.3f}s")
    print("Note: Appear/Disappear are independent of distance within this scope.")
    print("Note: Within-district migration across boundaries is classified as appear/disappear,")
    print("      whereas Seoul-wide comparisons can classify the same nf_id as Updated.")

if __name__ == "__main__":
    main()
