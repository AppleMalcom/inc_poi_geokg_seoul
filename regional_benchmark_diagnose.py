"""Read-only diagnostic for regional POI delta classification.

No Neo4j writes, no PostgreSQL table creation, deletion or inserts.
Runs the SAME classification predicates as regional_benchmark/delta.py.
"""
import argparse
import os
import time
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row

load_dotenv(Path(__file__).resolve().parent / ".env")

SQL = """
WITH t1 AS (
 SELECT * FROM mart.poi WHERE snapshot='2024' AND left(adm_cd,5)=%s
), t2 AS (
 SELECT * FROM mart.poi WHERE snapshot='2026' AND left(adm_cd,5)=%s
), changes AS (
 SELECT CASE
  WHEN t1.nf_id IS NULL THEN 'appear'
  WHEN t2.nf_id IS NULL THEN 'disappear'
  WHEN t1.poi_nm IS DISTINCT FROM t2.poi_nm
        OR t1.poi_cl_dc IS DISTINCT FROM t2.poi_cl_dc
        OR t1.adm_cd IS DISTINCT FROM t2.adm_cd
        OR ((t1.geom IS NULL) IS DISTINCT FROM (t2.geom IS NULL))
   OR (t1.geom IS NOT NULL AND t2.geom IS NOT NULL
       AND NOT ST_DWithin(t1.geom, t2.geom, 0.1))
  THEN 'updated'
  ELSE 'no_change' END AS change_type
 FROM t1 FULL JOIN t2 ON t1.nf_id=t2.nf_id
)
SELECT change_type, count(*) AS n
FROM changes GROUP BY change_type ORDER BY change_type
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--region", default="11230", help="5 digit district code")
    args = parser.parse_args()
    if len(args.region) != 5 or not args.region.isdigit():
        parser.error("--region must contain 5 digits")
    with psycopg.connect(
        host=os.getenv("PG_HOST"), port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DATABASE"), user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"), row_factory=dict_row,
        options="-c default_transaction_read_only=on"
    ) as conn:
        print(f"[DIAG] district={args.region}, comparing 2024 to 2026", flush=True)
        started = time.perf_counter()
        rows = conn.execute(SQL, (args.region, args.region)).fetchall()
        elapsed = time.perf_counter() - started
    counts = {row["change_type"]: row["n"] for row in rows}
    total = sum(counts.values())
    changed = sum(counts.get(k, 0) for k in ("appear", "updated", "disappear"))
    for kind in ("appear", "updated", "disappear", "no_change"):
        print(f"{kind:>10}: {counts.get(kind,0):,}")
    print(f"change_ratio: {changed/total:.2%}" if total else "No POIs found")
    print(f"read-only SQL classification: {elapsed:.3f}s")
    print("Note: the measured SELECT omits INSERT/COMMIT materialization costs.")


if __name__ == "__main__":
    main()
