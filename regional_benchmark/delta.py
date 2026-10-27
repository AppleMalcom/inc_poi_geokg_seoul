"""Region-specific POI delta detection for benchmark runs."""
import time

DETECT_SQL = """
WITH t1 AS (
 SELECT * FROM mart.poi WHERE snapshot='2024' AND left(adm_cd,5)=%s
), t2 AS (
 SELECT * FROM mart.poi WHERE snapshot='2026' AND left(adm_cd,5)=%s
), changes AS (
 SELECT coalesce(t1.nf_id,t2.nf_id) AS nf_id,
 CASE WHEN t1.nf_id IS NULL THEN 'appear'
      WHEN t2.nf_id IS NULL THEN 'disappear'
      WHEN (to_jsonb(t1) - 'snapshot' - 'geom' - 'longitude' - 'latitude')
            IS DISTINCT FROM
           (to_jsonb(t2) - 'snapshot' - 'geom' - 'longitude' - 'latitude')
        OR ((t1.geom IS NULL) IS DISTINCT FROM (t2.geom IS NULL))
        OR (t1.geom IS NOT NULL AND t2.geom IS NOT NULL
            AND NOT ST_DWithin(t1.geom,t2.geom,0.1))
      THEN 'updated'
      ELSE 'no_change' END AS change_type
 FROM t1 FULL JOIN t2 ON t1.nf_id=t2.nf_id
)
INSERT INTO mart.benchmark_inc_poi
SELECT p.*, c.change_type FROM changes c
JOIN mart.poi p ON p.nf_id=c.nf_id
 AND p.snapshot=(CASE WHEN c.change_type='disappear' THEN '2024' ELSE '2026' END)
WHERE c.change_type <> 'no_change'
"""


def initialize(conn):
    """One-time DDL, not part of measured change detection."""
    conn.execute("""CREATE TABLE IF NOT EXISTS mart.benchmark_inc_poi AS
                    SELECT p.*, NULL::text AS change_type
                    FROM mart.poi p WHERE false""")
    conn.commit()


def generate(conn, sigungu_cd):
    """Recreate region delta; measure INSERT detection/materialization only."""
    conn.execute("TRUNCATE mart.benchmark_inc_poi")
    conn.commit()
    started = time.perf_counter()
    conn.execute(DETECT_SQL, (sigungu_cd, sigungu_cd))
    conn.commit()
    elapsed = time.perf_counter() - started
    counts = conn.execute("""SELECT change_type, count(*) AS n
        FROM mart.benchmark_inc_poi GROUP BY change_type""").fetchall()
    conn.commit()
    return elapsed, {r["change_type"]: r["n"] for r in counts}


def districts(conn):
    rows = conn.execute("""SELECT sigungu_cd::text AS code,
         sigungu_nm AS name FROM mart.bnd_sigungu_pg
         WHERE left(sigungu_cd::text,2)='11' ORDER BY sigungu_cd""").fetchall()
    conn.commit()
    if len(rows) != 25:
        raise RuntimeError(f"Expected 25 Seoul districts, found {len(rows)}")
    return rows


def count_snapshot(conn, region, snapshot):
    n = conn.execute("""SELECT count(*) AS n FROM mart.poi
       WHERE snapshot=%s AND left(adm_cd,5)=%s""", (snapshot, region)).fetchone()["n"]
    conn.commit()
    return n
