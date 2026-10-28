"""Validate watched properties and exactly applied T1/T2 coordinates, outside timing."""
import math

from regional_benchmark.legacy_batch import prepare_active_row
from regional_benchmark.engine import stream_snapshot


def same(a, b):
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, (int, float)) and not isinstance(a, bool):
        try:
            return math.isclose(float(a), float(b), rel_tol=1e-12, abs_tol=1e-10)
        except (TypeError, ValueError):
            return False
    return str(a) == str(b)


def verify_baseline(conn, session, region, size):
    expected = {str(r["nf_id"]) for rows in stream_snapshot(conn, "2024", size, region)
                for r in rows}
    actual = set(session.run("MATCH (p:POI) RETURN toString(p.nf_id) AS id").value("id"))
    if expected != actual:
        raise AssertionError(f"T1 baseline IDs mismatch: missing={len(expected-actual)} "
                             f"extra={len(actual-expected)}")



def stream_expected_target(conn, region, method, size):
    """Stream T2 rows and the location expected in the resulting graph.

    Batch: all T2 coordinates.
    Incremental: updated/appeared use T2; unchanged retain T1 coordinates.
    Deleted POIs are checked separately as tombstones.
    """
    if method == "batch":
        for rows in stream_snapshot(conn, "2026", size, region):
            for row in rows:
                row["expected_longitude"] = row["longitude"]
                row["expected_latitude"] = row["latitude"]
            yield rows
        return

    import time

    prefix = 2 if region == "11" else 5
    query = f"""
       WITH changed AS (
           SELECT DISTINCT nf_id FROM mart.benchmark_inc_poi
           WHERE change_type IN ('appear', 'updated')
       )
       SELECT t2.*,
              CASE WHEN changed.nf_id IS NOT NULL THEN t2.longitude
                   ELSE t1.longitude END AS expected_longitude,
              CASE WHEN changed.nf_id IS NOT NULL THEN t2.latitude
                   ELSE t1.latitude END AS expected_latitude
       FROM mart.poi AS t2
       LEFT JOIN changed ON changed.nf_id = t2.nf_id
       LEFT JOIN mart.poi AS t1 ON t1.nf_id = t2.nf_id
                              AND t1.snapshot = '2024'
       WHERE t2.snapshot = '2026'
         AND left(t2.adm_cd,{prefix}) = %s
       ORDER BY t2.nf_id
    """
    with conn.cursor(name=f"rg_verify_{time.monotonic_ns()}") as cursor:
        cursor.execute(query, (region,))
        while True:
            rows = cursor.fetchmany(size)
            if not rows:
                break
            yield rows
    conn.commit()

def verify(conn, session, region, method, size):
    """Validate IDs, watched properties, WGS84 point and one matching IN edge."""
    expected_ids = set()
    for rows in stream_expected_target(conn, region, method, size):
        ids = [str(row["nf_id"]) for row in rows]
        expected_ids.update(ids)
        results = session.run("""
          MATCH (p:POI) WHERE p.nf_id IN $ids
          OPTIONAL MATCH (p)-[r:IN]->(a:adm_dong)
          RETURN toString(p.nf_id) AS id, properties(p) AS props,
                 p.location AS loc, count(r) AS rels, collect(a.adm_cd) AS codes
        """, ids=ids)
        graph = {r["id"]:r for r in results}
        if set(graph) != set(ids):
            raise AssertionError(f"Target node ID mismatch in {region}")
        for row in rows:
            entry = graph[str(row["nf_id"])]
            expected = prepare_active_row(row)
            # Unwatched attributes may differ by source version and do not
            # trigger an incremental update. Only compare the declared watch set.
            for key in ("poi_nm", "poi_cl_dc", "adm_cd"):
                if not same(entry["props"].get(key), expected["properties"].get(key)):
                    raise AssertionError(f"Watched property mismatch nf_id={row['nf_id']}, key={key}")
            point = entry["loc"]
            if point is None:
                raise AssertionError(f"Missing location nf_id={row['nf_id']}")
            if row["expected_latitude"] is None or row["expected_longitude"] is None:
                raise AssertionError(f"Missing expected baseline coordinates for nf_id={row['nf_id']}")
            lat1, lon1 = math.radians(float(row["expected_latitude"])), math.radians(float(row["expected_longitude"]))
            lat2, lon2 = math.radians(point.latitude), math.radians(point.longitude)
            term = math.sin((lat1-lat2)/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin((lon1-lon2)/2)**2
            meters = 2*6371008.8*math.asin(min(1.0, math.sqrt(max(term,0.0))))
            if meters > 0.01:
                raise AssertionError(f"Position mismatch nf_id={row['nf_id']} ({meters:.4f}m)")
            if entry["rels"] != 1 or [str(x) for x in entry["codes"]] != [expected["adm_cd"]]:
                raise AssertionError(f"IN relation mismatch nf_id={row['nf_id']}")
    active = set(session.run("MATCH (p:POI) RETURN toString(p.nf_id) AS id").value("id"))
    if active != expected_ids:
        raise AssertionError(f"Active POI set mismatch missing={len(expected_ids-active)} "
                             f"extra={len(active-expected_ids)}")
    if method == "batch":
        if session.run("MATCH (p:POITombstone) RETURN count(p) AS n").single()["n"]:
            raise AssertionError("Batch left tombstones")
    else:
        rows = conn.execute("""SELECT nf_id::text AS id FROM mart.benchmark_inc_poi
                   WHERE change_type='disappear'""").fetchall()
        expected_tombstone_ids = {r["id"] for r in rows}
        conn.commit()
        actual_ids = set(session.run("MATCH (p:POITombstone) RETURN toString(p.nf_id) AS id").value("id"))
        if actual_ids != expected_tombstone_ids:
            raise AssertionError(f"Tombstone IDs mismatch: missing={len(expected_tombstone_ids-actual_ids)}, extra={len(actual_ids-expected_tombstone_ids)}")
        edge_count = session.run("""MATCH (:POITombstone)-[r:IN]-(:adm_dong)
                                    RETURN count(r) AS n""").single()["n"]
        if edge_count:
            raise AssertionError("Tombstones have active IN edges")
    return len(expected_ids)
