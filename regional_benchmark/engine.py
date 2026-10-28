"""Shared region-scoped Neo4j operations. Imports original Cypher implementations."""
import time

from regional_benchmark import legacy_batch as batch
from regional_benchmark import legacy_incremental as inc


def stream_snapshot(conn, snapshot, batch_size, region):
    with conn.cursor(name=f"rg_snapshot_{snapshot}_{time.monotonic_ns()}") as c:
        c.execute("""SELECT * FROM mart.poi WHERE snapshot=%s
                     AND left(adm_cd,5)=%s ORDER BY nf_id""", (snapshot, region))
        while True:
            rows = c.fetchmany(batch_size)
            if not rows:
                break
            yield rows
    conn.commit()


def stream_delta(conn, change_type, batch_size):
    with conn.cursor(name=f"rg_delta_{time.monotonic_ns()}") as c:
        c.execute("""SELECT * FROM mart.benchmark_inc_poi
                     WHERE change_type=%s ORDER BY nf_id""", (change_type,))
        while True:
            rows = c.fetchmany(batch_size)
            if not rows:
                break
            yield rows
    conn.commit()


def reset(conn, session, region, size):
    started = time.perf_counter()
    print(f"  [RESET] {region}: deleting prior POIs...", flush=True)
    batch.delete_all_poi_nodes(session, size)
    print("  [RESET] loading 2024 baseline...", flush=True)
    loaded = 0
    for rows in stream_snapshot(conn, "2024", size, region):
        session.execute_write(batch.rebuild_upsert_batch,
                              [batch.prepare_active_row(row) for row in rows])
        loaded += len(rows)
        if loaded % 10000 < len(rows):
            print(f"  [RESET] loaded {loaded:,} rows", flush=True)
    print(f"  [RESET] completed {loaded:,} rows in {time.perf_counter()-started:.1f}s (excluded)", flush=True)


def rebuild(conn, session, region, size):
    acc = batch.new_counter_accumulator()
    t0 = time.perf_counter()
    deleted = batch.delete_all_poi_nodes(session, size)
    for k, v in deleted.items():
        acc[k] += v
    delete_sec = time.perf_counter() - t0
    for rows in stream_snapshot(conn, "2026", size, region):
        for counters in session.execute_write(batch.rebuild_upsert_batch,
                             [batch.prepare_active_row(row) for row in rows]):
            batch.accumulate(acc, counters)
    total = time.perf_counter()-t0
    return {"total_sec":total,"apply_sec":total,
            "delete_sec":delete_sec,"load_sec":total-delete_sec,
            "detect_sec":0.0}, acc


def apply_delta(conn, session, size):
    acc = inc.new_counter_accumulator()
    times = {}
    started = time.perf_counter()
    t = time.perf_counter()
    for rows in stream_delta(conn, "disappear", size):
        counters = session.execute_write(inc.tombstone_batch,
                                         [str(row["nf_id"]) for row in rows])
        inc.accumulate(acc, counters)
    times["disappear_sec"] = time.perf_counter()-t
    for change_type in ("appear", "updated"):
        t = time.perf_counter()
        for rows in stream_delta(conn, change_type, size):
            for counters in session.execute_write(inc.upsert_batch,
                              [inc.prepare_active_row(row) for row in rows]):
                inc.accumulate(acc, counters)
        times[change_type+"_sec"] = time.perf_counter()-t
    apply_sec = time.perf_counter()-started
    times["apply_sec"] = apply_sec
    return times, acc
