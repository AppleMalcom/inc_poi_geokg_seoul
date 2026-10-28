"""Read-only verification of the EXISTING Seoul-wide Incremental graph.

Run only after an incremental update has completed and before any graph reset.
Uses the current mart.benchmark_inc_poi delta table; it must be the same
1m Seoul-wide delta used for the incremental apply.
"""
import os
import time
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from neo4j import GraphDatabase
from psycopg.rows import dict_row

from regional_benchmark import validation

load_dotenv(Path(__file__).resolve().parent / ".env")

def main():
    with psycopg.connect(
        host=os.getenv("PG_HOST"), port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DATABASE"), user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"), row_factory=dict_row,
        options="-c default_transaction_read_only=on",
    ) as pg:
        rows = pg.execute("""
            SELECT change_type, count(*) AS n
            FROM mart.benchmark_inc_poi
            GROUP BY change_type
        """).fetchall()
        counts = {r["change_type"]: r["n"] for r in rows}
        print(f"[CHECK] current delta counts={counts}", flush=True)
        expected = {"appear": 784635, "disappear": 1115951, "updated": 26185}
        if counts != expected:
            raise RuntimeError(
                "Current delta table does not match the known Seoul-wide 1m "
                f"pilot delta: {counts} != {expected}; no verification run."
            )
        with GraphDatabase.driver(
            os.getenv("NEO4J_URI"),
            auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD")),
        ) as driver:
            driver.verify_connectivity()
            with driver.session(database=os.getenv("NEO4J_DATABASE")) as session:
                active = session.run("MATCH (p:POI) RETURN count(p) AS n").single()["n"]
                tomb = session.run(
                    "MATCH (p:POITombstone) RETURN count(p) AS n"
                ).single()["n"]
                print(f"[CHECK] Neo4j POI={active:,}, tombstones={tomb:,}", flush=True)
                if (active, tomb) != (1574276, 1115951):
                    raise RuntimeError(
                        "Graph does not match the expected post-incremental "
                        "POI/tombstone counts; inspect before verification."
                    )
                print("[VERIFY] Existing incremental graph: starting...", flush=True)
                start = time.perf_counter()
                validation.verify(pg, session, "11", "incremental", 1000)
                print(
                    f"[VERIFY] PASS: {time.perf_counter() - start:.3f}s "
                    "(excluded from benchmark timing)", flush=True
                )

if __name__ == "__main__":
    main()
