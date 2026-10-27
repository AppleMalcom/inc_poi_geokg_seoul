"""Execute balanced 6-run district benchmark: two warmups and four measurements."""
import argparse
import csv
import os
from datetime import datetime
from pathlib import Path

import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv
from neo4j import GraphDatabase

from regional_benchmark import delta, engine, validation

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")
COUNTERS = ("nodes_created", "nodes_deleted", "relationships_created",
            "relationships_deleted", "labels_added", "labels_removed", "properties_set")
FIELDS = ("region_code", "region_name", "position", "round", "method", "role",
          "t1_count", "t2_count", "appear_count", "updated_count", "disappear_count",
          "unchanged_count", "change_ratio",
          "detect_sec", "apply_sec", "total_sec", "delete_sec", "load_sec",
          "appear_sec", "updated_sec", "disappear_sec",
          *COUNTERS, "validation_passed", "error")


def order(idx):
    return (["batch", "incremental", "incremental", "batch", "batch", "incremental"]
            if idx % 2 == 0 else
            ["incremental", "batch", "batch", "incremental", "incremental", "batch"])


def record(path, row):
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists() and path.stat().st_size > 0
    with path.open("a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if not exists:
            writer.writeheader()
        writer.writerow({key: row.get(key, "") for key in FIELDS})


def parse(default_method):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--apply", action="store_true")
    p.add_argument("--confirm-destructive-benchmark", action="store_true",
                   help="Acknowledge that benchmark resets remove all POI/Tombstone nodes")
    p.add_argument("--region", help="Five-digit Seoul sigungu code; omit to run all 25")
    p.add_argument("--method", choices=["batch", "incremental", "both"],
                   default=default_method)
    p.add_argument("--batch-size", type=int, default=1000)
    p.add_argument("--output", default=str(ROOT / "results" / f"regional_benchmark_{datetime.now():%Y%m%d_%H%M%S}.csv"))
    a = p.parse_args()
    if a.batch_size <= 0 or (a.region and (not a.region.isdigit() or len(a.region) != 5)):
        p.error("Invalid batch size or sigungu code")
    if a.apply and not a.confirm_destructive_benchmark:
        p.error("--apply requires --confirm-destructive-benchmark")
    return a


def main(default_method="both"):
    a = parse(default_method)
    with psycopg.connect(host=os.getenv("PG_HOST"), port=os.getenv("PG_PORT"),
                        dbname=os.getenv("PG_DATABASE"), user=os.getenv("PG_USER"),
                        password=os.getenv("PG_PASSWORD"), row_factory=dict_row) as pg:
        districts = delta.districts(pg)
        if a.region:
            districts = [d for d in districts if d["code"] == a.region]
            if not districts:
                raise ValueError(f"No district: {a.region}")
        for d in districts:
            n1 = delta.count_snapshot(pg, d["code"], "2024")
            n2 = delta.count_snapshot(pg, d["code"], "2026")
            print(f"{d['name']} ({d['code']}): 2024={n1}, 2026={n2}", flush=True)
        if not a.apply:
            print("Read-only inspection complete. Use --apply --confirm-destructive-benchmark "
                  "ONLY with a dedicated benchmark Neo4j database.")
            return
        unmapped = pg.execute("""SELECT count(*) AS n FROM mart.poi
                    WHERE snapshot IN ('2024','2026') AND
                    (adm_cd IS NULL OR longitude IS NULL OR latitude IS NULL)""").fetchone()["n"]
        pg.commit()
        if unmapped:
            raise RuntimeError(f"Source has {unmapped} unmapped/missing-coordinate POIs")
        delta.initialize(pg)
        with GraphDatabase.driver(os.getenv("NEO4J_URI"),
                  auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD"))) as driver:
            driver.verify_connectivity()
            with driver.session(database=os.getenv("NEO4J_DATABASE")) as session:
                for i, d in enumerate(districts):
                    code, name = d["code"], d["name"]
                    t1_count = delta.count_snapshot(pg, code, "2024")
                    t2_count = delta.count_snapshot(pg, code, "2026")
                    _, region_counts = delta.generate(pg, code)  # statistics only, outside timing
                    unchanged = t1_count - region_counts.get("updated", 0) - region_counts.get("disappear", 0)
                    universe = t1_count + region_counts.get("appear", 0)
                    change_ratio = (sum(region_counts.values()) / universe) if universe else 0.0
                    schedule = order(i) if a.method == "both" else [a.method]*3
                    for position, method in enumerate(schedule, 1):
                        role = "warmup" if position <= (2 if a.method=="both" else 1) else "measure"
                        row = {"region_code":code,"region_name":name,"position":position,
                               "round": (position+1)//2 if a.method=="both" else position,
                               "method":method, "role":role, "t1_count":t1_count,
                               "t2_count":t2_count, "validation_passed":False,
                               "unchanged_count":unchanged, "change_ratio":change_ratio,
                               **{kind+"_count":region_counts.get(kind,0) for kind in
                                  ("appear","updated","disappear")}}
                        try:
                            engine.reset(pg, session, code, a.batch_size)
                            validation.verify_baseline(pg, session, code, a.batch_size)
                            if method == "batch":
                                times, counters = engine.rebuild(pg, session, code, a.batch_size)
                                counts = {}
                            else:
                                detect_sec, counts = delta.generate(pg, code)
                                times, counters = engine.apply_delta(pg, session, a.batch_size)
                                times["detect_sec"] = detect_sec
                                times["total_sec"] = times["apply_sec"] + detect_sec
                            row.update(times)
                            row.update(counters)
                            row.update({kind+"_count":counts.get(kind,0) for kind in
                                        ("appear","updated","disappear")})
                            validation.verify(pg, session, code, method, a.batch_size)
                            row["validation_passed"] = True
                            print(f"  {position}/{len(schedule)} {method} {role}: "
                                  f"{times['total_sec']:.3f}s PASS", flush=True)
                        except Exception as exc:
                            pg.rollback()
                            row["error"] = f"{type(exc).__name__}: {exc}"
                            record(Path(a.output), row)
                            raise
                        record(Path(a.output), row)
    print(f"Raw results: {a.output}")


if __name__ == "__main__":
    main()
