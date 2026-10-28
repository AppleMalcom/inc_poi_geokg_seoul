"""Execute balanced 6-run district benchmark: two warmups and four measurements."""
import argparse
import csv
import os
import statistics
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
    scope = p.add_mutually_exclusive_group()
    scope.add_argument("--region", help="Five-digit Seoul sigungu code; omit to run all 25")
    scope.add_argument("--all-seoul", action="store_true", help="Experiment 1: Seoul-wide POIs in one scope")
    p.add_argument("--pilot", action="store_true", help="One measured Batch and one Incremental; no warmups")
    p.add_argument("--experiment1-three", action="store_true",
                   help="Experiment 1: three measured Inc/Batch pairs (previous pilot treated as warmup)")
    p.add_argument("--method", choices=["batch", "incremental", "both"],
                   default=default_method)
    p.add_argument("--batch-size", type=int, default=1000)
    p.add_argument("--output", default=str(ROOT / "results" / f"regional_benchmark_{datetime.now():%Y%m%d_%H%M%S}.csv"))
    a = p.parse_args()
    if a.batch_size <= 0 or (a.region and (not a.region.isdigit() or len(a.region) != 5)):
        p.error("Invalid batch size or sigungu code")
    if a.experiment1_three and (not a.all_seoul or a.pilot or a.method != "both"):
        p.error("--experiment1-three requires --all-seoul, --method both, and no --pilot")
    if a.apply and not a.confirm_destructive_benchmark:
        p.error("--apply requires --confirm-destructive-benchmark")
    return a


def main(default_method="both"):
    a = parse(default_method)
    with psycopg.connect(host=os.getenv("PG_HOST"), port=os.getenv("PG_PORT"),
                        dbname=os.getenv("PG_DATABASE"), user=os.getenv("PG_USER"),
                        password=os.getenv("PG_PASSWORD"), row_factory=dict_row) as pg:
        districts = ([{"code":"11", "name":"서울특별시"}] if a.all_seoul else delta.districts(pg))
        if a.region:
            districts = [d for d in districts if d["code"] == a.region]
            if not districts:
                raise ValueError(f"No district: {a.region}")
        for d in districts:
            n1 = delta.count_snapshot(pg, d["code"], "2024")
            n2 = delta.count_snapshot(pg, d["code"], "2026")
            print(f"{d['name']} ({d['code']}): 2024={n1}, 2026={n2}", flush=True)
            if n1 == 0 or n2 == 0:
                raise RuntimeError(f"Zero snapshot count for {d['code']}; refusing to benchmark. Verify geographic scope filter.")
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
        print("[CONFIG] Geometry change threshold: 1.0m; scope=" +
              ("all Seoul" if a.all_seoul else "district"), flush=True)
        with GraphDatabase.driver(os.getenv("NEO4J_URI"),
                  auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD"))) as driver:
            driver.verify_connectivity()
            with driver.session(database=os.getenv("NEO4J_DATABASE")) as session:
                for i, d in enumerate(districts):
                    code, name = d["code"], d["name"]
                    t1_count = delta.count_snapshot(pg, code, "2024")
                    t2_count = delta.count_snapshot(pg, code, "2026")
                    print(f"  [PREFLIGHT] {name}: generating 1m delta statistics...", flush=True)
                    _, region_counts = delta.generate(pg, code)  # statistics only, outside timing
                    print(f"  [PREFLIGHT] delta={region_counts}", flush=True)
                    unchanged = t1_count - region_counts.get("updated", 0) - region_counts.get("disappear", 0)
                    universe = t1_count + region_counts.get("appear", 0)
                    change_ratio = (sum(region_counts.values()) / universe) if universe else 0.0
                    schedule = (["incremental", "batch"]*3 if a.experiment1_three
                                else [ "batch", "incremental" ] if a.pilot and a.method == "both"
                                else [a.method] if a.pilot
                                else order(i) if a.method == "both" else [a.method]*3)
                    summary_times = {"batch": [], "incremental": []}
                    print(f"  [SCHEDULE] {schedule}", flush=True)
                    for position, method in enumerate(schedule, 1):
                        role = ("measure" if a.pilot or a.experiment1_three else
                                "warmup" if position <= (2 if a.method=="both" else 1) else "measure")
                        row = {"region_code":code,"region_name":name,"position":position,
                               "round": (position+1)//2 if a.method=="both" else position,
                               "method":method, "role":role, "t1_count":t1_count,
                               "t2_count":t2_count, "validation_passed":False,
                               "unchanged_count":unchanged, "change_ratio":change_ratio,
                               **{kind+"_count":region_counts.get(kind,0) for kind in
                                  ("appear","updated","disappear")}}
                        print(f"  [RUN] {position}/{len(schedule)} {method} {role}: resetting baseline", flush=True)
                        try:
                            engine.reset(pg, session, code, a.batch_size)
                            validation.verify_baseline(pg, session, code, a.batch_size)
                            print(f"  [MEASURE] {method} starting", flush=True)
                            if method == "batch":
                                times, counters = engine.rebuild(pg, session, code, a.batch_size)
                                counts = {}
                            else:
                                detect_sec, counts = delta.generate(pg, code)
                                print(f"  [DETECT] {detect_sec:.3f}s; changes={counts}", flush=True)
                                times, counters = engine.apply_delta(pg, session, a.batch_size)
                                times["detect_sec"] = detect_sec
                                times["total_sec"] = times["apply_sec"] + detect_sec
                            row.update(times)
                            row.update(counters)
                            if method == "incremental" and counts != region_counts:
                                raise AssertionError(f"Delta counts changed: {counts} != {region_counts}")
                            print(f"  [VERIFY] apply={times['apply_sec']:.3f}s, checking graph...", flush=True)
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
                        if role == "measure" and row["validation_passed"]:
                            summary_times[method].append(times["total_sec"])
                    if a.experiment1_three:
                        summary_path = Path(a.output).with_name(
                            Path(a.output).stem + "_summary.csv"
                        )
                        summary_path.parent.mkdir(parents=True, exist_ok=True)
                        with summary_path.open("w", newline="", encoding="utf-8-sig") as handle:
                            writer = csv.DictWriter(handle, fieldnames=[
                                "region_code", "method", "n", "mean_sec",
                                "sample_stddev_sec", "min_sec", "max_sec",
                            ])
                            writer.writeheader()
                            for measured_method in ("incremental", "batch"):
                                values = summary_times[measured_method]
                                if len(values) != 3:
                                    raise AssertionError(
                                        f"Expected 3 valid {measured_method} measurements, got {len(values)}"
                                    )
                                mean = statistics.mean(values)
                                sd = statistics.stdev(values)
                                writer.writerow({
                                    "region_code": code, "method": measured_method,
                                    "n": len(values), "mean_sec": f"{mean:.6f}",
                                    "sample_stddev_sec": f"{sd:.6f}",
                                    "min_sec": f"{min(values):.6f}",
                                    "max_sec": f"{max(values):.6f}",
                                })
                                print(
                                    f"  [SUMMARY] {measured_method}: n=3, "
                                    f"mean={mean:.3f}s, sample SD={sd:.3f}s; "
                                    f"runs={[round(v, 3) for v in values]}",
                                    flush=True,
                                )
                        print(f"  [SUMMARY] Saved: {summary_path}", flush=True)
    print(f"Raw results: {a.output}")


if __name__ == "__main__":
    main()
