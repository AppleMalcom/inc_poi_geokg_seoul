# Seoul 25-district Batch vs Incremental benchmark (Experiment B)

## Usage

Inspect the regions without writing:

```powershell
python regional_benchmark_runner.py --region 11230
```

Run one district (then remove `--region` for all 25):

```powershell
python regional_benchmark_runner.py --region 11230 --apply --confirm-destructive-benchmark
```

**Warning:** the benchmark resets **every** `:POI` and `:POITombstone` node in the configured Neo4j database before each run. Use an isolated benchmark Neo4j database, NOT your primary graph. The preexisting administrative nodes and their hierarchy are retained. PostgreSQL original `mart.poi` and `mart.inc_poi` are read-only; the program creates/truncates `mart.benchmark_inc_poi`.

The existing `benchmark_batch.py` and `benchmark_incremental.py` entry points are replaced with wrappers. To get the balanced paired experiment, use **`regional_benchmark_runner.py`** with the default `--method both`. Standalone wrappers produce only their respective method, with one warmup and two measured runs.

## Protocol

- Each district is independently scoped by `left(adm_cd,5)`, for T1 and T2 respectively. Moving from district A to B is therefore A/disappear and B/appear; in a citywide comparison it may be updated.
- Six runs per district: 1 Batch warmup, 1 Incremental warmup, 2 measured Batch, 2 measured Incremental. Odd/even district sequence alternates `B,I,I,B,B,I` and `I,B,B,I,I,B`.
- The same T1 graph is recreated outside timing before each method.
- Batch timing includes POI removal and full T2 load.
- Incremental timing includes SQL change detection / materialization and Neo4j delta application. Delta table truncation and one-time DDL are **excluded**; PostgreSQL detection time includes the INSERT and COMMIT. Timed Neo4j apply includes reading the delta stream.
- Every run verifies T2 active IDs, mapped properties, location (0.1 m tolerance), `IN` edges, and tombstone counts. Invalid runs are logged as failed and stop the benchmark.
- GeoKG index and administrative hierarchy should already exist and be identical for both methods.
- No CPU, memory or storage benchmark is performed.
- Raw results are appended to `results/regional_benchmark.csv`; do not accidentally mix different experiment configurations in the same CSV.

## Caveats

The delta detector compares every Neo4j-mapped source attribute (except geometry, coordinates and snapshot), and compares geometry with a 0.1-meter threshold. This **expands** the original four watched fields (`poi_nm`, `poi_cl_dc`, `adm_cd`, `geom`) to support full attribute equality against Batch. It may therefore generate a larger Updated count than the conference implementation. If the original watched-field definition must remain identical, choose either (a) scope validation to watched fields, or (b) explicitly report this expanded change definition. Do not silently compare these results with conference counts.

2024 and 2026 data originate from different providers; provider/schema differences may affect Updated counts. With just two measured runs per method per region, per-region means and standard deviations are descriptive only. Running all 25 districts does not isolate the causal effect of change ratio.

Since a delta snapshot table is shared, **do not run multiple benchmark processes concurrently**.
