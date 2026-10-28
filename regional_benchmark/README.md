## Experiment 2: 25 district automated runs

First verify Gangnam with one Batch and one Incremental run (excluded from full experiment):

```powershell
python regional_benchmark_runner.py --region 11230 --pilot --apply --confirm-destructive-benchmark
```

Full Experiment 2 runs 8 times per district: two warmups and 3 valid measurements per method. The district order alternates the starting method. Use a fixed output path so the experiment can resume:

```powershell
python regional_benchmark_runner.py --experiment2 --output results/experiment_2/regional_raw.csv --apply --confirm-destructive-benchmark
```

After interruption or an exception, use the same path and --resume to skip already validated runs:

```powershell
python regional_benchmark_runner.py --experiment2 --output results/experiment_2/regional_raw.csv --resume --apply --confirm-destructive-benchmark
```

The summary is written to `results/experiment_2/regional_raw_summary.csv` after each completed district. Sample standard deviation uses n-1. Do not reuse a pilot or Experiment 1 CSV. Keep the same snapshot, threshold, Neo4j environment and batch-size when resuming.

# Seoul GeoKG benchmark: Experiments 1 and 2

## Experiment 1: all Seoul (1m geometry change tolerance)

Read-only preflight:

```powershell
python regional_benchmark_runner.py --all-seoul
```

First do a pilot with one Batch and one Incremental, no warmups:

```powershell
python regional_benchmark_runner.py --all-seoul --pilot --apply --confirm-destructive-benchmark
```

Only after pilot passes, run six citywide runs (warmup once per method, two measured runs per method):

```powershell
python regional_benchmark_runner.py --all-seoul --apply --confirm-destructive-benchmark
```

Seoul-wide comparisons use `left(adm_cd,2)='11'` for both 2024 and 2026. Moving across Seoul district boundaries counts as a shared POI, not two unrelated POIs. This is different from Experiment 2. The program deletes ALL POI/Tombstone graph nodes before each run, retaining administrative nodes. Baseline reconstruction of about 1.9 million POIs is outside timing but may be slow. Validation is also outside timing. The final graph remains at T2, not T1. Do not launch simultaneous benchmark processes because they use the same delta table and Neo4j graph.

## Experiment 2: 25 districts

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
- Every run verifies T2 active IDs, mapped properties, location (1 m tolerance), `IN` edges, and tombstone counts. Invalid runs are logged as failed and stop the benchmark.
- GeoKG index and administrative hierarchy should already exist and be identical for both methods.
- No CPU, memory or storage benchmark is performed.
- Raw results are appended to `results/regional_benchmark.csv`; do not accidentally mix different experiment configurations in the same CSV.

## Caveats

The delta detector uses the **original four watched fields**: `poi_nm`, `poi_cl_dc`, `adm_cd`, and `geom` with a 1 m displacement threshold. `nf_id` is the identity key. `adm_nm` is not a watched field because `adm_cd` is the administrative area key. Because unwatched properties may differ between snapshots, Incremental does not guarantee full-property equivalence to a Batch rebuild. Validation checks the active IDs, watched properties, location (1 m), and `IN` relationships; any full-property discrepancy must be reported separately, not described as total graph equality.

2024 and 2026 data originate from different providers; provider/schema differences may affect Updated counts. With just two measured runs per method per region, per-region means and standard deviations are descriptive only. Running all 25 districts does not isolate the causal effect of change ratio.

Since a delta snapshot table is shared, **do not run multiple benchmark processes concurrently**.
