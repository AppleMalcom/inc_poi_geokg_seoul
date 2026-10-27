# Seoul district benchmark

Experiment B compares Batch and Incremental updating independently across Seoul's 25 districts. Each method runs once for warmup and twice for measurement. Incremental includes PostgreSQL change detection; Batch includes full graph replacement. Each run starts from the same 2024 baseline and is validated against the 2026 target. Existing source tables `mart.poi` and `mart.inc_poi` must remain unchanged.

Development branch: `feature/regional-benchmark`.
