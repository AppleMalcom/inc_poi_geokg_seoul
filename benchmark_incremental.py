"""Regional benchmark entry point (incremental).

For balanced paired experiments, run regional_benchmark_runner.py --method both.
This wrapper runs only incremental, with one warmup and two measured executions.
"""
from regional_benchmark_runner import main

if __name__ == "__main__":
    main("incremental")
