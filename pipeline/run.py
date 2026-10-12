"""ORCHESTRATOR: run the whole pipeline for one day, in order.

    python -m pipeline.run --date 2026-10-03
    python -m pipeline.run --date 2026-10-03 --from-step load     # resume after fixing a failure
"""
import argparse
import time
from datetime import date, timedelta
from pipeline.db import connect_warehouse

STEPS = ["extract", "transform", "load", "quality"]


def run_step(step, run_date):
    # Imported lazily: re-running only `load` shouldn't have to start Spark.
    if step == "extract":
        from pipeline.extract import extract
        extract.run(run_date)
    elif step == "transform":
        from pipeline.transform import transform
        transform.run(run_date)
    elif step == "load":
        from pipeline.load import load
        load.run(run_date)
    elif step == "quality":
        from pipeline.quality import checks
        checks.run()

def check_for_gap(run_date):
    """Refuse to run a date if the day before it was never loaded (re-running older dates is fine)."""
    with connect_warehouse() as conn:
        last = conn.execute("SELECT max(run_date) FROM dw.pipeline_runs").fetchone()[0]
    expected = run_date - timedelta(days=1)
    if last is not None and last < expected:
        missing = (expected - last).days
        raise RuntimeError(
            f"Gap detected: last loaded day is {last}, but {run_date} needs {expected} first "
            f"({missing} day(s) missing). Run the missing day(s), or pass --allow-gap.")

def run(run_date, from_step="extract", allow_gap=False):
    if not allow_gap:
        check_for_gap(run_date)
    for step in STEPS[STEPS.index(from_step):]:
        print(f"[{run_date}] {step}")
        started = time.time()
        run_step(step, run_date)          # any exception stops the loop here: later steps never run
        print(f"[{run_date}] {step} finished in {time.time() - started:.1f}s\n")
    print(f"[{run_date}] pipeline succeeded")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    parser.add_argument("--from-step", choices=STEPS, default="extract")
    parser.add_argument("--allow-gap", action="store_true", help="skip the missing-day check")
    args = parser.parse_args()
    run(args.date, args.from_step, args.allow_gap)