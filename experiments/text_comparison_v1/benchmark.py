"""Isolated per-pair peak RSS measurement in fresh child processes."""
from pathlib import Path
import argparse
import subprocess
import sys
import time

from .common import read, write


def run(root):
    root = Path(root)
    inputs = read(root / "FROZEN_PROJECT_INPUTS.json")
    rows = []
    for pair in inputs["pairs"]:
        base = root / "isolated_performance" / pair["pair_key"]
        source = base / "inputs.json"
        write(source, {**inputs, "pairs": [pair]})
        start = time.perf_counter()
        result = subprocess.run([sys.executable, "-m", "experiments.text_comparison_v1.run", "run",
                                 "--inputs", str(source), "--output", str(base / "run")],
                                capture_output=True, text=True, check=True)
        measured = read(base / "run/performance.json")
        rows.append({"pair_key": pair["pair_key"], "child_pid_scope": "fresh process per pair",
                     "seconds_including_process_start": time.perf_counter() - start,
                     "pipeline_seconds": measured["seconds"], "peak_rss_kib": measured["process_peak_rss_kib"],
                     "artifact_bytes": measured["artifact_bytes"], "model_calls": 0, "input_tokens": 0, "output_tokens": 0})
        print(pair["source_pair_id"], round(rows[-1]["pipeline_seconds"], 3), rows[-1]["peak_rss_kib"], flush=True)
    write(root / "reports/ISOLATED_PERFORMANCE.json", {"method": "Fresh Python process for each frozen document pair; Linux ru_maxrss KiB",
                                                      "pairs": rows})


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    run(parser.parse_args().root)
