"""Frozen offline runner reusing the original TEXT execution and replay path."""
import argparse
from pathlib import Path

from experiments.text_comparison_v1.run import run
from experiments.text_comparison_v1.common import read, file_hash
from .sections import materialize
from .relations import relate
from .facts import extract, compare


def frozen_run(root, name):
    root = Path(root)
    manifest = read(root / "reports/CANDIDATE_MANIFEST.json")
    repo = Path(manifest["repository"])
    for path, expected in manifest["code_files"].items():
        if file_hash(repo / path) != expected:
            raise ValueError("Frozen candidate changed: " + path)
    run(root / "FROZEN_PROJECT_INPUTS.json", root / name,
        materializer=materialize, relation_matcher=relate,
        extractor=extract, comparator=compare)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--root", required=True)
    p.add_argument("--name", required=True)
    a = p.parse_args()
    frozen_run(a.root, a.name)
