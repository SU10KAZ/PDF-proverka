#!/usr/bin/env python3
"""Построить только диагностический отчёт кандидатов листов."""
from __future__ import annotations
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.services.stage_comparison.sheet_matching_diagnostic import run_comparison_diagnostic

parser = argparse.ArgumentParser()
parser.add_argument("comparison_dir", type=Path)
args = parser.parse_args()
diagnostic, json_path, md_path = run_comparison_diagnostic(args.comparison_dir)
print(f"V2 pages: {len(diagnostic['stage_1_to_stage_2'])}; V3 pages: {len(diagnostic['stage_2_to_stage_1'])}")
print(json_path)
print(md_path)
