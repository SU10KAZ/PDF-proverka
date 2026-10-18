#!/usr/bin/env python3
"""Prepare or run standalone whole-specification comparison (Codex Luna only)."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def main() -> int:
    from backend.app.services.version_comparison.specification_run import prepare, run

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--run-codex", action="store_true", help="Запустить gpt-6-luna через авторизацию Codex; без флага только локальная подготовка")
    parser.add_argument("--structure", action="store_true", help="Копировать ячейки MD программно; Luna определяет структуру всей спецификации")
    parser.add_argument("--parallel", type=int, default=50)
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--codex-bin")
    args = parser.parse_args()
    try:
        prepared = prepare(args.manifest.resolve(), structure=args.structure)
        result = run(prepared, args.output.resolve(), execute=args.run_codex, parallel=args.parallel,
                     timeout_s=args.timeout, binary=args.codex_bin)
    except (ValueError, OSError) as exc:
        parser.exit(2, f"Ошибка: {exc}\n")
    if args.run_codex:
        print(json.dumps({"extractions_complete": result["extractions_complete"], "execution": result["execution"],
                          "report": str(args.output / "report.md")}, ensure_ascii=False))
        return 0 if result["extractions_complete"] else 2
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
