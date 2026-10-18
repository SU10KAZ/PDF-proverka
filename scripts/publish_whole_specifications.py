#!/usr/bin/env python3
"""Attach a completed whole-specification report to an existing comparison run."""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def main() -> None:
    from backend.app.services.version_comparison.specification_export import publish, publish_series

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path, help="report.json полного сравнения")
    parser.add_argument("object_id")
    parser.add_argument("run_id", nargs="?", help="run_id одного сравнения")
    parser.add_argument("--series-id", help="вместо одного прогона опубликовать манифест переходов серии")
    args = parser.parse_args()
    try:
        report = json.loads(args.report.read_text(encoding="utf-8"))
        if args.series_id:
            if args.run_id:
                parser.error("run_id нельзя задавать вместе с --series-id")
            paths = publish_series(args.object_id, args.series_id, report)
        else:
            if not args.run_id:
                parser.error("нужен run_id или --series-id")
            paths = [publish(args.object_id, args.run_id, report)]
    except (OSError, ValueError, KeyError, TypeError) as exc:
        parser.exit(2, f"Ошибка: {exc}\n")
    for path in paths:
        print(path)


if __name__ == "__main__":
    main()
