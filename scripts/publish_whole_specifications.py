#!/usr/bin/env python3
"""Attach a completed whole-specification report to an existing comparison run."""
import argparse
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def series_manifest(report: dict, input_bytes: bytes, record: dict) -> dict:
    """Split a whole-history result by explicit version IDs in its exact input."""
    if report.get("schema") != "whole-specification/1" or report.get("manifest_sha256") != hashlib.sha256(input_bytes).hexdigest():
        raise ValueError("отчёт не соответствует исходному манифесту")
    inputs = json.loads(input_bytes)
    if inputs.get("schema") != "whole-specification-input/v1":
        raise ValueError("неверный формат исходного манифеста")
    by_id = {item["id"]: item for item in report["reports"]}
    if len(by_id) != len(report["reports"]):
        raise ValueError("повтор id в результатах")
    grouped, used = {}, set()
    for item in inputs["pairs"]:
        if item["id"] in used or item["id"] not in by_id:
            raise ValueError("состав результатов не совпадает с манифестом")
        used.add(item["id"])
        key = (item["old"]["version_id"], item["new"]["version_id"])
        grouped.setdefault(key, []).append(by_id[item["id"]])
    if used != set(by_id):
        raise ValueError("в отчёте есть лишние результаты")
    expected = {(pair["a"], pair["b"]) for pair in record["pairs"]}
    if set(grouped) != expected:
        raise ValueError("манифест должен содержать все переходы выбранной серии")
    return {"schema": "whole-specification-series/1", "transitions": [
        {"run_id": pair["run_id"], "report": {**report, "reports": grouped[(pair["a"], pair["b"])]}}
        for pair in record["pairs"]]}


def main() -> None:
    from backend.app.services.version_comparison.specification_export import publish, publish_series

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path, help="report.json полного сравнения")
    parser.add_argument("object_id")
    parser.add_argument("run_id", nargs="?", help="run_id одного сравнения")
    parser.add_argument("--series-id", help="вместо одного прогона опубликовать манифест переходов серии")
    parser.add_argument("--input-manifest", type=Path, help="исходный manifest.json для публикации общего report.json всей истории")
    args = parser.parse_args()
    try:
        report = json.loads(args.report.read_text(encoding="utf-8"))
        if args.series_id:
            if args.run_id:
                parser.error("run_id нельзя задавать вместе с --series-id")
            if args.input_manifest:
                from backend.app.services.version_comparison import paths
                record = json.loads(paths.series_path(args.object_id, args.series_id).read_text(encoding="utf-8"))
                report = series_manifest(report, args.input_manifest.read_bytes(), record)
            paths = publish_series(args.object_id, args.series_id, report)
        else:
            if args.input_manifest:
                parser.error("--input-manifest требует --series-id")
            if not args.run_id:
                parser.error("нужен run_id или --series-id")
            paths = [publish(args.object_id, args.run_id, report)]
    except (OSError, ValueError, KeyError, TypeError) as exc:
        parser.exit(2, f"Ошибка: {exc}\n")
    for path in paths:
        print(path)


if __name__ == "__main__":
    main()
