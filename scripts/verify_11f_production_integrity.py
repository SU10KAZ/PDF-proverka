#!/usr/bin/env python3
"""Этап 11F — сверка целостности production-дерева ПОСЛЕ прогона.

Снимок «до» снят тем же кодом перед боевым вызовом. Сверяется каждый файл по
sha256: 11F обязан не менять production-проект ни одним байтом (§33).

Содержимое файлов никуда не выводится — только пути и отпечатки.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Optional


def snapshot(root: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        out[str(path.relative_to(root))] = digest
    return out


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="11F — целостность production")
    parser.add_argument("--root", required=True, type=Path,
                        help="корень production-версии проекта")
    parser.add_argument("--before", required=True, type=Path,
                        help="JSON снимка «до»")
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args(argv)

    before_raw = json.loads(args.before.read_text(encoding="utf-8"))
    raw_files = before_raw.get("files") if isinstance(before_raw, dict) else before_raw
    if not isinstance(raw_files, dict):
        raise SystemExit("снимок «до» не содержит карты файлов")
    # Снимок «до» хранит на файл объект {sha256, size}; принимаем и голую строку.
    before = {
        name: (value.get("sha256") if isinstance(value, dict) else str(value))
        for name, value in raw_files.items()
    }

    after = snapshot(args.root)

    added = sorted(set(after) - set(before))
    removed = sorted(set(before) - set(after))
    changed = sorted(k for k in set(before) & set(after) if before[k] != after[k])

    report = {
        "stage": "11F",
        "root": str(args.root),
        "files_before": len(before),
        "files_after": len(after),
        "added": added,
        "removed": removed,
        "changed": changed,
        "identical": not (added or removed or changed),
    }
    report["verdict"] = "PASS" if report["identical"] else "FAIL"
    args.out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in report.items()
                      if k not in ("added", "removed", "changed")},
                     ensure_ascii=False, indent=2))
    if not report["identical"]:
        print("added:", added[:20])
        print("removed:", removed[:20])
        print("changed:", changed[:20])
    return 0 if report["identical"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
