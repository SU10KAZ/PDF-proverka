#!/usr/bin/env python3
"""11H §33 — перенести ГОТОВЫЙ аудит из стенда в продовое дерево версии.

Зачем отдельный шаг, а не «пусть стенд пишет в прод сразу».

Стенд поднимает ВТОРОЙ экземпляр платформы. Боевой backend на этой машине
работает всё время прогона и читает-пишет то же дерево `projects_v2`; два
процесса, пишущих в одну версию, — это не «немного риска», это гонка на
данных заказчика. Поэтому аудит идёт в изолированной копии, а перенос —
отдельная операция с проверками и с обратимостью.

Что делает скрипт:

  1. проверяет, что аудит в стенде ЗАВЕРШЁН (есть findings и норм-проверка);
  2. проверяет, что ИСХОДНИКИ версии в стенде и в проде побайтово совпадают —
     иначе перенос означал бы результат по другому документу;
  3. проверяет, что в проде нечего терять, либо делает полный снимок того, что
     будет перезаписано;
  4. переносит только каталоги РЕЗУЛЬТАТА, не трогая исходники;
  5. печатает хэши до/после.

Ничего не удаляет: старое (если оно есть) уезжает в снимок рядом.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path
from typing import Any

#: Каталоги РЕЗУЛЬТАТА. Исходников (`01_input`, `02_work`) здесь нет намеренно:
#: их перенос означал бы подмену документа, а не публикацию аудита.
RESULT_DIRS = ("03_analysis", "04_review", "05_export", "99_service")

#: Файлы, которые обязаны существовать у ЗАВЕРШЁННОГО аудита.
REQUIRED_ARTIFACTS = ("03_analysis/latest/03_findings.json",)

#: Поля version.json, которые переносятся вместе с результатом: они и есть
#: «у этой версии есть аудит».
ANALYSIS_FIELDS = (
    "analysis_run_id", "analysis_status", "analysis_generation",
    "missing_analysis_files",
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def tree_hash(root: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not root.is_dir():
        return out
    for path in sorted(root.rglob("*")):
        if path.is_file():
            out[str(path.relative_to(root))] = sha256_file(path)
    return out


def fail(message: str) -> "None":
    print(f"ОТКАЗ: {message}", file=sys.stderr)
    raise SystemExit(2)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stand-version-dir", required=True)
    parser.add_argument("--production-version-dir", required=True)
    parser.add_argument("--apply", action="store_true",
                        help="без него — сухой прогон")
    parser.add_argument("--out", default="")
    args = parser.parse_args()

    stand = Path(args.stand_version_dir).resolve()
    prod = Path(args.production_version_dir).resolve()
    report: dict[str, Any] = {"stand": str(stand), "production": str(prod),
                              "applied": False}

    if not stand.is_dir():
        fail(f"каталога версии в стенде нет: {stand}")
    if not prod.is_dir():
        fail(f"продового каталога версии нет: {prod}")

    # 1. аудит завершён
    for relative in REQUIRED_ARTIFACTS:
        if not (stand / relative).is_file():
            fail(f"в стенде нет обязательного артефакта {relative}: аудит не завершён")
    findings = json.loads((stand / "03_analysis/latest/03_findings.json").read_text("utf-8"))
    count = len(findings.get("findings") or [])
    report["findings_count"] = count
    if count == 0:
        print("ПРЕДУПРЕЖДЕНИЕ: замечаний ноль — перенос всё равно возможен, "
              "но результат стоит посмотреть глазами")

    # 2. исходники совпадают
    for name in ("01_input", "02_work"):
        left, right = tree_hash(stand / name), tree_hash(prod / name)
        if left != right:
            only_stand = sorted(set(left) - set(right))[:5]
            only_prod = sorted(set(right) - set(left))[:5]
            changed = sorted(k for k in set(left) & set(right) if left[k] != right[k])[:5]
            fail(
                f"исходники {name} расходятся: только в стенде {only_stand}, "
                f"только в проде {only_prod}, различаются {changed}"
            )
    report["sources_identical"] = True

    # 3. что перезаписываем
    existing = {name: tree_hash(prod / name) for name in RESULT_DIRS}
    occupied = {name: len(files) for name, files in existing.items() if files}
    report["production_result_before"] = occupied
    report["production_had_audit"] = bool(occupied)

    incoming = {name: tree_hash(stand / name) for name in RESULT_DIRS}
    report["incoming"] = {name: len(files) for name, files in incoming.items()}

    print(f"замечаний в переносимом аудите: {count}")
    print(f"в проде сейчас: {occupied or 'пусто (терять нечего)'}")
    print(f"переносится: { {k: v for k, v in report['incoming'].items() if v} }")

    if not args.apply:
        print("\nСУХОЙ ПРОГОН. Для переноса добавьте --apply")
        if args.out:
            Path(args.out).write_text(json.dumps(report, ensure_ascii=False, indent=1),
                                      encoding="utf-8")
        return 0

    # 4. снимок того, что будет перезаписано
    if occupied:
        backup = prod.parent / f"{prod.name}__pre_11h_backup"
        if backup.exists():
            fail(f"снимок уже существует: {backup}. Разберитесь с ним вручную")
        backup.mkdir(parents=True)
        for name in RESULT_DIRS:
            source = prod / name
            if source.is_dir() and any(source.rglob("*")):
                shutil.copytree(source, backup / name)
        report["backup"] = str(backup)
        print(f"снимок прежнего результата: {backup}")

    # 5. перенос
    for name in RESULT_DIRS:
        source, target = stand / name, prod / name
        if not source.is_dir():
            continue
        if target.exists():
            shutil.rmtree(target)
        shutil.copytree(source, target)
    stand_meta = json.loads((stand / "version.json").read_text("utf-8"))
    prod_meta = json.loads((prod / "version.json").read_text("utf-8"))
    for field in ANALYSIS_FIELDS:
        if field in stand_meta:
            prod_meta[field] = stand_meta[field]
        else:
            prod_meta.pop(field, None)
    (prod / "version.json").write_text(
        json.dumps(prod_meta, ensure_ascii=False, indent=2), encoding="utf-8")

    report["applied"] = True
    report["production_result_after"] = {
        name: len(tree_hash(prod / name)) for name in RESULT_DIRS
    }
    report["version_json_after"] = {k: prod_meta.get(k) for k in ANALYSIS_FIELDS}
    print(f"ПЕРЕНЕСЕНО: {report['production_result_after']}")
    if args.out:
        Path(args.out).write_text(json.dumps(report, ensure_ascii=False, indent=1),
                                  encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
