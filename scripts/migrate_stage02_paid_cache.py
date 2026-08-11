#!/usr/bin/env python3
"""Поднять per-run кэши Stage 02 на уровень версии (03_analysis).

Зачем
-----
В v2-раскладке ``output_dir`` — это ``<version>/03_analysis/runs/<run_id>``,
новый каталог на КАЖДЫЙ запуск этапа. Кэш оплаченных ответов складывался внутрь
него и потому не переиспользовался между прогонами: перезапуск после сбоя
платил за уже оплаченные блоки заново. Код починен
(``stage02_paid_cache.cache_dir_for_output``), этот скрипт переносит то, что
уже накоплено, — иначе 22 219 оплаченных ответов останутся в мёртвых
run-каталогах.

Как
---
Файл кэша адресуется sha256 от промпта и идентичности картинки, содержимое
неизменяемо. Поэтому переносим ЖЁСТКОЙ ССЫЛКОЙ: цель и источник — один инод,
330 МБ не дублируются, оригиналы остаются на месте (ничего не удаляется).
Одноимённый файл в цели считается тем же ответом и пропускается.

Использование
-------------
    python scripts/migrate_stage02_paid_cache.py            # план, без записи
    python scripts/migrate_stage02_paid_cache.py --apply    # выполнить
    python scripts/migrate_stage02_paid_cache.py --apply --project СТ26_01-14-ОВ2-2-РД_V1
"""
from __future__ import annotations

import argparse
import os
import shutil
import sys
from collections import Counter
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

CACHE_DIRNAME = "_stage02_paid_response_cache"
# Служебные ветки: корзина, бэкапы разрушительных операций, тестовые слепки.
# Переносить их бессмысленно — эти прогоны никогда не повторятся.
SKIP_PARTS = ("_system", "_trash", "destructive_backups")


def _v2_root() -> Path:
    return _ROOT / "projects_v2"


def find_per_run_caches(project_filter: str = "") -> list[Path]:
    """Каталоги кэша, лежащие внутри одноразовых run-каталогов."""
    root = _v2_root()
    if not root.exists():
        return []
    found: list[Path] = []
    for cache_dir in root.glob(
        "objects/*/disciplines/*/documents/*/versions/*/03_analysis/runs/*/" + CACHE_DIRNAME
    ):
        if any(part in SKIP_PARTS for part in cache_dir.parts):
            continue
        if project_filter:
            # .../documents/<code>/versions/...
            parts = cache_dir.parts
            try:
                code = parts[parts.index("documents") + 1]
            except (ValueError, IndexError):
                continue
            if code != project_filter:
                continue
        found.append(cache_dir)
    return sorted(found)


def target_dir_for(cache_dir: Path) -> Path:
    """<version>/03_analysis/_stage02_paid_response_cache/ — общий для прогонов."""
    # cache_dir = <...>/03_analysis/runs/<run_id>/_stage02_paid_response_cache
    return cache_dir.parent.parent.parent / CACHE_DIRNAME


def migrate(apply: bool, project_filter: str = "") -> dict:
    stats = Counter()
    per_project: Counter = Counter()

    for cache_dir in find_per_run_caches(project_filter):
        target = target_dir_for(cache_dir)
        parts = cache_dir.parts
        try:
            code = parts[parts.index("documents") + 1]
        except (ValueError, IndexError):
            code = "?"
        stats["каталогов"] += 1

        files = sorted(cache_dir.glob("*.json"))
        if not files:
            continue
        if apply:
            target.mkdir(parents=True, exist_ok=True)

        for src in files:
            dst = target / src.name
            if dst.exists():
                stats["уже_есть"] += 1
                continue
            stats["к_переносу"] += 1
            per_project[code] += 1
            if not apply:
                continue
            try:
                os.link(src, dst)
                stats["перенесено"] += 1
            except OSError:
                # Разные файловые системы или отказ в ссылке — копируем.
                try:
                    shutil.copy2(src, dst)
                    stats["скопировано"] += 1
                except OSError as exc:
                    stats["ошибок"] += 1
                    print(f"  ! {src}: {exc}")

    return {"stats": stats, "per_project": per_project}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="выполнить перенос")
    parser.add_argument("--project", default="", help="только один код документа")
    args = parser.parse_args()

    result = migrate(apply=args.apply, project_filter=args.project)
    stats = result["stats"]
    per_project = result["per_project"]

    mode = "ВЫПОЛНЕНО" if args.apply else "ПЛАН (записи не было)"
    print(f"\n=== Перенос кэша Stage 02 на уровень версии — {mode} ===")
    print(f"  run-каталогов с кэшем : {stats['каталогов']}")
    print(f"  ответов к переносу    : {stats['к_переносу']}")
    print(f"  уже на месте          : {stats['уже_есть']}")
    if args.apply:
        print(f"  перенесено ссылкой    : {stats['перенесено']}")
        print(f"  скопировано           : {stats['скопировано']}")
        print(f"  ошибок                : {stats['ошибок']}")

    if per_project:
        print("\n  Топ документов:")
        for code, count in per_project.most_common(15):
            print(f"    {count:>6}  {code}")

    if not args.apply and stats["к_переносу"]:
        print("\n  Повторить с --apply, чтобы выполнить.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
