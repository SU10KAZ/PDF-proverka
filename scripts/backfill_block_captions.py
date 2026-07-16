#!/usr/bin/env python3
"""Backfill: гуманизация ссылок на блоки в СТАРЫХ 03_findings.json.

Новые прогоны чистит пост-проход в findings_merge (block_captions.py);
этот скрипт — разовый проход по уже существующим данным projects_v2.

По умолчанию — DRY-RUN: только отчёт, файлы не трогаются.
Применение — явно флагом --apply; перед записью каждого файла создаётся
бэкап `03_findings.json.bak-captions` рядом (существующий бэкап не
перезаписывается — прогон идемпотентен, бэкап хранит исходник).

Запуск:
    python scripts/backfill_block_captions.py                # dry-run, все проекты
    python scripts/backfill_block_captions.py --filter ТХ.ВТ # dry-run по подстроке пути
    python scripts/backfill_block_captions.py --apply        # применить
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.services.findings.block_captions import (  # noqa: E402
    build_block_caption_map,
    humanize_findings,
)

BACKUP_SUFFIX = ".bak-captions"


def process_dir(latest_dir: Path, apply: bool) -> dict | None:
    findings_path = latest_dir / "03_findings.json"
    if not findings_path.exists():
        return None
    try:
        data = json.loads(findings_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"error": "битый JSON", "path": str(findings_path)}
    findings = data.get("findings")
    if not isinstance(findings, list) or not findings:
        return None

    captions = build_block_caption_map(latest_dir)
    if not captions:
        return None

    stats = humanize_findings(findings, captions)
    if not stats["findings_changed"]:
        return None

    stats["path"] = str(findings_path.relative_to(_ROOT))
    if apply:
        backup = findings_path.with_name(findings_path.name + BACKUP_SUFFIX)
        if not backup.exists():  # первый прогон сохраняет исходник
            shutil.copy2(findings_path, backup)
        findings_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8",
        )
    return stats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="записать изменения (без флага — dry-run)")
    ap.add_argument("--filter", default="",
                    help="подстрока пути версии для выборочного прогона")
    args = ap.parse_args()

    pattern = "projects_v2/objects/*/disciplines/*/documents/*/versions/*/03_analysis/latest"
    dirs = sorted(_ROOT.glob(pattern))
    if args.filter:
        dirs = [d for d in dirs if args.filter in str(d)]

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"[{mode}] версий к проверке: {len(dirs)}")

    total = {"files": 0, "findings_changed": 0, "ids_replaced": 0, "related_added": 0}
    for d in dirs:
        stats = process_dir(d, apply=args.apply)
        if not stats:
            continue
        if "error" in stats:
            print(f"  !! {stats['error']}: {stats['path']}")
            continue
        total["files"] += 1
        for k in ("findings_changed", "ids_replaced", "related_added"):
            total[k] += stats[k]
        print(f"  {stats['path']}: замечаний {stats['findings_changed']}, "
              f"замен {stats['ids_replaced']}, +related {stats['related_added']}")

    print(f"\nИтого [{mode}]: файлов {total['files']}, "
          f"замечаний {total['findings_changed']}, замен {total['ids_replaced']}, "
          f"перенесено в related_block_ids {total['related_added']}")
    if not args.apply and total["files"]:
        print("Файлы НЕ изменены. Для применения: --apply (создаст .bak-captions)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
