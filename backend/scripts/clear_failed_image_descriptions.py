#!/usr/bin/env python3
"""Очистить failed/error image-блоки из сохранённых description JSON.

Использование:

    # Все пары одной сессии (обе стороны):
    python backend/scripts/clear_failed_image_descriptions.py <session_id>

    # Конкретная пара:
    python backend/scripts/clear_failed_image_descriptions.py <session_id> <pair_id>

    # Только конкретная сторона:
    python backend/scripts/clear_failed_image_descriptions.py <session_id> <pair_id> --side left

    # Также удалить salvaged (partial) блоки, не только error:
    python backend/scripts/clear_failed_image_descriptions.py <session_id> --include-salvaged

    # Dry-run: только показать, что будет удалено:
    python backend/scripts/clear_failed_image_descriptions.py <session_id> --dry-run

Что делает:
  * читает <session_root>/pairs/<pair_id>/text_enrichment/<side>_image_descriptions.json
  * убирает из `items` записи со status in {"error","no_image","render_failed"}
    (и опционально status="partial" если задан --include-salvaged);
  * пересчитывает агрегаты image_blocks_total/described/errors/pending/salvaged;
  * сохраняет файл обратно через temp-rename;
  * НЕ трогает кеш (cache/) — в нём всё равно только успешные ответы.

После очистки:
  * пары/стороны со status=partial вернутся в not_run/partial по числу
    оставшихся в файле описаний;
  * следующий прогон с `force=false` повторно вызовет Qwen на удалённых блоках
    (cache-key их не покрывает, потому что error/partial не кешировались).

Никаких runtime-артефактов не пишется в репозиторий — все правки только
внутри comparison/sessions/.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SESSIONS_ROOT = REPO_ROOT / "comparison" / "sessions"


_BAD_STATUSES = {"error", "no_image", "render_failed"}


def _process_file(
    path: Path,
    *,
    include_salvaged: bool,
    dry_run: bool,
) -> dict:
    if not path.exists():
        return {"path": str(path), "exists": False}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {"path": str(path), "error": f"read_failed:{exc}"}

    items = list(data.get("items") or [])
    if not items:
        return {"path": str(path), "items": 0, "removed": 0}

    bad_states = set(_BAD_STATUSES)
    if include_salvaged:
        bad_states.add("partial")

    kept: list[dict] = []
    removed: list[dict] = []
    for it in items:
        st = (it.get("status") or "").lower()
        if st in bad_states:
            removed.append(it)
        else:
            kept.append(it)

    if not removed:
        return {"path": str(path), "items": len(items), "removed": 0}

    # Пересчёт агрегатов
    described = sum(1 for it in kept if (it.get("status") or "").lower() == "done")
    salvaged = sum(1 for it in kept if (it.get("status") or "").lower() == "partial")
    errors = sum(1 for it in kept if (it.get("status") or "").lower() in _BAD_STATUSES)
    pending = sum(1 for it in kept if (it.get("status") or "").lower() == "pending")
    from_cache = sum(1 for it in kept if it.get("from_cache"))
    image_blocks_total = len(kept)

    new_data = dict(data)
    new_data["items"] = kept
    new_data["image_blocks_total"] = image_blocks_total
    new_data["described"] = described
    new_data["salvaged"] = salvaged
    new_data["errors"] = errors
    new_data["pending"] = pending
    new_data["from_cache"] = from_cache

    if dry_run:
        return {
            "path": str(path),
            "items_before": len(items),
            "items_after": len(kept),
            "removed": len(removed),
            "removed_statuses": sorted({(it.get("status") or "?") for it in removed}),
            "dry_run": True,
        }

    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(new_data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)

    return {
        "path": str(path),
        "items_before": len(items),
        "items_after": len(kept),
        "removed": len(removed),
        "removed_statuses": sorted({(it.get("status") or "?") for it in removed}),
    }


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Очистить error/partial блоки из *_image_descriptions.json",
    )
    ap.add_argument("session_id")
    ap.add_argument("pair_id", nargs="?", default=None)
    ap.add_argument("--side", choices=("left", "right", "both"), default="both")
    ap.add_argument("--include-salvaged", action="store_true",
                    help="Также удалить блоки status=partial (salvaged)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    session_dir = SESSIONS_ROOT / args.session_id
    if not session_dir.exists():
        print(f"[err] Сессия не найдена: {session_dir}", file=sys.stderr)
        return 2

    pairs_dir = session_dir / "pairs"
    if not pairs_dir.exists():
        print(f"[err] Нет каталога pairs: {pairs_dir}", file=sys.stderr)
        return 2

    if args.pair_id:
        pair_dirs = [pairs_dir / args.pair_id]
    else:
        pair_dirs = sorted(d for d in pairs_dir.iterdir() if d.is_dir())

    sides = ("left", "right") if args.side == "both" else (args.side,)

    total_removed = 0
    total_files = 0
    for pd in pair_dirs:
        for side in sides:
            f = pd / "text_enrichment" / f"{side}_image_descriptions.json"
            result = _process_file(f, include_salvaged=args.include_salvaged, dry_run=args.dry_run)
            if not result.get("exists", True):
                continue
            total_files += 1
            if result.get("removed", 0):
                total_removed += result["removed"]
                tag = "[dry]" if args.dry_run else "[ok]"
                statuses = ",".join(result.get("removed_statuses") or [])
                print(f"{tag} {pd.name}/{side}: removed {result['removed']} "
                      f"({statuses}) — items {result['items_before']} → {result['items_after']}")
            else:
                print(f"[--] {pd.name}/{side}: clean (items={result.get('items', 0)})")

    print()
    print(f"== summary ==  files_checked={total_files}  blocks_removed={total_removed}"
          f"  dry_run={args.dry_run}  include_salvaged={args.include_salvaged}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
