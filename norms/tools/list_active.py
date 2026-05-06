"""Собирает список действующих норм из vault'а.

- Обходит все *.md в Norms md/.
- Парсит имена через parse_filename.parse_filename.
- Применяет status_overrides.yaml (исключает cancelled, помечает replaced_by).
- Сохраняет active_norms.json.
- Печатает в консоль таблицу первых 50 записей (или --all).

Запуск:
    python3 list_active.py           # таблица + JSON
    python3 list_active.py --all     # вся таблица
    python3 list_active.py --quiet   # только JSON
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

from parse_filename import parse_filename

VAULT = Path(__file__).resolve().parent.parent / "vault"
HERE = Path(__file__).parent
OVERRIDES_PATH = HERE / "status_overrides.yaml"
OUTPUT_PATH = HERE / "active_norms.json"


def load_overrides() -> dict[str, object]:
    if not OVERRIDES_PATH.exists():
        return {}
    data = yaml.safe_load(OVERRIDES_PATH.read_text(encoding="utf-8")) or {}
    return data.get("overrides") or {}


def build_index(overrides: dict[str, object]) -> dict:
    active: list[dict] = []
    parse_failures: list[str] = []
    skipped_cancelled: list[str] = []

    for md in sorted(VAULT.glob("*.md")):
        if md.name.startswith("MOC - "):
            continue
        parsed = parse_filename(md.name)
        code = parsed["code"]

        if parsed["parse_confidence"] == "low":
            parse_failures.append(md.name)

        override = overrides.get(code)
        # Поддержка расширенного формата status_overrides.yaml (doc_status/...)
        is_cancelled = override == "cancelled" or (
            isinstance(override, dict) and str(override.get("doc_status", "")).lower() == "cancelled"
        )
        if is_cancelled:
            skipped_cancelled.append(code)
            continue

        entry = {
            "code": code,
            "type": parsed["type"],
            "year": parsed["year"],
            "title": parsed["title"],
            "file": parsed["file"],
            "status": "active",
        }
        if isinstance(override, dict):
            if "replaced_by" in override and override["replaced_by"]:
                entry["replaced_by"] = override["replaced_by"]
            elif str(override.get("doc_status", "")).lower() == "replaced" and override.get("replacement_doc"):
                entry["replaced_by"] = override["replacement_doc"]
        active.append(entry)

    return {
        "meta": {
            "indexed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "vault_path": str(VAULT),
            "total": len(active),
            "parse_failures": parse_failures,
            "skipped_cancelled": skipped_cancelled,
        },
        "norms": active,
    }


def print_table(norms: list[dict], limit: int | None) -> None:
    rows = norms if limit is None else norms[:limit]
    # Колонки: type (7), code (30), year (4), title (остаток до 120).
    print(f"{'TYPE':<8}{'CODE':<32}{'YEAR':<6}TITLE")
    print("-" * 120)
    for n in rows:
        year = str(n["year"]) if n["year"] else "—"
        title = n["title"][:70]
        print(f"{n['type']:<8}{n['code']:<32}{year:<6}{title}")
    if limit is not None and len(norms) > limit:
        print(f"... ({len(norms) - limit} ещё, используй --all)")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--all", action="store_true", help="Печатать всю таблицу")
    ap.add_argument("--quiet", action="store_true", help="Только JSON, без таблицы")
    args = ap.parse_args()

    if not VAULT.exists():
        print(f"ERROR: vault not found: {VAULT}", file=sys.stderr)
        return 1

    overrides = load_overrides()
    index = build_index(overrides)

    OUTPUT_PATH.write_text(
        json.dumps(index, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    meta = index["meta"]
    print(
        f"Сохранено {meta['total']} действующих норм → {OUTPUT_PATH.name}",
        file=sys.stderr,
    )
    if meta["skipped_cancelled"]:
        print(
            f"Пропущено отменённых: {len(meta['skipped_cancelled'])}",
            file=sys.stderr,
        )
    if meta["parse_failures"]:
        print(
            f"Низкая уверенность парсинга: {len(meta['parse_failures'])} (см. meta.parse_failures в JSON)",
            file=sys.stderr,
        )

    if not args.quiet:
        print()
        print_table(index["norms"], limit=None if args.all else 50)

    return 0


if __name__ == "__main__":
    sys.exit(main())
