"""Мерж high-confidence cancelled-кандидатов в status_overrides.yaml.

Стратегия:
  1. Читаем tools/cancellations_draft.yaml.
  2. Берём только high-confidence кандидаты.
  3. Проверяем защиты:
       - код уже есть в overrides;
       - код уже известен как replaced / имеет replacement_doc;
       - дубликат в draft;
       - конфликт с текущим статусом в index.
  4. Добавляем чистые записи в конец status_overrides.yaml отдельным блоком.

Запуск:
    python3 tools/merge_cancellations.py
    python3 tools/merge_cancellations.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
DRAFT_PATH = HERE / "cancellations_draft.yaml"
OVERRIDES_PATH = HERE / "status_overrides.yaml"
INDEX_PATH = HERE / "status_index.json"

MARKER = "# ─── Auto-merged cancellations (extract_cancellations.py) ───"


def _match_key(s: str) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", "", s).replace("_", ".").lower()


def _clean_code(code: str) -> str:
    code = code.strip()
    code = re.sub(r"[/.,;:\s]+$", "", code)
    code = re.sub(r"\s+", " ", code)
    return code


def _load_draft() -> tuple[list[dict], dict[str, str]]:
    raw = yaml.safe_load(DRAFT_PATH.read_text(encoding="utf-8")) or {}
    items = raw.get("items") or []
    highest_conf: dict[str, str] = {}
    order = {"high": 2, "medium": 1}
    for item in items:
        key = _match_key(str(item.get("code") or ""))
        conf = str(item.get("confidence") or "")
        if not key or conf not in order:
            continue
        if key not in highest_conf or order[conf] > order[highest_conf[key]]:
            highest_conf[key] = conf
    high_items = [item for item in items if item.get("confidence") == "high"]
    return high_items, highest_conf


def _parse_override(raw: object) -> dict:
    if isinstance(raw, str):
        val = raw.strip().lower()
        return {
            "doc_status": val if val in {"active", "replaced", "cancelled", "unknown"} else None,
            "replacement_doc": None,
        }
    if not isinstance(raw, dict):
        return {"doc_status": None, "replacement_doc": None}
    status = raw.get("doc_status", raw.get("status"))
    if "replaced_by" in raw and raw.get("replaced_by"):
        return {"doc_status": "replaced", "replacement_doc": str(raw["replaced_by"]).strip()}
    return {
        "doc_status": str(status).strip().lower() if status else None,
        "replacement_doc": str(raw.get("replacement_doc")).strip() if raw.get("replacement_doc") else None,
    }


def _load_overrides() -> tuple[set[str], set[str]]:
    raw = yaml.safe_load(OVERRIDES_PATH.read_text(encoding="utf-8")) or {}
    overrides = raw.get("overrides") or {}
    existing_keys: set[str] = set()
    replacement_keys: set[str] = set()
    for raw_code, raw_entry in overrides.items():
        key = _match_key(str(raw_code))
        existing_keys.add(key)
        parsed = _parse_override(raw_entry)
        if parsed["doc_status"] == "replaced" or parsed["replacement_doc"]:
            replacement_keys.add(key)
    return existing_keys, replacement_keys


def _load_index() -> tuple[dict[str, dict], set[str], set[str]]:
    data = json.loads(INDEX_PATH.read_text(encoding="utf-8"))
    by_key: dict[str, dict] = {}
    active_vault: set[str] = set()
    replacement_keys: set[str] = set()
    for entry in data.get("norms", []):
        key = _match_key(entry["code"])
        by_key[key] = entry
        for alias in entry.get("aliases") or []:
            by_key.setdefault(_match_key(alias), entry)
        if entry.get("source") == "vault" and entry.get("doc_status") == "active":
            active_vault.add(key)
            for alias in entry.get("aliases") or []:
                active_vault.add(_match_key(alias))
        if entry.get("doc_status") == "replaced" or entry.get("replacement_doc"):
            replacement_keys.add(key)
            for alias in entry.get("aliases") or []:
                replacement_keys.add(_match_key(alias))
    return by_key, active_vault, replacement_keys


def _yaml_escape(s: str) -> str:
    return s.replace('"', '\\"')


def _render_entry(code: str, source_file: str, line_no: int, today: str) -> str:
    return (
        f'  "{_yaml_escape(code)}":\n'
        f"    doc_status: cancelled\n"
        f'    details: "Извлечено из шапки {_yaml_escape(source_file)} (строка {line_no}, auto-merge high-confidence)"\n'
        f'    last_verified: "{today}"\n'
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="Не писать, только показать план.")
    args = ap.parse_args()

    if not DRAFT_PATH.exists():
        print(f"ERROR: draft не найден: {DRAFT_PATH}", file=sys.stderr)
        return 1
    if not INDEX_PATH.exists():
        print("ERROR: status_index.json не найден — сначала запусти build_status_index.py", file=sys.stderr)
        return 1

    high_items, highest_conf = _load_draft()
    existing_overrides, replacement_in_overrides = _load_overrides()
    index_by_key, active_vault, replacement_in_index = _load_index()

    counters = {
        "merged": 0,
        "skipped_existing_override": 0,
        "skipped_has_replacement": 0,
        "skipped_conflict_active": 0,
        "skipped_duplicate": 0,
        "skipped_other": 0,
    }
    skipped_details: list[tuple[str, str]] = []
    to_add: list[tuple[str, str, int]] = []
    seen_keys: set[str] = set()

    for item in high_items:
        code = _clean_code(str(item.get("code") or ""))
        source_file = str(item.get("source_file") or "")
        line_no = int(item.get("line_no") or 0)
        key = _match_key(code)

        if not code or not key:
            counters["skipped_other"] += 1
            skipped_details.append((code, "empty_after_clean"))
            continue
        if highest_conf.get(key) != "high":
            counters["skipped_conflict_active"] += 1
            skipped_details.append((code, "highest_confidence_not_high"))
            continue
        if key in seen_keys:
            counters["skipped_duplicate"] += 1
            skipped_details.append((code, "duplicate_in_draft"))
            continue
        if key in existing_overrides:
            counters["skipped_existing_override"] += 1
            skipped_details.append((code, "already_in_overrides"))
            continue
        if key in replacement_in_overrides or key in replacement_in_index:
            counters["skipped_has_replacement"] += 1
            skipped_details.append((code, "already_has_replacement"))
            continue

        entry = index_by_key.get(key)
        if entry:
            if entry.get("doc_status") == "replaced" or entry.get("replacement_doc"):
                counters["skipped_has_replacement"] += 1
                skipped_details.append((code, "index_has_replacement"))
                continue
            if entry.get("doc_status") == "cancelled":
                counters["skipped_existing_override"] += 1
                skipped_details.append((code, "already_cancelled_in_index"))
                continue
            if key in active_vault and item.get("confidence") != "high":
                counters["skipped_conflict_active"] += 1
                skipped_details.append((code, "active_without_high"))
                continue

        seen_keys.add(key)
        to_add.append((code, source_file, line_no))

    counters["merged"] = len(to_add)
    today = date.today().isoformat()

    print(f"Всего high-кандидатов:           {len(high_items)}", file=sys.stderr)
    print(f"merged:                         {counters['merged']}", file=sys.stderr)
    print(f"skipped_existing_override:      {counters['skipped_existing_override']}", file=sys.stderr)
    print(f"skipped_has_replacement:        {counters['skipped_has_replacement']}", file=sys.stderr)
    print(f"skipped_conflict_active:        {counters['skipped_conflict_active']}", file=sys.stderr)
    print(f"skipped_duplicate:              {counters['skipped_duplicate']}", file=sys.stderr)
    print(f"skipped_other:                  {counters['skipped_other']}", file=sys.stderr)
    if skipped_details:
        for code, reason in skipped_details:
            print(f"  [{reason}] {code}", file=sys.stderr)

    if args.dry_run:
        if to_add:
            print("\n(--dry-run) Примеры будущих записей:", file=sys.stderr)
            for code, source_file, line_no in to_add[:5]:
                print(_render_entry(code, source_file, line_no, today), file=sys.stderr)
        else:
            print("\n(--dry-run) Нечего добавлять.", file=sys.stderr)
        return 0

    if not to_add:
        print("Нечего добавлять — файл не изменён.", file=sys.stderr)
        return 0

    text = OVERRIDES_PATH.read_text(encoding="utf-8").rstrip() + "\n"
    if MARKER not in text:
        text += f"\n{MARKER}\n"
        text += f"# Сгенерировано {today}. Источник: cancellations_draft.yaml\n"
        text += "# Правила: только high-confidence cancelled без replacement_doc.\n"
    else:
        text += f"\n# (добавлено {today})\n"
    for code, source_file, line_no in to_add:
        text += _render_entry(code, source_file, line_no, today)

    OVERRIDES_PATH.write_text(text, encoding="utf-8")
    print(f"Записано → {OVERRIDES_PATH.name} (+{len(to_add)} записей)", file=sys.stderr)
    print("Теперь пересобери индекс:  python3 tools/build_status_index.py", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
