"""Мерж high-confidence пар из predecessors_draft.yaml в status_overrides.yaml.

Стратегия:
  1. Читаем черновик.
  2. Читаем текущий status_index.json (кто уже лежит в authoritative базе).
  3. Читаем status_overrides.yaml как plain text (сохраняем комментарии).
  4. Для каждой high-пары проверяем:
       - old_code ещё не присутствует как отдельный ключ в overrides;
       - old_code не совпадает с активной нормой из vault (чтобы не создавать
         противоречие «replaced vs active»);
       - current присутствует в status_index (не битая ссылка).
  5. Чистые пары аппендим в конец status_overrides.yaml блоком
     «── Auto-merged predecessors ──».
  6. Выводим сводку: добавлено / пропущено / причины.

Скрипт идемпотентный: повторный запуск ничего не добавит, если уже все
пары смержены.

Запуск:
    python3 tools/merge_predecessors.py            # реальная запись
    python3 tools/merge_predecessors.py --dry-run  # только покажет план
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
DRAFT_PATH = HERE / "predecessors_draft.yaml"
OVERRIDES_PATH = HERE / "status_overrides.yaml"
INDEX_PATH = HERE / "status_index.json"

MARKER = "# ─── Auto-merged predecessors (extract_predecessors.py) ───"


def _match_key(s: str) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", "", s).replace("_", ".").lower()


def _clean_code(code: str) -> str:
    """Убирает хвостовой мусор вида '/', '.', ',', пробелы."""
    code = code.strip()
    code = re.sub(r"[/.,;:\s]+$", "", code)
    code = re.sub(r"\s+", " ", code)
    return code


def _load_draft() -> list[dict]:
    raw = yaml.safe_load(DRAFT_PATH.read_text(encoding="utf-8")) or {}
    return [p for p in (raw.get("predecessors") or []) if p.get("confidence") == "high"]


def _load_index_keys() -> tuple[set[str], set[str]]:
    """Возвращает (активные_vault_коды, все_коды_индекса) в match-key форме."""
    data = json.loads(INDEX_PATH.read_text(encoding="utf-8"))
    active_vault: set[str] = set()
    all_codes: set[str] = set()
    for n in data.get("norms", []):
        code_key = _match_key(n["code"])
        all_codes.add(code_key)
        for a in n.get("aliases") or []:
            all_codes.add(_match_key(a))
        if n.get("source") == "vault" and n.get("doc_status") == "active":
            active_vault.add(code_key)
            for a in n.get("aliases") or []:
                active_vault.add(_match_key(a))
    return active_vault, all_codes


def _load_overrides_keys() -> set[str]:
    raw = yaml.safe_load(OVERRIDES_PATH.read_text(encoding="utf-8")) or {}
    overrides = raw.get("overrides") or {}
    return {_match_key(str(k)) for k in overrides.keys()}


def _yaml_escape(s: str) -> str:
    return s.replace('"', '\\"')


def _render_entry(old_code: str, current: str, source_file: str, source_line: int, today: str) -> str:
    return (
        f'  "{_yaml_escape(old_code)}":\n'
        f"    doc_status: replaced\n"
        f'    replacement_doc: "{_yaml_escape(current)}"\n'
        f'    details: "Извлечено из преамбулы {_yaml_escape(source_file)} (строка {source_line}, auto-merge high-confidence)"\n'
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
        print(f"ERROR: status_index.json не найден — сначала запусти build_status_index.py", file=sys.stderr)
        return 1

    high_pairs = _load_draft()
    active_vault, all_codes = _load_index_keys()
    existing_overrides = _load_overrides_keys()

    to_add: list[tuple[str, str, str, int]] = []
    skipped: list[tuple[str, str, str]] = []  # (old_code, current, reason)
    seen_keys: set[str] = set()

    for p in high_pairs:
        old_code = _clean_code(str(p["old_code"]))
        current = _clean_code(str(p["current"]))
        source_file = str(p["source_file"])
        source_line = int(p["source_line"])

        old_key = _match_key(old_code)
        cur_key = _match_key(current)

        if not old_code or not current:
            skipped.append((old_code, current, "empty_after_clean"))
            continue
        if old_key in seen_keys:
            skipped.append((old_code, current, "duplicate_in_draft"))
            continue
        if old_key in existing_overrides:
            skipped.append((old_code, current, "already_in_overrides"))
            continue
        if old_key in active_vault:
            skipped.append((old_code, current, "conflict_active_in_vault"))
            continue
        if cur_key not in all_codes:
            skipped.append((old_code, current, "current_not_in_index"))
            continue

        seen_keys.add(old_key)
        to_add.append((old_code, current, source_file, source_line))

    today = date.today().isoformat()

    print(f"Всего high-пар в черновике: {len(high_pairs)}", file=sys.stderr)
    print(f"К добавлению: {len(to_add)}", file=sys.stderr)
    print(f"Пропущено:    {len(skipped)}", file=sys.stderr)
    if skipped:
        reasons: dict[str, int] = {}
        for _, _, r in skipped:
            reasons[r] = reasons.get(r, 0) + 1
        print(f"  причины: {reasons}", file=sys.stderr)
        for old, cur, reason in skipped:
            print(f"    [{reason}] {old} → {cur}", file=sys.stderr)

    if args.dry_run:
        print("\n(--dry-run) Ничего не записано. Примеры будущих записей:", file=sys.stderr)
        for old, cur, sf, sl in to_add[:5]:
            print(_render_entry(old, cur, sf, sl, today), file=sys.stderr)
        return 0

    if not to_add:
        print("Нечего добавлять — файл не изменён.", file=sys.stderr)
        return 0

    text = OVERRIDES_PATH.read_text(encoding="utf-8").rstrip() + "\n"
    if MARKER not in text:
        text += f"\n{MARKER}\n"
        text += f"# Сгенерировано {today}. Источник: predecessors_draft.yaml\n"
        text += "# Правила: только high-confidence пары, без конфликта с vault.\n"
    else:
        text += f"\n# (добавлено {today})\n"
    for old, cur, sf, sl in to_add:
        text += _render_entry(old, cur, sf, sl, today)

    OVERRIDES_PATH.write_text(text, encoding="utf-8")
    print(f"Записано → {OVERRIDES_PATH.name} (+{len(to_add)} записей)", file=sys.stderr)
    print("Теперь пересобери индекс:  python3 tools/build_status_index.py", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
