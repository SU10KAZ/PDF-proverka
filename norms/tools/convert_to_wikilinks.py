"""Создаёт копию vault'а с включёнными wiki-ссылками для графа Obsidian.

Действия:
1. Копирует "Norms md/" → "Norms md graph/" (skip если уже существует).
2. В каждом MD:
   a. Добавляет YAML-frontmatter с aliases (разные формы кода: 'ГОСТ 25192-82',
      'ГОСТ 25192', 'ГОСТ 25192-82' с точками/подчёркиваниями).
   b. Заменяет "[ГОСТ XXX](#)" → "[[ГОСТ XXX]]" (только для ссылок на нормы).
3. Пишет report.json.

Идемпотентный: повторный запуск не дублирует frontmatter.

Запуск:
    python3 convert_to_wikilinks.py              # скопировать и конвертировать
    python3 convert_to_wikilinks.py --overwrite  # перезаписать существующий Norms md graph
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
from pathlib import Path

from parse_filename import parse_filename

VAULT_SRC = Path(__file__).resolve().parent.parent / "vault"
VAULT_DST = Path(__file__).resolve().parent.parent / "vault_graph"
HERE = Path(__file__).parent
REPORT_PATH = HERE / "wikilinks_report.json"

# Ссылка-кандидат для преобразования: [<любой текст начинающийся с типа нормы>](#)
# Тип нормы должен начинаться с начала текста скобки (иначе зацепим "в ред. [ГОСТ...]").
_LINK_RE = re.compile(
    r"\[((?:ГОСТ\s+Р|ГОСТ|СП|СНиП|ПУЭ|ВСН|МДС|РД)[^\]]*?)\]\(#\)"
)

_FRONTMATTER_START = "---\n"


def generate_aliases(parsed: dict) -> list[str]:
    """Генерирует набор алиасов для разных форм написания кода."""
    if parsed["parse_confidence"] != "high":
        return []
    type_ = parsed["type"]
    code_raw = parsed["code_raw"]  # например 'ГОСТ Р 10_0_02-2019' → code_raw='10_0_02-2019'
    aliases = set()

    # Основные формы с годом
    aliases.add(f"{type_} {code_raw}")
    aliases.add(f"{type_} {code_raw.replace('_', '.')}")

    # Формы без года (часто в тексте ссылаются без года)
    # Берём всё до последнего дефиса
    m = re.match(r"^(.+)-\d{2,4}$", code_raw)
    if m:
        base = m.group(1)
        aliases.add(f"{type_} {base}")
        aliases.add(f"{type_} {base.replace('_', '.')}")

    return sorted(aliases)


def has_frontmatter(content: str) -> bool:
    return content.startswith(_FRONTMATTER_START)


def build_frontmatter(aliases: list[str]) -> str:
    if not aliases:
        return ""
    lines = ["---", "aliases:"]
    for a in aliases:
        lines.append(f'  - "{a}"')
    lines.append("---")
    lines.append("")
    return "\n".join(lines)


def convert_links(content: str) -> tuple[str, int]:
    """Заменяет [норма](#) → [[норма]]. Возвращает (новый_текст, счётчик)."""
    count = 0

    def replace(match: re.Match) -> str:
        nonlocal count
        count += 1
        text = match.group(1).strip()
        return f"[[{text}]]"

    new_content = _LINK_RE.sub(replace, content)
    return new_content, count


def process_file(path: Path) -> dict:
    """Обрабатывает один MD-файл: добавляет frontmatter и конвертирует ссылки."""
    original = path.read_text(encoding="utf-8")
    parsed = parse_filename(path.name)
    aliases = generate_aliases(parsed)

    body = original
    added_frontmatter = False
    if aliases and not has_frontmatter(original):
        fm = build_frontmatter(aliases)
        body = fm + original
        added_frontmatter = True

    body, links_converted = convert_links(body)

    if body != original:
        path.write_text(body, encoding="utf-8")

    return {
        "file": path.name,
        "aliases": aliases,
        "added_frontmatter": added_frontmatter,
        "links_converted": links_converted,
        "parse_confidence": parsed["parse_confidence"],
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--overwrite", action="store_true",
                    help="Удалить существующий Norms md graph/ перед копированием")
    args = ap.parse_args()

    if not VAULT_SRC.exists():
        print(f"ERROR: src vault not found: {VAULT_SRC}", file=sys.stderr)
        return 1

    if VAULT_DST.exists():
        if args.overwrite:
            print(f"Удаляю существующий {VAULT_DST}", file=sys.stderr)
            shutil.rmtree(VAULT_DST)
        else:
            print(
                f"ERROR: {VAULT_DST} уже существует. Используйте --overwrite.",
                file=sys.stderr,
            )
            return 1

    print(f"Копирую {VAULT_SRC} → {VAULT_DST}...", file=sys.stderr)
    shutil.copytree(VAULT_SRC, VAULT_DST)

    print("Обработка файлов...", file=sys.stderr)
    results = []
    total_links = 0
    total_fm = 0
    for md in sorted(VAULT_DST.glob("*.md")):
        r = process_file(md)
        results.append(r)
        total_links += r["links_converted"]
        if r["added_frontmatter"]:
            total_fm += 1

    report = {
        "src": str(VAULT_SRC),
        "dst": str(VAULT_DST),
        "files_processed": len(results),
        "frontmatter_added": total_fm,
        "links_converted": total_links,
        "low_confidence_skipped_aliases": [
            r["file"] for r in results if not r["aliases"]
        ],
        "details": results,
    }
    REPORT_PATH.write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(
        f"Готово: файлов {len(results)}, frontmatter добавлен в {total_fm}, "
        f"ссылок конвертировано {total_links}",
        file=sys.stderr,
    )
    print(f"Отчёт: {REPORT_PATH}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
