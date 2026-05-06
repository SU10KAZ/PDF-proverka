"""Поиск пункта нормативного документа в vault'е.

Usage:
    python3 find_paragraph.py "СП 256.1325800.2016" "15.30"
    python3 find_paragraph.py "ГОСТ 10180-2012" "1.1"

Алгоритм:
1. Нормализуем код (точки → подчёркивания).
2. Находим MD-файл по glob "<code_raw>*_document.md".
3. Ищем regex ^<punct>\\b — строка начинается с номера пункта.
4. Возвращаем текст от найденной строки до:
   - начала следующего пункта такого же или меньшего уровня (15.30 → 15.31 или 16)
   - строки "## СТРАНИЦА" или "### BLOCK"
   - любого markdown-заголовка "# ..."

Exit code: 0 — найдено, 1 — не найдено или ошибка.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

from parse_filename import normalize_user_code

VAULT = Path(__file__).resolve().parent.parent / "vault"

# Паттерны, означающие конец пункта.
# Любой другой номер пункта (X.Y... в начале строки), новая страница, блок, заголовок.
_BOUNDARY_PATTERNS = [
    re.compile(r"^\d+(\.\d+)*\b"),             # следующий нумерованный пункт
    re.compile(r"^## СТРАНИЦА"),                # разметка страницы
    re.compile(r"^### BLOCK\b"),                # разметка блока
    re.compile(r"^#{1,6}\s"),                   # любой markdown-заголовок
]


def find_file(user_code: str) -> Path | None:
    """Найти MD-файл по коду документа."""
    key = normalize_user_code(user_code)
    # Ищем по префиксу имени. Например, 'СП 256_1325800_2016' → glob 'СП 256_1325800_2016*'.
    matches = sorted(VAULT.glob(f"{key}*_document.md"))
    if matches:
        return matches[0]
    # Fallback: без суффикса _document.md (на случай если имя нестандартное).
    matches = sorted(VAULT.glob(f"{key}*"))
    if matches:
        return matches[0]
    return None


def is_boundary(line: str, start_paragraph: str) -> bool:
    """Является ли строка границей конца искомого пункта.

    start_paragraph — номер искомого пункта ('15.30').
    Граница: строка с другим номером пункта, либо разметка страницы/блока/заголовка.
    НЕ граница: строка продолжения этого же пункта, пустая строка, обычный текст.
    """
    stripped = line.lstrip()
    # Разметка страницы/блока/заголовка — граница.
    if _BOUNDARY_PATTERNS[1].match(stripped):
        return True
    if _BOUNDARY_PATTERNS[2].match(stripped):
        return True
    if _BOUNDARY_PATTERNS[3].match(stripped):
        return True
    # Проверка номера пункта — с учётом возможного **bold**.
    numbered = _strip_md_decoration(line)
    m = _BOUNDARY_PATTERNS[0].match(numbered)
    if m:
        found_num = m.group(0)
        if found_num.startswith(start_paragraph + "."):
            return False  # подпункт искомого
        if found_num == start_paragraph:
            return False  # повтор
        return True
    return False


_HEADING_RE = re.compile(r"^#{1,6}\s+")


def _strip_md_decoration(s: str) -> str:
    """Снять ведущие markdown-украшения: '**', '*', '_', пробелы."""
    return s.lstrip().lstrip("*_").lstrip()


def _collect_from(lines: list[str], start_idx: int, paragraph: str) -> list[str]:
    collected = [lines[start_idx]]
    for j in range(start_idx + 1, len(lines)):
        if is_boundary(lines[j], paragraph):
            break
        collected.append(lines[j])
    while collected and not collected[-1].strip():
        collected.pop()
    return collected


def find_paragraph(file: Path, paragraph: str) -> tuple[int, list[str]] | None:
    """Найти пункт в MD-файле. Возвращает (номер_строки, текст) или None.

    Приоритет: сначала matching строки, которые являются markdown-заголовками
    (раздел выделен как "##### 1 Область применения"), затем обычные строки.
    Это защищает от ложных срабатываний типа "1 июля 2013 года".
    """
    lines = file.read_text(encoding="utf-8").splitlines()
    start_regex = re.compile(rf"^{re.escape(paragraph)}\b")

    heading_hit: int | None = None
    plain_hit: int | None = None

    for i, line in enumerate(lines):
        # Строка-заголовок: "##### 15.30 ..." — код содержимого после #.
        h = _HEADING_RE.match(line)
        if h:
            body = _strip_md_decoration(line[h.end():])
            if start_regex.match(body):
                if heading_hit is None:
                    heading_hit = i
                continue
        # Обычная строка, начинается с номера пункта (возможно в **bold**).
        if start_regex.match(_strip_md_decoration(line)):
            if plain_hit is None:
                plain_hit = i

    if heading_hit is not None:
        return heading_hit + 1, _collect_from(lines, heading_hit, paragraph)
    if plain_hit is not None:
        return plain_hit + 1, _collect_from(lines, plain_hit, paragraph)
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("code", help="Код нормы, например 'СП 256.1325800.2016'")
    ap.add_argument("paragraph", help="Номер пункта, например '15.30'")
    ap.add_argument(
        "--max-lines",
        type=int,
        default=0,
        help="Лимит строк вывода (0 = без лимита)",
    )
    args = ap.parse_args()

    if not VAULT.exists():
        print(f"ERROR: vault not found: {VAULT}", file=sys.stderr)
        return 1

    file = find_file(args.code)
    if file is None:
        print(
            f"ERROR: файл не найден для кода '{args.code}' "
            f"(искали '{normalize_user_code(args.code)}*' в {VAULT})",
            file=sys.stderr,
        )
        return 1

    result = find_paragraph(file, args.paragraph)
    if result is None:
        print(
            f"ERROR: пункт '{args.paragraph}' не найден в {file.name}",
            file=sys.stderr,
        )
        return 1

    line_num, lines = result
    print(f"# Файл: {file.name}", file=sys.stderr)
    print(f"# Строка: {line_num}", file=sys.stderr)

    if args.max_lines and len(lines) > args.max_lines:
        for line in lines[: args.max_lines]:
            print(line)
        print(
            f"# (обрезано: {len(lines) - args.max_lines} строк ещё)",
            file=sys.stderr,
        )
    else:
        for line in lines:
            print(line)

    return 0


if __name__ == "__main__":
    sys.exit(main())
