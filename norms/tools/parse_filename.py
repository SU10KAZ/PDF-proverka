"""Парсер имён MD-файлов нормативной базы.

Вход: имя файла вида "СП 256_1325800_2016_ Свод правил_ ... _document.md".
Выход: dict с полями type, code, code_raw, year, title, parse_confidence.
"""
from __future__ import annotations

import re
from pathlib import Path

# (type, regex). Узкие паттерны раньше широких.
_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("ГОСТ Р", re.compile(r"^ГОСТ\s+Р\s+([\d_\-\.]+)")),
    ("ГОСТ", re.compile(r"^ГОСТ\s+([\d_\-\.]+)")),
    ("СНиП", re.compile(r"^СНиП\s+([\d_\-\.]+)")),
    ("СП", re.compile(r"^СП\s+([\d_\-\.]+)")),
    ("ВСН", re.compile(r"^ВСН\s+([\d_\-\.]+)")),
    ("МДС", re.compile(r"^МДС\s+([\d_\-\.]+)")),
    ("РД", re.compile(r"^РД\s+([\d_\-\.]+)")),
    ("ПУЭ", re.compile(r"^(?:ПУЭ|ПЭУ)\s*(\d+)")),
    ("ФЗ", re.compile(r"^Федеральный закон.*?N\s+(\d+-ФЗ)")),
]

_OTHER_PREFIXES = (
    "Постановление",
    "Приказ",
    "Распоряжение",
    "Решение",
    "Определение",
    "Письмо",
    "Информационное",
    "Инструкция",
    "Методика",
    "Изменение",
    "Поправка",
    "Градостроительный",
    "Справочная",
)

_SUFFIX = "_document.md"


def _strip_suffix(name: str) -> str:
    if name.endswith(_SUFFIX):
        return name[: -len(_SUFFIX)]
    if name.endswith(".md"):
        return name[:-3]
    return name


def _extract_year(code: str) -> int | None:
    """Год — последние 2–4 цифры после последнего разделителя в коде."""
    m = re.search(r"[\-_](\d{2,4})\s*$", code)
    if not m:
        m = re.search(r"(\d{4})\s*$", code)
    if not m:
        return None
    y = int(m.group(1))
    if y < 100:
        y += 1900  # 85 → 1985, 02 → 1902 (старых норм с двухзначным годом — до 2000)
    return y


def _normalize_code(type_: str, code_raw: str) -> str:
    """Каноничная форма: для СП/ГОСТ Р с номером XXX_XXXXXXX_YYYY → точки."""
    if type_ in ("СП", "ГОСТ Р") and code_raw.count("_") >= 2:
        # СП 256_1325800_2016 → СП 256.1325800.2016
        return f"{type_} {code_raw.replace('_', '.')}"
    return f"{type_} {code_raw}"


def _extract_title(stem: str, match_end: int) -> str:
    """Название — после кода, до конца имени. Обрезаем до 80 симв."""
    tail = stem[match_end:].strip()
    tail = tail.lstrip("_ ").strip()
    if len(tail) > 80:
        tail = tail[:77].rstrip() + "..."
    return tail


def parse_filename(name: str) -> dict:
    """Разобрать имя MD-файла нормы.

    Возвращает: {type, code, code_raw, year, title, file, parse_confidence}.
    """
    file = name
    stem = _strip_suffix(name)

    for type_, regex in _PATTERNS:
        m = regex.match(stem)
        if not m:
            continue
        code_raw = m.group(1).rstrip("_- ")
        code = _normalize_code(type_, code_raw)
        year = _extract_year(code_raw)
        title = _extract_title(stem, m.end())
        return {
            "type": type_,
            "code": code,
            "code_raw": code_raw,
            "year": year,
            "title": title,
            "file": file,
            "parse_confidence": "high",
        }

    # Неструктурированные документы (приказы, постановления и т.д.)
    for prefix in _OTHER_PREFIXES:
        if stem.startswith(prefix):
            return {
                "type": "other",
                "code": stem[:80],
                "code_raw": stem[:80],
                "year": _extract_year(stem),
                "title": stem[:80],
                "file": file,
                "parse_confidence": "low",
            }

    return {
        "type": "unknown",
        "code": stem[:80],
        "code_raw": stem[:80],
        "year": None,
        "title": stem[:80],
        "file": file,
        "parse_confidence": "low",
    }


def normalize_user_code(user_code: str) -> str:
    """Нормализовать код от пользователя для поиска файла.

    'СП 256.1325800.2016' → 'СП 256_1325800_2016'
    'ГОСТ 10180-2012' → 'ГОСТ 10180-2012'
    """
    return user_code.strip().replace(".", "_")


if __name__ == "__main__":
    # Самотест: проверка на реальных именах.
    import json
    import sys

    vault = Path(__file__).resolve().parent.parent / "vault"
    if not vault.exists():
        print(f"ERROR: vault not found: {vault}", file=sys.stderr)
        sys.exit(1)

    stats: dict[str, int] = {}
    low_conf: list[str] = []
    for md in sorted(vault.glob("*.md")):
        r = parse_filename(md.name)
        stats[r["type"]] = stats.get(r["type"], 0) + 1
        if r["parse_confidence"] == "low":
            low_conf.append(f"{r['type']:10s} {md.name}")

    print(json.dumps(stats, ensure_ascii=False, indent=2))
    print(f"\nlow_confidence: {len(low_conf)}")
    for line in low_conf[:20]:
        print(f"  {line}")
