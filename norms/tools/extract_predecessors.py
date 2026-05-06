"""Извлекает пары (предшественник → текущая норма) из преамбул vault MD.

Ищет заголовки вида "N. Взамен ..." / "#### N ВЗАМЕН ..." / "ВВЕДЕН ВЗАМЕН ..."
в первых PREAMBLE_LINES строках каждого документа и достаёт оттуда коды
предшественников.

Результат — черновик tools/predecessors_draft.yaml. Никакой автоматической
записи в status_overrides.yaml: черновик предназначен для ручного ревью.

Запуск:
    python3 tools/extract_predecessors.py
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from parse_filename import parse_filename  # noqa: E402

VAULT = HERE.parent / "vault"
OUTPUT = HERE / "predecessors_draft.yaml"

PREAMBLE_LINES = 300
WINDOW_LINES = 3  # сколько строк смотреть после заголовка "Взамен"

# Строка-заголовок раздела "Взамен" в преамбуле. Ловим обёртки вида:
#   "4. Взамен"              (классический ГОСТ)
#   "#### 5 ВЗАМЕН"          (новый формат ГОСТ)
#   "5 ВЗАМЕН"               (без #)
#   "- 4 ВЗАМЕН"             (листинг)
#   "ВВЕДЕН ВЗАМЕН"          (приказы / новые СП)
_HEADER_RE = re.compile(
    r"""^\s*
        (?:[#]{1,6}\s*)?                  # markdown-заголовок
        (?:[-–—*]\s*)?                    # маркер списка
        (?:\*{1,2})?                      # начало жирного
        (?:\d+[\.)]?\s+)?                 # номер пункта "4." / "5)"
        (?:\*{1,2})?                      # снова **
        (?:введен\s+)?                    # "ВВЕДЕН ВЗАМЕН"
        взамен\b
        [\s\*:\.]*                        # хвост до кода
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Шумовые фразы — описание ЧУЖОЙ цепочки, не про текущий документ.
# "Взамен ГОСТ X Постановлением Госстандарта ... введен в действие ГОСТ Y"
# встречается в списке ссылок текущего документа на другие нормы.
_NOISE_RE = re.compile(
    r"постановлен|введ[её]н\s+в\s+действие|приказом\s+рос|примечан",
    re.IGNORECASE,
)

# Извлечение кодов из произвольной строки (после чистки markdown).
# Порядок важен: "ГОСТ Р" проверяем раньше "ГОСТ".
_CODE_RES: list[tuple[str, re.Pattern[str]]] = [
    ("ГОСТ Р", re.compile(r"ГОСТ\s+Р\s+\d[\d\._\-/]*(?:[-:]\d{2,4})?")),
    ("ГОСТ",   re.compile(r"(?<!Р\s)ГОСТ\s+\d[\d\._\-/]*(?:[-:]\d{2,4})?")),
    ("СНиП",   re.compile(r"СНиП\s+[\dIVX][\dIVX\._\-]*(?:-\d{2,4})?")),
    ("СП",     re.compile(r"СП\s+\d[\d\._\-]*")),
    ("ВСН",    re.compile(r"ВСН\s+\d[\d\._\-/]*")),
    ("МДС",    re.compile(r"МДС\s+\d[\d\._\-/]*")),
    ("РД",     re.compile(r"РД\s+\d[\d\._\-/]*")),
]

# Типы, для которых парсинг имени файла надёжно даёт current code.
# Приказы/постановления пропускаем — у них в имени нет кода утверждённого СП.
_TARGET_TYPES = {"ГОСТ", "ГОСТ Р", "СП", "СНиП", "ВСН", "МДС", "РД"}


def _clean_markdown(s: str) -> str:
    """Снимает markdown-обёртки, чтобы регексы кодов работали чисто."""
    s = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", s)  # [X](#) → X
    s = s.replace("*", "")
    s = re.sub(r"\s+", " ", s)
    return s


def _extract_codes(text: str) -> list[tuple[str, str]]:
    cleaned = _clean_markdown(text)
    found: list[tuple[str, str]] = []
    seen: set[str] = set()
    for type_, rx in _CODE_RES:
        for m in rx.finditer(cleaned):
            code = m.group(0).strip().rstrip(".,;:")
            code = re.sub(r"\s+", " ", code)
            if code in seen:
                continue
            seen.add(code)
            found.append((type_, code))
    return found


def _norm_key(code: str) -> str:
    return re.sub(r"\s+", "", code).replace("_", ".").lower()


def scan_file(path: Path) -> list[dict]:
    parsed = parse_filename(path.name)
    if parsed["type"] not in _TARGET_TYPES:
        return []
    current = parsed["code"]
    if not current:
        return []
    current_key = _norm_key(current)

    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []

    lines = text.splitlines()[:PREAMBLE_LINES]
    results: list[dict] = []
    reported_in_file: set[str] = set()

    for i, line in enumerate(lines):
        if not _HEADER_RE.match(line):
            continue
        window = " ".join(lines[i : i + WINDOW_LINES])
        if _NOISE_RE.search(window):
            continue
        codes = _extract_codes(window)
        if not codes:
            continue
        # Confidence: "high" — если код есть в той же строке что и "Взамен".
        same_line_codes = {c for _, c in _extract_codes(line)}
        for type_, code in codes:
            key = _norm_key(code)
            if key == current_key:
                continue
            if key in reported_in_file:
                continue
            reported_in_file.add(key)
            results.append(
                {
                    "old_code": code,
                    "old_type": type_,
                    "current": current,
                    "source_file": path.name,
                    "source_line": i + 1,
                    "context": line.strip()[:200],
                    "confidence": "high" if code in same_line_codes else "medium",
                }
            )
    return results


def _yaml_escape(s: str) -> str:
    return s.replace('"', '\\"')


def emit_yaml(pairs: list[dict]) -> str:
    lines: list[str] = [
        "# AUTO-GENERATED by tools/extract_predecessors.py",
        "# Не мержить автоматически в status_overrides.yaml.",
        "# Человеку: просмотреть пары, отсеять ложные срабатывания,",
        "# перенести валидные записи в status_overrides.yaml как:",
        "#",
        "#   <old_code>:",
        "#     doc_status: replaced",
        "#     replacement_doc: <current>",
        "#     details: \"Извлечено из преамбулы <source_file>\"",
        "#     last_verified: <дата ревью>",
        "#",
        f"# total: {len(pairs)}",
        "",
        "predecessors:",
    ]
    for p in pairs:
        lines.append(f"  - old_code: \"{_yaml_escape(p['old_code'])}\"")
        lines.append(f"    old_type: \"{_yaml_escape(p['old_type'])}\"")
        lines.append(f"    current: \"{_yaml_escape(p['current'])}\"")
        lines.append(f"    source_file: \"{_yaml_escape(p['source_file'])}\"")
        lines.append(f"    source_line: {p['source_line']}")
        lines.append(f"    context: \"{_yaml_escape(p['context'])}\"")
        lines.append(f"    confidence: {p['confidence']}")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    if not VAULT.exists():
        print(f"ERROR: vault не найден: {VAULT}", file=sys.stderr)
        return 1

    all_pairs: list[dict] = []
    scanned = 0
    for md in sorted(VAULT.glob("*.md")):
        if md.name.startswith("MOC - "):
            continue
        scanned += 1
        all_pairs.extend(scan_file(md))

    # Дедуп по (old_code_key, current_key) — один источник на пару.
    dedup: dict[tuple[str, str], dict] = {}
    for p in all_pairs:
        key = (_norm_key(p["old_code"]), _norm_key(p["current"]))
        # Приоритет: high > medium.
        if key not in dedup or (
            p["confidence"] == "high" and dedup[key]["confidence"] != "high"
        ):
            dedup[key] = p

    pairs = sorted(
        dedup.values(),
        key=lambda x: (x["old_type"], x["old_code"], x["source_file"]),
    )

    OUTPUT.write_text(emit_yaml(pairs), encoding="utf-8")

    by_conf: dict[str, int] = {}
    by_type: dict[str, int] = {}
    for p in pairs:
        by_conf[p["confidence"]] = by_conf.get(p["confidence"], 0) + 1
        by_type[p["old_type"]] = by_type.get(p["old_type"], 0) + 1

    print(f"Просканировано MD: {scanned}", file=sys.stderr)
    print(f"Найдено пар (после дедупа): {len(pairs)}", file=sys.stderr)
    print(f"  по confidence: {by_conf}", file=sys.stderr)
    print(f"  по типу:       {by_type}", file=sys.stderr)
    print(f"Сохранено → {OUTPUT.name}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
