"""Извлекает кандидатов на doc_status=cancelled из шапок vault MD.

Ищет прямые статусные сигналы вроде "ОТМЕНЕН", "УТРАТИЛ СИЛУ",
"ДОКУМЕНТ НЕ ДЕЙСТВУЕТ" в первых PREAMBLE_LINES строках документа.

Принцип работы консервативный:
  - анализируем только начало файла;
  - игнорируем служебные примечания про ссылочные документы;
  - игнорируем "взамен"/"заменен"/"в связи с введением в действие";
  - игнорируем частичные отмены ("в части", "абзац утратил силу", "п. 3.2");
  - в YAML сохраняем только medium/high.

Результат — черновик tools/cancellations_draft.yaml.

Запуск:
    python3 tools/extract_cancellations.py
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from parse_filename import parse_filename  # noqa: E402

VAULT = HERE.parent / "vault"
OUTPUT = HERE / "cancellations_draft.yaml"

PREAMBLE_LINES = 320
SNIPPET_RADIUS = 2
HEADER_ZONE_LINES = 120

_STATUS_RE = re.compile(
    r"\b("
    r"отмен[её]н(?:\s+с\b)?|"
    r"утратил[аи]?\s+силу|"
    r"документ\s+не\s+действует|"
    r"не\s+действует|"
    r"прекратил(?:а|о)?\s+действие"
    r")\b",
    re.IGNORECASE,
)

_DIRECT_STATUS_LINE_RE = re.compile(
    r"""^\s*
        (?:[*#>\-–—\[\]()\.]+\s*)*
        (?:
            документ\s+не\s+действует|
            не\s+действует|
            отмен[её]н(?:\s+с\b.*)?|
            утратил[аи]?\s+силу(?:\s+с\b.*)?|
            прекратил(?:а|о)?\s+действие(?:\s+с\b.*)?
        )
        [\s.!:;,\-–—()"]*
        $
    """,
    re.IGNORECASE | re.VERBOSE,
)

_PARTIAL_RE = re.compile(
    r"\b("
    r"в\s+части|"
    r"частично|"
    r"абзац|"
    r"пункт|пункты|п\.|пп\.|подпункт|"
    r"раздел|глава|статья|ст\."
    r")\b",
    re.IGNORECASE,
)

_REPLACEMENT_RE = re.compile(
    r"\b("
    r"взамен|"
    r"введ[её]н\s+взамен|"
    r"замен[её]н|"
    r"замен[аы]ющ|"
    r"в\s+связи\s+с\s+введением\s+в\s+действие|"
    r"действуют\b"
    r")",
    re.IGNORECASE,
)

_REFERENCE_NOISE_RE = re.compile(
    r"\b("
    r"при\s+пользовании|"
    r"ссылочн\w+|"
    r"положение,\s+в\s+котором\s+дана\s+ссылка|"
    r"настоящим\s+(?:стандартом|сводом\s+правил|документом)|"
    r"подготовлен\s+на\s+основе\s+применения|"
    r"консультантплюс:\s*примечание|"
    r"список\s+изменяющих\s+документов"
    r")\b",
    re.IGNORECASE,
)

_STRUCTURAL_NOISE_RE = re.compile(
    r"""^\s*(
        -\s*\d+[\.)]|
        \d+(?:\.\d+){0,3}\.?|
        [а-яa-z]\)
    )\s+""",
    re.IGNORECASE | re.VERBOSE,
)

_CODE_RES: list[tuple[str, re.Pattern[str]]] = [
    ("ГОСТ Р", re.compile(r"ГОСТ\s+Р\s+\d[\d\._\-/]*(?:[-:]\d{2,4})?")),
    ("ГОСТ", re.compile(r"(?<!Р\s)ГОСТ\s+\d[\d\._\-/]*(?:[-:]\d{2,4})?")),
    ("СНиП", re.compile(r"СНиП\s+[\dIVX][\dIVX\._\-]*(?:-\d{2,4})?")),
    ("СП", re.compile(r"СП\s+\d[\d\._\-]*")),
    ("ВСН", re.compile(r"ВСН\s+\d[\d\._\-/]*")),
    ("МДС", re.compile(r"МДС\s+\d[\d\._\-/]*")),
    ("РД", re.compile(r"РД\s+\d[\d\._\-/]*")),
    ("ПУЭ", re.compile(r"(?:ПУЭ|ПЭУ)\s*\d+(?:[\._-]\d+)*")),
    ("ФЗ", re.compile(r"\d+-ФЗ")),
]


def _match_key(s: str) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", "", s).replace("_", ".").lower()


def _clean_code(code: str) -> str:
    code = code.strip()
    code = re.sub(r"[/.,;:\s]+$", "", code)
    code = re.sub(r"\s+", " ", code)
    return code


def _clean_markdown(s: str) -> str:
    s = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", s)
    s = s.replace("*", "")
    s = s.replace("#", "")
    s = s.replace("`", "")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _extract_codes(text: str) -> list[str]:
    cleaned = _clean_markdown(text)
    found: list[str] = []
    seen: set[str] = set()
    for _, rx in _CODE_RES:
        for match in rx.finditer(cleaned):
            code = _clean_code(match.group(0))
            key = _match_key(code)
            if not key or key in seen:
                continue
            seen.add(key)
            found.append(code)
    return found


def _normalize_snippet(lines: list[str]) -> str:
    return _clean_markdown(" ".join(line.strip() for line in lines if line.strip()))


def _is_status_zone_line(line_no: int) -> bool:
    return line_no <= HEADER_ZONE_LINES


def _classify_candidate(code: str, line_no: int, line: str, snippet: str) -> tuple[str | None, str]:
    low_line = line.lower()
    low_snippet = snippet.lower()

    if _REFERENCE_NOISE_RE.search(low_snippet):
        return None, "reference_noise"
    if _PARTIAL_RE.search(low_snippet):
        return None, "partial_status"
    if _REPLACEMENT_RE.search(low_snippet):
        return None, "replacement_context"
    if _STRUCTURAL_NOISE_RE.match(line):
        return None, "section_or_list_item"
    if "|" in line or "|" in snippet:
        return None, "table_or_editorial"

    current_key = _match_key(code)
    foreign_codes = [
        found for found in _extract_codes(snippet)
        if _match_key(found) != current_key
    ]
    if foreign_codes:
        return None, "foreign_code_context"

    direct_line = _DIRECT_STATUS_LINE_RE.match(_clean_markdown(line)) is not None
    explicit_doc_phrase = bool(re.search(r"\bдокумент\s+не\s+действует\b", low_snippet))
    short_line = len(_clean_markdown(line)) <= 160
    short_snippet = len(snippet) <= 220
    in_header_zone = _is_status_zone_line(line_no)

    score = 0
    if in_header_zone:
        score += 2
    if direct_line:
        score += 2
    if explicit_doc_phrase:
        score += 2
    if short_line:
        score += 1
    if short_snippet:
        score += 1
    if "утратил силу" in low_snippet and "документ" not in low_snippet and not direct_line:
        score -= 1

    if score >= 5:
        return "high", "direct_header_status"
    if score >= 3:
        return "medium", "possible_header_status"
    return None, "low_signal"


def scan_file(path: Path) -> dict | None:
    parsed = parse_filename(path.name)
    code = _clean_code(parsed["code"])
    if not code:
        return None

    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None

    lines = text.splitlines()[:PREAMBLE_LINES]
    best: dict | None = None

    for idx, raw_line in enumerate(lines):
        match = _STATUS_RE.search(raw_line)
        if not match:
            continue

        start = max(0, idx - SNIPPET_RADIUS)
        end = min(len(lines), idx + SNIPPET_RADIUS + 1)
        snippet = _normalize_snippet(lines[start:end])
        matched_text = _clean_markdown(match.group(0))
        confidence, reason = _classify_candidate(code, idx + 1, raw_line, snippet)
        if confidence is None:
            continue

        candidate = {
            "code": code,
            "proposed_status": "cancelled",
            "confidence": confidence,
            "reason": reason,
            "source_file": path.name,
            "line_no": idx + 1,
            "matched_text": matched_text,
            "snippet": snippet[:400],
        }

        if best is None:
            best = candidate
            continue

        priority = {"high": 2, "medium": 1}
        current = priority[candidate["confidence"]]
        saved = priority[best["confidence"]]
        if current > saved or (current == saved and candidate["line_no"] < best["line_no"]):
            best = candidate

    return best


def emit_yaml(items: list[dict], scanned_files: int) -> str:
    high = sum(1 for item in items if item["confidence"] == "high")
    medium = sum(1 for item in items if item["confidence"] == "medium")
    payload = {
        "summary": {
            "scanned_files": scanned_files,
            "candidates_total": len(items),
            "high": high,
            "medium": medium,
        },
        "items": items,
    }
    return yaml.safe_dump(
        payload,
        allow_unicode=True,
        sort_keys=False,
        width=1000,
    )


def main() -> int:
    if not VAULT.exists():
        print(f"ERROR: vault не найден: {VAULT}", file=sys.stderr)
        return 1

    scanned = 0
    found: list[dict] = []
    for md in sorted(VAULT.glob("*.md")):
        if md.name.startswith("MOC - "):
            continue
        scanned += 1
        candidate = scan_file(md)
        if candidate is not None:
            found.append(candidate)

    found.sort(key=lambda item: (item["confidence"] != "high", item["code"], item["source_file"]))
    OUTPUT.write_text(emit_yaml(found, scanned), encoding="utf-8")

    high = sum(1 for item in found if item["confidence"] == "high")
    medium = sum(1 for item in found if item["confidence"] == "medium")
    print(f"Просканировано MD: {scanned}", file=sys.stderr)
    print(f"Кандидатов cancelled: {len(found)}", file=sys.stderr)
    print(f"  high:   {high}", file=sys.stderr)
    print(f"  medium: {medium}", file=sys.stderr)
    print(f"Черновик записан: {OUTPUT}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
