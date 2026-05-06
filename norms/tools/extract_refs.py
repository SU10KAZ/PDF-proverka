"""Извлекает ссылки на нормы из тела каждого MD-файла.

Что делает:
1. Собирает `refs_graph.json` — граф цитирований (кто на кого ссылается).
2. Инъектирует в каждый `.md`:
   - YAML frontmatter с каноничным кодом, типом, годом, алиасом
   - Секцию `## Связанные нормы` в конце файла

Идемпотентно: повторный запуск перезаписывает frontmatter и секцию,
основной текст не трогает.

Использование:
    python3 extract_refs.py --dry-run          # только собрать граф, не писать
    python3 extract_refs.py --limit 5          # обработать 5 файлов
    python3 extract_refs.py                    # полный проход
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from parse_filename import parse_filename  # noqa: E402

VAULT = Path(__file__).resolve().parent.parent / "vault"
ACTIVE_JSON = Path(__file__).parent / "active_norms.json"
OUTPUT_JSON = Path(__file__).parent / "refs_graph.json"

# Узкие паттерны раньше широких (ГОСТ Р до ГОСТ).
REF_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("ГОСТ Р", re.compile(r"ГОСТ\s+Р\s+(\d[\d\.\-/]*\d|\d)")),
    ("ГОСТ", re.compile(r"ГОСТ\s+(?!Р\s)(\d[\d\.\-/]*\d|\d)")),
    ("СП", re.compile(r"СП\s+(\d[\d\.\-]*\d)")),
    ("СНиП", re.compile(r"СНиП\s+(\d[\d\.\-\*]*\d)")),
    ("ВСН", re.compile(r"ВСН\s+(\d[\d\.\-]*\d)")),
    ("МДС", re.compile(r"МДС\s+(\d[\d\.\-]*\d)")),
    ("РД", re.compile(r"РД\s+(\d[\d\.\-]*\d)")),
]

FZ_PATTERN = re.compile(r"[Nn№]\s*(\d+)[-–]ФЗ")

SECTION_MARKER = "## Связанные нормы"


def load_active_norms() -> dict[str, dict]:
    """Загружает active_norms.json → map каноничный_код → запись нормы."""
    data = json.loads(ACTIVE_JSON.read_text(encoding="utf-8"))
    return {n["code"]: n for n in data["norms"]}


# Год в конце кода: "-82", "-2012", ".2016"
_YEAR_SUFFIX = re.compile(r"[-.]\d{2,4}$")


def _strip_year(code: str) -> str:
    """'ГОСТ 10434-82' → 'ГОСТ 10434'; 'СП 256.1325800.2016' → 'СП 256.1325800'."""
    return _YEAR_SUFFIX.sub("", code)


def build_loose_index(known_codes: dict[str, dict]) -> dict[str, list[str]]:
    """Map: код_без_года → [полные_коды в vault'е].

    Нужно чтобы 'ГОСТ 10434' в тексте (без года) разрешился в 'ГОСТ 10434-82'.
    Если на короткий префикс приходится >1 нормы — оставляем неоднозначным.
    """
    loose: dict[str, list[str]] = {}
    for code in known_codes:
        short = _strip_year(code)
        if short == code:
            continue  # не было года в коде — пропускаем
        loose.setdefault(short, []).append(code)
    return loose


def resolve_code(code: str, known: dict[str, dict], loose: dict[str, list[str]]) -> str | None:
    """Возвращает каноничный код в vault'е или None."""
    if code in known:
        return code
    # Loose-match: код без года → один кандидат
    candidates = loose.get(code, [])
    if len(candidates) == 1:
        return candidates[0]
    return None


def dedupe_unknown(codes: set[str]) -> list[str]:
    """Убирает 'ГОСТ X', если в наборе есть 'ГОСТ X-год'."""
    full_forms = {c for c in codes if _YEAR_SUFFIX.search(c)}
    short_to_drop = {_strip_year(c) for c in full_forms}
    return sorted(codes - short_to_drop)


def extract_refs_from_text(text: str) -> tuple[set[str], set[str]]:
    """Вытаскивает упоминания норм и ФЗ. Возвращает (norm_codes, fz_refs)."""
    norm_codes: set[str] = set()
    for type_, pattern in REF_PATTERNS:
        for m in pattern.finditer(text):
            num = m.group(1).strip().rstrip("-.")
            if num:
                norm_codes.add(f"{type_} {num}")
    fz_refs: set[str] = {f"№ {m.group(1)}-ФЗ" for m in FZ_PATTERN.finditer(text)}
    return norm_codes, fz_refs


def strip_existing_frontmatter(content: str) -> str:
    """Если начинается с '---\\n...\\n---\\n', отрезает блок."""
    if not content.startswith("---\n"):
        return content
    end = content.find("\n---\n", 4)
    if end == -1:
        return content
    return content[end + 5:]


def strip_existing_refs_section(body: str) -> str:
    """Удаляет секцию '## Связанные нормы' (от заголовка до следующей ## или EOF).

    Соседние секции (например '## Похожие по смыслу') не трогаются.
    """
    pattern = re.compile(
        rf"\n{re.escape(SECTION_MARKER)}\s*\n.*?(?=\n## |\Z)",
        re.DOTALL,
    )
    return pattern.sub("\n", body).rstrip() + "\n"


def _yaml_escape(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"')


def build_frontmatter(code: str, norm_type: str, title: str, year: int | None) -> str:
    lines = ["---", f'code: "{_yaml_escape(code)}"', f'type: "{_yaml_escape(norm_type)}"']
    if year:
        lines.append(f"year: {year}")
    lines.append(f'aliases:\n  - "{_yaml_escape(code)}"')
    if title:
        lines.append(f'title: "{_yaml_escape(title)}"')
    lines.append("---")
    lines.append("")
    return "\n".join(lines)


def build_refs_section(
    cited_in_vault: list[tuple[str, str]],
    cited_not_in_vault: list[str],
    fz_refs: list[str],
) -> str:
    lines = ["", SECTION_MARKER, ""]
    if cited_in_vault:
        lines.append("**Цитируется в тексте (есть в vault'е):**")
        lines.append("")
        for code, fname in sorted(cited_in_vault):
            stem = fname[:-3] if fname.endswith(".md") else fname
            lines.append(f"- [[{stem}|{code}]]")
        lines.append("")
    if cited_not_in_vault:
        lines.append("**Упоминается, но не в vault'е:**")
        lines.append("")
        for code in sorted(cited_not_in_vault):
            lines.append(f"- {code}")
        lines.append("")
    if fz_refs:
        lines.append("**Федеральные законы:**")
        lines.append("")
        for fz in sorted(fz_refs):
            lines.append(f"- {fz}")
        lines.append("")
    if not (cited_in_vault or cited_not_in_vault or fz_refs):
        lines.append("_Ссылок на другие нормы не найдено._")
        lines.append("")
    return "\n".join(lines)


def process_file(
    md_path: Path,
    known_codes: dict[str, dict],
    loose_index: dict[str, list[str]],
    graph: dict,
    write: bool,
) -> dict:
    """Обрабатывает один файл. Возвращает stats."""
    parsed = parse_filename(md_path.name)
    if parsed["parse_confidence"] == "low":
        return {"skipped": True, "reason": "low_confidence"}

    self_code = parsed["code"]
    content = md_path.read_text(encoding="utf-8")
    body = strip_existing_refs_section(strip_existing_frontmatter(content))

    norm_codes, fz_refs = extract_refs_from_text(body)
    norm_codes.discard(self_code)

    # Разрешаем: строгое совпадение или loose по префиксу без года.
    resolved: dict[str, str] = {}  # raw_mention → canonical_code
    unknown: set[str] = set()
    for code in norm_codes:
        c = resolve_code(code, known_codes, loose_index)
        if c and c != self_code:
            resolved[code] = c
        elif not c:
            unknown.add(code)

    # Уникальные цитируемые нормы (после разрешения)
    cited_in_vault: list[tuple[str, str]] = sorted(
        {(c, known_codes[c]["file"]) for c in resolved.values()}
    )
    cited_not_in_vault: list[str] = dedupe_unknown(unknown)

    graph[self_code] = {
        "file": md_path.name,
        "cited_in_vault": [c for c, _ in cited_in_vault],
        "cited_unknown": cited_not_in_vault,
        "fz_refs": sorted(fz_refs),
    }

    if write:
        fm = build_frontmatter(self_code, parsed["type"], parsed["title"], parsed["year"])
        refs = build_refs_section(cited_in_vault, cited_not_in_vault, sorted(fz_refs))
        new_content = fm + body.rstrip() + "\n" + refs
        md_path.write_text(new_content, encoding="utf-8")

    return {
        "skipped": False,
        "refs_in_vault": len(cited_in_vault),
        "refs_unknown": len(cited_not_in_vault),
        "fz_refs": len(fz_refs),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="Только собрать refs_graph.json, файлы не трогать")
    ap.add_argument("--limit", type=int, help="Обработать только N файлов (для отладки)")
    args = ap.parse_args()

    known_codes = load_active_norms()
    loose_index = build_loose_index(known_codes)
    print(f"Известных норм в vault'е: {len(known_codes)}", file=sys.stderr)
    print(f"Loose-индекс (код без года): {len(loose_index)}", file=sys.stderr)

    graph: dict = {}
    total = skipped = total_refs = 0

    files = sorted(VAULT.glob("*.md"))
    if args.limit:
        files = files[: args.limit]

    for md in files:
        total += 1
        try:
            r = process_file(md, known_codes, loose_index, graph, write=not args.dry_run)
            if r["skipped"]:
                skipped += 1
            else:
                total_refs += r["refs_in_vault"]
        except Exception as e:
            print(f"ERROR: {md.name}: {e}", file=sys.stderr)
            skipped += 1

    output = {
        "meta": {
            "total_nodes": len(graph),
            "total_edges_in_vault": total_refs,
            "skipped": skipped,
            "dry_run": args.dry_run,
        },
        "graph": graph,
    }
    OUTPUT_JSON.write_text(
        json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(f"Обработано: {total}", file=sys.stderr)
    print(f"Пропущено (low_confidence или ошибка): {skipped}", file=sys.stderr)
    print(f"Связей (в vault'е): {total_refs}", file=sys.stderr)
    print(f"Граф сохранён: {OUTPUT_JSON}", file=sys.stderr)
    if args.dry_run:
        print("DRY-RUN: файлы не изменялись", file=sys.stderr)


if __name__ == "__main__":
    main()
