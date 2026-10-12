"""Приём пакета новых MD-норм в формат vault.

Новые выгрузки (Chandra, КонсультантПлюс) приходят с именами вида
``ГОСТ_10180-2012__Межгосударственный_стандарт__Бетоны_results.md``: без
frontmatter, со строками ``> **Created:**`` / ``> **Crop:**`` в каждом блоке.
``parse_filename`` такие имена не распознаёт, а код нормы build_status_index
берёт ТОЛЬКО из имени файла. Скрипт переписывает пакет в отдельную папку
в конвенции vault (``ГОСТ 10180-2012_ Название_document.md``) и пишет отчёт.

Обозначение определяется так:
  1. заголовок КонсультантПлюс в начале текста ("ГОСТ 10180-2012. ...");
  2. имя файла (подчёркивания в нём означают и пробел, и точку);
  3. расхождение 1 и 2, дубль или нераспознанное имя → в отчёт на ручной разбор.

Vault, status_index и прод скрипт не трогает: пишет только в --out.

Запуск:
    python3 intake_vault_batch.py --src /home/coder/norms_incoming/2026-10-12 \\
        --out /path/to/vault_candidate --report /path/to/intake_report.json
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent))
from build_status_index import _match_key  # noqa: E402
from parse_filename import parse_filename  # noqa: E402

HERE = Path(__file__).parent
STATUS_INDEX = HERE / "status_index.json"
DESIGNATION_MAP = HERE / "intake_designation_map.yaml"
_MAPPED_CODE_RE = re.compile(r"^(ГОСТ Р|ГОСТ|СанПиН|СНиП|СП|ПУЭ)\s+(\S+)$")

# Префиксы типов в именах новых файлов: узкие раньше широких.
_FILENAME_TYPES: list[tuple[str, str]] = [
    ("ГОСТ_Р_ИСО_", "ГОСТ Р ИСО"),
    ("ГОСТ_Р_МЭК_", "ГОСТ Р МЭК"),
    ("ГОСТ_Р_", "ГОСТ Р"),
    ("ГОСТ_IEC_", "ГОСТ IEC"),
    ("ГОСТ_ISO_", "ГОСТ ISO"),
    ("ГОСТ_", "ГОСТ"),
    ("СанПиН_", "СанПиН"),
    ("СНиП_", "СНиП"),
    ("СП_", "СП"),
    ("ВСН_", "ВСН"),
    ("МДС_", "МДС"),
    ("РД_", "РД"),
]
_CODE_RAW_RE = re.compile(r"^[0-9][0-9_\-.]*[0-9]")

# Обозначение в заголовке КонсультантПлюс: "ГОСТ 10180-2012. Межгосударственный ..."
_CONTENT_TYPE = (
    r"ГОСТ Р ИСО|ГОСТ Р МЭК|ГОСТ Р|ГОСТ IEC|ГОСТ ISO|ГОСТ|СанПиН|СНиП|СП|ВСН|МДС|РД"
)
_CONTENT_RE = re.compile(
    r'"\s*(' + _CONTENT_TYPE + r')\s+([0-9][0-9A-Za-z.\-/ ]*?[0-9])\*?(?=\.?[\s"(]|\.?$)'
)
_CONTENT_HEAD_CHARS = 6000

_MIN_TEXT_BYTES = 4000
_NOISE_LINE_RE = re.compile(r"^>\s*\*\*(Created|Crop):\*\*")
_PAGE_RE = re.compile(r"^## Page (\d+)\s*$")


def _key(code: str) -> str:
    """Ключ сравнения обозначений: как в status_index, плюс без '*' и '-'/'.'-путаницы."""
    return _match_key(code).replace("*", "")


def _filename_stem(name: str) -> str:
    for suffix in ("_results.md", "_document.md", ".md"):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


def _humanize(text: str) -> str:
    """'Бетоны__Методы_от_06_04_2021' → 'Бетоны_ Методы от 06_04_2021'.

    '__' в новых именах стоит на месте '. ' — в vault это '_ '. Одиночное '_'
    между цифрами (даты, номера) сохраняем, остальные — пробел.
    """
    text = text.replace("__", "\x00")
    text = re.sub(r"(?<![0-9])_|_(?![0-9])", " ", text)
    return re.sub(r" {2,}", " ", text.replace("\x00", "_ ")).strip(" _")


def designation_from_filename(stem: str) -> dict:
    head, _, tail = stem.partition("__")
    for prefix, type_ in _FILENAME_TYPES:
        if not head.startswith(prefix):
            continue
        m = _CODE_RAW_RE.match(head[len(prefix):])
        if not m:
            break
        code_raw = m.group(0)
        rest = head[len(prefix) + m.end():].strip("_")
        title = " ".join(p for p in (_humanize(rest), _humanize(tail)) if p)
        return {"type": type_, "code_raw": code_raw, "title": title}
    if re.match(r"^(ПЭУ|ПУЭ)_?7", head):
        return {"type": "ПУЭ", "code_raw": "7", "title": "Изд."}
    m = re.match(r"^ФЗ_(\d+-ФЗ)$", head)
    if m:
        return {"type": "ФЗ", "code_raw": m.group(1), "title": _humanize(tail)}
    m = re.match(r"^ПП_РФ_№_?(\d+)(.*)$", head)
    if m:
        return {"type": "ПП РФ", "code_raw": f"№ {m.group(1)}", "title": _humanize(m.group(2) + "__" + tail)}
    return {"type": "other", "code_raw": None, "title": _humanize(stem)}


def designation_from_content(text: str) -> dict | None:
    m = _CONTENT_RE.search(text[:_CONTENT_HEAD_CHARS])
    if not m:
        return None
    return {"type": m.group(1), "code": f"{m.group(1)} {m.group(2).strip()}"}


def _vault_name(type_: str, code_raw: str | None, title: str, fallback: str) -> str:
    if type_ == "ПУЭ":
        return f"ПУЭ {code_raw} Изд._document.md"
    if type_ == "ПП РФ":
        return f"ПП РФ {code_raw}_document.md"
    if code_raw is None:
        return f"{fallback[:120].rstrip()}_document.md"
    code_part = code_raw.replace(".", "_")
    name = f"{type_} {code_part}"
    if title:
        name += f"_ {title[:60].rstrip()}"
    return f"{name}_document.md"


def clean_body(text: str) -> str:
    out: list[str] = []
    for line in text.splitlines():
        if _NOISE_LINE_RE.match(line) or line.startswith("Path: "):
            continue
        m = _PAGE_RE.match(line)
        out.append(f"## СТРАНИЦА {m.group(1)}" if m else line)
    return re.sub(r"\n{3,}", "\n\n", "\n".join(out)).strip() + "\n"


def _frontmatter(parsed: dict, source_file: str, source: str) -> str:
    code = parsed["code"]
    aliases = sorted({code, code.replace(".", "_"), code.replace("_", ".")})
    lines = ["---", f"code: {json.dumps(code, ensure_ascii=False)}",
             f"type: {json.dumps(parsed['type'], ensure_ascii=False)}"]
    if parsed.get("year"):
        lines.append(f"year: {parsed['year']}")
    lines.append("aliases:")
    lines += [f"  - {json.dumps(a, ensure_ascii=False)}" for a in aliases]
    lines += [f"title: {json.dumps(parsed.get('title') or '', ensure_ascii=False)}",
              f"source_file: {json.dumps(source_file, ensure_ascii=False)}",
              f"designation_source: {source}", "---", ""]
    return "\n".join(lines)


def load_designation_map(path: Path = DESIGNATION_MAP) -> dict[str, str]:
    if not path.is_file():
        return {}
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return dict(data.get("designations") or {})


def process_file(path: Path, designation_map: dict[str, str]) -> dict:
    text = path.read_text(encoding="utf-8", errors="replace")
    stem = _filename_stem(path.name)
    by_name = designation_from_filename(stem)
    by_content = designation_from_content(text)
    rec: dict = {"source_file": path.name, "size": path.stat().st_size, "issues": []}
    mapped = next((code for prefix, code in designation_map.items() if stem.startswith(prefix)), None)

    if mapped:
        m = _MAPPED_CODE_RE.match(mapped)
        if not m:
            raise SystemExit(f"{DESIGNATION_MAP.name}: неподдерживаемый код {mapped!r}")
        source = "designation_map"
        type_, code_raw = m.group(1), m.group(2)
        by_name = {**by_name, "title": _humanize(stem)}
    elif by_name["type"] == "other" or by_name["type"] in ("ПУЭ", "ПП РФ"):
        # Приказы/письма/постановления: обозначение из текста не берём —
        # первое "ГОСТ ..." в нём обычно ссылка на чужой документ.
        source = "filename"
        type_, code_raw = by_name["type"], by_name["code_raw"]
    else:
        type_, code_raw = by_name["type"], by_name["code_raw"]
        name_code = f"{type_} {code_raw}"
        if by_content is None:
            source = "filename"
            rec["issues"].append("no_title_in_text")
        elif _key(by_content["code"]) == _key(name_code):
            source = "content+filename"
            type_ = by_content["type"]
            code_raw = by_content["code"][len(type_) + 1:]
        else:
            source = "filename"
            rec["issues"].append("content_filename_conflict")
            rec["content_code"] = by_content["code"]

    vault_name = _vault_name(type_, code_raw, by_name["title"], _humanize(stem))
    parsed = parse_filename(vault_name)
    rec.update({
        "vault_file": vault_name,
        "type": parsed["type"],
        "code": parsed["code"],
        "key": _key(parsed["code"]),
        "designation_source": source,
        "parse_confidence": parsed["parse_confidence"],
    })
    if parsed["type"] == "unknown":
        rec["issues"].append("unknown_document_type")
    # Выгрузка только титульной страницы КонсультантПлюс: текста нормы нет,
    # пунктов и цитат из такого файла не получить.
    pages = sum(1 for line in text.splitlines() if _PAGE_RE.match(line))
    if pages <= 1 and len(text.encode("utf-8")) < _MIN_TEXT_BYTES:
        rec["issues"].append("no_text")
    intended = f"{type_} {code_raw}" if code_raw is not None else None
    if intended and type_ != "ПУЭ" and _key(parsed["code"]) != _key(intended):
        rec["issues"].append("parse_roundtrip_mismatch")
        rec["intended_code"] = intended
    rec["_body"] = clean_body(text)
    rec["_parsed"] = parsed
    return rec


def run(src: Path, out: Path, report_path: Path, status_index: Path) -> dict:
    files = sorted(p for p in src.glob("*.md") if p.is_file())
    if not files:
        raise SystemExit(f"в {src} нет *.md верхнего уровня")
    if out.exists() and any(out.iterdir()):
        raise SystemExit(f"{out} не пуст — укажите новую папку")
    out.mkdir(parents=True, exist_ok=True)

    designation_map = load_designation_map()
    records = [process_file(p, designation_map) for p in files]

    by_key: dict[str, list[dict]] = defaultdict(list)
    for r in records:
        by_key[r["key"]].append(r)
    for group in by_key.values():
        if len(group) < 2:
            continue
        # Отдельный файл нормы предпочтительнее приказа, содержащего её текст.
        group.sort(key=lambda r: (r["designation_source"] == "designation_map", -r["size"]))
        for r in group:
            r["issues"].append("duplicate_designation")
        for r in group[1:]:
            r["skipped"] = True

    names: set[str] = set()
    for r in records:
        if r.get("skipped"):
            continue
        if r["vault_file"] in names:
            r["issues"].append("vault_name_collision")
            r["skipped"] = True
            continue
        names.add(r["vault_file"])
        fm = _frontmatter(r["_parsed"], r["source_file"], r["designation_source"])
        (out / r["vault_file"]).write_text(fm + r["_body"], encoding="utf-8")

    old = json.loads(status_index.read_text(encoding="utf-8"))["norms"]
    new_keys = {r["key"] for r in records if not r.get("skipped")}
    standard = {"ГОСТ", "ГОСТ Р", "СП", "СНиП", "ВСН", "МДС", "РД", "СанПиН", "ФЗ", "ПУЭ"}
    lost = sorted(
        n["code"] for n in old
        if n.get("source") == "vault" and n.get("type") in standard
        and _key(n["code"]) not in new_keys
    )
    old_keys = {_key(n["code"]) for n in old}
    for r in records:
        r["in_old_index"] = r["key"] in old_keys

    # Смена редакции: тот же номер документа, другой год → кандидат в
    # status_overrides (replaced_by), иначе старые ссылки станут DOCUMENT_MISSING.
    new_by_base: dict[str, list[str]] = defaultdict(list)
    for r in records:
        if not r.get("skipped"):
            new_by_base[_base_key(r["code"])].append(r["code"])
    edition_changed = {
        code: sorted(new_by_base[_base_key(code)])
        for code in lost if new_by_base.get(_base_key(code))
    }
    lost = [code for code in lost if code not in edition_changed]

    for r in records:
        r.pop("_body", None)
        r.pop("_parsed", None)
    issues: dict[str, int] = defaultdict(int)
    for r in records:
        for i in r["issues"]:
            issues[i] += 1
    summary = {
        "src": str(src), "out": str(out),
        "files_in": len(records),
        "files_written": len(names),
        "by_designation_source": _count(r["designation_source"] for r in records),
        "by_type": _count(r["type"] for r in records if not r.get("skipped")),
        "issues": dict(issues),
        "new_not_in_old_index": sum(1 for r in records if not r["in_old_index"] and not r.get("skipped")),
        "old_vault_norms_edition_changed": len(edition_changed),
        "old_vault_norms_lost": len(lost),
    }
    report = {
        "summary": summary,
        "edition_changed": edition_changed,
        "lost_old_norms": lost,
        "files": records,
    }
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")
    return summary


def _base_key(code: str) -> str:
    """Ключ без года редакции: 'ГОСТ 6133-2019' → 'гост6133'."""
    return re.sub(r"[-.](19|20)?\d{2}$", "", _key(code))


def _count(values) -> dict[str, int]:
    c: dict[str, int] = defaultdict(int)
    for v in values:
        c[v] += 1
    return dict(sorted(c.items(), key=lambda kv: -kv[1]))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--src", type=Path, required=True, help="папка с новыми *_results.md (только верхний уровень)")
    ap.add_argument("--out", type=Path, required=True, help="новая пустая папка для vault-кандидата")
    ap.add_argument("--report", type=Path, required=True, help="куда писать JSON-отчёт")
    ap.add_argument("--status-index", type=Path, default=STATUS_INDEX, help="старый индекс для сверки")
    args = ap.parse_args()
    summary = run(args.src, args.out, args.report, args.status_index)
    print(json.dumps(summary, ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
