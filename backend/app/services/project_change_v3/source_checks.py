"""Deterministic source checks over the final ProjectChanges (0 model calls).

Each check reads only the frozen source package (``page.json``: native PDF
text, structured Markdown, tables) and the final cards.  A finding never edits
or removes a card: it is a review signal shown next to the card, and a run
with findings ends in REVIEW.  Every check has its own flag (off by default).

* ABSENCE_CONTRADICTED  — a card says «в OLD не было / не указано», but an OLD
  page states the same thing (the most frequent error of Opus: WRONG_OLD_STATE).
* ELEVATION_UNCLEAR     — on one page «0.000 = B», a relative mark r and an
  absolute mark a do not satisfy B + r = a; the card repeats the numbers
  without a caveat.  Worded as «требует уточнения», not «ошибка»: the label
  may denote another point (pipe axis vs. pipe bottom).
* TABLE_TOTAL_MISMATCH  — an «Итого/Всего» cell differs from the sum of its
  rows while another column of the same table does add up (the law is first
  proven on the table itself).
* OPTION_NOT_REFLECTED  — equipment the source marks as «опция», named in a
  card without the word.
* LEGEND_ONLY           — every piece of evidence of a card is a legend line or
  a list of symbols: a documentary change, not an engineering decision.
* UNCOVERED_TABLE_ROW   — a row of a consumption/balance table changed between
  OLD and NEW, and no card or hint mentions the row with its new value.
"""
from __future__ import annotations

import os
import re
from typing import Any, Iterable

from .dedupe import salient_tokens

SOURCE_CHECKS_SCHEMA = "projectchange_v3_source_checks/1"
FLAGS = {
    "absence": "PROJECT_COMPARISON_V3_ABSENCE_CHECK",
    "numeric": "PROJECT_COMPARISON_V3_NUMERIC_CHECK",
    "option": "PROJECT_COMPARISON_V3_OPTION_CHECK",
    "documentary": "PROJECT_COMPARISON_V3_DOCUMENTARY_FLAG",
    "table_rows": "PROJECT_COMPARISON_V3_TABLE_ROWS",
}
CAVEAT = re.compile(r"уточн|противореч|расхожд|конфликт|неясн|несоглас|не\s+сход", re.I)
ABSENT = re.compile(
    r"не\s+(?:указ|предусм|показ|зада|обознач|примен|выполн|нормир|был|бы[лт]и|существ|приведен|определ)"
    r"|отсутств|^\s*(?:нет|—|-|–)\s*$",
    re.I,
)
_WORD = re.compile(r"[а-яёa-z]+", re.I)
META_LINE = re.compile(r"^\W*(?:description|summary|entities|verification|key\s+\w+|note|notes|context|level)\W*:", re.I)
MAX_LINE = 400
STOP = {
    "также", "который", "которые", "между", "через", "после", "только", "сохранен", "сохранена", "сохранено",
    "указан", "указано", "указана", "предусмотрен", "предусмотрено", "предусмотрена", "отсутствует",
    "отсутствуют", "показан", "показано", "показаны", "задана", "задано", "было", "были", "была", "нет",
    "листе", "лист", "схеме", "схема", "плане", "корпуса", "корпус", "этаже", "этажа", "здания",
}


def enabled_checks() -> dict[str, bool]:
    return {name: os.environ.get(env, "").strip() == "1" for name, env in FLAGS.items()}


# ---------------------------------------------------------------- source text

def _norm(text: str) -> str:
    return str(text or "").replace("ё", "е").replace("Ё", "Е").replace("−", "-").replace("–", "-")


def page_lines(page: dict[str, Any]) -> list[str]:
    parts = [str(page.get("native_page_text") or "")]
    for block in page.get("blocks") or []:
        parts.append(str(block.get("structured_md") or ""))
        parts.extend(str(t) for t in block.get("tables") or [])
    lines = []
    for part in parts:
        for line in _norm(part).splitlines():
            line = re.sub(r"\s+", " ", line).strip()
            # Recognition meta (a model's own description of the sheet) is not document text.
            if line and not META_LINE.match(line) and len(line) <= MAX_LINE:
                lines.append(line)
    return lines


def _stems(text: str) -> set[str]:
    out = set()
    for word in _WORD.findall(_norm(text).casefold()):
        if len(word) >= 4 and word not in STOP:
            out.add(word[:6])
    return out


def _card_text(change: dict[str, Any]) -> str:
    fields = [change.get(k) for k in ("engineering_subject", "scope", "change_summary", "old_state", "new_state")]
    for p in change.get("changed_parameters") or []:
        fields.extend([p.get("name"), p.get("old_value"), p.get("new_value"), p.get("location")])
    return _norm(" ".join(str(x or "") for x in fields))


def _numbers(text: str) -> set[str]:
    return {m.replace(",", ".") for m in re.findall(r"(?<![\d.,])\d+(?:[.,]\d+)?(?![\d])", _norm(text))}


# ---------------------------------------------------------------- 3. absence

def _absence_claims(change: dict[str, Any]) -> Iterable[dict[str, Any]]:
    for ordinal, p in enumerate(change.get("changed_parameters") or [], start=1):
        old, new = str(p.get("old_value") or ""), str(p.get("new_value") or "")
        if new.strip() and (not old.strip() or ABSENT.search(old)):
            yield {"parameter_ordinal": ordinal, "claim": f"{p.get('name')}: {old or '(пусто)'} → {new}",
                   "subject": str(p.get("name") or ""), "value": new}
    for sentence in re.split(r"(?<=[.;])\s+", str(change.get("old_state") or "")):
        if ABSENT.search(sentence):
            yield {"parameter_ordinal": None, "claim": sentence.strip(), "subject": ABSENT.sub(" ", sentence), "value": ""}


def _windows(pages, side: str) -> list[tuple[int, str, set[str], set[str]]]:
    """Two consecutive lines of one page: PDF text wraps a sentence across lines."""
    out = []
    for (s, n), page in sorted(pages.items()):
        if s != side:
            continue
        lines = page_lines(page)
        for i, line in enumerate(lines):
            text = line if i + 1 == len(lines) else f"{line} {lines[i + 1]}"
            out.append((n, text, _stems(text), salient_tokens(text)))
    return out


def _stem_frequency(pages) -> dict[str, int]:
    freq: dict[str, int] = {}
    for page in pages.values():
        for line in page_lines(page):
            for stem in _stems(line):
                freq[stem] = freq.get(stem, 0) + 1
    return freq


ABSENCE_SPECIFIC_MAX = 100   # a stem on at most this many lines of the document names something specific
ABSENCE_RARE_MAX = 20        # three stems this rare on one line name the same item


def _value_tokens(value: str) -> set[str]:
    return {t for t in salient_tokens(value)
            if (any(ch.isdigit() for ch in t) and not re.fullmatch(r"\d", t)) or re.fullmatch(r"[a-z][a-z\-]+", t)}


def absence_findings(changes, pages) -> list[dict[str, Any]]:
    freq = _stem_frequency(pages)
    old_windows = _windows(pages, "OLD")
    out = []
    for change in changes:
        for claim in _absence_claims(change):
            stems = _stems(claim["subject"])
            specific = {s for s in stems if freq.get(s, 0) <= ABSENCE_SPECIFIC_MAX}
            rare = {s for s in stems if freq.get(s, 0) <= ABSENCE_RARE_MAX}
            values = _value_tokens(claim["value"])
            hit = None
            for n, text, text_stems, text_tokens in old_windows:
                shared = values & text_tokens
                numbers_only = shared and all(re.fullmatch(r"[\d.]+", t) for t in shared)
                if shared and len(specific & text_stems) >= (2 if numbers_only else 1):
                    hit = (n, text, f"значение {', '.join(sorted(shared))}")
                    break
                if len(rare & text_stems) >= 3:
                    hit = (n, text, "то же наименование")
                    break
            if hit:
                n, text, why = hit
                out.append({
                    "check": "ABSENCE_CONTRADICTED", "projectchange_id": change.get("projectchange_id"),
                    "parameter_ordinal": claim["parameter_ordinal"], "claim": claim["claim"][:300],
                    "source": [{"side": "OLD", "physical_page": n, "quote": text[:240]}], "match": why,
                    "message_ru": f"Карточка утверждает, что в OLD этого нет, но на OLD{n} есть ({why}): «{text[:160]}». Проверьте состояние OLD.",
                })
    return out


# ---------------------------------------------------------------- 4. numbers

_BASE = re.compile(r"0[.,]000[^=\n]{0,25}=\s*(\d{2,3}[.,]\d{1,3})")
_REL = re.compile(r"(?:оси\s+трубы|отм\w*|низ\w*|верх\w*)[^=\n\d]{0,25}=\s*([+\-]?\d{1,2}[.,]\d{2,3})", re.I)
_ABS = re.compile(r"(?:ур\.?\s*земли|абс\w*|отметка)[^\d\n]{0,12}(\d{2,3}[.,]\d{2})(?!\d)", re.I)


def _f(x: str) -> float:
    return float(x.replace(",", "."))


def elevation_findings(changes, pages) -> list[dict[str, Any]]:
    out = []
    for (side, n), page in sorted(pages.items()):
        text = _norm(str(page.get("native_page_text") or ""))
        base = _BASE.search(text)
        if not base:
            continue
        b = _f(base.group(1))
        rels = [(_f(m.group(1)), m.group(1)) for m in _REL.finditer(text)]
        abss = [(_f(m.group(1)), m.group(1)) for m in _ABS.finditer(text) if abs(_f(m.group(1)) - b) < 30]
        if not rels or not abss:
            continue
        for r, r_raw in rels:
            diffs = sorted((abs(b + r - a), a_raw) for a, a_raw in abss)
            if diffs[0][0] <= 0.02:
                continue
            d, a_raw = diffs[0]
            if d > 0.5:
                continue
            values = {r_raw.replace(".", ","), a_raw.replace(".", ","), r_raw.replace(",", "."), a_raw.replace(",", ".")}
            cards = [c for c in changes if any(v.lstrip("+-") in _card_text(c) for v in values)]
            quiet = [c for c in cards if not CAVEAT.search(_card_text(c))]
            out.append({
                "check": "ELEVATION_UNCLEAR", "projectchange_id": quiet[0].get("projectchange_id") if quiet else None,
                "related_projectchange_ids": [c.get("projectchange_id") for c in quiet],
                "parameter_ordinal": None,
                "claim": f"{side}{n}: 0.000 = {base.group(1)}, отметка {r_raw}, абсолютная {a_raw}",
                "source": [{"side": side, "physical_page": n, "quote": f"0.000 = {base.group(1)}; {r_raw}; {a_raw}"}],
                "message_ru": (f"{side}{n}: {base.group(1)} {'+' if r >= 0 else '−'} {abs(r):.2f} = {b + r:.2f}, "
                               f"а подписано {a_raw} (разница {d:.2f} м). Требует уточнения, к какой точке относится отметка."),
            })
    return [f for f in out if f["related_projectchange_ids"]]


def _md_rows(table: str) -> list[list[str]]:
    rows = []
    for line in str(table).splitlines():
        if line.strip().startswith("|") and not re.fullmatch(r"\|[\s\-:|]*\|?", line.strip()):
            rows.append([c.strip() for c in line.strip().strip("|").split("|")])
    return rows


def _num(cell: str) -> float | None:
    cell = cell.replace(" ", "").replace("**", "")
    if not re.fullmatch(r"-?\d+(?:[.,]\d+)?", cell):
        return None
    return _f(cell)


def table_total_findings(changes, pages) -> list[dict[str, Any]]:
    out = []
    for (side, n), page in sorted(pages.items()):
        for block in page.get("blocks") or []:
            for table in block.get("tables") or []:
                rows = _md_rows(table)
                start = 0
                for i, row in enumerate(rows):
                    if not any(re.search(r"итого|всего", c, re.I) for c in row[:3]):
                        continue
                    body = [r for r in rows[start:i] if not any(re.search(r"итого|всего", c, re.I) for c in r[:3])]
                    start = i + 1
                    if len(body) < 2:
                        continue
                    agree, disagree = [], []
                    for col, cell in enumerate(row):
                        total = _num(cell)
                        vals = [_num(r[col]) for r in body if col < len(r)]
                        vals = [v for v in vals if v is not None]
                        if total is None or len(vals) < 2 or col < 1:
                            continue
                        s = round(sum(vals), 3)
                        (agree if abs(s - total) <= 0.011 else disagree).append((col, total, s))
                    if agree and disagree:
                        for col, total, s in disagree:
                            out.append({
                                "check": "TABLE_TOTAL_MISMATCH", "projectchange_id": None, "parameter_ordinal": None,
                                "claim": f"{side}{n}: «{row[0][:40]}» колонка {col + 1}: {total} при сумме строк {s}",
                                "source": [{"side": side, "physical_page": n, "quote": " | ".join(row)[:240]}],
                                "message_ru": f"{side}{n}: итог {total} не равен сумме строк {s}, хотя другие колонки этой таблицы сходятся. Требует уточнения.",
                            })
    return out


# ---------------------------------------------------------------- 5a/5b/5c

def option_findings(changes, pages) -> list[dict[str, Any]]:
    new_pages = sorted((k, v) for k, v in pages.items() if k[0] == "NEW")
    lines_by_page = {n: page_lines(page) for (_, n), page in new_pages}
    out = []
    for change in changes:
        text = _card_text(change)
        if re.search(r"опци", text, re.I):
            continue
        names = {w for w in re.findall(r"[A-Za-z][A-Za-z\-]{3,}", text) if w.casefold() not in {"wilo", "type"}}
        for name in sorted(names):
            hit = None
            for n, lines in lines_by_page.items():
                for i, line in enumerate(lines):
                    if name.casefold() in line.casefold():
                        window = " ".join(lines[max(0, i - 1): i + 4])
                        if re.search(r"опци", window, re.I):
                            hit = (n, line)
                            break
                if hit:
                    break
            if hit:
                out.append({
                    "check": "OPTION_NOT_REFLECTED", "projectchange_id": change.get("projectchange_id"),
                    "parameter_ordinal": None, "claim": name,
                    "source": [{"side": "NEW", "physical_page": hit[0], "quote": hit[1][:240]}],
                    "message_ru": f"«{name}» в NEW{hit[0]} отмечено как опция, а карточка подаёт его как обязательную комплектацию.",
                })
    return out


_LEGEND = re.compile(r"легенд|условн\w*\s+обознач", re.I)
_LEGEND_LINE = re.compile(r"^\W*трубопровод\s+\w+", re.I)
_FITTING = re.compile(r"кран|клапан|задвижк|затвор|смесител|фильтр|счетчик|компенсатор|опор", re.I)


def _legend_like(fragment: str) -> bool:
    text = _norm(fragment)
    if _LEGEND.search(text) or _LEGEND_LINE.search(text):
        return True
    items = [x for x in re.split(r"[;/]|\s·\s", text) if x.strip()]
    return len(items) >= 3 and sum(bool(_FITTING.search(x)) for x in items) >= 3


def documentary_findings(changes, pages) -> list[dict[str, Any]]:
    out = []
    for change in changes:
        evidence = change.get("evidence_items") or []
        new = [e for e in evidence if e.get("side") == "NEW"]
        if not new:
            continue
        if all(_legend_like(str(e.get("relevant_fragment") or "")) for e in evidence):
            out.append({
                "check": "LEGEND_ONLY", "projectchange_id": change.get("projectchange_id"), "parameter_ordinal": None,
                "claim": str(change.get("engineering_subject") or "")[:200],
                "source": [{"side": e.get("side"), "physical_page": e.get("physical_page"),
                            "quote": str(e.get("relevant_fragment") or "")[:200]} for e in evidence[:3]],
                "message_ru": "Все доказательства карточки — строки легенды или перечни условных обозначений: "
                              "это изменение оформления, инженерное решение на чертеже не показано.",
            })
    return out


def _row_key(row: list[str], name_cell: str) -> str:
    """Row number + first word stem: «2 | Административные раб.» and «2 | Административные работники» meet."""
    number = next((c for c in row[:2] if re.fullmatch(r"\d{1,3}", c.strip())), "")
    words = _WORD.findall(_norm(name_cell).casefold())
    return f"{number}:{words[0][:6] if words else ''}"


def _consumption_rows(pages, side: str) -> dict[str, tuple[int, list[str], list[str], str]]:
    rows: dict[str, tuple[int, list[str], list[str], str]] = {}
    for (s, n), page in sorted(pages.items()):
        if s != side:
            continue
        for block in page.get("blocks") or []:
            for table in block.get("tables") or []:
                if not re.search(r"водопотреблен|баланс", _norm(table), re.I):
                    continue
                for row in _md_rows(table):
                    name_cell = next((c for c in row if re.search(r"[А-Яа-я]{4,}", c)), "")
                    if not name_cell or re.search(r"итого|всего", name_cell, re.I):
                        continue
                    nums = [c for c in row if _num(c.replace("*", "")) is not None]
                    if len(nums) < 2:
                        continue
                    rows.setdefault(_row_key(row, name_cell), (n, row, nums, name_cell))
    return rows


def _value(cell: str) -> float:
    return _num(cell.replace("*", "")) or 0.0


def table_row_findings(changes, hints, pages) -> list[dict[str, Any]]:
    old, new = _consumption_rows(pages, "OLD"), _consumption_rows(pages, "NEW")
    texts = [_card_text(c) for c in changes] + [_norm(" ".join(str(h.get(k) or "") for k in (
        "engineering_subject", "suspected_change", "missing_proof_or_conflict"))) for h in hints]
    text_numbers = [[_f(x) for x in re.findall(r"(?<![\d.,])\d+(?:[.,]\d+)?(?![\d])", t)] for t in texts]
    out = []
    for key in sorted(set(old) & set(new)):
        (on, orow, onums, oname), (nn, nrow, nnums, nname) = old[key], new[key]
        if len(onums) != len(nnums):
            continue
        changed = [(o, v) for o, v in zip(onums, nnums) if abs(_value(o) - _value(v)) > 1e-9]
        if not changed:
            continue
        stems = _stems(nname) | _stems(oname)
        new_values = sorted({_value(v) for _, v in changed})

        def found(nums: list[float]) -> int:
            return sum(any(abs(value - x) <= 0.006 for x in nums) for value in new_values)

        covered = any(
            (len(stems & _stems(t)) >= min(2, len(stems)) and found(nums) >= 1)
            or (len(new_values) >= 2 and found(nums) >= 2)   # the card names the row's new numbers
            for t, nums in zip(texts, text_numbers)
        )
        if covered:
            continue
        out.append({
            "check": "UNCOVERED_TABLE_ROW", "projectchange_id": None, "parameter_ordinal": None,
            "claim": f"«{nname[:60]}»: " + "; ".join(f"{o} → {v}" for o, v in changed),
            "source": [{"side": "OLD", "physical_page": on, "quote": " | ".join(orow)[:240]},
                       {"side": "NEW", "physical_page": nn, "quote": " | ".join(nrow)[:240]}],
            "message_ru": f"Строка таблицы «{nname[:60]}» изменилась (OLD{on} → NEW{nn}: "
                          + ", ".join(f"{o} → {v}" for o, v in changed) + "), но ни одна карточка или подсказка её не называет.",
        })
    return out


# ---------------------------------------------------------------- entry point

def run_source_checks(
    *,
    projectchanges: list[dict[str, Any]],
    unresolved_hints: list[dict[str, Any]],
    page_records: dict[tuple[str, int], dict[str, Any]],
    enabled: dict[str, bool] | None = None,
) -> dict[str, Any]:
    enabled = enabled if enabled is not None else enabled_checks()
    findings: list[dict[str, Any]] = []
    if enabled.get("absence"):
        findings += absence_findings(projectchanges, page_records)
    if enabled.get("numeric"):
        findings += elevation_findings(projectchanges, page_records)
        findings += table_total_findings(projectchanges, page_records)
    if enabled.get("option"):
        findings += option_findings(projectchanges, page_records)
    if enabled.get("documentary"):
        findings += documentary_findings(projectchanges, page_records)
    if enabled.get("table_rows"):
        findings += table_row_findings(projectchanges, unresolved_hints, page_records)
    for index, finding in enumerate(findings, start=1):
        finding["finding_id"] = f"SC-{index:03d}"
    counts: dict[str, int] = {}
    for finding in findings:
        counts[finding["check"]] = counts.get(finding["check"], 0) + 1
    return {
        "schema": SOURCE_CHECKS_SCHEMA,
        "enabled": {name: bool(enabled.get(name)) for name in FLAGS},
        "findings": findings,
        "summary": {"findings_total": len(findings), "by_check": counts},
        "note": "Детерминированные сигналы для проверки человеком; карточки не меняются и не удаляются.",
    }
