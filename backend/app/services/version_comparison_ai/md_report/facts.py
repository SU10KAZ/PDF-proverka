"""Реестр фактов сводки: у каждого пункта MD — свой ID и паспорт (переход, лист, страницы A/B, строка MD,
исходная запись, степень подтверждения).

Факт — строка-пункт «- …» листа или раздела перехода. ID — порядковый номер пункта в файле («Ф0001»),
поэтому разбор одного и того же файла всегда даёт те же ID. Для внутреннего отчёта (сводка составлена
программой из сохранённых данных) паспорт дополняется записью-источником из ``records``: поля «было/стало»,
members, вопросы, задание и лист, подтверждение значений. Для загруженного MD паспорт — из структуры файла.

Модель источников не пишет: она называет ID фактов, подписи, страницы и строки подставляет программа.
"""
from __future__ import annotations

import re
from typing import Any

FACT_ID = re.compile(r"[ФF]\d{4}")
# «Старая: л.1 (стр. PDF 50), л.2 (стр. PDF 51); новая: л.1 (стр. PDF 52). …» — позиция полной спецификации.
_LOC_TOKEN = r"(?:л\.[^\s(,;]+(?: \(стр\. PDF \d+\))?|стр\. PDF \d+|—)"
_LOC_LIST = rf"{_LOC_TOKEN}(?:, {_LOC_TOKEN})*"
SPEC_LOC = re.compile(rf"^Старая: ({_LOC_LIST}); новая: ({_LOC_LIST})\.(?:\s|$)")
LOC = re.compile(r"(?:л\.([^\s(,]+))?\s*(?:\(?стр\. PDF (\d+)\)?)?")
DESIGNATION = re.compile(r"[A-Za-zА-Яа-яЁё][A-Za-zА-Яа-яЁё.\-/]*-?\d[\w.\-/]*")
CONFIRMATION = {
    "spec_text": "сравнение текста спецификации (программа)",
    "text": "ИИ-описание листа; значения есть в текстовом слое листа",
    "image": "ИИ-описание листа; значения прочитаны по изображению",
    "unverified": "ИИ-описание листа; значение не подтверждено",
    "question": "вопрос ИИ — не установлено",
    "ai_questions": "вопрос ИИ — не установлено",
    "single": "ИИ-описание добавленного/удалённого листа",
    "summary": "по тексту сводки (исходная запись не передана)",
    "gap": "описания нет",
}


def normalize_id(x: str) -> str:
    """Латинская F вместо «Ф» — тот же ID."""
    x = (x or "").strip()
    return "Ф" + x[1:] if x[:1] in ("F", "f") else x


def _locs(text: str) -> tuple[list[str], list[int]]:
    sheets, pages = [], []
    for part in text.split(","):
        m = LOC.search(part.strip())
        if not m:
            continue
        if m.group(1):
            sheets.append(f"л.{m.group(1)}")
        if m.group(2):
            pages.append(int(m.group(2)))
    return sheets, pages


def _plain(line: str) -> str:
    text = re.sub(r"^\s*-\s+", "", line)
    return re.sub(r"\*\*|__|`+", "", text).strip()


def _identity(rec: dict | None, text: str) -> str | None:
    """Обозначение одной линии/аппарата для цепочек «было → стало → стало»: только с буквенно-цифровым
    обозначением (ЩР-1.2, гр.3, ВРУ-1). Без него одинаковость объекта не подтверждена."""
    if rec and rec.get("kind") == "ai_change":
        name = f"{rec.get('object') or ''}"
    elif rec:
        return None
    else:
        m = re.match(r"^\*\*(.+?)\*\*", re.sub(r"^\s*-\s+", "", text))
        name = m.group(1) if m else ""
    found = DESIGNATION.findall(name)
    return re.sub(r"\s+", " ", name).strip().casefold() if found else None


def build(manifest: dict, lines: list[str], records: dict[str, dict] | None = None) -> dict[str, Any]:
    """Факты по порядку файла и указатель «строка → ID»."""
    records = records or {}
    facts: list[dict] = []
    for t in manifest["transitions"]:
        sheets = t["sheets"]
        for ln in range(t["line_start"], t["line_end"] + 1):
            raw = lines[ln - 1]
            if not raw.strip().startswith("- "):
                continue
            s = next((x for x in sheets if x["line_start"] <= ln <= x["line_end"]), None)
            text = _plain(raw)
            rec = records.get(str(ln))
            kind = (rec or {}).get("kind") or ("question" if text.startswith("_Требует уточнения") else
                                                "gap" if re.match(r"^_.*_$", text) else "summary")
            a_pages = [p for p in [(s or {}).get("a_page")] if p]
            b_pages = [p for p in [(s or {}).get("b_page")] if p]
            sheet_a = sheet_b = [s["sheet"]] if s and s.get("sheet") else []
            m = SPEC_LOC.match(text)
            if kind == "spec_entry" or m:
                if rec and kind == "spec_entry":
                    sheet_a = [f"л.{x['sheet']}" for x in rec.get("old_locations") or [] if x.get("sheet")]
                    sheet_b = [f"л.{x['sheet']}" for x in rec.get("new_locations") or [] if x.get("sheet")]
                    a_pages, b_pages = list(rec.get("old_pages") or []), list(rec.get("new_pages") or [])
                elif m:
                    sheet_a, a_pages = _locs(m.group(1))
                    sheet_b, b_pages = _locs(m.group(2))
                    kind = "spec_entry"
                text_conf = "spec_text" if rec else "summary"
            elif kind == "ai_change":
                text_conf = rec.get("confirmation") or "text"
            elif kind == "ai_questions":
                text_conf = "question"
            elif kind == "ai_single":
                text_conf = "single"
            else:
                text_conf = kind if kind in CONFIRMATION else "summary"
            if rec and kind in ("ai_change", "ai_single", "ai_questions"):
                a_pages = [rec["a_page"]] if rec.get("a_page") else a_pages
                b_pages = [rec["b_page"]] if rec.get("b_page") else b_pages
            confirmation = CONFIRMATION.get(text_conf, text_conf)
            if (rec or {}).get("unclear"):
                confirmation += "; есть неясность"
            facts.append({
                "fact_id": f"Ф{len(facts) + 1:04d}", "md_line": ln, "transition_id": t["transition_id"],
                "transition": t["label"], "sheet_id": (s or {}).get("sheet_id"),
                "sheet": " · ".join(dict.fromkeys(sheet_a + sheet_b)) or ((s or {}).get("sheet") or None),
                "sheet_title": (s or {}).get("title") or ((s or {}).get("heading") or "").lstrip("# ").strip() or None,
                "pages_a": a_pages, "pages_b": b_pages, "kind": kind, "confirmation_code": text_conf,
                "confirmation": confirmation, "text": text, "identity": _identity(rec, raw),
                "record": _record_ref(rec), "details": _details(rec)})
    by_line = {f["md_line"]: f["fact_id"] for f in facts}
    return {"facts": facts, "by_line": by_line, "with_records": bool(records),
            "total": len(facts)}


def _record_ref(rec: dict | None) -> dict | None:
    """Адрес исходной записи (для паспорта и выгрузки), без значений."""
    if not rec:
        return None
    keys = ("kind", "run_id", "job_id", "spid", "change_id", "entry_id")
    return {k: rec[k] for k in keys if rec.get(k)}


def _details(rec: dict | None) -> dict | None:
    """Сохранённые значения записи: поля «было/стало», members, вопросы, ограничения."""
    if not rec:
        return None
    out = {k: rec[k] for k in ("object", "place", "change_kind", "fields", "members", "mapping", "limitation",
                               "questions", "summary", "sheet_kind", "single") if rec.get(k)}
    return out or None


def index(registry: dict) -> dict[str, dict]:
    return {f["fact_id"]: f for f in registry["facts"]}


def in_ranges(registry: dict, refs: list[dict]) -> list[str]:
    """ID фактов, строки которых входят в диапазоны ссылок ``md_from``–``md_to`` (порядок файла)."""
    out: list[str] = []
    for f in registry["facts"]:
        if any(r["md_from"] <= f["md_line"] <= r["md_to"] for r in refs) and f["fact_id"] not in out:
            out.append(f["fact_id"])
    return out


SHORT_CONF = {"spec_text": "СО-текст", "text": "текст", "image": "изобр.", "unverified": "не подтв.",
              "question": "вопрос", "ai_questions": "вопрос", "single": "лист", "summary": "сводка", "gap": "нет описания"}


def _norm(x: str) -> str:
    return re.sub(r"[\s«»\"']+", "", (x or "").casefold()).replace("x", "×").replace("х", "×")


def _fmt_fields(fields: list[dict], text: str) -> str:
    """Поля «было → стало», которых нет в тексте пункта (повтор значений из текста не передаётся)."""
    seen = _norm(text)
    out = []
    for f in fields or []:
        before, after = f.get("before") or "", f.get("after") or ""
        unclear = f.get("certainty", "confirmed") != "confirmed"
        if not unclear and _norm(before) in seen and _norm(after) in seen:
            continue
        unit = f" {f['unit']}" if f.get("unit") else ""
        out.append(f"{f.get('name') or 'значение'}: {before or '—'} → {after or '—'}{unit}" + (" (неясно)" if unclear else ""))
    return "; ".join(out)


TEXT_CHARS = 160        # текст пункта в задании составителя — для опознания; смысл несут проверенные формулировки
EXTRA_CHARS = 220       # сохранённые значения записи, которых нет в тексте, — одной строкой на вид


def _cut(text: str, n: int) -> str:
    text = re.sub(r"\s+", " ", text or "").strip()
    return text if len(text) <= n else text[:n - 1] + "…"


def model_line(f: dict) -> str:
    """Строка факта для задания составителя: ID, переход, лист, страницы A→B, подтверждение, начало текста
    пункта и сохранённые значения записи, которых нет в тексте (поля, members, вопросы, ограничение).
    «обозн.» — у объекта есть буквенно-цифровое обозначение (основание для цепочки значений одной линии)."""
    where = f["transition"] + (f" {f['sheet']}" if f.get("sheet") else "")
    pages = "/".join(map(str, f["pages_a"])) or "—", "/".join(map(str, f["pages_b"])) or "—"
    conf = SHORT_CONF.get(f.get("confirmation_code"), f.get("confirmation_code") or "")
    if "неясность" in (f.get("confirmation") or ""):
        conf += ", неясно"
    if f.get("identity"):
        conf += ", обозн."
    shown = _cut(f["text"], TEXT_CHARS)
    row = f"{f['fact_id']} [{where}; стр. {pages[0]}→{pages[1]}; {conf}] {shown}"
    d = f.get("details") or {}
    extra = []
    flds = _fmt_fields(d.get("fields"), shown)
    if flds:
        extra.append("поля: " + flds)
    mem = "; ".join(f"{m.get('object')}: {mf}" for m in d.get("members") or []
                    for mf in [_fmt_fields(m.get("fields"), shown)] if mf)
    if mem:
        extra.append("члены: " + mem)
    if d.get("questions"):
        extra.append("вопросы: " + " | ".join(f"{q.get('object')}: {q.get('question')}" for q in d["questions"]))
    if d.get("limitation"):
        extra.append("ограничение: " + d["limitation"])
    return row + "".join(f"\n  {_cut(x, EXTRA_CHARS)}" for x in extra)


def passport(f: dict) -> dict:
    """Паспорт факта для отчёта и PDF."""
    return {k: f.get(k) for k in ("fact_id", "md_line", "transition", "sheet", "sheet_title", "pages_a", "pages_b",
                                  "confirmation", "text", "record")}
