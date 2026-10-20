"""Отчёт для заказчика: представление над готовым проверенным результатом (report_v2 + реестр фактов).

Это не новый анализ: строки берутся из сохранённых итогов составителя и фактов. Из клиентского вида убраны
ID фактов, реестры покрытия и источников, подписи процесса, счётчики полноты и время анализа — в данных
отчёта они остаются (report.json, facts.json, checks.json, исходный MD). Важные изменения раскладываются
по переходам, в которых они произошли (по переходу каждого факта), а не выносятся отдельным разделом.

Вид (``build``) — одна хронологическая таблица: переход и даты, масштаб, суть (первое предложение — жирное),
пункты «объект — было → стало», последствие. Редакционный вариант вида (отдельный вызов модели по решению
пользователя) заполняет те же поля; шаблон PDF от источника вида не зависит.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from . import schemas, stages

NO_CHANGES = "Технических изменений не выявлено."
SCOPE_NONE = "Без технических изменений"
POINTS_MAX = 6
_SPEC = re.compile(r"^Старая:.*?;\s*новая:.*?(?:\)|—)\.\s+")
_KV = re.compile(r"^(Наименование|Марка|раздел|Количество|Поставщик|Примечание|Тип)\b:?\s*(.*)$")
_CUT = re.compile(r",|\s(?:предназнач\w*|для|который|которая|которые|в составе)\s")
_ORDINAL = re.compile(r",?\s*(?:первое|второе|третье|четвёртое|четвертое)\s+описание\b", re.I)
_ID = re.compile(r"\s*\[?\b[ФF]\d{4}\b\]?")


def _date(iso: str | None) -> str:
    if not iso:
        return ""
    y, m, d = iso.split("-")
    return f"{d}.{m}.{y}"


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", _ID.sub("", text or "")).strip()


# «A»/«B» во внутренних описаниях — стороны пары; заказчику — номера выдач.
_SIDE = re.compile(r"(?<=[а-яёА-ЯЁ»)] )([AB])(?=[\s,.;:])")
# Методические оговорки (о сводке, сопоставлении, доказательности) — во внутреннем слое, не у заказчика.
_METHOD = re.compile(r"сводк|сопоставлен|не доказыва|установлено частично|по данным сравнения|MD|фрагмент"
                     r"|изображени|реестр|покрыти", re.I)


def _sides(text: str, a: str, b: str) -> str:
    return _SIDE.sub(lambda m: a if m.group(1) == "A" else b, text)


def _impact(text: str) -> str:
    """Последствие без методических оговорок: части через «;» с такими оговорками отбрасываются."""
    parts = [p.strip() for p in re.split(r";\s*", text) if p.strip() and not _METHOD.search(p)]
    out = "; ".join(parts).rstrip(".")
    return (out[0].upper() + out[1:] + ".") if out else ""


def _short(name: str, limit: int = 48) -> str:
    """Короткое обозначение объекта: без описательного хвоста («… предназначен для …»)."""
    name = _ORDINAL.sub("", name or "").strip(" ,;—")
    if len(name) <= limit:
        return name
    head = _CUT.split(name, 1)[0].strip()
    return head if len(head) <= limit + 12 else head[:limit].rsplit(" ", 1)[0] + "…"


def fact_point(f: dict) -> dict[str, str] | None:
    """Пункт из сохранённого факта: объект (жирный) и суть изменения — без строк MD, страниц и ID."""
    text = _clean(f.get("text"))
    if f.get("kind") == "spec_entry":
        body = _SPEC.sub("", text).rstrip(".")
        kind = (f.get("details") or {}).get("change_kind")
        if kind not in ("added", "removed", "changed"):
            return None                     # перенос, переименование группы, неразрешённое — не пункт для заказчика
        body = re.sub(r"^(Добавлено: не было →|Удалено:)\s*", "", body).replace(" → отсутствует", "")
        kv, changes = {}, []
        for part in (x.strip() for x in body.split(";")):
            m = _KV.match(part)
            if "→" in part and not part.startswith("Поз."):
                changes.append(part[0].lower() + part[1:])
            elif m:
                kv[m.group(1)] = m.group(2).strip()
        name = _short(kv.get("Наименование", ""), 40)
        # При смене марки она уже есть в «было → стало»: объект — только наименование.
        obj = name if any(c.startswith("марка") for c in changes) and name else \
            " ".join(x for x in (name, kv.get("Марка", "")) if x)
        where = f" (раздел «{kv['раздел']}»)" if kv.get("раздел") else ""
        qty = f", {kv['Количество'].rstrip('.')}" if kv.get("Количество") else ""
        if kind == "added":
            return {"object": obj, "text": f"добавлено в спецификацию{qty}{where}"}
        if kind == "removed":
            return {"object": obj, "text": f"исключено из спецификации{qty}{where}"}
        return {"object": obj, "text": "; ".join(changes)} if changes else None
    d = f.get("details") or {}
    obj = d.get("object") or ""
    body = text
    if obj and body.startswith(obj):
        body = body[len(obj):].lstrip(" —:")
    # Снятое/изменённое требование — с листом: отмена на одном листе не распространяется на раздел.
    if d.get("change_kind") == "требование" and f.get("sheet"):
        body = f"{body.rstrip('.')} ({f['sheet']})."
    return {"object": _short(obj), "text": body}


# Редакционный порядок внутри перехода: питание и защита, противопожарные системы и огнезащита,
# сечения и подключения, комплектация и объёмы, затем прочее. Порядок внимания, а не оценка дефекта.
_RANK = (
    re.compile(r"\bQF|\bQS|\bQFD|ВА-\d|ДИФ|АД-\d|ВН-\d|автомат|уставк|Ir=|Ics|ВРУ|ввод|TA\d|трансформатор", re.I),
    re.compile(r"СПЗ|ШУН|ШУ-ДП|пожар|огнез|огнест|EI\s?\d|ОКЛ|ОГ-\d|проходк|дымоудал", re.I),
    re.compile(r"ППГ|ВВГ|FRHF|сечени|\d+[хx×]\d|кабел|подключ|линия|линии|трасс", re.I),
    re.compile(r"количеств|\d\s*(?:м|шт|кг)\b|резерв|комплект|УЭРМ|счётчик|счетчик", re.I),
)


def rank(point: dict) -> int:
    hay = f"{point.get('object', '')} {point.get('text', '')}"
    return next((k for k, rx in enumerate(_RANK) if rx.search(hay)), len(_RANK))


def build(report: dict, registry: dict, meta: dict, editorial: dict[str, dict] | None = None) -> dict[str, Any]:
    """Вид для заказчика из проверенного отчёта (без обращения к модели).

    ``editorial`` — сохранённый редакционный отбор по переходам (client_edit): для этих переходов строка берётся
    из принятых проверкой пунктов отбора; остальные — шаблоном из отчёта."""
    idx = {f["fact_id"]: f for f in registry.get("facts") or []}
    rows = []
    trs = report["transitions"]
    # Важные изменения — в переходы их фактов (не отдельным разделом; будущее изменение не уходит в ранний переход).
    extra: dict[str, list[str]] = {}
    for s in report.get("significant_changes") or []:
        for fid in s.get("sources") or []:
            f = idx.get(fid)
            if f:
                extra.setdefault(f["transition"], []).append(fid)
    for t in trs:
        label = f"{t['from_version']}→{t['to_version']}"
        scope = schemas.normalize_scope(t["change_scope"]) or t["change_scope"]
        comp = t.get("completeness") or {}
        none = scope == "Нет изменений содержания" and comp.get("complete") is not False and not t.get("changes")
        row = {"label": label, "from_version": t["from_version"], "to_version": t["to_version"],
               "dates": [_date(t.get("from_date")), _date(t.get("to_date"))],
               "scale": SCOPE_NONE if none else scope, "lead": "", "points": [], "impact": "", "questions": []}
        if none:
            row["lead"] = NO_CHANGES
            rows.append(row)
            continue
        ed = (editorial or {}).get(label)
        if ed:
            a = ed["answer"]
            if a.get("change_scope"):          # масштаб, оценённый при отборе (отчёт сразу для заказчика)
                row["scale"] = SCOPE_NONE if a["change_scope"] == "Нет изменений содержания" else a["change_scope"]
            row["lead"] = _clean(a["lead"])
            row["points"] = [{"object": _clean(k["point"]["object"]), "text": _clean(k["point"]["text"])}
                             for k in ed["kept"]]
            row["questions"] = [_clean(q["text"]) for q in a.get("questions") or []]
            row["impact"] = _clean(ed.get("impact", a.get("impact")))
            row["source"] = "editorial"
            rows.append(row)
            continue
        if scope == "Нет изменений содержания":
            row["lead"] = "Содержательных изменений в описанных листах не выявлено."
        else:
            row["lead"] = _clean(t.get("main_changes"))
        used = set()
        # Сначала — важные изменения этого перехода, затем строки таблицы составителя.
        for fid in extra.get(label, []):
            p = fact_point(idx[fid])
            if p and fid not in used and p["text"]:
                row["points"].append(p)
                used.add(fid)
        for ch in t.get("changes") or []:
            if set(ch.get("sources") or []) & used:
                continue
            row["points"].append({"object": _short(_clean(ch["object"])),
                                  "text": f"{_clean(ch['before'])} → {_clean(ch['after'])}"})
            used.update(ch.get("sources") or [])
        row["points"] = [p for p in row["points"] if not _METHOD.search(p["text"])]
        row["points"].sort(key=rank)
        row["points"] = row["points"][:POINTS_MAX]
        for pt in row["points"]:
            pt["text"] = _sides(pt["text"], t["from_version"], t["to_version"])
        impact = _impact(_clean(t.get("construction_impact")))
        if impact and not re.search(r"не выявлен", impact):
            row["impact"] = _sides(impact, t["from_version"], t["to_version"])
        rows.append(row)
    first, last = trs[0], trs[-1]
    return {"schema": "client_view/v1", "title": "Изменения проектных решений",
            "object": meta.get("object") or "", "code": report.get("project_code") or "",
            "section": meta.get("section") or "",
            "versions": f"{first['from_version']}–{last['to_version']}",
            "period": "–".join(x for x in (_date(first.get("from_date")), _date(last.get("to_date"))) if x),
            "rows": rows, "source": "editorial" if editorial else "report"}


# ------------------------------------------------------------------ шапка: объект и раздел

def object_title(object_id: str) -> str:
    """Название объекта из реестра портала (без номера «213. »); только чтение."""
    try:
        from backend.app.core.config import OBJECTS_FILE_PATH
        data = json.loads(Path(OBJECTS_FILE_PATH).read_text(encoding="utf-8"))
    except (OSError, ValueError, ImportError):
        return ""
    items = data if isinstance(data, list) else data.get("objects", data)
    items = items.values() if isinstance(items, dict) else items
    for o in items:
        if isinstance(o, dict) and o.get("id") == object_id:
            return re.sub(r"^\s*\d+\.\s*", "", str(o.get("name") or "")).replace('"', "«", 1).replace('"', "»", 1)
    return ""


def section_title(pdf_path: Path, code: str) -> str:
    """Название раздела с титульного листа выдачи: строка сразу после шифра (только чтение PDF)."""
    try:
        import fitz
        doc = fitz.open(str(pdf_path))
        lines = [x.strip() for x in doc[0].get_text().split("\n") if x.strip()]
        doc.close()
    except Exception:  # noqa: BLE001 — нет PDF или не читается: раздел просто не показывается
        return ""
    for k, line in enumerate(lines[:-1]):
        if code and line.replace(" ", "") == code.replace(" ", ""):
            nxt = lines[k + 1]
            if 3 <= len(nxt) <= 120 and re.search(r"[А-Яа-яЁё]{4}", nxt) and not re.search(r"\d{3}", nxt) \
                    and not _NOT_SECTION.match(nxt):
                return nxt
    return ""


# Строки штампа и титула, которые идут за шифром вместо названия раздела («ОБЪЕКТ: Жилой комплекс…»).
_NOT_SECTION = re.compile(r"(?:ОБЪЕКТ|ЗАКАЗЧИК|АДРЕС|РАБОЧАЯ ДОКУМЕНТАЦИЯ|В ПРОИЗВОДСТВО|Генеральный директор|"
                          r"Главный инженер|Изменение\s*\d|Изм\.?\s|Москва)", re.I)
