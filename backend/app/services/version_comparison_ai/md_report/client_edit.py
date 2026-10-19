"""Редакционный отбор содержания отчёта для заказчика — по одному обращению на переход, из ПОЛНОГО набора фактов.

Вход перехода — все сохранённые факты отчёта (реестр facts.json зафиксированного снимка), а не только строки,
выбранные составителем: описания листов с полями «было → стало», вопросы, добавленные/удалённые листы и позиции
спецификации. Повторяющиеся позиции спецификации (одно наименование и марка в разных разделах) программа сводит
в группы с кодом «С001» и итогом количества — код раскрывается программой обратно в ID фактов.

Модель выбирает главное и пишет строку перехода; каждый пункт ссылается на факты, а значения «было → стало»
отдаются отдельно (``values``), чтобы программа проверила всю связку: факт этого перехода → объект (обозначения
объекта есть в факте) → параметр → было → стало (сторона и направление — по полям факта) → единицы. Пункт, не
прошедший проверку, не попадает в отчёт. Результат хранится отдельно от фактов (``client/editorial/<снимок>/``)
вместе с версией промпта и отпечатком снимка; PDF и скачивание используют сохранённое без обращения к модели.
"""
from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

PROMPT_VERSION = "client-edit/2"
FIELD_CHARS = 110            # значение поля «было/стало» в пакете
TEXT_CHARS = 260             # текст описания без полей
NAME_CHARS = 36
MEMBER_CHARS = 60
TITLE_CHARS = 34
POINTS_MAX = 8

_SPEC_HEAD = re.compile(r"^Старая:.*?;\s*новая:.*?(?:\)|—)\.\s+")
_KV = re.compile(r"^(Поз\.|Наименование|Марка|раздел|Количество|Поставщик|Примечание|Тип)\s*:?\s*(.*)$")
_VERSION = re.compile(r"\bV\d{1,2}\b")
_ORDINAL_NUM = re.compile(r"^\d+-?(?:го|й|м|х|ом|ая|ой|ий|ые|ых)$")
_NUM = re.compile(r"\d+(?:[.,]\d+)?")
_DESIGNATION = re.compile(r"(?<![\w])(?=[\w.\-/…–]*\d)(?=[\w.\-/…–]*[A-Za-zА-Яа-яЁё])[\w][\w.\-/…–]*")


def _cut(s: str, n: int) -> str:
    s = re.sub(r"\s+", " ", s or "").strip()
    return s if len(s) <= n else s[:n - 1].rstrip() + "…"


def _spec_parts(f: dict) -> tuple[dict[str, str], list[tuple[str, str, str]]]:
    """Позиция СО: поля «ключ значение» и изменения «поле: было → стало»."""
    body = _SPEC_HEAD.sub("", f.get("text") or "").rstrip(".")
    body = re.sub(r"^(Добавлено: не было →|Удалено:)\s*", "", body).replace(" → отсутствует", "")
    kv, changes = {}, []
    for part in (x.strip() for x in body.split(";")):
        if "→" in part and ":" in part.split("→")[0]:
            name, rest = part.split(":", 1)
            before, after = rest.split("→", 1)
            changes.append((name.strip(), before.strip(), after.strip()))
            continue
        m = _KV.match(part)
        if m:
            kv[m.group(1)] = m.group(2).strip()
    return kv, changes


def _qty(s: str) -> tuple[float | None, str]:
    m = re.match(r"^\s*(\d+(?:[.,]\d+)?)\s*([^\d\s].*)?$", s or "")
    if not m:
        return None, ""
    return float(m.group(1).replace(",", ".")), (m.group(2) or "").strip(" .")


def _fmt(x: float) -> str:
    return (f"{x:.2f}".rstrip("0").rstrip(".")).replace(".", ",")


# ------------------------------------------------------------------ пакет перехода

def packet(facts: list[dict], label: str) -> tuple[str, dict[str, list[str]], dict[str, str]]:
    """Текст фактов перехода для модели, раскрытие кодов групп СО и опорный текст групп.

    Возвращает (текст, {код группы: [ID фактов]}, {код группы: строка группы})."""
    own = [f for f in facts if f["transition"] == label]
    lines: list[str] = []
    sheets = [f for f in own if f["kind"] in ("ai_change", "ai_single")]
    if sheets:
        lines.append("## Описания листов (поля: было → стало)")
    for f in sheets:
        title = re.split(r"\s+—\s+часть листа", f.get("sheet_title") or "")[0]
        head = f"{f['fact_id']} [{f.get('sheet') or 'лист'} {_cut(title, TITLE_CHARS)}]".replace(" ]", "]")
        if f["kind"] == "ai_single":
            lines.append(f"{head} лист добавлен/удалён | {_cut(f['text'], TEXT_CHARS)}")
            continue
        d = f.get("details") or {}
        star = "★ " if important(f) else ""
        bits = [f"{star}{head} {d.get('change_kind') or 'изменение'}", f"объект: {_cut(d.get('object'), 80)}"]
        if d.get("place"):
            bits.append(f"место: {_cut(d['place'], 50)}")
        if d.get("mapping") and d["mapping"] != "one_to_one":
            bits.append(f"соответствие объектов: {d['mapping']}")
        if f.get("confirmation_code") in ("unverified", "image") and "неясн" in (f.get("confirmation") or ""):
            bits.append("есть неясность")
        fields = d.get("fields") or []
        for x in fields:
            unit = f" [{x['unit']}]" if x.get("unit") else ""
            flag = " (неясно)" if x.get("certainty") == "unclear" else ""
            bits.append(f"{_cut(x.get('name'), 36)}: {_cut(x.get('before') or 'нет', FIELD_CHARS)} → "
                        f"{_cut(x.get('after') or 'нет', FIELD_CHARS)}{unit}{flag}")
        if not fields:
            bits.append(_cut(f["text"], TEXT_CHARS))
        # Состав (members) — значения по отдельным линиям/аппаратам; повтор полей факта не дублируется.
        seen = {(x.get("before"), x.get("after")) for x in fields}
        mem = []
        for m in d.get("members") or []:
            vals = [f"{_cut(x.get('name'), 24)}: {_cut(x.get('before') or 'нет', MEMBER_CHARS)} → "
                    f"{_cut(x.get('after') or 'нет', MEMBER_CHARS)}" for x in m.get("fields") or []
                    if (x.get("before"), x.get("after")) not in seen]
            if vals:
                mem.append(f"{_cut(m.get('object'), 40)} — " + "; ".join(vals))
        if mem:
            bits.append("состав: " + " / ".join(mem))
        lines.append(" | ".join(bits))
    qs = [f for f in own if f["kind"] == "ai_questions"]
    if qs:
        lines.append("\n## Вопросы, не установленные описанием листа (не утверждать как изменение)")
    for f in qs:
        for q in (f.get("details") or {}).get("questions") or []:
            lines.append(f"{f['fact_id']} [{f.get('sheet') or 'лист'}] вопрос: {_cut(q.get('object'), 80)} — "
                         f"{_cut(q.get('question'), 170)}")
    spec = [f for f in own if f["kind"] == "spec_entry"]
    changed = [f for f in spec if (f.get("details") or {}).get("change_kind") == "changed"]
    unresolved = [f for f in spec if (f.get("details") or {}).get("change_kind") == "unresolved"]
    if changed:
        lines.append("\n## Спецификация: изменённые позиции")
    for f in changed:
        kv, ch = _spec_parts(f)
        what = " ".join(x for x in (_cut(kv.get("Наименование"), NAME_CHARS), kv.get("Марка", "")) if x)
        sect = f" | раздел: {_cut(kv['раздел'], 40)}" if kv.get("раздел") else ""
        lines.append(f"{f['fact_id']} {what}{sect} | " + "; ".join(f"{n}: {_cut(a, 70)} → {_cut(b, 70)}" for n, a, b in ch))
    groups: dict[str, list[str]] = {}
    support: dict[str, str] = {}
    by: dict[tuple, list[dict]] = defaultdict(list)
    for f in spec:
        kind = (f.get("details") or {}).get("change_kind")
        if kind in ("added", "removed"):
            kv, _ = _spec_parts(f)
            by[(kind, kv.get("Наименование", ""), kv.get("Марка", ""))].append(f)
    if by:
        lines.append("\n## Спецификация: добавленные и удалённые позиции (сведены по наименованию и марке)")
    for k, ((kind, name, mark), fs) in enumerate(sorted(by.items(), key=lambda x: (x[0][0], -len(x[1]), x[0][1])), 1):
        word = "добавлено" if kind == "added" else "исключено"
        what = " ".join(x for x in (_cut(name, NAME_CHARS), mark) if x) or "позиция"
        qty = [_qty(_spec_parts(f)[0].get("Количество", "")) for f in fs]
        units = {u for v, u in qty if v is not None}
        total = ""
        if all(v is not None for v, _ in qty) and len(units) <= 1:
            total = f", всего {_fmt(sum(v for v, _ in qty))} {next(iter(units), '')}".rstrip()
        sects = sorted({_spec_parts(f)[0].get("раздел", "") for f in fs} - {""})
        where = f" | разделы: {_cut('; '.join(sects), 70)}" if sects else ""
        if len(fs) == 1:
            q = _spec_parts(fs[0])[0].get("Количество", "")
            line = f"{fs[0]['fact_id']} {word}: {what}{', ' + q if q else ''}{where}"
        else:
            code = f"С{k:03d}"
            groups[code] = [f["fact_id"] for f in fs]
            line = f"{code} {word} позиций: {len(fs)}: {what}{total}{where}"
            support[code] = line
        lines.append(line)
    moved = [f for f in spec if (f.get("details") or {}).get("change_kind") in ("moved", "section_renamed")]
    if moved:
        lines.append("\n## Спецификация: перенос позиций и переименование групп (без изменения позиции)")
    for f in moved:
        body = _SPEC_HEAD.sub("", f["text"])
        body = re.sub(r";\s*(Поз\.|Код|Поставщик)[^;]*", "", body.replace("\\|", "").replace("|", ""))
        lines.append(f"{f['fact_id']} {_cut(re.sub(r'\s+', ' ', body), 150)}")
    if unresolved:
        lines.append("\n## Спецификация: соответствие позиций неоднозначно (не утверждать замену)")
    for f in unresolved:
        lines.append(f"{f['fact_id']} {_cut(_SPEC_HEAD.sub('', f['text']), 200)}")
    return "\n".join(lines), groups, support


# ------------------------------------------------------------------ задание и схема

def schema() -> dict[str, Any]:
    s = {"type": "string"}
    ids = {"type": "array", "items": s}
    value = {"type": "object", "additionalProperties": False, "required": ["parameter", "before", "after", "fact_id"],
             "properties": {"parameter": s, "before": s, "after": s, "fact_id": s}}
    point = {"type": "object", "additionalProperties": False,
             "required": ["object", "text", "fact_ids", "values", "status"],
             "properties": {"object": s, "text": s, "fact_ids": ids, "values": {"type": "array", "items": value},
                            "status": {"type": "string", "enum": ["confirmed", "needs_clarification"]}}}
    question = {"type": "object", "additionalProperties": False, "required": ["text", "fact_ids"],
                "properties": {"text": s, "fact_ids": ids}}
    skipped = {"type": "object", "additionalProperties": False, "required": ["fact_id", "reason"],
               "properties": {"fact_id": s, "reason": s}}
    return {"type": "object", "additionalProperties": False,
            "required": ["transition", "lead", "points", "impact", "impact_fact_ids", "questions", "not_included"],
            "properties": {"transition": s, "lead": s, "points": {"type": "array", "items": point},
                           "impact": s, "impact_fact_ids": ids, "questions": {"type": "array", "items": question},
                           "not_included": {"type": "array", "items": skipped}}}


def prompt_text() -> str:
    return (Path(__file__).resolve().parent.parent / "prompts" / "md_report_client_edit.md").read_text(encoding="utf-8")


def prompt_for(t: dict, packet_text: str, template: str, seed: str) -> str:
    """Задание перехода; метки блока данных подбираются так, чтобы их не было в данных."""
    from .. import prompt as old_prompt
    from . import stages
    label = f"{t['from_version']}→{t['to_version']}"
    begin, end = old_prompt.boundary({"packet_id": seed}, packet_text)
    return stages._fill(template, {"TRANSITION": label, "SCOPE": str(t.get("change_scope") or ""),
                                   "PREV": t["from_version"], "FACTS": packet_text, "BEGIN": begin, "END": end})


# ------------------------------------------------------------------ проверка ответа

def _norm(s: str) -> str:
    s = (s or "").lower().replace("ё", "е")
    s = re.sub(r"[×xх]", "x", s)
    s = s.replace(",", ".").replace("«", "").replace("»", "").replace('"', "")
    return re.sub(r"\s+", "", s)


def _nums(s: str) -> list[str]:
    return [n.replace(",", ".") for n in _NUM.findall(s or "")]


def _fact_sides(f: dict) -> list[tuple[str, str]]:
    """Пары (было, стало) факта для проверки направления."""
    if f["kind"] == "ai_change":
        d = f.get("details") or {}
        pairs = [(x.get("before") or "", x.get("after") or "")
                 for x in (d.get("fields") or []) + [y for m in d.get("members") or [] for y in m.get("fields") or []]]
        return pairs or [("", f.get("text") or "")]
    if f["kind"] == "spec_entry":
        kind = (f.get("details") or {}).get("change_kind")
        kv, ch = _spec_parts(f)
        whole = " ".join(f"{k} {v}" for k, v in kv.items())
        if kind == "added":
            return [("", whole)]
        if kind == "removed":
            return [(whole, "")]
        return [(a, b) for _, a, b in ch] + [(whole, whole)]
    return [("", f.get("text") or "")]


def _fact_text(f: dict) -> str:
    d = f.get("details") or {}
    extra = [d.get("object") or "", d.get("place") or ""]
    extra += [f"{x.get('name')} {x.get('before')} {x.get('after')} {x.get('unit')}" for x in d.get("fields") or []]
    extra += [f"{q.get('object')} {q.get('question')}" for q in d.get("questions") or []]
    for m in d.get("members") or []:
        extra.append(f"{m.get('object')} {m.get('place')}")
        extra += [f"{x.get('name')} {x.get('before')} {x.get('after')} {x.get('unit')}" for x in m.get("fields") or []]
    return " ".join([f.get("text") or "", f.get("sheet") or "", f.get("sheet_title") or ""] + extra)


def _side_ok(claim: str, side: str) -> bool:
    if not claim.strip() or _norm(claim) in ("нет", "небыло", "отсутствует", "—", "-"):
        return True
    parts = [x for x in _norm(claim).split("…") if x]     # значение, сокращённое в пакете («…»), — по частям
    if parts and all(x in _norm(side) for x in parts):
        return True
    nums = _nums(claim)
    return bool(nums) and all(n in _nums(side) for n in nums)


def _all_fields(f: dict) -> list[dict]:
    d = f.get("details") or {}
    return (d.get("fields") or []) + [y for m in d.get("members") or [] for y in m.get("fields") or []]


_CUT_WORD = re.compile(r"([\w\-«»\"]{3,})…(?=[\s,.;:)]|$)")


def repair_cut(text: str, source: str) -> tuple[str, bool]:
    """Обрубок «FIRESTO…» (значение сокращено в пакете, модель его переписала) — дописать до целого слова из
    указанных фактов. Возвращает (текст, остались ли обрывы). Диапазоны «TA1.1…TA1.6» не трогаются."""
    left = False

    def fix(m):
        nonlocal left
        frag = m.group(1)
        full = re.search(r"(?<![\w])" + re.escape(frag) + r"[\w\-]*", source)
        if full and len(full.group(0)) > len(frag):
            return full.group(0)
        left = True
        return m.group(0)
    return _CUT_WORD.sub(fix, text), left


def _value_ok(before: str, after: str, sides: list[tuple[str, str]]) -> bool:
    """Значение подтверждается парой «было/стало» факта; составное («63 / 20 / 100 → 50 / 63 / 40») — попарно,
    каждая часть своей парой того же факта, с тем же направлением."""
    if any(_side_ok(before, a) and _side_ok(after, b) for a, b in sides):
        return True
    bs, as_ = re.split(r"\s*[/;]\s*", before.strip()), re.split(r"\s*[/;]\s*", after.strip())
    if len(bs) < 2 or len(bs) != len(as_):
        return False
    return all(any(_side_ok(x, a) and _side_ok(y, b) and (_nums(x) or _nums(y)) for a, b in sides
                   if a.strip() or b.strip()) for x, y in zip(bs, as_))


_FORBIDDEN = [
    (re.compile(r"\b[ФF]\d{4}\b|\bС\d{3}\b"), "служебный ID факта или группы в тексте"),
    (re.compile(r"\bMD\b|сводк|стр\. PDF|реестр|покрытие листов|покрыти[ея] отч", re.I), "служебная подпись процесса"),
    (re.compile(r"(?<=[а-яё»)] )[AB](?=[\s,.;:])"), "обозначение стороны A/B вместо номера выдачи"),
    (re.compile(r"из-за ошибк|ошибк\w* проектировщ|по требованию заказчик|для приведения к норм|по причине", re.I),
     "причина решения без основания"),
    (re.compile(r"переделк|затрат|стоимост|задержк|штраф|нарушени", re.I), "переделки/затраты/нарушения без основания"),
]


def check(ans: dict, t: dict, facts: list[dict], groups: dict[str, list[str]], support: dict[str, str]) -> dict:
    """Проверка ответа перехода. ``hard`` — ответ не годится (повтор при наличии лимита);
    ``dropped`` — пункты, не прошедшие проверку связки (в отчёт не попадают); ``kept`` — принятые пункты."""
    label = f"{t['from_version']}→{t['to_version']}"
    idx = {f["fact_id"]: f for f in facts}
    own = {f["fact_id"] for f in facts if f["transition"] == label}
    hard: list[str] = []
    if ans.get("transition") != label:
        hard.append(f"transition «{ans.get('transition')}» вместо «{label}»")
    lead = (ans.get("lead") or "").strip()
    if not lead or len(lead) > 230 or len(re.findall(r"[.!?](?:\s|$)", lead)) > 1:
        hard.append("lead — ровно одно предложение до 230 знаков")
    texts = [lead, ans.get("impact") or ""] + [q.get("text") or "" for q in ans.get("questions") or []]
    texts += [f"{p.get('object')} {p.get('text')}" for p in ans.get("points") or []]
    for rx, why in _FORBIDDEN:
        for tx in texts:
            m = rx.search(tx)
            if m:
                hard.append(f"{why}: «{m.group(0)}» в «{_cut(tx, 80)}»")
                break

    def expand(ids: list[str]) -> tuple[list[str], list[str]]:
        out, bad = [], []
        for i in ids:
            i = i.strip().strip("[]")
            if i in groups:
                out += groups[i]
            elif i in own:
                out.append(i)
            else:
                bad.append(i)
        return out, bad

    kept, dropped = [], []
    pts = ans.get("points") or []
    if not pts or len(pts) > POINTS_MAX:
        hard.append(f"пунктов {len(pts)} — нужно от 1 до {POINTS_MAX}")
    for p in pts:
        why: list[str] = []
        ids, bad = expand(p.get("fact_ids") or [])
        if bad:
            why.append("ID не из этого перехода: " + ", ".join(bad[:5]))
        if not ids:
            why.append("нет фактов")
        cited = [idx[i] for i in ids if i in idx]
        hay = _norm(" ".join(_fact_text(f) for f in cited) + " " +
                    " ".join(support[c] for c in (p.get("fact_ids") or []) if c in support))
        toks = [x for tok in _DESIGNATION.findall(p.get("object") or "") for x in re.split(r"[/–…,]+", tok)]
        for tok in toks:
            if len(tok) >= 3 and re.search(r"\d", tok) and not _ORDINAL_NUM.match(tok) and _norm(tok) not in hay:
                why.append(f"обозначение «{tok}» не встречается в указанных фактах")
        for n in _nums(_VERSION.sub(" ", f"{p.get('object')} {p.get('text')}")):
            if n not in _nums(hay.replace("x", " ")) and n not in hay:
                why.append(f"число {n} не найдено в указанных фактах")
        said = _norm(f"{p.get('text')} " + " ".join(f"{v.get('before')} {v.get('after')}" for v in p.get("values") or []))
        uncertain = any(f["kind"] == "ai_questions" or f.get("confirmation_code") == "unverified" or
                        (f.get("details") or {}).get("change_kind") == "unresolved" or
                        any(x.get("certainty") == "unclear" and any(len(_norm(y)) >= 3 and _norm(y) in said
                                                                     for y in (x.get("before"), x.get("after")) if y)
                            for x in _all_fields(f))
                        for f in cited)
        for v in p.get("values") or []:
            fid = (v.get("fact_id") or "").strip()
            if fid in groups:
                sides = [("", support[fid])] if "добавлено" in support[fid] else [(support[fid], "")]
            elif fid in own and fid in ids:
                sides = _fact_sides(idx[fid])
            else:
                why.append(f"значение «{v.get('parameter')}» ссылается на {fid or 'пустой ID'} вне пункта/перехода")
                continue
            if not _value_ok(v.get("before") or "", v.get("after") or "", sides):
                why.append(f"«{v.get('parameter')}: {_cut(v.get('before'), 40)} → {_cut(v.get('after'), 40)}» "
                           f"не подтверждается полями {fid} (сторона или направление)")
        if uncertain and p.get("status") == "confirmed":
            why.append("пункт опирается на неясный факт/вопрос, но помечен как подтверждённый")
        src = " ".join(_fact_text(f) for f in cited) + " " + " ".join(support[c] for c in p.get("fact_ids") or []
                                                                     if c in support)
        fixed, left = repair_cut(p.get("text") or "", src)
        if left:
            why.append("в тексте пункта обрезанное значение «…»")
        p = dict(p, text=fixed)
        (dropped if why else kept).append({"point": p, "fact_ids": ids, "problems": why})
    if pts and len(dropped) * 2 > len(pts):
        hard.append(f"не прошли проверку {len(dropped)} из {len(pts)} пунктов")
    imp_ids, bad = expand(ans.get("impact_fact_ids") or [])
    if bad:
        hard.append("impact_fact_ids не из этого перехода: " + ", ".join(bad[:5]))
    imp = ans.get("impact") or ""
    sentences = re.split(r"(?<=[.!?])\s+", imp.strip())
    if len(sentences) > 2:
        imp = " ".join(sentences[:2])        # лишнее отсекается (не дописывается); повтор не нужен
    pool = _norm(" ".join(_fact_text(idx[i]) for i in imp_ids + [x for k in kept for x in k["fact_ids"]] if i in idx))
    for n in _nums(_VERSION.sub(" ", imp)):
        if n not in pool:
            hard.append(f"число {n} в последствии не найдено в фактах")
    for q in ans.get("questions") or []:
        _, bad = expand(q.get("fact_ids") or [])
        if bad or not q.get("fact_ids"):
            hard.append(f"вопрос «{_cut(q.get('text'), 50)}» без фактов этого перехода")
    return {"ok": not hard and not dropped, "hard": hard, "kept": kept, "dropped": dropped, "impact": imp}


def problems_text(chk: dict) -> list[str]:
    out = list(chk["hard"])
    for d in chk["dropped"]:
        out.append(f"пункт «{_cut(d['point'].get('object'), 50)}»: " + "; ".join(d["problems"]))
    return out


# ------------------------------------------------------------------ проверка отбора (покрытие важных фактов)

_REQ = re.compile(r"прокладк|в трубе|труб[аеуы]\s|огнез|огнест|EI\s?\d{2,3}|IP\s?\d{2}|управлен|регулир|мастик|раствор|"
                  r"герметик|плит|конструкци|материал", re.I)
_MARK = re.compile(r"ВА-\d|ДИФ-|ВН-\d|АД-\d|ППГ|ВВГ|АППГ|FRHF|ШУН|ЩУВ|счётчик|счетчик|И\d00-|CE\d{3}", re.I)
_SECT = re.compile(r"\d+\s*[хx×]\s*\(?\d")
_NOM = re.compile(r"\b\d+(?:[.,]\d+)?\s*А\b")
_WORD = re.compile(r"[a-zа-яё]{4,}")


def important(f: dict) -> str | None:
    """Категория важного факта описания листа — то, что не должно теряться за расчётными числами:
    материалы, способ прокладки, огнезащита, управление; смена аппарата/марки; сечение; номинал."""
    if f["kind"] != "ai_change":
        return None
    d = f.get("details") or {}
    ck = d.get("change_kind")
    if ck == "документ":
        return None
    if ck in ("требование", "трасса") and _REQ.search(f["text"]):
        return "требование, материал или прокладка"
    for x in _all_fields(f):
        b, a = x.get("before") or "", x.get("after") or ""
        if _norm(b) == _norm(a):
            continue
        if _MARK.search(b + a):
            return "аппарат или марка"
        if _SECT.search(a) and (_SECT.search(b) or not b):
            return "сечение или кабель"
        if _NOM.search(a) and _NOM.search(b):
            return "номинал"
    if ck == "замена" and _REQ.search(f["text"]):
        return "требование, материал или прокладка"
    return None


_CALC = re.compile(r"ток|iкз|паден|cos|\bр[урp]\b|\biр\b|расч|нагрузк|мощност|значени|кз|sр", re.I)


def _value_fields(f: dict) -> list[dict]:
    """Поля без расчётных величин: одинаковые расчётные числа у разных линий не делают их одним объектом."""
    return [x for x in _all_fields(f) if not _CALC.search(x.get("name") or "")]


def _afters(f: dict) -> set[str]:
    return {_norm(x.get("after") or "")[:60] for x in _value_fields(f) if len(_norm(x.get("after") or "")) >= 12}


def _pairs(f: dict) -> set[tuple[str, str]]:
    return {(_norm(x.get("before") or ""), _norm(x.get("after") or "")) for x in _value_fields(f)
            if len(_norm(x.get("after") or "")) >= 6 and _norm(x.get("before") or "") != _norm(x.get("after") or "")}


def _words(f: dict) -> set[str]:
    return set(_WORD.findall(" ".join(str(x.get("after") or "") for x in _all_fields(f)).lower()))


def _tokens(s: str) -> set[str]:
    out = set()
    for tk in _DESIGNATION.findall(s or ""):
        for x in re.split(r"[/–…,]+", tk):
            x = x.rstrip(".-")
            if len(x) >= 3 and re.search(r"\d", x) and not _ORDINAL_NUM.match(x) and not re.match(r"(?i)л\.\d", x):
                out.add(_norm(x))
    return out


def coverage(facts: list[dict], label: str, kept: list[dict], decisions: dict[str, dict] | None = None) -> dict:
    """Что стало с каждым важным фактом перехода: включён в пункт; объединён (повтор того же значения на другом
    листе или тот же объект, что у пункта); исключён — с причиной из решений редактуры/отбора; иначе — не разобран.
    Внутренняя проверка отбора: заказчику не выводится и не требует печатать все факты."""
    idx = {f["fact_id"]: f for f in facts}
    decisions = decisions or {}
    cited = {i: k["point"].get("object") for k in kept for i in k["fact_ids"]}
    # «То же на другом листе» — только с фактами ДРУГИХ листов (на том же листе это другой объект).
    def other_sheet(i, f):
        return idx[i].get("sheet") != f.get("sheet")
    cited_ids = [i for i in cited if i in idx]
    point_tokens = [(_tokens(f"{k['point'].get('object')} {k['point'].get('text')}"), k["point"].get("object"))
                    for k in kept]
    out = {}
    for f in facts:
        if f["transition"] != label:
            continue
        cat = important(f)
        if not cat:
            continue
        fid = f["fact_id"]
        rec = {"category": cat, "sheet": f.get("sheet"), "object": (f.get("details") or {}).get("object")}
        if fid in cited:
            rec |= {"status": "included", "point": cited[fid]}
        elif fid in decisions:
            rec |= decisions[fid]
        elif (j := next((i for i in cited_ids if other_sheet(i, f) and _pairs(f) & _pairs(idx[i])), None)):
            rec |= {"status": "merged", "point": cited[j], "reason": "то же изменение на другом листе"}
        elif (j := next((i for i in cited_ids if other_sheet(i, f) and _afters(f) & _afters(idx[i])), None)):
            rec |= {"status": "merged", "point": cited[j], "reason": "то же значение на другом листе"}
        elif (w := _words(f)) and (j := next((i for i in cited_ids if other_sheet(i, f) and len(w & _words(idx[i])) >= 4
                                              and len(w & _words(idx[i])) * 3 >= 2 * len(w)), None)):
            rec |= {"status": "merged", "point": cited[j], "reason": "то же требование на другом листе"}
        elif (tk := _tokens((f.get("details") or {}).get("object") or "")) and any(tk & pt for pt, _ in point_tokens):
            rec |= {"status": "merged", "reason": "тот же объект, что у пункта",
                    "point": next(o for pt, o in point_tokens if tk & pt)}
        else:
            rec |= {"status": "unresolved"}
        out[fid] = rec
    counts: dict[str, int] = {}
    for r in out.values():
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    return {"facts": out, "counts": counts, "unresolved": [k for k, r in out.items() if r["status"] == "unresolved"]}


# ------------------------------------------------------------------ редакционные проверки текста для заказчика

_UNCERTAIN = re.compile(r"уточн|неоднозначн|не установлен|не подтвержд", re.I)


def editorial_problems(kept: list[dict], questions: list[dict], impact: str, impact_ids: list[str],
                       facts: list[dict]) -> list[str]:
    """Повторы и опора последствия: вопрос — в одном месте; последствие не повторяет оговорки и опирается
    только на принятые пункты (не на отброшенное утверждение)."""
    out = []
    if _UNCERTAIN.search(impact or ""):
        out.append("последствие повторяет оговорку/вопрос — неопределённость указывается при изменении или в вопросе")
    used = {i for k in kept for i in k["fact_ids"]}
    stray = [i for i in impact_ids or [] if i not in used]
    if stray:
        out.append("последствие опирается на факты вне принятых пунктов: " + ", ".join(stray[:6]))
    for q in questions:
        qt = _tokens(q.get("text") or "")
        for k in kept:
            pt = k["point"].get("text") or ""
            if _UNCERTAIN.search(pt) and qt & _tokens(f"{k['point'].get('object')} {pt}"):
                out.append(f"вопрос «{_cut(q.get('text'), 50)}» повторяет оговорку пункта «{_cut(k['point'].get('object'), 40)}»")
    return out


# ------------------------------------------------------------------ хранение по переходам

def _root(rdir: Path) -> Path:
    return rdir / "client" / "editorial"


def data_key(facts: list[dict], t: dict) -> str:
    """Отпечаток данных перехода: его факты и строка отчёта. Не зависит от других переходов и от версии задания."""
    label = f"{t['from_version']}→{t['to_version']}"
    own = [f for f in facts if f["transition"] == label]
    row = {k: t.get(k) for k in ("from_version", "to_version", "from_date", "to_date", "change_scope", "completeness")}
    return hashlib.sha256(json.dumps({"facts": own, "row": row}, ensure_ascii=False, sort_keys=True).encode()).hexdigest()[:16]


def _file(rdir: Path, kind: str, label: str, key: str) -> Path:
    return _root(rdir) / kind / f"{label.replace('→', '-')}-{key}.json"


def _inputs(rdir: Path) -> tuple[dict, list[dict]]:
    report = json.loads((rdir / "report.json").read_text(encoding="utf-8"))
    facts = json.loads((rdir / "facts.json").read_text(encoding="utf-8"))["facts"]
    return report, facts


def _migrate(rdir: Path, report: dict, facts: list[dict]) -> None:
    """Результаты прежней раскладки (папка снимка целиком) — в раскладку по переходам, если данные те же."""
    from backend.app.services.common.atomic_json import atomic_write_json
    root = _root(rdir)
    if not root.exists():
        return
    trs = {f"{t['from_version']}→{t['to_version']}": t for t in report["transitions"]}
    for old in root.glob("*/results/*.json"):
        snap_dir = old.parent.parent
        if not re.fullmatch(r"[0-9a-f]{16}", snap_dir.name):
            continue
        meta = json.loads((snap_dir / "meta.json").read_text(encoding="utf-8"))
        if meta["snapshot"]["facts.json"] != hashlib.sha256((rdir / "facts.json").read_bytes()).hexdigest():
            continue
        r = json.loads(old.read_text(encoding="utf-8"))
        t = trs.get(r["transition"])
        if not t:
            continue
        dst = _file(rdir, "results", r["transition"], data_key(facts, t))
        if not dst.exists():
            dst.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_json(dst, r | {"prompt_version": meta.get("prompt_version"), "migrated_from": snap_dir.name,
                                        "calls_dir": str(snap_dir / "calls" / r["transition"].replace("→", "-"))})


def status(rdir: Path, *, write: bool = True) -> dict:
    """Готовность клиентского содержания по переходам (без обращения к модели).
    ``write=False`` (просмотр viewer): без переноса прежней раскладки — ничего не пишется."""
    report, facts = _inputs(rdir)
    if write:
        _migrate(rdir, report, facts)
    out = {"transitions": {}, "missing": []}
    for t in edited_transitions(report):
        label = f"{t['from_version']}→{t['to_version']}"
        key = data_key(facts, t)
        res, ed = _file(rdir, "results", label, key), _file(rdir, "edits", label, key)
        src = "edit" if ed.exists() else "model" if res.exists() else None
        out["transitions"][label] = {"key": key, "source": src}
        if not src:
            out["missing"].append(label)
    out["ready"] = not out["missing"]
    return out


def load(rdir: Path) -> dict[str, dict] | None:
    """Клиентское содержание по переходам: правка редактора (если есть) поверх результата отбора; None — нет."""
    try:
        report, facts = _inputs(rdir)
    except (OSError, ValueError, KeyError):
        return None
    _migrate(rdir, report, facts)
    out = {}
    for t in edited_transitions(report):
        label = f"{t['from_version']}→{t['to_version']}"
        key = data_key(facts, t)
        for kind in ("edits", "results"):
            f = _file(rdir, kind, label, key)
            if f.exists():
                out[label] = json.loads(f.read_text(encoding="utf-8")) | {"source": kind}
                break
    return out or None


def estimate(rdir: Path, labels: list[str] | None = None) -> dict:
    """Объём подготовки: пакеты недостающих переходов. Токены — по замеру 19.10 (≈1,9 знака на токен вместе
    со служебной частью исполнителя); это оценка, не подтверждённый расход."""
    report, facts = _inputs(rdir)
    st = status(rdir)
    labels = labels if labels is not None else st["missing"]
    trs = {f"{t['from_version']}→{t['to_version']}": t for t in report["transitions"]}
    tpl_chars = len(prompt_text())
    items = []
    for label in labels:
        text, _, _ = packet(facts, label)
        chars = len(text) + tpl_chars
        items.append({"transition": label, "chars": chars, "tokens": round(chars / CHARS_PER_TOKEN)})
    n = len(items)
    return {"transitions": items, "calls": n, "extra_calls_max": min(EXTRA_CALLS, n),
            "input_tokens": sum(i["tokens"] for i in items),
            "input_tokens_max": sum(i["tokens"] for i in items) + (max((i["tokens"] for i in items), default=0)
                                                                   * min(EXTRA_CALLS, n)),
            "chars": sum(i["chars"] for i in items), "model_note": "оценка по замеру прежнего отбора"}


def save_edit(rdir: Path, label: str, answer: dict, decisions: dict[str, dict], *, editor: str, note: str = "") -> dict:
    """Редакторская правка перехода (отдельный клиентский слой; ответы модели, отчёт и факты не меняются).

    Правка проходит ту же проверку связки, что и ответ модели, плюс редакционные проверки повторов и покрытие
    важных фактов. Не прошедшая — не сохраняется."""
    from backend.app.services.common.atomic_json import atomic_write_json
    report, facts = _inputs(rdir)
    t = next(x for x in report["transitions"] if f"{x['from_version']}→{x['to_version']}" == label)
    key = data_key(facts, t)
    _, groups, support = packet(facts, label)
    chk = check(answer, t, facts, groups, support)
    problems = problems_text(chk)
    imp_ids = [i for x in answer.get("impact_fact_ids") or [] for i in (groups.get(x) or [x])]
    problems += editorial_problems(chk["kept"], answer.get("questions") or [], chk["impact"], imp_ids, facts)
    own = {f["fact_id"] for f in facts if f["transition"] == label}
    bad = [k for k in decisions if k not in own]
    if bad:
        problems.append("решения по фактам не этого перехода: " + ", ".join(bad[:5]))
    for k, v in decisions.items():
        if v.get("status") not in ("merged", "excluded") or not (v.get("reason") or "").strip():
            problems.append(f"решение по {k}: нужен статус merged/excluded и конкретная причина")
    cov = coverage(facts, label, chk["kept"], decisions)
    if cov["unresolved"]:
        problems.append("не разобраны важные факты: " + ", ".join(cov["unresolved"][:12]))
    if problems:
        raise ValueError("; ".join(problems))
    base = _file(rdir, "results", label, key)
    rec = {"transition": label, "key": key, "answer": answer, "impact": chk["impact"], "kept": chk["kept"],
           "dropped": [], "decisions": decisions, "coverage": cov, "editor": editor, "note": note,
           "basis": {"result": base.name if base.exists() else None,
                     "result_sha256": hashlib.sha256(base.read_bytes()).hexdigest() if base.exists() else None},
           "groups": {k: v for k, v in groups.items() if any(k in (p["point"].get("fact_ids") or []) for p in chk["kept"])}}
    dst = _file(rdir, "edits", label, key)
    dst.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(dst, rec)
    return rec


# ------------------------------------------------------------------ выполнение

CHARS_PER_TOKEN = 1.9
EXTRA_CALLS = 2


class Budget:
    """Общий лимит фактических обращений на запуск (основные + повторы)."""
    def __init__(self, calls: int, extra: int):
        self.calls, self.extra, self.used, self.extra_used = calls, extra, 0, 0

    def take(self, retry: bool) -> bool:
        if self.used >= self.calls + self.extra or (retry and self.extra_used >= self.extra):
            return False
        self.used += 1
        self.extra_used += int(retry)
        return True


def edited_transitions(report: dict) -> list[dict]:
    """Переходы, для которых нужен отбор: всё, кроме подтверждённого отсутствия изменений."""
    from . import schemas
    out = []
    for t in report["transitions"]:
        scope = schemas.normalize_scope(t["change_scope"]) or t["change_scope"]
        none = scope == "Нет изменений содержания" and (t.get("completeness") or {}).get("complete") is not False \
            and not t.get("changes")
        if not none:
            out.append(t)
    return out


def run(rdir: Path, describer, budget: Budget, *, only: list[str] | None = None, log=print,
        cancel=None, on_usage=None) -> dict:
    """Отбор недостающих переходов (``only`` — порядок и состав). Готовое по тем же данным не запрашивается."""
    from ..executor import CancelToken, DescribeTask
    from backend.app.services.common.atomic_json import atomic_write_json
    from datetime import datetime, timezone
    report, facts = _inputs(rdir)
    _migrate(rdir, report, facts)
    template = prompt_text()
    trs = {f"{t['from_version']}→{t['to_version']}": t for t in edited_transitions(report)}
    order = only or list(trs)
    summary, usage = {}, {"calls": 0, "input_tokens": 0, "cached_input_tokens": 0, "output_tokens": 0, "seconds": 0.0}
    tok = cancel or CancelToken()
    for label in order:
        t = trs[label]
        key = data_key(facts, t)
        res_file = _file(rdir, "results", label, key)
        if res_file.exists() or _file(rdir, "edits", label, key).exists():
            summary[label] = {"reused": True}
            continue
        text, groups, support = packet(facts, label)
        base = prompt_for(t, text, template, f"{key}-{label}")
        feedback, best, n = "", None, 0
        while True:
            if getattr(tok, "cancelled", False):
                break
            if not budget.take(retry=n > 0):
                log(f"{label}: лимит обращений исчерпан")
                break
            n += 1
            adir = _root(rdir) / "calls" / f"{label.replace('→', '-')}-{key}" / f"attempt-{n}"
            adir.mkdir(parents=True, exist_ok=True)
            prompt = base + feedback
            (adir / "prompt.md").write_text(prompt, encoding="utf-8")
            task = DescribeTask(prompt=prompt, schema=schema(), image_paths=[], timeout_s=1800,
                                marker=f"vcce-{key}", packet_id=key, chunk_id=label,
                                raw_path=str(adir / "answer.raw.txt"))
            res = describer.run(task, tok)
            u = res.usage or {}
            usage["calls"] += 1
            usage["seconds"] += round(res.duration_ms / 1000, 1)
            for k in ("input_tokens", "cached_input_tokens", "output_tokens"):
                usage[k] += int(u.get(k) or 0)
            if on_usage:
                on_usage(dict(usage))
            rec = {"attempt": n, "ok": res.ok, "error_kind": res.error_kind, "error": res.error,
                   "seconds": round(res.duration_ms / 1000, 1), "usage": u, "prompt_chars": len(prompt),
                   "prompt_version": PROMPT_VERSION, "at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
            if res.ok:
                atomic_write_json(adir / "answer.json", res.parsed)
                chk = check(res.parsed, t, facts, groups, support)
                rec["check"] = {k: chk[k] for k in ("ok", "hard")} | {
                    "dropped": [{"object": x["point"].get("object"), "problems": x["problems"]} for x in chk["dropped"]],
                    "kept": len(chk["kept"])}
                if not chk["hard"]:
                    best = (res.parsed, chk)
                feedback = ("\n\nПРЕДЫДУЩИЙ ОТВЕТ НЕ ПРОШЁЛ ПРОВЕРКУ ПРОГРАММЫ:\n- " + "\n- ".join(problems_text(chk))
                            + "\nВерни полный исправленный ответ по той же схеме.\n")
            atomic_write_json(adir / "call.json", rec)
            log(f"{label}: попытка {n} — {'ok' if res.ok else res.error_kind} {rec['seconds']} с, вход {u.get('input_tokens')}")
            if best or (not res.ok and res.error_kind in ("QUOTA", "AUTH", "CANCELLED")):
                break           # пункты, не прошедшие проверку, отбрасываются; повтор — только при жёстких ошибках
        if best:
            ans, chk = best
            decisions = {x["fact_id"]: {"status": "excluded", "reason": x.get("reason") or ""}
                         for x in ans.get("not_included") or [] if x.get("fact_id")}
            atomic_write_json(res_file, {"transition": label, "key": key, "answer": ans, "attempts": n,
                                         "prompt_version": PROMPT_VERSION, "impact": chk["impact"],
                                         "kept": chk["kept"], "dropped": chk["dropped"],
                                         "coverage": coverage(facts, label, chk["kept"], decisions),
                                         "editorial": editorial_problems(
                                             chk["kept"], ans.get("questions") or [], chk["impact"],
                                             [i for x in ans.get("impact_fact_ids") or [] for i in (groups.get(x) or [x])],
                                             facts),
                                         "groups": {k: v for k, v in groups.items()
                                                    if any(k in (p["point"].get("fact_ids") or []) for p in chk["kept"])}})
        summary[label] = {"attempts": n, "accepted": bool(best)}
    return {"transitions": summary, "calls": budget.used, "usage": usage}


def recheck(rdir: Path) -> dict[str, dict]:
    """Перепроверить сохранённые ответы модели текущей проверкой — без обращения к модели."""
    from backend.app.services.common.atomic_json import atomic_write_json
    report, facts = _inputs(rdir)
    _migrate(rdir, report, facts)
    trs = {f"{t['from_version']}→{t['to_version']}": t for t in report["transitions"]}
    out = {}
    for f in sorted((_root(rdir) / "results").glob("*.json")):
        r = json.loads(f.read_text(encoding="utf-8"))
        _, groups, support = packet(facts, r["transition"])
        chk = check(r["answer"], trs[r["transition"]], facts, groups, support)
        r.update(kept=chk["kept"], dropped=chk["dropped"], impact=chk["impact"], hard=chk["hard"],
                 coverage=coverage(facts, r["transition"], chk["kept"]))
        atomic_write_json(f, r)
        out[r["transition"]] = {"kept": len(chk["kept"]), "dropped": [x["point"]["object"] for x in chk["dropped"]],
                                "coverage": r["coverage"]["counts"]}
    return out
