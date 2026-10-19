"""Сводка листа: группы изменений поверх сохранённых пунктов (без повторного анализа чертежей).

Два шага:

1. Программное объединение там, где соответствие однозначно: пункты одной категории
   с одинаковым набором пар «было → стало» (и одинаковым ограничением) — один блок U.
2. Редакционное сведение блоков в группы — одно текстовое обращение на лист (по
   разрешению), без изображений и PDF. Ответ проверяет программа: каждый пункт ровно в
   одной группе, категории не смешаны, в тексте группы только числа и марки её
   пунктов, пары «было → стало» существуют у пунктов, нет обобщений «везде/все»,
   причин и оценок правильности. Группа, не прошедшая проверку, разворачивается в
   программные блоки. Ограничения и статусы неопределённости группе приписывает
   программа из пунктов — модель их не переписывает.

Слой хранится отдельно от ответов модели на части листа (``layers/summary-…``) со
своей версией и ключом кэша; исходный ответ сохраняется побайтно. Без сводки (или при
ошибке) остаётся подробный отчёт.
"""
from __future__ import annotations

import hashlib
import json
import re
import time
from pathlib import Path
from typing import Any

from backend.app.services.stage_comparison.ai import response_contract

from . import context as ctx
from . import executor as ex
from . import settings
from . import verify as verify_mod
from .schema import CATEGORIES, CATEGORY_LABELS
from .validate import _numbers

SUMMARY_VERSION = "vs1"
TEMPLATE = Path(__file__).with_name("prompts") / "summarize_sheet.md"
VALIDATOR_VERSION = 2   # 2: «соответствующие позиции» — не оценка; оценкой считается «соответствует нормам/расчёту»
FORBIDDEN = re.compile(r"\b(везде|во всех|всех|все|всё|из-за|в связи с|для того|чтобы|правильн\w*|корректн\w*|"
                       r"соответству\w* (?:норм|требован|расч[её]т|проект)\w*|ошибк\w*|обоснован\w*|"
                       r"верн(?:о|ый|ая|ое|ые))\b", re.IGNORECASE)
ARROW = re.compile(r"«([^»]+)»\s*(?:→|->|на)\s*«([^»]+)»")

SUMMARY_SCHEMA: dict = {
    "type": "object", "additionalProperties": False, "required": ["sheet", "groups"],
    "properties": {
        "sheet": {"type": "string"},
        "groups": {"type": "array", "items": {
            "type": "object", "additionalProperties": False,
            "required": ["group_id", "category", "title", "brief", "unit_ids"],
            "properties": {"group_id": {"type": "string"}, "category": {"type": "string", "enum": list(CATEGORIES)},
                           "title": {"type": "string"}, "brief": {"type": "string"},
                           "unit_ids": {"type": "array", "items": {"type": "string"}}}}},
    },
}


def _digest(obj: Any) -> str:
    return settings.digest(obj)


def template_digest() -> str:
    return hashlib.sha256(TEMPLATE.read_bytes()).hexdigest()


# ------------------------------------------------------------------ вход: пункты с итоговой категорией

def items_for_summary(res: dict, verification: dict | None) -> list[dict[str, Any]]:
    """Пункты листа с категорией после проверки предупреждений. Исходные поля не меняются."""
    ver = (verification or {}).get("items") or {}
    out = []
    for it in res["items"]:
        v = ver.get(it["item_id"])
        cat = v["final_category"] if v else it["category"]
        out.append({"item_id": it["item_id"], "category": cat, "check_class": v["class"] if v and cat == "needs_check"
                    else "", "summary": it["summary"], "location": it.get("location") or "",
                    "limitation": (it.get("limitation") or "").strip(),
                    "values": [{"before": x["before"], "after": x["after"], "unit": x["unit"]} for x in it["values"]],
                    "evidence_ids": it["evidence_ids"], "verification": v})
    return out


def _pairs(it: dict) -> tuple:
    return tuple(sorted({(" ".join(v["before"].split()), " ".join(v["after"].split())) for v in it["values"]}))


def units(items: list[dict]) -> list[dict[str, Any]]:
    """Программное объединение: одинаковые категория, класс проверки, ограничение и пары «было → стало»."""
    by_key: dict[tuple, list[dict]] = {}
    order: list[tuple] = []
    for it in items:
        p = _pairs(it)
        key = (it["category"], it["check_class"], it["limitation"], p) if p else ("single", it["item_id"])
        if key not in by_key:
            order.append(key)
        by_key.setdefault(key, []).append(it)
    out = []
    for n, key in enumerate(order, 1):
        members = by_key[key]
        first = members[0]
        if len(members) == 1:
            brief = _first_sentence(first["summary"])
        else:
            from .view import delta
            pairs = "; ".join(delta(b, a) for b, a in key[3])
            brief = f"Одинаковое изменение в {len(members)} пунктах: {pairs}."
        out.append({"unit_id": f"U{n}", "category": first["category"], "check_class": first["check_class"],
                    "item_ids": [m["item_id"] for m in members], "brief": brief})
    return out


def _first_sentence(text: str) -> str:
    from .view import first_sentence
    return first_sentence(text)


# ------------------------------------------------------------------ обращение

def build_prompt(sheet_label: str, us: list[dict], items: list[dict]) -> tuple[str, str]:
    by_id = {i["item_id"]: i for i in items}
    data = {"sheet": sheet_label, "units": [{
        "unit_id": u["unit_id"], "category": u["category"], "check_class": u["check_class"] or None,
        "items": [{"item_id": i, "summary": by_id[i]["summary"], "location": by_id[i]["location"],
                   "values": by_id[i]["values"]} for i in u["item_ids"]]} for u in us]}
    marker = hashlib.sha256(json.dumps(data, ensure_ascii=False).encode()).hexdigest()[:12]
    text = (TEMPLATE.read_text(encoding="utf-8")
            .replace("{{BEGIN}}", f"<<<ДАННЫЕ-{marker}>>>").replace("{{END}}", f"<<<КОНЕЦ-{marker}>>>")
            .replace("{{DATA}}", json.dumps(data, ensure_ascii=False, indent=1)))
    return text, marker


# ------------------------------------------------------------------ проверка ответа

def _text_of(items: list[dict]) -> str:
    return " ".join(f"{i['summary']} {i['location']} " + " ".join(f"{v['before']} {v['after']} {v['unit']}"
                                                                   for v in i["values"]) for i in items)


def check_group(g: dict, members: list[dict]) -> list[str]:
    problems = []
    cats = {m["category"] for m in members}
    if len(cats) > 1:
        problems.append("в группе смешаны категории")
    elif g["category"] not in cats:
        problems.append("категория группы не совпадает с категорией пунктов")
    if "needs_check" in cats and len({m["check_class"] for m in members}) > 1:
        problems.append("в группе «Нужна проверка» смешаны разные классы неопределённости")
    text = f"{g['title']} {g['brief']}"
    src = _text_of(members)
    extra_nums = _numbers(text) - _numbers(src) - {str(len(members))}
    if extra_nums:
        problems.append("числа не из пунктов группы: " + ", ".join(sorted(extra_nums)))
    norm_src = ctx.norm_mark(src.replace(" ", ""))
    extra_marks = [m for m in ctx.marks(text) if ctx.norm_mark(m) not in norm_src]
    if extra_marks:
        problems.append("марки не из пунктов группы: " + ", ".join(extra_marks))
    bad = sorted({m.group(0).lower() for m in FORBIDDEN.finditer(text)})
    if bad:
        problems.append("недопустимые слова (обобщение, причина или оценка): " + ", ".join(bad))
    for b, a in ARROW.findall(g["brief"]):
        ok = any(b.strip() in v["before"] and a.strip() in v["after"] for m in members for v in m["values"])
        if not ok:
            problems.append(f"пары «{b}» → «{a}» нет ни у одного пункта группы")
    return problems


def assemble(us: list[dict], items: list[dict], answer: dict | None, sheet_label: str) -> dict[str, Any]:
    """Группы по ответу (если он прошёл проверку) или по программным блокам; учёт каждого пункта."""
    by_item = {i["item_id"]: i for i in items}
    by_unit = {u["unit_id"]: u for u in us}
    groups, rejected, used = [], [], set()
    schema_errors = response_contract.validate(answer, SUMMARY_SCHEMA) if answer is not None else []
    if answer is not None and not schema_errors and answer["sheet"] == sheet_label:
        for g in answer["groups"]:
            uids = [u for u in g["unit_ids"] if u in by_unit]
            unknown = [u for u in g["unit_ids"] if u not in by_unit]
            dup = [u for u in uids if u in used]
            members = [by_item[i] for u in uids for i in by_unit[u]["item_ids"]]
            problems = check_group(g, members) if members else ["пустая группа"]
            if unknown:
                problems.append("неизвестные блоки: " + ", ".join(unknown))
            if dup:
                problems.append("блоки уже в другой группе: " + ", ".join(dup))
            if problems:
                rejected.append({"group_id": g["group_id"], "title": g["title"], "brief": g["brief"],
                                 "unit_ids": g["unit_ids"], "problems": problems})
                continue
            used.update(uids)
            groups.append({"group_id": g["group_id"], "category": g["category"], "title": g["title"],
                           "brief": g["brief"], "brief_source": "ai", "unit_ids": uids})
    elif answer is not None:
        rejected.append({"group_id": "*", "problems": schema_errors[:5] or ["ответ относится к другому листу"]})
    # Блоки без группы (нет ответа, группа отклонена, блок пропущен) — каждый своей группой.
    for u in us:
        if u["unit_id"] not in used:
            groups.append({"group_id": f"P{u['unit_id']}", "category": u["category"], "title": "",
                           "brief": u["brief"], "brief_source": "program", "unit_ids": [u["unit_id"]]})
            used.add(u["unit_id"])
    for g in groups:
        ids = [i for u in g["unit_ids"] for i in by_unit[u]["item_ids"]]
        g["item_ids"] = ids
        g["category_label"] = CATEGORY_LABELS.get(g["category"], g["category"])
        g["limitations"] = [{"item_id": i, "text": by_item[i]["limitation"]} for i in ids if by_item[i]["limitation"]]
        g["checks"] = [{"item_id": i, "class": by_item[i]["check_class"],
                        "class_label": verify_mod.CLASS_LABELS.get(by_item[i]["check_class"], "")}
                       for i in ids if by_item[i]["category"] == "needs_check"]
        g["verified_items"] = [i for i in ids if (by_item[i]["verification"] or {}).get("class") == "resolved"]
        g["uncertain"] = bool(g["limitations"] or g["checks"])
        g["evidence_ids"] = [e for i in ids for e in by_item[i]["evidence_ids"]]
    mapping: dict[str, list[str]] = {}
    for g in groups:
        for i in g["item_ids"]:
            mapping.setdefault(i, []).append(g["group_id"])
    coverage = {"items_total": len(items), "mapped_once": sum(1 for v in mapping.values() if len(v) == 1),
                "missing": [i for i in by_item if i not in mapping],
                "duplicated": [i for i, v in mapping.items() if len(v) > 1],
                "evidence_total": len({e for i in items for e in i["evidence_ids"]}),
                "evidence_in_groups": len({e for g in groups for e in g["evidence_ids"]})}
    coverage["ok"] = not coverage["missing"] and not coverage["duplicated"] and \
        coverage["evidence_total"] == coverage["evidence_in_groups"]
    order = {c: n for n, c in enumerate(("content", "formatting", "service", "needs_check"))}
    groups.sort(key=lambda g: order.get(g["category"], 9))
    return {"groups": groups, "rejected": rejected, "mapping": {k: v[0] for k, v in mapping.items()},
            "coverage": coverage}


# ------------------------------------------------------------------ слой

def layer_key(items: list[dict], us: list[dict], describer: ex.ChangeDescriber | None) -> str:
    rt = describer.describe_runtime() if describer else {}
    return _digest({"v": SUMMARY_VERSION, "items": [{k: i[k] for k in ("item_id", "category", "check_class",
                                                                       "summary", "limitation", "values")}
                                                    for i in items],
                    "units": us, "template": template_digest(), "schema": _digest(SUMMARY_SCHEMA),
                    "executor": rt.get("executor"), "model": rt.get("model"), "effort": rt.get("effort")})[:16]


def build(sheet_dir: Path, object_id: str, *, describer: ex.ChangeDescriber | None = None,
          allow_call: bool = False, materialize: bool = True) -> dict[str, Any]:
    """Сводка листа. Обращение к модели — только при ``allow_call`` и отсутствии готового слоя.

    ``materialize=False`` (показ viewer): ничего не пишется — ни слой проверки (нет его —
    ``verify.LayerNotPrepared``), ни пересобранная сводка, ни обращение к модели.
    """
    from backend.app.services.version_comparison.jobs import may_write
    materialize = may_write(materialize)
    res = json.loads((sheet_dir / "result.json").read_text(encoding="utf-8"))
    ver = verify_mod.build(sheet_dir, object_id, materialize=materialize)
    items = items_for_summary(res, ver)
    us = units(items)
    sheet_label = f"{res.get('title')} — лист {res['b'].get('sheet_no') or res['a'].get('sheet_no')}"
    key = layer_key(items, us, describer)
    ldir = sheet_dir / "layers" / f"summary-{SUMMARY_VERSION}-{key}"
    out_f = ldir / "summary.json"
    if out_f.exists():
        done = json.loads(out_f.read_text(encoding="utf-8"))
        if done.get("validator") == VALIDATOR_VERSION:
            return done
        # Новая версия проверки ответа: пересборка из сохранённого ответа, без нового обращения.
        prev = ldir / f"summary.validator{done.get('validator', 1)}.json"
        if materialize and not prev.exists():
            prev.write_bytes(out_f.read_bytes())
        ans_f = ldir / "answer.json"
        answer = json.loads(ans_f.read_text(encoding="utf-8")) if ans_f.exists() else None
        out = {k: v for k, v in done.items() if k not in ("groups", "rejected", "mapping", "coverage")}
        out.update(validator=VALIDATOR_VERSION, reassembled_from="answer.json", **assemble(us, items, answer, sheet_label))
        out["mode"] = "editorial" if answer is not None else "programmatic"
        if materialize:
            out_f.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
        return out
    base = {"schema": "version_comparison_ai.summary/v1", "validator": VALIDATOR_VERSION, "version": SUMMARY_VERSION, "key": key,
            "spid": res["spid"], "verification_key": ver["key"], "units": us,
            "note": "Сводка собрана из сохранённых пунктов; чертежи повторно не анализировались. "
                    "Инженером не подтверждено."}
    if not allow_call or describer is None or not materialize:
        return {**base, "mode": "programmatic", **assemble(us, items, None, sheet_label)}
    prompt_text, marker = build_prompt(sheet_label, us, items)
    ldir.mkdir(parents=True, exist_ok=True)
    (ldir / "prompt.md").write_text(prompt_text, encoding="utf-8")
    task = ex.DescribeTask(prompt=prompt_text, schema=SUMMARY_SCHEMA, image_paths=[],
                           timeout_s=settings.call_timeout_s(), marker=f"vcai-sum-{marker}",
                           packet_id=res["packet_id"], chunk_id="summary",
                           raw_path=str(ldir / "answer.raw.attempt1.txt"))
    t0 = time.time()
    r = describer.run(task, ex.CancelToken())
    call = {"executor": describer.describe_runtime(), "ok": r.ok, "error_kind": r.error_kind, "error": r.error,
            "usage": r.usage, "seconds": round(time.time() - t0, 1), "images": 0,
            "prompt_bytes": len(prompt_text.encode("utf-8"))}
    (ldir / "call.json").write_text(json.dumps(call, ensure_ascii=False, indent=1), encoding="utf-8")
    answer = r.parsed if r.ok else None
    if answer is not None:
        (ldir / "answer.json").write_text(json.dumps(answer, ensure_ascii=False, indent=1), encoding="utf-8")
    out = {**base, "mode": "editorial" if answer is not None else "programmatic", "call": call,
           **assemble(us, items, answer, sheet_label)}
    out_f.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    return out


def latest(sheet_dir: Path) -> dict[str, Any] | None:
    found = sorted((sheet_dir / "layers").glob(f"summary-{SUMMARY_VERSION}-*/summary.json"),
                   key=lambda p: p.stat().st_mtime)
    return json.loads(found[-1].read_text(encoding="utf-8")) if found else None
