"""Детерминированная проверка ответа модели и учёт каждой записи (план §6).

Полнота учёта и полнота анализа разделены (поправка А от 15.10). Каждая запись
листа попадает ровно в одну из корзин:

* ``ai_reviewed`` — описана ИИ (описана, объединена, служебная, оформление,
  неоднозначна), а для графики (G/M/N) — с просмотренным изображением;
* ``engine_classified`` — классифицирована движком, в ИИ не уходила;
* ``linked_unverified`` — учтена через связь с текстовым изменением, отдельно не
  проверялась;
* ``not_reviewed`` — не просмотрена: модель так ответила, не ответила, ответ
  отклонён, часть не отправлена, графика без изображения.

Координаты из ответа модели не берутся нигде: в схеме их нет.
"""
from __future__ import annotations

import re
from typing import Any

from backend.app.services.stage_comparison.ai import response_contract

from . import context as ctx
from .schema import ANSWER_SCHEMA, CATEGORIES

GRAPHIC_TYPES = ("region", "block", "annotation")
REVIEWED_FATES = ("described", "merged", "service", "formatting", "needs_check")
_NUM = re.compile(r"\d+(?:[.,]\d+)?")

ISSUE_TEXT = {
    "SCHEMA": "ответ не соответствует схеме",
    "MIXED_INPUT": "ответ относится к другому пакету или части",
    "UNKNOWN_ID": "ссылка на несуществующую запись",
    "FOREIGN_ID": "ссылка на запись другой части листа",
    "UNKNOWN_CONTEXT": "ссылка на несуществующий контекст",
    "UNKNOWN_IMAGE": "ссылка на изображение, которого не было в части",
    "NO_EVIDENCE": "пункт без действительных доказательств",
    "DUPLICATE_DISPOSITION": "у записи несколько судеб — взята первая",
    "DISPOSITION_ITEM_MISMATCH": "судьба указывает на пункт, который эту запись не содержит",
    "MISSING_DISPOSITION": "запись не получила судьбы от модели",
    "DIRECTION_UNVERIFIED": "значение «было/стало» не найдено на своей стороне",
    "UNSUPPORTED_TOKEN": "число или марка из текста пункта не найдены в доказательствах",
    "UNREADABLE_NOT_FLAGGED": "нечитаемое значение описано без неопределённости",
    "CROSS_CHECK_ONE_SIDE": "расхождение между листами без доказательств с двух страниц",
    "GRAPHIC_WITHOUT_IMAGE": "графика описана без просмотра изображения — не считается проверенной",
    "EMPTY_ANSWER": "пустой ответ при непустом списке записей",
}


def _norm(text: str) -> str:
    return " ".join(str(text or "").replace(",", ".").split()).casefold()


def _side_texts(rec: dict, contexts: dict, xrefs: dict, side: str) -> str:
    parts = [rec.get("before" if side == "a" else "after") or "", rec.get("label") or ""]
    for f in (rec.get("fields") or {}).values():
        parts.append(f[0] if side == "a" else f[1])
    for cid in rec.get("context_ids") or []:
        c = contexts.get(cid)
        if c and c.get(side):
            s = c[side]
            parts += [s.get("row") or "", s.get("near") or ""] + list((s.get("spec") or {}).values())
        x = xrefs.get(cid)
        if x and x.get("side", "").lower() == side:
            parts += [x.get("text") or ""] + list((x.get("fields") or {}).values())
    return _norm(" ".join(parts))


def _numbers(text: str) -> set[str]:
    return {n.replace(",", ".") for n in _NUM.findall(text or "")}


def check_chunk(packet: dict[str, Any], chunk: dict[str, Any], answer: Any) -> dict[str, Any]:
    """Проверить ответ на одну часть. Возвращает пункты, судьбы записей и замечания."""
    recs = {r["id"]: r for r in packet["records"]}
    chunk_ids = list(chunk["record_ids"])
    in_chunk = set(chunk_ids)
    contexts = {c["id"]: c for c in packet.get("contexts") or []}
    xrefs = {x["id"]: x for x in packet.get("xrefs") or []}
    chunk_images = set(chunk["image_ids"])
    img_records = {i["id"]: set(i.get("record_ids") or []) for i in packet.get("images") or []}
    issues: list[dict[str, Any]] = []

    def issue(code: str, ids_: list[str] | None = None, detail: str = "") -> None:
        issues.append({"code": code, "message": ISSUE_TEXT[code], "ids": ids_ or [], "detail": detail})

    def all_not_reviewed(reason: str) -> dict[str, Any]:
        return {"items": [], "fates": {i: {"fate": "not_reviewed", "source": "program", "item_id": "",
                                          "reason": reason} for i in chunk_ids}, "issues": issues, "ok": False}

    errors = response_contract.validate(answer, ANSWER_SCHEMA)
    if errors:
        issue("SCHEMA", detail="; ".join(errors[:5]))
        return all_not_reviewed(ISSUE_TEXT["SCHEMA"])
    if answer["packet_id"] != packet["packet_id"] or answer["chunk_id"] != chunk["chunk_id"]:
        issue("MIXED_INPUT", detail=f"{answer['packet_id']}/{answer['chunk_id']}")
        return all_not_reviewed(ISSUE_TEXT["MIXED_INPUT"])

    items: dict[str, dict[str, Any]] = {}
    for raw in answer["items"]:
        it = dict(raw)
        it["flags"] = []
        ev = []
        for e in it["evidence_ids"]:
            if e not in recs:
                issue("UNKNOWN_ID", [e], f"пункт {it['item_id']}")
            elif e not in in_chunk:
                issue("FOREIGN_ID", [e], f"пункт {it['item_id']}")
            else:
                ev.append(e)
        it["evidence_ids"] = ev
        cx = [c for c in it["context_ids"] if c in contexts or c in xrefs]
        for c in set(it["context_ids"]) - set(cx):
            issue("UNKNOWN_CONTEXT", [c], f"пункт {it['item_id']}")
        it["context_ids"] = cx
        imgs = [i for i in it["images_used"] if i in chunk_images]
        for i in set(it["images_used"]) - set(imgs):
            issue("UNKNOWN_IMAGE", [i], f"пункт {it['item_id']}")
        it["images_used"] = imgs
        if not ev:
            issue("NO_EVIDENCE", [], f"пункт {it['item_id']}")
            it["flags"].append("no_evidence")
            it["category"] = "needs_check"
        # Направление: «было» — на стороне A, «стало» — на стороне B.
        for v in it["values"]:
            r = recs.get(v["evidence_id"])
            if not r or v["evidence_id"] not in ev:
                continue
            ok_a = not v["before"] or _norm(v["before"]) in _side_texts(r, contexts, xrefs, "a")
            ok_b = not v["after"] or _norm(v["after"]) in _side_texts(r, contexts, xrefs, "b")
            if not (ok_a and ok_b):
                issue("DIRECTION_UNVERIFIED", [v["evidence_id"]], f"пункт {it['item_id']}: "
                      f"{v['before']!r} → {v['after']!r}")
                it["flags"].append("direction_unverified")
        # Числа и марки текста пункта должны быть в доказательствах.
        support = " ".join(_side_texts(recs[e], contexts, xrefs, s) for e in ev for s in ("a", "b"))
        support += " " + " ".join(_norm(x.get("text") or "") + " " + _norm(" ".join((x.get("fields") or {}).values()))
                                  for c in cx for x in [xrefs.get(c) or {}])
        support += " " + " ".join(_norm(f"{v['before']} {v['after']} {v['unit']}") for v in it["values"])
        missing = sorted((_numbers(it["summary"]) - _numbers(support))
                         | {m for m in ctx.marks(it["summary"]) if _norm(m) not in support})
        if missing:
            issue("UNSUPPORTED_TOKEN", ev, f"пункт {it['item_id']}: {', '.join(missing)}")
            it["flags"].append("unsupported_token")
        if any("unreadable" in recs[e]["flags"] for e in ev) and it["category"] != "needs_check" \
                and not it["limitation"].strip():
            issue("UNREADABLE_NOT_FLAGGED", ev, f"пункт {it['item_id']}")
            it["flags"].append("unreadable_not_flagged")
        if it["cross_check"]:
            own = {"A": packet["a"]["page"], "B": packet["b"]["page"]}
            pages_ = {("этот лист",)} if ev else set()
            pages_ |= {(xrefs[c]["side"], xrefs[c]["page"]) for c in cx
                       if c in xrefs and xrefs[c]["page"] != own.get(xrefs[c]["side"])}
            if len(pages_) < 2:
                issue("CROSS_CHECK_ONE_SIDE", ev, f"пункт {it['item_id']}")
                it["flags"].append("cross_check_one_side")
        if it["flags"] and it["category"] in CATEGORIES and it["category"] != "needs_check":
            it["original_category"] = it["category"]
            it["category"] = "needs_check"
        items[it["item_id"]] = it

    fates: dict[str, dict[str, Any]] = {}
    for d in answer["dispositions"]:
        rid = d["id"]
        if rid not in recs:
            issue("UNKNOWN_ID", [rid], "в dispositions")
            continue
        if rid not in in_chunk:
            issue("FOREIGN_ID", [rid], "в dispositions")
            continue
        if rid in fates:
            issue("DUPLICATE_DISPOSITION", [rid])
            continue
        fate, item_id = d["fate"], d["item_id"]
        if fate != "not_reviewed":
            it = items.get(item_id)
            if not it or rid not in it["evidence_ids"]:
                issue("DISPOSITION_ITEM_MISMATCH", [rid], f"пункт {item_id!r}")
                fates[rid] = {"fate": "not_reviewed", "source": "program", "item_id": "",
                              "reason": ISSUE_TEXT["DISPOSITION_ITEM_MISMATCH"]}
                continue
            if it["category"] == "needs_check":
                fate = "needs_check"
            # Графика без просмотренного изображения, где она видна, — не проверена.
            if recs[rid]["type"] in GRAPHIC_TYPES and not any(rid in img_records.get(i, ()) for i in it["images_used"]):
                issue("GRAPHIC_WITHOUT_IMAGE", [rid], f"пункт {item_id}")
                fates[rid] = {"fate": "not_reviewed", "source": "program", "item_id": item_id,
                              "reason": ISSUE_TEXT["GRAPHIC_WITHOUT_IMAGE"]}
                continue
        fates[rid] = {"fate": fate, "source": "ai", "item_id": item_id if fate != "not_reviewed" else "",
                      "reason": d["reason"]}
    missing = [i for i in chunk_ids if i not in fates]
    if missing:
        if not answer["items"] and not answer["dispositions"]:
            issue("EMPTY_ANSWER", missing)
        else:
            issue("MISSING_DISPOSITION", missing)
        for i in missing:
            fates[i] = {"fate": "not_reviewed", "source": "program", "item_id": "",
                        "reason": ISSUE_TEXT["MISSING_DISPOSITION"]}
    ok = all(f["fate"] in REVIEWED_FATES for f in fates.values()) and not any(
        i["code"] in ("UNKNOWN_ID", "FOREIGN_ID", "UNKNOWN_IMAGE", "UNKNOWN_CONTEXT") for i in issues)
    return {"items": list(items.values()), "fates": fates, "issues": issues, "ok": ok,
            "unreadable_images": answer["unreadable_images"], "context_requests": answer["context_requests"]}


def ledger(packet: dict[str, Any], fates: dict[str, dict[str, Any]]) -> tuple[dict[str, Any], dict[str, int]]:
    """Судьба каждой записи листа и счётчики четырёх корзин."""
    out: dict[str, Any] = {}
    counts = {"ai_reviewed": 0, "engine_classified": 0, "linked_unverified": 0, "not_reviewed": 0}
    for r in packet["records"]:
        rid = r["id"]
        if r["group"] == "engine":
            out[rid] = {"bucket": "engine_classified", "fate": r["fate"], "source": "engine"}
        elif r["group"] == "linked":
            linked_fates = [fates.get(t, {}).get("fate") for t in r.get("links") or []]
            out[rid] = {"bucket": "linked_unverified", "fate": "linked", "source": "program", "links": r["links"],
                        "linked_text_reviewed": bool(linked_fates) and all(f in REVIEWED_FATES for f in linked_fates)}
        else:
            f = fates.get(rid) or {"fate": "not_reviewed", "source": "program", "item_id": "",
                                   "reason": "часть с этой записью не разобрана"}
            bucket = "ai_reviewed" if f["fate"] in REVIEWED_FATES else "not_reviewed"
            out[rid] = {"bucket": bucket, **f}
        counts[out[rid]["bucket"]] += 1
    return out, counts
