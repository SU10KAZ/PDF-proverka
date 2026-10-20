"""Проверка ответа модели «Изменения в листе» и сборка результата листа (без ИИ).

Проверка согласованности — не доказательство правильности содержания:

* схема, ``packet_id``/``part_id`` (опечатка в ``packet_id`` при верном ``part_id`` — замечание,
  не отказ), ID кандидатов и изображений;
* у каждого поля — степень подтверждения: ``text`` (значение найдено в тексте своей
  стороны), ``image`` (прочитано с изображения, которое модель указала), ``unverified``
  (ни того, ни другого). Значение с изображения не отклоняется;
* «исправлен текст» — только с проверкой того же значения того же объекта в другом
  представлении (``other_representation``); иначе пункт уходит в «требует уточнения»;
* служебная фраза (облако, отметка, дата выпуска, подпись) вырезается из пункта, а не
  уничтожает его; пункт без остатка — в исключённое;
* предложения с причинами и оценками правильности вырезаются;
* полнота: кандидат вне всех списков — «не рассмотрен» (лист «Частично»).

Рамки доказательств берёт программа из кандидатов, не из ответа.
"""
from __future__ import annotations

import copy
import difflib
import re
from typing import Any

from backend.app.services.stage_comparison.ai import response_contract

from .. import geometry as geo
from . import schema as sc

_HOMO = str.maketrans("ABCEHKMOPTXaceopxyх×–—", "АВСЕНКМОРТХасеорхухх--")
SERVICE = re.compile(r"(облак|отметк\w* изменени|отметк\w* «?\d{1,2}\.\d|дат\w* выпуск|подпис|\bQR\b|штамп|"
                     r"рамк\w* лист|номер\w* изменени|таблиц\w* изменени)", re.IGNORECASE)
REASON = re.compile(r"\b(из-за|в связи с|для того|чтобы|правильн\w*|корректн\w*|ошибк\w*|обоснован\w*|"
                    r"соответству\w* (?:норм|требован|расч[её]т)\w*)\b", re.IGNORECASE)
_NUM = re.compile(r"\d+(?:[.,]\d+)?")
_SENT = re.compile(r"(?<=[.!?])\s+(?=[А-ЯЁA-Z«])")
# Модель переписывает 16-значный packet_id с опечаткой (пропущен/лишний символ, на проде сходство 0,94–0,97).
# Ответ всё равно на свой пакет — вызов один на пакет; совсем чужой номер по-прежнему отклоняется.
PACKET_ID_MIN_RATIO = 0.8


def norm(text: str) -> str:
    t = str(text or "").translate(_HOMO).casefold().replace(",", ".")
    return re.sub(r"\s+", "", t)


def _side_texts(packet: dict, cids: list[str], side: str) -> str:
    by = {c["id"]: c for c in packet["candidates"]}
    ptr = packet["pointers"]
    parts: list[str] = []
    key_rows = "rows_a" if side == "A" else "rows_b"
    for cid in cids:
        c = by.get(cid)
        if not c:
            continue
        parts += [r["text"] for r in c.get(key_rows) or []]
        parts += list(c.get("labels_a" if side == "A" else "labels_b") or [])
        parts.append(c.get("words_a" if side == "A" else "words_b") or "")
        parts.append(c.get("header") or "")
        for pid in c.get("pointers") or []:
            parts.append(ptr[pid]["before" if side == "A" else "after"])
        for x in packet.get("xrefs") or []:
            if x["side"] == side and cid in x["for"]:
                parts.append(x["text"])
    for x in packet.get("_extra_texts", {}).get(side, []):
        parts.append(x)
    return norm(" ".join(parts))


def _confirm(field: dict, a_text: str, b_text: str, has_image: bool) -> str:
    ok_a = not field["before"].strip() or norm(field["before"]) in a_text
    ok_b = not field["after"].strip() or norm(field["after"]) in b_text
    if ok_a and ok_b:
        return "text"
    if field["source"] in ("image", "text_and_image") and has_image:
        return "image"
    return "unverified"


def _strip(summary: str, rx: re.Pattern) -> tuple[str, list[str]]:
    sents = [s for s in _SENT.split(summary.strip()) if s.strip()]
    keep, cut = [], []
    for s in sents:
        (cut if rx.search(s) else keep).append(s)
    return " ".join(keep).strip(), cut


def _evidence(packet: dict, cids: list[str], values: dict[str, set[str]]) -> list[dict[str, Any]]:
    """Рамки на страницах A/B: строки и указатели, где встречаются значения пункта; иначе — место целиком."""
    by = {c["id"]: c for c in packet["candidates"]}
    ptr = packet["pointers"]
    pages = {"A": packet["a"]["page"], "B": packet["b"]["page"]}
    out: list[dict[str, Any]] = []
    for cid in cids:
        c = by.get(cid)
        if not c:
            continue
        found = False
        if c["kind"] == "table":
            for side, key in (("A", "rows_a"), ("B", "rows_b")):
                rows = c.get(key) or []
                # Сначала строки по обозначению; по совпадению значения — только если обозначения нет
                # (одинаковый размер бывает у разных изделий).
                hit = [r for r in rows if r.get("key") and norm(r["key"]) in values["keys"]]
                if not hit and not values["keys"] & {norm(r.get("key") or "") for r in c.get("rows_a", []) + c.get("rows_b", [])}:
                    hit = [r for r in rows if any(v and v in norm(r["text"]) for v in values[side])]
                for r in hit:
                    out.append({"side": side, "page": pages[side], "bbox": r["box"], "what": f"строка таблицы «{r.get('key') or '—'}»", "candidate": cid})
                    found = True
        else:
            for pid in c.get("pointers") or []:
                q = ptr[pid]
                hit_a = q["before"] and any(v and v in norm(q["before"]) for v in values["A"])
                hit_b = q["after"] and any(v and v in norm(q["after"]) for v in values["B"])
                if hit_a and q.get("bbox_a"):
                    out.append({"side": "A", "page": pages["A"], "bbox": q["bbox_a"], "what": f"надпись «{q['before'][:40]}»", "candidate": cid})
                    found = True
                if hit_b and q.get("bbox_b"):
                    out.append({"side": "B", "page": pages["B"], "bbox": q["bbox_b"], "what": f"надпись «{q['after'][:40]}»", "candidate": cid})
                    found = True
        if not found and c.get("area"):
            out.append({"side": "A", "page": pages["A"], "bbox": c["area"], "what": "место кандидата", "candidate": cid,
                        "system": "A"})
    for n, e in enumerate(out, 1):
        e["id"] = f"E{n}"
    return out


def check(packet: dict, answer: Any) -> dict[str, Any]:
    """Проверить ответ. Возвращает {ok, changes, unresolved, excluded, issues, coverage, context_requests}."""
    issues: list[dict[str, Any]] = []

    def issue(code: str, text: str, ref: str = "") -> None:
        issues.append({"code": code, "text": text, "ref": ref})

    answer = copy.deepcopy(answer)      # ответ модели не меняется: он сохраняется побайтно и для кэша
    errors = response_contract.validate(answer, sc.SHEET_CHANGES_SCHEMA)
    if errors:
        issue("SCHEMA", "; ".join(errors[:5]))
        return {"ok": False, "schema_errors": errors[:10], "issues": issues}
    typo = answer["packet_id"] != packet["packet_id"] and difflib.SequenceMatcher(
        None, answer["packet_id"], packet["packet_id"]).ratio() >= PACKET_ID_MIN_RATIO
    if (answer["packet_id"] != packet["packet_id"] and not typo) or answer["part_id"] != packet["part_id"]:
        issue("MIXED_INPUT", f"{answer['packet_id']}/{answer['part_id']}")
        return {"ok": False, "issues": issues}
    if typo:
        issue("PACKET_ID_TYPO", f"{answer['packet_id']} → {packet['packet_id']}")
    cands = {c["id"] for c in packet["candidates"]}
    image_ids = {i["id"] for i in packet["images"]} | set(packet.get("_extra_images") or [])
    img_cands = {i["id"]: set(i["candidates"]) for i in packet["images"]}

    def clean_ids(ids: list[str], ref: str) -> list[str]:
        bad = [i for i in ids if i not in cands]
        if bad:
            issue("UNKNOWN_CANDIDATE", ", ".join(bad), ref)
        return [i for i in ids if i in cands]

    def clean_imgs(ids: list[str], ref: str) -> list[str]:
        bad = [i for i in ids if i not in image_ids]
        if bad:
            issue("UNKNOWN_IMAGE", ", ".join(bad), ref)
        return [i for i in ids if i in image_ids]

    changes, unresolved, excluded = [], [], []
    for n, raw in enumerate(answer["changes"], 1):
        ref = f"CH{n}"
        ch = dict(raw)
        ch["candidate_ids"] = clean_ids(ch["candidate_ids"], ref)
        ch["image_ids"] = clean_imgs(ch["image_ids"], ref)
        ch["flags"] = []
        if not ch["candidate_ids"]:
            issue("NO_CANDIDATE", "пункт без действительных кандидатов", ref)
            ch["flags"].append("no_candidate")
        a_text = _side_texts(packet, ch["candidate_ids"], "A")
        b_text = _side_texts(packet, ch["candidate_ids"], "B")
        has_img = bool(ch["image_ids"])
        all_fields = list(ch["fields"]) + [f for m in ch["members"] for f in m["fields"]]
        for f in all_fields:
            f["confirmation"] = _confirm(f, a_text, b_text, has_img)
            if f["confirmation"] == "unverified":
                issue("VALUE_UNVERIFIED", f"{f['name']}: «{f['before']}» → «{f['after']}» — нет ни в тексте своей стороны, "
                      "ни на указанном изображении", ref)
        # Служебная часть и причины — вырезать предложения, а не весь пункт.
        summary, cut = _strip(ch["summary"], SERVICE)
        if cut:
            issue("SERVICE_PART_REMOVED", " | ".join(cut), ref)
        if not summary:
            excluded.append({"candidate_ids": ch["candidate_ids"], "reason": "revision_mark",
                             "note": "пункт состоял только из служебной части: " + ch["summary"][:200], "source": "program"})
            continue
        summary2, cut2 = _strip(summary, REASON)
        if cut2 and summary2:
            issue("REASON_REMOVED", " | ".join(cut2), ref)
            summary = summary2
        ch["summary"] = summary
        # «Исправлен текст» — только с проверенным вторым представлением того же объекта.
        if ch["kind"] == "text_correction":
            o = ch["other_representation"]
            afters = {norm(f["after"]) for f in ch["fields"] if f["after"].strip()}
            ok = (o["what"].strip() and o["value_a"].strip() and norm(o["value_a"]) == norm(o["value_b"])
                  and norm(o["value_a"]) in afters and o["image_id"] in ch["image_ids"]
                  and (not img_cands.get(o["image_id"]) or img_cands[o["image_id"]] & set(ch["candidate_ids"])))
            if not ok:
                issue("TEXT_CORRECTION_UNCHECKED", "«исправлен текст» без проверки того же значения в другом представлении", ref)
                unresolved.append({"object": ch["object"], "place": ch["place"],
                                   "question": f"{summary} Исправлен ли только текст или изменено значение — "
                                               "по другому представлению объекта не подтверждено.",
                                   "candidate_ids": ch["candidate_ids"], "image_ids": ch["image_ids"],
                                   "fields": ch["fields"], "source": "program"})
                continue
        values = {"A": {norm(f["before"]) for f in all_fields if f["before"].strip()},
                  "B": {norm(f["after"]) for f in all_fields if f["after"].strip()},
                  "keys": {norm(f[k]) for f in all_fields for k in ("before", "after") if f[k].strip()}}
        ch["evidence"] = _evidence(packet, ch["candidate_ids"], values)
        ch["kind_label"] = sc.KIND_LABELS.get(ch["kind"], ch["kind"])
        ch["confirmation"] = ("unverified" if any(f["confirmation"] == "unverified" for f in all_fields)
                              else "image" if any(f["confirmation"] == "image" for f in all_fields) else "text")
        ch["unclear"] = any(f["certainty"] == "unclear" for f in all_fields) or bool(ch["limitation"].strip())
        changes.append(ch)
    for raw in answer["unresolved"]:
        u = dict(raw, candidate_ids=clean_ids(raw["candidate_ids"], "unresolved"),
                 image_ids=clean_imgs(raw["image_ids"], "unresolved"), source="model")
        unresolved.append(u)
    for raw in answer["excluded"]:
        excluded.append(dict(raw, candidate_ids=clean_ids(raw["candidate_ids"], "excluded"), source="model"))
    for n, c in enumerate(changes, 1):
        c["id"] = f"CH{n}"
    for n, u in enumerate(unresolved, 1):
        u["id"] = f"U{n}"
        vals = {"A": set(), "B": set(), "keys": set()}
        u["evidence"] = _evidence(packet, u["candidate_ids"], vals)
    covered = {i for x in changes + unresolved + excluded for i in x["candidate_ids"]}
    not_reviewed = sorted(cands - covered, key=lambda v: int(v[1:]))
    if not_reviewed:
        issue("NOT_REVIEWED", ", ".join(not_reviewed))
    coverage = {"candidates": len(cands), "in_changes": len({i for c in changes for i in c["candidate_ids"]}),
                "in_unresolved": len({i for u in unresolved for i in u["candidate_ids"]}),
                "excluded": len({i for e in excluded for i in e["candidate_ids"]}), "not_reviewed": not_reviewed}
    return {"ok": True, "changes": changes, "unresolved": unresolved, "excluded": excluded, "issues": issues,
            "coverage": coverage, "context_requests": answer["context_requests"]}


def state_of(chk: dict, packet: dict) -> str:
    if not chk.get("ok"):
        return "failed"
    if chk["coverage"]["not_reviewed"]:
        return "partial"
    if chk["unresolved"]:
        return "needs_clarification"
    if not chk["changes"]:
        notes = packet["engine"].get("notes") or []
        return "partial" if notes else "no_construction_changes"
    return "done"


STATE_LABELS = {
    "not_processed": "Не обработано", "queued": "В очереди", "running": "В обработке", "done": "Готово",
    "needs_clarification": "Требуется уточнение", "partial": "Частично",
    "no_construction_changes": "Строительных изменений не выявлено", "stale": "Описание устарело",
    "failed": "Ошибка", "cancelled": "Отменено", "engine_same": "По данным сравнения изменений нет",
}


def box_a(packet: dict, e: dict) -> list[float]:
    """Рамка доказательства в системе A (холст просмотрщика)."""
    if e["side"] == "B" and e.get("system") != "A" and packet.get("transform"):
        return geo.b_page_to_a(packet["transform"], e["bbox"])
    return e["bbox"]
