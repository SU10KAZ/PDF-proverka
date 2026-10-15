"""Текст для модели: инструкции (шаблон) + данные пакета в ограждённом блоке.

Один русский шаблон без EN-копии (план §5.4): правки шаблона доходят до модели
без рассинхрона. Данные документов стоят между метками, уникальными для пакета,
поэтому надпись в PDF не может «закрыть» блок данных.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from . import settings
from .records import FATE_LABELS

TEMPLATE = Path(__file__).with_name("prompts") / "describe_sheet.md"

CHANNELS = {
    "geometry": "графика", "color": "цвет и оформление линий", "text": "текст", "text_coverage": "текстовый слой",
    "zones": "служебные зоны", "annotations": "пометки (аннотации)", "warnings": "предупреждения",
    "spec_positions": "позиции СО",
}
STATES = {"checked": "проверено", "unchecked": "НЕ проверено", "not_applicable": "не применяется"}
REASONS = {
    "MATCH_LOW_CONFIDENCE": "сопоставление листов с низкой уверенностью",
    "MATCH_AMBIGUOUS": "сопоставление листов неоднозначно",
    "MATCH_UNRESOLVED": "лист не сопоставлен",
    "ALIGNMENT_UNRELIABLE": "совмещение листов ненадёжно",
    "ALIGNMENT_PARTIAL": "совмещена только часть листа",
    "PARTIAL_COVERAGE": "часть площади не проверена (предупреждение)",
    "DUPLICATE_SHEET": "дубль листа в версии",
    "RENDER_FAILED": "рендер не выполнен",
    "NO_TEXT_LAYER": "нет текстового слоя",
    "TEXT_UNREADABLE_IN_DIFF_AREA": "в областях отличий есть надписи без текстового слоя",
    "STAMP_NOT_RECOGNIZED": "штамп не распознан",
    "SPEC_NOT_PARSED": "спецификация не разобрана",
}
KINDS = {
    "changed": "изменено", "added": "добавлено (B)", "removed": "удалено (A)", "moved": "сдвиг",
    "text": "текст", "light": "светлое", "color": "цвет", "style": "оформление линии", "text_moved": "сдвиг надписи",
    "shifted": "сдвиг", "block_moved": "перемещённый блок",
}
TYPES = {"text": "текст", "region": "регион", "block": "блок", "spec": "позиция СО", "service": "служебное поле",
         "zone": "служебная зона", "overlay": "наложенный объект", "annotation": "пометка"}
FLAGS = {
    "unreadable": "нечитаемые знаки «###»", "in_moved_block": "внутри перемещённого блока",
    "norm_symbol": "замена латиница/кириллица", "norm_number_format": "формат числа «,»/«.»",
    "text_unreadable": "надпись без текстового слоя", "text_and_graphics": "текст и графика в одной области",
    "region_text_truncated": "список слов региона усечён движком", "ambiguous": "граница позиции неоднозначна",
    "zone_bbox": "координаты — рамка зоны, не поля",
    "warning_removed_not_approval": "исчезновение предупреждения не означает согласования",
}


def template_text() -> str:
    return TEMPLATE.read_text(encoding="utf-8")


def prompt_digest() -> str:
    return hashlib.sha256(template_text().encode("utf-8")).hexdigest()


def boundary(packet: dict[str, Any], body: str) -> tuple[str, str]:
    seed = packet.get("packet_id") or settings.digest(packet.get("spid"))
    for salt in range(100):
        token = hashlib.sha256(f"{seed}:{salt}".encode()).hexdigest()[:12]
        if token not in body:
            return f"<<<ДАННЫЕ {token}>>>", f"<<<КОНЕЦ ДАННЫХ {token}>>>"
    raise ValueError("не удалось подобрать метку блока данных")


def cell(value: Any, limit: int = 300) -> str:
    text = "" if value is None else str(value)
    text = " / ".join(part.strip() for part in text.splitlines() if part.strip())
    if len(text) > limit:
        text = text[:limit] + " …"
    return text.replace("|", "\\|") or "—"


def box(r: list[float] | None) -> str:
    return "null" if not r else "[" + ", ".join(f"{v:.2f}" for v in r) + "]"


def _side(p: dict[str, Any], side: str) -> str:
    s = p[side.lower()]
    if s.get("page") is None:
        return f"{side}: листа нет в версии ({cell(s.get('label'))})"
    return (f"{side}: {cell(s.get('label'))}, файл «{cell(s.get('file_name'))}», стр. PDF {s['page']}, "
            f"лист по штампу {cell(s.get('sheet_no'))}, размер {box(s.get('page_size'))} pt, /Rotate {s.get('rotation')}")


def passport(p: dict[str, Any]) -> list[str]:
    eng = p["engine"]
    lines = ["## Паспорт", "",
             f"- Проект: {cell(p['project'].get('title'))} (шифр {cell(p['project'].get('code'))})",
             f"- Направление: A → B (A — прежняя версия, B — новая)",
             f"- {_side(p, 'A')}", f"- {_side(p, 'B')}",
             f"- Пара листов: {cell(p.get('title'))} (spid {p['spid']})",
             "", "## Статус движка и ограничения", "",
             f"- Статус движка: {cell(eng.get('status_label'))}",
             f"- Сопоставление листов: уверенность {cell(eng.get('match_confidence'))}"
             + (f", часть листа: {cell(eng.get('part'))}" if eng.get("part") else "")]
    if eng.get("reasons"):
        lines.append("- Причины: " + "; ".join(REASONS.get(r.split(":")[0], r) if not r.startswith("UNCHECKED_CHANNEL")
                                              else "не проверен канал «" + CHANNELS.get(r.split(":")[1], r) + "»"
                                              for r in eng["reasons"]))
    for name, ch in (eng.get("channels") or {}).items():
        state = STATES.get(ch.get("state"), ch.get("state"))
        extra = f" — {cell(ch.get('reason'))}" if ch.get("reason") else ""
        lines.append(f"- Канал «{CHANNELS.get(name, name)}»: {state}{extra}")
    if any(eng.get("truncated", {}).values()):
        lines.append("- ВНИМАНИЕ: результат движка усечён (" + ", ".join(k for k, v in eng["truncated"].items() if v)
                     + "): часть записей может отсутствовать.")
    return lines


def coordinates(p: dict[str, Any]) -> list[str]:
    c = p["coordinates"]
    return ["## Система координат", "",
            "- Рамки — pt (1/72 дюйма) видимой страницы (после /Rotate), [x0, y0, x1, y1], начало — верхний "
            "левый угол, X вправо, Y вниз.",
            "- bbox A — на странице A, bbox B — на странице B. null — стороны нет; рамка не придумывается.",
            f"- Холст наложения — система листа A, {c['canvas']['dpi']} dpi; матрица совмещения страница B → система A: "
            f"{c['transform_b_to_a']}.",
            "- Рамка вырезки (crop) шире рамки записи и задаётся отдельно; поворот изображения вырезки не меняет "
            "координат."]


def records_table(recs: list[dict[str, Any]]) -> list[str]:
    lines = ["| ID | вид | было (A) | стало (B) | bbox A | bbox B | признаки | контекст | изображения |",
             "|---|---|---|---|---|---|---|---|---|"]
    for r in recs:
        kind = f"{TYPES.get(r['type'], r['type'])}: {KINDS.get(r['kind'], r['kind'])}"
        if r.get("label"):
            kind += f" — {cell(r['label'], 120)}"
        flags = ", ".join(FLAGS.get(f, f) for f in r.get("flags") or [])
        if r.get("links"):
            flags = (flags + "; " if flags else "") + "связан с " + ", ".join(r["links"])
        lines.append(f"| {r['id']} | {cell(kind, 160)} | {cell(r['before'])} | {cell(r['after'])} | {box(r['bbox_a'])} | "
                     f"{box(r['bbox_b'])} | {cell(flags) if flags else '—'} | {', '.join(r.get('context_ids') or []) or '—'}"
                     f" | {', '.join(r.get('image_ids') or []) or 'нет'} |")
    return lines


def contexts(p: dict[str, Any], cids: list[str], xids: list[str]) -> list[str]:
    by_id = {c["id"]: c for c in p.get("contexts") or []}
    xby = {x["id"]: x for x in p.get("xrefs") or []}
    lines = ["## Контексты", ""]
    for cid in cids:
        c = by_id[cid]
        lines.append(f"### {cid} — к записи {c['record']}")
        for side in ("A", "B"):
            s = c.get(side.lower())
            if not s:
                lines.append(f"- {side}: нет")
                continue
            lines.append(f"- {side}, стр. PDF {s['page']}, рамка {box(s.get('box'))}:")
            if s.get("spec"):
                lines.append("  - позиция СО: " + "; ".join(f"{k}: {cell(v, 200)}" for k, v in s["spec"].items()))
            if s.get("row"):
                lines.append(f"  - строка: {cell(s['row'], 500)}")
            if s.get("near") and s.get("near") != s.get("row"):
                lines.append(f"  - окрестность: {cell(s['near'], 500)}")
        lines.append("")
    if xids:
        lines += ["## Межлистовой контекст (другие страницы этих же двух версий)", ""]
        for xid in xids:
            x = xby[xid]
            what = ("позиция СО: " + "; ".join(f"{k}: {cell(v, 200)}" for k, v in (x.get("fields") or {}).items())
                    if x.get("fields") else f"строка: {cell(x.get('text'), 400)}")
            lines.append(f"- {xid} — версия {x['side']}, стр. PDF {x['page']} ({cell(x.get('page_label'))}), рамка "
                         f"{box(x.get('bbox'))}; найдено: {cell(x['method'])}; к записям {', '.join(x.get('for_ids') or [])}. "
                         f"{what}")
        lines.append("")
    return lines


def images_list(p: dict[str, Any], iids: list[str]) -> list[str]:
    by_id = {i["id"]: i for i in p.get("images") or []}
    lines = ["## Изображения (приложены к сообщению в этом порядке)", ""]
    kinds = {"AB": "вырезка A (сверху) и B (снизу) из исходных PDF, без пометок",
             "OV": "наложение движка с рамками записей (серое — общее, красное — только A, синее — только B)",
             "SHEET": "весь лист (наложение, уменьшено) с рамкой этой части"}
    for n, iid in enumerate(iids, 1):
        im = by_id[iid]
        rot = f", изображение повёрнуто на {im['image_rotation_deg']}° по часовой для чтения" if im.get(
            "image_rotation_deg") else ""
        recs = f"; записи {', '.join(im.get('record_ids') or [])}" if im.get("record_ids") else ""
        lines.append(f"{n}. {iid} — {kinds.get(im['kind'], im['kind'])}; crop A {box(im.get('crop_a'))}, crop B "
                     f"{box(im.get('crop_b'))}, {im.get('dpi')} dpi{rot}{recs}")
    return lines


def program_section(recs: list[dict[str, Any]]) -> list[str]:
    groups: dict[str, list[str]] = {}
    for r in recs:
        if r["group"] != "ai":
            groups.setdefault(r["fate"], []).append(r["id"])
    lines = ["## Учтено программой (не разбирать)", ""]
    if not groups:
        lines.append("- нет")
    for fate, idl in groups.items():
        shown = ", ".join(idl[:60]) + (f" … и ещё {len(idl) - 60}" if len(idl) > 60 else "")
        lines.append(f"- {FATE_LABELS.get(fate, fate)}: {len(idl)} — {shown}")
    return lines


def data_md(p: dict[str, Any], chunk: dict[str, Any] | None) -> str:
    recs = p["records"]
    if chunk is None:
        ai = [r for r in recs if r["group"] == "ai"]
        iids = [i["id"] for i in p.get("images") or []]
    else:
        want = set(chunk["record_ids"])
        ai = [r for r in recs if r["id"] in want]
        iids = chunk["image_ids"]
    cids = [c for r in ai for c in r.get("context_ids") or [] if c.startswith("C")]
    xids = sorted({x for r in ai for x in r.get("context_ids") or [] if x.startswith("X")},
                  key=lambda v: int(v[1:]))
    lines = [f"# Пара листов {p['spid']}: {cell(p.get('title'))}", ""]
    lines += passport(p) + [""] + coordinates(p) + [""] + images_list(p, iids) + [""]
    lines += ["## Записи для разбора", ""]
    lines += records_table(ai) if ai else ["- нет"]
    lines += [""] + contexts(p, cids, xids)
    if chunk is None or chunk.get("index") == 1:
        lines += program_section(recs) + [""]
    if chunk is not None and p.get("chunks") and len(p["chunks"]) > 1:
        others = [c for c in p["chunks"] if c["chunk_id"] != chunk["chunk_id"]]
        lines += [f"## Другие части листа ({len(others)}) — их записи здесь не разбирать", ""]
        for c in others:
            ids_ = c["record_ids"]
            lines.append(f"- {c['chunk_id']}: {len(ids_)} записей ({', '.join(ids_[:12])}{' …' if len(ids_) > 12 else ''})")
        lines.append("")
    return "\n".join(lines)


def render_chunk(p: dict[str, Any], chunk: dict[str, Any]) -> str:
    body = data_md(p, chunk)
    begin, end = boundary(p, body)
    instr = template_text().replace("{{BEGIN}}", begin).replace("{{END}}", end)
    task = (f"ЗАДАНИЕ\n\npacket_id: {p['packet_id']}\nchunk_id: {chunk['chunk_id']}\n"
            f"Часть {chunk['index']} из {len(p['chunks'])}. Записей для разбора: {len(chunk['record_ids'])}. "
            f"Изображений: {len(chunk['image_ids'])}.\n")
    return f"{instr}\n{task}\n{begin}\n{body}\n{end}\n"


def render_sheet(p: dict[str, Any]) -> str:
    """Пакет листа целиком — для просмотра человеком (в модель уходят части)."""
    head = ["<!-- Пакет ИИ-описания пары листов. Сформирован программой из результатов движка. -->", ""]
    if not p.get("ai_needed"):
        head += [f"> ИИ не требуется: {cell(p.get('engine_only_reason'))}", ""]
    chunks = p.get("chunks") or []
    if chunks:
        head += [f"Частей для модели: {len(chunks)} — " + ", ".join(
            f"{c['chunk_id']} ({len(c['record_ids'])} записей, {len(c['image_ids'])} изобр.)" for c in chunks), ""]
    return "\n".join(head) + data_md(p, None)
