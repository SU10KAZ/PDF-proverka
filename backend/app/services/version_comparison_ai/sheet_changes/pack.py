"""Пакет листа «Изменения в листе»: кандидаты → изображения, текст для модели, манифест.

Одно обращение на лист. Изображения: обзор листа B с рамками кандидатов; для мест на
чертеже — общие вырезки соседних мест (A, B и наложение в одном файле); для таблиц —
вырезка таблицы A и B, если без картинки вывод не проверить (числа с единицами в
изменённом тексте, графика в строке или таблица целиком). Чисто текстовые изменения
строк уходят без изображений. Координаты в текст модели не пишутся.
"""
from __future__ import annotations

import hashlib
import json
import re
import statistics
from pathlib import Path
from typing import Any

import numpy as np

from backend.app.services.version_comparison.status import STATUS_LABELS

from .. import context as ctx
from .. import geometry as geo
from .. import images as img
from .. import packet as old_packet
from .. import prompt as old_prompt
from .. import settings
from . import collect as col_mod
from . import schema as sc_schema
from . import tables as tb

SCHEMA = "version_comparison_ai.sheet_changes_packet/v1"
TEMPLATE = Path(__file__).resolve().parent.parent / "prompts" / "sheet_changes.md"
GROUP_MAX_PT = 520.0
IMAGE_MAX_PX = 1950
TEXT_PX = 14.0
DPI_RANGE = (150.0, 300.0)
XREF_LIMIT = 2
POINTERS_SHOWN = 30


def template_digest() -> str:
    return hashlib.sha256(TEMPLATE.read_bytes()).hexdigest()


def _sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _hstack(parts: list[np.ndarray], gap: int = 8) -> np.ndarray:
    h = max(p.shape[0] for p in parts)
    cols = []
    for k, p in enumerate(parts):
        if p.shape[0] < h:
            p = np.vstack([p, img.blank(p.shape[1], h - p.shape[0])])
        cols.append(p)
        if k < len(parts) - 1:
            cols.append(np.full((h, gap, 3), 120, np.uint8))
    return np.hstack(cols)


def _resize(a: np.ndarray, f: float) -> np.ndarray:
    import cv2
    return cv2.resize(a, (max(1, int(a.shape[1] * f)), max(1, int(a.shape[0] * f))), interpolation=cv2.INTER_AREA)


def _text_dpi(words: list[list[Any]], box: list[float]) -> float:
    hs = [min(w[2] - w[0], w[3] - w[1]) for w in words if geo.intersects(box, [float(v) for v in w[:4]])]
    hs = [h for h in hs if 1.0 < h < 40]
    dpi = TEXT_PX * 72.0 / statistics.median(hs) if hs else 200.0
    return max(DPI_RANGE[0], min(DPI_RANGE[1], dpi))


def _place_groups(cands: list[dict]) -> list[dict]:
    groups: list[dict] = []
    for c in cands:
        if c["kind"] not in ("place", "shift") or not c.get("context_a"):
            continue
        box = c["context_a"]
        for g in groups:
            u = geo.union([g["box"], box])
            if max(u[2] - u[0], u[3] - u[1]) <= GROUP_MAX_PT:
                g["box"], g["ids"] = u, g["ids"] + [c["id"]]
                break
        else:
            groups.append({"box": list(box), "ids": [c["id"]]})
    return groups


def _needs_table_image(c: dict, ptr: dict) -> bool:
    if c["full"]:
        return True
    for pid in c["pointers"]:
        p = ptr[pid]
        if p["type"] in ("region", "block"):
            return True
        if col_mod.UNIT_NUMBER.search(f"{p['before']} {p['after']}"):
            return True
    return False


class Builder:
    def __init__(self, col: dict, out_dir: Path):
        self.col, self.sh = col, col["sheet"]
        self.out = Path(out_dir)
        self.images: list[dict] = []
        self.labels = {s: (self.sh.run[s.lower()].get("label") or s) for s in ("A", "B")}

    def _save(self, arr: np.ndarray, kind: str, meta: dict) -> dict:
        iid = f"I{len(self.images) + 1}"
        f = self.out / "images" / f"{iid}.png"
        sha = img.save_png(arr, f)
        rec = {"id": iid, "kind": kind, "file": f"images/{iid}.png", "sha256": sha,
               "size_px": [int(arr.shape[1]), int(arr.shape[0])], "bytes": f.stat().st_size, **meta}
        self.images.append(rec)
        return rec

    def overview(self, cands: list[dict]) -> dict | None:
        sh = self.sh
        pg = sh.docs["B"][sh.page["B"] - 1]
        w, h = pg.rect.width, pg.rect.height
        dpi = IMAGE_MAX_PX * 72.0 / max(w, h)
        arr, _ = img.render_crop(sh.docs["B"], sh.page["B"], [0, 0, w, h], dpi)
        if arr is None:
            return None
        s = dpi / 72.0
        boxes = []
        for c in cands:
            if c.get("area"):
                b = sh.b_box(geo.expand(c["area"], 4.0))
                boxes.append((c["id"], [v * s for v in b]))
        arr = img.draw_boxes(arr, boxes)
        arr = img.caption(arr, f"Обзор: лист B целиком ({self.labels['B']}), рамки — кандидаты O…; уменьшено")
        return self._save(arr, "overview", {"candidates": [c["id"] for c in cands if c.get("area")], "dpi": round(dpi, 1)})

    def place_image(self, g: dict, by_id: dict) -> dict:
        sh = self.sh
        box_a = [round(v, 1) for v in g["box"]]
        box_b = sh.b_box(box_a)
        w, h = box_a[2] - box_a[0], box_a[3] - box_a[1]
        long_pt, short_pt = max(w, h), min(w, h)
        dpi = min(_text_dpi(sh.words["A"], box_a), IMAGE_MAX_PX * 72.0 / long_pt,
                  IMAGE_MAX_PX * 72.0 / (2.6 * short_pt + 40))
        a, used_a = img.render_crop(sh.docs["A"], sh.page["A"], box_a, dpi)
        b, used_b = img.render_crop(sh.docs["B"], sh.page["B"], box_b, dpi)
        a = a if a is not None else img.blank(int(w * dpi / 72), 40)
        b = b if b is not None else img.blank(int(w * dpi / 72), 40)
        parts = [img.caption(a, f"A · {self.labels['A']} · стр. PDF {sh.page['A']}"),
                 img.caption(b, f"B · {self.labels['B']} · стр. PDF {sh.page['B']}")]
        ov, used = img.overlay_crop(sh.rdir, sh.result["canvas"], box_a, dpi)
        if ov is not None:
            s = dpi / 72.0
            marks = []
            for cid in g["ids"]:
                ar = by_id[cid].get("area")
                if ar:
                    marks.append((cid, [(ar[0] - used[0]) * s, (ar[1] - used[1]) * s,
                                        (ar[2] - used[0]) * s, (ar[3] - used[1]) * s]))
            ov = img.draw_boxes(ov, marks)
            ov = _resize(ov, 0.6)
            parts.append(img.caption(ov, "Наложение: серое — общее, красное — только A, синее — только B; рамки — O…"))
        arr = img.stack(parts) if w >= h else _hstack(parts)
        if max(arr.shape[:2]) > IMAGE_MAX_PX * 1.05:
            arr = _resize(arr, IMAGE_MAX_PX / max(arr.shape[:2]))
        return self._save(arr, "place", {"candidates": list(g["ids"]), "crop_a": geo.rounded(used_a),
                                         "crop_b": geo.rounded(used_b), "dpi": round(dpi, 1)})

    def table_image(self, c: dict) -> dict | None:
        sh = self.sh
        parts, crops = [], {}
        for side, key in (("A", "region_a"), ("B", "region_b")):
            r = c.get(key)
            if not r:
                continue
            # Таблица целиком — одной вырезкой; иначе — заголовок и затронутые строки, склеенные подряд.
            boxes = [geo.expand(r, 4.0)] if c["full"] else [geo.expand(b, 2.0) for b in
                                                             ([c["header_box"]] if c.get("header_box") else [])
                                                             + [row["box"] for row in c["rows_" + side.lower()]]]
            width = max(b[2] - b[0] for b in boxes)
            height = sum(b[3] - b[1] for b in boxes)
            dpi = min(_text_dpi(sh.words[side], geo.union(boxes)), IMAGE_MAX_PX * 72.0 / max(width, 1),
                      IMAGE_MAX_PX * 72.0 / (2.1 * height + 40))
            pieces, used_all = [], []
            for b in boxes:
                arr, used = img.render_crop(sh.docs[side], sh.page[side], b, dpi)
                if arr is not None:
                    pieces.append(arr)
                    used_all.append(geo.rounded(used))
            if pieces:
                arr = img.stack(pieces, gap=3) if len(pieces) > 1 else pieces[0]
                what = "таблица" if c["full"] else "заголовок и строки таблицы"
                parts.append(img.caption(arr, f"{side} · {self.labels[side]} · стр. PDF {sh.page[side]} · {what}"))
                crops[side] = used_all
        if not parts:
            return None
        arr = img.stack(parts)
        if max(arr.shape[:2]) > IMAGE_MAX_PX * 1.05:
            arr = _resize(arr, IMAGE_MAX_PX / max(arr.shape[:2]))
        return self._save(arr, "table", {"candidates": [c["id"]], "crop_a": crops.get("A"), "crop_b": crops.get("B")})


def _xrefs(col: dict, cands: list[dict]) -> list[dict]:
    """Строки других листов этих же двух версий с марками кандидатов (только текст)."""
    sh = col["sheet"]
    want: dict[str, list[str]] = {}
    for c in cands:
        for m in c.get("marks") or []:
            want.setdefault(m, []).append(c["id"])
    if not want:
        return []
    marks = []
    for c in cands:
        texts = list(c.get("labels_a") or []) + list(c.get("labels_b") or [])
        texts += [r["key"] for r in (c.get("rows_a") or []) + (c.get("rows_b") or []) if r.get("key")]
        for t in texts:
            for m in re.findall(r"[A-Za-zА-Яа-яЁё]{1,4}-[0-9][0-9A-Za-zА-Яа-яЁё*/.]*", t or ""):
                if m not in marks:
                    marks.append(m)
    hits = ctx.xrefs(sh.pages, marks, {"A": sh.page["A"], "B": sh.page["B"]}, XREF_LIMIT)
    rul_cache: dict[tuple[str, int], tb.Rulings] = {}
    out = []
    for n, h in enumerate(hits, 1):
        side, page = h["side"], h["page"]
        if (side, page) not in rul_cache:
            rul_cache[(side, page)] = tb.Rulings(sh.docs[side][page - 1])
        words = sh.pages.words(side, page)
        row = tb.row_at(rul_cache[(side, page)], words, h["bbox"])
        hdr = tb.header_for(rul_cache[(side, page)], words, row) if row else None
        text = (f"{hdr['text']} || {row['text']}" if hdr else row["text"]) if row else (h.get("text") or "")
        out.append({"id": f"X{n}", "side": side, "page": page, "page_label": ctx.page_label(sh.pages, side, page),
                    "mark": h["mark"], "text": text[:900], "bbox": row["box"] if row else h["bbox"],
                    "for": want.get(ctx.norm_mark(h["mark"]), [])})
    return out


def _engine_notes(sh: col_mod.Sheet) -> list[str]:
    res = sh.result
    notes = []
    for r in res.get("reasons") or []:
        if r.startswith("UNCHECKED_CHANNEL"):
            ch = r.split(":")[1]
            notes.append(f"не проверен канал «{old_prompt.CHANNELS.get(ch, ch)}»")
        else:
            notes.append(old_prompt.REASONS.get(r.split(":")[0], r))
    return notes


# ------------------------------------------------------------------ текст для модели

def _cell(v: Any, n: int = 900) -> str:
    return old_prompt.cell(v, n)


def render_data(p: dict) -> str:
    a, b = p["a"], p["b"]
    L = [f"# Лист «{_cell(p['title'], 160)}» · A стр. PDF {a['page']} → B стр. PDF {b['page']}", "",
         f"Проект: {_cell(p['project'].get('title'))} (шифр {_cell(p['project'].get('code'))}). "
         f"A — {_cell(a['label'])}, файл «{_cell(a['file_name'])}». B — {_cell(b['label'])}, файл «{_cell(b['file_name'])}». "
         f"Лист по штампу: {_cell(b.get('sheet_no') or a.get('sheet_no'))}.",
         f"Статус сравнения: {_cell(p['engine']['status_label'])}"
         + (f"; ограничения: {'; '.join(p['engine']['notes'])}" if p["engine"]["notes"] else "") + ".", "",
         "## Изображения (приложены в этом порядке)", ""]
    for k, im in enumerate(p["images"], 1):
        what = {"overview": "обзор листа B целиком, рамки кандидатов",
                "place": "сверху/слева A, затем B, затем наложение с рамками кандидатов",
                "table": "таблица: сверху A, ниже B"}[im["kind"]]
        L.append(f"{k}. {im['id']} — {what}; кандидаты: {', '.join(im['candidates'])}")
    L += ["", "## Кандидаты", ""]
    ptr = p["pointers"]
    xby = {}
    for x in p["xrefs"]:
        for cid in x["for"]:
            xby.setdefault(cid, []).append(x)
    for c in p["candidates"]:
        ims = [im["id"] for im in p["images"] if c["id"] in im["candidates"] and im["kind"] != "overview"]
        if c["kind"] == "table":
            L.append(f"### {c['id']} — таблица «{_cell(c['title'] or 'без названия', 200)}» · строк A: {len(c['rows_a'])}, "
                     f"B: {len(c['rows_b'])} · {'таблица целиком' if c['full'] else 'только затронутые строки и строки с теми же обозначениями'}")
            if c["header"]:
                L.append(f"Заголовок: {_cell(c['header'], 600)}")
            for side, key in (("A", "rows_a"), ("B", "rows_b")):
                L.append(f"Строки {side}:")
                L += [f"- {_cell(r['text'], 1500)}" for r in c[key]] or ["- нет"]
            pairs = ", ".join(x["key"] for x in c["pairs"]) or "нет"
            L.append(f"Предварительно по обозначению (может быть неверным: марка и место строки могут меняться): "
                     f"есть в A и B — {pairs}; только в A — {', '.join(c['unpaired_a']) or 'нет'}; "
                     f"только в B — {', '.join(c['unpaired_b']) or 'нет'}.")
        else:
            head = "место на чертеже" if c["kind"] == "place" else ("сдвиг элемента" if c["kind"] == "shift" else "запись")
            extra = []
            if c.get("view"):
                extra.append(f"вид: {_cell(c['view'], 120)}")
            if c.get("anchors"):
                extra.append("рядом: " + ", ".join(c["anchors"]))
            L.append(f"### {c['id']} — {head}" + (" · " + " · ".join(extra) if extra else ""))
            if c.get("labels_a") or c.get("labels_b"):
                L.append(f"Изменённые надписи: A — {', '.join('«' + _cell(t, 120) + '»' for t in c['labels_a']) or 'нет'}; "
                         f"B — {', '.join('«' + _cell(t, 120) + '»' for t in c['labels_b']) or 'нет'}.")
            if c.get("words_a") is not None:
                L.append(f"Слова в области A: {_cell(c.get('words_a'), 1500)}")
                L.append(f"Слова в области B: {_cell(c.get('words_b'), 1500)}")
        shown = c["pointers"][:POINTERS_SHOWN]
        desc = []
        for pid in shown:
            q = ptr[pid]
            ba = ""
            if q["before"] or q["after"]:
                ba = f" «{_cell(q['before'], 120) or '—'}» → «{_cell(q['after'], 120) or '—'}»"
            desc.append(f"{pid} {q['what']}{ba}")
        more = len(c["pointers"]) - len(shown)
        L.append("Указатели движка (позиционные; не доказательство замены изделия): " + "; ".join(desc)
                 + (f"; и ещё {more}" if more > 0 else "") + ".")
        if c.get("attached"):
            L.append(f"Внутри этого места ещё {len(c['attached'])} мелких указателей (сдвиги надписей, толщина линий, "
                     "сдвиги элементов) — смотри на изображении.")
        if c.get("links"):
            L.append(f"Те же обозначения на этом листе: {', '.join(c['links'])}.")
        if xby.get(c["id"]):
            L.append("Те же обозначения на других листах этих же версий: " + ", ".join(x["id"] for x in xby[c["id"]]) + ".")
        L.append(f"Изображения: {', '.join(ims) if ims else 'нет — изменение текстовое; при необходимости zoom'}.")
        L.append("")
    if p["xrefs"]:
        L += ["## Другие листы этих же двух версий (найдено по обозначениям, только текст)", ""]
        for x in p["xrefs"]:
            L.append(f"- {x['id']} — {x['side']}, стр. PDF {x['page']}"
                     f"{' (' + _cell(x['page_label'], 100) + ')' if x['page_label'] else ''}, «{_cell(x['mark'])}»: "
                     f"{_cell(x['text'], 900)}")
        L.append("")
    return "\n".join(L)


def render_prompt(p: dict) -> str:
    body = render_data(p)
    begin, end = old_prompt.boundary(p, body)
    instr = TEMPLATE.read_text(encoding="utf-8").replace("{{BEGIN}}", begin).replace("{{END}}", end)
    task = (f"ЗАДАНИЕ\n\npacket_id: {p['packet_id']}\npart_id: {p['part_id']}\n"
            f"Кандидатов: {len(p['candidates'])}. Изображений: {len(p['images'])}.\n")
    return f"{instr}\n{task}\n{begin}\n{body}\n{end}\n"


# ------------------------------------------------------------------ сборка

def build(object_id: str, run_id: str, spid: str, out_dir: Path) -> dict[str, Any]:
    out = Path(out_dir)
    if out.exists() and any(out.iterdir()):
        if not (out / "packet.json").exists():
            raise ValueError(f"папка {out} не пуста и не является пакетом — не перезаписываю")
        for f in sorted(out.rglob("*"), reverse=True):
            f.unlink() if f.is_file() else f.rmdir()
    out.mkdir(parents=True, exist_ok=True)
    col = col_mod.collect(object_id, run_id, spid)
    sh, cands = col["sheet"], col["candidates"]
    by_id = {c["id"]: c for c in cands}
    bld = Builder(col, out)
    bld.overview(cands)
    for c in cands:
        if c["kind"] == "table" and _needs_table_image(c, col["pointers"]):
            bld.table_image(c)
    for g in _place_groups(cands):
        bld.place_image(g, by_id)
    status = sh.result.get("status")
    p: dict[str, Any] = {
        "schema": SCHEMA, "method": settings.sc_versions() | {"template_digest": template_digest(),
                                                             "schema_digest": sc_schema.schema_digest()},
        "packet_id": None, "part_id": "P1", "object_id": object_id, "run_id": run_id, "spid": spid,
        "project": old_packet._project(object_id, sh.run["project_id"]), "title": sh.item.get("title"),
        "a": old_packet._side_info(sh.run, sh.item, sh.result, sh.pages, "A"),
        "b": old_packet._side_info(sh.run, sh.item, sh.result, sh.pages, "B"),
        "engine": {"algo": sh.result.get("algo"), "status": status, "status_label": STATUS_LABELS.get(status, status),
                   "result_sha256": old_packet._sha_file(sh.rdir / "result.json"), "notes": _engine_notes(sh),
                   "truncated": col["truncated"]},
        "candidates": cands, "pointers": col["pointers"], "ledger": col["ledger"], "counts": col["counts"],
        "xrefs": _xrefs(col, cands), "images": bld.images, "izm_b": col["izm_b"],
        "transform": {"matrix": sh.tr.get("matrix")}, "canvas": sh.result.get("canvas"),
    }
    p["packet_id"] = settings.digest({k: v for k, v in p.items() if k not in ("packet_id", "run_id", "ledger")})[:16]
    prompt_text = render_prompt(p)
    (out / "prompt.md").write_text(prompt_text, encoding="utf-8")
    data_text = render_data(p)
    p["budget"] = {"prompt_chars": len(prompt_text), "prompt_bytes": len(prompt_text.encode("utf-8")),
                   "data_chars": len(data_text), "images": len(bld.images),
                   "image_bytes": sum(i["bytes"] for i in bld.images),
                   "image_px": [i["size_px"] for i in bld.images]}
    p["provenance"] = {
        "inputs": [f"PDF A sha256 {sh.run['a']['sha256']}", f"PDF B sha256 {sh.run['b']['sha256']}",
                   f"профили версий: {paths_rel(object_id, sh.run['a']['sha256'])}, {paths_rel(object_id, sh.run['b']['sha256'])}",
                   f"результат движка: result.json sha256 {p['engine']['result_sha256']}"],
        "not_inputs": "эталонные разборы, ответы прежних заданий и контрольные перечни во вход не включаются",
    }
    (out / "packet.json").write_text(json.dumps(p, ensure_ascii=False, indent=1), encoding="utf-8")
    files = {str(f.relative_to(out)): {"sha256": _sha(f.read_bytes()), "size": f.stat().st_size}
             for f in sorted(out.rglob("*")) if f.is_file() and f.name != "manifest.json"}
    (out / "manifest.json").write_text(json.dumps({"schema": "version_comparison_ai.sheet_changes_manifest/v1",
                                                   "packet_id": p["packet_id"], "method": p["method"],
                                                   "provenance": p["provenance"], "files": files},
                                                  ensure_ascii=False, indent=1), encoding="utf-8")
    return p


def paths_rel(object_id: str, sha: str) -> str:
    return f"profiles/{sha[:12]}…"
