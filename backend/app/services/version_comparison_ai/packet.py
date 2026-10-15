"""Сборщик пакета пары листов: результат движка → packet.json + MD + изображения.

Читает только сохранённые артефакты прогона через ``jobs.get_run`` /
``jobs.sheet_result`` (метки версий и статус листа СО по позициям подставляет
прогон, а не кэшированный ``result.json``), профили страниц и исходные PDF.
Ничего не пересчитывает и ничего не пишет в папки движка.
"""
from __future__ import annotations

import hashlib
import json
import statistics
from pathlib import Path
from typing import Any

import cv2
import numpy as np

from backend.app.services.version_comparison import jobs, paths, pdfio, store
from backend.app.services.version_comparison.status import STATUS_LABELS

from . import context as ctx
from . import geometry as geo
from . import ids, images, prompt, records, settings
from .images import caption

SCHEMA = "version_comparison_ai.sheet_packet/v1"
MAX_CLUSTER_RECORDS = 15
MAX_MARKS = 60


def _sha_file(path: Path) -> str | None:
    if not path.exists():
        return None
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _side_info(run: dict, item: dict, result: dict | None, pages: ctx.Pages, side: str) -> dict[str, Any]:
    key = side.lower()
    page = item.get(f"{key}_page")
    rec = pages.page_rec(side, page) if page else None
    res_side = (result or {}).get(key) or {}
    return {"version_id": run[key]["version_id"], "label": run[key].get("label"), "file_name": run[key].get("file_name"),
            "sha256": run[key]["sha256"], "page": page,
            "sheet_no": res_side.get("sheet_no") or ((rec or {}).get("stamp") or {}).get("sheet_no"),
            "title": res_side.get("title") or ((rec or {}).get("stamp") or {}).get("title"),
            "page_size": geo.rounded([rec["width"], rec["height"]]) if rec else None,
            "rotation": rec.get("rotation") if rec else None, "scan": bool((rec or {}).get("scan"))}


def _project(object_id: str, project_id: str) -> dict[str, Any]:
    try:
        p = store.project(store.load_catalog(object_id), project_id)
        return {"title": p.get("title"), "code": p.get("code")}
    except store.CatalogError:
        return {"title": None, "code": None}


# ------------------------------------------------------------------ контекст

def _contexts(recs: list[dict], item: dict, pages: ctx.Pages, tr: dict) -> list[dict[str, Any]]:
    out = []
    spec_page = {side: (pages.page_rec(side, item.get(f"{side.lower()}_page")) or {}).get("kind") == "spec"
                 for side in ("A", "B")}
    for r in recs:
        if r["group"] != "ai" or r["type"] in ("overlay",) or not r["box"]:
            continue
        sides = {}
        for side in ("A", "B"):
            page = item.get(f"{side.lower()}_page")
            if not page:
                continue
            own = r["bbox_a"] if side == "A" else r["bbox_b"]
            box = own or (r["box"] if side == "A" else geo.a_to_b_page(tr, r["box"]))
            words = pages.words(side, page)
            entry: dict[str, Any] = {"page": page, "box": geo.rounded(box), "own_box": own is not None}
            if r["type"] in ("service", "zone"):
                entry["near"] = ctx.row_and_near(words, box)["row"] if box else ""
            else:
                rn = ctx.row_and_near(words, box, r["before"] if side == "A" else r["after"])
                entry["row"] = rn["row"]
                if r["type"] not in ("text", "spec"):
                    entry["near"] = rn["near"]
            if spec_page[side] and r["type"] in ("text", "spec", "region"):
                fields = ctx.spec_fields(ctx.spec_item_at(pages, side, page, box))
                if fields:
                    entry["spec"] = fields
            sides[side.lower()] = entry
        if sides:
            cid = ids.local("context", str(len(out) + 1))
            out.append({"id": cid, "record": r["id"], **sides})
            r["context_ids"] = [cid]
    return out


def _xrefs(recs: list[dict], contexts: list[dict], item: dict, pages: ctx.Pages) -> list[dict[str, Any]]:
    limit = settings.xref_hits()
    if not limit:
        return []
    by_rec = {c["record"]: c for c in contexts}
    found: dict[str, list[str]] = {}
    order: list[str] = []

    def add(token: str, rid: str) -> None:
        key = ctx.norm_mark(token)
        if key not in found:
            found[key] = []
            order.append(token)
        if rid not in found[key]:
            found[key].append(rid)

    ai = [r for r in recs if r["group"] == "ai"]
    per_rec: list[list[str]] = []
    for r in ai:
        own = ctx.marks(f"{r['before']} {r['after']} {r.get('label') or ''}")
        c = by_rec.get(r["id"]) or {}
        near = []
        for side in ("a", "b"):    # марки окрестности: обозначение щита рядом с изменённой маркой
            s = c.get(side) or {}
            near += ctx.marks(f"{s.get('row') or ''} {s.get('near') or ''}")
        per_rec.append([(t, r["id"]) for t in own + near])
    # По очереди от каждой записи: общий предел не съедается одной длинной строкой.
    depth = max((len(x) for x in per_rec), default=0)
    for k in range(depth):
        for lst in per_rec:
            if k < len(lst):
                add(*lst[k])
    hits = ctx.xrefs(pages, order[:MAX_MARKS], {"A": item.get("a_page"), "B": item.get("b_page")}, limit)
    rec_by_id = {r["id"]: r for r in recs}
    for h in hits:
        h["for_ids"] = found.get(ctx.norm_mark(h["mark"]), [])
        h["page_label"] = ctx.page_label(pages, h["side"], h["page"])
        for rid in h["for_ids"]:
            rec_by_id[rid].setdefault("context_ids", []).append(h["id"])
    return hits


# ------------------------------------------------------------------ вырезки

def _clusters(recs: list[dict]) -> list[dict[str, Any]]:
    gap = settings.cluster_gap_pt()
    lo, _hi = settings.dpi_range()
    max_w = settings.max_image_side() * 72.0 / lo - 2 * settings.crop_margin_pt()
    max_h = max_w / 2
    items = sorted((r for r in recs if r["group"] == "ai" and r["box"] and r["type"] != "overlay"),
                   key=lambda r: (round(r["box"][1] / 20.0), r["box"][0]))
    clusters: list[dict[str, Any]] = []
    for r in items:
        placed = False
        for c in clusters:
            if len(c["records"]) >= MAX_CLUSTER_RECORDS or not geo.intersects(geo.expand(c["box"], gap), r["box"]):
                continue
            merged = geo.union([c["box"], r["box"]])
            # Строка СО шире обычного предела: предел — не меньше самой широкой записи.
            wide = max(max_w, 1.1 * max(c["widest"], r["box"][2] - r["box"][0]))
            if merged[2] - merged[0] <= wide and merged[3] - merged[1] <= max_h:
                c["widest"] = max(c["widest"], r["box"][2] - r["box"][0])
                c["box"], placed = merged, True
                c["records"].append(r["id"])
                break
        if not placed:
            clusters.append({"box": list(r["box"]), "records": [r["id"]], "widest": r["box"][2] - r["box"][0]})
    # Досклейка: порядок обхода не должен дробить одну область на несколько вырезок.
    merged_any = True
    while merged_any:
        merged_any = False
        for i in range(len(clusters)):
            for j in range(i + 1, len(clusters)):
                a, b = clusters[i], clusters[j]
                if len(a["records"]) + len(b["records"]) > MAX_CLUSTER_RECORDS:
                    continue
                if not geo.intersects(geo.expand(a["box"], gap), b["box"]):
                    continue
                box = geo.union([a["box"], b["box"]])
                wide = max(max_w, 1.1 * max(a["widest"], b["widest"]))
                if box[2] - box[0] <= wide and box[3] - box[1] <= max_h:
                    a.update(box=box, records=a["records"] + b["records"], widest=max(a["widest"], b["widest"]))
                    del clusters[j]
                    merged_any = True
                    break
            if merged_any:
                break
    return clusters


def _text_dirs(result: dict, boxes: list[list[float]]) -> int:
    """Поворот изображения для чтения: по направлению слов движка в рамках записей."""
    t = result.get("text") or {}
    dirs = []
    for w in (t.get("only_a") or []) + (t.get("only_b") or []):
        if w.get("dir") and any(geo.intersects(b, w["pt"]) for b in boxes):
            dirs.append(w["dir"])
    for mv in t.get("moved") or []:
        if mv.get("dir") and any(geo.intersects(b, mv["a_pt"]) for b in boxes):
            dirs.append(mv["dir"])
    if not dirs:
        return 0
    up, down = dirs.count("up"), dirs.count("down")
    if up * 2 > len(dirs):
        return 90
    if down * 2 > len(dirs):
        return 270
    return 0


def _cluster_dpi(recs: list[dict], box: list[float]) -> int:
    lo, hi = settings.dpi_range()
    heights = []
    for r in recs:
        if r["type"] != "text":
            continue
        for b in (r["bbox_a"], r["bbox_b"]):
            if b:
                heights.append(max(1.0, min(b[2] - b[0], b[3] - b[1])))
    dpi = settings.text_px() * 72.0 / statistics.median(heights) if heights else 200.0
    dpi = max(lo, min(hi, dpi))
    w, h = box[2] - box[0], box[3] - box[1]
    fit = settings.max_image_side() * 72.0 / max(w, 2 * h + 60, 1.0)
    return int(max(36, min(dpi, fit)))


def _render_cluster(n: int, cl: dict, recs_by_id: dict, docs: dict, item: dict, run: dict, result: dict,
                    rdir: Path, out_dir: Path, tr: dict) -> list[dict[str, Any]]:
    recs = [recs_by_id[i] for i in cl["records"]]
    margin = settings.crop_margin_pt()
    crop_a = geo.expand(cl["box"], margin)
    crop_b = geo.a_to_b_page(tr, crop_a)
    dpi = _cluster_dpi(recs, crop_a)
    rot = _text_dirs(result, [r["box"] for r in recs])
    img_a, used_a = images.render_crop(docs["A"], item["a_page"], crop_a, dpi)
    img_b, used_b = images.render_crop(docs["B"], item["b_page"], crop_b, dpi)
    parts = []
    for side, img, used in (("A", img_a, used_a), ("B", img_b, used_b)):
        lab = run[side.lower()].get("label") or side
        page = item[f"{side.lower()}_page"]
        if img is None:
            img = images.blank(int((crop_a[2] - crop_a[0]) * dpi / 72), 40)
        parts.append(caption(images.rotate(img, rot), f"{side} · {lab} · стр. PDF {page}"))
    out = []
    iid_ab = ids.image(n)
    sha = images.save_png(images.stack(parts), out_dir / "images" / f"{iid_ab}.png")
    out.append({"id": iid_ab, "kind": "AB", "file": f"images/{iid_ab}.png", "sha256": sha,
                "crop_a": geo.rounded(used_a), "crop_b": geo.rounded(used_b), "dpi": dpi, "image_rotation_deg": rot,
                "record_ids": list(cl["records"])})
    ov, used = images.overlay_crop(rdir, result["canvas"], crop_a, dpi)
    if ov is not None:
        s = dpi / 72.0
        boxes = []
        for r in recs:
            b = [(r["box"][0] - used[0]) * s, (r["box"][1] - used[1]) * s,
                 (r["box"][2] - used[0]) * s, (r["box"][3] - used[1]) * s]
            boxes.append((r["id"], images.rotate_box(b, ov.shape[1], ov.shape[0], rot)))
        ov = images.draw_boxes(images.rotate(ov, rot), boxes)
        iid_ov = ids.image(n + 1)
        sha = images.save_png(caption(ov, "Наложение A/B (серое — общее, красное — только A, синее — только B)"),
                              out_dir / "images" / f"{iid_ov}.png")
        out.append({"id": iid_ov, "kind": "OV", "file": f"images/{iid_ov}.png", "sha256": sha,
                    "crop_a": geo.rounded(used), "crop_b": None, "dpi": dpi, "image_rotation_deg": rot,
                    "record_ids": list(cl["records"])})
    return out


def _sheet_image(n: int, rdir: Path, result: dict, boxes: list[list[float]], out_dir: Path) -> dict | None:
    prev_file = rdir / ((result.get("preview") or {}).get("file") or "preview.png")
    img = cv2.imread(str(prev_file))
    if img is None:
        return None
    img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
    canvas = result["canvas"]
    f = img.shape[1] / float(canvas["width_px"])
    marks = []
    for k, b in enumerate(boxes, 1):
        px = [v * f for v in geo.to_px(canvas, b)]
        marks.append((f"часть {k}" if len(boxes) > 1 else "эта часть", px))
    img = images.draw_boxes(img, marks)
    scale = min(1.0, 1400.0 / max(img.shape[:2]))
    if scale < 1.0:
        img = cv2.resize(img, (int(img.shape[1] * scale), int(img.shape[0] * scale)), interpolation=cv2.INTER_AREA)
    iid = ids.image(n)
    sha = images.save_png(caption(img, "Весь лист (наложение, уменьшено)"), out_dir / "images" / f"{iid}.png")
    return {"id": iid, "kind": "SHEET", "file": f"images/{iid}.png", "sha256": sha, "crop_a": None, "crop_b": None,
            "dpi": None, "image_rotation_deg": 0, "record_ids": []}


def _pack_chunks(clusters: list[dict], no_box: list[str]) -> list[dict[str, Any]]:
    max_rec, max_img = settings.chunk_records(), settings.chunk_images() - 1   # одно место — лист целиком
    chunks: list[dict[str, Any]] = []
    cur: dict[str, Any] = {"clusters": [], "record_ids": []}
    for k, cl in enumerate(clusters):
        if cur["clusters"] and (len(cur["record_ids"]) + len(cl["records"]) > max_rec
                                or 2 * (len(cur["clusters"]) + 1) > max_img):
            chunks.append(cur)
            cur = {"clusters": [], "record_ids": []}
        cur["clusters"].append(k)
        cur["record_ids"] += cl["records"]
    rest = list(no_box)
    while rest:
        room = max_rec - len(cur["record_ids"])
        if room <= 0:
            chunks.append(cur)
            cur = {"clusters": [], "record_ids": []}
            continue
        cur["record_ids"] += rest[:room]
        rest = rest[room:]
    if cur["record_ids"]:
        chunks.append(cur)
    return chunks


# ------------------------------------------------------------------ сборка

def build(object_id: str, run_id: str, spid: str, out_dir: Path) -> dict[str, Any]:
    """Собрать пакет пары листов в ``out_dir`` (папка создаётся заново)."""
    out_dir = Path(out_dir)
    if out_dir.exists() and any(out_dir.iterdir()):
        if not (out_dir / "packet.json").exists():
            raise ValueError(f"папка {out_dir} не пуста и не является пакетом — не перезаписываю")
        for f in sorted(out_dir.rglob("*"), reverse=True):
            f.unlink() if f.is_file() else f.rmdir()
    out_dir.mkdir(parents=True, exist_ok=True)
    run = jobs.get_run(object_id, run_id)
    data, rdir = jobs.sheet_result(object_id, run_id, spid)
    item, result = data["item"], data.get("result")
    pages = ctx.Pages(object_id, run["a"]["sha256"], run["b"]["sha256"])
    spec_file = paths.run_dir(object_id, run_id) / "spec.json"
    status = (result or {}).get("status") or item.get("result_status") or item.get("status")
    p: dict[str, Any] = {
        "schema": SCHEMA, "builder_version": settings.BUILDER_VERSION, "packet_id": None,
        "object_id": object_id, "project_id": run["project_id"], "project": _project(object_id, run["project_id"]),
        "run_id": run_id, "direction": "A→B", "spid": spid, "title": item.get("title"),
        "a": _side_info(run, item, result, pages, "A"), "b": _side_info(run, item, result, pages, "B"),
        "engine": {"algo": (result or {}).get("algo") or run["params"].get("algo"),
                   "result_sha256": _sha_file(rdir / "result.json") if rdir else None,
                   "spec_sha256": _sha_file(spec_file) if item.get("spec") else None,
                   "status": status, "status_label": STATUS_LABELS.get(status or "", status),
                   "reasons": (result or {}).get("reasons") or item.get("reasons") or [],
                   "channels": (result or {}).get("channels") or {},
                   "match_confidence": item.get("confidence"), "part": item.get("part"),
                   "item_kind": item.get("kind"), "truncated": {}},
        "coordinates": {"unit": "pt", "origin": "top_left_visible_page", "x": "right", "y": "down",
                        "canvas": {"system": "A", "dpi": ((result or {}).get("canvas") or {}).get("dpi"),
                                   "origin_pt": ((result or {}).get("canvas") or {}).get("origin_pt")},
                        "transform_b_to_a": ((result or {}).get("transform") or {}).get("matrix"),
                        "missing": None},
        "records": [], "contexts": [], "xrefs": [], "images": [], "chunks": [],
        "settings": settings.packet_settings(), "ai_needed": False, "engine_only_reason": None,
    }
    if result is None or rdir is None:
        reason = {"only_b": "лист добавлен", "only_a": "лист удалён"}.get(item.get("kind"), "нет результата движка")
        p["engine_only_reason"] = f"{reason}; описание по данным движка"
        return _finish(p, out_dir, rdir)

    recs, truncated = records.build(result, item, pages.page_rec("A", item["a_page"]),
                                    pages.page_rec("B", item["b_page"]))
    p["engine"]["truncated"] = truncated
    p["records"] = recs
    ai = [r for r in recs if r["group"] == "ai"]
    if not ai:
        p["engine_only_reason"] = ("записей для разбора нет: только служебные изменения, сдвиги или оформление"
                                   if recs else "отличий нет")
        return _finish(p, out_dir, rdir)
    p["ai_needed"] = True
    tr = result["transform"]
    p["contexts"] = _contexts(recs, item, pages, tr)
    p["xrefs"] = _xrefs(recs, p["contexts"], item, pages)

    clusters = _clusters(recs)
    in_cluster = {i for c in clusters for i in c["records"]}
    no_box = [r["id"] for r in ai if r["id"] not in in_cluster]
    packed = _pack_chunks(clusters, no_box)
    docs = {"A": pdfio.open_memory(pdfio.read_bytes(paths.source_pdf(object_id, run["a"]["sha256"]))),
            "B": pdfio.open_memory(pdfio.read_bytes(paths.source_pdf(object_id, run["b"]["sha256"])))}
    recs_by_id = {r["id"]: r for r in recs}
    n = 1
    chunk_images: list[list[str]] = []
    for ch in packed:
        imgs = []
        for k in ch["clusters"]:
            made = _render_cluster(n, clusters[k], recs_by_id, docs, item, run, result, rdir, out_dir, tr)
            n += len(made)
            p["images"] += made
            imgs += [m["id"] for m in made]
            for m in made:
                for rid in m["record_ids"]:
                    recs_by_id[rid].setdefault("image_ids", []).append(m["id"])
        sheet = _sheet_image(n, rdir, result, [clusters[k]["box"] for k in ch["clusters"]] or [], out_dir) \
            if ch["clusters"] else None
        if sheet:
            n += 1
            p["images"].append(sheet)
            imgs.insert(0, sheet["id"])
        chunk_images.append(imgs)
    p["packet_id"] = settings.digest({k: v for k, v in p.items() if k not in ("packet_id", "run_id")})[:16]
    for k, (ch, imgs) in enumerate(zip(packed, chunk_images), 1):
        p["chunks"].append({"chunk_id": f"{p['packet_id']}-K{k}", "index": k, "record_ids": ch["record_ids"],
                            "image_ids": imgs,
                            "box": geo.rounded(geo.union([clusters[c]["box"] for c in ch["clusters"]]))})
    return _finish(p, out_dir, rdir)


def _finish(p: dict[str, Any], out_dir: Path, rdir: Path | None) -> dict[str, Any]:
    if not p["packet_id"]:
        p["packet_id"] = settings.digest({k: v for k, v in p.items() if k not in ("packet_id", "run_id")})[:16]
    p["counts"] = records.counts(p["records"])
    p["ids"] = {r["id"]: ids.full(p["object_id"], p["run_id"], p["spid"], r["ref"]) for r in p["records"]}
    (out_dir / "packet.json").write_text(json.dumps(p, ensure_ascii=False, indent=1), encoding="utf-8")
    (out_dir / "sheet.md").write_text(prompt.render_sheet(p), encoding="utf-8")
    for ch in p["chunks"]:
        d = out_dir / "chunks" / ch["chunk_id"].rsplit("-", 1)[1]
        d.mkdir(parents=True, exist_ok=True)
        (d / "prompt.md").write_text(prompt.render_chunk(p, ch), encoding="utf-8")
    files = {}
    for f in sorted(out_dir.rglob("*")):
        if f.is_file() and f.name != "manifest.json":
            files[str(f.relative_to(out_dir))] = {"sha256": _sha_file(f), "size": f.stat().st_size}
    manifest = {"schema": "version_comparison_ai.packet_manifest/v1", "packet_id": p["packet_id"],
                "builder_version": settings.BUILDER_VERSION, "prompt_version": settings.PROMPT_VERSION,
                "prompt_digest": prompt.prompt_digest(), "settings_digest": settings.digest(p["settings"]),
                "sources": {"A": p["a"]["sha256"], "B": p["b"]["sha256"]},
                "engine": {"algo": p["engine"]["algo"], "result_sha256": p["engine"]["result_sha256"],
                           "spec_sha256": p["engine"]["spec_sha256"]},
                "coordinates": p["coordinates"], "files": files, "ids": p["ids"]}
    (out_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8")
    return p
