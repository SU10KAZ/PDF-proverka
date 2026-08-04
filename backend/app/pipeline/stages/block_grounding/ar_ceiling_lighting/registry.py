"""Кросс-листовой реестр условных обозначений комплекта.

Один лист может содержать неполную легенду (например, на листе 001 нет
подписи «вывод под настенный светильник», хотя символ на плане есть).
Реестр строится ДО обработки блоков из легенд ВСЕХ найденных однотипных
PDF; лист без собственной подписи получает эталон вида из реестра
(source=cross_sheet_legend_registry, tier 4).

Слияние записей — только через геометрический гейт совпадения сигнатур.
Конфликтующие значения не перезаписываются: обе записи остаются, плюс
GEOMETRY_CONFLICT в реестре.
"""
from __future__ import annotations

import json
from pathlib import Path

from . import coords, inventory, legend

# гейт слияния сигнатур: относительный допуск размеров элементов
MERGE_SIZE_TOL = 0.22


def _signatures_compatible(a: dict, b: dict) -> bool:
    if len(a["circles"]) != len(b["circles"]) or len(a["rects"]) != len(b["rects"]):
        return False
    for va, vb in zip(a["circles"], b["circles"]):
        if abs(va - vb) > MERGE_SIZE_TOL * max(vb, 1.0):
            return False
    for (aw, ah), (bw, bh) in zip(a["rects"], b["rects"]):
        if abs(aw - bw) > MERGE_SIZE_TOL * max(bw, 1.0) or \
                abs(ah - bh) > MERGE_SIZE_TOL * max(bh, 1.0):
            return False
    if a["n_diag_lines"] != b["n_diag_lines"] or a["n_axis_lines"] != b["n_axis_lines"]:
        return False
    if sorted(a.get("colors") or {}) != sorted(b.get("colors") or {}):
        return False
    if a.get("inner_letters") != b.get("inner_letters"):
        return False
    return True


def _aspect(zone) -> float | None:
    if not zone:
        return None
    w = zone[2] - zone[0]
    h = zone[3] - zone[1]
    if h <= 0.1:
        return None
    return round(w / h, 2)


def build_legend_registry(pdf_paths: list[str]) -> dict:
    """Реестр легенд по списку PDF. Детерминирован (сортировка по имени)."""
    entries: list[dict] = []
    conflicts: list[dict] = []
    sheets: list[dict] = []

    for pdf_path in sorted(pdf_paths, key=lambda p: Path(p).name):
        name = Path(pdf_path).name
        try:
            cp = coords.open_canonical(pdf_path, 0)
            inv = inventory.collect_inventory(cp)
            inventory.pair_half_arcs(inv)
            ref = legend.parse_reference(cp, inv)
        except coords.CanonicalSpaceError as exc:
            sheets.append({"pdf": name, "state": "skipped", "error": str(exc)})
            continue
        templates = [t for t in ref["templates"]
                     if t["kind"] not in ("unresolved_legend_row",)]
        sheets.append({"pdf": name, "state": "parsed",
                       "legend_rows": len(ref["legend_labels"]),
                       "profile_templates": len(templates)})
        for tpl in templates:
            sig = tpl["signature"]
            merged = False
            for entry in entries:
                if entry["kind"] != tpl["kind"]:
                    continue
                if _signatures_compatible(entry["signature"], sig):
                    if name not in entry["pdfs"]:
                        entry["pdfs"].append(name)
                    if tpl["label"] not in entry["labels"]:
                        entry["labels"].append(tpl["label"])
                    merged = True
                    break
                # тот же вид, несовместимая геометрия → конфликт, не перезапись
                conflicts.append({
                    "type": "GEOMETRY_CONFLICT",
                    "what": f"реестр легенд: вид «{tpl['kind']}»",
                    "candidates": [entry["pdfs"], [name]],
                    "detail": "сигнатуры символа с разных листов не прошли "
                              "геометрический гейт слияния — записи хранятся раздельно",
                })
            if not merged:
                entries.append({
                    "kind": tpl["kind"],
                    "label": tpl["label"],
                    "labels": [tpl["label"]],
                    "signature": sig,
                    "aspect_ratio": _aspect(tpl.get("symbol_zone")),
                    "layers": sorted((sig.get("layers") or {}).keys()),
                    "colors": sorted((sig.get("colors") or {}).keys()),
                    "rotation_invariant": True,  # сигнатура из min/max сторон и счётчиков линий
                    "pdfs": [name],
                    "legend_bbox": tpl.get("symbol_zone"),
                    "provenance": "sheet_legend_label",
                    "tier": 4,
                })

    entries.sort(key=lambda e: (e["kind"], e["label"]))
    return {
        "schema_version": 1,
        "profile_id": "ar_ceiling_lighting",
        "entries": entries,
        "conflicts": conflicts,
        "sheets": sheets,
    }


def save_legend_registry(registry: dict, path: str | Path) -> Path:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(registry, ensure_ascii=False, indent=1, sort_keys=True),
                   encoding="utf-8")
    return out


def load_legend_registry(path: str | Path) -> list[dict]:
    """Записи реестра для инъекции в шаблоны листа ([] если файла нет)."""
    p = Path(path)
    if not p.is_file():
        return []
    data = json.loads(p.read_text(encoding="utf-8"))
    return list(data.get("entries") or [])
