"""Оркестратор профиля: PDF → инвентарь → справочная область → символы →
помещения → граф → артефакты.

Артефакты (out_dir):
  semantic_graph.json, raw_vector_inventory.json, metrics.json,
  diagnostic_overlay.svg, <block_id>_apartments.md, README.md.
Повторный запуск на том же файле байт-в-байт стабилен (время — только
в metrics.json, ключ timing).
"""
from __future__ import annotations

import json
import time
from pathlib import Path

from . import coords, dimensions as dims_mod, graph as graph_mod, inventory, legend, rooms, symbols
from .spatial import SpatialIndex


def build_ar_ceiling_lighting_result(pdf_path: str, *, page_index: int = 0,
                                     block_id: str | None = None) -> dict:
    t0 = time.monotonic()
    cp = coords.open_canonical(pdf_path, page_index)
    coords.verify_pdfplumber(cp, pdf_path, page_index)
    inv = inventory.collect_inventory(cp)
    inventory.pair_half_arcs(inv)

    ref = legend.parse_reference(cp, inv)
    scope_of = make_scope(cp, ref)
    legend_zone = ref.get("legend_zone")

    elements = symbols.collect_symbol_elements(inv, scope_of, legend_zone)
    clusters = symbols.cluster_elements(elements)
    texts_index = _make_texts_index(inv)
    syms = symbols.classify_clusters(clusters, ref["templates"], texts_index)

    ceil_markers, ceil_unpaired = symbols.detect_ceiling_markers(inv, scope_of, legend_zone)
    labels = symbols.split_number_labels(inv, scope_of, legend_zone)

    marks, marks_rejected = rooms.find_room_marks(inv, scope_of)
    room_data = rooms.build_room_regions(inv, cp, marks)

    devices_stub = [{"symbol_id": s["symbol_id"], "center": s["center"]}
                    for s in syms if s["kind"] != "unresolved_symbol"]
    dims, consumed = dims_mod.detect_dimensions(inv, scope_of, labels, devices_stub)

    graph = graph_mod.assemble(cp, inv, ref, syms, ceil_markers, ceil_unpaired, labels,
                               dims, consumed, marks, marks_rejected, room_data, None)
    guides = dims_mod.detect_centering_guides(inv, scope_of, graph["lights"])
    for light in graph["lights"]:
        pair = guides["confirmed"].get(light["symbol_id"])
        light["centered_by_guides"] = bool(pair)
        if pair:
            light["centering_evidence"] = list(pair)
    graph["centering"] = {"chains_total": guides["chains_total"],
                          "diag_chains": guides["diag_chains"],
                          "lights_confirmed": len(guides["confirmed"])}

    graph["source"] = {"pdf_file": Path(pdf_path).name, "page_index": page_index,
                       "block_id": block_id or Path(pdf_path).stem.split("—")[-1].strip()}
    graph["coordinate_space"] = {
        "units": "pt", "origin": "mediabox top-left", "block_rect": list(cp.block_rect),
        "self_check": cp.self_check,
        "scale_mm_per_pt": next((d["sheet_scale_mm_per_pt"] for d in dims
                                 if d.get("sheet_scale_mm_per_pt")), None),
    }
    elapsed = round(time.monotonic() - t0, 2)
    return {"cp": cp, "inv": inv, "ref": ref, "graph": graph, "room_data": room_data,
            "elapsed_s": elapsed}


def make_scope(cp, ref):
    """block_scope: bbox целиком в CropBox и левее справочной колонки.

    Fail-closed: объект, зацепивший границу кропа или справочную колонку,
    в план не попадает (вернётся 'crop_clipped'/'reference')."""
    x0, y0, x1, y1 = cp.block_rect
    ref_x0 = min(ref.get("ref_col_x0") or x1, x1)

    def scope_of(bbox) -> str:
        if bbox[2] >= ref_x0:
            return "reference"
        if bbox[0] >= x0 - 0.5 and bbox[1] >= y0 - 0.5 and bbox[2] <= x1 + 0.5 and bbox[3] <= y1 + 0.5:
            return "block"
        if bbox[2] < x0 or bbox[0] > x1 or bbox[3] < y0 or bbox[1] > y1:
            return "reference"
        return "crop_clipped"

    return scope_of


def _make_texts_index(inv):
    index = SpatialIndex(cell=14.0)
    for t in inv["texts"]:
        index.insert(t["tid"], t["bbox"])

    def query(bbox, pad=0.0):
        out = []
        for tid in index.query(bbox, pad=pad):
            t = inv["texts"][tid]
            cx = (t["bbox"][0] + t["bbox"][2]) / 2
            cy = (t["bbox"][1] + t["bbox"][3]) / 2
            if bbox[0] - pad <= cx <= bbox[2] + pad and bbox[1] - pad <= cy <= bbox[3] + pad:
                out.append(t)
        return out

    return query


def write_artifacts(result: dict, out_dir: str, *, pdf_path: str) -> dict:
    from .render_md import render_markdown
    from .overlay import render_overlay_svg

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    graph = result["graph"]
    block_id = graph["source"]["block_id"]

    def dump(name, payload):
        path = out / name
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=1, sort_keys=True),
                        encoding="utf-8")
        return str(path)

    paths = {}
    paths["semantic_graph"] = dump("semantic_graph.json", graph)
    inv = result["inv"]
    paths["raw_inventory"] = dump("raw_vector_inventory.json", {
        "texts": inv["texts"],
        "drawings": inv["drawings"],
        "circles": inv["circles"],
        "quads": inv["quads"],
        "segments_total": len(inv["segments"]),
        "segments_by_layer": _count_by(inv["segments"], "layer"),
        "words_fitz_total": len(inv["words"]),
    })
    metrics = dict(graph["validation"])
    metrics["timing"] = {"elapsed_s": result["elapsed_s"]}
    metrics["self_check"] = result["cp"].self_check
    metrics["centering"] = graph["centering"]
    paths["metrics"] = dump("metrics.json", metrics)

    md = render_markdown(graph)
    md_path = out / f"{block_id}_apartments.md"
    md_path.write_text(md, encoding="utf-8")
    paths["markdown"] = str(md_path)

    svg = render_overlay_svg(result, pdf_path)
    svg_path = out / "diagnostic_overlay.svg"
    svg_path.write_text(svg, encoding="utf-8")
    paths["overlay"] = str(svg_path)

    readme = _readme(graph, pdf_path)
    (out / "README.md").write_text(readme, encoding="utf-8")
    paths["readme"] = str(out / "README.md")
    return paths


def _count_by(items, key):
    counts: dict[str, int] = {}
    for item in items:
        k = str(item.get(key) or "")
        counts[k] = counts.get(k, 0) + 1
    return dict(sorted(counts.items()))


def _readme(graph, pdf_path) -> str:
    v = graph["validation"]
    return f"""# Вектограф: профиль «АР. План потолков и освещения» (shadow-пилот)

Детерминированное извлечение из векторного слоя PDF: без LLM, без OCR,
без растрового распознавания. Символы классифицируются по условным
обозначениям самого листа, номера групп читаются по координатам
отдельных знаков, помещения восстанавливаются заливкой от марки.

## Запуск

```bash
python scripts/build_ar_ceiling_lighting_description.py \\
  --pdf "{pdf_path}" \\
  --out-dir "<эта папка>"
```

## Файлы

| Файл | Содержимое |
|---|---|
| `{graph['source']['block_id']}_apartments.md` | итоговое человекочитаемое описание квартир |
| `semantic_graph.json` | полный граф: квартиры, помещения, потолки, свет, группы, размеры, provenance |
| `raw_vector_inventory.json` | сырой инвентарь векторного слоя (тексты, drawings, окружности) |
| `metrics.json` | счётчики, самопроверка координат, время |
| `diagnostic_overlay.svg` | диагностическая схема поверх плана |

## Ограничения текущего профиля

- Требуется вектор-слой (сканы не поддерживаются) и `page.rotation == 0`.
- Границы потолочных зон при нескольких марках в одном помещении не
  восстанавливаются — такие зоны остаются `unresolved`.
- Название помещения берётся только из строки ведомости листа; иные
  источники имён не используются.
- Мастер-выключатель получает область действия «квартира» без
  пофидерных connections-рёбер (на плане они не показаны).
- Числа без полной размерной конструкции размером не считаются.

## Итог последнего прогона

Квартир {v['apartments_total']}, помещений {v['rooms_total']}
(с именем {v['rooms_named']}), потолочных марок {v['ceiling_zones_total']},
световых точек {v['lights_total']}, выключателей {v['switches_total']},
мастер-выключателей {v['master_switches_total']},
групп подтверждено {v['groups_confirmed']} / неполных {v['groups_incomplete']},
конфликтов {v['conflicts_total']}.
"""
