"""Оркестратор профиля: PDF → инвентарь → справочная область → символы →
помещения → граф → артефакты.

Статусы результата (гейт применимости):
  complete — профиль применим, легенда разобрана;
  partial  — план извлечён, но часть контура не разобрана (например,
             электрическая легенда) — с явными warnings;
  no_graph — профиль неприменим к блоку (profile_not_applicable /
             rotation_unsupported) — НЕ пустой граф;
  error    — исключение/канонизация (canonical_space_invalid, exception).

Артефакты (out_dir):
  semantic_graph.json, metrics.json, diagnostic_overlay.svg,
  description Markdown, compact_fixture.json, README.md;
  raw_vector_inventory.json — только по запросу (include_raw_inventory).
Повторный запуск на том же файле байт-в-байт стабилен (время — только
в metrics.json, ключ timing).
"""
from __future__ import annotations

import json
import re
import time
import traceback
from pathlib import Path

from . import coords, dimensions as dims_mod, graph as graph_mod, inventory, legend, rooms, symbols
from .coords import CanonicalSpaceError, RotationUnsupported
from .spatial import SpatialIndex

PROFILE_ID = "ar_ceiling_lighting"
PROFILE_VERSION = "2026.08.05-2"

# виды устройств, которые обязаны иметь эталон, чтобы классифицировать план
DEVICE_TEMPLATE_KINDS = {
    "light_output", "chandelier_output", "wall_light_output",
    "switch_1", "switch_2", "switch_changeover", "master_switch", "smoke_detector",
}

SHEET_NAME_RE = re.compile(r"план\s+потолк|потолок\s+и\s+освещени", re.I)


def run_profile(pdf_path: str, *, page_index: int = 0, block_id: str | None = None,
                legend_registry: list[dict] | None = None) -> dict:
    """Полный запуск с гейтом применимости. Никогда не поднимает исключение."""
    try:
        return build_ar_ceiling_lighting_result(
            pdf_path, page_index=page_index, block_id=block_id,
            legend_registry=legend_registry)
    except RotationUnsupported as exc:
        return {"status": "no_graph", "reason": "rotation_unsupported",
                "error": str(exc), "graph": None, "warnings": [str(exc)]}
    except CanonicalSpaceError as exc:
        return {"status": "error", "reason": "canonical_space_invalid",
                "error": str(exc), "graph": None, "warnings": [str(exc)]}
    except Exception as exc:  # noqa: BLE001 — пакетная обработка не падает целиком
        return {"status": "error", "reason": "exception",
                "error": f"{type(exc).__name__}: {exc}",
                "traceback": traceback.format_exc(limit=6),
                "graph": None, "warnings": []}


def build_ar_ceiling_lighting_result(pdf_path: str, *, page_index: int = 0,
                                     block_id: str | None = None,
                                     legend_registry: list[dict] | None = None) -> dict:
    t0 = time.monotonic()
    cp = coords.open_canonical(pdf_path, page_index)
    coords.verify_pdfplumber(cp, pdf_path, page_index)
    inv = inventory.collect_inventory(cp)
    inventory.pair_half_arcs(inv)

    ref = legend.parse_reference(cp, inv)
    scope_of = make_scope(cp, ref)
    legend_zones = ref.get("legend_zones") or []

    elements = symbols.collect_symbol_elements(inv, scope_of, legend_zones)
    marks, marks_rejected = rooms.find_room_marks(inv, scope_of)
    ceil_markers, ceil_unpaired = symbols.detect_ceiling_markers(inv, scope_of, legend_zones)

    # --- гейт применимости профиля (сильные признаки) ---
    gate = _applicability_gate(cp, inv, ref, elements, marks, ceil_markers)
    if not gate["applicable"]:
        return {
            "status": "no_graph", "reason": "profile_not_applicable",
            "graph": None, "gate": gate, "warnings": gate["warnings"],
            "elapsed_s": round(time.monotonic() - t0, 2),
        }

    warnings: list[str] = list(cp.warnings)
    warnings.extend(ref.get("warnings") or [])

    templates = _merge_registry_templates(ref["templates"], legend_registry, warnings)
    device_templates = [t for t in templates if t["kind"] in DEVICE_TEMPLATE_KINDS]
    if elements and not device_templates:
        warnings.append(
            "LIGHTING_LEGEND_NOT_PARSED: на плане есть профильные символы, но ни одна "
            "строка электрической легенды не разобрана — световые устройства не удалось "
            "классифицировать по доступной легенде")

    clusters = symbols.cluster_elements(elements)
    texts_index = _make_texts_index(inv)
    syms = symbols.classify_clusters(clusters, templates, texts_index)
    classified_n = sum(1 for s in syms if s["kind"] != "unresolved_symbol")
    if elements and device_templates and classified_n == 0:
        warnings.append(
            "LIGHTING_SYMBOLS_UNCLASSIFIED: профильные символы на плане есть, но ни один "
            "не совпал с эталонами легенды (включая кросс-листовой реестр) — световые "
            "устройства не удалось классифицировать по доступной легенде")
    labels = symbols.split_number_labels(inv, scope_of, legend_zones)

    room_data = rooms.build_room_regions(inv, cp, marks)

    devices_stub = [{"symbol_id": s["symbol_id"], "center": s["center"]}
                    for s in syms if s["kind"] != "unresolved_symbol"]
    dims, consumed, dim_conflicts = dims_mod.detect_dimensions(inv, scope_of, labels, devices_stub)

    graph = graph_mod.assemble(cp, inv, ref, syms, ceil_markers, ceil_unpaired, labels,
                               dims, consumed, marks, marks_rejected, room_data, dim_conflicts)
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
        "crop_equals_media": cp.crop_equals_media,
        "self_check": cp.self_check,
        "scale_mm_per_pt": next((d["sheet_scale_mm_per_pt"] for d in dims
                                 if d.get("sheet_scale_mm_per_pt")), None),
    }

    status, status_reason = _status_of(graph, warnings, cp)
    graph["warnings"] = sorted(set(graph.get("warnings", []) + warnings))
    graph["status"] = status
    graph["profile_id"] = PROFILE_ID
    graph["profile_version"] = PROFILE_VERSION
    graph["validation"]["profile_status"] = status
    graph["validation"]["warnings_total"] = len(graph["warnings"])

    elapsed = round(time.monotonic() - t0, 2)
    return {"status": status, "reason": status_reason, "cp": cp, "inv": inv, "ref": ref,
            "graph": graph, "room_data": room_data, "gate": gate,
            "warnings": graph["warnings"], "elapsed_s": elapsed}


def _applicability_gate(cp, inv, ref, elements, marks, ceil_markers) -> dict:
    """Сильные признаки профиля; меньше двух — no_graph/profile_not_applicable."""
    sheet_name = (ref.get("sheet_meta") or {}).get("sheet_name") or ""
    name_match = bool(SHEET_NAME_RE.search(sheet_name))
    layer_names = {s["layer"] for s in inv["segments"]} | {t["layer"] for t in inv["texts"]}
    profile_layers = [la for la in layer_names
                      if legend.PROFILE_SYMBOL_LAYERS_RE.search(la or "")]
    legend_profile_rows = sum(sec.get("profile_rows", 0)
                              for sec in ref.get("legend_sections") or [])
    signals = {
        "sheet_name_match": name_match,
        "profile_layers": sorted(profile_layers),
        "legend_profile_rows": legend_profile_rows,
        "ceiling_markers": len(ceil_markers),
        "room_marks": len(marks),
        "light_symbol_elements": len(elements),
        "apartment_cards": len(ref.get("apartment_cards") or []),
    }
    strong = sum((
        name_match,
        len(profile_layers) >= 2,
        legend_profile_rows > 0,
        len(ceil_markers) > 0,
        len(marks) > 0,
        len(elements) > 0,
    ))
    gate = {"applicable": strong >= 2, "strong_signals": strong, "signals": signals,
            "warnings": []}
    if not gate["applicable"]:
        gate["warnings"].append(
            f"PROFILE_NOT_APPLICABLE: сильных признаков {strong} < 2 "
            f"(имя листа: {sheet_name or 'не извлечено'})")
    return gate


def _merge_registry_templates(own_templates: list[dict], registry: list[dict] | None,
                              warnings: list[str]) -> list[dict]:
    """Кросс-листовой реестр: добавляем эталоны видов, которых нет в
    легенде текущего листа (tier 4, source=cross_sheet_legend_registry)."""
    templates = list(own_templates)
    if not registry:
        return templates
    own_kinds = {t["kind"] for t in own_templates if t["kind"] in DEVICE_TEMPLATE_KINDS}
    added = []
    for entry in registry:
        kind = entry.get("kind")
        if kind not in DEVICE_TEMPLATE_KINDS or kind in own_kinds:
            continue
        templates.append({
            "kind": kind,
            "label": entry.get("label") or legend.KIND_RU.get(kind, kind),
            "label_bbox": None,
            "symbol_zone": None,
            "signature": entry["signature"],
            "source": "cross_sheet_legend_registry",
            "registry_pdfs": entry.get("pdfs") or [],
            "tier": 4,
        })
        added.append(kind)
    if added:
        warnings.append(
            "LEGEND_FROM_REGISTRY: виды " + ", ".join(sorted(added)) +
            " взяты из кросс-листового реестра легенд (на этом листе подписи нет)")
    return templates


def _status_of(graph, warnings, cp) -> tuple[str, str | None]:
    if any(w.startswith("LIGHTING_LEGEND_NOT_PARSED") for w in warnings):
        return "partial", "lighting_legend_not_parsed"
    if any(w.startswith("LIGHTING_SYMBOLS_UNCLASSIFIED") for w in warnings):
        return "partial", "lighting_symbols_unclassified"
    if cp.text_decoding == "text_decoding_unusable":
        return "partial", "text_decoding_unusable"
    if graph["validation"]["rooms_total"] == 0:
        return "partial", "no_room_marks"
    return "complete", None


def make_scope(cp, ref):
    """block_scope: bbox целиком в области плана.

    Область плана = CropBox минус справочная колонка (ref_col_x0). На
    листах без «скрытой» области (CropBox == MediaBox) работает только
    граница справочной колонки. Fail-closed: объект, зацепивший границу,
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


def compact_fixture(graph: dict) -> dict:
    """Компактный регрессионный слепок блока (для fixture-тестов и матрицы)."""
    v = graph["validation"]
    return {
        "profile_id": graph.get("profile_id"),
        "profile_version": graph.get("profile_version"),
        "status": graph.get("status"),
        "apartments": sorted(a["id"] for a in graph["apartments"]),
        "rooms_by_apartment": {a["id"]: len(a["rooms"]) for a in graph["apartments"]},
        "validation": {k: v[k] for k in sorted(v)},
        "groups_confirmed": sorted(g["group_id"] for g in graph["groups"]
                                   if g["state"] == "confirmed"),
        "warnings": graph.get("warnings") or [],
        "conflicts_total": len(graph.get("conflicts") or []),
    }


def write_artifacts(result: dict, out_dir: str, *, pdf_path: str,
                    include_raw_inventory: bool = True,
                    markdown_name: str | None = None) -> dict:
    from .render_md import render_markdown
    from .overlay import render_overlay_svg

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    def dump(name, payload):
        path = out / name
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=1, sort_keys=True),
                        encoding="utf-8")
        return str(path)

    paths = {}
    if result.get("graph") is None:
        # no_graph / error: честный маленький артефакт вместо пустого графа
        paths["semantic_graph"] = dump("semantic_graph.json", {
            "profile_id": PROFILE_ID, "profile_version": PROFILE_VERSION,
            "status": result.get("status"), "reason": result.get("reason"),
            "error": result.get("error"), "gate": result.get("gate"),
            "warnings": result.get("warnings") or [], "graph": None,
        })
        paths["metrics"] = dump("metrics.json", {
            "status": result.get("status"), "reason": result.get("reason"),
            "timing": {"elapsed_s": result.get("elapsed_s")},
        })
        return paths

    graph = result["graph"]
    block_id = graph["source"]["block_id"]

    paths["semantic_graph"] = dump("semantic_graph.json", graph)
    if include_raw_inventory:
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
    metrics["status"] = graph.get("status")
    metrics["timing"] = {"elapsed_s": result["elapsed_s"]}
    metrics["self_check"] = result["cp"].self_check
    metrics["centering"] = graph["centering"]
    metrics["gate"] = result.get("gate")
    paths["metrics"] = dump("metrics.json", metrics)

    md = render_markdown(graph)
    md_path = out / (markdown_name or f"{block_id}_apartments.md")
    md_path.write_text(md, encoding="utf-8")
    paths["markdown"] = str(md_path)

    paths["compact_fixture"] = dump("compact_fixture.json", compact_fixture(graph))

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
обозначениям самого листа (при отсутствии подписи — по кросс-листовому
реестру легенд комплекта), номера групп читаются по координатам
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
| `description_full.md` / `<block_id>_apartments.md` | итоговое человекочитаемое описание квартир |
| `semantic_graph.json` | полный граф: квартиры, помещения, потолки, свет, группы, размеры, provenance |
| `compact_fixture.json` | компактный регрессионный слепок |
| `metrics.json` | счётчики, статус, самопроверка координат, время |
| `diagnostic_overlay.svg` | диагностическая схема поверх плана |
| `raw_vector_inventory.json` | сырой инвентарь (только с --include-raw-inventory) |

## Ограничения текущего профиля

- Требуется вектор-слой (сканы не поддерживаются) и `page.rotation == 0`
  (иначе честный `no_graph: rotation_unsupported`).
- Границы потолочных зон при нескольких марках в одном помещении не
  восстанавливаются — такие зоны остаются `unresolved`.
- Название помещения берётся только из строки ведомости листа; иные
  источники имён не используются.
- Мастер-выключатель получает область действия «квартира» без
  пофидерных connections-рёбер (на плане они не показаны).
- Числа без полной размерной конструкции размером не считаются; связь
  «размер → устройство» только по близости — tier 2, «Требует проверки».

## Итог последнего прогона

Статус {graph.get('status')}. Квартир {v['apartments_total']},
помещений {v['rooms_total']} (с именем {v['rooms_named']}),
потолочных марок {v['ceiling_zones_total']},
световых точек {v['lights_total']} (настенных {v.get('wall_lights_total', 0)}),
выключателей {v['switches_total']},
мастер-выключателей {v['master_switches_total']},
групп подтверждено {v['groups_confirmed']} / неполных {v['groups_incomplete']},
конфликтов {v['conflicts_total']}, предупреждений {v.get('warnings_total', 0)}.
"""
