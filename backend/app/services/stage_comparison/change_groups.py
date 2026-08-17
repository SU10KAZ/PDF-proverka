"""Этап 5Б.3: второй уровень над неизменяемыми atomic regions 5Б.2.

``atomic region`` остаётся точным доказательством. ``change group`` — только
presentation/analysis слой, который не меняет bbox и состав атомарных областей.
"""
from __future__ import annotations

import copy
import hashlib
import json
import math
import os
import tempfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import cv2
import numpy as np


SCHEMA_VERSION = 1


def _area(box):
    return max(0.0, float(box[2]) - float(box[0])) * max(0.0, float(box[3]) - float(box[1]))


def _union(boxes):
    boxes = [box for box in boxes if box and len(box) == 4]
    return [min(box[0] for box in boxes), min(box[1] for box in boxes), max(box[2] for box in boxes), max(box[3] for box in boxes)]


def _gap(first, second):
    dx = max(0.0, float(first[0]) - float(second[2]), float(second[0]) - float(first[2]))
    dy = max(0.0, float(first[1]) - float(second[3]), float(second[1]) - float(first[3]))
    return math.hypot(dx, dy)


def _intersects(first, second, padding=0.0):
    return first[0] - padding <= second[2] and second[0] - padding <= first[2] and first[1] - padding <= second[3] and second[1] - padding <= first[3]


def _page_map(document):
    return {int(page.get("pdf_page")): page for page in document.get("pages") or [] if page.get("pdf_page") is not None}


def _block_maps(page):
    boxes, types = {}, {}
    for block in page.get("blocks") or []:
        block_id, box = block.get("block_id"), block.get("bbox_pdf_visual")
        if block_id:
            if box: boxes[str(block_id)] = [float(value) for value in box]
            types[str(block_id)] = str(block.get("type") or block.get("semantic_type") or "")
    return boxes, types


def _effective_role(region, block_types):
    ids = set(region.get("left_block_ids") or []) | set(region.get("right_block_ids") or [])
    if any(block_types.get(str(block_id)) == "stamp" for block_id in ids):
        return "stamp"
    # Старый positional role мог пометить нижнюю часть большого plan-block как
    # stamp. Если region явно находится только в нештамповых blocks, исправляем
    # роль лишь для уровня группировки; сам atomic region не изменяется.
    if ids and not any(block_types.get(str(block_id)) == "stamp" for block_id in ids):
        return "drawing"
    return str(region.get("region_role") or "drawing")


def _shared_blocks(first, second):
    left = set(first.get("left_block_ids") or []) & set(second.get("left_block_ids") or [])
    right = set(first.get("right_block_ids") or []) & set(second.get("right_block_ids") or [])
    return sorted(left | right)


def _disjoint_known_blocks(first, second):
    for side in ("left_block_ids", "right_block_ids"):
        a, b = set(first.get(side) or []), set(second.get(side) or [])
        if a and b and not a.intersection(b):
            return True
    return False


def _support_between(first, second, supporting, page_area):
    """Supporting evidence подтверждает близкую связь, но не задаёт bbox."""
    if _gap(first["bbox"], second["bbox"]) > math.sqrt(page_area) * .045:
        return []
    found = []
    for index, evidence in enumerate(supporting or []):
        box = evidence.get("bbox")
        if not box or _area(box) > page_area * .16:
            continue
        if _intersects(box, first["bbox"], 18) and _intersects(box, second["bbox"], 18):
            found.append(str(evidence.get("evidence_id") or f"support_{index:04d}"))
    return found


def _component_metrics(indices, regions, page_area):
    boxes = [regions[index]["bbox"] for index in indices]
    bbox = _union(boxes)
    atomic_area = sum(_area(box) for box in boxes)
    group_area = _area(bbox)
    fill = min(1.0, atomic_area / max(group_area, 1e-9))
    return bbox, atomic_area, group_area, fill


def build_change_groups(atomic_regions, *, page_width, page_height, block_types=None, supporting_evidence=None):
    """Сгруппировать, не изменяя atomic regions и не используя raw diff."""
    regions = copy.deepcopy(list(atomic_regions or []))
    original_digest = hashlib.sha256(json.dumps(regions, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
    block_types = block_types or {}
    page_area = max(float(page_width) * float(page_height), 1.0)
    page_diagonal = math.hypot(float(page_width), float(page_height))
    direct_limit = max(36.0, page_diagonal * .028)
    roles = [_effective_role(region, block_types) for region in regions]
    parent = list(range(len(regions)))
    members = {index: {index} for index in range(len(regions))}
    edge_reasons: dict[tuple[int, int], list[str]] = {}

    def find(index):
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def merge(first, second):
        first, second = find(first), find(second)
        if first == second:
            return
        parent[second] = first
        members[first] |= members.pop(second)

    candidates = []
    for first in range(len(regions)):
        for second in range(first + 1, len(regions)):
            if roles[first] != roles[second] or _disjoint_known_blocks(regions[first], regions[second]):
                continue
            distance = _gap(regions[first]["bbox"], regions[second]["bbox"])
            shared = _shared_blocks(regions[first], regions[second])
            same_types = sorted(set(regions[first].get("change_types") or []) & set(regions[second].get("change_types") or []))
            support = _support_between(regions[first], regions[second], supporting_evidence, page_area)
            # Нужны минимум два сигнала. Сам факт same_block недостаточен.
            proximity = distance <= direct_limit
            strong_proximity = distance <= direct_limit * .45
            signal_count = int(proximity) + int(bool(shared)) + int(bool(same_types)) + int(bool(support))
            if signal_count < 2 or (not proximity and not support):
                continue
            reasons = []
            if shared: reasons.append("same_block")
            if proximity: reasons.append(f"distance:{distance:.3f}")
            if same_types: reasons.append("same_change_type:" + ",".join(same_types))
            if support: reasons.append("shared_vector_support:" + ",".join(support))
            if strong_proximity: reasons.append("structural_continuity")
            candidates.append((distance, first, second, reasons))

    for _, first, second, reasons in sorted(candidates, key=lambda row: (round(row[0], 6), regions[row[1]].get("region_id", ""), regions[row[2]].get("region_id", ""))):
        root_a, root_b = find(first), find(second)
        if root_a == root_b:
            edge_reasons[(first, second)] = reasons
            continue
        proposed = sorted(members[root_a] | members[root_b])
        bbox, atomic_area, group_area, fill = _component_metrics(proposed, regions, page_area)
        area_ratio = group_area / page_area
        # Не допускаем цепочку маленьких островков через большое пустое поле.
        minimum_fill = .012 if len(proposed) >= 4 else .025
        if fill < minimum_fill or (area_ratio > .22 and atomic_area / page_area < .08):
            continue
        merge(root_a, root_b)
        edge_reasons[(first, second)] = reasons

    grouped = defaultdict(list)
    for index in range(len(regions)):
        grouped[find(index)].append(index)
    ordered = sorted(grouped.values(), key=lambda values: (_union([regions[i]["bbox"] for i in values])[1], _union([regions[i]["bbox"] for i in values])[0], min(regions[i].get("region_id", "") for i in values)))
    groups = []
    for serial, indices in enumerate(ordered, 1):
        bbox, atomic_area, group_area, fill = _component_metrics(indices, regions, page_area)
        atomic_ids = [str(regions[index].get("region_id")) for index in sorted(indices, key=lambda value: str(regions[value].get("region_id")))]
        reasons = sorted({reason for (first, second), values in edge_reasons.items() if first in indices and second in indices for reason in values})
        block_ids = sorted({str(block_id) for index in indices for key in ("left_block_ids", "right_block_ids") for block_id in regions[index].get(key) or []})
        membership = []
        for index in indices:
            local = sorted({reason for (first, second), values in edge_reasons.items() if index in (first, second) and first in indices and second in indices for reason in values})
            membership.append({"atomic_region_id": regions[index].get("region_id"), "grouping_reasons": local or ["single_atomic_region"]})
        confidence = 1.0 if len(indices) == 1 else min(.98, .55 + .08 * min(4, len(reasons)) + .12 * min(1.0, fill / .12))
        groups.append({
            "group_id": f"group_{serial:03d}", "atomic_region_ids": atomic_ids,
            "bbox": [round(float(value), 3) for value in bbox],
            "change_types": sorted({kind for index in indices for kind in regions[index].get("change_types") or []}),
            "region_role": roles[indices[0]], "block_ids": block_ids,
            "confidence": round(confidence, 4), "grouping_reasons": reasons or ["single_atomic_region"],
            "membership": membership,
            "metrics": {
                "atomic_region_count": len(indices), "page_area_ratio": round(group_area / page_area, 6),
                "atomic_area_ratio": round(atomic_area / page_area, 6), "fill_ratio": round(fill, 6),
                "empty_space_ratio": round(1.0 - fill, 6),
            },
        })
    assert original_digest == hashlib.sha256(json.dumps(regions, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
    return groups


def evaluate_change_groups(atomic_report, left_document, right_document):
    left_pages, right_pages = _page_map(left_document), _page_map(right_document)
    items = []
    for source in atomic_report.get("items") or []:
        left_page, right_page = int(source["left_page"]), int(source["right_page"])
        left_meta, right_meta = left_pages.get(left_page) or {}, right_pages.get(right_page) or {}
        size = left_meta.get("page_size") or {}
        _, left_types = _block_maps(left_meta); _, right_types = _block_maps(right_meta)
        groups = build_change_groups(
            source.get("regions") or [], page_width=float(size.get("width") or 1), page_height=float(size.get("height") or 1),
            block_types={**right_types, **left_types}, supporting_evidence=source.get("supporting_vector_evidence") or [],
        )
        for group in groups:
            group["left_page"], group["right_page"] = left_page, right_page
        items.append({
            "left_page": left_page, "right_page": right_page,
            # Точная неизменённая копия evidence level 5Б.2.
            "atomic_regions": copy.deepcopy(source.get("regions") or []),
            "change_groups": groups,
            "supporting_evidence": copy.deepcopy(source.get("supporting_vector_evidence") or []),
            "summary": {"atomic_regions": len(source.get("regions") or []), "change_groups": len(groups)},
        })
    return {
        "schema_version": SCHEMA_VERSION, "kind": "stage_comparison_change_groups_v5b3",
        "settings": {"llm_used": False, "vision_used": False, "findings_created": False, "atomic_regions_changed": False, "raw_primitives_used": False},
        "items": items,
        "summary": {"pairs": len(items), "atomic_regions": sum(item["summary"]["atomic_regions"] for item in items), "change_groups": sum(item["summary"]["change_groups"] for item in items)},
    }


def _render_page(pdf_path, page_number, long_side=1500):
    import fitz
    document = fitz.open(str(pdf_path))
    try:
        page = document[page_number - 1]
        scale = long_side / max(float(page.rect.width), float(page.rect.height), 1.0)
        pixmap = page.get_pixmap(matrix=fitz.Matrix(scale, scale), colorspace=fitz.csGRAY, alpha=False)
        return np.frombuffer(pixmap.samples, dtype=np.uint8).reshape((pixmap.height, pixmap.width)), float(page.rect.width), float(page.rect.height)
    finally:
        document.close()


def write_diagnostics(directory, report, left_pdf):
    directory = Path(directory); directory.mkdir(parents=True, exist_ok=True)
    for item in report.get("items") or []:
        image, width, height = _render_page(left_pdf, int(item["left_page"])); base = cv2.cvtColor(image, cv2.COLOR_GRAY2BGR)
        atomic, group, combined = base.copy(), base.copy(), base.copy()
        def corners(box):
            return (int(box[0] * image.shape[1] / width), int(box[1] * image.shape[0] / height)), (int(box[2] * image.shape[1] / width), int(box[3] * image.shape[0] / height))
        for region in item.get("atomic_regions") or []:
            first, second = corners(region["bbox"])
            cv2.rectangle(atomic, first, second, (255, 80, 0), 1); cv2.rectangle(combined, first, second, (255, 80, 0), 1)
            cv2.putText(atomic, str(region.get("region_id") or "R"), (first[0], max(12, first[1] - 3)), cv2.FONT_HERSHEY_SIMPLEX, .34, (255, 80, 0), 1, cv2.LINE_AA)
        for change_group in item.get("change_groups") or []:
            first, second = corners(change_group["bbox"]); color = (0, 140, 255) if change_group["region_role"] == "stamp" else (0, 0, 220)
            for target in (group, combined):
                cv2.rectangle(target, first, second, color, 2); cv2.putText(target, str(change_group["group_id"]).replace("group_", "G"), (first[0], max(14, first[1] - 5)), cv2.FONT_HERSHEY_SIMPLEX, .5, color, 1, cv2.LINE_AA)
        stem = f"v2_{int(item['left_page']):03d}_v3_{int(item['right_page']):03d}"
        paths = {"atomic_view": directory / f"{stem}_atomic.png", "group_view": directory / f"{stem}_groups.png", "combined_view": directory / f"{stem}_combined.png"}
        cv2.imwrite(str(paths["atomic_view"]), atomic); cv2.imwrite(str(paths["group_view"]), group); cv2.imwrite(str(paths["combined_view"]), combined)
        item["diagnostics"] = {**(item.get("diagnostics") or {}), **{key: str(value) for key, value in paths.items()}}


def _atomic_write(path, text):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            output.write(text); output.flush(); os.fsync(output.fileno())
        os.replace(temporary, path)
    except BaseException:
        try: os.unlink(temporary)
        except OSError: pass
        raise


def write_report(directory, report):
    directory = Path(directory); json_path, md_path = directory / "change_groups.json", directory / "change_groups.md"
    _atomic_write(json_path, json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    lines = ["# Этап 5Б.3 — atomic regions → change groups", "", "Atomic regions скопированы из 5Б.2 без изменений. Group bbox вычислен только по bbox входящих atomic regions.", ""]
    for item in report.get("items") or []:
        lines += [f"## V2 {item['left_page']} ↔ V3 {item['right_page']}", "", f"Atomic regions: {item['summary']['atomic_regions']}; change groups: {item['summary']['change_groups']}.", "", "| Group | Atomic | Role | Types | Page area | Fill | Empty | Confidence | Reasons |", "| --- | ---: | --- | --- | ---: | ---: | ---: | ---: | --- |"]
        for group in item.get("change_groups") or []:
            metrics = group["metrics"]
            lines.append(f"| {group['group_id']} | {metrics['atomic_region_count']} | {group['region_role']} | {', '.join(group['change_types'])} | {metrics['page_area_ratio']:.2%} | {metrics['fill_ratio']:.2%} | {metrics['empty_space_ratio']:.2%} | {group['confidence']:.2f} | {', '.join(group['grouping_reasons'])} |")
        diagnostic = item.get("diagnostics") or {}
        links = [f"[{name}]({os.path.relpath(path, directory)})" for name, path in diagnostic.items()]
        lines += ["", " · ".join(links), ""]
    _atomic_write(md_path, "\n".join(lines)); return json_path, md_path
