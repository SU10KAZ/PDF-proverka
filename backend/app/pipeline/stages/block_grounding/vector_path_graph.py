"""Общие векторные примитивы для клеммных схем и аксонометрических выносок.

Слой не знает предметную область: он разворачивает ``page.get_drawings()`` в
отрезки, строит связные компоненты по концам/T-соединениям, находит клеммные овалы
и связывает текстовые выноски с цветной геометрией. Простое пересечение двух линий
без конца или точки не считается соединением.
"""
from __future__ import annotations

import math
import re
from collections import Counter


def _color_tuple(color):
    return tuple(round(float(value), 3) for value in color) if color is not None else None


def _is_black(color, tolerance: float = 0.03) -> bool:
    return color is not None and max(abs(float(value)) for value in color) <= tolerance


def _is_neutral(color, tolerance: float = 0.06) -> bool:
    return color is not None and max(color) - min(color) <= tolerance


def _is_vivid(color, chroma: float = 0.2) -> bool:
    return color is not None and max(color) - min(color) >= chroma


def _absolute_bbox(page, bbox_norm):
    if not bbox_norm or len(bbox_norm) < 4:
        return None
    try:
        x0, y0, x1, y1 = (float(value) for value in bbox_norm[:4])
    except (TypeError, ValueError):
        return None
    if x1 <= x0 or y1 <= y0:
        return None
    return (x0 * float(page.rect.width), y0 * float(page.rect.height),
            x1 * float(page.rect.width), y1 * float(page.rect.height))


def _rect_intersects_bbox(rect, bbox) -> bool:
    if rect is None:
        return False
    if bbox is None:
        return True
    return not (float(rect.x1) < bbox[0] or float(rect.x0) > bbox[2]
                or float(rect.y1) < bbox[1] or float(rect.y0) > bbox[3])


def _drawings_for_bbox(page, bbox_norm):
    bbox = _absolute_bbox(page, bbox_norm)
    return [drawing for drawing in page.get_drawings()
            if _rect_intersects_bbox(drawing.get("rect"), bbox)]


def point_segment_distance(point, segment) -> float:
    """Евклидово расстояние от точки до конечного отрезка."""
    px, py = point
    x1, y1 = segment["p1"]
    x2, y2 = segment["p2"]
    dx, dy = x2 - x1, y2 - y1
    if dx == 0 and dy == 0:
        return math.hypot(px - x1, py - y1)
    t = max(0.0, min(1.0, ((px - x1) * dx + (py - y1) * dy) / (dx * dx + dy * dy)))
    return math.hypot(px - (x1 + t * dx), py - (y1 + t * dy))


def flatten_line_segments(
    drawings,
    *,
    color_mode: str = "any",
    stroke_only: bool = True,
    min_length: float = 1.0,
) -> list[dict]:
    """Развернуть line-items drawings в нормализованные отрезки с provenance."""
    segments = []
    for drawing_index, drawing in enumerate(drawings):
        color = drawing.get("color")
        if stroke_only and drawing.get("fill") is not None:
            continue
        if color_mode == "black" and not _is_black(color):
            continue
        if color_mode == "neutral" and not _is_neutral(color):
            continue
        if color_mode == "vivid" and not _is_vivid(color):
            continue
        for item_index, item in enumerate(drawing.get("items") or []):
            if item[0] != "l":
                continue
            start, end = item[1], item[2]
            length = math.hypot(float(end.x) - float(start.x), float(end.y) - float(start.y))
            if length < min_length:
                continue
            segments.append({
                "id": f"segment-{len(segments) + 1}",
                "p1": (float(start.x), float(start.y)),
                "p2": (float(end.x), float(end.y)),
                "length": round(length, 4),
                "color": _color_tuple(color),
                "width": float(drawing.get("width") or 0),
                "drawing_index": drawing_index,
                "item_index": item_index,
            })
    return segments


def detect_terminal_ellipses(drawings) -> list[dict]:
    """Найти физические клеммные окружности, собранные CAD из двух полуовалов.

    В пилотном СОВ каждая видимая окружность 8.5×8.5 pt экспортирована двумя
    соседними paths по 8.5×4.3 pt. Возвращать их как две клеммы нельзя: это
    удваивает инвентарь и создаёт ложные сети из одного физического контакта.
    """
    halves = []
    for drawing_index, drawing in enumerate(drawings):
        rect = drawing.get("rect")
        kinds = tuple(item[0] for item in (drawing.get("items") or []))
        if not rect or not _is_black(drawing.get("color")) or drawing.get("fill") is not None:
            continue
        if kinds != ("c", "c") or not (7.0 <= float(rect.width) <= 10.0 and 3.0 <= float(rect.height) <= 6.0):
            continue
        halves.append({
            "center": ((float(rect.x0) + float(rect.x1)) / 2, (float(rect.y0) + float(rect.y1)) / 2),
            "bbox": [float(rect.x0), float(rect.y0), float(rect.x1), float(rect.y1)],
            "drawing_index": drawing_index,
        })

    anchors = []
    used = set()
    for index, upper in enumerate(halves):
        if index in used:
            continue
        candidates = []
        for other_index, lower in enumerate(halves):
            if other_index == index or other_index in used:
                continue
            same_axis = abs(upper["center"][0] - lower["center"][0]) <= 0.35
            touching = min(
                abs(upper["bbox"][3] - lower["bbox"][1]),
                abs(lower["bbox"][3] - upper["bbox"][1]),
            ) <= 0.35
            if same_axis and touching:
                candidates.append((abs(upper["center"][1] - lower["center"][1]), other_index, lower))
        if not candidates:
            used.add(index)
            bbox = list(upper["bbox"])
            drawing_indexes = [upper["drawing_index"]]
            shape_state = "half_unpaired"
        else:
            _distance, other_index, lower = min(candidates)
            used.update((index, other_index))
            bbox = [
                min(upper["bbox"][0], lower["bbox"][0]),
                min(upper["bbox"][1], lower["bbox"][1]),
                max(upper["bbox"][2], lower["bbox"][2]),
                max(upper["bbox"][3], lower["bbox"][3]),
            ]
            drawing_indexes = [upper["drawing_index"], lower["drawing_index"]]
            shape_state = "merged_halves"
        anchors.append({
            "id": f"terminal-{len(anchors) + 1}",
            "center": ((bbox[0] + bbox[2]) / 2, (bbox[1] + bbox[3]) / 2),
            "bbox": bbox,
            "drawing_indexes": drawing_indexes,
            "shape_state": shape_state,
        })
    return anchors


def _bbox_distance(left, right) -> float:
    return math.hypot(
        max(float(left[0]) - float(right[2]), 0, float(right[0]) - float(left[2])),
        max(float(left[1]) - float(right[3]), 0, float(right[1]) - float(left[3])),
    )


def _point_in_bbox(point, bbox, margin: float = 0.0) -> bool:
    return (
        float(bbox[0]) - margin <= point[0] <= float(bbox[2]) + margin
        and float(bbox[1]) - margin <= point[1] <= float(bbox[3]) + margin
    )


def detect_terminal_parent_regions(page, drawings, terminals: list[dict], component_labels: list[dict]) -> list[dict]:
    """Найти внешние оболочки приборов и назначить им позиционные подписи."""
    candidates = []
    for drawing in drawings:
        rect = drawing.get("rect")
        kinds = tuple(item[0] for item in (drawing.get("items") or []))
        if not rect or kinds != ("qu",):
            continue
        if not (40 <= float(rect.width) <= float(page.rect.width) * 0.5):
            continue
        if not (35 <= float(rect.height) <= float(page.rect.height) * 0.85):
            continue
        bbox = [float(rect.x0), float(rect.y0), float(rect.x1), float(rect.y1)]
        terminal_ids = [
            terminal["id"] for terminal in terminals
            if _point_in_bbox(terminal["center"], bbox, margin=0.7)
        ]
        if len(terminal_ids) >= 2:
            candidates.append({"bbox": bbox, "terminal_ids": terminal_ids})

    # Внутренние таблицы клемм тоже quads. Родителем является максимальная внешняя
    # оболочка, поэтому вложенные кандидаты с меньшим числом клемм отбрасываем.
    regions = []
    for candidate in candidates:
        bbox = candidate["bbox"]
        nested = any(
            other is not candidate
            and _point_in_bbox((bbox[0], bbox[1]), other["bbox"], margin=0.2)
            and _point_in_bbox((bbox[2], bbox[3]), other["bbox"], margin=0.2)
            and len(other["terminal_ids"]) > len(candidate["terminal_ids"])
            for other in candidates
        )
        if nested:
            continue
        if component_labels:
            label = min(component_labels, key=lambda item: _bbox_distance(bbox, item["bbox"]))
            distance = _bbox_distance(bbox, label["bbox"])
        else:
            label, distance = None, None
        regions.append({
            "id": f"parent-region-{len(regions) + 1}",
            "bbox": [round(value, 3) for value in bbox],
            "terminal_ids": candidate["terminal_ids"],
            "component_id": label.get("id") if label and distance <= 30 else None,
            "component_name": label.get("name") if label and distance <= 30 else None,
            "label_distance": round(distance, 3) if distance is not None else None,
        })
    return regions


_TERMINAL_LABEL_RE = re.compile(
    r"^(?:[+\-]+|\d+|D0|D1/T|GND\d*|U\+|\+U|BEEP|GREEN|RED|"
    r"DOOR\d*|EXIT\d*|SENS\d*|L\*?|N\*?|PE|COM|NC|NO|\+D|-D|АЛС)$",
    re.IGNORECASE,
)


def _terminal_text_label(words, terminal) -> tuple[str | None, float | None]:
    x, y = terminal["center"]
    candidates = []
    for word in words:
        token = str(word[4])
        if not _TERMINAL_LABEL_RE.fullmatch(token):
            continue
        wx = (float(word[0]) + float(word[2])) / 2
        wy = (float(word[1]) + float(word[3])) / 2
        if abs(wy - y) > 7 or abs(wx - x) > 75:
            continue
        horizontal_gap = max(float(word[0]) - x, 0, x - float(word[2]))
        candidates.append((horizontal_gap + 1.5 * abs(wy - y), token))
    if not candidates:
        return None, None
    distance, token = min(candidates)
    return token, round(distance, 3)


def _external_segments(segments: list[dict], regions: list[dict]) -> list[dict]:
    """Удалить геометрию целиком внутри корпуса прибора, сохранив внешние линии."""
    result = []
    for segment in segments:
        internal = any(
            _point_in_bbox(segment["p1"], region["bbox"], margin=0.4)
            and _point_in_bbox(segment["p2"], region["bbox"], margin=0.4)
            for region in regions if region.get("component_id")
        )
        if not internal:
            result.append(segment)
    return result


def build_terminal_wiring_topology(
    page,
    component_labels: list[dict],
    *,
    bbox_norm=None,
) -> dict:
    """Построить консервативный семантический граф внешних клемм СОВ."""
    drawings = _drawings_for_bbox(page, bbox_norm)
    terminals = detect_terminal_ellipses(drawings)
    regions = detect_terminal_parent_regions(page, drawings, terminals, component_labels)
    words = page.get_text("words")
    absolute_bbox = _absolute_bbox(page, bbox_norm)
    if absolute_bbox:
        words = [
            word for word in words
            if absolute_bbox[0] <= (float(word[0]) + float(word[2])) / 2 <= absolute_bbox[2]
            and absolute_bbox[1] <= (float(word[1]) + float(word[3])) / 2 <= absolute_bbox[3]
        ]

    parent_by_terminal = {}
    for region in regions:
        if not region.get("component_id"):
            continue
        for terminal_id in region["terminal_ids"]:
            parent_by_terminal[terminal_id] = {
                "component_id": region["component_id"],
                "component_name": region["component_name"],
                "parent_source": "enclosing_region",
            }
    for terminal in terminals:
        if terminal["id"] in parent_by_terminal or not component_labels:
            continue
        point_bbox = [*terminal["center"], *terminal["center"]]
        nearest = min(component_labels, key=lambda item: _bbox_distance(point_bbox, item["bbox"]))
        distance = _bbox_distance(point_bbox, nearest["bbox"])
        if distance <= 120:
            parent_by_terminal[terminal["id"]] = {
                "component_id": nearest["id"],
                "component_name": nearest["name"],
                "parent_source": "nearest_label_inferred",
            }

    raw_segments = flatten_line_segments(
        drawings, color_mode="black", stroke_only=True, min_length=1.0
    )
    segments = _external_segments(raw_segments, regions)
    graph = build_segment_components(segments, tolerance=0.6)

    # Клемма является электрическим стыком: она объединяет все path-компоненты,
    # чьи концы касаются окружности. Ближайший единственный узел здесь недостаточен.
    component_ids = [component["id"] for component in graph["components"]]
    parent = {component_id: component_id for component_id in component_ids}

    def find(component_id):
        while parent[component_id] != component_id:
            parent[component_id] = parent[parent[component_id]]
            component_id = parent[component_id]
        return component_id

    def union(left, right):
        left, right = find(left), find(right)
        if left != right:
            parent[right] = left

    touched_components = {}
    for terminal in terminals:
        touched = {
            graph["component_by_node"].get(node_index)
            for node_index, point in enumerate(graph["nodes"])
            if math.hypot(point[0] - terminal["center"][0], point[1] - terminal["center"][1]) <= 4.8
        }
        touched.discard(None)
        touched_components[terminal["id"]] = sorted(touched)
        if touched:
            first, *rest = sorted(touched)
            for component_id in rest:
                union(first, component_id)

    network_members = {}
    terminal_rows = []
    for terminal in terminals:
        label, label_distance = _terminal_text_label(words, terminal)
        touched = touched_components[terminal["id"]]
        network_root = find(touched[0]) if touched else None
        if network_root:
            network_members.setdefault(network_root, []).append(terminal["id"])
        terminal_rows.append({
            **terminal,
            **parent_by_terminal.get(terminal["id"], {
                "component_id": None, "component_name": None, "parent_source": "not_extracted"
            }),
            "label": label,
            "label_distance": label_distance,
            "segment_component_ids": touched,
            "network_root": network_root,
        })
    terminal_by_id = {terminal["id"]: terminal for terminal in terminal_rows}

    networks = []
    connections = []
    for member_ids in network_members.values():
        if len(member_ids) < 2:
            continue
        members = [terminal_by_id[terminal_id] for terminal_id in member_ids]
        component_ids_in_network = sorted({
            member["component_id"] for member in members if member.get("component_id")
        })
        network_id = f"net-{len(networks) + 1}"
        for member in members:
            member["network_id"] = network_id
        cross_component = len(component_ids_in_network) >= 2
        topology_state = (
            "confirmed_pair" if cross_component and len(members) == 2
            else "multi_terminal_review" if cross_component
            else "internal_or_unbound"
        )
        endpoint_rows = [{
            "terminal_id": member["id"],
            "component_id": member.get("component_id"),
            "component": member.get("component_name"),
            "terminal": member.get("label"),
        } for member in members]
        network = {
            "id": network_id,
            "terminal_ids": member_ids,
            "component_ids": component_ids_in_network,
            "endpoints": endpoint_rows,
            "topology_state": topology_state,
        }
        networks.append(network)
        if cross_component:
            connections.append({
                "id": f"connection-{len(connections) + 1}",
                "network_id": network_id,
                "endpoints": endpoint_rows,
                "component_ids": component_ids_in_network,
                "cable_type": None,
                "cable_state": "not_extracted",
                "topology_state": topology_state,
            })

    cross_terminal_ids = {
        endpoint["terminal_id"] for connection in connections for endpoint in connection["endpoints"]
    }
    cross_terminals = [terminal_by_id[terminal_id] for terminal_id in cross_terminal_ids]
    return {
        "terminals": terminal_rows,
        "parent_regions": regions,
        "networks": networks,
        "connections": connections,
        "validation": {
            "terminal_half_shapes": sum(len(item.get("drawing_indexes") or []) for item in terminals),
            "terminal_anchors": len(terminals),
            "terminals_parent_bound": sum(1 for item in terminals if item["id"] in parent_by_terminal),
            "terminals_label_bound": sum(1 for item in terminal_rows if item.get("label")),
            "wire_segments_raw": len(raw_segments),
            "wire_segments": len(segments),
            "terminals_attached": sum(1 for item in terminal_rows if item["segment_component_ids"]),
            "terminal_networks": len(networks),
            "terminals_in_networks": sum(len(network["terminal_ids"]) for network in networks),
            "cross_component_networks": len(connections),
            "confirmed_pair_connections": sum(
                1 for item in connections if item["topology_state"] == "confirmed_pair"
            ),
            "multi_terminal_networks": sum(
                1 for item in connections if item["topology_state"] == "multi_terminal_review"
            ),
            "cross_endpoint_parent_rate": round(
                sum(1 for item in cross_terminals if item.get("component_id")) / max(len(cross_terminals), 1), 3
            ),
            "cross_endpoint_label_rate": round(
                sum(1 for item in cross_terminals if item.get("label")) / max(len(cross_terminals), 1), 3
            ),
            "path_state": "semantic_partial" if connections else "geometry_only",
        },
    }


def build_segment_components(segments: list[dict], *, tolerance: float = 0.6) -> dict:
    """Связные компоненты по совпадающим концам и T-соединениям.

    Чистый X-crossing не соединяется: связь появляется, только если конец одного
    сегмента лежит на другом. Это соответствует электрическим схемам без junction-dot.
    """
    node_by_grid = {}
    node_points = []

    def node_id(point):
        key = (round(point[0] / tolerance), round(point[1] / tolerance))
        if key not in node_by_grid:
            node_by_grid[key] = len(node_points)
            node_points.append(point)
        return node_by_grid[key]

    segment_nodes = []
    for segment in segments:
        a, b = node_id(segment["p1"]), node_id(segment["p2"])
        segment_nodes.append((a, b))
    parent = list(range(len(node_points)))

    def find(node):
        while parent[node] != node:
            parent[node] = parent[parent[node]]
            node = parent[node]
        return node

    def union(left, right):
        left, right = find(left), find(right)
        if left != right:
            parent[right] = left

    for left, right in segment_nodes:
        union(left, right)

    # T-junction: endpoint касается внутренности другого отрезка. O(N²) приемлемо
    # для уже профильтрованного wire-корпуса (~1k сегментов), но не для сырого CAD.
    for node, point in enumerate(node_points):
        for index, segment in enumerate(segments):
            a, b = segment_nodes[index]
            if node in (a, b):
                continue
            if point_segment_distance(point, segment) <= tolerance:
                x1, y1 = segment["p1"]
                x2, y2 = segment["p2"]
                dx, dy = x2 - x1, y2 - y1
                denom = dx * dx + dy * dy
                projection = ((point[0] - x1) * dx + (point[1] - y1) * dy) / denom if denom else 0
                if tolerance / max(segment["length"], tolerance) < projection < 1 - tolerance / max(segment["length"], tolerance):
                    union(node, a)
                    union(node, b)

    components = {}
    for index, (left, right) in enumerate(segment_nodes):
        root = find(left)
        slot = components.setdefault(root, {"segment_indexes": [], "node_indexes": set()})
        slot["segment_indexes"].append(index)
        slot["node_indexes"].update((left, right))
    normalized = []
    component_by_node = {}
    for component_index, slot in enumerate(components.values(), start=1):
        cid = f"component-{component_index}"
        for node in slot["node_indexes"]:
            component_by_node[node] = cid
        normalized.append({
            "id": cid,
            "segment_indexes": slot["segment_indexes"],
            "node_indexes": sorted(slot["node_indexes"]),
        })
    return {
        "nodes": node_points,
        "segment_nodes": segment_nodes,
        "components": normalized,
        "component_by_node": component_by_node,
        "find_root": find,
    }


def attach_anchors_to_components(
    anchors: list[dict],
    component_graph: dict,
    *,
    max_distance: float = 6.0,
) -> list[dict]:
    """Привязать центр якоря к ближайшему узлу сегментного графа."""
    nodes = component_graph.get("nodes") or []
    attached = []
    for anchor in anchors:
        if not nodes:
            attached.append({**anchor, "component_id": None, "distance": None})
            continue
        distance, node = min(
            (math.hypot(point[0] - anchor["center"][0], point[1] - anchor["center"][1]), index)
            for index, point in enumerate(nodes)
        )
        component_id = component_graph["component_by_node"].get(node) if distance <= max_distance else None
        attached.append({**anchor, "component_id": component_id, "distance": round(distance, 3)})
    return attached


def terminal_network_diagnostics(page, *, bbox_norm=None) -> dict:
    """Метрики готовности клеммной схемы к построению рёбер."""
    drawings = _drawings_for_bbox(page, bbox_norm)
    terminals = detect_terminal_ellipses(drawings)
    wire_segments = flatten_line_segments(drawings, color_mode="black", stroke_only=True, min_length=1.0)
    graph = build_segment_components(wire_segments, tolerance=0.6)
    attached = attach_anchors_to_components(terminals, graph, max_distance=6.0)
    counts = Counter(item["component_id"] for item in attached if item.get("component_id"))
    network_ids = {component_id for component_id, count in counts.items() if count >= 2}
    return {
        "terminal_anchors": len(terminals),
        "wire_segments": len(wire_segments),
        "terminals_attached": sum(1 for item in attached if item.get("component_id")),
        "terminal_networks": len(network_ids),
        "terminals_in_networks": sum(count for component_id, count in counts.items() if component_id in network_ids),
        "largest_terminal_network": max(counts.values(), default=0),
        "path_state": "geometry_ready_labels_pending",
    }


def _distance_to_bbox(point, bbox) -> float:
    x, y = point
    x0, y0, x1, y1 = bbox
    return math.hypot(max(x0 - x, 0, x - x1), max(y0 - y, 0, y - y1))


def extract_callout_leaders(page, *, bbox_norm=None) -> list[dict]:
    """Связать текстовые выноски лотков/гильз с концами и цветом геометрии."""
    bbox = _absolute_bbox(page, bbox_norm)
    callout_blocks = []
    for block in page.get_text("blocks"):
        text = " ".join(str(block[4]).split())
        center = ((float(block[0]) + float(block[2])) / 2, (float(block[1]) + float(block[3])) / 2)
        in_bbox = bbox is None or (bbox[0] <= center[0] <= bbox[2] and bbox[1] <= center[1] <= bbox[3])
        if in_bbox and text.startswith(("Лестничный лоток", "Листовой лоток", "Гильзы в перекрытии")):
            callout_blocks.append({"text": text, "bbox": tuple(float(v) for v in block[:4])})

    drawings = _drawings_for_bbox(page, bbox_norm)
    neutral = flatten_line_segments(drawings, color_mode="neutral", stroke_only=True, min_length=5.0)
    # Выноска обязательно диагональна; ортогональные рамки/оси отбрасываем.
    neutral = [segment for segment in neutral
               if abs(segment["p1"][0] - segment["p2"][0]) > 0.3
               and abs(segment["p1"][1] - segment["p2"][1]) > 0.3]
    vivid = flatten_line_segments(drawings, color_mode="vivid", stroke_only=False, min_length=0.1)

    result = []
    for index, callout in enumerate(callout_blocks, start=1):
        leaders = []
        for segment in neutral:
            for label_end, target in ((segment["p1"], segment["p2"]), (segment["p2"], segment["p1"])):
                if _distance_to_bbox(label_end, callout["bbox"]) >= 8.0:
                    continue
                if not vivid:
                    nearest_distance, nearest = None, None
                else:
                    nearest_distance, nearest = min(
                        ((point_segment_distance(target, candidate), candidate) for candidate in vivid),
                        key=lambda item: item[0],
                    )
                leaders.append({
                    "label_end": tuple(round(v, 3) for v in label_end),
                    "target": tuple(round(v, 3) for v in target),
                    "target_color": nearest.get("color") if nearest else None,
                    "target_distance": round(nearest_distance, 3) if nearest_distance is not None else None,
                    "geometry_linked": nearest_distance is not None and nearest_distance <= 7.0,
                })
        result.append({"id": f"callout-{index}", **callout, "leaders": leaders})
    return result
