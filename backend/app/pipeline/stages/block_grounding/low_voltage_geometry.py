"""Детерминированный профиль СС из векторного слоя PDF.

Подпрофили одной дисциплины намеренно разделены по геометрической грамматике:

* ``aps_structural`` — ПО → АЛС → этаж → адресное устройство;
* ``tray_axonometry`` — точный инвентарь лотков/гильз, топология visual_unverified;
* ``terminal_wiring`` — компоненты/клеммы и подтверждённые внешние path-связи;
  неоднозначные многоклеммные сети остаются на проверке.

Никакого OCR/LLM. Все неполные связи явно маркируются, а не достраиваются догадкой.
"""
from __future__ import annotations

import collections
import re
from pathlib import Path
from typing import Optional

from .profiled_graph_localization import ru_state, ru_subtype


PROFILE_ID = "low_voltage_scheme"

_ADDRESS_RE = re.compile(
    r"^(?P<controller>\d+)(?P<family>[A-Za-zА-Яа-я]+)(?P<loop>\d+)\."
    r"(?P<start>\d+)(?:\.{2,3}(?P<end>\d+))?(?:\((?P<logical>\d+)\))?$"
)
_DEVICE_TYPE_RE = re.compile(
    r"^(?:МДУ-1С|АМ-4|ИЗ|РМ-4|РМ-1С|2ДППК|BGB|ШУН/В|ШУ|"
    r"Затвор\+сигнализатор|СКУД)$",
    re.IGNORECASE,
)
_ZONE_RE = re.compile(r"^(?:Вж|Вкр|Пкр|ПД|ДУ|ВЕлш)\d[\w.]*$", re.IGNORECASE)
_AUX_RE = re.compile(r"^(?:Д|2ДППК|BGB|Затвор\+сигнализатор|СКУД|упр\.|лифтом)$", re.IGNORECASE)
_ALS_RE = re.compile(r"АЛС\s*(\d+)\s*[.]?\s*(\d+)", re.IGNORECASE)

_TRAY_RE = re.compile(
    r"(?P<kind>Лестничный|Листовой)\s+лоток\s+(?P<system>СБ|СПЗ|СЛЗ)\s*"
    r"(?P<partition>с\s+перегородкой\s*)?"
    r"(?P<width>\d+)\s*[хx]\s*(?P<height>\d+)\s*,?\s*"
    r"L\s*=\s*(?P<length>[\d.,]+)\s*м",
    re.IGNORECASE | re.DOTALL,
)
_SLEEVE_RE = re.compile(
    r"Гильзы\s+в\s+перекрытии\s*(?P<count>\d+)\s*шт\.?\s*"
    r"[∅⌀Ø]\s*(?P<diameter>\d+)\s*,?\s*L\s*=\s*(?P<length>\d+)\s*мм",
    re.IGNORECASE | re.DOTALL,
)

_COMPONENT_PATTERNS = (
    r"STR-1AP-M", r"STR20-1AP-IP-M", r"AA-07BD\s+SILVER", r"УК-2П",
    r"ИО\s*102-26", r"SA1", r"VD1", r"Кнопка\s+выход",
    r"Электромагнитный\s+замок", r"Вызывная\s+панель",
    r"Адресный\s+релейный\s+модуль", r"Устройство\s+дистанционного\s+пуска",
    r"Монтажная\s+коробка",
)
_COMPONENT_SEARCH_NAMES = (
    "STR-1AP-M", "STR20-1AP-IP-M", "AA-07BD SILVER", "УК-2П",
    "ИО 102-26", "SA1", "VD1", "Кнопка выход", "Электромагнитный замок",
    "Вызывная панель", "Адресный релейный модуль",
    "Устройство дистанционного пуска", "Монтажная коробка",
)
_CABLE_RE = re.compile(
    r"(?:КСПВПнг\(А\)-HF\s+\d[хx]\d[хx][\d,]+|"
    r"U/UTP\s+Cat5e\s+ZH\s+нг\(А\)-HF\s+\d[хx]\d[хx][\d,]+)",
    re.IGNORECASE,
)


def _cx(word) -> float:
    return (float(word[0]) + float(word[2])) / 2.0


def _cy(word) -> float:
    return (float(word[1]) + float(word[3])) / 2.0


def classify_low_voltage_subtype(vector_text: str) -> Optional[str]:
    """Определить геометрическую грамматику блока СС только по вектор-тексту."""
    text = vector_text or ""
    addresses = sum(1 for token in re.split(r"\s+", text) if _ADDRESS_RE.fullmatch(token))
    if addresses >= 10 and "АЛС" in text:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        floor_markers = []
        for index, line in enumerate(lines):
            if re.fullmatch(r"\d+\s*этаж(?:а|ей|и)?", line, re.IGNORECASE):
                floor_markers.append(line)
            elif line.lower() == "этаж" and index > 0 and re.fullmatch(r"\d+", lines[index - 1]):
                floor_markers.append(lines[index - 1] + " этаж")
            elif re.fullmatch(r"Кровл(?:я|е)", line, re.IGNORECASE):
                floor_markers.append("Кровля")
        # Два и более уровня = самостоятельная этажная структурная схема. Один/ноль —
        # план, узел или продолжение, где адреса/АЛС точны, но этажную иерархию строить нельзя.
        return "aps_structural" if len(floor_markers) >= 2 else "aps_fragment"
    if len(_TRAY_RE.findall(text)) + len(_SLEEVE_RE.findall(text)) >= 3:
        return "tray_axonometry"
    terminal_signals = sum(text.count(x) for x in ("READER", "RS-485", "DOOR", "EXIT", "SENS"))
    if terminal_signals >= 4 or ("Схема электрических подключений" in text and _CABLE_RE.search(text)):
        return "terminal_wiring"
    return None


def _distinct_tokens(text: str) -> list[str]:
    tokens = []
    patterns = (_ADDRESS_RE,)
    for token in re.split(r"\s+", text or ""):
        if any(p.fullmatch(token) for p in patterns) or token.startswith(("АЛС", "STR-", "Узел")):
            if token not in tokens:
                tokens.append(token)
        if len(tokens) >= 10:
            break
    if len(tokens) < 3:
        for token in re.findall(r"(?:Лестничный|Листовой|Гильзы|КСПВПнг|RS-485)", text or ""):
            if token not in tokens:
                tokens.append(token)
    return tokens


def _find_page_index(doc, vector_text: str) -> Optional[int]:
    needles = _distinct_tokens(vector_text)
    if not needles:
        return 0 if doc.page_count == 1 else None
    best, hits = None, -1
    for index in range(doc.page_count):
        text = doc[index].get_text()
        score = sum(1 for token in needles if token in text)
        if score > hits:
            best, hits = index, score
    return best if hits >= max(1, len(needles) // 2) else None


def _source(pdf_path: Path, page_index: int, subtype: str) -> dict:
    filename = Path(pdf_path).name
    code = re.search(r"(\d{2}АВ-РД-[А-Яа-яA-Za-z0-9.\-]+)", filename)
    return {
        "pdf_file": filename,
        "page_index": page_index,
        "section": code.group(1).rstrip(".") if code else None,
        "subtype": subtype,
    }


def _extract_floors(words) -> list[dict]:
    floors = []
    for word in words:
        token = str(word[4])
        if token == "этаж":
            numbers = [
                candidate for candidate in words
                if re.fullmatch(r"\d+", str(candidate[4]))
                and abs(_cx(candidate) - _cx(word)) < 14
                and 0 < _cy(candidate) - _cy(word) < 32
            ]
            if numbers:
                number = int(min(numbers, key=lambda w: _cy(w) - _cy(word))[4])
                floors.append({"id": str(number), "floor": number, "y": round(_cy(word), 2)})
        elif re.search(r"Кровл", token, re.IGNORECASE):
            floors.append({"id": "Кровля", "floor": "Кровля", "y": round(_cy(word), 2)})
    unique = {str(row["id"]): row for row in floors}
    return sorted(unique.values(), key=lambda row: row["y"])


def _extract_rooms(words, floors: list[dict]) -> list[dict]:
    rooms = []
    if not floors:
        return rooms
    for word in words:
        if str(word[4]).lower() != "пом.":
            continue
        numbers = [
            candidate for candidate in words
            if str(candidate[4]).startswith("№")
            and abs(_cy(candidate) - _cy(word)) < 5
            and 0 < _cx(candidate) - _cx(word) < 100
        ]
        if not numbers:
            continue
        number = min(numbers, key=lambda w: _cx(w))
        floor = min(floors, key=lambda row: abs(row["y"] - _cy(word)))
        rooms.append({
            "name": f"пом. {number[4]}",
            "floor": floor["floor"],
            "x": round(_cx(word), 2),
            "y": round(_cy(word), 2),
        })
    return rooms


def _nearest_label(words, address_word, pattern, *, dx: float, dy: float) -> Optional[str]:
    ax, ay = _cx(address_word), _cy(address_word)
    candidates = []
    for word in words:
        token = str(word[4])
        if not pattern.fullmatch(token):
            continue
        wx, wy = _cx(word), _cy(word)
        if abs(wx - ax) < dx and 3 < abs(wy - ay) < dy:
            candidates.append((abs(wx - ax) + 1.5 * abs(wy - ay), token))
    return min(candidates)[1] if candidates else None


def _family_device_type(family: str) -> Optional[str]:
    """Семейство в адресе само является типовым идентификатором устройства.

    Точную расшифровку производителя не выдумываем. Для ``A`` тип обязан лежать
    отдельной подписью (МДУ/АМ/ИЗ); BTH/BTM/BGB/SC/... уже различают семейства узлов.
    """
    family = (family or "").upper()
    if not family or family == "A":
        return None
    return f"адресное устройство {family}"


def _near_attributes(words, address_word) -> list[str]:
    ax, ay = _cx(address_word), _cy(address_word)
    found = []
    for word in words:
        token = str(word[4])
        if (_ZONE_RE.fullmatch(token) or _AUX_RE.fullmatch(token)):
            if abs(_cx(word) - ax) < 34 and abs(_cy(word) - ay) < 48:
                if token not in found:
                    found.append(token)
    return found


def _device_bbox(address_word, words, page_w: float, page_h: float) -> list[float]:
    ax, ay = _cx(address_word), _cy(address_word)
    nearby = [
        word for word in words
        if abs(_cx(word) - ax) < 34 and abs(_cy(word) - ay) < 48
    ] or [address_word]
    return [
        round(min(float(w[0]) for w in nearby) / page_w, 5),
        round(min(float(w[1]) for w in nearby) / page_h, 5),
        round(max(float(w[2]) for w in nearby) / page_w, 5),
        round(max(float(w[3]) for w in nearby) / page_h, 5),
    ]


def _extract_root(words) -> Optional[str]:
    po = [word for word in words if str(word[4]) == "ПО"]
    for word in po:
        numbers = [
            candidate for candidate in words
            if re.fullmatch(r"№\d+", str(candidate[4]))
            and abs(_cx(candidate) - _cx(word)) < 24
            and abs(_cy(candidate) - _cy(word)) < 28
        ]
        if numbers:
            return f"ПО {min(numbers, key=lambda w: abs(_cy(w) - _cy(word)))[4]}"
    return "ПО" if po else None


def _build_aps_graph(
    pdf_path: Path,
    page_index: int,
    words,
    page_w: float,
    page_h: float,
    *,
    subtype: str = "aps_structural",
) -> dict:
    floors = _extract_floors(words)
    rooms = _extract_rooms(words, floors)
    root = _extract_root(words)
    devices = []
    labels = collections.Counter()

    for index, word in enumerate(w for w in words if _ADDRESS_RE.fullmatch(str(w[4]))):
        match = _ADDRESS_RE.fullmatch(str(word[4]))
        tag_start = int(match.group("start"))
        tag_end = int(match.group("end") or tag_start)
        logical = int(match.group("logical")) if match.group("logical") else None
        # В части альбомов тег имеет вид ``1BTH1.165(107)``: число после точки —
        # позиционное обозначение устройства, в скобках — адрес в шлейфе. Для контроля
        # ёмкости/непрерывности используем именно логический адрес; оба значения сохраняем.
        start = logical if logical is not None else tag_start
        end = logical if logical is not None else tag_end
        loop_id = f"АЛС{match.group('controller')}.{match.group('loop')}"
        floor = min(floors, key=lambda row: (abs(row["y"] - _cy(word)), row["y"])) if floors else None
        same_floor_rooms = [r for r in rooms if floor and r["floor"] == floor["floor"]]
        room = min(same_floor_rooms, key=lambda row: abs(row["x"] - _cx(word))) if same_floor_rooms else None
        nearby_type = _nearest_label(words, word, _DEVICE_TYPE_RE, dx=31, dy=28)
        family_type = _family_device_type(match.group("family"))
        device_type = nearby_type or family_type
        attributes = _near_attributes(words, word)
        zone = next((value for value in attributes if _ZONE_RE.fullmatch(value)), None)
        labels[str(word[4])] += 1
        devices.append({
            "id": f"device-{index + 1}",
            "address": str(word[4]),
            "address_family": match.group("family"),
            "tag_start": tag_start,
            "tag_end": tag_end,
            "logical_address": logical,
            "address_start": start,
            "address_end": end,
            "address_slots": list(range(start, end + 1)),
            "loop": loop_id,
            "floor": floor["floor"] if floor else None,
            "room": room["name"] if room else None,
            "device_type": device_type,
            "device_type_source": "nearby_label" if nearby_type else (
                "address_family" if family_type else "not_extracted"
            ),
            "zone": zone,
            "attributes": attributes,
            "x": round(_cx(word), 2),
            "y": round(_cy(word), 2),
            "bbox_page": _device_bbox(word, words, page_w, page_h),
            "status": "present" if device_type and floor else "requires_review",
        })

    loop_labels = {
        f"АЛС{m.group(1)}.{m.group(2)}"
        for m in _ALS_RE.finditer(" ".join(str(w[4]) for w in words).replace("\n", " "))
    }
    loop_labels.update(device["loop"] for device in devices)
    loops = []
    for loop_id in sorted(loop_labels, key=lambda s: [int(x) for x in re.findall(r"\d+", s)]):
        members = [device for device in devices if device["loop"] == loop_id]
        slots = sorted({slot for device in members for slot in device["address_slots"]})
        raw_gaps = [n for n in range(min(slots), max(slots) + 1) if n not in slots] if slots else []
        # ``1BTH1.165(107)`` и ``1A1.168`` на одном АЛС используют одновременно
        # логический адрес в скобках и позиционный номер после точки. Их числовые шкалы
        # нельзя смешивать в один continuity-check — это породило бы сотни ложных дыр.
        has_logical = any(device.get("logical_address") is not None for device in members)
        has_direct = any(device.get("logical_address") is None for device in members)
        mixed_addressing = has_logical and has_direct
        gaps = [] if mixed_addressing else raw_gaps
        loop_floors = []
        for floor in floors:
            count = sum(1 for device in members if device["floor"] == floor["floor"])
            if count:
                loop_floors.append({"floor": floor["floor"], "address_points": count})
        loops.append({
            "id": loop_id,
            "root": root,
            "address_points": len(members),
            "address_slots": len(slots),
            "slot_min": min(slots) if slots else None,
            "slot_max": max(slots) if slots else None,
            "scope_gaps": gaps,
            "address_continuity_state": (
                "not_comparable_mixed_addressing" if mixed_addressing else "comparable_in_fragment"
            ),
            "floors": loop_floors,
        })

    floor_rows = []
    for floor in floors:
        members = [device for device in devices if device["floor"] == floor["floor"]]
        floor_rows.append({
            **floor,
            "address_points": len(members),
            "loops": sorted({device["loop"] for device in members}),
            "rooms": [room["name"] for room in rooms if room["floor"] == floor["floor"]],
        })

    edges = []
    for loop in loops:
        if root:
            edges.append({"from": root, "to": loop["id"], "type": "contains_loop", "state": "present"})
        for row in loop["floors"]:
            floor_id = f"этаж:{row['floor']}"
            edges.append({"from": loop["id"], "to": floor_id, "type": "serves_floor", "state": "present"})
    for device in devices:
        if device["floor"] is not None:
            edges.append({
                "from": f"этаж:{device['floor']}",
                "to": device["id"],
                "type": "contains_device",
                "state": "present",
            })

    duplicate_labels = sorted(label for label, count in labels.items() if count > 1)
    floor_bound = sum(1 for device in devices if device["floor"] is not None)
    type_bound = sum(1 for device in devices if device["device_type"] is not None)
    warnings = []
    if duplicate_labels:
        warnings.append("повтор адресов: " + ", ".join(duplicate_labels))
    for loop in loops:
        if loop["scope_gaps"]:
            warnings.append(
                f"{loop['id']}: разрывы адресного диапазона {loop['scope_gaps']} "
                "(граница фрагмента возможна; не считать дефектом без кросс-листовой проверки)"
            )
    status = "ok" if not duplicate_labels and floor_bound == len(devices) and type_bound == len(devices) else "needs_review"
    return {
        "profile_id": PROFILE_ID,
        "subtype": subtype,
        "source": _source(pdf_path, page_index, subtype),
        "root": root,
        "loops": loops,
        "floors": floor_rows,
        "devices": devices,
        "edges": edges,
        "validation": {
            "address_points_total": len(devices),
            "address_labels_unique": len(labels),
            "address_slots_total": len({(d["loop"], slot) for d in devices for slot in d["address_slots"]}),
            "devices_type_bound": type_bound,
            "devices_floor_bound": floor_bound,
            "loops_total": len(loops),
            "floors_total": len(floors),
            "duplicate_address_labels": duplicate_labels,
            "hierarchy_complete": bool(root and loops and floor_bound == len(devices)),
            "physical_loop_path_state": "visual_unverified",
        },
        "warnings": warnings,
        "status": status,
    }


def _build_tray_graph(pdf_path: Path, page_index: int, text: str, *, page=None, bbox_norm=None) -> dict:
    normalized = re.sub(r"\s+", " ", text or "").strip()
    elements = []
    for index, match in enumerate(_TRAY_RE.finditer(normalized), start=1):
        elements.append({
            "id": f"tray-{index}",
            "kind": "tray",
            "tray_type": match.group("kind").capitalize(),
            "system": match.group("system").upper(),
            "partition": bool(match.group("partition")),
            "width_mm": int(match.group("width")),
            "height_mm": int(match.group("height")),
            "length_m": float(match.group("length").replace(",", ".")),
            "field_state": "present",
        })
    for index, match in enumerate(_SLEEVE_RE.finditer(normalized), start=1):
        elements.append({
            "id": f"sleeve-{index}",
            "kind": "sleeves",
            "count": int(match.group("count")),
            "diameter_mm": int(match.group("diameter")),
            "length_mm": int(match.group("length")),
            "field_state": "present",
        })
    callouts = []
    if page is not None:
        try:
            from backend.app.pipeline.stages.block_grounding.vector_path_graph import (
                extract_callout_leaders,
            )

            callouts = extract_callout_leaders(page, bbox_norm=bbox_norm)
        except Exception:
            callouts = []
    for element, callout in zip(elements, callouts):
        leaders = callout.get("leaders") or []
        element["geometry_targets"] = [leader.get("target") for leader in leaders]
        element["geometry_colors"] = sorted({str(leader.get("target_color")) for leader in leaders})
        element["geometry_state"] = (
            "present" if leaders and all(leader.get("geometry_linked") for leader in leaders)
            else "visual_unverified"
        )
    leader_total = sum(len(callout.get("leaders") or []) for callout in callouts)
    leader_linked = sum(
        1 for callout in callouts for leader in (callout.get("leaders") or [])
        if leader.get("geometry_linked")
    )
    geometry_elements = sum(1 for element in elements if element.get("geometry_state") == "present")
    title = next((line.strip() for line in (text or "").splitlines() if line.strip().startswith("Узел")), None)
    return {
        "profile_id": PROFILE_ID,
        "subtype": "tray_axonometry",
        "source": _source(pdf_path, page_index, "tray_axonometry"),
        "title": title,
        "elements": elements,
        "connections": [],
        "validation": {
            "elements_total": len(elements),
            "trays_total": sum(1 for item in elements if item["kind"] == "tray"),
            "sleeve_callouts_total": sum(1 for item in elements if item["kind"] == "sleeves"),
            "inventory_state": "present" if elements else "not_extracted",
            "topology_state": "visual_unverified",
            "callouts_total": len(callouts),
            "leader_targets_total": leader_total,
            "leader_targets_linked": leader_linked,
            "elements_geometry_linked": geometry_elements,
            "callout_link_rate": round(leader_linked / max(leader_total, 1), 3),
        },
        "warnings": ["связность цветных сегментов требует трассировки vector paths; инвентарь точный"],
        "status": "inventory_only" if elements else "not_extracted",
    }


def _bbox_gap(left, right) -> float:
    import math

    return math.hypot(
        max(float(left[0]) - float(right[2]), 0, float(right[0]) - float(left[2])),
        max(float(left[1]) - float(right[3]), 0, float(right[1]) - float(left[3])),
    )


def _position_terminal_components(page, components: list[dict], bbox_norm=None) -> list[dict]:
    """Добавить bbox к текстовым экземплярам компонентов, включая переносы строк."""
    by_name = collections.defaultdict(list)
    for component in components:
        by_name[component["name"].lower()].append(component)
    absolute_bbox = None
    if bbox_norm and len(bbox_norm) >= 4:
        absolute_bbox = [
            float(bbox_norm[0]) * float(page.rect.width),
            float(bbox_norm[1]) * float(page.rect.height),
            float(bbox_norm[2]) * float(page.rect.width),
            float(bbox_norm[3]) * float(page.rect.height),
        ]

    for search_name in _COMPONENT_SEARCH_NAMES:
        targets = by_name.get(search_name.lower()) or []
        if not targets:
            continue
        rects = [list(map(float, rect)) for rect in page.search_for(search_name)]
        if absolute_bbox:
            rects = [rect for rect in rects if not (
                rect[2] < absolute_bbox[0] or rect[0] > absolute_bbox[2]
                or rect[3] < absolute_bbox[1] or rect[1] > absolute_bbox[3]
            )]
        clusters = []
        for rect in rects:
            cluster = next((item for item in clusters if _bbox_gap(item, rect) <= 20), None)
            if cluster is None:
                clusters.append(rect)
            else:
                cluster[:] = [
                    min(cluster[0], rect[0]), min(cluster[1], rect[1]),
                    max(cluster[2], rect[2]), max(cluster[3], rect[3]),
                ]
        clusters.sort(key=lambda rect: (rect[1], rect[0]))
        for component, bbox in zip(targets, clusters):
            component["bbox_abs"] = [round(value, 3) for value in bbox]
            component["bbox_page"] = [
                round(bbox[0] / float(page.rect.width), 5),
                round(bbox[1] / float(page.rect.height), 5),
                round(bbox[2] / float(page.rect.width), 5),
                round(bbox[3] / float(page.rect.height), 5),
            ]
            component["position_state"] = "present"
    for component in components:
        component.setdefault("position_state", "not_extracted")
    return components


def _build_terminal_graph(pdf_path: Path, page_index: int, text: str, *, page=None, bbox_norm=None) -> dict:
    normalized = re.sub(r"\s+", " ", text or "").strip()
    components = []
    for pattern in _COMPONENT_PATTERNS:
        for match in re.finditer(pattern, normalized, re.IGNORECASE):
            name = re.sub(r"\s+", " ", match.group(0)).strip()
            components.append({"id": f"component-{len(components) + 1}", "name": name, "field_state": "present"})
    if page is not None:
        components = _position_terminal_components(page, components, bbox_norm=bbox_norm)
    cables = sorted({
        re.sub(r"\s+", " ", match.group(0)).strip().replace("x", "х").replace("X", "х")
        for match in _CABLE_RE.finditer(normalized)
    })
    interfaces = sorted({
        token for token in ("READER 1", "READER 2", "RS-485", "SENS1", "SENS2", "LAN", "АЛС АПС")
        if token in normalized
    })
    topology = {}
    if page is not None:
        try:
            from backend.app.pipeline.stages.block_grounding.vector_path_graph import (
                build_terminal_wiring_topology,
            )

            component_labels = [
                {"id": item["id"], "name": item["name"], "bbox": item["bbox_abs"]}
                for item in components if item.get("bbox_abs")
            ]
            topology = build_terminal_wiring_topology(
                page, component_labels, bbox_norm=bbox_norm
            )
        except Exception:
            topology = {}
    path_validation = topology.get("validation") or {}
    connections = topology.get("connections") or []
    confirmed_connections = [
        item for item in connections if item.get("topology_state") == "confirmed_pair"
    ]
    warnings = []
    if connections:
        warnings.append(
            "кабельная марка ещё не назначена path-сетям; подтверждённые пары можно использовать без марки"
        )
    if any(item.get("topology_state") == "multi_terminal_review" for item in connections):
        warnings.append(
            "многоклеммные сети сохранены для проверки и не превращены в попарные рёбра"
        )
    if not connections:
        warnings.append("клеммные рёбра не извлечены из vector paths")
    return {
        "profile_id": PROFILE_ID,
        "subtype": "terminal_wiring",
        "source": _source(pdf_path, page_index, "terminal_wiring"),
        "components": components,
        "cables": cables,
        "interfaces": interfaces,
        "terminals": topology.get("terminals") or [],
        "networks": topology.get("networks") or [],
        "connections": connections,
        "confirmed_connections": confirmed_connections,
        "validation": {
            "components_total": len(components),
            "cable_types_total": len(cables),
            "interfaces_total": len(interfaces),
            "topology_state": "partial_confirmed" if confirmed_connections else "not_extracted",
            "required_next_layer": "cable-label binding + multi-terminal network disambiguation",
            **path_validation,
        },
        "warnings": warnings,
        "status": "partial_topology" if confirmed_connections else "requires_path_tracing",
    }


def build_low_voltage_graph(
    pdf_path: Path,
    vector_text: str,
    *,
    bbox_norm: Optional[list] = None,
    polygon_norm: Optional[list] = None,
) -> Optional[dict]:
    """Построить структурированное описание блока СС. Fail-soft: не СС → None."""
    subtype = classify_low_voltage_subtype(vector_text)
    if not subtype:
        return None
    try:
        import fitz

        doc = fitz.open(str(pdf_path))
    except Exception:
        return None
    try:
        page_index = _find_page_index(doc, vector_text)
        if page_index is None:
            return None
        page = doc[page_index]
        if subtype in ("aps_structural", "aps_fragment"):
            words = page.get_text("words")
            if polygon_norm or bbox_norm:
                # Общий клип Вектографа: в боевых result.json блок является частью
                # полной страницы, и соседняя схема не должна протекать в его граф.
                from backend.app.pipeline.stages.block_grounding.singleline_graph_geometry import (
                    _clip_words_to_bbox,
                    _clip_words_to_polygon,
                )

                if polygon_norm:
                    words = _clip_words_to_polygon(
                        words, polygon_norm, float(page.rect.width), float(page.rect.height)
                    )
                elif bbox_norm:
                    words = _clip_words_to_bbox(
                        words, bbox_norm, float(page.rect.width), float(page.rect.height)
                    )
            return _build_aps_graph(
                Path(pdf_path), page_index, words,
                float(page.rect.width), float(page.rect.height),
                subtype=subtype,
            )
        if subtype == "tray_axonometry":
            return _build_tray_graph(
                Path(pdf_path), page_index, vector_text, page=page, bbox_norm=bbox_norm
            )
        return _build_terminal_graph(
            Path(pdf_path), page_index, vector_text, page=page, bbox_norm=bbox_norm
        )
    except Exception:
        return None
    finally:
        doc.close()


def evaluate_low_voltage_gate(graph: Optional[dict]) -> dict:
    """Гейт пригодности по режимам: полный граф, инвентарь или подтверждённые рёбра."""
    if not graph:
        return {"use": False, "reasons": ["структура СС не построена"], "warnings": [], "metrics": {}}
    subtype = graph.get("subtype")
    validation = graph.get("validation") or {}
    reasons = []
    warnings = list(graph.get("warnings") or [])
    if subtype == "aps_structural":
        total = validation.get("address_points_total") or 0
        floor_rate = (validation.get("devices_floor_bound") or 0) / max(total, 1)
        type_rate = (validation.get("devices_type_bound") or 0) / max(total, 1)
        if total < 10:
            reasons.append(f"мало адресных точек ({total} < 10)")
        if floor_rate < 0.95:
            reasons.append(f"привязка к этажам {floor_rate:.0%} < 95%")
        if type_rate < 0.90:
            reasons.append(f"привязка типов {type_rate:.0%} < 90%")
        metrics = {"address_points": total, "floor_rate": round(floor_rate, 3),
                   "type_rate": round(type_rate, 3), "hierarchy_complete": validation.get("hierarchy_complete")}
        return {"use": not reasons, "mode": "hierarchy", "reasons": reasons,
                "warnings": warnings, "metrics": metrics}
    if subtype == "aps_fragment":
        total = validation.get("address_points_total") or 0
        type_rate = (validation.get("devices_type_bound") or 0) / max(total, 1)
        if total < 10:
            reasons.append(f"мало адресных точек ({total} < 10)")
        warnings.append("фрагмент без собственной этажной оси: доступен адресный инвентарь, не этажная иерархия")
        return {"use": not reasons, "mode": "address_inventory", "reasons": reasons,
                "warnings": warnings, "metrics": {"address_points": total,
                                                     "type_rate": round(type_rate, 3),
                                                     "hierarchy_complete": False}}
    if subtype == "tray_axonometry":
        total = validation.get("elements_total") or 0
        if total < 3:
            reasons.append(f"мало элементов ({total} < 3)")
        return {"use": not reasons, "mode": "inventory_only", "reasons": reasons,
                "warnings": warnings, "metrics": {"elements_total": total, "topology_complete": False}}
    confirmed = validation.get("confirmed_pair_connections") or 0
    parent_rate = validation.get("cross_endpoint_parent_rate") or 0
    label_rate = validation.get("cross_endpoint_label_rate") or 0
    if confirmed < 1:
        reasons.append("нет подтверждённых двухклеммных path-соединений")
    if parent_rate < 0.90:
        reasons.append(f"привязка внешних клемм к приборам {parent_rate:.0%} < 90%")
    if label_rate < 0.90:
        reasons.append(f"подписи внешних клемм {label_rate:.0%} < 90%")
    return {
        "use": not reasons,
        "mode": "confirmed_connections_only",
        "reasons": reasons,
        "warnings": warnings,
        "metrics": {
            "components_total": validation.get("components_total"),
            "confirmed_connections": confirmed,
            "multi_terminal_networks": validation.get("multi_terminal_networks"),
            "endpoint_parent_rate": parent_rate,
            "endpoint_label_rate": label_rate,
            "topology_complete": False,
        },
    }


def render_low_voltage_graph_markdown(graph: dict) -> str:
    """Человекочитаемое логическое описание трёх подпрофилей СС."""
    if not graph:
        return ""
    subtype = graph.get("subtype")
    source = graph.get("source") or {}
    lines = [f"# Эталонная текстовая разметка СС: {ru_subtype(subtype)}", "",
             f"**Источник:** {source.get('pdf_file')}", ""]
    if subtype in ("aps_structural", "aps_fragment"):
        v = graph.get("validation") or {}
        lines += [
            f"Корень: **{graph.get('root') or 'не извлечён'}**",
            f"Тип схемы: **{ru_subtype(subtype)}**. Адресных точек: {v.get('address_points_total')}; "
            f"шлейфов: {v.get('loops_total')}; "
            f"этажей: {v.get('floors_total')}; состояние: **{ru_state(graph.get('status'))}**.",
            "", "## Иерархия", "",
        ]
        for loop in graph.get("loops") or []:
            floor_text = ", ".join(f"{r['floor']} ({r['address_points']})" for r in loop["floors"])
            lines.append(f"- {graph.get('root')} → **{loop['id']}** → {floor_text}")
            if loop.get("scope_gaps"):
                lines.append(f"  - разрывы диапазона: {loop['scope_gaps']} (нужна кросс-листовая проверка)")
        lines += ["", "## Адресные устройства", "",
                  "| Адрес | АЛС | Этаж | Помещение | Тип | Зона/атрибуты |",
                  "| --- | --- | --- | --- | --- | --- |"]
        for device in graph.get("devices") or []:
            attrs = ", ".join(device.get("attributes") or []) or "—"
            lines.append(
                f"| {device['address']} | {device['loop']} | {device.get('floor') or '—'} | "
                f"{device.get('room') or '—'} | {device.get('device_type') or '—'} | {attrs} |"
            )
    elif subtype == "tray_axonometry":
        validation = graph.get("validation") or {}
        lines += [f"Состояние: **{ru_state(graph.get('status'))}** — инвентарь точный, связность пока не извлечена.",
                  f"Выноски → цветная геометрия: {validation.get('leader_targets_linked')}/"
                  f"{validation.get('leader_targets_total')} (доля привязки {validation.get('callout_link_rate')}).", "",
                  "| ID | Элемент | Система | Размер/количество | Длина |",
                  "| --- | --- | --- | --- | --- |"]
        for item in graph.get("elements") or []:
            if item["kind"] == "tray":
                name = item["tray_type"] + " лоток" + (" с перегородкой" if item["partition"] else "")
                size = f"{item['width_mm']}×{item['height_mm']} мм"
                length = f"{item['length_m']} м"
                system = item["system"]
            else:
                name = "Гильзы в перекрытии"
                size = f"{item['count']} шт., ∅{item['diameter_mm']} мм"
                length = f"{item['length_mm']} мм"
                system = "—"
            lines.append(f"| {item['id']} | {name} | {system} | {size} | {length} |")
    else:
        validation = graph.get("validation") or {}
        lines += [f"Состояние: **{ru_state(graph.get('status'))}**.", "",
                  "Компоненты: " + ", ".join(item["name"] for item in graph.get("components") or []),
                  "", "Кабели: " + ", ".join(graph.get("cables") or []), "",
                  f"Векторные трассы: физических клемм {validation.get('terminal_anchors')} "
                  f"(полуформ CAD: {validation.get('terminal_half_shapes')}), внешних сегментов "
                  f"{validation.get('wire_segments')}, сетей {validation.get('terminal_networks')}, "
                  f"межкомпонентных сетей {validation.get('cross_component_networks')}.", "",
                  f"Подтверждённых пар: {validation.get('confirmed_pair_connections')}; "
                  f"многоклеммных сетей на проверке: {validation.get('multi_terminal_networks')}.",
                  "", "## Клеммные связи", ""]
        for connection in graph.get("connections") or []:
            endpoints = " ↔ ".join(
                f"{item.get('component') or '?'}[{item.get('terminal') or '?'}]"
                for item in connection.get("endpoints") or []
            )
            lines.append(
                f"- {ru_state(connection.get('topology_state'))}: {endpoints}; кабель: "
                f"{connection.get('cable_type') or 'не назначен'}"
            )
    if graph.get("warnings"):
        lines += ["", "## Требует внимания", ""] + [f"- {warning}" for warning in graph["warnings"]]
    return "\n".join(lines) + "\n"
