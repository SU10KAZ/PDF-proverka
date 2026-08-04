"""Универсальный профиль «Условные обозначения» (легенда) для всех дисциплин.

Легенда — это не план и не узел: у неё нет осей, размерных цепочек и трасс.
Её содержание — таблица строк «код — графический образец — параметр — расшифровка»,
и именно эти связи нужны аудиту: по легенде проверяются марки стен, типы
трубопроводов, обозначения машиномест и т.п.

До появления этого профиля такие блоки наследовали профиль листа («план потолка
и освещения»), и вся расшифровка терялась: в описание уходили обрывки
(«материал — 5: газобетонных, бетонных») вместо связок «СН-1.2 → стена из
газобетонных блоков D600 → 250 мм».

Привязка детерминированная, по вектор-слою:
  * строки легенды задаёт колонка расшифровок (самый правый столбец длинных текстов);
  * код берётся из левых колонок по общей Y-полосе строки;
  * параметр (толщина, диаметр) привязывается к ближайшему графическому образцу
    НИЖЕ себя — размерная надпись в CAD ставится над образцом, который измеряет;
  * привязка параметра дополнительно сверяется с числом внутри расшифровки:
    совпало — provenance «геометрия и текст», не совпало — «только геометрия».
"""
from __future__ import annotations

import collections
import re
from pathlib import Path
from typing import Any, Optional

from .hvac_geometry import _base, _bbox, _bbox_norm, _center, _clip_copied_page, _lines

PROFILE_LEGEND = "legend"
PROFILE_RU = "Условные обозначения"

# Легенда должна быть ПРЕДМЕТОМ блока, а не его деталью: «Схема подвода воды
# с условными обозначениями» — это схема, а не легенда.
_SUBJECT_RE = re.compile(
    r"^(?:на\s+(?:фрагменте|чертеже|листе|изображении|схеме)\s+)?"
    r"(?:представлен[аыо]?\s+|приведен[аыо]?\s+|показан[аыо]?\s+|изображен[аыо]?\s+)?"
    r"(?:таблиц[аы]\s+|перечень\s+)?"
    r"(?:легенд|услов\w+\s+(?:график\w+\s+)?обозначен)",
    re.I,
)
_TITLE_RE = re.compile(
    r"(услов\w*\s+(?:график\w*\s+)?обозначен|^\s*легенда\b|принят\w*\s+обозначен|"
    r"перечен\w*\s+услов|обозначени\w*\s+на\s+чертеж)",
    re.I | re.M,
)
# Осевая марка «3.Б», «П.25» — верный признак того, что перед нами чертёж,
# а не самостоятельная легенда.
_AXIS_RE = re.compile(r"(?<![\d.])\d{1,2}\.[А-ЯA-Z]{1,2}(?![\w.])")
_CODE_RE = re.compile(
    r"^(?:[А-ЯA-Z]{1,4}\s*[-–—.]?\s*\d{1,3}(?:[.,]\d{1,3})*|"
    r"[А-ЯA-Z]{1,4}\s*\d{1,3}(?:[.,]\d{1,3})*|"
    r"[А-ЯA-Z]{1,3}\s*[/.]\s*[А-ЯA-Z]{1,3}|"
    r"[+\-]?\d{1,3}[.,]\d{3}|\d{1,4})$"
)
_VALUE_RE = re.compile(r"^[+\-]?\d{1,5}(?:[.,]\d{1,3})?$")
_DASH_RE = re.compile(r"^\s*[-–—]\s*")
# Код нередко стоит внутри самой расшифровки: «С-7, перегородка из газобетона…».
_INLINE_CODE_RE = re.compile(
    r"^([А-ЯA-Z]{1,4}\s*[-–—]\s*\d{1,3}(?:[.,]\d{1,3})*)\s*[,;:]\s*(\S.*)$")

_MAX_WORDS = 150          # легенда компактна; крупный блок — это чертёж с легендой в углу
_MAX_AXES = 1


def classify_legend_profile(text: str, *, description: str = "") -> Optional[str]:
    """`legend`, если блок целиком является легендой; иначе None."""
    body = str(text or "")
    desc = re.sub(r"\s+", " ", str(description or "")).strip()
    if desc.startswith("{"):        # старые описания-JSON не несут темы блока
        desc = ""
    first = re.split(r"(?<=[.!?])\s", desc)[0] if desc else ""
    subject = bool(_SUBJECT_RE.match(first))
    if not subject and not _TITLE_RE.search(body):
        return None
    words = {w for line in body.split("\n") for w in line.split() if w}
    if len(words) > _MAX_WORDS:
        return None
    if len(set(_AXIS_RE.findall(body))) > _MAX_AXES:
        return None
    if subject:
        return PROFILE_LEGEND
    # Без подтверждения от описания требуем каркас строк-расшифровок.
    rows = [ln for ln in body.split("\n") if _DASH_RE.match(ln) and len(ln.strip()) > 5]
    return PROFILE_LEGEND if len(rows) >= 5 else None


def _legend_lines(page) -> list[dict]:
    """Строки блока с координатами страницы.

    Спаны склеиваются без разделителя: пробел между спанами разорвал бы CAD-код
    («Т11.1» → «Т 11.1»), и код перестал бы совпадать с числом внутри расшифровки,
    на котором держится самопроверка привязки. Поворот листа снимать не нужно —
    `_bbox` уже приводит рамку к системе отображения страницы.
    """
    result = []
    for block in page.get_text("dict").get("blocks") or []:
        for line in block.get("lines") or []:
            spans = line.get("spans") or []
            text = re.sub(r"\s+", " ", "".join(str(s.get("text") or "") for s in spans)).strip()
            if not text:
                continue
            bbox = _bbox(page, line["bbox"])
            result.append({"text": text, "bbox": bbox, "center": _center(bbox),
                           "src": bbox})
    return result


def _cluster(values: list[float], gap: float) -> list[list[float]]:
    result: list[list[float]] = []
    for value in sorted(values):
        if result and value - result[-1][-1] <= gap:
            result[-1].append(value)
        else:
            result.append([value])
    return result


def _samples(page) -> list[dict]:
    """Графические образцы легенды — компактные штрихи и заливки в своей колонке."""
    out = []
    width, height = page.rect.width, page.rect.height
    for item in page.get_drawings():
        rect = item.get("rect")
        if rect is None:
            continue
        w, h = float(rect.width), float(rect.height)
        if w < width * 0.02 or w > width * 0.6 or h > height * 0.12:
            continue
        out.append({"x0": float(rect.x0), "x1": float(rect.x1),
                    "y0": float(rect.y0), "y1": float(rect.y1)})
    out.sort(key=lambda s: s["y0"])
    return out


def _panel_entries(lines: list[dict], samples: list[dict], lh: float) -> list[dict]:
    """Строки одной колонки-панели: расшифровки + коды слева + параметры."""
    desc_lines = sorted((ln for ln in lines if ln["role"] == "desc"),
                        key=lambda ln: ln["center"][1])
    left_lines = [ln for ln in lines if ln["role"] == "left"]
    entries: list[dict] = []
    for line in desc_lines:
        text = re.sub(r"\s+", " ", line["text"]).strip()
        if not text or (_TITLE_RE.search(text) and len(text) < 40):
            continue
        cy = line["center"][1]
        # Новая строка легенды: тире, собственный код слева или разрыв по вертикали.
        has_code = any(ln["bbox"][1] - lh * 0.6 <= line["bbox"][3]
                       and ln["bbox"][3] + lh * 0.6 >= line["bbox"][1]
                       and not _VALUE_RE.match(ln["text"].strip())
                       for ln in left_lines)
        inline = _INLINE_CODE_RE.match(text)
        gap_ok = not entries or cy - entries[-1]["y_last"] > lh * 1.6
        if _DASH_RE.match(text) or has_code or inline or gap_ok:
            body = _DASH_RE.sub("", text)
            codes = []
            if inline:
                codes.append(inline.group(1).strip())
                body = inline.group(2).strip()
            entries.append({
                "text": body, "x0": line["bbox"][0], "x1": line["bbox"][2],
                "y0": line["bbox"][1], "y1": line["bbox"][3],
                "y_last": cy, "bbox": list(line["bbox"]),
                "src": list(line["src"]), "codes": codes, "values": [],
            })
        else:                                  # перенос длинной расшифровки
            entries[-1]["text"] += " " + text
            entries[-1]["y1"] = line["bbox"][3]
            entries[-1]["y_last"] = cy
            entries[-1]["bbox"][3] = line["bbox"][3]
            entries[-1]["src"][1] = min(entries[-1]["src"][1], line["src"][1])
            entries[-1]["src"][3] = max(entries[-1]["src"][3], line["src"][3])

    leftovers = []
    for line in left_lines:
        text = re.sub(r"\s+", " ", line["text"]).strip()
        if not text or (_TITLE_RE.search(text) and len(text) < 40):
            continue
        cy = line["center"][1]
        band = next((e for e in entries if e["y0"] - lh * 0.6 <= cy <= e["y1"] + lh * 0.6), None)
        if not _VALUE_RE.match(text):
            if band is not None:
                band["codes"].append(text)
                continue
        else:
            # Размерная надпись стоит НАД образцом, который измеряет.
            below = [s for s in samples if s["y0"] >= line["bbox"][3] - lh * 0.3]
            target = None
            if below:
                sample = min(below, key=lambda s: s["y0"])
                mid = (sample["y0"] + sample["y1"]) / 2
                target = next((e for e in entries
                               if e["y0"] - lh * 1.2 <= mid <= e["y1"] + lh * 1.2), None)
            target = target or band
            if target is not None:
                confirmed = text.replace(",", ".") in target["text"].replace(",", ".")
                target["values"].append({"value": text, "text_confirmed": confirmed})
                continue
        leftovers.append({"text": text, "bbox": line["src"], "center": _center(line["src"])})
    for entry in entries:
        entry["leftovers"] = []
    if entries:
        entries[0]["leftovers"] = leftovers
    else:
        return [{"__leftovers__": leftovers}] if leftovers else []
    return entries


def _extract_entries(page) -> tuple[list[dict], list[dict]]:
    """Строки легенды и непривязанные надписи блока.

    Легенда часто раскладывается в несколько колонок-панелей на одном листе,
    поэтому колонка расшифровок ищется не одна: каждая самостоятельная колонка
    длинных текстов образует свою панель со своей зоной кодов слева.
    """
    lines = _legend_lines(page)
    # Заголовок «Условные обозначения» стоит над таблицей в своей колонке и
    # строкой легенды не является: оставленный в наборе, он на короткой легенде
    # перетягивает на себя роль колонки расшифровок и обнуляет разбор.
    lines = [ln for ln in lines
             if not (_TITLE_RE.search(ln["text"]) and len(ln["text"]) < 40)]
    if not lines:
        return [], []
    heights = sorted(ln["bbox"][3] - ln["bbox"][1] for ln in lines)
    lh = heights[len(heights) // 2] or 8.0
    page_width = page.rect.width

    columns = _cluster([ln["bbox"][0] for ln in lines], gap=max(lh * 1.5, 12.0))
    col_info = []
    for col in columns:
        lo, hi = col[0], col[-1]
        members = [ln for ln in lines if lo - 0.1 <= ln["bbox"][0] <= hi + 0.1]
        long_texts = [ln for ln in members if len(ln["text"]) >= 12]
        col_info.append({"lo": lo, "hi": hi, "members": members, "long": len(long_texts)})
    desc_cols = [c for c in col_info if c["long"] >= 2]
    if not desc_cols:
        desc_cols = [max(col_info, key=lambda c: c["long"])] if col_info else []
    if not desc_cols:
        return [], []

    all_samples = _samples(page)
    entries: list[dict] = []
    leftovers: list[dict] = []
    prev_edge = -1e9
    for index, col in enumerate(desc_cols):
        left_edge = col["lo"] - 0.1
        right_edge = (desc_cols[index + 1]["lo"] - 0.1
                      if index + 1 < len(desc_cols) else 1e9)
        panel = []
        for line in lines:
            x0, x1 = line["bbox"][0], line["bbox"][2]
            if col["lo"] - 0.1 <= x0 <= col["hi"] + 0.1 and len(line["text"]) >= 4:
                panel.append({**line, "role": "desc"})
            elif prev_edge <= x0 and x1 <= left_edge + 1.0:
                panel.append({**line, "role": "left"})
        samples = [s for s in all_samples if prev_edge <= s["x0"] and s["x1"] <= left_edge + 2.0]
        produced = _panel_entries(panel, samples, lh)
        for entry in produced:
            if "__leftovers__" in entry:
                leftovers.extend(entry["__leftovers__"])
                continue
            leftovers.extend(entry.pop("leftovers", []))
            entries.append(entry)
        prev_edge = right_edge
    entries.sort(key=lambda e: (e["x0"], e["y0"]))
    return entries, leftovers


def build_legend_graph(pdf_path: Path, *, block_id=None, subtype_hint=None):
    try:
        import fitz
        with fitz.open(str(pdf_path)) as doc:
            if doc.page_count != 1:
                return None
            return _graph(doc[0], Path(pdf_path), block_id, subtype_hint or "легенда")
    except Exception:
        return None


def build_legend_graph_from_source(pdf_path: Path, *, page_index, bbox_norm,
                                   polygon_norm=None, block_id=None, subtype_hint=None):
    try:
        import fitz
        source = fitz.open(str(pdf_path))
        cropped = None
        try:
            sp = source[page_index]
            w, h = sp.rect.width, sp.rect.height
            crop = fitz.Rect(bbox_norm[0] * w, bbox_norm[1] * h,
                             bbox_norm[2] * w, bbox_norm[3] * h) & sp.rect
            ur = crop * sp.derotation_matrix
            ur.normalize()
            off = sp.cropbox_position
            ur = fitz.Rect(ur.x0 + off.x, ur.y0 + off.y, ur.x1 + off.x, ur.y1 + off.y)
            cropped = fitz.open()
            cropped.insert_pdf(source, from_page=page_index, to_page=page_index)
            target = cropped[0]
            if polygon_norm:
                inv = ~sp.transformation_matrix
                pts = [tuple(fitz.Point(float(x) * w, float(y) * h)
                             * sp.derotation_matrix * inv) for x, y in polygon_norm]
                _clip_copied_page(target, pts)
            target.set_cropbox(ur)
            return _graph(target, Path(pdf_path), block_id, subtype_hint or "легенда")
        finally:
            if cropped is not None:
                cropped.close()
            source.close()
            fitz.TOOLS.store_shrink(100)
    except Exception:
        return None


def _graph(page, pdf: Path, block_id, subtype: str):
    entries, leftovers = _extract_entries(page)
    if not entries:
        return None
    nodes: list[dict] = []
    edges: list[dict] = []
    confirmed_values = 0
    total_values = 0

    for entry in entries:
        index = len(nodes) + 1
        meaning = {
            "id": f"node-{index}", "label": entry["text"][:220],
            "node_type": "legend_meaning",
            "x": round((entry["x0"] + entry["x1"]) / 2, 3),
            "y": round((entry["y0"] + entry["y1"]) / 2, 3),
            "bbox_page": _bbox_norm(page, entry["src"]),
            "container_ids": [], "field_state": "present",
        }
        nodes.append(meaning)
        for code in entry["codes"]:
            nodes.append({
                "id": f"node-{len(nodes) + 1}", "label": code, "node_type": "legend_code",
                "x": meaning["x"], "y": meaning["y"], "bbox_page": meaning["bbox_page"],
                "container_ids": [], "field_state": "present",
            })
            edges.append({
                "id": f"edge-{len(edges) + 1}", "from": nodes[-1]["id"], "to": meaning["id"],
                "edge_type": "обозначает", "edge_state": "legend_row_confirmed",
            })
        for value in entry["values"]:
            total_values += 1
            state = ("legend_value_geometry_and_text" if value["text_confirmed"]
                     else "legend_value_geometry_only")
            confirmed_values += 1 if value["text_confirmed"] else 0
            nodes.append({
                "id": f"node-{len(nodes) + 1}", "label": value["value"],
                "node_type": "legend_value",
                "x": meaning["x"], "y": meaning["y"], "bbox_page": meaning["bbox_page"],
                "container_ids": [], "field_state": "present",
                "text_confirmed": value["text_confirmed"],
            })
            anchor = next((n for n in reversed(nodes)
                           if n["node_type"] == "legend_code"
                           and n["bbox_page"] == meaning["bbox_page"]), meaning)
            edges.append({
                "id": f"edge-{len(edges) + 1}", "from": anchor["id"], "to": nodes[-1]["id"],
                "edge_type": "параметр", "edge_state": state,
            })

    for item in leftovers:
        nodes.append({
            "id": f"node-{len(nodes) + 1}", "label": item["text"][:120],
            "node_type": "legend_note",
            "x": round(item["center"][0], 3), "y": round(item["center"][1], 3),
            "bbox_page": _bbox_norm(page, item["bbox"]),
            "container_ids": [], "field_state": "не привязано к строке легенды",
        })

    rows_with_code = sum(1 for e in entries if e["codes"])
    graph = _base(
        page, pdf, block_id, PROFILE_LEGEND, subtype, nodes=nodes, edges=edges,
        validation={
            "legend_entries_total": len(entries),
            "legend_entries_with_code": rows_with_code,
            "legend_code_rate": round(rows_with_code / len(entries), 3),
            "legend_values_total": total_values,
            "legend_values_text_confirmed": confirmed_values,
            "legend_unbound_labels_total": len(leftovers),
            "topology_state": "legend_row_bindings",
        },
        warnings=["строка легенды связывает код, параметр и расшифровку только при "
                  "совпадении Y-полосы и колонки — догадки не добавляются",
                  "параметр привязан к ближайшему образцу ниже надписи; расхождение с "
                  "числом в расшифровке отмечено состоянием связи"],
    )
    text = page.get_text().strip()
    graph["validation"].update({
        "pdf_text_characters": len(text),
        "pdf_words_total": len(page.get_text("words")),
        "source_layer_state": "text_available" if text else "no_pdf_text_layer",
        "description_depth": "legend_semantic_rows",
    })
    return graph


def evaluate_legend_gate(graph) -> dict:
    if not graph:
        return {"use": False, "complete": False, "reasons": ["граф не построен"],
                "complete_reasons": []}
    v = graph["validation"]
    reasons = [] if v.get("legend_entries_total", 0) >= 2 else ["строк легенды меньше двух"]
    partial = []
    if v.get("source_layer_state") == "no_pdf_text_layer":
        partial.append("нет текстового слоя PDF")
    if v.get("legend_unbound_labels_total", 0) > v.get("legend_entries_total", 0):
        partial.append("непривязанных надписей больше, чем строк легенды")
    graph["readiness"] = {"complete": not partial, "reasons": partial}
    return {"use": not reasons, "complete": not partial, "reasons": reasons,
            "complete_reasons": partial, "metrics": v}


def add_legend_secondary_description(graph, text):
    if graph is not None and str(text or "").strip():
        graph["secondary_description"] = {
            "source": "описание исходного блока",
            "text": re.sub(r"\s+", " ", str(text)).strip(),
            "evidence_state": "без координат", "warning": "не участвует в рёбрах",
        }
    return graph


def render_legend_markdown(graph) -> str:
    v = graph["validation"]
    nodes = {n["id"]: n for n in graph.get("nodes", [])}
    rows: dict[str, dict[str, Any]] = collections.OrderedDict()
    for node in graph.get("nodes", []):
        if node["node_type"] == "legend_meaning":
            rows[node["id"]] = {"text": node["label"], "codes": [], "values": []}
    for edge in graph.get("edges", []):
        src, dst = nodes.get(edge["from"]), nodes.get(edge["to"])
        if not src or not dst:
            continue
        if edge["edge_type"] == "обозначает" and dst["id"] in rows:
            rows[dst["id"]]["codes"].append(src["label"])
        elif edge["edge_type"] == "параметр":
            host = src if src["node_type"] == "legend_meaning" else None
            if host is None:
                host = next((nodes[i] for i in rows
                             if nodes[i]["bbox_page"] == src["bbox_page"]), None)
            if host is not None and host["id"] in rows:
                mark = "" if edge["edge_state"] == "legend_value_geometry_and_text" else " (?)"
                rows[host["id"]]["values"].append(dst["label"] + mark)

    lines = [
        f"# Эталонная текстовая разметка: {PROFILE_RU}", "",
        f"**Источник:** {graph['source']['pdf_file']}",
        "**Метод:** текст с координатами и вектор-геометрия PDF; строка легенды собрана "
        "по общей Y-полосе, параметр — по ближайшему образцу под надписью.", "",
        "## 1. Краткий результат", "",
        f"Строк легенды: {v.get('legend_entries_total', 0)}; из них с кодом: "
        f"{v.get('legend_entries_with_code', 0)}; параметров: {v.get('legend_values_total', 0)} "
        f"(подтверждено текстом расшифровки: {v.get('legend_values_text_confirmed', 0)}).",
        "**Уровень описания:** расшифровка условных обозначений строками.", "",
        "## 2. Расшифровка обозначений", "",
        "| Код | Параметр | Значение обозначения |", "|---|---|---|",
    ]
    for row in rows.values():
        code = ", ".join(row["codes"]) or "—"
        value = ", ".join(row["values"]) or "—"
        text = row["text"].replace("|", "/")
        lines.append(f"| {code} | {value} | {text} |")

    notes = [n["label"] for n in graph.get("nodes", []) if n["node_type"] == "legend_note"]
    lines += ["", "## 3. Надписи вне строк легенды", ""]
    lines.append("- " + ", ".join(notes[:40]) + (" …" if len(notes) > 40 else "")
                 if notes else "- нет")
    if graph.get("secondary_description"):
        lines += ["", "## 4. Дополнительное описание без координат", "",
                  graph["secondary_description"]["text"], "",
                  "Описание не создаёт узлы и рёбра."]
    tail = "5" if graph.get("secondary_description") else "4"
    lines += ["", f"## {tail}. Полнота и ограничения", "",
              "Доступная структура описана полностью."
              if graph.get("readiness", {}).get("complete") else "Полнота ограничена источником."]
    lines += [f"- {x}" for x in graph.get("warnings", [])]
    if v.get("legend_values_total", 0) > v.get("legend_values_text_confirmed", 0):
        lines.append("- пометка (?) у параметра: привязан геометрически, но в тексте "
                     "расшифровки это число не встречается")
    return "\n".join(lines) + "\n"
