"""Диагностическая схема: растровая подложка листа + векторные оверлеи
результата (области помещений, марки, потолки, свет, выключатели,
размеры, непривязанное, конфликты) в координатах MediaBox.
"""
from __future__ import annotations

import base64
import collections

import fitz

APT_COLORS = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
              "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"]

RENDER_SCALE = 0.6


def _esc(text: str) -> str:
    return (str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            .replace('"', "&quot;"))


def render_overlay_svg(result: dict, pdf_path: str) -> str:
    cp = result["cp"]
    graph = result["graph"]
    room_data = result["room_data"]
    w, h = cp.media_rect[2], cp.media_rect[3]

    pix = cp.page.get_pixmap(matrix=fitz.Matrix(RENDER_SCALE, RENDER_SCALE), alpha=False)
    png_b64 = base64.b64encode(pix.tobytes("png")).decode("ascii")

    parts: list[str] = []
    parts.append(f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w:.0f} {h:.0f}" '
                 f'font-family="sans-serif">')
    parts.append(f'<image href="data:image/png;base64,{png_b64}" x="0" y="0" '
                 f'width="{w:.0f}" height="{h:.0f}"/>')

    # граница блока и справочной колонки
    bx0, by0, bx1, by1 = cp.block_rect
    parts.append(f'<rect x="{bx0:.0f}" y="{by0:.0f}" width="{bx1 - bx0:.0f}" '
                 f'height="{by1 - by0:.0f}" fill="none" stroke="#0055ff" '
                 'stroke-width="2" stroke-dasharray="12 6"/>')

    # области помещений: горизонтальные прогоны клеток, цвет — по квартире
    apt_ids = sorted({r["apartment"] for r in graph["rooms"]})
    apt_color = {a: APT_COLORS[i % len(APT_COLORS)] for i, a in enumerate(apt_ids)}
    grid = room_data["grid"]
    owner_rows: dict[tuple[str, int], list[int]] = collections.defaultdict(list)
    for (i, j), mark in sorted(room_data["cell_owner"].items()):
        owner_rows[(mark, j)].append(i)
    mark_apt = {r["mark"]: r["apartment"] for r in graph["rooms"]}
    parts.append('<g opacity="0.18">')
    for (mark, j), cols in sorted(owner_rows.items()):
        color = apt_color.get(mark_apt.get(mark, ""), "#999")
        cols.sort()
        run_start = cols[0]
        prev = cols[0]
        for c in cols[1:] + [None]:
            if c is not None and c == prev + 1:
                prev = c
                continue
            x = grid.x0 + run_start * grid.cell
            y = grid.y0 + j * grid.cell
            parts.append(f'<rect x="{x:.1f}" y="{y:.1f}" '
                         f'width="{(prev - run_start + 1) * grid.cell:.1f}" '
                         f'height="{grid.cell:.1f}" fill="{color}"/>')
            if c is not None:
                run_start = prev = c
    parts.append("</g>")

    for room in graph["rooms"]:
        x, y = room["center"]
        parts.append(f'<text x="{x:.0f}" y="{y - 6:.0f}" font-size="9" fill="#003" '
                     f'text-anchor="middle">{_esc(room["mark"])}</text>')

    for z in graph["ceiling_zones"]:
        b = z["bbox"]
        parts.append(f'<rect x="{b[0]:.1f}" y="{b[1]:.1f}" width="{b[2] - b[0]:.1f}" '
                     f'height="{b[3] - b[1]:.1f}" fill="none" stroke="#2244cc" stroke-width="1.2"/>')

    for light in graph["lights"]:
        x, y = light["center"]
        color = "#cc00cc" if light["kind"] == "chandelier_output" else "#ff3399"
        parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="8" fill="none" '
                     f'stroke="{color}" stroke-width="1.6"/>')
        if light["groups"]:
            parts.append(f'<text x="{x + 9:.0f}" y="{y - 7:.0f}" font-size="8" '
                         f'fill="{color}">{_esc("/".join(light["groups"]))}</text>')
        if light.get("centered_by_guides"):
            parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="2" fill="{color}"/>')

    for sw in graph["switches"]:
        b = sw["bbox"]
        parts.append(f'<rect x="{b[0]:.1f}" y="{b[1]:.1f}" width="{b[2] - b[0]:.1f}" '
                     f'height="{b[3] - b[1]:.1f}" fill="none" stroke="#dd2222" stroke-width="1.4"/>')
    for m in graph["master_switches"]:
        b = m["bbox"]
        parts.append(f'<rect x="{b[0]:.1f}" y="{b[1]:.1f}" width="{b[2] - b[0]:.1f}" '
                     f'height="{b[3] - b[1]:.1f}" fill="none" stroke="#009933" stroke-width="1.8"/>')

    for dim in graph["dimensions"]:
        if not str(dim.get("binding_state", "")).startswith("device_to"):
            continue
        p1, p2 = dim["line"]["p1"], dim["line"]["p2"]
        parts.append(f'<line x1="{p1[0]:.1f}" y1="{p1[1]:.1f}" x2="{p2[0]:.1f}" '
                     f'y2="{p2[1]:.1f}" stroke="#ff8800" stroke-width="1.6"/>')
        parts.append(f'<text x="{dim["center"][0]:.0f}" y="{dim["center"][1] - 3:.0f}" '
                     f'font-size="7" fill="#ff8800">{dim["value_mm"]}</text>')

    for sym in graph["unresolved_symbols"]:
        b = sym["bbox"]
        parts.append(f'<rect x="{b[0] - 2:.1f}" y="{b[1] - 2:.1f}" width="{b[2] - b[0] + 4:.1f}" '
                     f'height="{b[3] - b[1] + 4:.1f}" fill="none" stroke="#ff6600" '
                     'stroke-width="1.6" stroke-dasharray="4 3"/>')
    for c in graph["conflicts"]:
        b = c.get("bbox")
        if not b:
            continue
        x, y = (b[0] + b[2]) / 2, (b[1] + b[3]) / 2
        parts.append(f'<path d="M {x - 6:.1f} {y - 6:.1f} L {x + 6:.1f} {y + 6:.1f} '
                     f'M {x - 6:.1f} {y + 6:.1f} L {x + 6:.1f} {y - 6:.1f}" '
                     'stroke="#ee0000" stroke-width="2.4" fill="none"/>')

    legend = [("#0055ff", "граница блока (CropBox)"), ("#2244cc", "потолочные марки"),
              ("#cc00cc", "вывод под люстру"), ("#ff3399", "вывод под светильник"),
              ("#dd2222", "выключатели"), ("#009933", "мастер-выключатели"),
              ("#ff8800", "размерные конструкции"), ("#ff6600", "неразрешённые символы"),
              ("#ee0000", "GEOMETRY_CONFLICT")]
    ly = h - 20 * len(legend) - 16
    parts.append(f'<rect x="12" y="{ly - 14:.0f}" width="300" height="{20 * len(legend) + 20:.0f}" '
                 'fill="#ffffff" opacity="0.85"/>')
    for i, (color, label) in enumerate(legend):
        y = ly + i * 20
        parts.append(f'<rect x="20" y="{y - 9:.0f}" width="14" height="10" fill="none" '
                     f'stroke="{color}" stroke-width="2"/>')
        parts.append(f'<text x="42" y="{y:.0f}" font-size="12" fill="#111">{_esc(label)}</text>')
    parts.append("</svg>")
    return "\n".join(parts)
