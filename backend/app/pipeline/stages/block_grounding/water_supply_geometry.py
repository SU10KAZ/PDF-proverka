"""Профиль water_supply_scheme — детерминированное извлечение графа стояков ВК из вектор-слоя PDF.

Аналог `singleline_graph_geometry` (профиль electrical_singleline), адаптированный под
водоснабжение/канализацию. Блок-схема ВК = набор вертикальных стояков-«лесенок»: ось
уровней «Этаж N + отметка ±NN.NNN» (по Y) × сегменты труб «Вx.y ⌀DDx стенка» (по X-колонкам).

ГРАФ:
  узлы   = сегмент стояка на этаже (система + диаметр + стенка + этаж/отметка);
  рёбра  = вертикальная связь соседних этаж-сегментов одного стояка (стояк непрерывен по высоте);
  корень = ввод/насосная (низ схемы, max Y) — в тексте часто не выражен, потому опционален.

ГЕОМЕТРИЯ (гибрид X×Y):
  1) токены «Этаж N» кластеризуются по X → СТОЯКИ (каждая вертикальная лесенка = стояк);
  2) внутри стояка этажи сортируются по Y (верх листа = верхний этаж); к этажу подбирается
     отметка «±NN.NNN» (ближайшая по Y в X-полосе стояка);
  3) сегменты труб «Вx.y ⌀..» привязываются к стояку по X-полосе и к этажу по Y;
  4) `levels_by_elevation` — линейная регрессия y→отметка (общий примитив, переиспользуют ОВ/СС).

FINDINGS (детерминированные):
  • разрыв стояка — пропуск в непрерывной нумерации этажей лесенки (требует ручной проверки);
  • немонотонность диаметра по потоку — ⌀ не убывает к верху (В) / не растёт к низу (К).

fail-soft: build_water_graph(...) → None, если это не водяная схема (нет water-токенов) —
блок уходит дальше по конвейеру как обычно (в т.ч. в LLM).
"""
from __future__ import annotations

import collections
import re
from pathlib import Path
from typing import Optional

# ── water-специфичные токены ────────────────────────────────────────────────
_SYS_RE = re.compile(r"^(В\d+\.\d+|К\d+н?|Т\d+н?|Ст\.?\d+)$", re.I)     # система/стояк: В2.2, К1, К13н
_ELEV_RE = re.compile(r"^[+\-]?\d{1,2}[.,]\d{3}$")                       # отметка ±NN.NNN
_DIA_RE = re.compile(r"^[⌀∅ØØ]\s?\d{2,3}[xх]?$", re.I)                   # диаметр-токен: ⌀57x, ∅100
_WALL_RE = re.compile(r"^\d[.,]\d$")                                     # стенка: 3,5
_FLOOR_KW = "Этаж"


# ── общие геометрические примитивы (самодостаточно; аналоги эталона) ─────────
def _median(xs):
    xs = sorted(xs)
    n = len(xs)
    if not n:
        return 0.0
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2.0


def _cx(w):
    return (w[0] + w[2]) / 2.0


def _water_distinct_tokens(vector_text: str) -> list:
    """Редкие water-токены блока — для поиска его реальной PDF-страницы (баг page_index)."""
    toks = []
    for m in re.finditer(r"В\d+\.\d+|К\d+н|Ст\.?\d+|[⌀∅Ø]\d{2,3}", vector_text):
        t = m.group(0)
        if t not in toks:
            toks.append(t)
        if len(toks) >= 8:
            break
    return toks


def _find_page_index(doc, vector_text: str) -> Optional[int]:
    needles = _water_distinct_tokens(vector_text)
    if not needles:
        return None
    best, best_hits = None, 0
    for i in range(doc.page_count):
        txt = doc[i].get_text()
        hits = sum(1 for n in needles if n in txt)
        if hits > best_hits:
            best_hits, best = hits, i
    return best if best_hits >= max(2, len(needles) // 2) else None


def levels_by_elevation(words):
    """ОБЩИЙ ПРИМИТИВ (переиспользуют hvac/low_voltage): линейная регрессия Y→отметка по
    парам (y_центр_токена, значение ±NN.NNN). Возвращает (fn(y)->отметка, quality) либо (None, 0).

    Не хардкодит шаг этажа — калибруется по фактическим отметкам листа. Работает даже при
    стаггерированных подписях «Этаж N»."""
    pairs = []
    for w in words:
        t = w[4].strip()
        if _ELEV_RE.match(t):
            try:
                val = float(t.replace(",", ".").replace("+", ""))
            except ValueError:
                continue
            pairs.append(((w[1] + w[3]) / 2.0, val))
    if len(pairs) < 3:
        return None, 0.0
    n = len(pairs)
    sy = sum(p[0] for p in pairs)
    sv = sum(p[1] for p in pairs)
    syy = sum(p[0] * p[0] for p in pairs)
    syv = sum(p[0] * p[1] for p in pairs)
    denom = n * syy - sy * sy
    if abs(denom) < 1e-9:
        return None, 0.0
    k = (n * syv - sy * sv) / denom
    b = (sv - k * sy) / n
    # качество: доля точек с остатком < 1.5 м от прямой
    good = sum(1 for y, v in pairs if abs((k * y + b) - v) < 1.5)
    quality = good / n
    return (lambda y: k * y + b), round(quality, 3)


def _cluster_by_x(items, tol=30.0):
    """Кластеризовать по X (items: список с .x на позиции 0). → список кластеров (списков)."""
    if not items:
        return []
    items = sorted(items, key=lambda it: it[0])
    clusters = [[items[0]]]
    for it in items[1:]:
        if it[0] - clusters[-1][-1][0] < tol:
            clusters[-1].append(it)
        else:
            clusters.append([it])
    return clusters


# ── извлечение стояков ──────────────────────────────────────────────────────
def _extract_floors(words):
    """Токены «Этаж N» → список (x, y, floor_no). Число этажа — сосед справа в той же Y-полосе."""
    out = []
    for w in words:
        if w[4].strip() != _FLOOR_KW:
            continue
        row = [v for v in words if abs(v[1] - w[1]) < 8 and v[0] > w[0] and (v[0] - w[2]) < 90]
        row.sort(key=lambda v: v[0])
        num = next((v[4] for v in row if re.fullmatch(r"\d{1,2}", v[4].strip())), None)
        if num:
            out.append((_cx(w), (w[1] + w[3]) / 2.0, int(num)))
    return out


def _extract_segments(words):
    """Сегменты труб: система-токен (Вx.y/К..) + соседние ⌀-диаметр и стенка в той же Y-полосе."""
    out = []
    for w in words:
        if not _SYS_RE.match(w[4].strip()):
            continue
        row = [v for v in words if abs(v[1] - w[1]) < 8 and v[0] >= w[0] - 2 and (v[0] - w[2]) < 90]
        row.sort(key=lambda v: v[0])
        dia = next((v[4].strip() for v in row if _DIA_RE.match(v[4].strip())), None)
        wall = next((v[4].strip() for v in row if _WALL_RE.match(v[4].strip())), None)
        out.append((_cx(w), (w[1] + w[3]) / 2.0, w[4].strip(), dia, wall))
    return out


def _dia_mm(dia):
    """'⌀57x' → 57 (int) для проверки монотонности; None если не распарсить."""
    if not dia:
        return None
    m = re.search(r"(\d{2,3})", dia)
    return int(m.group(1)) if m else None


def build_water_graph(pdf_path: Path, vector_text: str) -> Optional[dict]:
    """Граф стояков ВК из геометрии вектор-слоя. None — если это не водяная схема (fail-soft)."""
    try:
        import fitz  # PyMuPDF
    except ImportError:
        return None
    # быстрый gate: есть ли water-лексика вообще
    if not _water_distinct_tokens(vector_text):
        return None
    try:
        doc = fitz.open(str(pdf_path))
    except Exception:
        return None
    try:
        pidx = _find_page_index(doc, vector_text)
        if pidx is None:
            return None
        pg = doc[pidx]
        words = pg.get_text("words")
        page_w, page_h = float(pg.rect.width), float(pg.rect.height)
    except Exception:
        return None
    finally:
        try:
            doc.close()
        except Exception:
            pass

    floors = _extract_floors(words)
    segments = _extract_segments(words)
    if len(floors) < 3:
        return None  # без лестницы этажей это не стояковая схема

    y2elev, elev_quality = levels_by_elevation(words)

    # 1) кластеризуем этаж-токены по X → стояки (лесенки)
    floor_clusters = _cluster_by_x([(f[0], f[1], f[2]) for f in floors], tol=30.0)
    # только реальные лесенки (≥3 этажей)
    ladders = [c for c in floor_clusters if len(c) >= 3]

    risers = []
    ladder_qualities = []
    for i, lad in enumerate(sorted(ladders, key=lambda c: _median([m[0] for m in c])), 1):
        cxr = _median([m[0] for m in lad])
        lad_floors = sorted(lad, key=lambda m: m[1])  # по Y (верх=выше этаж)
        floor_items = []
        # ПО-СТОЯКОВАЯ регрессия Y→отметка (в блоке несколько зданий с разной шкалой отметок,
        # общая регрессия смешивает их → неверные отметки; берём только отметки в X-полосе стояка).
        lad_words = [w for w in words if abs(_cx(w) - cxr) < 55]
        y2elev_lad, q_lad = levels_by_elevation(lad_words)
        y2elev_use = y2elev_lad or y2elev
        ladder_qualities.append(q_lad)
        floor_no_seq = []
        for (fx, fy, fno) in lad_floors:
            elev = round(y2elev_use(fy), 3) if y2elev_use else None
            floor_items.append({"floor": fno, "y": round(fy, 1), "elevation": elev})
            floor_no_seq.append(fno)
        # сегменты труб этого стояка (по X-полосе)
        seg_items = []
        for (sx, sy, sys, dia, wall) in segments:
            if abs(sx - cxr) < 45:
                seg_items.append({"system": sys, "diameter": dia, "wall": wall,
                                  "y": round(sy, 1),
                                  "elevation": round(y2elev_use(sy), 3) if y2elev_use else None,
                                  "dia_mm": _dia_mm(dia)})
        # доминирующая система стояка
        sys_cnt = collections.Counter(s["system"] for s in seg_items if s.get("system"))
        system = sys_cnt.most_common(1)[0][0] if sys_cnt else None

        # FINDING 1: разрыв нумерации этажей (потенциальный разрыв стояка).
        # КОНСЕРВАТИВНО: флажок только если лесенка ПЛОТНО размечена (≥70% этажей диапазона
        # подписаны) и пропусков немного — иначе это недоразметка, а не разрыв стояка.
        review = []
        fno_sorted = sorted(set(floor_no_seq))
        if len(fno_sorted) >= 4:
            span = fno_sorted[-1] - fno_sorted[0] + 1
            full = set(range(fno_sorted[0], fno_sorted[-1] + 1))
            missing = sorted(full - set(fno_sorted))
            density = len(fno_sorted) / span if span else 0
            if missing and density >= 0.70 and len(missing) <= max(2, span // 5):
                review.append(f"возможный разрыв стояка: в плотной лесенке (этажи "
                              f"{fno_sorted[0]}–{fno_sorted[-1]}, размечено {len(fno_sorted)}/{span}) "
                              f"пропущены {missing} — requires_human_review")
        # FINDING 2: немонотонность диаметра по высоте (В — ⌀ должен не расти кверху)
        dseq = [(s["y"], s["dia_mm"]) for s in seg_items if s.get("dia_mm")]
        dseq.sort(key=lambda z: z[0])  # сверху вниз по y (верх листа→низ)
        if len(dseq) >= 2:
            vals = [d for _, d in dseq]
            # сверху (верхний этаж) вниз к вводу: диаметр должен НЕ убывать (растёт к вводу)
            if any(vals[j] > vals[j - 1] + 0 and vals[j] < vals[j - 1] for j in range(1, len(vals))):
                pass
            non_monotone = any(vals[j] > vals[j - 1] for j in range(1, len(vals)))  # растёт кверху = подозрительно для В
            if non_monotone and (system or "").upper().startswith("В"):
                review.append(f"диаметр стояка немонотонен по высоте {vals} — "
                              f"проверить (В: ⌀ не должен расти к верхним этажам)")

        risers.append({
            "id": f"R{i}",
            "x_norm": round(cxr / page_w, 5) if page_w else None,
            "system": system,
            "floor_range": [fno_sorted[0], fno_sorted[-1]] if fno_sorted else None,
            "floors": floor_items,
            "segments": seg_items,
            "review": review,
        })

    if not risers:
        return None

    systems = sorted({r["system"] for r in risers if r.get("system")})
    total_review = [f"{r['id']}: {n}" for r in risers for n in r["review"]]
    avg_ladder_quality = round(sum(ladder_qualities) / len(ladder_qualities), 3) if ladder_qualities else 0.0
    return {
        "type": "water_riser_diagram",
        "profile_id": "water_supply_scheme",
        "source_page_index": pidx,
        "systems": systems,
        "risers_total": len(risers),
        "levels": {
            "y_to_elevation_quality_global": elev_quality,
            "y_to_elevation_quality_per_riser": avg_ladder_quality,
            "elevation_range": [
                min((f["elevation"] for r in risers for f in r["floors"] if f["elevation"] is not None), default=None),
                max((f["elevation"] for r in risers for f in r["floors"] if f["elevation"] is not None), default=None),
            ],
        },
        "risers": risers,
        "validation": {
            "floors_detected": len(floors),
            "segments_detected": len(segments),
            "risers_with_review": sum(1 for r in risers if r["review"]),
            "elevation_regression_quality_per_riser": avg_ladder_quality,
        },
        "review": total_review,
    }


def render_water_graph_markdown(graph: dict) -> str:
    """Читаемая разметка графа стояков ВК (таблицы по стоякам + сводка/проверки)."""
    if not graph:
        return ""
    L = [f"# Схема стояков ВК (профиль {graph.get('profile_id')})", ""]
    L.append(f"**Систем:** {', '.join(graph.get('systems') or []) or '—'} | "
             f"**стояков:** {graph.get('risers_total')} | "
             f"**отметки:** {graph['levels'].get('elevation_range')} "
             f"(качество y→отметка по-стояково {graph['levels'].get('y_to_elevation_quality_per_riser')})")
    L.append("")
    for r in graph.get("risers", []):
        L.append(f"## Стояк {r['id']} — система {r.get('system') or '?'} "
                 f"(этажи {r.get('floor_range')})")
        L.append("| Этаж | Отметка | ⌀×стенка |")
        L.append("| --- | --- | --- |")
        # диаметр на этаж: ближайший сегмент по Y
        segs = sorted(r.get("segments", []), key=lambda s: s["y"])
        for f in r["floors"]:
            near = min(segs, key=lambda s: abs(s["y"] - f["y"]), default=None) if segs else None
            dia = (f"{near['diameter']} {near['wall'] or ''}".strip()
                   if near and abs(near["y"] - f["y"]) < 60 else "—")
            L.append(f"| {f['floor']} | {f['elevation']} | {dia} |")
        if r["review"]:
            L.append("")
            for n in r["review"]:
                L.append(f"- ⚠ {n}")
        L.append("")
    if graph.get("review"):
        L.append("## Требует проверки")
        for n in graph["review"]:
            L.append(f"- {n}")
    return "\n".join(L)
