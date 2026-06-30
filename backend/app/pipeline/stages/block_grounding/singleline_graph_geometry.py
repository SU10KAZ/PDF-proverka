"""singleline_graph_geometry — полный граф однолинейной схемы (топология из геометрии PDF).

Восстанавливает граф ввод→панели(РПn)→отходящие линии по чертежу:
- находит правильную PDF-страницу блока (page_index из result.json НЕ совпадает с PDF — баг нумерации);
- кластеризует токены чертежа по X-колонкам (каждая колонка = отходящая линия);
- внутри панели (QF-префикс=РПn) QF и коды цепей идут слева-направо → МОНОТОННАЯ привязка
  (резерв пропускает код) — без коллизий, проверено визуально на РП1/РП2/РП3;
- автомат/уставка/полюса/управление(АСУД/ПС)/резерв — из геометрии колонки;
- параметры линии — из валидированной таблицы (structure_singleline_text, физика 100%) по коду.

build_singleline_graph(pdf_path, vector_text, *, panel_hint) -> dict | None
Возвращает None, если это не однолинейная feeder-схема или PDF/страница недоступны. fail-soft.
"""
from __future__ import annotations

import collections
import re
from pathlib import Path
from typing import Optional

from backend.app.pipeline.stages.block_grounding.singleline_structurer import structure_singleline_text

_PANEL = {"1": "РП1", "2": "РП2", "3": "РП3", "4": "РП4 (АВР)", "5": "РП5"}
_FLOOR_RE = re.compile(
    r"(-?\d+\s*-?\s*\d*\s*эт|МОП|ЛК\d|подвал|кровл|антресол|-1\s?этаж|тех\.?\s*помещ|электрощитов|лобби|коридор)",
    re.I)


def _distinct_tokens(vector_text: str) -> list:
    """Несколько редких токенов из вектора блока — для поиска его PDF-страницы."""
    toks = []
    for m in re.finditer(r"К1\.1\.\S+|QF\d+\.\d+|\d{3}\.\d+А|\d{2,3}кВт", vector_text):
        t = m.group(0)
        if t not in toks:
            toks.append(t)
        if len(toks) >= 8:
            break
    return toks


def _find_page_index(doc, vector_text: str) -> Optional[int]:
    needles = _distinct_tokens(vector_text)
    if not needles:
        return None
    best, best_hits = None, 0
    for i in range(doc.page_count):
        txt = doc[i].get_text()
        hits = sum(1 for n in needles if n in txt)
        if hits > best_hits:
            best_hits, best = hits, i
    return best if best_hits >= max(2, len(needles) // 2) else None


def _near(qx, qy, arr, dx, dymin, dymax):
    c = sorted((abs(x - qx), y, t) for x, y, t in arr if abs(x - qx) < dx and dymin < (y - qy) < dymax)
    return c[0][2] if c else None


def _fnum(v):
    return v if v is not None else None


def build_singleline_graph(pdf_path: Path, vector_text: str, *, panel_hint: str = "ВРУ") -> Optional[dict]:
    """Построить граф однолинейной схемы. None — если не feeder-схема / нет геометрии."""
    base = structure_singleline_text(vector_text)
    if not base or base.get("feeder_total", 0) < 3:
        return None
    params = {f["circuit_code"]: f for s in base["bus_sections"] for f in s["feeders"]}

    try:
        import fitz
        doc = fitz.open(str(pdf_path))
    except Exception:
        return None
    try:
        pidx = _find_page_index(doc, vector_text)
        if pidx is None:
            return None
        pg = doc[pidx]
        words = pg.get_text("words")
    except Exception:
        return None
    finally:
        try:
            doc.close()
        except Exception:
            pass

    def coll(pat, pred=None):
        return [(w[0], w[1], w[4]) for w in words if re.match(pat, w[4]) and (pred is None or pred(w[4]))]

    qf_all = [(w[0], w[1], w[4]) for w in words if re.fullmatch(r"QF\d+\.\d+", w[4])]
    if len(qf_all) < 3:
        return None
    # исходящие vs вводные по Y (вводные — нижний ряд)
    ys = sorted(q[1] for q in qf_all)
    y_split = ys[0] + (ys[-1] - ys[0]) * 0.6 if ys[-1] - ys[0] > 60 else ys[-1] + 1
    qf_out = sorted([q for q in qf_all if q[1] <= y_split])
    n_incomers = len(qf_all) - len(qf_out)
    codes = coll(r"^К1\.1\.", lambda t: "кВт" not in t)
    BA = coll(r"^ВА")
    KA = [(w[0], w[1], w[4]) for w in words if re.fullmatch(r"\d+кА", w[4])]
    AMP = [(w[0], w[1], w[4]) for w in words if re.fullmatch(r"\d+А", w[4])]
    POLE = [(w[0], w[1], w[4]) for w in words if re.fullmatch(r"[123]Р", w[4])]
    RES = [(w[0], w[1]) for w in words if "езерв" in w[4]]
    PS = [(w[0], w[1]) for w in words if re.fullmatch(r"ПС", w[4])]
    ASUD = [(w[0], w[1]) for w in words if "АСУД" in w[4]]

    def pref(qn):
        return re.match(r"QF(\d+)", qn).group(1)

    geo = {}
    for qx, qy, qn in qf_out:
        geo[qn] = {
            "ba": _near(qx, qy, BA, 34, -95, 110),
            "ka": _near(qx, qy, KA, 44, -30, 100),
            "amp": _near(qx, qy, AMP, 44, -30, 100),
            "pole": _near(qx, qy, POLE, 44, -95, 110),
            "reserve": any(abs(x - qx) < 38 and -320 < (y - qy) < 80 for x, y in RES),
            "control": ([t for t in (["ПС"] if any(abs(x - qx) < 42 and -320 < (y - qy) < 130 for x, y in PS) else [])]
                        + (["АСУД"] if any(abs(x - qx) < 42 and -320 < (y - qy) < 130 for x, y in ASUD) else [])),
        }

    # монотонная привязка кода по панели (резерв пропускает код)
    assign = {}
    dup = collections.Counter(q[2] for q in qf_out)
    for p in sorted(set(pref(q[2]) for q in qf_out)):
        pq = sorted([q for q in qf_out if pref(q[2]) == p])
        xs = [q[0] for q in pq]
        pc = sorted([c for c in codes if min(xs) - 45 <= c[0] <= max(xs) + 45])
        ci = 0
        for qx, qy, qn in pq:
            if geo[qn]["reserve"]:
                assign[qn] = None
            elif ci < len(pc):
                assign[qn] = pc[ci][2]; ci += 1
            else:
                assign[qn] = None

    feeders = []
    for qx, qy, qn in qf_out:
        g = geo[qn]
        code = assign.get(qn)
        p = params.get(code) if code else None
        consumer = p.get("consumer") if p else ("Резерв (свободная ячейка)" if g["reserve"] else None)
        status = "reserve" if g["reserve"] else ("active" if p else "ambiguous")
        review = []
        if status == "ambiguous":
            review.append("колонка без сопоставленного кода — requires_review")
        if dup[qn] > 1:
            review.append(f"метка {qn} повторяется ({dup[qn]}×)")
        in_a = None
        if g["amp"]:
            try:
                in_a = float(re.sub(r"\D", "", g["amp"]))
            except ValueError:
                in_a = None
        if p and in_a and p.get("I_a") and in_a < p["I_a"]:
            review.append(f"номинал {in_a:.0f}А < тока {p['I_a']}А — проверить")
        loc = None
        if consumer:
            m = _FLOOR_RE.search(consumer)
            loc = m.group(0) if m else None
        feeders.append({
            "qf": qn, "panel": _PANEL.get(pref(qn), panel_hint),
            "consumer": consumer, "location": loc, "circuit_code": code,
            "breaker_type": g["ba"], "breaker_poles": g["pole"],
            "breaker_icn": g["ka"], "breaker_in": g["amp"],
            "P_inst_kw": (p or {}).get("P_inst_kw"), "Kc": (p or {}).get("Kc"),
            "cosphi": (p or {}).get("cosphi"), "P_calc_kw": (p or {}).get("P_calc_kw"),
            "I_a": (p or {}).get("I_a"), "cable": (p or {}).get("cable"),
            "length_m": (p or {}).get("length_m"), "voltage_drop_pct": (p or {}).get("voltage_drop_pct"),
            "Ikz_ka": (p or {}).get("Ikz_ka"), "routing": (p or {}).get("routing"),
            "phase": (p or {}).get("phase"), "control": g["control"],
            "status": status, "review": review,
        })

    feeders.sort(key=lambda f: (int(re.match(r"QF(\d+)", f["qf"]).group(1)),
                                [float(x) for x in f["qf"][2:].split(".")]))
    panels_map = collections.defaultdict(list)
    for f in feeders:
        panels_map[f["panel"]].append(f)
    panels = [{"id": name, "name": name, "feeder_count": len(fl),
               "active": sum(1 for x in fl if x["status"] == "active"),
               "reserve": sum(1 for x in fl if x["status"] == "reserve"),
               "feeders": fl}
              for name, fl in sorted(panels_map.items())]

    linked = [f for f in feeders if f.get("circuit_code")]
    reserve = [f for f in feeders if f["status"] == "reserve"]
    ambiguous = [f for f in feeders if f["status"] == "ambiguous"]
    review_items = [{"qf": f["qf"], "code": f.get("circuit_code"), "notes": f["review"]}
                    for f in feeders if f["review"]]
    bv = base["validation"]
    return {
        "panel": panel_hint,
        "type": "single_line_calc_diagram",
        "source_page_index": pidx,
        "feeders_total": len(feeders),
        "incomers": n_incomers,
        "panels": panels,
        "validation": {
            "active": len(linked), "reserve": len(reserve), "ambiguous": len(ambiguous),
            "breaker_bound": f"{sum(1 for f in feeders if f.get('breaker_type'))}/{len(feeders)}",
            "power_rate": bv.get("power_rate"), "current_rate": bv.get("current_rate"),
            "codes_total": len(params), "codes_linked": len({f.get('circuit_code') for f in linked}),
        },
        "review": review_items,
        "feeders_flat": feeders,
    }
