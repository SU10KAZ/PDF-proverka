"""Структурирование вектор-текста блока в пространственные группы («блоки с началом и концом»).

Упрощённый родственник Вектографа: без доменной модели, чисто геометрия текста вектор-слоя
PDF. Строки-атомы связываются в группы union-find'ом по близости координат — так рассыпанные
подписи 2D-чертежа собираются в связные блоки, а не «схлопываются» в кашу плоской Y-склейки
(`md_mirror_reconcile._block_text`).

Правило связи (масштаб — медианная высота строки), приоритет ВЕРТИКАЛЬНОГО стека:
  • вертикальный стек: верт. зазор ≤ TV·h И X-перекрытие ≥ OV (общий столбец подписи/легенды);
  • та же строка: интервалы пересекаются по Y И гориз. зазор ≤ TH·h (соседние слова строки).
Соседние КОЛОНКИ (большой гориз. зазор, нет X-перекрытия) НЕ сливаются — это и лечит «кашу».

Две точки применения:
  • `compute_text_groups(...)` → группы с bbox, нормированным к региону блока (coords_norm),
    для оверлея «области» в UI (совпадает с рендером `/blocks/region-image`);
  • `render_grouped_text(...)` → текст с явными разделителями групп (для будущей подачи в Stage 02).

Всё fail-soft: любая ошибка → пустой список / плоский текст.
"""
from __future__ import annotations

from typing import List, Optional, Sequence

# Пороги (множители на медианную высоту строки). Подобраны на схеме ОСУП; env-тюнинг — позже.
TV = 2.0   # верт. зазор для стека
TH = 0.8   # гориз. зазор для «той же строки»
OV = 0.3   # мин. доля X-перекрытия для вертикального стека


def _median(xs: Sequence[float]) -> float:
    xs = sorted(xs)
    return xs[len(xs) // 2] if xs else 0.0


def _gap(a0: float, a1: float, b0: float, b1: float) -> float:
    """Зазор между интервалами: 0 если пересекаются, иначе положительное расстояние."""
    if a1 < b0:
        return b0 - a1
    if b1 < a0:
        return a0 - b1
    return 0.0


def _x_overlap_ratio(ai: dict, aj: dict) -> float:
    ov = min(ai["x1"], aj["x1"]) - max(ai["x0"], aj["x0"])
    if ov <= 0:
        return 0.0
    wmin = min(ai["x1"] - ai["x0"], aj["x1"] - aj["x0"]) or 1.0
    return ov / wmin


class _UF:
    def __init__(self, n: int):
        self.p = list(range(n))

    def find(self, x: int) -> int:
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[ra] = rb


def line_atoms(words) -> List[dict]:
    """PyMuPDF words `(x0,y0,x1,y1,слово,block_no,line_no,word_no)` → атомы-строки (bbox+текст)."""
    lines: dict = {}
    for w in words:
        try:
            x0, y0, x1, y1, word, bno, lno, wno = w[:8]
        except (ValueError, TypeError):
            continue
        a = lines.setdefault((bno, lno),
                             {"x0": x0, "y0": y0, "x1": x1, "y1": y1, "w": []})
        a["x0"] = min(a["x0"], x0); a["y0"] = min(a["y0"], y0)
        a["x1"] = max(a["x1"], x1); a["y1"] = max(a["y1"], y1)
        a["w"].append((wno, x0, word))
    atoms: List[dict] = []
    for a in lines.values():
        text = " ".join(t[2] for t in sorted(a["w"], key=lambda t: (t[0], t[1]))).strip()
        if not text:
            continue
        atoms.append({
            "x0": a["x0"], "y0": a["y0"], "x1": a["x1"], "y1": a["y1"],
            "h": max(1.0, a["y1"] - a["y0"]), "text": text,
        })
    return atoms


def cluster_atoms(atoms: List[dict], *, tv: float = TV, th: float = TH,
                  ov: float = OV) -> List[dict]:
    """Union-find по геометрии. Возвращает кластеры {atoms, x0,y0,x1,y1} в координатах атомов."""
    n = len(atoms)
    if n == 0:
        return []
    if n == 1:
        a = atoms[0]
        return [{"atoms": atoms, "x0": a["x0"], "y0": a["y0"], "x1": a["x1"], "y1": a["y1"]}]
    med_h = _median([a["h"] for a in atoms]) or 10.0
    Tv, Th = med_h * tv, med_h * th
    uf = _UF(n)
    for i in range(n):
        ai = atoms[i]
        for j in range(i + 1, n):
            aj = atoms[j]
            vgap = _gap(ai["y0"], ai["y1"], aj["y0"], aj["y1"])
            hgap = _gap(ai["x0"], ai["x1"], aj["x0"], aj["x1"])
            vstack = vgap <= Tv and _x_overlap_ratio(ai, aj) >= ov
            samerow = vgap <= 0.0 and hgap <= Th
            if vstack or samerow:
                uf.union(i, j)
    groups: dict = {}
    for i in range(n):
        groups.setdefault(uf.find(i), []).append(atoms[i])
    clusters: List[dict] = []
    for g in groups.values():
        g.sort(key=lambda a: (round(a["y0"], 1), a["x0"]))  # порядок чтения внутри группы
        clusters.append({
            "atoms": g,
            "x0": min(a["x0"] for a in g), "y0": min(a["y0"] for a in g),
            "x1": max(a["x1"] for a in g), "y1": max(a["y1"] for a in g),
        })
    # порядок групп: по колонкам (левый край, банды ~4 строки), затем сверху вниз
    band = (med_h * 4) or 1.0
    clusters.sort(key=lambda c: (round(c["x0"] / band), round(c["y0"], 1)))
    return clusters


def render_grouped_text(clusters: List[dict]) -> str:
    """Текст с явными разделителями групп — «блоки с началом и концом» для подачи в LLM."""
    out: List[str] = []
    for k, c in enumerate(clusters, 1):
        out.append(f"━━━ группа {k} ━━━")
        out.extend(a["text"] for a in c["atoms"])
    return "\n".join(out)


def _clamp01(v: float) -> float:
    return 0.0 if v < 0 else (1.0 if v > 1 else v)


def compute_text_groups(
    pdf_path,
    coords_norm,
    vector_text: str = "",
    polygon_norm=None,
    page_index: Optional[int] = None,
    page_index_fallback: int = 0,
) -> List[dict]:
    """Группы блока с bbox, нормированным к региону `coords_norm` [0,1].

    Страница: сначала явный `page_index` (0-based, авторитетный — из document_graph, той же
    раскладки, что и обычный кроп блока); иначе поиск по `vector_text` (`_find_page_index`);
    иначе `page_index_fallback`. ВАЖНО: result.json.page_index бывает +1 к fitz-индексу
    (см. блок 9Q9M-YM9X-AHA: result=7 vs document_graph=6) → нельзя брать его как 0-based.
    fail-soft → [].

    Возвращает: [{"n": int, "bbox": [x0,y0,x1,y1] в [0,1], "text": [строки], "natoms": int}].
    """
    try:
        if not (coords_norm and len(coords_norm) == 4 and pdf_path and __import__("os").path.exists(str(pdf_path))):
            return []
        import fitz

        from .singleline_graph_geometry import (
            _clip_words_to_bbox,
            _clip_words_to_polygon,
            _find_page_index,
        )

        doc = fitz.open(str(pdf_path))
        try:
            if page_index is not None:
                pidx = int(page_index)
            else:
                pidx = _find_page_index(doc, vector_text or "")
                if pidx is None:
                    pidx = int(page_index_fallback or 0)
            if pidx < 0 or pidx >= doc.page_count:
                return []
            pg = doc[pidx]
            W, H = float(pg.rect.width), float(pg.rect.height)
            words = pg.get_text("words")
            clipped = (_clip_words_to_polygon(words, polygon_norm, W, H)
                       if polygon_norm else _clip_words_to_bbox(words, coords_norm, W, H))
            atoms = line_atoms(clipped)
            if not atoms:
                return []
            clusters = cluster_atoms(atoms)
        finally:
            doc.close()

        rx0, ry0 = float(coords_norm[0]) * W, float(coords_norm[1]) * H
        rx1, ry1 = float(coords_norm[2]) * W, float(coords_norm[3]) * H
        rw, rh = (rx1 - rx0) or 1.0, (ry1 - ry0) or 1.0
        out: List[dict] = []
        for k, c in enumerate(clusters, 1):
            out.append({
                "n": k,
                "bbox": [
                    round(_clamp01((c["x0"] - rx0) / rw), 4),
                    round(_clamp01((c["y0"] - ry0) / rh), 4),
                    round(_clamp01((c["x1"] - rx0) / rw), 4),
                    round(_clamp01((c["y1"] - ry0) / rh), 4),
                ],
                "text": [a["text"] for a in c["atoms"]],
                "natoms": len(c["atoms"]),
            })
        return out
    except Exception:
        return []
