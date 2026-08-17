"""Только диагностическое сравнение листов PreparedDocument.

Модуль не создаёт и не изменяет карту листов. Он намеренно раскладывает
сигналы по отдельности, чтобы решение о будущем matcher принималось по данным,
а не по одному непрозрачному числу.
"""
from __future__ import annotations

import json
import math
import os
import re
import tempfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1
_TOKEN_RE = re.compile(r"[^\W_]+", re.UNICODE)
_GRID = 4


def _round(value: float | None) -> float | None:
    return round(value, 4) if value is not None else None


def _tokens(value: Any) -> list[str]:
    return _TOKEN_RE.findall(str(value or "").lower())


def _cosine_counts(left: Counter, right: Counter) -> float | None:
    if not left or not right:
        return None
    dot = sum(left[key] * right.get(key, 0) for key in left)
    norm = math.sqrt(sum(x * x for x in left.values()) * sum(x * x for x in right.values()))
    return dot / norm if norm else None


def _text_similarity(left: Any, right: Any) -> float | None:
    return _cosine_counts(Counter(_tokens(left)), Counter(_tokens(right)))


def _equal_score(left: Any, right: Any) -> float | None:
    left = "" if left is None else str(left).strip()
    right = "" if right is None else str(right).strip()
    if not left or not right:
        return None
    return 1.0 if left.casefold() == right.casefold() else 0.0


def _mean(values: list[float | None]) -> float | None:
    values = [value for value in values if value is not None]
    return sum(values) / len(values) if values else None


def _count_similarity(left: int, right: int) -> float:
    return 1.0 - abs(left - right) / max(1, left, right)


def _grid_signature(page: dict, *, by_type: bool = True) -> Counter:
    signature: Counter = Counter()
    for block in page.get("blocks") or []:
        bbox = block.get("normalized_bbox") or []
        if len(bbox) != 4:
            continue
        try:
            x0, y0, x1, y1 = (float(v) for v in bbox)
        except (TypeError, ValueError):
            continue
        cx, cy = (x0 + x1) / 2, (y0 + y1) / 2
        col = min(_GRID - 1, max(0, int(cx * _GRID)))
        row = min(_GRID - 1, max(0, int(cy * _GRID)))
        kind = block.get("type") if by_type else "all"
        signature[(kind, row, col)] += 1
    return signature


def _block_structure(left: dict, right: dict) -> float | None:
    lb, rb = left.get("blocks") or [], right.get("blocks") or []
    if not lb or not rb:
        return None
    kinds = ("text", "image", "stamp")
    count_score = _mean([
        _count_similarity(sum(block.get("type") == kind for block in lb), sum(block.get("type") == kind for block in rb))
        for kind in kinds
    ])
    layout_score = _cosine_counts(_grid_signature(left), _grid_signature(right))
    return _mean([count_score, layout_score])


def _geometry(left: dict, right: dict) -> float | None:
    lb, rb = left.get("blocks") or [], right.get("blocks") or []
    if not lb or not rb:
        return None
    def areas(page: dict) -> list[float]:
        result = []
        for block in page.get("blocks") or []:
            bbox = block.get("normalized_bbox") or []
            if len(bbox) == 4:
                try:
                    result.append(max(0.0, (float(bbox[2]) - float(bbox[0])) * (float(bbox[3]) - float(bbox[1]))))
                except (TypeError, ValueError):
                    pass
        return result
    la, ra = areas(left), areas(right)
    area_score = _count_similarity(round(sum(la) * 1000), round(sum(ra) * 1000)) if la and ra else None
    return _mean([
        _count_similarity(len(lb), len(rb)),
        _cosine_counts(_grid_signature(left, by_type=False), _grid_signature(right, by_type=False)),
        area_score,
        _equal_score(left.get("rotation"), right.get("rotation")),
    ])


def _vector_metrics(left: dict, right: dict) -> float | None:
    lm, rm = left.get("source_metrics") or {}, right.get("source_metrics") or {}
    keys = ("pdf_text_characters", "pdf_words", "drawing_objects", "image_placements", "image_area_ratio_sum_capped")
    if not lm or not rm:
        return None
    scores = []
    for key in keys:
        try:
            a, b = float(lm.get(key, 0)), float(rm.get(key, 0))
        except (TypeError, ValueError):
            continue
        scores.append(1.0 - abs(a - b) / max(1.0, abs(a), abs(b)))
    return _mean(scores)


def _entities(page: dict) -> str:
    return "\n".join(str(block.get("entities") or "") for block in page.get("blocks") or [] if block.get("entities"))


def _stamp_similarity(left: dict, right: dict) -> float | None:
    ls, rs = left.get("stamp") or {}, right.get("stamp") or {}
    if not ls or not rs:
        return None
    return _mean([_equal_score(ls.get(key), rs.get(key)) for key in ("stage", "object", "organization", "revisions")])


def _page_position(left: dict, right: dict, left_count: int, right_count: int) -> float:
    a = (int(left.get("pdf_page") or 1) - 1) / max(1, left_count - 1)
    b = (int(right.get("pdf_page") or 1) - 1) / max(1, right_count - 1)
    return 1.0 - abs(a - b)


def _visual_fingerprints(pdf_path: Path | None) -> dict[int, tuple[int, ...]]:
    """Очень дешёвый 16×16 бинарный отпечаток страницы без ML.

    Он намеренно остаётся только ещё одним диагностическим сигналом: рамки и
    штампы в проектных PDF могут делать разные листы внешне похожими.
    """
    if pdf_path is None or not pdf_path.is_file():
        return {}
    try:
        import fitz
        doc = fitz.open(pdf_path)
    except Exception:
        return {}
    result: dict[int, tuple[int, ...]] = {}
    try:
        for i, page in enumerate(doc):
            pix = page.get_pixmap(matrix=fitz.Matrix(0.06, 0.06), colorspace=fitz.csGRAY, alpha=False)
            samples = []
            for row in range(16):
                y = min(pix.height - 1, int((row + 0.5) * pix.height / 16))
                for col in range(16):
                    x = min(pix.width - 1, int((col + 0.5) * pix.width / 16))
                    samples.append(pix.samples[y * pix.stride + x])
            threshold = sum(samples) / len(samples)
            result[i + 1] = tuple(1 if value < threshold else 0 for value in samples)
    finally:
        doc.close()
    return result


def _visual_similarity(left: tuple[int, ...] | None, right: tuple[int, ...] | None) -> float | None:
    if not left or not right or len(left) != len(right):
        return None
    return 1.0 - sum(a != b for a, b in zip(left, right)) / len(left)


def _features(left: dict, right: dict, *, left_count: int, right_count: int, left_fp=None, right_fp=None) -> dict:
    scores = {
        "sheet_number": _equal_score(left.get("sheet_number"), right.get("sheet_number")),
        "sheet_name": _text_similarity(left.get("sheet_name"), right.get("sheet_name")),
        "document_code": _equal_score((left.get("_document") or {}).get("code"), (right.get("_document") or {}).get("code")),
        "stamp_data": _stamp_similarity(left, right),
        "page_text": _text_similarity((left.get("text") or {}).get("from_blocks"), (right.get("text") or {}).get("from_blocks")),
        "block_structure": _block_structure(left, right),
        "entities": _text_similarity(_entities(left), _entities(right)),
        "geometry": _geometry(left, right),
        "vector_metrics": _vector_metrics(left, right),
        "page_position": _page_position(left, right, left_count, right_count),
        "visual_fingerprint": _visual_similarity(left_fp, right_fp),
    }
    # A–E — независимые представления для сравнения. Равное усреднение здесь
    # намеренно: веса не подстраиваются под эту конкретную пару V2/V3.
    combinations = {
        "A_stamp_number_name": _mean([scores["stamp_data"], scores["sheet_number"], scores["sheet_name"]]),
        "B_text": scores["page_text"],
        "C_blocks_geometry": _mean([scores["block_structure"], scores["geometry"]]),
        "D_vector_metrics": scores["vector_metrics"],
        # Шифр общего документа — константа внутри конкретной пары PDF,
        # поэтому фиксируется отдельно, но не раздувает диагностический score.
        "E_all_available": _mean([value for key, value in scores.items() if key != "document_code"]),
    }
    return {"features": {key: _round(value) for key, value in scores.items()}, "combinations": {key: _round(value) for key, value in combinations.items()}}


def _candidate(left: dict, right: dict, **kwargs) -> dict:
    values = _features(left, right, **kwargs)
    return {
        "candidate_page": right.get("pdf_page"),
        "candidate_sheet": right.get("sheet_number"),
        "candidate_name": right.get("sheet_name"),
        **values,
        "diagnostic_score": values["combinations"]["E_all_available"],
    }


def _rank(left_pages: list[dict], right_pages: list[dict], *, left_fp: dict, right_fp: dict) -> list[dict]:
    rows = []
    for left in left_pages:
        candidates = [_candidate(left, right, left_count=len(left_pages), right_count=len(right_pages), left_fp=left_fp.get(left.get("pdf_page")), right_fp=right_fp.get(right.get("pdf_page"))) for right in right_pages]
        candidates.sort(key=lambda item: (-(item["diagnostic_score"] if item["diagnostic_score"] is not None else -1), item["candidate_page"]))
        margin = (candidates[0]["diagnostic_score"] or 0) - (candidates[1]["diagnostic_score"] or 0) if len(candidates) > 1 else 1.0
        best = candidates[0] if candidates else None
        score = best["diagnostic_score"] if best else 0.0
        confidence = "high" if score >= 0.85 and margin >= 0.20 else "medium" if score >= 0.65 and margin >= 0.08 else "low"
        rows.append({
            "source_page": left.get("pdf_page"), "source_sheet": left.get("sheet_number"), "source_name": left.get("sheet_name"),
            "top_candidates": candidates[:3], "all_candidates": candidates,
            "best_score": _round(score), "margin_to_second": _round(margin), "diagnostic_confidence": confidence,
        })
    return rows


def _duplicates(pages: list[dict], key: str) -> dict[str, list[int]]:
    groups: dict[str, list[int]] = defaultdict(list)
    for page in pages:
        value = str(page.get(key) or "").strip()
        if value:
            groups[value].append(page.get("pdf_page"))
    return {key: value for key, value in sorted(groups.items()) if len(value) > 1}


def _feature_summary(rows: list[dict]) -> dict:
    names = list(rows[0]["all_candidates"][0]["features"]) if rows and rows[0]["all_candidates"] else []
    result = {}
    for name in names:
        margins, coverage = [], 0
        for row in rows:
            ranked = sorted((candidate["features"].get(name) for candidate in row["all_candidates"] if candidate["features"].get(name) is not None), reverse=True)
            if ranked:
                coverage += 1
            if len(ranked) > 1:
                margins.append(ranked[0] - ranked[1])
        result[name] = {"coverage_pages": coverage, "mean_top1_margin": _round(sum(margins) / len(margins) if margins else 0.0)}
    return result


def _unique_sheet_anchors(left_pages: list[dict], right_pages: list[dict]) -> list[dict]:
    """Независимые диагностические якоря: номер существует ровно раз с каждой стороны."""
    left, right = _duplicates(left_pages, "sheet_number"), _duplicates(right_pages, "sheet_number")
    left_by_sheet = {str(page.get("sheet_number")): page for page in left_pages if page.get("sheet_number") and str(page.get("sheet_number")) not in left}
    right_by_sheet = {str(page.get("sheet_number")): page for page in right_pages if page.get("sheet_number") and str(page.get("sheet_number")) not in right}
    return [{"sheet_number": sheet, "stage_1_page": left_by_sheet[sheet]["pdf_page"], "stage_2_page": right_by_sheet[sheet]["pdf_page"]} for sheet in sorted(set(left_by_sheet) & set(right_by_sheet), key=lambda value: (len(value), value))]


def _anchor_feature_agreement(rows: list[dict], anchors: list[dict]) -> dict:
    """Проверка сигналов на якорях без подгонки весов к V2/V3."""
    by_source = {row["source_page"]: row for row in rows}
    metric_names = list(rows[0]["all_candidates"][0]["features"]) if rows and rows[0]["all_candidates"] else []
    metric_names += ["A_stamp_number_name", "B_text", "C_blocks_geometry", "D_vector_metrics", "E_all_available"]
    result = {}
    for metric in metric_names:
        eligible = wins = 0
        gaps = []
        for anchor in anchors:
            row = by_source.get(anchor["stage_1_page"])
            if not row:
                continue
            nested = "features" if metric in (row["all_candidates"][0].get("features") or {}) else "combinations"
            reference = next((item for item in row["all_candidates"] if item["candidate_page"] == anchor["stage_2_page"]), None)
            value = reference.get(nested, {}).get(metric) if reference else None
            available = [item.get(nested, {}).get(metric) for item in row["all_candidates"]]
            available = [item for item in available if item is not None]
            if value is None or not available:
                continue
            eligible += 1
            best = max(available)
            wins += value >= best - 0.0001
            gaps.append(best - value)
        result[metric] = {"anchors_with_value": eligible, "anchor_top1_or_tie": wins, "mean_gap_to_best": _round(sum(gaps) / len(gaps) if gaps else 0.0)}
    return result


def _conflicts(forward: list[dict], reverse: list[dict]) -> dict:
    forward_by_page = {row["source_page"]: row for row in forward}
    reverse_by_page = {row["source_page"]: row for row in reverse}
    claimants: dict[int, list[int]] = defaultdict(list)
    for row in forward:
        if row["top_candidates"]:
            claimants[row["top_candidates"][0]["candidate_page"]].append(row["source_page"])
    collisions = [{"stage_2_page": page, "stage_1_claimants": claimants_list} for page, claimants_list in sorted(claimants.items()) if len(claimants_list) > 1]
    mutual = []
    for row in forward:
        top = row["top_candidates"][0] if row["top_candidates"] else None
        back = reverse_by_page.get(top["candidate_page"]) if top else None
        if back and back["top_candidates"] and back["top_candidates"][0]["candidate_page"] == row["source_page"]:
            mutual.append({"stage_1_page": row["source_page"], "stage_2_page": top["candidate_page"], "confidence": row["diagnostic_confidence"]})
    mutual_v2 = {item["stage_1_page"] for item in mutual}
    mutual_v3 = {item["stage_2_page"] for item in mutual}
    return {
        "mutual_top1": mutual,
        "top1_collisions": collisions,
        "not_mutual_stage_1": [page for page in sorted(forward_by_page) if page not in mutual_v2],
        "not_mutual_stage_2": [page for page in sorted(reverse_by_page) if page not in mutual_v3],
    }


def _number_content_disagreements(rows: list[dict]) -> list[dict]:
    """Случаи, где номер листа и текстовая часть указывают на разные страницы."""
    result = []
    for row in rows:
        candidates = row.get("all_candidates") or []
        top = candidates[0] if candidates else None
        by_text = max(candidates, key=lambda item: item["features"].get("page_text") if item["features"].get("page_text") is not None else -1, default=None)
        if not top or not by_text or top["candidate_page"] == by_text["candidate_page"]:
            continue
        if row.get("source_sheet") and top.get("candidate_sheet") == row.get("source_sheet"):
            result.append({
                "source_page": row["source_page"],
                "sheet_number_candidate": top["candidate_page"],
                "text_candidate": by_text["candidate_page"],
                "sheet_number_score": top["features"].get("sheet_number"),
                "text_score": by_text["features"].get("page_text"),
            })
    return result


def _same_sheet_page_offsets(rows: list[dict]) -> dict[str, int]:
    offsets: Counter = Counter()
    for row in rows:
        if not row.get("top_candidates"):
            continue
        top = row["top_candidates"][0]
        if row.get("source_sheet") and row.get("source_sheet") == top.get("candidate_sheet"):
            offsets[int(top["candidate_page"]) - int(row["source_page"])] += 1
    return {str(key): value for key, value in sorted(offsets.items())}


def build_sheet_matching_diagnostic(prepared_left: dict, prepared_right: dict, *, left_pdf: Path | None = None, right_pdf: Path | None = None) -> dict:
    """Построить кандидатные оценки; никаких сопоставлений не применяет."""
    left_pages = [dict(page, _document=prepared_left.get("document") or {}) for page in prepared_left.get("pages") or []]
    right_pages = [dict(page, _document=prepared_right.get("document") or {}) for page in prepared_right.get("pages") or []]
    left_fp, right_fp = _visual_fingerprints(left_pdf), _visual_fingerprints(right_pdf)
    forward = _rank(left_pages, right_pages, left_fp=left_fp, right_fp=right_fp)
    reverse = _rank(right_pages, left_pages, left_fp=right_fp, right_fp=left_fp)
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "stage_comparison_sheet_matching_diagnostic",
        "input": {"stage_1": prepared_left.get("document") or {}, "stage_2": prepared_right.get("document") or {}},
        "settings": {"llm_used": False, "page_map_changed": False, "pdf_overlay_used": False, "visual_fingerprint": "16x16 grayscale threshold hash" if left_fp and right_fp else "unavailable"},
        "stage_1_to_stage_2": forward,
        "stage_2_to_stage_1": reverse,
        "feature_discriminativeness": {"stage_1_to_stage_2": _feature_summary(forward), "stage_2_to_stage_1": _feature_summary(reverse)},
        "independent_unique_sheet_anchors": _unique_sheet_anchors(left_pages, right_pages),
        "anchor_feature_agreement": _anchor_feature_agreement(forward, _unique_sheet_anchors(left_pages, right_pages)),
        "special_cases": {
            "stage_1_pages_without_stamp": [page.get("pdf_page") for page in left_pages if not page.get("stamp")],
            "stage_2_pages_without_stamp": [page.get("pdf_page") for page in right_pages if not page.get("stamp")],
            "repeated_sheet_numbers": {"stage_1": _duplicates(left_pages, "sheet_number"), "stage_2": _duplicates(right_pages, "sheet_number")},
            "repeated_sheet_names": {"stage_1": _duplicates(left_pages, "sheet_name"), "stage_2": _duplicates(right_pages, "sheet_name")},
            "top1_consistency": _conflicts(forward, reverse),
            "same_sheet_pdf_page_offsets": _same_sheet_page_offsets(forward),
            "sheet_number_vs_text_disagreements": _number_content_disagreements(forward),
        },
    }


def _atomic_json(path: Path, value: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    fd, temp_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(payload)
            stream.flush(); os.fsync(stream.fileno())
        os.replace(temp_name, path)
    except BaseException:
        try: os.unlink(temp_name)
        except OSError: pass
        raise
    return path


def _cell(value: Any) -> str:
    return str(value if value not in (None, "") else "—").replace("|", "\\|").replace("\n", " ")


def _reason(candidate: dict) -> str:
    features = candidate.get("features") or {}
    strongest = [name for name, score in features.items() if score is not None and score >= 0.8]
    return ", ".join(strongest[:5]) if strongest else "ни один отдельный сигнал не является сильным"


def _risk(confidence: str) -> str:
    return {"high": "низкий", "medium": "средний", "low": "высокий"}.get(confidence, "высокий")


def write_sheet_matching_report(comparison_dir: str | Path, diagnostic: dict) -> tuple[Path, Path]:
    comparison_dir = Path(comparison_dir)
    json_path = comparison_dir / "diagnostics" / "sheet_matching_diagnostic.json"
    md_path = comparison_dir / "diagnostics" / "sheet_matching_diagnostic.md"
    _atomic_json(json_path, diagnostic)
    lines = ["# Диагностическое сопоставление листов V2 ↔ V3", "", "Автоматическая карта листов не создавалась и не изменялась. Все оценки детерминированные; LLM и наложение PDF не использовались.", ""]
    for row in diagnostic["stage_1_to_stage_2"]:
        lines.extend([f"## V2 page {row['source_page']} / Sheet {_cell(row['source_sheet'])} / {_cell(row['source_name'])}", ""])
        for index, candidate in enumerate(row["top_candidates"], 1):
            scores = candidate["features"]
            compact = "; ".join(f"{key}={value:.2f}" for key, value in scores.items() if value is not None)
            lines.append(f"- TOP {index} → V3 page {candidate['candidate_page']} / Sheet {_cell(candidate['candidate_sheet'])}: score **{candidate['diagnostic_score']:.2f}** — {compact}")
        top = row["top_candidates"][0] if row["top_candidates"] else {}
        lines.extend([f"", f"Почему TOP 1 победил: {_reason(top)}.", f"Риск ошибки: **{_risk(row['diagnostic_confidence'])}**; отрыв от TOP 2: **{row['margin_to_second']:.2f}**.", ""])
    lines.extend(["## Сводка V2 → V3", "", "| V2 page | V2 sheet | V2 name | Best V3 page | V3 sheet | V3 name | Score | Margin to #2 | Confidence |", "| ---: | --- | --- | ---: | --- | --- | ---: | ---: | --- |"])
    for row in diagnostic["stage_1_to_stage_2"]:
        top = row["top_candidates"][0] if row["top_candidates"] else {}
        lines.append("| " + " | ".join([_cell(row["source_page"]), _cell(row["source_sheet"]), _cell(row["source_name"]), _cell(top.get("candidate_page")), _cell(top.get("candidate_sheet")), _cell(top.get("candidate_name")), f"{row['best_score']:.2f}", f"{row['margin_to_second']:.2f}", _cell(row["diagnostic_confidence"])]) + " |")
    lines.extend(["", "## Обратная диагностика V3 → V2", "", "| V3 page | TOP 1 V2 | TOP 2 V2 | TOP 3 V2 | Confidence |", "| ---: | --- | --- | --- | --- |"])
    for row in diagnostic["stage_2_to_stage_1"]:
        top = row["top_candidates"]
        cells = [f"{candidate['candidate_page']} ({candidate['diagnostic_score']:.2f})" for candidate in top]
        cells += ["—"] * (3 - len(cells))
        lines.append("| " + " | ".join([_cell(row["source_page"]), *cells, _cell(row["diagnostic_confidence"])]) + " |")
    lines.extend(["", "## Особые случаи", "", "```json", json.dumps(diagnostic["special_cases"], ensure_ascii=False, indent=2), "```", "", "## Дискриминирующая сила признаков", "", "Это не точность без размеченного эталона: показатель — средний отрыв лучшего кандидата от второго. Чем отрыв больше при хорошем покрытии, тем лучше признак разделяет кандидатов.", "", "| Признак | Покрытие V2 | Средний отрыв TOP 1–2 |", "| --- | ---: | ---: |"])
    for name, summary in diagnostic["feature_discriminativeness"]["stage_1_to_stage_2"].items():
        lines.append(f"| {name} | {summary['coverage_pages']} | {summary['mean_top1_margin']:.3f} |")
    lines.extend(["", "## Проверка на независимых якорях", "", "Якорь — номер листа, который встречается ровно один раз в каждой версии. Это не разметка модели и не используется для изменения карты; он нужен только для проверки остальных сигналов.", "", "| Признак / комбинация | Якорей с оценкой | TOP 1 или равенство TOP 1 | Средний проигрыш лучшему |", "| --- | ---: | ---: | ---: |"])
    for name, summary in diagnostic["anchor_feature_agreement"].items():
        lines.append(f"| {name} | {summary['anchors_with_value']} | {summary['anchor_top1_or_tie']} | {summary['mean_gap_to_best']:.3f} |")
    lines.append("")
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text("\n".join(lines), encoding="utf-8")
    return json_path, md_path


def run_comparison_diagnostic(comparison_dir: str | Path) -> tuple[dict, Path, Path]:
    comparison_dir = Path(comparison_dir)
    paths = []
    for stage in ("stage_1", "stage_2"):
        found = sorted((comparison_dir / stage).glob("documents/*/versions/*/03_analysis/latest/prepared_comparison/prepared_document.json"))
        if len(found) != 1:
            raise ValueError(f"expected exactly one PreparedDocument in {stage}, got {len(found)}")
        paths.append(found[0])
    models = [json.loads(path.read_text(encoding="utf-8")) for path in paths]
    versions = [path.parents[3] for path in paths]
    pdfs = [version / (model.get("document", {}).get("source_pdf") or "") for version, model in zip(versions, models)]
    diagnostic = build_sheet_matching_diagnostic(models[0], models[1], left_pdf=pdfs[0], right_pdf=pdfs[1])
    json_path, md_path = write_sheet_matching_report(comparison_dir, diagnostic)
    return diagnostic, json_path, md_path


__all__ = ["build_sheet_matching_diagnostic", "write_sheet_matching_report", "run_comparison_diagnostic"]
