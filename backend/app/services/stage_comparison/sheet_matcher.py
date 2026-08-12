"""Консервативный production matcher листов на основе PreparedDocument.

Ни OCR, ни LLM, ни сравнение содержания не запускаются. Результат не является
глобальным assignment: неоднозначность остаётся ``uncertain`` вместо догадки.
"""
from __future__ import annotations

import json
import math
import os
import tempfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from .sheet_matching_diagnostic import _features, _unique_sheet_anchors


SCHEMA_VERSION = 1
_ANCHOR_NAME_MIN = 0.92
_ANCHOR_MIN_MARGIN = 0.05
_LEVEL2_MIN_SCORE = 0.75
_LEVEL2_MIN_MARGIN = 0.18
_CLAIM_MIN_SCORE = 0.60
_REMOVED_ADDED_MAX_SCORE = 0.55
_INDEPENDENT_SIGNALS = ("sheet_name", "page_text", "block_structure", "entities", "vector_metrics", "geometry")


def _round(value: float | None) -> float | None:
    return round(value, 4) if value is not None else None


def _as_pages(document: dict) -> list[dict]:
    metadata = document.get("document") or {}
    return [dict(page, _document=metadata) for page in document.get("pages") or []]


def _score(left: dict, right: dict, *, left_count: int, right_count: int) -> dict:
    values = _features(left, right, left_count=left_count, right_count=right_count)
    return {
        "right_page": right.get("pdf_page"),
        "right_sheet": right.get("sheet_number"),
        "right_name": right.get("sheet_name"),
        "signals": values["features"],
        "score": values["combinations"]["E_all_available"],
    }


def _rank(left_pages: list[dict], right_pages: list[dict]) -> dict[int, list[dict]]:
    result = {}
    for left in left_pages:
        candidates = [_score(left, right, left_count=len(left_pages), right_count=len(right_pages)) for right in right_pages]
        candidates.sort(key=lambda item: (-(item["score"] if item["score"] is not None else -1), item["right_page"]))
        result[int(left["pdf_page"])] = candidates
    return result


def _visual_fingerprint_subset(pdf_path: str | Path | None, pages: set[int]) -> dict[int, tuple[int, ...]]:
    """Посчитать fingerprint только для короткого списка спорных страниц."""
    if not pages:
        return {}
    try:
        import fitz
        document = fitz.open(str(pdf_path))
    except Exception:
        return {}
    result: dict[int, tuple[int, ...]] = {}
    try:
        for page_number in sorted(pages):
            if page_number < 1 or page_number > document.page_count:
                continue
            pix = document[page_number - 1].get_pixmap(matrix=fitz.Matrix(0.06, 0.06), colorspace=fitz.csGRAY, alpha=False)
            samples = []
            for row in range(16):
                y = min(pix.height - 1, int((row + 0.5) * pix.height / 16))
                for col in range(16):
                    x = min(pix.width - 1, int((col + 0.5) * pix.width / 16))
                    samples.append(pix.samples[y * pix.stride + x])
            threshold = sum(samples) / len(samples)
            result[page_number] = tuple(1 if value < threshold else 0 for value in samples)
    finally:
        document.close()
    return result


def _apply_visual_tie_breakers(ranks: dict[int, list[dict]], *, left_pdf, right_pdf, skip_left: set[int]) -> bool:
    """Добавить не более 8% visual tie-breaker только кандидатам с малым margin."""
    selected = {page: candidates[:2] for page, candidates in ranks.items()
                if page not in skip_left and len(candidates) > 1 and _margin(candidates) <= 0.12}
    left_fingerprints = _visual_fingerprint_subset(left_pdf, set(selected))
    right_fingerprints = _visual_fingerprint_subset(right_pdf, {candidate["right_page"] for candidates in selected.values() for candidate in candidates})
    if not left_fingerprints or not right_fingerprints:
        return False
    for left_page, candidates in selected.items():
        left_fp = left_fingerprints.get(left_page)
        for candidate in candidates:
            right_fp = right_fingerprints.get(candidate["right_page"])
            if not left_fp or not right_fp:
                continue
            visual = 1.0 - sum(a != b for a, b in zip(left_fp, right_fp)) / len(left_fp)
            candidate["signals"]["visual_fingerprint"] = _round(visual)
            candidate["base_score"] = candidate["score"]
            candidate["score"] = _round((candidate["score"] or 0.0) + (visual - 0.5) * 0.08)
            candidate["visual_tie_breaker"] = True
        ranks[left_page].sort(key=lambda item: (-(item["score"] if item["score"] is not None else -1), item["right_page"]))
    return True


def _margin(candidates: list[dict]) -> float:
    if len(candidates) < 2:
        return 1.0
    return max(0.0, float(candidates[0].get("score") or 0) - float(candidates[1].get("score") or 0))


def _candidate_reason(candidate: dict) -> list[str]:
    signals = candidate.get("signals") or {}
    return [f"{name}>={value:.2f}" for name, value in signals.items() if value is not None and value >= 0.8 and name != "document_code"]


def _independent_signal_count(candidate: dict) -> int:
    signals = candidate.get("signals") or {}
    return sum(signals.get(name) is not None and signals[name] >= 0.75 for name in _INDEPENDENT_SIGNALS)


def _has_identity_corroboration(candidate: dict) -> bool:
    """Не позволяет одинаковым титульным/пустым листам стать auto-парой."""
    signals = candidate.get("signals") or {}
    number = signals.get("sheet_number")
    name = signals.get("sheet_name") or 0.0
    text = signals.get("page_text") or 0.0
    entities = signals.get("entities") or 0.0
    return bool(
        (number == 1.0 and (name >= 0.70 or text >= 0.75))
        or (name >= 0.90 and text >= 0.70)
        or (text >= 0.90 and entities >= 0.70)
    )


def _reverse_lookup(right_ranks: dict[int, list[dict]]) -> dict[int, int | None]:
    return {page: (items[0]["right_page"] if items else None) for page, items in right_ranks.items()}


def _anchors(left_pages: list[dict], right_pages: list[dict], forward: dict[int, list[dict]], reverse: dict[int, list[dict]]) -> list[dict]:
    right_by_page = {int(page["pdf_page"]): page for page in right_pages}
    result = []
    for anchor in _unique_sheet_anchors(left_pages, right_pages):
        left_page, right_page = anchor["stage_1_page"], anchor["stage_2_page"]
        candidate = next((item for item in forward[left_page] if item["right_page"] == right_page), None)
        reverse_top = reverse.get(right_page, [{}])[0].get("right_page") if reverse.get(right_page) else None
        name_score = (candidate or {}).get("signals", {}).get("sheet_name")
        if candidate and reverse_top == left_page and _margin(forward[left_page]) >= _ANCHOR_MIN_MARGIN and (name_score is None or name_score >= _ANCHOR_NAME_MIN):
            result.append({
                "left_page": left_page, "right_page": right_page, "status": "matched",
                "confidence": 1.0, "score": candidate["score"], "top2_score": forward[left_page][1]["score"] if len(forward[left_page]) > 1 else None,
                "margin": _margin(forward[left_page]), "method": "unique_sheet_number+name+mutual_top1",
                "signals": candidate["signals"], "reasons": ["unique_sheet_number", "mutual_top1", *(_candidate_reason(candidate))],
            })
    return result


def _level2_candidates(
    left_pages: list[dict], right_pages: list[dict], forward: dict[int, list[dict]], reverse: dict[int, list[dict]],
    used_left: set[int], used_right: set[int],
) -> list[dict]:
    """Высокий порог для неякорных взаимных пар. Конфликты фильтруются отдельно."""
    candidates = []
    for left in left_pages:
        left_page = int(left["pdf_page"])
        if left_page in used_left or not forward.get(left_page):
            continue
        top = forward[left_page][0]
        right_page = int(top["right_page"])
        if right_page in used_right:
            continue
        reverse_top = reverse.get(right_page, [{}])[0].get("right_page") if reverse.get(right_page) else None
        if reverse_top != left_page:
            continue
        if (top.get("score") or 0) < _LEVEL2_MIN_SCORE or _margin(forward[left_page]) < _LEVEL2_MIN_MARGIN:
            continue
        if _independent_signal_count(top) < 2:
            continue
        if not _has_identity_corroboration(top):
            continue
        candidates.append({
            "left_page": left_page, "right_page": right_page, "status": "matched",
            "confidence": _round(min(0.99, (top["score"] or 0) * 0.7 + _margin(forward[left_page]) * 0.3)),
            "score": top["score"], "top2_score": forward[left_page][1]["score"] if len(forward[left_page]) > 1 else None,
            "margin": _margin(forward[left_page]), "method": "mutual_top1+multiple_independent_signals",
            "signals": top["signals"], "reasons": ["mutual_top1", "margin_sufficient", *_candidate_reason(top)],
        })
    return candidates


def _claim_conflicts(forward: dict[int, list[dict]], used_left: set[int], used_right: set[int]) -> tuple[set[int], set[int], list[dict]]:
    claimants: dict[int, list[int]] = defaultdict(list)
    for left_page, candidates in forward.items():
        if left_page in used_left or not candidates:
            continue
        top = candidates[0]
        if top["right_page"] not in used_right and (top.get("score") or 0) >= _CLAIM_MIN_SCORE:
            claimants[int(top["right_page"])].append(left_page)
    conflict_left, conflict_right = set(), set()
    details = []
    for right, lefts in sorted(claimants.items()):
        if len(lefts) > 1:
            conflict_left.update(lefts); conflict_right.add(right)
            details.append({"type": "many_to_one", "right_page": right, "left_pages": lefts})
    return conflict_left, conflict_right, details


def _outcome(page: int, status: str, *, candidates: list[dict], reason: str) -> dict:
    top = candidates[0] if candidates else {}
    return {
        "page": page, "status": status, "confidence": _round(top.get("score") or 0.0),
        "score": top.get("score"), "top2_score": candidates[1].get("score") if len(candidates) > 1 else None,
        "margin": _round(_margin(candidates)), "candidate_page": top.get("right_page"),
        "signals": top.get("signals") or {}, "reasons": [reason],
    }


def match_prepared_documents(
    left_document: dict, right_document: dict, *, left_pdf: str | Path | None = None, right_pdf: str | Path | None = None,
) -> dict:
    """Вернуть immutable-план карты листов, ничего не применяя на диске."""
    left_pages, right_pages = _as_pages(left_document), _as_pages(right_document)
    forward = _rank(left_pages, right_pages)
    reverse = _rank(right_pages, left_pages)
    anchors = _anchors(left_pages, right_pages, forward, reverse)
    used_left = {item["left_page"] for item in anchors}
    used_right = {item["right_page"] for item in anchors}
    visual_used = _apply_visual_tie_breakers(forward, left_pdf=left_pdf, right_pdf=right_pdf, skip_left=used_left)
    visual_used = _apply_visual_tie_breakers(reverse, left_pdf=right_pdf, right_pdf=left_pdf, skip_left=used_right) or visual_used
    conflict_left, conflict_right, conflicts = _claim_conflicts(forward, used_left, used_right)

    # Второй уровень разрешён только когда лист не участвует ни в каком
    # конкурирующем claim. В частности, нельзя «выбрать лучший» из 2→1.
    second = [item for item in _level2_candidates(left_pages, right_pages, forward, reverse, used_left, used_right)
              if item["left_page"] not in conflict_left and item["right_page"] not in conflict_right]
    used_left.update(item["left_page"] for item in second)
    used_right.update(item["right_page"] for item in second)

    left_outcomes = []
    for left in left_pages:
        page = int(left["pdf_page"])
        match = next((item for item in anchors + second if item["left_page"] == page), None)
        if match:
            left_outcomes.append({**match, "page": page, "candidate_page": match["right_page"]})
        elif page in conflict_left:
            left_outcomes.append(_outcome(page, "uncertain", candidates=forward[page], reason="conflicting_claim_for_same_new_page"))
        elif (forward[page][0].get("score") or 0) <= _REMOVED_ADDED_MAX_SCORE:
            left_outcomes.append(_outcome(page, "removed", candidates=forward[page], reason="no_plausible_new_page_candidate"))
        else:
            left_outcomes.append(_outcome(page, "uncertain", candidates=forward[page], reason="insufficient_evidence_for_safe_match"))

    right_outcomes = []
    for right in right_pages:
        page = int(right["pdf_page"])
        match = next((item for item in anchors + second if item["right_page"] == page), None)
        if match:
            right_outcomes.append({**match, "page": page, "candidate_page": match["left_page"]})
        elif page in conflict_right:
            candidates = reverse[page]
            right_outcomes.append(_outcome(page, "uncertain", candidates=candidates, reason="conflicting_claim_from_old_pages"))
        elif (reverse[page][0].get("score") or 0) <= _REMOVED_ADDED_MAX_SCORE:
            right_outcomes.append(_outcome(page, "added", candidates=reverse[page], reason="no_plausible_old_page_candidate"))
        else:
            right_outcomes.append(_outcome(page, "uncertain", candidates=reverse[page], reason="insufficient_evidence_for_safe_match"))

    matches = sorted(anchors + second, key=lambda item: item["left_page"])
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "stage_comparison_sheet_matching",
        "settings": {"llm_used": False, "pdf_overlay_used": False, "visual_fingerprint_used": visual_used, "strategy": "conservative_two_level"},
        "input": {"left": left_document.get("document") or {}, "right": right_document.get("document") or {}},
        "page_metadata": {
            "left": [{"page": page.get("pdf_page"), "sheet": page.get("sheet_number"), "name": page.get("sheet_name")} for page in left_pages],
            "right": [{"page": page.get("pdf_page"), "sheet": page.get("sheet_number"), "name": page.get("sheet_name")} for page in right_pages],
        },
        "matches": matches,
        "left_outcomes": left_outcomes,
        "right_outcomes": right_outcomes,
        "conflicts": conflicts,
        "summary": {
            "matched": len(matches),
            "removed": sum(item["status"] == "removed" for item in left_outcomes),
            "added": sum(item["status"] == "added" for item in right_outcomes),
            "uncertain_left": sum(item["status"] == "uncertain" for item in left_outcomes),
            "uncertain_right": sum(item["status"] == "uncertain" for item in right_outcomes),
        },
    }


def alignment_items_from_result(result: dict) -> list[dict]:
    """Представить результат в существующем page_alignment без ложных пар.

    Взаимные, но недостаточно доказанные TOP-1 показываются рядом в mode
    ``uncertain``. Это только удобное расположение для ручной проверки: в
    ``matches`` они не попадают, не участвуют в дальнейшем анализе и не имеют
    статуса auto/matched.
    """
    matched = {item["left_page"]: item for item in result.get("matches") or []}
    used_right = {item["right_page"] for item in result.get("matches") or []}
    left_outcomes = {item.get("page", item.get("left_page")): item for item in result.get("left_outcomes") or []}
    right_outcomes = {item.get("page", item.get("right_page")): item for item in result.get("right_outcomes") or []}
    conflicts = result.get("conflicts") or []
    conflict_left = {page for conflict in conflicts for page in conflict.get("left_pages") or []}
    conflict_right = {conflict.get("right_page") for conflict in conflicts if conflict.get("right_page") is not None}
    uncertain_preview: dict[int, int] = {}
    preview_notes: dict[int, str] = {}
    for left_page, outcome in left_outcomes.items():
        right_page = outcome.get("candidate_page")
        if outcome.get("status") != "uncertain" or not right_page:
            continue
        reverse = right_outcomes.get(right_page) or {}
        # Только взаимный и 1↔1 кандидат, не конфликтующая претензия.
        if (reverse.get("status") == "uncertain" and reverse.get("candidate_page") == left_page
                and left_page not in conflict_left and right_page not in conflict_right):
            uncertain_preview[left_page] = right_page
            used_right.add(right_page)
            preview_notes[left_page] = "sheet-matcher: uncertain review candidate; not an automatic match"

    # При конфликте 2→1 также оставляем систему честной (нет auto-пары), но
    # показываем рядом наиболее сильного кандидата. Иначе листы, которые
    # оператору нужно сравнить глазами в первую очередь, оказываются в
    # разных концах карты. Остальные претенденты остаются с пустой стороной.
    for conflict in conflicts:
        right_page = conflict.get("right_page")
        contenders = []
        for left_page in conflict.get("left_pages") or []:
            outcome = left_outcomes.get(left_page) or {}
            if outcome.get("status") == "uncertain" and outcome.get("candidate_page") == right_page:
                contenders.append((float(outcome.get("score") or 0), int(left_page)))
        if contenders and right_page not in used_right:
            _, selected_left = max(contenders, key=lambda item: (item[0], item[1]))
            uncertain_preview[selected_left] = int(right_page)
            used_right.add(int(right_page))
            preview_notes[selected_left] = "sheet-matcher: conflicting review candidate; not an automatic match"

    left_rows = []
    for page in sorted(left_outcomes):
        outcome = left_outcomes[page]
        match = matched.get(page)
        if match:
            note = f"sheet-matcher: matched; {match['method']}"
            left_rows.append({"left_page": page, "right_page": match["right_page"], "mode": "auto", "note": note})
        elif page in uncertain_preview:
            right_page = uncertain_preview[page]
            left_rows.append({
                "left_page": page, "right_page": right_page, "mode": "uncertain",
                "note": preview_notes.get(page, "sheet-matcher: uncertain review candidate; not an automatic match"),
            })
        else:
            left_rows.append({"left_page": page, "right_page": None, "mode": "blank", "note": f"sheet-matcher: {outcome['status']}; {outcome['reasons'][0]}"})

    # Вставляем неиспользованные новые листы в их естественное место правой
    # последовательности, а не приклеиваем их всем хвостом в конец.
    items = []
    for page in sorted(right_outcomes):
        if page in used_right:
            continue
        outcome = right_outcomes[page]
        row = {"left_page": None, "right_page": page, "mode": "blank", "note": f"sheet-matcher: {outcome['status']}; {outcome['reasons'][0]}"}
        before = next((index for index, existing in enumerate(left_rows)
                       if existing.get("right_page") is not None and existing["right_page"] > page), len(left_rows))
        left_rows.insert(before, row)
    for index, item in enumerate(left_rows, 1):
        items.append({"slot": index, **item})
    return items


def alignment_has_manual_items(items: list[dict]) -> bool:
    return any(str(item.get("mode") or "").lower() == "manual" for item in items or [] if isinstance(item, dict))


def write_sheet_matching_result(path: str | Path, result: dict) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(payload); stream.flush(); os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        try: os.unlink(temporary)
        except OSError: pass
        raise
    return path


def write_sheet_matching_report(path: str | Path, result: dict) -> Path:
    """Короткая сводка применяемой карты, отдельная от полного candidate JSON."""
    path = Path(path)
    left_meta = {item["page"]: item for item in (result.get("page_metadata") or {}).get("left") or []}
    right_meta = {item["page"]: item for item in (result.get("page_metadata") or {}).get("right") or []}
    matched = {item["left_page"]: item for item in result.get("matches") or []}
    lines = ["# Рабочее автоматическое сопоставление листов", "", "Консервативный matcher: спорные пары не создавались автоматически.", "", "| V2 page | V2 sheet/name | V3 page | V3 sheet/name | Status | Confidence | Method |", "| ---: | --- | ---: | --- | --- | ---: | --- |"]
    def label(meta: dict | None) -> str:
        meta = meta or {}
        return f"{meta.get('sheet') or '—'} / {meta.get('name') or '—'}".replace("|", "\\|")
    for outcome in result.get("left_outcomes") or []:
        page = outcome.get("page")
        item = matched.get(page)
        target = item.get("right_page") if item else outcome.get("candidate_page")
        lines.append("| " + " | ".join([
            str(page), label(left_meta.get(page)), str(target or "—"), label(right_meta.get(target)),
            str(outcome.get("status") or "—"), f"{float(outcome.get('confidence') or 0):.2f}",
            str(outcome.get("method") or (outcome.get("reasons") or ["—"])[0]),
        ]) + " |")
    for outcome in result.get("right_outcomes") or []:
        if outcome.get("status") != "added":
            continue
        page = outcome.get("page")
        lines.append(f"| — | — | {page} | {label(right_meta.get(page))} | added | {float(outcome.get('confidence') or 0):.2f} | {(outcome.get('reasons') or ['—'])[0]} |")
    lines.extend(["", "## Счётчики", "", *[f"- {key}: {value}" for key, value in (result.get("summary") or {}).items()], ""])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


__all__ = ["match_prepared_documents", "alignment_items_from_result", "alignment_has_manual_items", "write_sheet_matching_result", "write_sheet_matching_report"]
