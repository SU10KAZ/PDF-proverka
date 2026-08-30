"""Deterministic factual TEXT deltas for accepted sheet-link groups.

Stage 3 consumes only Stage 2 ``remaining_for_comparison`` fragment ids.  The
production path intentionally contains no model call: the August 2026 real-data
benchmark found no factual gain over the conservative deterministic baseline.
"""
from __future__ import annotations

import difflib
import hashlib
import json
import re
import unicodedata
from collections import Counter, defaultdict, deque
from typing import Any, Iterable, Mapping

from . import room_schedule, text_comparison


VERSION = 1
KIND = "stage_comparison_text_differences"
ALGORITHM = "deterministic_text_differences_v1_5"
PRODUCTION_PATH = "deterministic_only"
SIMILARITY_THRESHOLD = 0.82
AMBIGUITY_THRESHOLD = 0.55

_AUDIT_LANGUAGE_RE = re.compile(
    r"критич|нарушен|необходимо\s+исправ|влияет\s+на\s+стоимост|"
    r"ухудшен|улучшен|ошибк[аи]\s+проектиров",
    re.IGNORECASE,
)
_NUMBER_RE = re.compile(r"(?<![a-zа-я0-9])\d+(?:[.,]\d+)*(?!\d)", re.I)
_TOKEN_RE = re.compile(r"[a-zа-я0-9]+(?:[.\-/][a-zа-я0-9]+)*", re.I)
_LEADING_KEY_RE = re.compile(
    r"^([a-zа-я0-9]+(?:[.\-][a-zа-я0-9]+)+)(?=\s|[|:]|$)", re.I
)
_LEADING_MARK_RE = re.compile(
    r"^([a-zа-я]+\d+(?:[.\-][a-zа-я0-9]+)*)(?=\s|[|:]|$)", re.I
)
_FIELD_RE = re.compile(
    r"^(заказчик|проектировщик|генеральный\s+проектировщик|том|год)\s*[:\-]?",
    re.I,
)
_MIXED_SCRIPT_TRANSLATION = str.maketrans({
    "a": "а", "b": "в", "c": "с", "e": "е", "h": "н", "k": "к",
    "m": "м", "o": "о", "p": "р", "t": "т", "x": "х", "y": "у",
})


def _normalize_ocr_confusables(value: str) -> str:
    def replace_token(match: re.Match[str]) -> str:
        token = match.group(0)
        if re.search(r"[a-z]", token) and re.search(r"[а-я]", token):
            return token.translate(_MIXED_SCRIPT_TRANSLATION)
        return token

    text = re.sub(r"[a-zа-я0-9./-]+", replace_token, value)
    text = re.sub(r"\b1з(?=ав(?:\b|-))", "13", text)
    if re.match(r"^уз(?=\s*(?:[;|\[]|-))", text) and "заполнение деформационных швов" in text:
        text = "у3" + text[2:]
    return text


def canonicalize(value: str) -> str:
    """Canonical text for Stage 3 exact equivalence and similarity."""
    text = unicodedata.normalize("NFKC", str(value or "")).lower().replace("ё", "е")
    text = _normalize_ocr_confusables(text)
    text = re.sub(r"\\(?:geq|ge)", "≥", text)
    text = re.sub(r"\\(?:leq|le)", "≤", text)
    text = re.sub(r"\\text\s*\{([^{}]*)\}", r"\1", text)
    text = re.sub(
        r"\[(?:hatched block|cross-hatched block|diagonal lines block|thin line)\]",
        "",
        text,
    )
    text = text.replace("м^3", "м3").replace("м³", "м3")
    text = text.replace("м^2", "м2").replace("м²", "м2")
    text = text.replace("–", "-").replace("—", "-").replace("−", "-")
    text = re.sub(r"(?<=\d),(?=\d)", ".", text)
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"(?<=[≥≤])\s+", "", text)
    text = re.sub(
        r"(?<=\d)\s+(?=(?:мм|см|м2|м3|кг|мг|л|ч|мин)(?:\b|[/]))",
        "", text,
    )
    text = re.sub(r"\s*([|:;,.()\-/])\s*", r"\1", text)
    return text.strip(" .")


def stable_key(value: str) -> str:
    text = canonicalize(value)
    field = _FIELD_RE.match(text)
    if field:
        return re.sub(r"\s+", "_", field.group(1))
    raw = unicodedata.normalize("NFKC", str(value or "")).lower().replace("ё", "е")
    raw = raw.replace("–", "-").replace("—", "-").replace("−", "-")
    raw = re.sub(r"\s+", " ", raw).strip()
    match = _LEADING_KEY_RE.match(raw) or _LEADING_MARK_RE.match(raw)
    if not match:
        return ""
    key = canonicalize(match.group(1))
    # A bare decimal value is a measurement, not a row identifier.
    if re.fullmatch(r"\d+[.]\d+", key):
        remainder = raw[len(match.group(1)):]
        if not re.match(r"\s+[a-zа-я]{2,}\b", remainder, re.I):
            return ""
    return key


def _tokens(value: str) -> set[str]:
    return {token for token in _TOKEN_RE.findall(canonicalize(value)) if len(token) > 1}


def _identifiers(value: str) -> set[str]:
    return {
        token for token in _TOKEN_RE.findall(canonicalize(value))
        if any(char.isdigit() for char in token)
        and (any(char.isalpha() for char in token) or "." in token or "-" in token)
    }


def _skeleton(value: str) -> str:
    text = canonicalize(value)
    text = re.sub(r"\d+(?:[.]\d+)*", "#", text)
    text = re.sub(r"#+", "#", text)
    return text.strip(" #-.:/")


def similarity(left: str, right: str) -> float:
    return difflib.SequenceMatcher(
        None, canonicalize(left), canonicalize(right), autojunk=False
    ).ratio()


def _equivalence_key(value: str) -> str:
    """Ignore presentation punctuation only; preserve the alphanumeric order."""
    return re.sub(r"[^a-zа-я0-9≥≤]+", "", canonicalize(value))


def is_graphic_description(fragment: dict[str, Any]) -> bool:
    text = str(fragment.get("text") or "").strip().lower()
    return bool(re.match(
        r"^(?:the image contains|the image shows|a blue circular official stamp|"
        r"\[(?:oval|triangle|circle|hatched block|diagonal lines block)\b)",
        text,
    ))


def _token_overlap(left: str, right: str) -> float:
    left_tokens, right_tokens = _tokens(left), _tokens(right)
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / min(len(left_tokens), len(right_tokens))


def _candidate_score(left: dict[str, Any], right: dict[str, Any]) -> float:
    left_text, right_text = str(left["text"]), str(right["text"])
    left_key, right_key = stable_key(left_text), stable_key(right_text)
    if left_key and left_key == right_key:
        return 1.0
    left_skeleton, right_skeleton = _skeleton(left_text), _skeleton(right_text)
    if left_skeleton and left_skeleton == right_skeleton:
        return 0.97
    ratio = similarity(left_text, right_text)
    shared_ids = _identifiers(left_text) & _identifiers(right_text)
    if shared_ids and ratio >= 0.68 and _token_overlap(left_text, right_text) >= 0.45:
        return min(0.96, ratio + 0.04)
    if (
        ratio >= SIMILARITY_THRESHOLD
        and _token_overlap(left_text, right_text) >= 0.5
        and max(len(left_text), len(right_text)) >= 24
    ):
        return ratio
    return 0.0


def _anchors(fragments: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    output = []
    for fragment in fragments:
        output.append({
            "fragment_id": str(fragment["id"]),
            "page": int(fragment["pdf_page"]),
            "bboxes": list(fragment.get("bboxes") or []),
        })
    return output


def _different_values(before: str, after: str) -> tuple[list[str], list[str]]:
    old = _NUMBER_RE.findall(before)
    new = _NUMBER_RE.findall(after)
    old_counts, new_counts = Counter(old), Counter(new)
    old_only = list((old_counts - new_counts).elements())
    new_only = list((new_counts - old_counts).elements())
    return old_only, new_only


def _changed_summary(before: str, after: str) -> str:
    key = stable_key(before) or stable_key(after)
    old_values, new_values = _different_values(before, after)
    if old_values and new_values:
        prefix = key or "Значение"
        return f"{prefix}: {', '.join(old_values)} → {', '.join(new_values)}."

    old_words = canonicalize(before).split()
    new_words = canonicalize(after).split()
    matcher = difflib.SequenceMatcher(None, old_words, new_words, autojunk=False)
    old_delta: list[str] = []
    new_delta: list[str] = []
    for tag, left_start, left_end, right_start, right_end in matcher.get_opcodes():
        if tag == "equal":
            continue
        old_delta.extend(old_words[left_start:left_end])
        new_delta.extend(new_words[right_start:right_end])
    old_text = " ".join(old_delta).strip()
    new_text = " ".join(new_delta).strip()
    if old_text and new_text and len(old_text) + len(new_text) <= 110:
        prefix = f"{key}: " if key else ""
        return f"{prefix}{old_text} → {new_text}."

    subject = re.split(r"[.!?]", before.strip(), maxsplit=1)[0].strip()
    if len(subject) > 90:
        subject = subject[:87].rstrip() + "…"
    return f"{subject or 'Текст'} — текст изменён."


def _single_summary(text: str, action: str) -> str:
    compact = re.sub(r"\s+", " ", text).strip()
    if len(compact) <= 160:
        return compact
    first = re.split(r"(?<=[.!?])\s+", compact, maxsplit=1)[0]
    if len(first) > 130:
        first = first[:127].rstrip() + "…"
    return f"{first} — текст {action}."


def _changed_item(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    before, after = str(left["text"]), str(right["text"])
    return {
        "key": stable_key(before) or stable_key(after) or None,
        "summary": _changed_summary(before, after),
        "before": before,
        "after": after,
        "left_fragment_ids": [str(left["id"])],
        "right_fragment_ids": [str(right["id"])],
        "left_pages": [int(left["pdf_page"])],
        "right_pages": [int(right["pdf_page"])],
        "left_anchors": _anchors([left]),
        "right_anchors": _anchors([right]),
    }


def _paired_item(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    """Exact source provenance for a deterministic pair used by Stage 4."""
    before, after = str(left["text"]), str(right["text"])
    return {
        "before": before,
        "after": after,
        "left_fragment_ids": [str(left["id"])],
        "right_fragment_ids": [str(right["id"])],
        "left_pages": [int(left["pdf_page"])],
        "right_pages": [int(right["pdf_page"])],
        "left_anchors": _anchors([left]),
        "right_anchors": _anchors([right]),
    }


def _removed_item(fragment: dict[str, Any]) -> dict[str, Any]:
    before = str(fragment["text"])
    return {
        "summary": _single_summary(before, "удалён"),
        "before": before,
        "left_fragment_ids": [str(fragment["id"])],
        "left_pages": [int(fragment["pdf_page"])],
        "left_anchors": _anchors([fragment]),
    }


def _added_item(fragment: dict[str, Any]) -> dict[str, Any]:
    after = str(fragment["text"])
    return {
        "summary": _single_summary(after, "добавлен"),
        "after": after,
        "right_fragment_ids": [str(fragment["id"])],
        "right_pages": [int(fragment["pdf_page"])],
        "right_anchors": _anchors([fragment]),
    }


def _room_identity(
    fragments: list[dict[str, Any]],
) -> tuple[dict[str, str], dict[str, str]]:
    """Номер помещения как ключ строки — там, где заголовок его доказал.

    «1.9 С/у 6,95» и «1.8 С/у 4,38» — разные помещения с одинаковым названием.
    Сравнение по похожести текста их путает: скелет строки у обеих «с/у», и
    выигрывает та, что случайно ближе по цифрам. На паре АР так склеились 32
    санузла — и каждый унёс с собой настоящее изменение своей площади.

    Возвращает (номер → единственный фрагмент, фрагмент → номер). Номер,
    встретившийся на стороне дважды, ключом не считается: неоднозначность
    решается вопросом инженеру, а не выбором первого кандидата.
    """
    widths: dict[str, int] = {}
    for fragment in fragments:
        if fragment.get("source_kind") != "table_row":
            continue
        units = room_schedule.header_units(fragment)
        if units:
            group = str(fragment.get("source_group") or "")
            widths[group] = max(widths.get(group, 0), max(units))
    by_code: dict[str, list[str]] = defaultdict(list)
    code_of: dict[str, str] = {}
    for fragment in fragments:
        if fragment.get("source_kind") != "table_row":
            continue
        width = widths.get(str(fragment.get("source_group") or ""))
        if not width:
            continue
        units = room_schedule.row_units(room_schedule.row_cells(fragment), width)
        if not units or len(units) != 1:
            continue
        code = units[0][0]
        fragment_id = str(fragment["id"])
        by_code[code].append(fragment_id)
        code_of[fragment_id] = code
    unique = {
        code: ids[0] for code, ids in by_code.items() if len(ids) == 1
    }
    return unique, code_of


def _is_evidence_source(fragment: Mapping[str, Any]) -> bool:
    """Может ли единица участвовать в утверждениях, а не только в совпадении.

    Отсутствие метки читается как Markdown: так ведут себя все единицы,
    записанные до появления провенанса, и молча поражать их в правах нельзя.
    """
    source = fragment.get("source")
    return source is None or source == text_comparison.SOURCE_MARKDOWN


def compare_group(
    left_fragments: list[dict[str, Any]],
    right_fragments: list[dict[str, Any]],
) -> dict[str, Any]:
    """Compare one accepted many-to-many sheet group conservatively."""
    left_by_id = {str(item["id"]): item for item in left_fragments}
    right_by_id = {str(item["id"]): item for item in right_fragments}
    unused_left = set(left_by_id)
    unused_right = set(right_by_id)
    # Единица, прочитанная из вектор-слоя PDF, имеет право ТОЛЬКО подтвердить
    # дословное совпадение. У неё нет объекта-владельца: строка «500А» на
    # чертеже принадлежит конкретному аппарату, но сама об этом не знает.
    # Сближать такие строки по похожести — значит выдумывать изменения вроде
    # «500А → 3200А» из двух несвязанных подписей, а объявлять их пропавшими —
    # рождать сотни ложных «удалено» на каждом чертеже. Поэтому дальше точного
    # совпадения они не идут ни в одну корзину.
    advisory_left = {
        fragment_id
        for fragment_id, item in left_by_id.items()
        if not _is_evidence_source(item)
    }
    advisory_right = {
        fragment_id
        for fragment_id, item in right_by_id.items()
        if not _is_evidence_source(item)
    }
    changed_items: list[dict[str, Any]] = []
    same_items: list[dict[str, Any]] = []

    exact_left: dict[str, deque[str]] = defaultdict(deque)
    exact_right: dict[str, deque[str]] = defaultdict(deque)
    for item in left_fragments:
        exact_left[_equivalence_key(str(item["text"]))].append(str(item["id"]))
    for item in right_fragments:
        exact_right[_equivalence_key(str(item["text"]))].append(str(item["id"]))
    for value in sorted(set(exact_left) & set(exact_right)):
        while exact_left[value] and exact_right[value]:
            left_id = exact_left[value].popleft()
            right_id = exact_right[value].popleft()
            unused_left.discard(left_id)
            unused_right.discard(right_id)
            same_items.append(_paired_item(left_by_id[left_id], right_by_id[right_id]))

    unused_left -= advisory_left
    unused_right -= advisory_right

    # Помещение сопоставляется со своим помещением, а не с похожей строкой.
    left_rooms, left_code_of = _room_identity(left_fragments)
    right_rooms, right_code_of = _room_identity(right_fragments)
    for code in sorted(set(left_rooms) & set(right_rooms)):
        left_id, right_id = left_rooms[code], right_rooms[code]
        if left_id not in unused_left or right_id not in unused_right:
            continue
        unused_left.discard(left_id)
        unused_right.discard(right_id)
        changed_items.append(
            _changed_item(left_by_id[left_id], right_by_id[right_id])
        )

    left_keys: dict[str, list[str]] = defaultdict(list)
    right_keys: dict[str, list[str]] = defaultdict(list)
    for fragment_id in unused_left:
        key = stable_key(str(left_by_id[fragment_id]["text"]))
        if key:
            left_keys[key].append(fragment_id)
    for fragment_id in unused_right:
        key = stable_key(str(right_by_id[fragment_id]["text"]))
        if key:
            right_keys[key].append(fragment_id)
    for key in sorted(set(left_keys) & set(right_keys)):
        if len(left_keys[key]) != 1 or len(right_keys[key]) != 1:
            continue
        left_id, right_id = left_keys[key][0], right_keys[key][0]
        unused_left.discard(left_id)
        unused_right.discard(right_id)
        changed_items.append(_changed_item(left_by_id[left_id], right_by_id[right_id]))

    candidates = []
    ambiguous_pairs = []
    for left_id in unused_left:
        for right_id in unused_right:
            left_code = left_code_of.get(left_id)
            right_code = right_code_of.get(right_id)
            if left_code and right_code and left_code != right_code:
                # Два помещения с разными номерами — не одна строка, как бы
                # похожи ни были их названия.
                continue
            left, right = left_by_id[left_id], right_by_id[right_id]
            score = _candidate_score(left, right)
            raw_similarity = similarity(str(left["text"]), str(right["text"]))
            if score:
                candidates.append((score, left_id, right_id))
            elif raw_similarity >= AMBIGUITY_THRESHOLD:
                ambiguous_pairs.append((raw_similarity, left_id, right_id))
    for _, left_id, right_id in sorted(candidates, reverse=True):
        if left_id not in unused_left or right_id not in unused_right:
            continue
        unused_left.remove(left_id)
        unused_right.remove(right_id)
        changed_items.append(_changed_item(left_by_id[left_id], right_by_id[right_id]))

    ambiguous_items: list[dict[str, Any]] = []
    ambiguity_left: set[str] = set()
    ambiguity_right: set[str] = set()
    for _, left_id, right_id in sorted(ambiguous_pairs, reverse=True):
        if (
            left_id in unused_left and right_id in unused_right
            and left_id not in ambiguity_left and right_id not in ambiguity_right
        ):
            ambiguity_left.add(left_id)
            ambiguity_right.add(right_id)
            ambiguous_items.append(_paired_item(left_by_id[left_id], right_by_id[right_id]))
    order_left = {str(item["id"]): index for index, item in enumerate(left_fragments)}
    order_right = {str(item["id"]): index for index, item in enumerate(right_fragments)}
    changed_items.sort(key=lambda item: (
        min(item["left_pages"]), order_left[item["left_fragment_ids"][0]]
    ))
    same_items.sort(key=lambda item: (
        min(item["left_pages"]), order_left[item["left_fragment_ids"][0]]
    ))
    ambiguous_items.sort(key=lambda item: (
        min(item["left_pages"]), order_left[item["left_fragment_ids"][0]]
    ))
    removed_items = [
        _removed_item(left_by_id[fragment_id])
        for fragment_id in sorted(unused_left, key=order_left.get)
    ]
    added_items = [
        _added_item(right_by_id[fragment_id])
        for fragment_id in sorted(unused_right, key=order_right.get)
    ]
    return {
        "same": same_items,
        "changed": changed_items,
        "removed": removed_items,
        "added": added_items,
        "ambiguous": ambiguous_items,
        "ambiguity_count": len(ambiguous_items),
        "exact_equivalents": len(same_items),
    }


def source_signature(exclusions: dict[str, Any]) -> str:
    source = {
        "algorithm": ALGORITHM,
        "text_exclusion_contract_sha256": exclusions.get("contract_sha256"),
        "text_source_signature": exclusions.get("source_signature"),
    }
    encoded = json.dumps(source, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def build_text_differences(
    *, pair_id: str, generated_at: str, exclusions: dict[str, Any],
    comparison: dict[str, Any], links: list[dict[str, Any]],
    labels: dict[str, dict[int, str]] | None = None,
) -> dict[str, Any]:
    if not exclusions.get("valid", True):
        raise ValueError("text_exclusions_invalid")
    remaining = {
        side: {str(value) for value in (comparison.get("remaining") or {}).get(side, [])}
        for side in ("left", "right")
    }
    excluded = {
        side: {str(value) for value in (exclusions.get("excluded_fragment_ids") or {}).get(side, [])}
        for side in ("left", "right")
    }
    for side in ("left", "right"):
        if remaining[side] & excluded[side]:
            raise ValueError("remaining_contains_excluded_fragment")
    fragments = {
        side: [
            item for item in comparison.get("fragments", {}).get(side, [])
            if str(item.get("id")) in remaining[side]
            and not is_graphic_description(item)
        ]
        for side in ("left", "right")
    }
    labels = labels or {"left": {}, "right": {}}
    groups = []
    totals = {
        "sheet_groups_with_differences": 0, "changed": 0,
        "removed": 0, "added": 0, "model_ambiguity": 0,
        "model_failures": 0,
    }
    for link in links:
        left_pages = sorted({int(page) for page in link.get("left_pages") or []})
        right_pages = sorted({int(page) for page in link.get("right_pages") or []})
        left_page_set, right_page_set = set(left_pages), set(right_pages)
        result = compare_group(
            [item for item in fragments["left"] if int(item["pdf_page"]) in left_page_set],
            [item for item in fragments["right"] if int(item["pdf_page"]) in right_page_set],
        )
        if not any(result[bucket] for bucket in ("changed", "removed", "added")):
            continue
        group = {
            "id": str(link.get("id") or ""),
            "left_pages": left_pages,
            "right_pages": right_pages,
            "left_labels": [labels.get("left", {}).get(page) or f"Страница {page}" for page in left_pages],
            "right_labels": [labels.get("right", {}).get(page) or f"Страница {page}" for page in right_pages],
            "changed": result["changed"],
            "removed": result["removed"],
            "added": result["added"],
            "deterministic_same": result["same"],
            "deterministic_ambiguities": result["ambiguous"],
            "ambiguity_count": result["ambiguity_count"],
            "exact_equivalents": result["exact_equivalents"],
        }
        groups.append(group)
        totals["sheet_groups_with_differences"] += 1
        for bucket in ("changed", "removed", "added"):
            totals[bucket] += len(result[bucket])
        totals["model_ambiguity"] += int(result["ambiguity_count"])
    return {
        "version": VERSION,
        "kind": KIND,
        "pair_id": pair_id,
        "algorithm": ALGORITHM,
        "production_path": PRODUCTION_PATH,
        "generated_at": generated_at,
        "source_signature": source_signature(exclusions),
        "sheet_groups": groups,
        "summary": totals,
        "model": {
            "used": False,
            "failures": 0,
            "reason": "benchmark_did_not_improve_deterministic_baseline",
        },
        "constraints": {
            "factual_differences_only": True,
            "graphics_analyzed": False,
            "engineering_findings_created": False,
            "same_on_linked_sheet_reanalyzed": False,
            "found_on_other_sheet_reanalyzed": False,
            "one_row_per_sheet_group": True,
        },
    }


def public_view(payload: dict[str, Any] | None, *, stale: bool = False) -> dict[str, Any] | None:
    if (
        not isinstance(payload, dict)
        or payload.get("version") != VERSION
        or payload.get("kind") != KIND
    ):
        return None
    return {**payload, "stale": bool(stale)}


def validate_model_response(
    payload: Any, *, left_fragments: list[dict[str, Any]],
    right_fragments: list[dict[str, Any]],
) -> dict[str, Any]:
    """Validate a future model result strictly; any defect fails closed."""
    if not isinstance(payload, dict) or set(payload) != {"changed", "removed", "added"}:
        raise ValueError("invalid_model_response")
    definitions = {
        "changed": ({"left_ids", "right_ids", "summary", "before", "after"}, ("left_ids", "right_ids")),
        "removed": ({"left_ids", "summary", "before"}, ("left_ids",)),
        "added": ({"right_ids", "summary", "after"}, ("right_ids",)),
    }
    source = {
        "left_ids": {str(item["id"]): str(item["text"]) for item in left_fragments},
        "right_ids": {str(item["id"]): str(item["text"]) for item in right_fragments},
    }
    used = {"left_ids": set(), "right_ids": set()}
    for bucket, (required, id_fields) in definitions.items():
        items = payload.get(bucket)
        if not isinstance(items, list):
            raise ValueError("invalid_model_response")
        for item in items:
            if not isinstance(item, dict) or set(item) != required:
                raise ValueError("invalid_model_response")
            if not isinstance(item.get("summary"), str) or _AUDIT_LANGUAGE_RE.search(item["summary"]):
                raise ValueError("invalid_model_response")
            referenced_text = {"left_ids": "", "right_ids": ""}
            for field in id_fields:
                ids = item.get(field)
                if not isinstance(ids, list) or not ids or len(ids) != len(set(ids)):
                    raise ValueError("invalid_model_response")
                if any(not isinstance(value, str) or value not in source[field] for value in ids):
                    raise ValueError("hallucinated_fragment_id")
                if used[field] & set(ids):
                    raise ValueError("reused_fragment_id")
                used[field].update(ids)
                referenced_text[field] = "\n".join(source[field][value] for value in ids)
            if "before" in item and item["before"] != referenced_text["left_ids"]:
                raise ValueError("model_before_not_verbatim")
            if "after" in item and item["after"] != referenced_text["right_ids"]:
                raise ValueError("model_after_not_verbatim")
            supported_numbers = _NUMBER_RE.findall(
                referenced_text["left_ids"] + " " + referenced_text["right_ids"]
            )
            if not set(_NUMBER_RE.findall(item["summary"])) <= set(supported_numbers):
                raise ValueError("hallucinated_value")
    return payload


__all__ = [
    "ALGORITHM", "KIND", "PRODUCTION_PATH", "VERSION",
    "build_text_differences", "canonicalize", "compare_group", "is_graphic_description", "public_view",
    "similarity", "source_signature", "stable_key", "validate_model_response",
]
