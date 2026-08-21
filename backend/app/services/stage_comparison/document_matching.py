"""Fast approximate one-to-one pairing for P and RD document filenames."""
from __future__ import annotations

import re
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any


MIN_DOCUMENT_SIMILARITY = 0.72
_MARKERS = {
    "ар", "вк", "вт", "икео", "иос", "кр", "ов", "оди", "оос",
    "па", "пос", "пз", "пзу", "тх", "ээ",
}
_NOISE_WORDS = {"v", "версия", "из", "итог", "копия", "корр", "страница", "том"}
_TOKEN_RE = re.compile(r"[a-zа-я]+|\d+(?:\.\d+)*", re.IGNORECASE)
_NUMBER_RE = re.compile(r"^\d+(?:\.\d+)*$")


def _name_features(filename: str) -> tuple[str, list[tuple[str, str | None]]]:
    stem = Path(str(filename or "")).stem.casefold().replace("ё", "е")
    tokens = _TOKEN_RE.findall(stem)
    groups: list[tuple[str, str | None]] = []
    for index, token in enumerate(tokens):
        if token not in _MARKERS:
            continue
        number = tokens[index + 1] if index + 1 < len(tokens) and _NUMBER_RE.fullmatch(tokens[index + 1]) else None
        groups.append((token, number))
    compact_tokens: list[str] = []
    for index, token in enumerate(tokens):
        if token in _NOISE_WORDS:
            continue
        if token == "v" and index + 1 < len(tokens) and _NUMBER_RE.fullmatch(tokens[index + 1]):
            continue
        if index > 0 and tokens[index - 1] == "v" and _NUMBER_RE.fullmatch(token):
            continue
        if re.fullmatch(r"v\d+", token):
            continue
        compact_tokens.append(token)
    return "".join(compact_tokens), groups


def document_name_similarity(left_filename: str, right_filename: str) -> float:
    """Score cosmetic/revision-tolerant name similarity without semantic models."""
    left_compact, left_groups = _name_features(left_filename)
    right_compact, right_groups = _name_features(right_filename)
    if not left_compact or not right_compact:
        return 0.0
    character_similarity = SequenceMatcher(None, left_compact, right_compact).ratio()
    same_markers = [
        (left_marker, left_number, right_number)
        for left_marker, left_number in left_groups
        for right_marker, right_number in right_groups
        if left_marker == right_marker
    ]
    if left_groups and right_groups and not same_markers:
        return round(min(character_similarity, 0.52), 4)
    if not same_markers:
        return round(character_similarity, 4)
    marker_scores: list[float] = []
    for _marker, left_number, right_number in same_markers:
        if left_number and right_number:
            marker_scores.append(
                0.96 + 0.04 * character_similarity
                if left_number == right_number
                else min(character_similarity, 0.58)
            )
        else:
            marker_scores.append(0.88 + 0.12 * character_similarity)
    return round(max(marker_scores), 4)


def _maximum_weight_assignment(weights: list[list[float]]) -> list[tuple[int, int]]:
    """Return a maximum-weight one-to-one assignment using O(n³) Hungarian."""
    if not weights or not weights[0]:
        return []
    rows = len(weights)
    columns = len(weights[0])
    size = max(rows, columns)
    padded = [
        [weights[row][column] if row < rows and column < columns else 0.0 for column in range(size)]
        for row in range(size)
    ]
    u = [0.0] * (size + 1)
    v = [0.0] * (size + 1)
    matched_row = [0] * (size + 1)
    previous_column = [0] * (size + 1)
    for row in range(1, size + 1):
        matched_row[0] = row
        column0 = 0
        min_value = [float("inf")] * (size + 1)
        used = [False] * (size + 1)
        while True:
            used[column0] = True
            row0 = matched_row[column0]
            delta = float("inf")
            column1 = 0
            for column in range(1, size + 1):
                if used[column]:
                    continue
                cost = 1.0 - padded[row0 - 1][column - 1]
                current = cost - u[row0] - v[column]
                if current < min_value[column]:
                    min_value[column] = current
                    previous_column[column] = column0
                if min_value[column] < delta:
                    delta = min_value[column]
                    column1 = column
            for column in range(size + 1):
                if used[column]:
                    u[matched_row[column]] += delta
                    v[column] -= delta
                else:
                    min_value[column] -= delta
            column0 = column1
            if matched_row[column0] == 0:
                break
        while True:
            column1 = previous_column[column0]
            matched_row[column0] = matched_row[column1]
            column0 = column1
            if column0 == 0:
                break
    assignment = []
    for column in range(1, size + 1):
        row = matched_row[column] - 1
        if 0 <= row < rows and column - 1 < columns:
            assignment.append((row, column - 1))
    return assignment


def suggest_document_pairing(
    left_documents: list[dict[str, Any]],
    right_documents: list[dict[str, Any]],
) -> dict[str, Any]:
    """Put confident approximate pairs first and all unmatched documents last."""
    weights = [
        [
            document_name_similarity(
                str(left.get("filename") or Path(str(left.get("pdf_path") or "")).name),
                str(right.get("filename") or Path(str(right.get("pdf_path") or "")).name),
            )
            for right in right_documents
        ]
        for left in left_documents
    ]
    accepted = [
        (left_index, right_index, weights[left_index][right_index])
        for left_index, right_index in _maximum_weight_assignment(weights)
        if weights[left_index][right_index] >= MIN_DOCUMENT_SIMILARITY
    ]
    accepted.sort(key=lambda item: item[0])
    matched_left = {item[0] for item in accepted}
    matched_right = {item[1] for item in accepted}
    unmatched_left = [index for index in range(len(left_documents)) if index not in matched_left]
    unmatched_right = [index for index in range(len(right_documents)) if index not in matched_right]

    left_order = [str(left_documents[left]["pdf_path"]) for left, _right, _score in accepted]
    right_order = [str(right_documents[right]["pdf_path"]) for _left, right, _score in accepted]
    # Do not place unrelated leftovers opposite each other: each unmatched
    # document gets its own incomplete row at the bottom for explicit manual work.
    for left_index in unmatched_left:
        left_order.append(str(left_documents[left_index]["pdf_path"]))
        right_order.append(None)
    for right_index in unmatched_right:
        left_order.append(None)
        right_order.append(str(right_documents[right_index]["pdf_path"]))
    matches = [
        {
            "left_pdf": str(left_documents[left]["pdf_path"]),
            "right_pdf": str(right_documents[right]["pdf_path"]),
            "similarity": score,
        }
        for left, right, score in accepted
    ]
    return {
        "left_order": left_order,
        "right_order": right_order,
        "confirmed_pairs": [
            {"left_pdf": item["left_pdf"], "right_pdf": item["right_pdf"]} for item in matches
        ],
        "matches": matches,
        "matched_count": len(matches),
        "unmatched_left_count": len(unmatched_left),
        "unmatched_right_count": len(unmatched_right),
    }


__all__ = [
    "MIN_DOCUMENT_SIMILARITY",
    "document_name_similarity",
    "suggest_document_pairing",
]
