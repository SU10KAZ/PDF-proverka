"""Lossless, readable source dictionaries; never summarise or normalise evidence.

Native PDF text often repeats identical lines. Store every distinct line once
with its ordered run counts, encode long layout padding runs by exact counts,
and share long identical strings across fields.
An exact local round trip is mandatory. Unique inputs retain their old format.
"""
from __future__ import annotations

from collections import Counter
import hashlib
import json
import re
from typing import Any

VERSION = "projectchange_v3_source_dictionary/2"
REF = "__source_text_ref__"
LINES = "__source_text_lines__"
SEGMENTS = "__source_text_segments__"
_PADDING = re.compile(r" {32,}|\t{32,}")
MIN_SAVING_BYTES = 4096
NOTE = (
    "SOURCE DATA uses a lossless text dictionary. The actual page/region data is in data. "
    "An object with __source_text_ref__ means the exact value from strings at that key. "
    "An object with __source_text_lines__ encodes one text: concatenate lines[index] count times "
    "for each [index,count] in runs, in order. Line endings are preserved in lines. "
    "An object with __source_text_segments__ encodes one text as ordered segments: "
    "a string is literal text; [whitespace,count] repeats that exact space or tab count times. "
    "For example [\"A\",[\" \",100],\"B\\n\"] means A, 100 spaces, then B and a newline. "
    "These counts preserve PDF layout padding; they are not quantities in the drawing. "
    "Repeated occurrences are retained by their counts and order, not removed. "
    "Use the readable text in the dictionary as source evidence; references are not missing values."
)


def _json(value):
    return json.dumps(value, ensure_ascii=False)


def _bytes(value):
    return len(_json(value).encode("utf-8"))


def _smaller(before, after):
    return after <= before * 0.8 and before - after >= MIN_SAVING_BYTES


def _native(text):
    if len(text) < MIN_SAVING_BYTES:
        return text
    lines, indices, runs = [], {}, []
    for line in text.splitlines(keepends=True):
        if line not in indices:
            indices[line] = len(lines)
            lines.append(line)
        index = indices[line]
        if runs and runs[-1][0] == index:
            runs[-1][1] += 1
        else:
            runs.append([index, 1])
    candidates = [{LINES: {"lines": lines, "runs": runs}}]
    # Sorted PDF extraction can turn horizontal distances into hundreds of
    # literal spaces per line. Unique lines do not benefit from line deduping.
    # Encode only exact homogeneous padding runs, keeping all glyphs, line
    # endings, and the precise amount and kind of whitespace reversible.
    segments, start = [], 0
    for match in _PADDING.finditer(text):
        if match.start() > start:
            segments.append(text[start:match.start()])
        segments.append([match[0][0], len(match[0])])
        start = match.end()
    if segments:
        if start < len(text):
            segments.append(text[start:])
        candidates.append({SEGMENTS: segments})
    encoded = min(candidates, key=_bytes)
    return encoded if _smaller(_bytes(text), _bytes(encoded)) else text


def _has_reserved(value):
    if isinstance(value, dict):
        return bool({REF, LINES, SEGMENTS} & value.keys()) or any(_has_reserved(v) for v in value.values())
    if isinstance(value, list):
        return any(_has_reserved(v) for v in value)
    return False


def decode(encoded: dict[str, Any]) -> Any:
    """Local audit decoder, not a tool requested from the model."""
    def expand(value):
        if isinstance(value, dict):
            if set(value) == {REF}:
                return encoded["strings"][value[REF]]
            if set(value) == {LINES}:
                content = value[LINES]
                return "".join(expand(content["lines"][i]) * count for i, count in content["runs"])
            if set(value) == {SEGMENTS}:
                return "".join(segment[0] * segment[1] if isinstance(segment, list) else expand(segment)
                               for segment in value[SEGMENTS])
            return {key: expand(v) for key, v in value.items()}
        if isinstance(value, list):
            return [expand(v) for v in value]
        return value

    return expand(encoded["data"])


def compact(data: Any) -> tuple[Any, dict[str, Any]]:
    """Apply only when it materially saves bytes, and prove exact source equivalence."""
    if not isinstance(data, dict) or not isinstance(data.get("pages"), list):
        return data, {}
    raw = _json(data)
    raw_bytes = len(raw.encode("utf-8"))
    if raw_bytes < 2 * MIN_SAVING_BYTES or _has_reserved(data):
        return data, {}

    def native(value):
        if isinstance(value, dict):
            return {k: _native(v) if k == "native_page_text" and isinstance(v, str) else native(v)
                    for k, v in value.items()}
        if isinstance(value, list):
            return [native(v) for v in value]
        return value

    packed = native(data)
    counts = Counter()

    def count(value):
        if isinstance(value, str) and len(value) >= 256:
            counts[value] += 1
        elif isinstance(value, dict):
            for v in value.values():
                count(v)
        elif isinstance(value, list):
            for v in value:
                count(v)

    count(packed)
    strings = {f"T{i}": value for i, (value, n) in enumerate(counts.items(), 1) if n > 1}
    ids = {value: key for key, value in strings.items()}

    def references(value):
        if isinstance(value, str) and value in ids:
            return {REF: ids[value]}
        if isinstance(value, dict):
            return {k: references(v) for k, v in value.items()}
        if isinstance(value, list):
            return [references(v) for v in value]
        return value

    result = {"source_encoding": VERSION, "note": NOTE, "strings": strings, "data": references(packed)}
    encoded_bytes = _bytes(result)
    if not _smaller(raw_bytes, encoded_bytes):
        return data, {}
    if decode(result) != data:
        raise ValueError("lossless source dictionary round trip failed")
    return result, {"version": VERSION, "raw_source_bytes": raw_bytes,
                    "encoded_source_bytes": encoded_bytes, "saved_source_bytes": raw_bytes - encoded_bytes,
                    "raw_source_sha256": hashlib.sha256(raw.encode("utf-8")).hexdigest(),
                    "roundtrip_verified": True}
