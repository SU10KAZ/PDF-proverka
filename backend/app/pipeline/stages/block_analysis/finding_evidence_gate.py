"""Evidence-first publication gate for Stage 01 findings.

The gate does not delete model candidates.  It splits them into publishable and
deferred sets, annotating every deferred item with deterministic reasons.  This
prevents lack of context, recommendations and speculative checks from becoming
customer-facing findings while preserving them for later inspection/retrieval.
"""
from __future__ import annotations

import re
import unicodedata
from collections.abc import Mapping
from typing import Any


PUBLISHABLE_CLAIM_TYPES = frozenset({
    "direct_violation", "contradiction", "explicit_omission",
})
_CONTROL_FIELDS = frozenset({
    "claim_type", "affected_entity", "evidence_quote", "evidence_kind",
    "context_status", "confidence", "counterevidence_checked", "required_context",
})
_SPECULATIVE_RE = re.compile(
    r"\b(?:если|возможно|вероятно|может|могут|предполож|необходимо уточнить|"
    r"требуется уточнить|требуется проверить|без сверки|не исключено)\b",
    re.IGNORECASE,
)
_INTERNAL_METADATA_RE = re.compile(
    r"(?:переданн\w*\s+(?:текстов\w*\s+)?(?:разметк|контекст)|"
    r"текстов\w*[/\-]?экспортн\w*\s+разметк|сопровождающ\w*\s+разметк|"
    r"исправить\s+(?:наименование\s+и\s+)?тип\s+фрагмент|"
    r"исправить\s+.*(?:индексац|разметк)|контекст\s+блока\s+определяет)",
    re.IGNORECASE,
)
_VAGUE_COUNT_RE = re.compile(
    r"(?:многочисленн|существенно\s+больше|явно\s+больше|значительно\s+больше)",
    re.IGNORECASE,
)
_SEVERITY_RANK = {
    "КРИТИЧЕСКОЕ": 5,
    "ЭКОНОМИЧЕСКОЕ": 4,
    "ЭКСПЛУАТАЦИОННОЕ": 3,
    "ПРОВЕРИТЬ ПО СМЕЖНЫМ": 2,
    "РЕКОМЕНДАТЕЛЬНОЕ": 1,
}


# Отдельный namespace: текущий publication gate несколько раз целиком
# перезаписывает _evidence_gate. Observe-only receipt не должен менять ни
# его причины, ни публикацию finding.
SYMBOL_EVIDENCE_FIELD = "_finding_evidence_observations"

# Канон — кириллица из PDF-векторного слоя. Таблица намеренно мала и явна:
# цифро-буквенные OCR-догадки (0/O, 1/I, P/П, I/И) сюда не входят.
HOMOGLYPH_TO_CYRILLIC = {
    "A": "А",
    "B": "В",
    "C": "С",
    "D": "Д",
    "E": "Е",
    "H": "Н",
    "K": "К",
    "M": "М",
    "O": "О",
    "P": "Р",
    "R": "Р",
    "T": "Т",
    "U": "У",
    "V": "В",
    "X": "Х",
    "Y": "У",
}
_HOMOGLYPH_TRANSLATION = str.maketrans(HOMOGLYPH_TO_CYRILLIC)
_DASH_TRANSLATION = str.maketrans({
    "‐": "-", "‑": "-", "‒": "-", "–": "-", "—": "-", "−": "-",
})
# Доменный OCR alias из живых документов. Это whole-token правило: общий
# посимвольный У→Ч был бы опасен.
_TOKEN_ALIASES = {
    "УУРИО": "УЧРИО",
    "УЧРИО": "УЧРИО",
}
_OUTER_QUOTES = " \t\r\n«»„“”\"'\x60"
_IDENTIFIER_CHARS = "A-Za-zА-Яа-яЁё0-9"
_IDENTIFIER_TOKEN_RE = re.compile(
    rf"(?<![{_IDENTIFIER_CHARS}])"
    rf"(?P<token>[{_IDENTIFIER_CHARS}]"
    rf"(?:[{_IDENTIFIER_CHARS}./_()\-]{{0,62}}"
    rf"[{_IDENTIFIER_CHARS})])?)"
    rf"(?![{_IDENTIFIER_CHARS}])"
)
_MARK_TOKEN_RE = re.compile(
    rf"(?<![{_IDENTIFIER_CHARS}])"
    rf"(?P<token>[A-ZА-ЯЁ]{{1,4}}-?\d+(?:\.\d+)*)"
    rf"(?![{_IDENTIFIER_CHARS}])",
    re.IGNORECASE,
)
_QUOTED_RE = re.compile(
    r"«([^»\n]{1,80})»|“([^”\n]{1,80})”|\"([^\"\n]{1,80})\"|"
    r"\x60([^\x60\n]{1,80})\x60"
)
_PAIR_RE = re.compile(
    rf"(?<![{_IDENTIFIER_CHARS}])"
    rf"(?P<left>[{_IDENTIFIER_CHARS}]"
    rf"(?:[{_IDENTIFIER_CHARS}./_()\-]{{0,62}}[{_IDENTIFIER_CHARS})])?)"
    rf"\s*(?:→|⇒|->|вместо|против)\s*"
    rf"(?P<right>[{_IDENTIFIER_CHARS}]"
    rf"(?:[{_IDENTIFIER_CHARS}./_()\-]{{0,62}}[{_IDENTIFIER_CHARS})])?)"
    rf"(?![{_IDENTIFIER_CHARS}])",
    re.IGNORECASE,
)
_SCRIPT_MISMATCH_RE = re.compile(
    r"(?:латиниц|кириллиц|алфавит|гомоглиф|ocr|распознав|"
    r"вперемешку|смешен\w*\s+(?:букв|символ|алфавит|обознач|марк))",
    re.IGNORECASE,
)
_DESIGNATION_MISMATCH_RE = re.compile(
    r"(?:несоответств|расхожд|различ|противореч)\w*.{0,40}"
    r"(?:обознач|марк|символ|букв)|"
    r"(?:обознач|марк|символ|букв)\w*.{0,40}"
    r"(?:несоответств|расхожд|различ|противореч|вместо)",
    re.IGNORECASE,
)
_ABSENCE_RE = re.compile(
    r"\b(?:отсутств\w*|нет|не\s+(?:указ|привед|нанес|обознач|"
    r"подпис|найд|задан|показ)\w*)\b",
    re.IGNORECASE,
)
_SPECULATIVE_ABSENCE_RE = re.compile(
    r"\b(?:возможно|вероятно|предполож|необходимо\s+уточнить|"
    r"требуется\s+(?:уточнить|проверить)|проверить\s+наличие|"
    r"неоднознач|неполно)\b",
    re.IGNORECASE,
)


def _spelling_key(value: Any) -> str:
    """NFKC/case/dash normalization without changing the alphabet."""
    text = unicodedata.normalize("NFKC", str(value or ""))
    text = text.strip(_OUTER_QUOTES).translate(_DASH_TRANSLATION).upper()
    return re.sub(r"\s+", " ", text).strip()


def normalize_homoglyph_token(value: Any) -> str:
    """Return a conservative, offset-independent canonical token.

    Internal dots, hyphens, slashes and parentheses are preserved. Therefore
    П11.1 never collapses into П111 and П1 never equals П-1.
    """
    text = _spelling_key(value).replace("×", "Х")
    text = text.translate(_HOMOGLYPH_TRANSLATION)
    return _TOKEN_ALIASES.get(text, text)


def _clean_identifier(value: Any) -> str:
    return str(value or "").strip(_OUTER_QUOTES + " ,;:")


def _is_identifier(value: Any, *, allow_single: bool = False) -> bool:
    token = _clean_identifier(value)
    match = _IDENTIFIER_TOKEN_RE.fullmatch(token)
    if not match or match.group("token") != token or len(token) > 64:
        return False
    letters = [char for char in token if char.isalpha()]
    if not letters or token.isdigit():
        return False
    if any(char.isdigit() for char in token):
        return True
    if _spelling_key(token) in _TOKEN_ALIASES:
        return True
    if len(letters) == 1:
        return allow_single and letters[0].isupper()
    return all(not char.isalpha() or char.isupper() for char in token)


def _quoted_values(text: str) -> list[tuple[str, int, int]]:
    values: list[tuple[str, int, int]] = []
    for match in _QUOTED_RE.finditer(text or ""):
        value = next((group for group in match.groups() if group is not None), "")
        values.append((_clean_identifier(value), match.start(), match.end()))
    return values


def _token_matches(text: str) -> list[re.Match[str]]:
    """Token spans plus mark endpoints inside ranges such as D11-D17."""
    matches = list(_IDENTIFIER_TOKEN_RE.finditer(text or ""))
    matches.extend(_MARK_TOKEN_RE.finditer(text or ""))
    matches.sort(key=lambda match: (match.start("token"), match.end("token")))
    return matches


def _identifiers_in_text(
    text: str, *, allow_single: bool = False,
) -> list[str]:
    values: list[str] = []
    seen_spans: set[tuple[int, int, str]] = set()
    for match in _token_matches(text):
        token = _clean_identifier(match.group("token"))
        signature = (match.start("token"), match.end("token"), token)
        if (
            signature not in seen_spans
            and _is_identifier(token, allow_single=allow_single)
        ):
            seen_spans.add(signature)
            values.append(token)
    return values


def _claim_text(item: Mapping[str, Any]) -> str:
    return "\n".join(
        str(item.get(key) or "")
        for key in ("finding", "problem", "description", "evidence_quote")
        if item.get(key)
    )


def _mismatch_pairs(item: Mapping[str, Any]) -> list[tuple[str, str]]:
    text = _claim_text(item)
    if not (
        _SCRIPT_MISMATCH_RE.search(text)
        or _DESIGNATION_MISMATCH_RE.search(text)
    ):
        return []

    raw_pairs: list[tuple[str, str]] = []
    quoted = [
        value
        for value, _, _ in _quoted_values(text)
        if _is_identifier(value, allow_single=True)
    ]
    raw_pairs.extend(zip(quoted, quoted[1:]))
    for match in _PAIR_RE.finditer(text):
        left, right = match.group("left"), match.group("right")
        if (
            _is_identifier(left, allow_single=True)
            and _is_identifier(right, allow_single=True)
        ):
            raw_pairs.append((left, right))

    # Global mark-system findings list the two alphabets in separate sentences.
    # Only the explicit script/alphabet wording permits this grouped scan.
    if _SCRIPT_MISMATCH_RE.search(text):
        groups: dict[str, list[str]] = {}
        for token in _identifiers_in_text(text):
            canonical = normalize_homoglyph_token(token)
            spelling = _spelling_key(token)
            if not canonical or not spelling:
                continue
            known = groups.setdefault(canonical, [])
            if spelling not in {_spelling_key(value) for value in known}:
                known.append(token)
        for tokens in groups.values():
            if len(tokens) >= 2:
                raw_pairs.append((tokens[0], tokens[1]))

    result: list[tuple[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for left, right in raw_pairs:
        left_spelling = _spelling_key(left)
        right_spelling = _spelling_key(right)
        canonical = normalize_homoglyph_token(left)
        if (
            not canonical
            or canonical != normalize_homoglyph_token(right)
            or left_spelling == right_spelling
            or (
                canonical == left_spelling
                and canonical == right_spelling
            )
        ):
            continue
        key = (canonical, left_spelling, right_spelling)
        if key in seen:
            continue
        seen.add(key)
        result.append((_clean_identifier(left), _clean_identifier(right)))
        if len(result) >= 8:
            break
    return result


def _page_key(value: Any) -> str:
    if value is None or isinstance(value, bool):
        return ""
    return str(value).strip()


def _append_unique(values: list[str], value: Any) -> None:
    rendered = str(value or "").strip()
    if rendered and rendered not in values:
        values.append(rendered)


def _append_pages(values: list[Any], value: Any) -> None:
    if isinstance(value, (list, tuple, set)):
        for part in value:
            _append_pages(values, part)
        return
    if not _page_key(value):
        return
    if _page_key(value) not in {_page_key(page) for page in values}:
        values.append(value)


def _graph_block_pages(document_graph: Any) -> dict[str, Any]:
    if not isinstance(document_graph, Mapping):
        return {}
    result: dict[str, Any] = {}
    for page in document_graph.get("pages") or []:
        if not isinstance(page, Mapping):
            continue
        page_number = page.get("page")
        if page_number is None:
            page_index = page.get("page_index")
            if isinstance(page_index, int):
                page_number = page_index + 1
        for field in ("text_blocks", "image_blocks"):
            for block in page.get(field) or []:
                if not isinstance(block, Mapping):
                    continue
                block_id = block.get("id") or block.get("block_id")
                if block_id:
                    result[str(block_id)] = page_number
    return result


def _finding_context(
    item: Mapping[str, Any],
    *,
    target_block_id: Any = None,
    target_page: Any = None,
    block_pages: Mapping[str, Any],
) -> tuple[list[str], list[Any], str | None]:
    block_ids: list[str] = []
    pages: list[Any] = []
    primary_target = str(target_block_id or "").strip() or None

    # A block-stage call is deliberately local even if retrieval evidence from
    # other sheets is present in the finding.
    if primary_target is not None or target_page is not None:
        _append_unique(block_ids, primary_target)
        _append_pages(pages, target_page)
    else:
        for key in (
            "block_evidence", "source_block_ids", "related_block_ids", "block_ids",
        ):
            value = item.get(key)
            if isinstance(value, (list, tuple, set)):
                for block_id in value:
                    _append_unique(block_ids, block_id)
            else:
                _append_unique(block_ids, value)
        _append_pages(pages, item.get("page"))
        for evidence in item.get("evidence") or []:
            if not isinstance(evidence, Mapping):
                continue
            _append_unique(block_ids, evidence.get("block_id"))
            _append_pages(pages, evidence.get("page"))

    for block_id in block_ids:
        _append_pages(pages, block_pages.get(block_id))
    return block_ids, pages, primary_target


def _ordered_vector_sources(
    item: Mapping[str, Any],
    *,
    vector_sources: Any,
    document_graph: Any,
    target_block_id: Any = None,
    target_page: Any = None,
) -> list[dict[str, Any]]:
    if not isinstance(vector_sources, Mapping):
        return []
    block_pages = _graph_block_pages(document_graph)
    records: dict[str, dict[str, Any]] = {}
    for raw_block_id, raw_record in vector_sources.items():
        block_id = str(raw_block_id or "").strip()
        if not block_id:
            continue
        if isinstance(raw_record, str):
            text = raw_record
            record: dict[str, Any] = {}
        elif isinstance(raw_record, Mapping):
            text = str(raw_record.get("text") or "")
            record = dict(raw_record)
        else:
            continue
        if not text.strip():
            continue
        record.update({
            "block_id": block_id,
            "text": text,
            "page": record.get("page", block_pages.get(block_id)),
        })
        records[block_id] = record

    explicit_ids, pages, primary_target = _finding_context(
        item,
        target_block_id=target_block_id,
        target_page=target_page,
        block_pages={
            **block_pages,
            **{
                block_id: record.get("page")
                for block_id, record in records.items()
                if record.get("page") is not None
            },
        },
    )
    for block_id in explicit_ids:
        record = records.get(block_id)
        if record is not None:
            _append_pages(pages, record.get("page"))

    ordered: list[dict[str, Any]] = []
    seen: set[str] = set()
    for block_id in explicit_ids:
        record = records.get(block_id)
        if record is None or block_id in seen:
            continue
        current = dict(record)
        current["scope"] = (
            "target_block" if primary_target == block_id else "referenced_block"
        )
        ordered.append(current)
        seen.add(block_id)

    explicit_set = set(explicit_ids)
    for page in pages:
        page_ids = sorted(
            block_id
            for block_id, record in records.items()
            if _page_key(record.get("page")) == _page_key(page)
        )
        for block_id in page_ids:
            if block_id in seen:
                continue
            current = dict(records[block_id])
            current["scope"] = (
                "same_page_neighbor" if explicit_set else "same_page_block"
            )
            ordered.append(current)
            seen.add(block_id)
    return ordered


def _vector_hits(
    canonical_token: str,
    sources: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    hits: list[dict[str, Any]] = []
    for source in sources:
        text = str(source.get("text") or "")
        for match in _token_matches(text):
            raw_token = match.group("token")
            if normalize_homoglyph_token(raw_token) != canonical_token:
                continue
            start, end = match.start("token"), match.end("token")
            line_start = text.rfind("\n", 0, start) + 1
            line_end = text.find("\n", end)
            if line_end < 0:
                line_end = len(text)
            evidence_start, evidence_end = line_start, line_end
            if evidence_end - evidence_start > 500:
                evidence_start = max(line_start, start - 200)
                evidence_end = min(line_end, end + 200)
            hits.append({
                "source": "pdf_vector_text",
                "block_id": source.get("block_id"),
                "page": source.get("page"),
                "block_kind": source.get("block_kind"),
                "scope": source.get("scope"),
                "matched_text": raw_token,
                "normalized_token": canonical_token,
                "evidence_text": text[evidence_start:evidence_end],
                "evidence_start_offset": evidence_start,
                "offset_start": start,
                "offset_end": end,
                "offset_in_line": start - line_start,
                "line_no": text.count("\n", 0, start) + 1,
                "offset_basis": "block_vector_text_unicode_chars",
                "_spelling": _spelling_key(raw_token),
            })
    return hits


def _ocr_artifact_observation(
    item: Mapping[str, Any],
    sources: list[dict[str, Any]],
) -> dict[str, Any] | None:
    for left, right in _mismatch_pairs(item):
        canonical = normalize_homoglyph_token(left)
        hits = _vector_hits(canonical, sources)
        if not hits:
            continue
        vector_spellings = {str(hit.get("_spelling") or "") for hit in hits}
        claimed_spellings = {_spelling_key(left), _spelling_key(right)}
        # Если в точном PDF реально есть обе формы, смешение настоящее.
        if (
            len(vector_spellings) != 1
            or not vector_spellings.issubset(claimed_spellings)
        ):
            continue
        truth_spelling = next(iter(vector_spellings))
        evidence = next(
            hit for hit in hits if hit.get("_spelling") == truth_spelling
        )
        evidence = {
            key: value for key, value in evidence.items() if key != "_spelling"
        }
        return {
            "schema_version": 1,
            "status": "ocr_artifact",
            "rule": "homoglyph_equivalent_vector_truth",
            "quoted_tokens": [left, right],
            "normalized_token": canonical,
            "vector_truth_token": evidence.get("matched_text"),
            "normalization_applied": True,
            "observe_only": True,
            "vector_evidence": evidence,
        }
    return None


def _absence_candidates(item: Mapping[str, Any]) -> list[str]:
    fields = [
        str(item.get(key) or "")
        for key in (
            "finding", "problem", "description", "evidence_quote",
        )
        if item.get(key)
    ]
    combined = "\n".join(fields)
    if not _ABSENCE_RE.search(combined):
        return []
    if _SPECULATIVE_ABSENCE_RE.search(combined):
        return []

    candidates: list[str] = []
    for text in fields:
        quotes = _quoted_values(text)
        tokens = list(_IDENTIFIER_TOKEN_RE.finditer(text))
        for predicate in _ABSENCE_RE.finditer(text):
            sentence_start = max(
                text.rfind("\n", 0, predicate.start()),
                text.rfind(".", 0, predicate.start()),
                text.rfind(";", 0, predicate.start()),
            ) + 1
            ends = [
                pos for pos in (
                    text.find("\n", predicate.end()),
                    text.find(".", predicate.end()),
                    text.find(";", predicate.end()),
                )
                if pos >= 0
            ]
            sentence_end = min(ends) if ends else len(text)
            local_quotes = [
                (value, start, end)
                for value, start, end in quotes
                if start >= sentence_start
                and end <= sentence_end
                and _is_identifier(value, allow_single=True)
            ]
            if local_quotes:
                value, _, _ = min(
                    local_quotes,
                    key=lambda row: min(
                        abs(row[1] - predicate.end()),
                        abs(predicate.start() - row[2]),
                    ),
                )
                _append_unique(candidates, value)
                continue

            after = [
                match.group("token")
                for match in tokens
                if predicate.end() <= match.start("token") < sentence_end
                and _is_identifier(match.group("token"), allow_single=True)
            ]
            if after:
                _append_unique(candidates, after[0])
                continue
            before = [
                match.group("token")
                for match in tokens
                if sentence_start <= match.start("token") < predicate.start()
                and _is_identifier(match.group("token"), allow_single=True)
            ]
            if before:
                _append_unique(candidates, before[-1])

    # affected_entity="дверь Д16" — допустимый fallback; value_found не
    # используется, потому что в omission-findings там часто перечислено
    # контрдоказательство, а не отсутствующий X.
    if not candidates:
        for token in _identifiers_in_text(
            str(item.get("affected_entity") or ""), allow_single=True,
        ):
            _append_unique(candidates, token)
    return candidates[:4]


def _false_absence_observation(
    item: Mapping[str, Any],
    sources: list[dict[str, Any]],
) -> dict[str, Any] | None:
    for token in _absence_candidates(item):
        canonical = normalize_homoglyph_token(token)
        hits = _vector_hits(canonical, sources)
        if not hits:
            continue
        evidence = {
            key: value for key, value in hits[0].items() if key != "_spelling"
        }
        return {
            "schema_version": 1,
            "status": "false_absence",
            "rule": "claimed_absent_token_present_in_vector",
            "quoted_tokens": [token],
            "normalized_token": canonical,
            "vector_truth_token": evidence.get("matched_text"),
            "normalization_applied": (
                _spelling_key(token) != canonical
                or _spelling_key(token)
                != _spelling_key(evidence.get("matched_text"))
            ),
            "observe_only": True,
            "vector_evidence": evidence,
        }
    return None


def observe_symbol_token_evidence(
    findings: list[dict[str, Any]],
    *,
    vector_sources: Mapping[str, Any] | None = None,
    document_graph: Mapping[str, Any] | None = None,
    target_block_id: Any = None,
    target_page: Any = None,
    enabled: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Annotate provable OCR artifacts without filtering any finding.

    Default is deliberately OFF. With enabled=True every finding is copied,
    inspected independently and left in the same position. Malformed sources
    or items are skipped; failures are reported but never raised.
    """
    source_count = len(vector_sources) if isinstance(vector_sources, Mapping) else 0
    report: dict[str, Any] = {
        "schema_version": 1,
        "enabled": bool(enabled),
        "observe_only": True,
        "candidates": len(findings or []),
        "vector_sources": source_count,
        "annotated_findings": 0,
        "observation_counts": {},
        "errors": 0,
    }
    if not enabled:
        return list(findings or []), report

    output: list[dict[str, Any]] = []
    annotated = 0
    counts: dict[str, int] = {}
    for raw in findings or []:
        if not isinstance(raw, dict):
            output.append(raw)
            continue
        item = dict(raw)
        try:
            sources = _ordered_vector_sources(
                item,
                vector_sources=vector_sources,
                document_graph=document_graph,
                target_block_id=target_block_id,
                target_page=target_page,
            )
            observations = [
                observation
                for observation in (
                    _ocr_artifact_observation(item, sources),
                    _false_absence_observation(item, sources),
                )
                if observation is not None
            ]
            if observations:
                existing = [
                    dict(value)
                    for value in item.get(SYMBOL_EVIDENCE_FIELD) or []
                    if isinstance(value, Mapping)
                ]
                signatures = {
                    (
                        value.get("status"),
                        value.get("normalized_token"),
                        (value.get("vector_evidence") or {}).get("block_id"),
                        (value.get("vector_evidence") or {}).get("offset_start"),
                    )
                    for value in existing
                }
                added = 0
                for observation in observations:
                    signature = (
                        observation.get("status"),
                        observation.get("normalized_token"),
                        (observation.get("vector_evidence") or {}).get("block_id"),
                        (observation.get("vector_evidence") or {}).get("offset_start"),
                    )
                    if signature in signatures:
                        continue
                    signatures.add(signature)
                    existing.append(observation)
                    status = str(observation["status"])
                    counts[status] = counts.get(status, 0) + 1
                    added += 1
                if added:
                    item[SYMBOL_EVIDENCE_FIELD] = existing
                    annotated += 1
        except Exception:
            report["errors"] += 1
        output.append(item)

    report["annotated_findings"] = annotated
    report["observation_counts"] = dict(sorted(counts.items()))
    return output, report


def _norm(value: Any) -> str:
    return re.sub(r"[^a-zа-яё0-9]+", " ", str(value or "").casefold()).strip()


def _confidence(value: Any) -> float:
    try:
        return max(0.0, min(1.0, float(value)))
    except (TypeError, ValueError):
        return 0.0


def _dedupe_key(finding: dict[str, Any]) -> tuple[str, str]:
    problem = _norm(finding.get("problem_class") or finding.get("category"))
    entity = _norm(finding.get("affected_entity") or finding.get("value_found"))
    return problem, entity


def gate_findings(
    findings: list[dict[str, Any]],
    *,
    min_confidence: float = 0.80,
    max_published: int = 3,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    """Split candidates into ``published`` and ``deferred`` with reason codes.

    Old cached/custom responses that contain none of the evidence-control fields
    pass through for backwards compatibility.  New v2-schema responses always
    contain all fields and therefore receive the full gate.
    """
    publishable: list[dict[str, Any]] = []
    deferred: list[dict[str, Any]] = []
    reason_counts: dict[str, int] = {}

    for raw in findings or []:
        if not isinstance(raw, dict):
            continue
        item = dict(raw)
        if not (_CONTROL_FIELDS & set(item)):
            item["_evidence_gate"] = {"status": "legacy_passthrough", "reasons": []}
            publishable.append(item)
            continue

        reasons: list[str] = []
        claim_type = str(item.get("claim_type") or "").strip()
        context_status = str(item.get("context_status") or "").strip()
        confidence = _confidence(item.get("confidence"))
        if claim_type not in PUBLISHABLE_CLAIM_TYPES:
            reasons.append("non_publishable_claim_type")
        if context_status != "sufficient":
            reasons.append("insufficient_context")
        if confidence < min_confidence:
            reasons.append("low_confidence")
        if item.get("counterevidence_checked") is not True:
            reasons.append("counterevidence_not_checked")
        if not str(item.get("affected_entity") or "").strip():
            reasons.append("affected_entity_missing")
        if not str(item.get("evidence_quote") or item.get("value_found") or "").strip():
            reasons.append("evidence_missing")
        if str(item.get("evidence_kind") or "").strip() in {"", "none"}:
            reasons.append("evidence_kind_missing")
        if _SPECULATIVE_RE.search(str(item.get("finding") or "")):
            reasons.append("speculative_wording")
        combined_text = " ".join(
            str(item.get(key) or "")
            for key in ("finding", "evidence_quote", "recommendation")
        )
        if _INTERNAL_METADATA_RE.search(combined_text):
            reasons.append("internal_metadata_comparison")
        if _VAGUE_COUNT_RE.search(combined_text):
            reasons.append("unquantified_visual_count")

        item["confidence"] = confidence
        item["_evidence_gate"] = {
            "status": "deferred" if reasons else "eligible",
            "reasons": reasons,
        }
        if reasons:
            deferred.append(item)
            for reason in set(reasons):
                reason_counts[reason] = reason_counts.get(reason, 0) + 1
        else:
            publishable.append(item)

    # Block-local semantic key is deliberately conservative.  The stronger
    # cross-block merge remains downstream, but repeated detector statements
    # about the same entity/problem no longer crowd out independent evidence.
    ranked = sorted(
        publishable,
        key=lambda item: (
            -_confidence(item.get("confidence", 1.0)),
            -_SEVERITY_RANK.get(str(item.get("severity") or ""), 0),
            _norm(item.get("finding")),
        ),
    )
    unique: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in ranked:
        key = _dedupe_key(item)
        if key != ("", "") and key in seen:
            duplicate = dict(item)
            duplicate["_evidence_gate"] = {"status": "deferred", "reasons": ["block_duplicate"]}
            deferred.append(duplicate)
            reason_counts["block_duplicate"] = reason_counts.get("block_duplicate", 0) + 1
            continue
        seen.add(key)
        if len(unique) >= max_published:
            overflow = dict(item)
            overflow["_evidence_gate"] = {"status": "deferred", "reasons": ["block_finding_cap"]}
            deferred.append(overflow)
            reason_counts["block_finding_cap"] = reason_counts.get("block_finding_cap", 0) + 1
            continue
        item["_evidence_gate"] = {"status": "published", "reasons": []}
        unique.append(item)

    report = {
        "schema_version": 1,
        "candidates": len([item for item in findings or [] if isinstance(item, dict)]),
        "published": len(unique),
        "deferred": len(deferred),
        "min_confidence": min_confidence,
        "max_published_per_block": max_published,
        "reason_counts": dict(sorted(reason_counts.items())),
    }
    return unique, deferred, report
