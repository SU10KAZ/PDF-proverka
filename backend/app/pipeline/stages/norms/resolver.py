"""Bounded normative resolver over real clauses from one vault document.

The resolver accepts Stage 03 document candidates, but never accepts its clause
or quote as evidence.  It first checks an optional clause hint, then ranks only
the clauses physically present in the resolved document.  The selected pair is
looked up again through ``norms_api.get_paragraph`` before publication.

The default implementation is deterministic and offline.  It makes zero model
calls; an eventual AI ranker may only reorder ``RankedClause`` objects already
created here and must still pass ``_verify_selected_clause``.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import statistics
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

from backend.app.pipeline.stages.findings_merge.normative_references import (
    harden_finding_normative_references,
    normalize_designation,
)
from norms.runtime import (
    configured_runtime_tools_path,
    configured_status_index_path,
    configured_vault_path,
)


VERIFIED = "VERIFIED"
NOT_VERIFIED = "NOT_VERIFIED"
DOCUMENT_MISSING = "DOCUMENT_MISSING"
AMBIGUOUS = "AMBIGUOUS"
WRONG_EDITION = "WRONG_EDITION"
SPECIAL_POLICY = "SPECIAL_POLICY"

RESOLVER_VERSION = "norm-resolver-v9"
CACHE_SCHEMA_VERSION = 1
MIN_SCORE = 0.36
MIN_COVERAGE = 0.18
MIN_MATCHED_TERMS = 2
MIN_MATCHED_ANCHORS = 2
AMBIGUOUS_GAP = 0.09
MAX_QUOTE_CHARS = 900


_TOKEN_RE = re.compile(r"[0-9]+(?:[.,][0-9]+)*|[a-zа-яё]+", re.IGNORECASE)
_DATE_RE = re.compile(r"(?<!\d)(\d{2})\.(\d{2})\.(\d{2}|\d{4})(?!\d)")
_CLAUSE_RE = re.compile(r"^\d+(?:\.\d+)*$")

_STOPWORDS = {
    "без", "более", "быть", "весь", "внутри", "для", "должен", "должна",
    "должно", "должны", "другой", "его", "если", "или", "как", "либо",
    "между", "может", "на", "над", "не", "него", "ниже", "один", "она",
    "они", "от", "по", "под", "при", "проект", "разный", "с", "со", "также",
    "того", "у", "указан", "указана", "указано", "указаны", "что", "этот",
    "эта", "эти", "является", "лист", "строка", "корпус", "комплект",
    "исправить", "привести", "проверить", "требуется", "данный", "данная",
    "текущий", "текущая", "фактически", "раздел", "значение", "значения",
}

# Prefix groups keep deterministic Russian matching useful without a mutable
# morphology dependency.  They are deliberately domain-specific and small.
_ROOT_GROUPS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("перечень", ("перечн", "ведомост", "список")),
    ("ссылка", ("ссылк", "цитир", "нормативн")),
    ("обозначение", ("обознач", "маркиров", "шифр", "идентифик")),
    ("соответствие", ("соответств", "согласован", "совпад", "противореч")),
    ("документ", ("документ", "чертеж")),
    ("кабель", ("кабел", "провод", "электропровод")),
    ("сечение", ("сечен", "жил")),
    ("нагрузка", ("нагруз", "мощност", "расчет")),
    ("таблица", ("таблиц", "итог")),
    ("трансформатор", ("трансформатор", "коэффициент")),
    ("счетчик", ("счетчик", "счётчик", "учет", "учёт")),
    ("проходка", ("проходк", "проход", "заделк")),
    ("огнестойкость", ("огнестой", "несгора", "пожар")),
    ("освещение", ("освещ", "светиль")),
    ("устройство", ("устройств", "оборудован", "аппарат")),
    ("наименование", ("наименован", "заголов")),
)

_BOILERPLATE_LINES = (
    "консультантплюс",
    "надежная правовая поддержка",
    "надёжная правовая поддержка",
    "www.consultant.ru",
    "[www.consultant.ru]",
    "страница ",
)

_GENERIC_ANCHORS = {
    "документ", "соответствие", "таблица", "обозначение", "значени",
    "значение", "устройство", "комплект", "требован", "проект", "лист",
    "данным", "данные", "указан", "указани", "приведен", "общих", "всех", "работу", "этом",
}
_OBLIGATION_RE = re.compile(
    r"\b(?:долж(?:ен|на|но|ны)|следует|не\s+допуска|требу(?:ется|ют)|"
    r"указыва(?:ют|ется)|выполня(?:ют|ется)|принима(?:ют|ется)|составля(?:ют|ет)|"
    r"определя(?:ют|ется)|приводят|включают|наносят|"
    r"необходимо|надлежит|присваивают)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class VaultClause:
    code: str
    paragraph: str
    text: str
    file: str
    line: int | None


@dataclass(frozen=True)
class RankedClause:
    clause: VaultClause
    score: float
    coverage: float
    matched_terms: tuple[str, ...]
    bm25: float
    phrase_overlap: float
    number_recall: float | None
    is_exact_hint: bool
    quote_candidate_similarity: float | None
    matched_anchors: tuple[str, ...]
    anchor_coverage: float
    has_normative_language: bool

    def compact(self) -> dict:
        return {
            "clause": self.clause.paragraph,
            "score": round(self.score, 4),
            "coverage": round(self.coverage, 4),
            "matched_terms": list(self.matched_terms),
            "matched_anchors": list(self.matched_anchors),
            "anchor_coverage": round(self.anchor_coverage, 4),
            "has_normative_language": self.has_normative_language,
            "file": self.clause.file,
            "line": self.clause.line,
            "excerpt": _quote_excerpt(self.clause.text, 280),
        }


def _match_key(value: str | None) -> str:
    return re.sub(r"\s+", "", str(value or "")).replace("_", ".").casefold()


def _display_code(value: str | None) -> str | None:
    return str(value).replace("_", ".") if value else None


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _stable_digest(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return _sha256_bytes(payload.encode("utf-8"))


def _file_digest(path: Path) -> str:
    try:
        return _sha256_bytes(path.read_bytes())
    except OSError:
        return "missing"


def _root_token(token: str) -> str:
    token = token.casefold().replace("ё", "е")
    if token in _STOPWORDS or len(token) < 3:
        return ""
    for canonical, prefixes in _ROOT_GROUPS:
        if any(token.startswith(prefix.replace("ё", "е")) for prefix in prefixes):
            return canonical
    # Conservative suffix removal improves inflection matching without turning
    # unrelated short technical words into the same token.
    if len(token) >= 7:
        for suffix in (
            "иями", "ями", "ами", "ого", "ему", "ому", "ыми", "ими", "ая",
            "яя", "ое", "ее", "ые", "ие", "ий", "ый", "ой", "ов", "ев",
            "ам", "ям", "ах", "ях", "ом", "ем", "а", "я", "ы", "и", "у",
            "ю", "е",
        ):
            if token.endswith(suffix) and len(token) - len(suffix) >= 5:
                return token[: -len(suffix)]
    return token


def _tokens(text: str | None) -> list[str]:
    result = []
    for raw in _TOKEN_RE.findall(str(text or "")):
        if raw[0].isdigit():
            result.append(raw.replace(",", "."))
            continue
        token = _root_token(raw)
        if token:
            result.append(token)
    return result


def _word_jaccard(left: str | None, right: str | None) -> float:
    a, b = set(_tokens(left)), set(_tokens(right))
    return len(a & b) / len(a | b) if a and b else 0.0


def _quote_excerpt(text: str, max_chars: int = MAX_QUOTE_CHARS) -> str:
    lines: list[str] = []
    for raw_line in str(text or "").splitlines():
        line = raw_line.strip()
        folded = line.casefold()
        if not line or any(folded.startswith(prefix) for prefix in _BOILERPLATE_LINES):
            continue
        lines.append(line)
    clean = "\n".join(lines).strip()
    if len(clean) <= max_chars:
        return clean
    cut = clean[:max_chars]
    boundary = max(cut.rfind(". "), cut.rfind("; "), cut.rfind("\n"))
    if boundary >= max_chars // 2:
        cut = cut[: boundary + 1]
    return cut.rstrip()


def _parse_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        pass
    match = _DATE_RE.search(text)
    if not match:
        return None
    day, month, year = (int(part) for part in match.groups())
    if year < 100:
        year += 2000
    try:
        return date(year, month, day)
    except ValueError:
        return None


def infer_document_date(output_dir: Path, data: dict | None = None) -> date | None:
    """Infer issue date from explicit metadata, then from title-block revisions."""
    data = data or {}
    meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
    for key in ("document_date", "issue_date", "stamp_date", "project_date"):
        parsed = _parse_date(meta.get(key))
        if parsed:
            return parsed

    # Normal runs use ``<version>/_output``; stage replays may use a nested
    # ``runs/<id>`` directory.  Locate the closest bounded version root instead
    # of depending on a single storage-generation layout.
    work_dir = next(
        (
            parent / "02_work"
            for parent in output_dir.parents[:4]
            if (parent / "02_work").is_dir()
        ),
        None,
    )
    if work_dir is None:
        return None
    candidates: list[date] = []
    for path in sorted(work_dir.glob("*.md")):
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        for line in text.splitlines():
            if "revision" not in line.casefold() and "измен" not in line.casefold():
                continue
            for match in _DATE_RE.finditer(line):
                parsed = _parse_date(match.group(0))
                if parsed:
                    candidates.append(parsed)
    return max(candidates) if candidates else None


def _finding_query(finding: dict, candidate: dict) -> str:
    fields = (
        finding.get("problem"),
        finding.get("description"),
        finding.get("finding"),
        finding.get("solution"),
        finding.get("recommendation"),
        finding.get("category"),
        finding.get("context"),
        candidate.get("reason"),
    )
    return "\n".join(str(value).strip() for value in fields if value)


def _evidence_digest(finding: dict, candidate: dict, document_date: date | None) -> str:
    evidence = {
        "finding_id": finding.get("id"),
        "problem": finding.get("problem") or finding.get("finding"),
        "description": finding.get("description"),
        "solution": finding.get("solution") or finding.get("recommendation"),
        "category": finding.get("category"),
        "context": finding.get("context"),
        "candidate": candidate,
        "document_date": document_date.isoformat() if document_date else None,
    }
    return _stable_digest(evidence)


class NormResolver:
    """Resolve Stage 03 candidates against a fixed offline Norms runtime."""

    def __init__(
        self,
        *,
        norms_api: Any | None = None,
        paragraphs_path: Path | None = None,
        vault_path: Path | None = None,
        status_index_path: Path | None = None,
        cache_path: Path | None = None,
    ) -> None:
        if norms_api is None:
            from norms._native_verify import _import_norms_api

            norms_api = _import_norms_api()
        tools_path = configured_runtime_tools_path()
        self.norms_api = norms_api
        self.paragraphs_path = paragraphs_path or tools_path / "paragraphs.jsonl"
        self.vault_path = vault_path or configured_vault_path()
        self.status_index_path = status_index_path or configured_status_index_path(tools_path)
        self.cache_path = cache_path
        self._clauses_by_code: dict[str, list[VaultClause]] | None = None
        self._cache = self._load_cache()
        self.cache_hits = 0
        self.cache_misses = 0
        self.ai_calls = 0
        self.document_timings_ms: dict[str, float] = defaultdict(float)

    def _load_cache(self) -> dict:
        if not self.cache_path or not self.cache_path.exists():
            return {"schema_version": CACHE_SCHEMA_VERSION, "entries": {}}
        try:
            value = json.loads(self.cache_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"schema_version": CACHE_SCHEMA_VERSION, "entries": {}}
        if value.get("schema_version") != CACHE_SCHEMA_VERSION:
            return {"schema_version": CACHE_SCHEMA_VERSION, "entries": {}}
        if not isinstance(value.get("entries"), dict):
            value["entries"] = {}
        return value

    def save_cache(self) -> None:
        if not self.cache_path:
            return
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.cache_path.with_suffix(self.cache_path.suffix + ".tmp")
        tmp.write_text(json.dumps(self._cache, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self.cache_path)

    def _load_clauses(self) -> dict[str, list[VaultClause]]:
        if self._clauses_by_code is not None:
            return self._clauses_by_code
        grouped: dict[str, list[VaultClause]] = defaultdict(list)
        seen: set[tuple[str, str]] = set()
        try:
            handle = self.paragraphs_path.open(encoding="utf-8")
        except OSError:
            self._clauses_by_code = {}
            return self._clauses_by_code
        with handle:
            for line in handle:
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue
                code = str(item.get("code") or "").strip()
                paragraph = str(item.get("paragraph") or "").strip()
                text = str(item.get("text") or "").strip()
                if not code or not _CLAUSE_RE.fullmatch(paragraph) or not text:
                    continue
                key = _match_key(code)
                identity = (key, paragraph)
                if identity in seen:
                    continue
                seen.add(identity)
                grouped[key].append(
                    VaultClause(
                        code=code,
                        paragraph=paragraph,
                        text=text,
                        file=str(item.get("file") or ""),
                        line=int(item["line"]) if isinstance(item.get("line"), int) else None,
                    )
                )
        self._clauses_by_code = dict(grouped)
        return self._clauses_by_code

    def _document_clauses(self, status: dict) -> list[VaultClause]:
        started = time.perf_counter()
        code = status.get("matched_code")
        clauses = list(self._load_clauses().get(_match_key(code), []))
        self.document_timings_ms[str(code or "unknown")] += (time.perf_counter() - started) * 1000
        return clauses

    def _document_digest(self, status: dict) -> str:
        file_name = status.get("file")
        return _file_digest(self.vault_path / str(file_name)) if file_name else "missing"

    def _rank(
        self,
        query: str,
        clauses: list[VaultClause],
        *,
        anchor_text: str | None,
        exact_hint: str | None,
        quote_candidate: str | None,
    ) -> list[RankedClause]:
        if not clauses:
            return []
        clause_tokens = [_tokens(item.text) for item in clauses]
        query_tokens = _tokens(query)
        if not query_tokens:
            return []
        query_terms = set(query_tokens)
        anchor_terms = {
            token for token in _tokens(anchor_text)
            if not token[0].isdigit() and token not in _GENERIC_ANCHORS
        }
        doc_freq: Counter[str] = Counter()
        for tokens in clause_tokens:
            doc_freq.update(set(tokens))
        count = len(clauses)
        idf = {
            term: math.log((count + 1.0) / (doc_freq.get(term, 0) + 0.5)) + 1.0
            for term in query_terms
        }
        available_terms = {term for term in query_terms if doc_freq.get(term, 0)}
        available_weight = sum(idf[term] for term in available_terms) or 1.0
        avg_len = statistics.fmean(len(tokens) for tokens in clause_tokens) or 1.0
        paragraph_numbers = {clause.paragraph for clause in clauses}
        query_bigrams = set(zip(query_tokens, query_tokens[1:]))
        query_numbers = {term for term in query_terms if term[0].isdigit()}

        raw_rows: list[dict[str, Any]] = []
        max_bm25 = 0.0
        for clause, tokens in zip(clauses, clause_tokens):
            tf = Counter(tokens)
            length_norm = 1.2 * (1 - 0.75 + 0.75 * len(tokens) / avg_len)
            bm25 = 0.0
            for term in query_terms:
                freq = tf.get(term, 0)
                if not freq:
                    continue
                bm25 += idf[term] * (freq * 2.2) / (freq + length_norm)
            max_bm25 = max(max_bm25, bm25)
            matched = tuple(sorted(query_terms & set(tokens)))
            coverage = sum(idf[term] for term in matched) / available_weight
            bigrams = set(zip(tokens, tokens[1:]))
            phrase_overlap = len(query_bigrams & bigrams) / max(1, len(query_bigrams))
            clause_numbers = {term for term in tokens if term[0].isdigit()}
            number_recall = (
                len(query_numbers & clause_numbers) / len(query_numbers)
                if query_numbers else None
            )
            quote_similarity = (
                _word_jaccard(quote_candidate, clause.text)
                if quote_candidate and clause.paragraph == exact_hint else None
            )
            matched_anchors = tuple(sorted(anchor_terms & set(tokens)))
            anchor_coverage = (
                len(matched_anchors) / len(anchor_terms) if anchor_terms else 0.0
            )
            raw_rows.append({
                "clause": clause,
                "tokens": tokens,
                "bm25": bm25,
                "matched": matched,
                "coverage": coverage,
                "phrase_overlap": phrase_overlap,
                "number_recall": number_recall,
                "quote_similarity": quote_similarity,
                "matched_anchors": matched_anchors,
                "anchor_coverage": anchor_coverage,
                "has_normative_language": bool(_OBLIGATION_RE.search(clause.text)),
            })

        ranked: list[RankedClause] = []
        for row in raw_rows:
            bm25_norm = row["bm25"] / max_bm25 if max_bm25 else 0.0
            number_component = row["number_recall"] if row["number_recall"] is not None else 0.0
            score = (
                0.40 * bm25_norm
                + 0.28 * row["coverage"]
                + 0.15 * row["anchor_coverage"]
                + 0.12 * row["phrase_overlap"]
                + 0.05 * number_component
            )
            is_exact = row["clause"].paragraph == exact_hint
            if is_exact and row["quote_similarity"] is not None:
                score += min(0.06, 0.06 * row["quote_similarity"])
            # Long aggregate section records repeat many child clauses and can
            # win by keyword stuffing. Prefer the actual leaf provision.
            if len(row["tokens"]) > 500:
                score *= 0.25
            if any(
                number.startswith(row["clause"].paragraph + ".")
                for number in paragraph_numbers
            ):
                # The index may contain a whole parent section and each leaf
                # clause. A parent is useful for recall, but too imprecise to
                # publish as the final reference while a child exists.
                score *= 0.20
            heading = row["clause"].text[:180].casefold().replace("ё", "е")
            if (
                row["clause"].paragraph in {"1", "2"}
                and "нормативные ссылки" in heading
            ):
                score *= 0.15
            ranked.append(
                RankedClause(
                    clause=row["clause"],
                    score=min(1.0, score),
                    coverage=row["coverage"],
                    matched_terms=row["matched"],
                    bm25=row["bm25"],
                    phrase_overlap=row["phrase_overlap"],
                    number_recall=row["number_recall"],
                    is_exact_hint=is_exact,
                    quote_candidate_similarity=row["quote_similarity"],
                    matched_anchors=row["matched_anchors"],
                    anchor_coverage=row["anchor_coverage"],
                    has_normative_language=row["has_normative_language"],
                )
            )
        return sorted(ranked, key=lambda item: (-item.score, item.clause.paragraph))

    @staticmethod
    def _strong(item: RankedClause | None) -> bool:
        quote_match = (item.quote_candidate_similarity or 0.0) if item else 0.0
        return bool(
            item
            and item.score >= MIN_SCORE
            and (
                item.coverage >= MIN_COVERAGE
                or quote_match >= 0.65
            )
            and len([term for term in item.matched_terms if not term[0].isdigit()])
            >= MIN_MATCHED_TERMS
            and len(item.matched_anchors) >= (
                1 if quote_match >= 0.65 else MIN_MATCHED_ANCHORS
            )
            and (
                item.has_normative_language
                or quote_match >= 0.65
            )
        )

    def _exact_hint_clause(
        self,
        designation: str,
        expected_code: str,
        paragraph: str | None,
    ) -> VaultClause | None:
        """Read the exact hint even when the retrieval JSONL missed the clause."""
        if not paragraph:
            return None
        try:
            actual = self.norms_api.get_paragraph(designation, paragraph, max_lines=80) or {}
        except Exception:  # noqa: BLE001 - candidate remains unproved
            return None
        text = str(actual.get("text") or "").strip()
        if (
            not actual.get("found")
            or not actual.get("authoritative")
            or not text
            or _match_key(actual.get("matched_code")) != _match_key(expected_code)
        ):
            return None
        return VaultClause(
            code=str(actual.get("matched_code") or expected_code),
            paragraph=paragraph,
            text=text,
            file=str(actual.get("file") or ""),
            line=actual.get("line") if isinstance(actual.get("line"), int) else None,
        )

    @staticmethod
    def _related_clause_numbers(left: str, right: str) -> bool:
        return (
            left.startswith(right + ".")
            or right.startswith(left + ".")
            or (
                "." in left
                and "." in right
                and left.rsplit(".", 1)[0] == right.rsplit(".", 1)[0]
            )
        )

    def _select(self, ranked: list[RankedClause], exact_hint: str | None) -> tuple[str, RankedClause | None]:
        if not ranked or not self._strong(ranked[0]):
            return NOT_VERIFIED, None
        top = ranked[0]
        exact = next((item for item in ranked if item.clause.paragraph == exact_hint), None)
        if self._strong(exact) and exact and exact.score >= top.score - 0.08:
            selected = exact
        else:
            selected = top

        competitors = [
            item for item in ranked
            if item.clause.paragraph != selected.clause.paragraph and self._strong(item)
        ]
        if selected.is_exact_hint and (selected.quote_candidate_similarity or 0.0) >= 0.65:
            return VERIFIED, selected
        if competitors:
            second = competitors[0]
            if (
                selected.score - second.score <= AMBIGUOUS_GAP
                and not self._related_clause_numbers(
                    selected.clause.paragraph, second.clause.paragraph
                )
            ):
                return AMBIGUOUS, None
        return VERIFIED, selected

    def _verify_selected_clause(
        self,
        designation: str,
        expected_code: str,
        selected: RankedClause,
    ) -> tuple[bool, dict]:
        try:
            actual = self.norms_api.get_paragraph(
                designation, selected.clause.paragraph, max_lines=80
            ) or {}
        except Exception as exc:  # noqa: BLE001 - verifier must fail closed
            return False, {"reason": f"lookup_failed:{type(exc).__name__}"}
        text = str(actual.get("text") or "").strip()
        matched_code = str(actual.get("matched_code") or "")
        if not actual.get("found") or not actual.get("authoritative") or not text:
            return False, {
                "reason": actual.get("resolution_reason") or "paragraph_not_confirmed"
            }
        if _match_key(matched_code) != _match_key(expected_code):
            return False, {
                "reason": "quote_from_wrong_document",
                "expected_code": expected_code,
                "matched_code": matched_code,
            }
        quote = _quote_excerpt(text)
        if not quote:
            return False, {"reason": "empty_quote_after_cleanup"}
        return True, {
            "source": "norms_vault",
            "authoritative": True,
            "matched_code": matched_code,
            "file": actual.get("file") or selected.clause.file,
            "line": actual.get("line") or selected.clause.line,
            "paragraph": selected.clause.paragraph,
            "quote": quote,
            "quote_digest": _sha256_bytes(text.encode("utf-8")),
            "resolution_reason": actual.get("resolution_reason") or "exact",
        }

    def resolve_reference(
        self,
        finding: dict,
        candidate: dict,
        *,
        document_date: date | None = None,
    ) -> dict:
        started = time.perf_counter()
        cited = str(candidate.get("cited_designation") or candidate.get("designation") or "").strip()
        canonical, normalization = normalize_designation(
            str(candidate.get("designation") or cited)
        )
        base = {
            "norm_designation": canonical,
            "cited_designation": cited or canonical,
            "canonical_designation": canonical,
            "current_designation": canonical,
            "resolution_status": NOT_VERIFIED,
            "document_status": "unknown",
            "edition_applicability": "not_assessed",
            "clause": None,
            "quote": None,
            "candidate_relevance": candidate.get("candidate_relevance", 0.5),
            "reason": candidate.get("reason") or "",
            "provenance": {
                "producer": RESOLVER_VERSION,
                "candidate_provenance": candidate.get("provenance") or {},
                "normalization": normalization,
                "ai_used": False,
            },
        }

        try:
            status = self.norms_api.get_norm_status(canonical) or {}
        except Exception as exc:  # noqa: BLE001
            status = {"found": False, "resolution_reason": f"lookup_failed:{type(exc).__name__}"}
        base["document_status"] = str(status.get("status") or "unknown")
        matched_designation = _display_code(status.get("matched_code"))
        if status.get("found") and matched_designation:
            canonical = matched_designation
            base["norm_designation"] = canonical
            base["canonical_designation"] = canonical
        current = status.get("replacement_doc") or status.get("current_version") or canonical
        base["current_designation"] = _display_code(str(current)) or canonical
        base["provenance"]["designation_evidence"] = {
            "source": "status_index",
            "authoritative": bool(status.get("authoritative")),
            "matched_code": status.get("matched_code"),
            "resolution_reason": status.get("resolution_reason"),
            "status_index_digest": _file_digest(self.status_index_path),
        }

        policy = None
        if canonical.upper().startswith("ПУЭ"):
            policy = {
                "status": SPECIAL_POLICY,
                "code": "PUE_VOLUNTARY_PARALLEL_SP_RECOMMENDED",
                "message": "ПУЭ требует отдельной проверки применимости и параллельной ссылки на СП.",
            }
        base["special_policy"] = policy

        effective_from = _parse_date(status.get("effective_from"))
        if status.get("status") in {"replaced", "outdated_edition"}:
            if document_date and effective_from and document_date < effective_from:
                base["edition_applicability"] = "historical_applicable"
            elif document_date and effective_from and document_date >= effective_from:
                base["edition_applicability"] = "wrong_for_document_date"
                base["resolution_status"] = WRONG_EDITION
                base["provenance"]["edition_evidence"] = {
                    "document_date": document_date.isoformat(),
                    "replacement_effective_from": effective_from.isoformat(),
                }
                base["duration_ms"] = round((time.perf_counter() - started) * 1000, 3)
                return base
            else:
                base["edition_applicability"] = "unknown_document_date"
        elif document_date and effective_from and document_date < effective_from:
            base["edition_applicability"] = "not_yet_effective"
            base["resolution_status"] = WRONG_EDITION
            base["provenance"]["edition_evidence"] = {
                "document_date": document_date.isoformat(),
                "edition_effective_from": effective_from.isoformat(),
            }
            base["duration_ms"] = round((time.perf_counter() - started) * 1000, 3)
            return base
        else:
            base["edition_applicability"] = "current"

        if not status.get("found") or not status.get("authoritative"):
            base["resolution_status"] = DOCUMENT_MISSING
            base["provenance"]["resolver_reason"] = "designation_not_in_canonical_index"
            base["duration_ms"] = round((time.perf_counter() - started) * 1000, 3)
            return base
        if not status.get("file") or status.get("source") != "vault":
            base["resolution_status"] = DOCUMENT_MISSING
            base["provenance"]["resolver_reason"] = "vault_document_missing"
            base["duration_ms"] = round((time.perf_counter() - started) * 1000, 3)
            return base

        clauses = self._document_clauses(status)
        exact_hint = str(candidate.get("clause_candidate") or "").strip() or None
        exact_clause = self._exact_hint_clause(
            canonical, str(status.get("matched_code") or canonical), exact_hint
        )
        if exact_clause and all(
            item.paragraph != exact_clause.paragraph for item in clauses
        ):
            clauses.append(exact_clause)
        document_digest = self._document_digest(status)
        status_index_digest = _file_digest(self.status_index_path)
        clauses_digest = _stable_digest([
            (item.paragraph, _sha256_bytes(item.text.encode("utf-8"))) for item in clauses
        ])
        evidence_digest = _evidence_digest(finding, candidate, document_date)
        cache_key = _stable_digest({
            "finding_evidence_digest": evidence_digest,
            "norm_designation": canonical,
            "vault_document_digest": document_digest,
            "status_index_digest": status_index_digest,
            "candidate_clauses_digest": clauses_digest,
            "resolver_version": RESOLVER_VERSION,
            "schema_version": CACHE_SCHEMA_VERSION,
            "ai_used": False,
        })
        cached = self._cache["entries"].get(cache_key)
        if isinstance(cached, dict):
            self.cache_hits += 1
            result = json.loads(json.dumps(cached, ensure_ascii=False))
            result.setdefault("provenance", {})["cache_hit"] = True
            result["duration_ms"] = round((time.perf_counter() - started) * 1000, 3)
            return result
        self.cache_misses += 1

        if document_digest == "missing":
            base["resolution_status"] = DOCUMENT_MISSING
            base["provenance"]["resolver_reason"] = "vault_document_missing"
            result = base
        elif not clauses:
            base["resolution_status"] = NOT_VERIFIED
            base["provenance"]["resolver_reason"] = "document_has_no_indexed_clauses"
            result = base
        else:
            query = _finding_query(finding, candidate)
            ranked = self._rank(
                query,
                clauses,
                anchor_text=finding.get("problem") or finding.get("finding"),
                exact_hint=exact_hint,
                quote_candidate=candidate.get("quote_candidate"),
            )
            decision, selected = self._select(ranked, exact_hint)
            base["resolution_status"] = decision
            base["provenance"].update({
                "finding_evidence_digest": evidence_digest,
                "vault_document_digest": document_digest,
                "status_index_digest": status_index_digest,
                "candidate_clauses_digest": clauses_digest,
                "resolver_version": RESOLVER_VERSION,
                "retrieval": {
                    "document_scope": status.get("matched_code"),
                    "clauses_considered": len(clauses),
                    "clause_candidate_checked_first": bool(exact_hint),
                    "top_candidates": [item.compact() for item in ranked[:3]],
                },
            })
            if decision == VERIFIED and selected is not None:
                verified, evidence = self._verify_selected_clause(
                    canonical, str(status.get("matched_code") or canonical), selected
                )
                if verified:
                    base["clause"] = selected.clause.paragraph
                    base["quote"] = evidence.pop("quote")
                    base["confidence"] = round(selected.score, 4)
                    base["provenance"]["verification"] = evidence
                    base["provenance"]["retrieval_strategy"] = (
                        "exact_clause_candidate"
                        if selected.is_exact_hint else "same_document_alternative"
                    )
                else:
                    base["resolution_status"] = NOT_VERIFIED
                    base["provenance"]["verification"] = evidence
            result = base

        result["duration_ms"] = round((time.perf_counter() - started) * 1000, 3)
        cache_value = json.loads(json.dumps(result, ensure_ascii=False))
        cache_value.get("provenance", {}).pop("cache_hit", None)
        self._cache["entries"][cache_key] = cache_value
        return result

    def resolve_finding(
        self,
        finding: dict,
        *,
        document_date: date | None = None,
    ) -> tuple[list[dict], float]:
        started = time.perf_counter()
        candidates = finding.get("candidate_norm_references")
        if not isinstance(candidates, list):
            harden_finding_normative_references(finding)
            candidates = finding.get("candidate_norm_references") or []
        references = [
            self.resolve_reference(finding, candidate, document_date=document_date)
            for candidate in candidates if isinstance(candidate, dict)
        ]
        finding["norm_references"] = references
        verified = [item for item in references if item.get("resolution_status") == VERIFIED]
        if not references:
            finding.pop("finding_norm_status", None)
            finding.pop("norm_paragraph_state", None)
        elif len(verified) == len(references):
            finding["finding_norm_status"] = "VERIFIED"
            finding["norm_paragraph_state"] = "paragraph_verified"
        elif verified:
            finding["finding_norm_status"] = "PARTIALLY_VERIFIED"
            finding["norm_paragraph_state"] = "paragraph_partially_verified"
        else:
            finding["finding_norm_status"] = "NOT_VERIFIED"
            finding["norm_paragraph_state"] = "paragraph_unverified"
        finding["norm"] = _legacy_norm_text(references)
        if len(verified) == 1 and len(references) == 1:
            finding["norm_quote"] = verified[0].get("quote")
            finding["norm_quote_source"] = "norm_resolver_vault"
        else:
            finding["norm_quote"] = None
            finding.pop("norm_quote_source", None)

        severity = str(finding.get("severity") or "").upper()
        if "КРИТИЧ" in severity and not verified:
            finding["critical_norm_notice"] = (
                "Критическое замечание. Точная нормативная ссылка не подтверждена."
            )
        else:
            finding.pop("critical_norm_notice", None)
        return references, (time.perf_counter() - started) * 1000


_RESOLUTION_RU = {
    VERIFIED: "подтверждено",
    NOT_VERIFIED: "не подтверждено",
    DOCUMENT_MISSING: "документ отсутствует в vault",
    AMBIGUOUS: "неоднозначно",
    WRONG_EDITION: "неверная редакция для даты документа",
}


def _legacy_norm_text(references: Iterable[dict]) -> str | None:
    parts: list[str] = []
    for ref in references:
        designation = str(ref.get("canonical_designation") or ref.get("norm_designation") or "").strip()
        if not designation:
            continue
        resolution = str(ref.get("resolution_status") or NOT_VERIFIED)
        label = f"{designation} ({_RESOLUTION_RU.get(resolution, resolution)})"
        current = str(ref.get("current_designation") or "").strip()
        if current and current != designation:
            applicability = ref.get("edition_applicability")
            if applicability == "historical_applicable":
                label += f", исторически применима; текущая редакция: {current}"
            else:
                label += f", текущая редакция: {current}"
        if resolution == VERIFIED and ref.get("clause"):
            label += f", п. {ref['clause']}"
        parts.append(label)
    return "; ".join(parts) if parts else None


def _duration_summary(values: list[float]) -> dict:
    if not values:
        return {"count": 0, "total_ms": 0.0, "mean_ms": 0.0, "p95_ms": 0.0}
    ordered = sorted(values)
    p95_index = min(len(ordered) - 1, math.ceil(len(ordered) * 0.95) - 1)
    return {
        "count": len(values),
        "total_ms": round(sum(values), 3),
        "mean_ms": round(statistics.fmean(values), 3),
        "p95_ms": round(ordered[p95_index], 3),
    }


def resolve_normative_references(
    output_dir: str | Path,
    *,
    document_date: date | str | None = None,
    resolver: NormResolver | None = None,
) -> dict:
    """Resolve and atomically republish all candidate refs in 03_findings.json."""
    output_dir = Path(output_dir)
    findings_path = output_dir / "03_findings.json"
    if not findings_path.exists():
        return {"ok": False, "error": "03_findings.json not found"}
    try:
        data = json.loads(findings_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {"ok": False, "error": str(exc)}
    findings = data.get("findings")
    if not isinstance(findings, list):
        return {"ok": False, "error": "findings is not a list"}

    parsed_date = _parse_date(document_date) or infer_document_date(output_dir, data)
    resolver = resolver or NormResolver(cache_path=output_dir / "norm_resolver_cache.json")
    reference_times: list[float] = []
    finding_times: list[float] = []
    per_document_times: dict[str, list[float]] = defaultdict(list)
    counts = Counter()
    total_started = time.perf_counter()
    for finding in findings:
        if not isinstance(finding, dict):
            continue
        references, finding_ms = resolver.resolve_finding(finding, document_date=parsed_date)
        finding_times.append(finding_ms)
        counts["total_findings"] += 1
        counts["candidate_references"] += len(finding.get("candidate_norm_references") or [])
        counts["resolved_references"] += len(references)
        for reference in references:
            counts[str(reference.get("resolution_status") or NOT_VERIFIED)] += 1
            if reference.get("special_policy"):
                counts["special_policy"] += 1
            reference_ms = float(reference.get("duration_ms") or 0.0)
            reference_times.append(reference_ms)
            document_code = str(
                reference.get("canonical_designation")
                or reference.get("norm_designation")
                or "unknown"
            )
            per_document_times[document_code].append(reference_ms)
    resolver.save_cache()

    report = {
        "ok": True,
        "resolver_version": RESOLVER_VERSION,
        "document_date": parsed_date.isoformat() if parsed_date else None,
        "total_findings": counts["total_findings"],
        "candidate_references": counts["candidate_references"],
        "resolved_references": counts["resolved_references"],
        "verified_references": counts[VERIFIED],
        "ambiguous": counts[AMBIGUOUS],
        "not_verified": counts[NOT_VERIFIED],
        "document_missing": counts[DOCUMENT_MISSING],
        "wrong_edition": counts[WRONG_EDITION],
        "special_policy": counts["special_policy"],
        "cache_hits": resolver.cache_hits,
        "cache_misses": resolver.cache_misses,
        "ai_calls": resolver.ai_calls,
        "performance": {
            "total_ms": round((time.perf_counter() - total_started) * 1000, 3),
            "per_reference": _duration_summary(reference_times),
            "per_finding": _duration_summary(finding_times),
            "per_document": {
                code: _duration_summary(values)
                for code, values in sorted(per_document_times.items())
            },
            "index_access_per_document_ms": {
                _display_code(code) or code: round(value, 3)
                for code, value in sorted(resolver.document_timings_ms.items())
            },
        },
    }
    meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
    meta["norm_resolver"] = {
        key: report[key]
        for key in (
            "resolver_version", "document_date", "candidate_references",
            "resolved_references", "verified_references", "ambiguous",
            "not_verified", "document_missing", "wrong_edition",
            "special_policy", "ai_calls",
        )
    }
    data["meta"] = meta
    tmp = findings_path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(findings_path)
    (output_dir / "norm_resolver_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return report


__all__ = [
    "AMBIGUOUS",
    "DOCUMENT_MISSING",
    "NOT_VERIFIED",
    "NormResolver",
    "RESOLVER_VERSION",
    "SPECIAL_POLICY",
    "VERIFIED",
    "WRONG_EDITION",
    "infer_document_date",
    "resolve_normative_references",
]
