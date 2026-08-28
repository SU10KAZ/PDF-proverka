"""Детерминированный верификатор ответа ИИ.

Принцип: ИИ может ИНТЕРПРЕТИРОВАТЬ доказательства, но не создавать их.
Любое значение, которое ИИ выдаёт как факт документа, должно быть доказуемо
присутствующим в пакете доказательств. Проверка — на стороне Python, без модели.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field

from schema import DIMENSIONS, OUTCOMES, DIRECTIONS, RESOLUTION_STATUS, CONFIDENCE, REVIEW_REASONS


@dataclass
class VerifyResult:
    ok: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {"ok": self.ok, "errors": self.errors, "warnings": self.warnings}


_DASHES = "–—−"


def _norm(s: str | None) -> str:
    """Нормализация для сравнения с исходником.

    Снимаем ровно то, что не является содержательным различием: регистр,
    пробелы, разделитель дробной части, типографские дефисы. Цифры и слова
    остаются нетронутыми — иначе верификатор пропустит выдуманное значение.
    """
    s = unicodedata.normalize("NFKC", (s or "")).strip().lower()
    s = s.replace(",", ".").replace("ё", "е")
    for d in _DASHES:
        s = s.replace(d, "-")
    return re.sub(r"\s+", " ", s)


_MARK = re.compile(r"^[»\s]+")


def _evidence_corpus(pkg: dict) -> tuple[str, str]:
    """Всё, что модель имела право видеть: сторона LEFT и сторона RIGHT."""
    left = [pkg.get("before_value") or ""]
    left += [_MARK.sub("", c) for c in pkg.get("left_context") or []]
    left += pkg.get("left_page_titles") or []

    right = [pkg.get("after_value") or ""]
    right += [_MARK.sub("", c) for c in pkg.get("right_context") or []]
    right += pkg.get("right_page_titles") or []

    return _norm("  ".join(left)), _norm("  ".join(right))


REQUIRED_FIELDS = (
    "resolution_status", "dimension", "direction", "outcome", "before_value",
    "after_value", "subject_label", "engineering_significance", "confidence",
    "evidence_quotes", "needs_human_review", "review_reason", "review_question",
    "reasoning_summary",
)


def verify(pkg: dict, resp: dict) -> VerifyResult:
    errors: list[str] = []
    warnings: list[str] = []

    # 1. Схема: наличие полей и закрытые перечисления.
    for f in REQUIRED_FIELDS:
        if f not in resp:
            errors.append(f"schema: отсутствует поле {f}")
    if errors:
        return VerifyResult(False, errors, warnings)

    enums = {
        "resolution_status": RESOLUTION_STATUS, "dimension": DIMENSIONS,
        "direction": DIRECTIONS, "outcome": OUTCOMES, "confidence": CONFIDENCE,
        "review_reason": REVIEW_REASONS,
    }
    for f, allowed in enums.items():
        if resp.get(f) not in allowed:
            errors.append(f"enum: {f}={resp.get(f)!r} вне допустимого множества")

    if not isinstance(resp.get("needs_human_review"), bool):
        errors.append("type: needs_human_review должно быть boolean")

    left, right = _evidence_corpus(pkg)

    # 2. Значения обязаны существовать в доказательствах своей стороны.
    bv, av = resp.get("before_value"), resp.get("after_value")
    if bv and _norm(bv) not in left:
        errors.append(f"grounding: before_value {bv!r} отсутствует в доказательствах LEFT")
    if av and _norm(av) not in right:
        errors.append(f"grounding: after_value {av!r} отсутствует в доказательствах RIGHT")

    # 3. LEFT/RIGHT не переставлены местами.
    if bv and av and _norm(bv) in right and _norm(av) in left and _norm(bv) not in left:
        errors.append("inversion: before/after выглядят переставленными местами")

    # 4. Направление согласовано с тем, что дал детерминированный Stage 3.
    bucket = pkg.get("stage3_bucket")
    if bucket == "added" and bv:
        errors.append("direction: Stage 3 не нашёл значения LEFT, но модель выдала before_value")
    if bucket == "removed" and av:
        errors.append("direction: Stage 3 не нашёл значения RIGHT, но модель выдала after_value")
    if bucket == "added" and resp["direction"] != "ADDED":
        warnings.append(f"direction: Stage 3 bucket=added, модель дала {resp['direction']}")
    if bucket == "removed" and resp["direction"] != "REMOVED":
        warnings.append(f"direction: Stage 3 bucket=removed, модель дала {resp['direction']}")

    # 5. Цитаты обязаны быть дословными.
    for q in resp.get("evidence_quotes") or []:
        side, text = q.get("side"), _norm(q.get("quote"))
        if not text:
            continue
        hay = left if side == "LEFT" else right
        if text not in hay:
            errors.append(f"quote: цитата {q.get('quote')!r} не найдена дословно в {side}")

    # 6. Согласованность статуса разрешения.
    st = resp.get("resolution_status")
    if st == "AI_RESOLVED":
        if resp.get("needs_human_review"):
            errors.append("consistency: AI_RESOLVED вместе с needs_human_review=true")
        if resp.get("dimension") == "UNKNOWN_DIMENSION":
            errors.append("consistency: AI_RESOLVED с UNKNOWN_DIMENSION запрещён политикой")
        if resp.get("outcome") == "REVIEW_REQUIRED":
            errors.append("consistency: AI_RESOLVED с outcome=REVIEW_REQUIRED противоречив")
        if not (resp.get("evidence_quotes") or []):
            errors.append("consistency: AI_RESOLVED без единой цитаты доказательства")
        if resp.get("confidence") == "UNKNOWN":
            errors.append("consistency: AI_RESOLVED с confidence=UNKNOWN")
    else:
        if st == "HUMAN_REQUIRED" and not resp.get("needs_human_review"):
            errors.append("consistency: HUMAN_REQUIRED без needs_human_review")
        if st == "HUMAN_REQUIRED" and not (resp.get("review_question") or "").strip():
            errors.append("consistency: HUMAN_REQUIRED без сформулированного вопроса")
        if resp.get("review_reason") == "NOT_APPLICABLE":
            errors.append("consistency: отказ без указания причины")

    # 7. Политика: UNKNOWN_DIMENSION обязан оставаться review-элементом.
    if resp.get("dimension") == "UNKNOWN_DIMENSION" and resp.get("outcome") != "REVIEW_REQUIRED":
        errors.append("policy: UNKNOWN_DIMENSION допускает только outcome=REVIEW_REQUIRED")

    # 8. Пустой вывод.
    if not (resp.get("reasoning_summary") or "").strip():
        errors.append("content: пустое обоснование")

    return VerifyResult(not errors, errors, warnings)
