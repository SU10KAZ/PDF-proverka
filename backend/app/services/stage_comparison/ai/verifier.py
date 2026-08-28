"""Детерминированный верификатор ответа ИИ.

Принцип один: ИИ может ИНТЕРПРЕТИРОВАТЬ доказательства, но не создавать их.
Любое значение, которое модель выдаёт как факт документа, обязано дословно
находиться в пакете, который ей дали. Проверка — на стороне Python, без
второй модели: иначе проверяющий ошибается ровно там же, где проверяемый.

Провал верификатора не публикуется никогда. Элемент возвращается человеку с
честной причиной, а не с исправленным задним числом ответом.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from ..unified_change_policy.contract import (
    CONFIDENCE_LEVELS,
    DIRECTIONS,
    EVIDENCE_DIMENSIONS,
    OUTCOMES,
    UNKNOWN_DIMENSION,
)
from . import schemas

VERIFIER_VERSION = "stage-comparison-ai-verifier.v1"

_REQUIRED_FIELDS = (
    "item_id", "resolution_status", "dimension", "direction", "outcome",
    "object_label", "facet_label", "before_value", "after_value",
    "confidence", "evidence_quotes", "needs_human_review",
    "human_reason", "human_question", "engineering_summary",
)

_ENUMS = {
    "resolution_status": schemas.RESOLUTION_STATUSES,
    "dimension": EVIDENCE_DIMENSIONS,
    "direction": DIRECTIONS,
    "outcome": OUTCOMES,
    "confidence": CONFIDENCE_LEVELS,
    "human_reason": schemas.REVIEW_REASONS,
}

#: Внутренние ссылки, которых в ответе модели быть не должно: их чеканит
#: бэкенд, и совпадение с настоящей ссылкой было бы случайным.
_FORBIDDEN_REF_RE = re.compile(
    r"(project_text_entity_|project_entity_|text_entity:|ureview_|uchg_|"
    r"tatom_|teva_|srel_|erel_|hquestion_)",
    re.I,
)
_BBOX_RE = re.compile(r"\b(bbox|x\s*[:=]\s*0\.\d+)", re.I)

_DASHES = "–—−"


@dataclass
class VerifyResult:
    ok: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {"ok": self.ok, "errors": self.errors, "warnings": self.warnings}


def normalize(value: Any) -> str:
    """Снять ровно то, что не является содержательным различием.

    Регистр, пробелы, разделитель дробной части, типографские дефисы. Цифры и
    слова остаются нетронутыми — иначе верификатор пропустит выдуманное
    значение, отличающееся от настоящего одной цифрой.
    """
    text = unicodedata.normalize("NFKC", str(value or "")).strip().lower()
    text = text.replace(",", ".").replace("ё", "е")
    for dash in _DASHES:
        text = text.replace(dash, "-")
    return re.sub(r"\s+", " ", text)


_CONTEXT_MARK = re.compile(r"^[»\s]+")


def _corpus(item: Mapping[str, Any]) -> tuple[str, str]:
    """Всё, что модель имела право видеть: сторона LEFT и сторона RIGHT."""
    left = [str(item.get("before_value") or "")]
    left += [_CONTEXT_MARK.sub("", line) for line in item.get("left_context") or []]
    right = [str(item.get("after_value") or "")]
    right += [_CONTEXT_MARK.sub("", line) for line in item.get("right_context") or []]
    return normalize("  ".join(left)), normalize("  ".join(right))


def verify_resolution(
    item: Mapping[str, Any],
    resolution: Mapping[str, Any],
) -> VerifyResult:
    """Проверить одно разрешение против одного элемента пакета."""
    errors: list[str] = []
    warnings: list[str] = []

    for name in _REQUIRED_FIELDS:
        if name not in resolution:
            errors.append(f"схема: отсутствует поле {name}")
    if errors:
        return VerifyResult(False, errors, warnings)

    for name, allowed in _ENUMS.items():
        if resolution.get(name) not in allowed:
            errors.append(
                f"перечисление: {name}={resolution.get(name)!r} вне допустимого множества"
            )
    if not isinstance(resolution.get("needs_human_review"), bool):
        errors.append("тип: needs_human_review должно быть логическим")

    if str(resolution.get("item_id") or "") != str(item.get("item_id") or ""):
        errors.append("привязка: разрешение относится к другому элементу")

    left, right = _corpus(item)
    before = resolution.get("before_value")
    after = resolution.get("after_value")

    # 1. Значения обязаны существовать в доказательствах своей стороны.
    if before and normalize(before) not in left:
        errors.append(
            f"обоснование: before_value {before!r} отсутствует в доказательствах LEFT"
        )
    if after and normalize(after) not in right:
        errors.append(
            f"обоснование: after_value {after!r} отсутствует в доказательствах RIGHT"
        )

    # 2. Стороны не переставлены местами.
    if (
        before and after
        and normalize(before) in right
        and normalize(after) in left
        and normalize(before) not in left
    ):
        errors.append("стороны: before и after выглядят переставленными местами")

    # 3. Направление согласовано с тем, что дал детерминированный Stage 3.
    bucket = str(item.get("stage3_bucket") or "")
    if bucket == "added" and before:
        errors.append(
            "направление: Stage 3 не нашёл значения слева, а модель выдала before_value"
        )
    if bucket == "removed" and after:
        errors.append(
            "направление: Stage 3 не нашёл значения справа, а модель выдала after_value"
        )
    if bucket == "added" and resolution.get("direction") != "ADDED":
        warnings.append(
            f"направление: Stage 3 дал «added», модель — {resolution.get('direction')}"
        )
    if bucket == "removed" and resolution.get("direction") != "REMOVED":
        warnings.append(
            f"направление: Stage 3 дал «removed», модель — {resolution.get('direction')}"
        )

    # 4. Цитаты обязаны быть дословными.
    quotes = resolution.get("evidence_quotes")
    if not isinstance(quotes, (list, tuple)):
        errors.append("тип: evidence_quotes должно быть массивом")
        quotes = []
    for quote in quotes:
        if not isinstance(quote, Mapping):
            errors.append("тип: цитата должна быть объектом")
            continue
        text = normalize(quote.get("quote"))
        if not text:
            continue
        haystack = left if quote.get("side") == "LEFT" else right
        if text not in haystack:
            errors.append(
                f"цитата: {quote.get('quote')!r} не найдена дословно в {quote.get('side')}"
            )

    # 5. Никаких внутренних ссылок и координат: их чеканит бэкенд.
    for name in ("object_label", "facet_label", "engineering_summary", "human_question"):
        value = str(resolution.get(name) or "")
        if _FORBIDDEN_REF_RE.search(value):
            errors.append(f"идентификаторы: поле {name} содержит внутреннюю ссылку")
        if _BBOX_RE.search(value):
            errors.append(f"координаты: поле {name} содержит выдуманную рамку")

    # 6. Согласованность статуса разрешения.
    status = resolution.get("resolution_status")
    if status == "AI_RESOLVED":
        if resolution.get("needs_human_review"):
            errors.append("согласованность: AI_RESOLVED вместе с needs_human_review")
        if resolution.get("dimension") == UNKNOWN_DIMENSION:
            errors.append(
                "политика: AI_RESOLVED с неопределённым типом изменения запрещён"
            )
        if resolution.get("outcome") == "REVIEW_REQUIRED":
            errors.append("согласованность: AI_RESOLVED с outcome=REVIEW_REQUIRED")
        if not quotes:
            errors.append("обоснование: AI_RESOLVED без единой цитаты")
        if resolution.get("confidence") == "UNKNOWN":
            errors.append("согласованность: AI_RESOLVED с неизвестной уверенностью")
        if not str(resolution.get("object_label") or "").strip():
            errors.append("объект: AI_RESOLVED без названия объекта")
        if not (before or after):
            errors.append("значения: AI_RESOLVED без before и после")
    else:
        if status == "HUMAN_REQUIRED" and not resolution.get("needs_human_review"):
            errors.append("согласованность: HUMAN_REQUIRED без needs_human_review")
        if status == "HUMAN_REQUIRED" and not str(
            resolution.get("human_question") or ""
        ).strip():
            errors.append("согласованность: HUMAN_REQUIRED без сформулированного вопроса")
        if resolution.get("human_reason") == "NOT_APPLICABLE":
            errors.append("причина: отказ без указания причины")

    # 7. Неопределённый тип изменения допускает только REVIEW_REQUIRED.
    if (
        resolution.get("dimension") == UNKNOWN_DIMENSION
        and resolution.get("outcome") != "REVIEW_REQUIRED"
    ):
        errors.append(
            "политика: неопределённый тип изменения допускает только REVIEW_REQUIRED"
        )

    if not str(resolution.get("engineering_summary") or "").strip():
        errors.append("содержание: пустое обоснование для инженера")

    return VerifyResult(not errors, errors, warnings)


def verify_batch(
    items: Sequence[Mapping[str, Any]],
    payload: Mapping[str, Any] | None,
) -> tuple[dict[str, VerifyResult], list[str]]:
    """Проверить ответ на партию: полноту, отсутствие лишнего, каждый элемент."""
    problems: list[str] = []
    if not isinstance(payload, Mapping):
        return {}, ["ответ модели не является объектом"]
    resolutions = payload.get("resolutions")
    if not isinstance(resolutions, (list, tuple)):
        return {}, ["ответ модели не содержит массив resolutions"]

    expected = {str(item.get("item_id") or ""): item for item in items}
    seen: dict[str, VerifyResult] = {}
    for resolution in resolutions:
        if not isinstance(resolution, Mapping):
            problems.append("элемент ответа не является объектом")
            continue
        item_id = str(resolution.get("item_id") or "")
        item = expected.get(item_id)
        if item is None:
            # Мощность ответа: модель не имеет права придумать элемент, о
            # котором её не спрашивали.
            problems.append(f"мощность: в ответе элемент {item_id!r} вне пакета")
            continue
        if item_id in seen:
            problems.append(f"мощность: элемент {item_id!r} разрешён дважды")
            continue
        seen[item_id] = verify_resolution(item, resolution)
    missing = sorted(set(expected) - set(seen))
    if missing:
        problems.append(f"полнота: без ответа осталось элементов: {len(missing)}")
    return seen, problems


__all__ = [
    "VERIFIER_VERSION",
    "VerifyResult",
    "normalize",
    "verify_batch",
    "verify_resolution",
]
