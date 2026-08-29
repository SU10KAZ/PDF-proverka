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

VERIFIER_VERSION = "stage-comparison-ai-verifier.v2"

_REQUIRED_FIELDS = (
    "item_id", "resolution_status", "dimension", "direction", "outcome",
    "object_label", "object_evidence_ref", "facet_label",
    "before_value", "before_evidence_ref",
    "after_value", "after_evidence_ref",
    "confidence", "evidence_quotes", "needs_human_review",
    "human_reason", "human_question", "engineering_summary",
)

#: Направления, совместимые с корзиной детерминированного Stage 3. «Добавлено»
#: не может быть «удалено» ни при какой уверенности модели: это не оттенок
#: формулировки, а противоположное утверждение о проекте.
_BUCKET_DIRECTIONS = {
    "added": {"ADDED"},
    "removed": {"REMOVED"},
    "changed": {"REPLACED", "INCREASED", "DECREASED", "ALTERED"},
}

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


def strip_context_mark(value: Any) -> str:
    """Снять маркер текущей строки «»».

    Маркер — наша собственная разметка пакета, а не текст документа. Модель
    честно копирует то, что видит, вместе с ним, и отклонять её за это значит
    ловить не выдумку, а собственное форматирование.
    """
    return _CONTEXT_MARK.sub("", str(value or ""))


def _line_text(line: Any) -> str:
    """Текст строки доказательства независимо от того, словарь это или строка."""
    if isinstance(line, Mapping):
        return strip_context_mark(line.get("text"))
    return strip_context_mark(line)


def _lines(item: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Адресуемые строки пакета: ссылка → строка, сторона, текст, фокус."""
    output: dict[str, dict[str, Any]] = {}
    for side, key in (("LEFT", "left_context"), ("RIGHT", "right_context")):
        for index, line in enumerate(item.get(key) or [], start=1):
            if isinstance(line, Mapping):
                ref = str(line.get("ref") or "")
                focus = bool(line.get("focus"))
                source = str(line.get("source") or "TEXT")
            else:
                # Пакет старого вида: ссылок нет, привязка невозможна.
                ref = ""
                focus = False
                source = "TEXT"
            if not ref:
                ref = f"{'L' if side == 'LEFT' else 'R'}{index}"
            text = _line_text(line)
            output[ref] = {
                "ref": ref,
                "side": side,
                "text": text,
                "normalized": normalize(text),
                # Токены, а не подстроки: «24.5» не должно находиться внутри
                # «124.55» и объявлять чужую строку строкой этого объекта.
                "tokens": line_tokens(text),
                "focus": focus,
                "source": source,
            }
    return output


def _corpus(item: Mapping[str, Any]) -> tuple[str, str]:
    """Всё, что модель имела право видеть: сторона LEFT и сторона RIGHT."""
    left = [str(item.get("before_value") or "")]
    left += [_line_text(line) for line in item.get("left_context") or []]
    right = [str(item.get("after_value") or "")]
    right += [_line_text(line) for line in item.get("right_context") or []]
    return normalize("  ".join(left)), normalize("  ".join(right))


_IDENTITY_TOKEN_RE = re.compile(r"[\w./\-]+", re.UNICODE)
_DIGIT_RE = re.compile(r"\d")


def line_tokens(value: Any) -> set[str]:
    """Слова и числа строки как множество, без склейки в одну подстроку."""
    return {
        token.strip("./-")
        for token in _IDENTITY_TOKEN_RE.findall(normalize(value))
        if token.strip("./-")
    }


def identity_tokens(label: Any) -> set[str]:
    """Чем объект отличается от соседнего объекта того же вида.

    Для «помещение 315.1» это «315.1»: слово «помещение» стоит в каждой строке
    экспликации и не отличает ничего. Если различающих цифр нет вовсе
    («кровля К5»), берутся длинные слова названия.
    """
    tokens = {
        token.strip("./-")
        for token in _IDENTITY_TOKEN_RE.findall(normalize(label))
    }
    numeric = {
        token for token in tokens
        if len(token) >= 2 and _DIGIT_RE.search(token)
    }
    if numeric:
        return numeric
    return {token for token in tokens if len(token) >= 4}


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
    before = strip_context_mark(resolution.get("before_value")) or None
    after = strip_context_mark(resolution.get("after_value")) or None

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
    #    Это не предупреждение: «добавлено» вместо «удалено» — противоположное
    #    утверждение о проекте, и публиковать его нельзя.
    bucket = str(item.get("stage3_bucket") or "")
    direction = str(resolution.get("direction") or "")
    if bucket == "added" and before:
        errors.append(
            "направление: Stage 3 не нашёл значения слева, а модель выдала before_value"
        )
    if bucket == "removed" and after:
        errors.append(
            "направление: Stage 3 не нашёл значения справа, а модель выдала after_value"
        )
    allowed_directions = _BUCKET_DIRECTIONS.get(bucket)
    if allowed_directions and direction not in allowed_directions:
        errors.append(
            f"направление: Stage 3 дал «{bucket}», модель — {direction};"
            " это противоположное утверждение, а не оттенок формулировки"
        )

    # 4. Цитаты обязаны быть дословными И лежать в названной строке.
    lines = _lines(item)
    quotes = resolution.get("evidence_quotes")
    if not isinstance(quotes, (list, tuple)):
        errors.append("тип: evidence_quotes должно быть массивом")
        quotes = []
    for quote in quotes:
        if not isinstance(quote, Mapping):
            errors.append("тип: цитата должна быть объектом")
            continue
        text = normalize(strip_context_mark(quote.get("quote")))
        if not text:
            continue
        side = str(quote.get("side") or "")
        haystack = left if side == "LEFT" else right
        if text not in haystack:
            errors.append(
                f"цитата: {quote.get('quote')!r} не найдена дословно в {side}"
            )
            continue
        ref = str(quote.get("evidence_ref") or "")
        line = lines.get(ref)
        if line is None:
            errors.append(f"привязка: цитата ссылается на несуществующую строку {ref!r}")
        elif line["side"] != side:
            errors.append(
                f"стороны: строка {ref} относится к {line['side']},"
                f" а цитата объявлена как {side}"
            )
        elif text not in line["normalized"]:
            errors.append(
                f"привязка: цитаты {quote.get('quote')!r} нет в строке {ref}"
            )

    # 4a. Значение обязано лежать в НАЗВАННОЙ строке своей стороны.
    #     Проверка «встречается где-нибудь на этой стороне» пропускала
    #     площадь соседнего помещения: обе строки лежат в одном окне.
    def _value_binding(value: str | None, ref_field: str, side: str) -> dict | None:
        if not value:
            return None
        ref = str(resolution.get(ref_field) or "")
        if not ref:
            errors.append(f"привязка: {ref_field} не указан при непустом значении")
            return None
        line = lines.get(ref)
        if line is None:
            errors.append(f"привязка: {ref_field}={ref!r} — такой строки в пакете нет")
            return None
        if line["side"] != side:
            errors.append(
                f"стороны: {ref_field} указывает на строку {ref} стороны"
                f" {line['side']}, а должно быть {side}"
            )
            return None
        if normalize(value) not in line["normalized"]:
            errors.append(
                f"привязка: значение {value!r} отсутствует в строке {ref}"
            )
            return None
        return line

    before_line = _value_binding(before, "before_evidence_ref", "LEFT")
    after_line = _value_binding(after, "after_evidence_ref", "RIGHT")

    # 4b. Объект обязан быть НАЗВАН в строке, из которой взято значение.
    #     Иначе «правильное число у другого объекта» проходит верификатор.
    object_label = str(resolution.get("object_label") or "").strip()
    object_ref = str(resolution.get("object_evidence_ref") or "")
    object_line = lines.get(object_ref) if object_ref else None
    if object_ref and object_line is None:
        errors.append(
            f"привязка: object_evidence_ref={object_ref!r} — такой строки в пакете нет"
        )
    if object_label and object_line is not None:
        tokens = identity_tokens(object_label)
        if tokens and not (tokens & object_line["tokens"]):
            errors.append(
                f"объект: {object_label!r} не назван в строке {object_ref}"
            )
        else:
            for line, where in ((before_line, "LEFT"), (after_line, "RIGHT")):
                if line is None or not tokens:
                    continue
                if not (tokens & line["tokens"]):
                    errors.append(
                        f"объект: значение стороны {where} взято из строки"
                        f" {line['ref']}, в которой объект {object_label!r}"
                        " не назван"
                    )

    # 4c. Хотя бы одна названная строка обязана быть той, вокруг которой
    #     собран элемент. Ответ, опирающийся только на соседние строки,
    #     разбирает другое расхождение.
    named = [
        line for line in (object_line, before_line, after_line)
        if line is not None
    ]
    if named and not any(line["focus"] for line in named):
        errors.append(
            "привязка: ни одна названная строка не относится к самому"
            " разбираемому расхождению"
        )

    # 5. Никаких внутренних ссылок и координат: их чеканит бэкенд.
    for name in ("object_label", "facet_label", "engineering_summary", "human_question"):
        value = str(resolution.get(name) or "")
        if _FORBIDDEN_REF_RE.search(value):
            errors.append(f"идентификаторы: поле {name} содержит внутреннюю ссылку")
        if _BBOX_RE.search(value):
            errors.append(f"координаты: поле {name} содержит выдуманную рамку")

    # 5a. Тип изменения, установленный детерминированным слоем, не обсуждается.
    state = item.get("deterministic_state")
    state = state if isinstance(state, Mapping) else {}
    known_dimension = str(state.get("dimension") or "")
    if (
        known_dimension
        and known_dimension != UNKNOWN_DIMENSION
        and resolution.get("dimension") != known_dimension
    ):
        errors.append(
            f"тип изменения: детерминированный слой установил {known_dimension},"
            f" модель вернула {resolution.get('dimension')}"
        )

    # 6. Согласованность статуса разрешения.
    status = resolution.get("resolution_status")
    if status == "AI_RESOLVED":
        coverage = state.get("recognition_coverage")
        coverage_status = str(
            (coverage or {}).get("status") or "UNKNOWN"
        ) if isinstance(coverage, Mapping) else "UNKNOWN"
        if coverage_status != "SUFFICIENT":
            # Модель не имеет права закрыть то, про что детерминированный слой
            # честно сказал «прочитано ненадёжно»: интерпретировать можно
            # доказательство, а не пропуск распознавания.
            errors.append(
                "полнота: разрешать элемент с непроверенным распознаванием"
                f" нельзя (полнота: {coverage_status})"
            )
        if not object_ref:
            errors.append("привязка: AI_RESOLVED без ссылки на строку с объектом")
        if before and not str(resolution.get("before_evidence_ref") or ""):
            errors.append("привязка: AI_RESOLVED без ссылки на строку LEFT")
        if after and not str(resolution.get("after_evidence_ref") or ""):
            errors.append("привязка: AI_RESOLVED без ссылки на строку RIGHT")
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
    "identity_tokens",
    "line_tokens",
    "normalize",
    "verify_batch",
    "verify_resolution",
]
