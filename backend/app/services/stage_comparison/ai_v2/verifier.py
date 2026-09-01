"""Deterministic verifier for multi-evidence whole-document reasoning."""
from __future__ import annotations

import json
import math
import re
import unicodedata
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

from ..ai import response_contract
from . import schemas
from .context import ContextBundle

VERIFIER_VERSION = "stage-comparison-ai-v2-verifier.v1"
ARITHMETIC_TOLERANCE = 0.005
_NUMBER_RE = re.compile(r"-?\d+(?:[.,]\d+)?")


@dataclass
class VerifyResult:
    ok: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "errors": list(self.errors),
            "warnings": list(self.warnings),
            "verifier_version": VERIFIER_VERSION,
        }


def normalize(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).lower()
    return " ".join(text.replace("ё", "е").replace(",", ".").split())


def _json_text(value: Any) -> str:
    return normalize(json.dumps(value, ensure_ascii=False, sort_keys=True))


def _values(value: Any) -> list[float]:
    output: list[float] = []
    for raw in _NUMBER_RE.findall(str(value or "").replace(",", ".")):
        try:
            output.append(float(raw))
        except ValueError:
            continue
    return output


def _grounded_number(item: Mapping[str, Any], number: float) -> bool:
    def walk(value: Any) -> bool:
        if isinstance(value, bool):
            return False
        if isinstance(value, (int, float)):
            return math.isclose(float(value), number, rel_tol=1e-9, abs_tol=1e-9)
        if isinstance(value, str):
            return any(math.isclose(found, number, rel_tol=1e-9, abs_tol=1e-9)
                       for found in _values(value))
        if isinstance(value, Mapping):
            return any(walk(child) for child in value.values())
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            return any(walk(child) for child in value)
        return False
    return walk(item)


def _grounded_value(item: Mapping[str, Any], value: Any) -> bool:
    """Require an exact scalar value, not a persuasive substring.

    In particular, ``1`` must not be considered evidence for a cable count
    merely because a designation such as ``1QF6`` occurs somewhere in the
    record.  Numeric values are tokenised and compared as numbers; textual
    values must equal a scalar leaf after normalisation.
    """
    if isinstance(value, bool) or value is None:
        return False
    if isinstance(value, (int, float)):
        return _grounded_number(item, float(value))
    claimed = normalize(value)
    if not claimed:
        return False

    def walk(candidate: Any) -> bool:
        if isinstance(candidate, Mapping):
            return any(walk(child) for child in candidate.values())
        if isinstance(candidate, Sequence) and not isinstance(
            candidate, (str, bytes)
        ):
            return any(walk(child) for child in candidate)
        return normalize(candidate) == claimed

    return walk(item)


def _side(ref: str, catalog: Mapping[str, Mapping[str, Any]]) -> str:
    return str((catalog.get(ref) or {}).get("side") or "")


def _identity_values(item: Mapping[str, Any], attribute: str) -> set[str]:
    if attribute == "canonical_identity":
        return {normalize(item.get("canonical_identity"))} - {""}
    if attribute == "node_type":
        return {normalize(item.get("entity_type"))} - {""}
    if attribute == "section":
        return {normalize(item.get("section"))} - {""}
    if attribute == "label":
        return {normalize(item.get("label"))} - {""}
    if attribute == "designation":
        values = [
            *list(item.get("designations") or ()),
            *list(item.get("source_tokens") or ()),
        ]
        return {normalize(value) for value in values if normalize(value)}
    if attribute == "mode":
        return {normalize(item.get("mode"))} - {""}
    return set()


def _verify_claim(
    claim: Mapping[str, Any], catalog: Mapping[str, Mapping[str, Any]],
    allowed_refs: set[str], errors: list[str], where: str,
) -> None:
    refs = [str(value) for value in claim.get("evidence_refs") or ()]
    for ref in refs:
        if ref not in catalog:
            errors.append(f"{where}: evidence_ref {ref!r} не существует")
        elif ref not in allowed_refs:
            errors.append(f"{where}: evidence_ref {ref!r} не передан задаче")
    for field_name in ("subject_ref", "object_ref"):
        ref = str(claim.get(field_name) or "")
        if ref and ref not in catalog:
            errors.append(f"{where}: {field_name} {ref!r} не существует")
        elif ref and ref not in allowed_refs:
            errors.append(f"{where}: {field_name} {ref!r} не передан задаче")

    kind = str(claim.get("kind") or "")
    subject_ref = str(claim.get("subject_ref") or "")
    object_ref = str(claim.get("object_ref") or "")
    subject = catalog.get(subject_ref) or {}
    obj = catalog.get(object_ref) or {}
    attribute = str(claim.get("attribute") or "")
    value = claim.get("value")

    if kind == "IDENTITY_FEATURE":
        if not subject_ref or not object_ref:
            errors.append(f"{where}: IDENTITY_FEATURE требует два объекта")
            return
        left = _identity_values(subject, attribute)
        right = _identity_values(obj, attribute)
        claimed = normalize(value)
        if not claimed or claimed not in left or claimed not in right:
            errors.append(
                f"{where}: признак {attribute}={value!r} отсутствует у обоих объектов"
            )
    elif kind == "VALUE":
        target = subject or (catalog.get(refs[0]) if refs else {}) or {}
        if not _grounded_value(target, value):
            errors.append(f"{where}: значение {value!r} не найдено в evidence")
        unit = claim.get("unit")
        if unit and normalize(unit) not in _json_text(target):
            errors.append(f"{where}: единица {unit!r} не найдена в evidence")
    elif kind == "GRAPH_RELATION":
        relation = normalize(value or attribute)
        found = False
        for ref in refs:
            item = catalog.get(ref) or {}
            endpoints = {str(item.get("from_entity") or ""), str(item.get("to_entity") or "")}
            subject_id = str(subject.get("entity_id") or "")
            object_id = str(obj.get("entity_id") or "")
            if (
                normalize(item.get("relation")) == relation
                and {subject_id, object_id} <= endpoints
            ):
                found = True
                break
        if not found:
            errors.append(f"{where}: заявленная графовая связь не существует")
    elif kind == "ARITHMETIC":
        operands = list(claim.get("operands") or ())
        expected = claim.get("expected")
        operation = str(claim.get("operation") or "")
        grounded: list[float] = []
        for index, operand in enumerate(operands):
            ref = str((operand or {}).get("evidence_ref") or "")
            number = (operand or {}).get("value")
            if ref not in catalog or ref not in allowed_refs:
                errors.append(f"{where}: операнд {index + 1} ссылается не на evidence")
                continue
            if not isinstance(number, (int, float)) or isinstance(number, bool):
                errors.append(f"{where}: операнд {index + 1} не число")
                continue
            if not _grounded_number(catalog[ref], float(number)):
                errors.append(
                    f"{where}: число {number} не найдено в evidence {ref}"
                )
                continue
            grounded.append(float(number))
        if expected is None or len(grounded) != len(operands) or not grounded:
            errors.append(f"{where}: арифметика не имеет проверяемых операндов")
            return
        calculated: float | None = None
        if operation == "SUM":
            calculated = sum(grounded)
        elif operation == "DIFFERENCE" and len(grounded) == 2:
            calculated = grounded[0] - grounded[1]
        elif operation == "PRODUCT":
            calculated = math.prod(grounded)
        elif operation == "RATIO" and len(grounded) == 2 and grounded[1] != 0:
            calculated = grounded[0] / grounded[1]
        if calculated is None:
            errors.append(f"{where}: операция {operation!r} неприменима")
        elif not math.isclose(
            calculated, float(expected), rel_tol=ARITHMETIC_TOLERANCE,
            abs_tol=ARITHMETIC_TOLERANCE,
        ):
            errors.append(
                f"{where}: арифметика даёт {calculated:g}, не {float(expected):g}"
            )


def _strong_identity(
    left: Mapping[str, Any], right: Mapping[str, Any],
) -> bool:
    for attribute in ("canonical_identity", "label", "designation"):
        if _identity_values(left, attribute) & _identity_values(right, attribute):
            return True
    return False


def _same_entity_guard(
    resolution: Mapping[str, Any], task: Mapping[str, Any],
    catalog: Mapping[str, Mapping[str, Any]], errors: list[str],
) -> None:
    refs = [str(value) for value in resolution.get("selected_candidate_refs") or ()]
    if len(refs) != 2:
        errors.append("тождество: требуется ровно один кандидат каждой стороны")
        return
    sides = {_side(ref, catalog) for ref in refs}
    if sides != {"LEFT", "RIGHT"}:
        errors.append("тождество: кандидаты должны относиться к разным сторонам")
        return
    left_ref = next(ref for ref in refs if _side(ref, catalog) == "LEFT")
    right_ref = next(ref for ref in refs if _side(ref, catalog) == "RIGHT")
    left, right = catalog[left_ref], catalog[right_ref]
    left_type = normalize(left.get("entity_type") or left.get("row_kind"))
    right_type = normalize(right.get("entity_type") or right.get("row_kind"))
    if left_type and right_type and left_type != right_type:
        errors.append("тождество: типы кандидатов различаются")
    left_section, right_section = normalize(left.get("section")), normalize(right.get("section"))
    if left_section and right_section and left_section != right_section:
        errors.append("тождество: кандидаты принадлежат разным секциям")
    if task.get("task_type") == schemas.FUNCTIONAL_IDENTITY and not _strong_identity(
        left, right
    ):
        errors.append("тождество: нет общего проверяемого идентификационного признака")


def _change_guard(
    resolution: Mapping[str, Any], task: Mapping[str, Any],
    catalog: Mapping[str, Mapping[str, Any]], errors: list[str],
) -> None:
    refs = [
        str(value) for value in [
            *list(resolution.get("evidence_refs") or ()),
            *list(resolution.get("selected_candidate_refs") or ()),
        ]
    ]
    changes = [catalog[ref] for ref in refs if ref.startswith("FAST:CHANGE:")]
    if not changes:
        errors.append("изменение: нет ссылки на исходную FAST-находку")
        return
    change = changes[0]
    left_refs = [
        f"LEFT:NODE:{value}" for value in change.get("left_nodes") or ()
        if f"LEFT:NODE:{value}" in catalog
    ]
    right_refs = [
        f"RIGHT:NODE:{value}" for value in change.get("right_nodes") or ()
        if f"RIGHT:NODE:{value}" in catalog
    ]
    # An asserted absence with no old-side entity is still bounded only by
    # recognition coverage.  The model cannot turn that into proven absence.
    if not left_refs or not right_refs:
        errors.append("изменение: одна сторона не имеет положительного объекта-доказательства")
        return
    selected = set(str(value) for value in resolution.get("selected_candidate_refs") or ())
    pairs = [(catalog[left], catalog[right]) for left in left_refs for right in right_refs
             if left in selected and right in selected]
    if not pairs or not any(_strong_identity(left, right) for left, right in pairs):
        errors.append("изменение: тождество узлов до/после не проверено")

    # A REVIEW_REQUIRED FAST change is the question, not independent proof of
    # its own values.  Require positive, non-FAST evidence on both sides.
    # For count claims, only a table row (or another future structured record)
    # is accepted; a device designation containing a digit is not a count.
    grounded: dict[str, list[Any]] = {"LEFT": [], "RIGHT": []}
    for claim in resolution.get("claims") or ():
        if not isinstance(claim, Mapping) or claim.get("kind") != "VALUE":
            continue
        subject_ref = str(claim.get("subject_ref") or "")
        side = _side(subject_ref, catalog)
        claim_refs = {str(value) for value in claim.get("evidence_refs") or ()}
        if side not in grounded or subject_ref not in claim_refs:
            continue
        if subject_ref.startswith("FAST:"):
            continue
        if claim.get("attribute") == "count" and ":ROW:" not in subject_ref:
            continue
        grounded[side].append(claim.get("value"))
    if not grounded["LEFT"] or not grounded["RIGHT"]:
        errors.append(
            "изменение: значения не подтверждены независимыми evidence обеих сторон"
        )
        return
    left_values = {normalize(value) for value in grounded["LEFT"]}
    right_values = {normalize(value) for value in grounded["RIGHT"]}
    verdict = str(resolution.get("verdict") or "")
    if verdict == "SUPPORTED_CHANGE" and left_values & right_values:
        errors.append("изменение: независимые значения сторон не различаются")
    if verdict == "FORMATTING_ONLY" and not left_values & right_values:
        errors.append("форматирование: нет одинакового значения на обеих сторонах")


def _mode_guard(
    resolution: Mapping[str, Any], catalog: Mapping[str, Mapping[str, Any]],
    errors: list[str],
) -> None:
    refs = [str(value) for value in resolution.get("selected_candidate_refs") or ()]
    modes = [normalize((catalog.get(ref) or {}).get("mode")) for ref in refs]
    if len(modes) != 2 or not all(modes) or modes[0] != modes[1]:
        errors.append("режимы: эквивалентность не подтверждена одинаковыми явными режимами")


def verify_resolution(
    task: Mapping[str, Any], resolution: Mapping[str, Any], bundle: ContextBundle,
) -> VerifyResult:
    errors: list[str] = []
    warnings: list[str] = []
    task_id = str(task.get("task_id") or "")
    task_type = str(task.get("task_type") or "")
    if str(resolution.get("task_id") or "") != task_id:
        errors.append("привязка: ответ относится к другой задаче")
    if str(resolution.get("task_type") or "") != task_type:
        errors.append("привязка: тип ответа не совпадает с типом задачи")
    verdict = str(resolution.get("verdict") or "")
    status = str(resolution.get("status") or "")
    special = (
        (status == "NEED_MORE_EVIDENCE" and verdict == "NEED_MORE_EVIDENCE")
        or (status == "UNRESOLVABLE" and verdict == "UNRESOLVABLE")
    )
    if not special and verdict not in schemas.VERDICTS_BY_TYPE.get(task_type, ()):
        errors.append(f"контракт: verdict {verdict!r} недопустим для {task_type}")

    focus = bundle.focused_by_task.get(task_id) or {}
    candidate_refs = set(str(value) for value in focus.get("candidate_refs") or ())
    context_refs = set(str(value) for value in focus.get("context_refs") or ())
    # Every whole-sheet record is present in Level 1, so it may ground a
    # claim.  Candidate selection remains restricted to Level 2.
    allowed_refs = set(bundle.evidence_catalog)
    selected = [str(value) for value in resolution.get("selected_candidate_refs") or ()]
    for ref in selected:
        if ref not in candidate_refs:
            errors.append(f"кандидат {ref!r} отсутствует в candidate_refs задачи")
    for ref in resolution.get("evidence_refs") or ():
        if str(ref) not in allowed_refs:
            errors.append(f"evidence_ref {ref!r} не существует")

    requested = list(resolution.get("requested_evidence") or ())
    if status == "NEED_MORE_EVIDENCE":
        if verdict != "NEED_MORE_EVIDENCE" or not requested:
            errors.append("добор: статус требует точного requested_evidence")
        if any(value not in schemas.EXPANSION_ALLOWLIST for value in requested):
            errors.append("добор: запрос вне закрытого справочника")
        return VerifyResult(not errors, errors, warnings)
    if status == "UNRESOLVABLE":
        if verdict != "UNRESOLVABLE":
            errors.append("отказ: UNRESOLVABLE требует одноимённый verdict")
        return VerifyResult(not errors, errors, warnings)
    if status != "RESOLVED":
        errors.append(f"контракт: неизвестный status {status!r}")
    if verdict in {"INSUFFICIENT_EVIDENCE", "NEED_MORE_EVIDENCE", "UNRESOLVABLE"}:
        errors.append("публикация: отказной verdict нельзя пометить RESOLVED")
    if resolution.get("confidence") in {None, "", "UNKNOWN"}:
        errors.append("публикация: нет определённой уверенности")
    if not resolution.get("evidence_refs"):
        errors.append("публикация: нет evidence_refs")
    claims = list(resolution.get("claims") or ())
    if not claims:
        errors.append("публикация: нет структурированных claims")
    for index, claim in enumerate(claims, 1):
        if not isinstance(claim, Mapping):
            errors.append(f"claim {index}: не объект")
            continue
        _verify_claim(claim, bundle.evidence_catalog, allowed_refs, errors, f"claim {index}")

    if verdict == "SAME_ENTITY":
        _same_entity_guard(resolution, task, bundle.evidence_catalog, errors)
    elif verdict in {"SUPPORTED_CHANGE", "FORMATTING_ONLY"} and task.get(
        "source_kind"
    ) == "CHANGE_INCOMPLETE_EVIDENCE":
        _change_guard(resolution, task, bundle.evidence_catalog, errors)
    elif verdict in {"DOCUMENT_ERROR", "CONFIRMED_CONTRADICTION"}:
        if not any(str(ref).startswith("FAST:INCONSISTENCY:")
                   for ref in resolution.get("evidence_refs") or ()):
            errors.append("противоречие: нет ссылки на детерминированный evidence")
    elif verdict == "EQUIVALENT":
        _mode_guard(resolution, bundle.evidence_catalog, errors)

    return VerifyResult(not errors, errors, warnings)


def verify_batch(
    tasks: Sequence[Mapping[str, Any]], payload: Mapping[str, Any],
    bundle: ContextBundle,
) -> tuple[dict[str, VerifyResult], list[str]]:
    batch_errors = response_contract.validate(payload, schemas.ANALYST_SCHEMA)
    if batch_errors:
        return {}, batch_errors
    by_task: dict[str, Mapping[str, Any]] = {}
    duplicate: set[str] = set()
    for value in payload.get("resolutions") or ():
        task_id = str(value.get("task_id") or "")
        if task_id in by_task:
            duplicate.add(task_id)
        by_task[task_id] = value
    expected = {str(task.get("task_id") or "") for task in tasks}
    extra = sorted(set(by_task) - expected)
    missing = sorted(expected - set(by_task))
    if extra:
        batch_errors.append("лишние task_id: " + ", ".join(extra))
    if missing:
        batch_errors.append("нет ответов для: " + ", ".join(missing))
    if duplicate:
        batch_errors.append("дубли task_id: " + ", ".join(sorted(duplicate)))
    results = {
        task_id: verify_resolution(task, by_task[task_id], bundle)
        for task in tasks
        if (task_id := str(task.get("task_id") or "")) in by_task
    }
    return results, batch_errors


__all__ = [
    "ARITHMETIC_TOLERANCE",
    "VERIFIER_VERSION",
    "VerifyResult",
    "normalize",
    "verify_batch",
    "verify_resolution",
]
