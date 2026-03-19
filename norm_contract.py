"""
norm_contract.py
----------------
Norm contract: классификация, обогащение и нормализация
нормативных полей в findings.

Центральный модуль для:
- Вычисления norm_status / norm_quote_status из raw полей
- Обогащения findings результатами norm verification
- Backward-compatible вычисления norm_confidence
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


# ─── Norm status classification ────────────────────────────────────────────

def classify_norm_status(finding: dict) -> str:
    """Классифицировать статус нормативной ссылки finding.

    Returns:
        "exact_quote"          — есть норма + точная цитата + высокая уверенность
        "paraphrased"          — есть норма + приблизительная цитата
        "norm_detected_no_quote" — есть норма, но нет цитаты
        "no_norm_cited"        — нет нормативной ссылки
        "not_found"            — норма указана, но не найдена при верификации
        "invalid_reference"    — норма заменена/отменена
    """
    norm = (finding.get("norm") or "").strip()
    norm_quote = finding.get("norm_quote")
    conf = finding.get("norm_confidence") or 0
    # Поля от norm verification (если обогащены)
    verification = finding.get("norm_verification", {})
    check_status = verification.get("status", "")

    if not norm:
        return "no_norm_cited"

    if check_status in ("replaced", "cancelled"):
        return "invalid_reference"
    if check_status == "not_found":
        return "not_found"

    if norm_quote and isinstance(norm_quote, str) and len(norm_quote) > 10:
        if conf >= 0.85:
            return "exact_quote"
        return "paraphrased"

    return "norm_detected_no_quote"


def classify_norm_quote_status(finding: dict) -> str:
    """Классифицировать статус цитаты.

    Returns:
        "exact"       — точная цитата с высокой уверенностью
        "approximate" — цитата есть, но приблизительная
        "missing"     — цитата отсутствует
    """
    norm_quote = finding.get("norm_quote")
    conf = finding.get("norm_confidence") or 0

    if not norm_quote or not isinstance(norm_quote, str) or len(norm_quote) < 5:
        return "missing"
    if conf >= 0.85:
        return "exact"
    return "approximate"


def compute_norm_confidence(finding: dict) -> float:
    """Вычислить norm_confidence из структурированных полей.

    Backward-compatible: если raw norm_confidence есть — использовать его,
    но корректировать вверх/вниз на основе verification данных.
    """
    raw_conf = finding.get("norm_confidence")
    if raw_conf is None:
        raw_conf = 0.0

    norm = (finding.get("norm") or "").strip()
    norm_quote = finding.get("norm_quote")
    verification = finding.get("norm_verification", {})

    # Нет нормы — confidence = 0
    if not norm:
        return 0.0

    base = float(raw_conf)

    # Бонус за verified status
    check_status = verification.get("status", "")
    if check_status == "active":
        base = max(base, 0.6)  # минимум 0.6 для подтверждённо действующей нормы
    elif check_status in ("replaced", "cancelled"):
        base = min(base, 0.3)  # не выше 0.3 для замёненной/отменённой

    # Бонус за verified quote
    if verification.get("paragraph_verified"):
        base = max(base, 0.9)  # подтверждённая цитата
    elif verification.get("actual_quote") and not norm_quote:
        # Верификация нашла цитату, а в finding её нет — подтянуть
        base = max(base, 0.75)

    # Бонус за exact quote
    if norm_quote and isinstance(norm_quote, str) and len(norm_quote) > 20:
        base = max(base, 0.7)

    return round(min(base, 1.0), 2)


# ─── Enrichment: findings <- norm_checks ──────────────────────────────────

def enrich_findings_from_norm_checks(
    findings: list[dict],
    norm_checks: dict,
) -> dict:
    """Обогатить findings данными из norm_checks.json.

    Добавляет/обновляет в каждом finding:
    - norm_verification: {status, edition_status, verified_via, ...}
    - norm_status: classification
    - norm_quote_status: classification
    - norm_quote: actual_quote если найдена и лучше текущей
    - norm_confidence: пересчитанный

    Returns: статистика обогащения.
    """
    checks = norm_checks.get("checks", [])
    paragraph_checks = norm_checks.get("paragraph_checks", [])

    # Индекс: doc_number → check
    check_index = {}
    for check in checks:
        doc = check.get("doc_number", "")
        if doc:
            check_index[doc] = check
        # Также индексируем по cited_as для fuzzy matching
        cited = check.get("norm_as_cited", "")
        if cited and cited != doc:
            check_index[cited] = check

    # Индекс: finding_id → paragraph_check
    para_index = {}
    for pc in paragraph_checks:
        fid = pc.get("finding_id", "")
        if fid:
            if fid not in para_index:
                para_index[fid] = []
            para_index[fid].append(pc)

    stats = {
        "total": len(findings),
        "enriched_verification": 0,
        "enriched_quote": 0,
        "status_upgrade": 0,
        "confidence_changed": 0,
    }

    for finding in findings:
        fid = finding.get("id", "")
        norm_raw = (finding.get("norm") or "").strip()

        # Найти matching check
        matched_check = None
        if norm_raw:
            # Пробуем по doc_number (нормализованному)
            for doc_key, check in check_index.items():
                if doc_key in norm_raw or norm_raw.startswith(doc_key):
                    matched_check = check
                    break
            # Fallback: по affected_findings
            if not matched_check:
                for check in checks:
                    if fid in check.get("affected_findings", []):
                        matched_check = check
                        break

        # Обогащаем norm_verification
        if matched_check:
            verification = {
                "status": matched_check.get("status", "unknown"),
                "edition_status": matched_check.get("edition_status", "unknown"),
                "verified_via": matched_check.get("verified_via", "unknown"),
                "needs_revision": matched_check.get("needs_revision", False),
                "current_version": matched_check.get("current_version"),
                "replacement_doc": matched_check.get("replacement_doc"),
            }
            finding["norm_verification"] = verification
            stats["enriched_verification"] += 1

        # Обогащаем paragraph data
        pcs = para_index.get(fid, [])
        for pc in pcs:
            actual_quote = pc.get("actual_quote")
            verified = pc.get("paragraph_verified", False)

            if actual_quote and isinstance(actual_quote, str) and len(actual_quote) > 10:
                current_quote = finding.get("norm_quote")
                # Обновляем если текущая цитата отсутствует или хуже
                if not current_quote or not isinstance(current_quote, str):
                    finding["norm_quote"] = actual_quote
                    finding["norm_source"] = "paragraph_check"
                    stats["enriched_quote"] += 1

            if verified:
                finding.setdefault("norm_verification", {})["paragraph_verified"] = True

        # Классифицируем
        finding["norm_status"] = classify_norm_status(finding)
        finding["norm_quote_status"] = classify_norm_quote_status(finding)
        finding["norm_policy_class"] = compute_norm_policy_class(finding)

        # Пересчитываем confidence
        old_conf = finding.get("norm_confidence")
        new_conf = compute_norm_confidence(finding)
        if old_conf != new_conf:
            stats["confidence_changed"] += 1
        finding["norm_confidence"] = new_conf

    return stats


# ─── Selective Critic calibration ──────────────────────────────────────────

# Пороги norm_confidence по severity для Selective Critic
NORM_CONFIDENCE_THRESHOLDS = {
    "КРИТИЧЕСКОЕ": 0.7,
    "ЭКОНОМИЧЕСКОЕ": 0.6,
    "ЭКСПЛУАТАЦИОННОЕ": 0.5,
    "РЕКОМЕНДАТЕЛЬНОЕ": 0.5,
    "ПРОВЕРИТЬ ПО СМЕЖНЫМ": 0.3,
}

DEFAULT_NORM_CONFIDENCE_THRESHOLD = 0.6


# ─── Norm policy class ────────────────────────────────────────────────────

def compute_norm_policy_class(finding: dict) -> str:
    """Вычислить norm_policy_class по severity.

    Returns:
        "required"    — КРИТИЧЕСКОЕ/ЭКОНОМИЧЕСКОЕ: норма обязательна
        "recommended" — ЭКСПЛУАТАЦИОННОЕ: норма желательна
        "optional"    — РЕКОМЕНДАТЕЛЬНОЕ/ПРОВЕРИТЬ ПО СМЕЖНЫМ: допустимо без нормы
    """
    severity = finding.get("severity", "")
    if severity in ("КРИТИЧЕСКОЕ", "ЭКОНОМИЧЕСКОЕ"):
        return "required"
    if severity == "ЭКСПЛУАТАЦИОННОЕ":
        return "recommended"
    return "optional"


def should_review_norm(finding: dict) -> bool:
    """Определить, нужна ли проверка нормативной ссылки finding.

    Учитывает norm_status, norm_policy_class, severity, norm_confidence.
    """
    norm_status = finding.get("norm_status") or classify_norm_status(finding)

    # Нет нормы → решение зависит от policy class
    if norm_status == "no_norm_cited":
        policy = finding.get("norm_policy_class") or compute_norm_policy_class(finding)
        return policy == "required"

    # Невалидная ссылка → обязательно проверять
    if norm_status in ("not_found", "invalid_reference"):
        return True

    # Дифференцированный порог по severity
    severity = finding.get("severity", "")
    threshold = NORM_CONFIDENCE_THRESHOLDS.get(
        severity, DEFAULT_NORM_CONFIDENCE_THRESHOLD
    )

    conf = finding.get("norm_confidence") or 0
    return conf < threshold
