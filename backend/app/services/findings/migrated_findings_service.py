"""
Сервис «migrated findings» — перенос экспертно подтверждённых замечаний из
предыдущей проверенной версии (V_{N-1}) в текущую версию (V_N) после recheck.

Главные принципы:
- accepted findings из V1 НЕ копируются автоматически в V2;
- каждое замечание проходит через recheck (deterministic-сравнение с уже
  найденными в V2 findings);
- ничего не дублируется (idempotency: повторный запуск не плодит migrated
  finding с тем же `origin_finding_id`);
- результат пишется ТОЛЬКО в `_output/migrated_findings_report.json` нужной
  версии (V2's _output, не V1's);
- в `03_findings.json` V2 добавляются только `still_relevant` items.

Migration statuses:
- still_relevant            — замечание актуально и в V2, добавляется в findings.
- duplicate_of_new_finding  — V2 уже самостоятельно нашла это нарушение;
                              существующий finding обогащается origin metadata.
- not_found_in_new_version  — в v2 не найдено корректного смыслового совпадения
                              (для non-critical замечаний). НЕ означает «устранено» —
                              автомат не вправе ставить такой вердикт без
                              подтверждения, проверьте вручную.
- possibly_resolved         — то же, что not_found_in_new_version, но для
                              КРИТИЧЕСКОГО замечания. Требует обязательной
                              проверки экспертом.
- needs_manual_review       — есть похожие кандидаты, но уверенности нет.
- false_positive_rejected   — локальный матч был отклонён семантической проверкой.
- not_verifiable            — недостаточно данных для recheck.
- source_missing            — соответствующий раздел/документ отсутствует в V2.
- current_findings_missing  — у V2 ещё нет 03_findings.json; recheck отложен.
- id_mismatch_in_source     — в expert_review.json есть accepted id, но findings
                              файла этой версии не содержит такого id (например,
                              expert принял замечание из norm-verified версии,
                              а fallback нашёл только pre_norm). Диагностика
                              записывается в отчёт.
"""
from __future__ import annotations

import json
import logging
import os
import re
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from backend.app.services.common import version_service
from backend.app.services.common.project_service import resolve_project_dir


logger = logging.getLogger(__name__)


SCHEMA_VERSION = 2
MIGRATED_REPORT_FILENAME = "migrated_findings_report.json"

PRE_ENRICHMENT_PREFIX = "_pre_enrichment_"
FINDINGS_FILE_CANDIDATES = (
    "03a_norms_verified.json",
    "03_findings.json",
    "03_findings_pre_norm.json",
)
EXPERT_REVIEW_FILE = "expert_review.json"

# Скоринг.
DUPLICATE_SCORE_THRESHOLD = 0.70  # legacy для _find_duplicate (backwards compat).
BORDERLINE_LOW = 0.45
BORDERLINE_HIGH = 0.95
# Уверенный матч без LLM = >= BORDERLINE_HIGH.
# Borderline = [BORDERLINE_LOW, BORDERLINE_HIGH) — требует LLM или ручной проверки.
# < BORDERLINE_LOW = точно не дубль.

# Конфиденс (отдельное поле в отчёте/UI) — clamp от raw_score к [0, 1].
# Точка 0.5 confidence = середина borderline-зоны.
def _to_confidence(raw_score: float) -> float:
    if raw_score <= 0.0:
        return 0.0
    if raw_score >= BORDERLINE_HIGH:
        return 1.0
    return max(0.0, min(1.0, raw_score / BORDERLINE_HIGH))


CRITICAL_SEVERITIES = {"критическое", "critical", "критично"}

# Конфигурация LLM recheck. Все настройки читаются из env при каждом запуске —
# чтобы тесты могли monkeypatch'ить, а оператор мог менять без рестарта.
LLM_RECHECK_DEFAULT_TIMEOUT_SEC = 120
LLM_RECHECK_DEFAULT_MAX_PAIRS = 10


def _llm_recheck_enabled() -> bool:
    """Default OFF. Включается env MIGRATED_FINDINGS_LLM_RECHECK=1."""
    return os.environ.get("MIGRATED_FINDINGS_LLM_RECHECK", "0").strip().lower() in {
        "1", "true", "yes", "on",
    }


def _llm_recheck_timeout_sec() -> int:
    raw = os.environ.get("MIGRATED_FINDINGS_LLM_TIMEOUT_SEC", "").strip()
    try:
        v = int(raw) if raw else LLM_RECHECK_DEFAULT_TIMEOUT_SEC
    except ValueError:
        v = LLM_RECHECK_DEFAULT_TIMEOUT_SEC
    # Hard floor/ceiling — защита от DoS.
    return max(10, min(v, 300))


def _llm_recheck_max_pairs() -> int:
    raw = os.environ.get("MIGRATED_FINDINGS_LLM_MAX_PAIRS", "").strip()
    try:
        v = int(raw) if raw else LLM_RECHECK_DEFAULT_MAX_PAIRS
    except ValueError:
        v = LLM_RECHECK_DEFAULT_MAX_PAIRS
    return max(0, min(v, 50))

# Какие значения `decision` в expert_review.json трактуются как «эксперт
# подтвердил». На сегодня модель в `expert_review.py` определяет только
# `accepted`/`rejected`, но реальные данные могут содержать legacy-варианты
# из старых проектов — расширяем по принципу либеральности на чтение.
ACCEPTED_DECISIONS = {
    "accepted", "agreed", "approved", "confirmed",
    "customer_confirmed",  # уровень заказчика, см. KnowledgeBaseEntry.status
}

REJECTED_DECISIONS = {
    "rejected", "hidden", "suggested_reject",
    "false_positive", "duplicate",
}


class MigratedFindingsError(RuntimeError):
    """Ошибка валидации (например, version_id=v1 для migrated check)."""


def _now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _load_json(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None


# ─── Поиск исходников версии (с fallback на _pre_enrichment_* backup) ──


def _pre_enrichment_dirs(output_dir: Path) -> list[Path]:
    """Все `_pre_enrichment_*` бэкапы в `_output/`, отсортированные по убыванию
    (самый свежий первым). Имена вида `_pre_enrichment_2026-05-18T10-05-28` —
    лексикографический порядок совпадает с хронологическим.
    """
    if not output_dir.exists():
        return []
    try:
        pres = sorted(
            (p for p in output_dir.iterdir()
             if p.is_dir() and p.name.startswith(PRE_ENRICHMENT_PREFIX)),
            reverse=True,
        )
    except OSError:
        return []
    return pres


def _find_findings_file_in_dir(d: Path) -> Optional[Path]:
    """Найти любой подходящий findings JSON в директории (по приоритету)."""
    for fname in FINDINGS_FILE_CANDIDATES:
        cand = d / fname
        if cand.exists() and _load_json(cand) is not None:
            return cand
    return None


def _resolve_version_sources(
    project_dir: Path, project_id: str, version_id: str,
    *, require_review: bool = True,
) -> dict[str, Any]:
    """Найти источники findings/expert_review для версии.

    Возвращает dict:
        {
            "origin": "primary" | "backup_pre_enrichment" | "missing",
            "findings_path": Optional[Path],
            "review_path": Optional[Path],
            "backup_dir": Optional[Path],
        }

    Параметры:
        require_review: если True (default), источник считается валидным только
            при наличии и findings, и expert_review.json. Это режим для
            «предыдущей проверенной версии». Для current version, где нам
            нужны только findings, передавать False.

    Порядок поиска:
    1. Основной `<version_dir>/_output/` (любой из FINDINGS_FILE_CANDIDATES + expert_review.json);
    2. Если нет, ищем последний `_pre_enrichment_*` внутри того же `_output/` —
       prepare-стадия для v2 переносит туда v1's артефакты. Read-only fallback.
    """
    result: dict[str, Any] = {
        "origin": "missing",
        "findings_path": None,
        "review_path": None,
        "backup_dir": None,
    }
    try:
        version_dir = version_service.get_version_dir(project_dir, project_id, version_id)
    except version_service.VersionNotFoundError:
        return result
    output_dir = version_dir / "_output"

    # 1. Прямые файлы: v2-раскладка (03_analysis/latest + 04_review) первой,
    # затем legacy `_output/`. Без v2-кандидатов на projects_v2-primary сервис
    # был слеп: findings лежат в 03_analysis/latest, вердикты — в 04_review.
    findings_dirs = [output_dir]
    review_candidates = [output_dir / EXPERT_REVIEW_FILE]
    try:
        from backend.app.services.storage.projects_v2_source_resolver import (
            is_projects_v2_version_dir,
        )
        if is_projects_v2_version_dir(version_dir):
            findings_dirs = [version_dir / "03_analysis" / "latest", output_dir]
            review_candidates = [
                version_dir / "04_review" / EXPERT_REVIEW_FILE,
                version_dir / "03_analysis" / "latest" / EXPERT_REVIEW_FILE,
                output_dir / EXPERT_REVIEW_FILE,
            ]
    except Exception:  # noqa: BLE001 — fail-soft: остаёмся на legacy-путях
        pass

    primary_findings = next(
        (p for d in findings_dirs if (p := _find_findings_file_in_dir(d)) is not None),
        None,
    )
    primary_review: Optional[Path] = next(
        (p for p in review_candidates if p.exists()), None,
    )

    if primary_findings is not None and (primary_review is not None or not require_review):
        result.update(
            origin="primary",
            findings_path=primary_findings,
            review_path=primary_review,
        )
        return result

    # 2. Fallback: ищем самый поздний `_pre_enrichment_*` бэкап, в котором
    # есть И findings, И expert_review.json. Бэкап без review не считается
    # валидным источником: review — обязательная часть «проверенной версии»,
    # без него мы не знаем, какие findings экспертом приняты.
    #
    # Бэкапы пробегаем по убыванию даты (новые первыми) и берём первый
    # полноценный. Если такого нет — fallback не сработает.
    best_backup: Optional[Path] = None
    best_backup_findings: Optional[Path] = None
    best_backup_review: Optional[Path] = None
    for backup in _pre_enrichment_dirs(output_dir):
        b_findings = _find_findings_file_in_dir(backup)
        b_review_path = backup / EXPERT_REVIEW_FILE
        b_review = b_review_path if b_review_path.exists() else None
        if b_findings is not None and b_review is not None:
            best_backup = backup
            best_backup_findings = b_findings
            best_backup_review = b_review
            logger.info(
                "migrated-findings: %s/%s using backup %s "
                "(findings=%s, review=%s)",
                project_id, version_id, backup.name,
                b_findings.name, b_review.name,
            )
            break

    findings_path = primary_findings or best_backup_findings
    review_path = primary_review or best_backup_review
    # Источник считается валидным:
    # - для previous-version recheck: при наличии findings+review;
    # - для current-version findings (require_review=False): достаточно findings.
    if findings_path is None:
        return result
    if require_review and review_path is None:
        return result

    origin = "primary" if (primary_findings and (primary_review or not require_review)) else "backup_pre_enrichment"
    result.update(
        origin=origin,
        findings_path=findings_path,
        review_path=review_path,
        backup_dir=best_backup if origin == "backup_pre_enrichment" else None,
    )
    return result


def _version_completed(project_dir: Path, project_id: str, version_id: str) -> bool:
    """Считать версию «проверенной», если у неё есть И findings, И expert_review
    (в основном `_output/` или в `_pre_enrichment_*` backup'е).

    Версия без expert_review не считается «проверенной» — мы не знаем, какие
    findings экспертом приняты, а без accepted-сигнала миграция бессмысленна.
    """
    sources = _resolve_version_sources(
        project_dir, project_id, version_id, require_review=True,
    )
    return sources["findings_path"] is not None and sources["review_path"] is not None


def get_previous_checked_version(
    project_id: str, current_version_id: str,
) -> Optional[str]:
    """Найти ближайшую более раннюю проверенную версию (по version_no).

    Returns:
        version_id предыдущей проверенной версии или None.
    """
    project_dir = resolve_project_dir(project_id)
    manifest = version_service.read_project_versions(project_dir, project_id)

    try:
        cur = next(
            v for v in manifest["versions"] if v["version_id"] == current_version_id
        )
    except StopIteration:
        raise version_service.VersionNotFoundError(
            f"Версия '{current_version_id}' не найдена"
        )
    cur_no = int(cur.get("version_no") or 0)

    earlier = [
        v for v in manifest["versions"]
        if int(v.get("version_no") or 0) < cur_no
    ]
    earlier.sort(key=lambda v: int(v.get("version_no") or 0), reverse=True)
    for v in earlier:
        if _version_completed(project_dir, project_id, v["version_id"]):
            return v["version_id"]
    return None


# ─── Чтение accepted findings из старой версии ─────────────────────────


def _is_accepted_decision(decision_value: Any, customer_confirmed: bool = False) -> bool:
    """Нормализованная проверка решения эксперта."""
    if customer_confirmed:
        return True
    if decision_value is None:
        return False
    s = str(decision_value).strip().lower()
    if not s:
        return False
    if s in REJECTED_DECISIONS:
        return False
    return s in ACCEPTED_DECISIONS


def _load_findings_from_version(
    project_dir: Path, project_id: str, version_id: str,
    *, require_review: bool = False,
) -> list[dict]:
    """Все findings указанной версии (без фильтра).

    Использует `_resolve_version_sources` — поддерживает read-only fallback на
    `_pre_enrichment_*` бэкап, если основной `_output/` пуст.

    По умолчанию `require_review=False`: для current-version нам нужны только
    findings, expert_review.json не обязателен. Для previous-version recheck
    источник проверяется отдельно через `_load_expert_review`.
    """
    sources = _resolve_version_sources(
        project_dir, project_id, version_id, require_review=require_review,
    )
    findings_path = sources["findings_path"]
    if findings_path is None:
        return []
    data = _load_json(findings_path)
    if not data:
        return []
    items = data.get("findings") or data.get("items") or []
    return [it for it in items if isinstance(it, dict)]


def _load_expert_review(project_dir: Path, project_id: str, version_id: str) -> dict[str, dict]:
    """Карта `finding_id → decision dict` из expert_review.json.

    Использует `_resolve_version_sources` — поддерживает read-only fallback на
    `_pre_enrichment_*` бэкап, если основной `_output/expert_review.json` пуст.
    """
    sources = _resolve_version_sources(project_dir, project_id, version_id)
    review_path = sources["review_path"]
    if review_path is None:
        return {}
    data = _load_json(review_path)
    if not data:
        return {}
    decisions = data.get("decisions", [])
    if not isinstance(decisions, list):
        return {}
    out: dict[str, dict] = {}
    for d in decisions:
        if not isinstance(d, dict):
            continue
        # Принимаем только findings (не optimization).
        item_type = (d.get("item_type") or "finding").lower()
        if item_type != "finding":
            continue
        fid = d.get("item_id") or d.get("id")
        if not fid:
            continue
        out[str(fid)] = d
    return out


def describe_version_source(
    project_id: str, version_id: str, *, require_review: bool = True,
) -> dict[str, Any]:
    """Диагностика: откуда были взяты данные версии (primary vs backup).

    Используется в `run_migrated_findings_check` для записи в отчёт. Для
    previous-version источника require_review=True (нужен expert_review для
    миграции). Для current-version можно передать False.
    """
    try:
        project_dir = resolve_project_dir(project_id)
    except FileNotFoundError:
        return {"origin": "missing", "findings_path": None, "review_path": None, "backup_dir": None}
    sources = _resolve_version_sources(
        project_dir, project_id, version_id, require_review=require_review,
    )
    return {
        "origin": sources["origin"],
        "findings_path": str(sources["findings_path"]) if sources["findings_path"] else None,
        "review_path": str(sources["review_path"]) if sources["review_path"] else None,
        "backup_dir": str(sources["backup_dir"]) if sources["backup_dir"] else None,
    }


def load_expert_accepted_findings(
    project_id: str, source_version_id: str,
) -> list[dict]:
    """Findings из старой версии, помеченные экспертом как accepted.

    Источник: `expert_review.json` (см. `expert_review.py` model: decisions[
    {item_id, item_type, decision}]). Findings без явного «accepted» не
    включаются — это критическое требование ТЗ.
    """
    project_dir = resolve_project_dir(project_id)
    review = _load_expert_review(project_dir, project_id, source_version_id)
    if not review:
        return []
    accepted_ids = {
        fid for fid, dec in review.items()
        if _is_accepted_decision(dec.get("decision"))
    }
    if not accepted_ids:
        return []
    findings = _load_findings_from_version(project_dir, project_id, source_version_id)
    return [f for f in findings if str(f.get("id", "")) in accepted_ids]


def load_id_mismatch_diagnostics(
    project_id: str, source_version_id: str,
) -> dict:
    """Диагностика: какие accepted id присутствуют в expert_review, но
    отсутствуют в findings. Это сигнал, что fallback нашёл не тот срез
    данных (например, review относится к norm-verified версии, а fallback
    подсунул pre_norm файл).
    """
    project_dir = resolve_project_dir(project_id)
    review = _load_expert_review(project_dir, project_id, source_version_id)
    if not review:
        return {
            "expert_accepted_count": 0,
            "matched_in_findings_count": 0,
            "missing_ids": [],
            "mismatch_detected": False,
        }
    accepted_ids = {
        fid for fid, dec in review.items()
        if _is_accepted_decision(dec.get("decision"))
    }
    findings = _load_findings_from_version(project_dir, project_id, source_version_id)
    finding_ids = {str(f.get("id", "")) for f in findings}
    missing = sorted(accepted_ids - finding_ids)
    return {
        "expert_accepted_count": len(accepted_ids),
        "matched_in_findings_count": len(accepted_ids & finding_ids),
        "missing_ids": missing,
        "mismatch_detected": bool(missing) and bool(accepted_ids),
    }


# ─── Кандидаты для recheck ────────────────────────────────────────────


def _extract_norm_refs(finding: dict) -> list[str]:
    """Извлечь нормативные ссылки из finding-а (минимум — `norm`)."""
    refs: list[str] = []
    n = finding.get("norm")
    if isinstance(n, str) and n.strip():
        refs.append(n.strip())
    if isinstance(n, list):
        refs.extend(str(x).strip() for x in n if str(x).strip())
    # `norm_refs` / `references` — на случай иной схемы.
    for key in ("norm_refs", "references"):
        v = finding.get(key)
        if isinstance(v, list):
            refs.extend(str(x).strip() for x in v if str(x).strip())
    return refs


def build_migration_candidates(
    project_id: str, current_version_id: str,
) -> list[dict]:
    """Список candidates из expert-accepted findings предыдущей проверенной версии."""
    prev = get_previous_checked_version(project_id, current_version_id)
    if not prev:
        return []
    accepted = load_expert_accepted_findings(project_id, prev)
    candidates: list[dict] = []
    for f in accepted:
        candidates.append({
            "origin_version_id": prev,
            "origin_finding_id": str(f.get("id", "")),
            "origin_title": f.get("problem") or f.get("title") or "",
            "origin_description": f.get("description", ""),
            "origin_severity": f.get("severity", ""),
            "origin_category": f.get("category", ""),
            "origin_norm_refs": _extract_norm_refs(f),
            "origin_evidence": f.get("evidence", []) or [],
            "origin_sheet": f.get("sheet", ""),
            "origin_page": f.get("page"),
            "origin_expert_status": "accepted",
            "current_version_id": current_version_id,
        })
    return candidates


# ─── Recheck (deterministic) ──────────────────────────────────────────


# Нормы, общие для всего раздела. Они ничего не говорят о сути замечания,
# поэтому совпадение по ним даёт минимальный вес (см. _norm_overlap_signal).
GENERIC_DISCIPLINE_NORMS = {
    # Общие требования к рабочей документации.
    "ГОСТ 21.501-2018",
    "ГОСТ 21.501-2011",
    "ГОСТ 21.110-2013",
    "ГОСТ Р 21.101-2020",
    "ГОСТ 21.502-2016",
    "ГОСТ 2.312-72",
    # Базовые СП на железобетон/сталь — встречаются в большинстве КЖ.
    "СП 63.13330.2018",
    "СП 16.13330.2017",
}


_NORM_HEAD_RE = re.compile(
    r"(?:СП|ГОСТ(?:\s+Р)?|ПУЭ|СНиП|СанПиН|ФЗ|ТР\s*ТС|МДС)\s*[\d.\-]+",
    re.IGNORECASE,
)
# Пункт нормы — строго после явного маркера "п." / "пп." / "раздел" /
# "разд.". Без маркера regex срабатывал бы и на сам номер СП.
_NORM_CLAUSE_RE = re.compile(
    r"(?:п\.?\s*п?\.?|разд(?:ел)?\.?)\s*([0-9]+(?:[.\-][0-9]+)+)",
    re.IGNORECASE,
)


def _normalize_norm_key(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().upper())


def _extract_norm_keys(refs: list[str]) -> set[str]:
    """Извлечь набор «нормализованных шапок» из ссылок (например {'СП 63.13330.2018', 'ГОСТ 21.501-2018'})."""
    out: set[str] = set()
    for r in refs:
        for m in _NORM_HEAD_RE.finditer(r):
            out.add(_normalize_norm_key(m.group(0)))
    return out


# После маркера "п." допускается список через запятую, например
# "п. 10.3.1, 10.3.2, 10.3.5".
_NORM_CLAUSE_TAIL_RE = re.compile(r"(?:,|\s+и\s+)\s*([0-9]+(?:[.\-][0-9]+)+)")


def _extract_norm_clauses(refs: list[str]) -> set[str]:
    """Извлечь набор номеров пунктов вместе с шапкой нормы.

    `СП 63.13330.2018, п. 10.3` → `{'СП 63.13330.2018|10.3'}`.
    `СП Y, п. 10.3.1, 10.3.2` → `{'СП Y|10.3.1', 'СП Y|10.3.2'}`.
    Без шапки клауза почти бесполезна (тот же '5.3' встречается в десятках норм).
    """
    out: set[str] = set()
    for r in refs:
        # Разбиваем по ';' — в одной строке часто перечисляются несколько норм
        # вида "СП X, п. 1; ГОСТ Y, п. 2".
        for chunk in re.split(r";", r):
            heads = list(_NORM_HEAD_RE.finditer(chunk))
            if not heads:
                continue
            head_key = _normalize_norm_key(heads[0].group(0))
            chunk_wo_heads = _NORM_HEAD_RE.sub("", chunk)
            # Найти все маркеры "п."/"раздел"/"пп."; от каждого собрать клаузы,
            # включая «хвост» через запятую.
            for m in _NORM_CLAUSE_RE.finditer(chunk_wo_heads):
                main_clause = m.group(1)
                out.add(f"{head_key}|{main_clause}")
                # После основного номера могут идти ещё через запятую/«и».
                tail_start = m.end()
                tail = chunk_wo_heads[tail_start:tail_start + 80]
                # Разрешаем продолжение списка только до первого «не-клауза»
                # символа (буква, точка с запятой, скобка и т.п.).
                # Берём по очереди, пока находим ", N.M(.K)" сразу за концом.
                pos = 0
                while pos < len(tail):
                    nm = _NORM_CLAUSE_TAIL_RE.match(tail, pos)
                    if not nm:
                        break
                    out.add(f"{head_key}|{nm.group(1)}")
                    pos = nm.end()
    return out


def _norm_overlap_signal(a_refs: list[str], b_refs: list[str]) -> tuple[float, bool, bool]:
    """Сигнал перекрытия нормативных ссылок.

    Returns:
        (score, head_match, clause_match)

        - clause_match=True (совпал пункт нормы) → score = 0.45.
        - head_match по «специфичной» норме → score = 0.30.
        - head_match только по generic-норме (ГОСТ 21.501 и т.п.) → score = 0.10.
        - нет совпадений → score = 0.0.

    Мотивация: общая ссылка на ГОСТ 21.501 не значит ничего по сути; совпадение
    пункта — сильный сигнал; совпадение специфичной нормы — средний.
    """
    if not a_refs or not b_refs:
        return 0.0, False, False
    a_heads = _extract_norm_keys(a_refs)
    b_heads = _extract_norm_keys(b_refs)
    a_clauses = _extract_norm_clauses(a_refs)
    b_clauses = _extract_norm_clauses(b_refs)

    head_inter = a_heads & b_heads
    clause_inter = a_clauses & b_clauses

    if clause_inter:
        return 0.45, True, True

    if head_inter:
        generic_set = {_normalize_norm_key(s) for s in GENERIC_DISCIPLINE_NORMS}
        specific = head_inter - generic_set
        if specific:
            return 0.30, True, False
        return 0.10, True, False

    return 0.0, False, False


def _norm_refs_overlap(a: list[str], b: list[str]) -> bool:
    """Backwards-compatible boolean (head overlap only, без учёта generic).

    Используется в API/тестах. Возвращает True, если есть пересечение хотя бы
    по «шапке» норм — без скоринга.
    """
    if not a or not b:
        return False
    return bool(_extract_norm_keys(a) & _extract_norm_keys(b))


def _pages_overlap(a: Any, b: Any) -> bool:
    """Пересекаются ли страницы (int или list[int])."""
    def to_set(v: Any) -> set[int]:
        if v is None:
            return set()
        if isinstance(v, list):
            return {int(x) for x in v if isinstance(x, (int, float))}
        if isinstance(v, (int, float)):
            return {int(v)}
        return set()

    return bool(to_set(a) & to_set(b))


# ─── Извлечение «смысловых» признаков замечания ────────────────────────

# Марки конструкций (Пм-25.2, ЗД-1, Кр-1, Пл-25, …) — типичный сигнал «об одном
# и том же объекте».
_MARK_RE = re.compile(
    r"\b(?:ЗД|Кр|Пм|Пл|Пд|ЭП|АП|ОП|Бл|БК|Б|С|К|Ст|КМ|КЖ)"
    r"[\-\.]?\d+(?:[.\-]\d+)*[\w-]*",
    re.IGNORECASE,
)
# Позиции деталей армирования (10-Г-1, 25-Г-57, 12-Х-1, 16-П-3, …).
_REBAR_POS_RE = re.compile(r"\b\d+-[А-ЯA-Z]-\d+(?:[.\-]\d+)*", re.IGNORECASE)
# Отметки уровней (+85,180; -1.800; +84,180).
_LEVEL_RE = re.compile(r"[+\-]?\d{1,3}[.,]\d{2,3}")
# Числовые значения с единицами измерения.
_NUM_WITH_UNIT_RE = re.compile(
    r"\d+(?:[.,]\d+)?\s*(?:мм|см|м[²³]?|кг(?:/м[³²]?)?|шт|%|°C?|МПа|кН/м[²³]?)",
    re.IGNORECASE,
)


def _extract_object_features(text: str) -> dict[str, set[str]]:
    """Извлечь «объектные» признаки из текста замечания.

    Возвращает наборы марок, позиций арматуры, отметок и количественных значений.
    Эти признаки сохраняются как есть, без шумоподавления — именно они
    отличают одно нарушение от другого.
    """
    if not text:
        return {"marks": set(), "rebar": set(), "levels": set(), "units": set()}
    marks = {m.group(0).upper() for m in _MARK_RE.finditer(text)}
    rebars = {m.group(0).upper() for m in _REBAR_POS_RE.finditer(text)}
    levels = {m.group(0).replace(",", ".") for m in _LEVEL_RE.finditer(text)}
    units = {m.group(0).replace(",", ".").lower() for m in _NUM_WITH_UNIT_RE.finditer(text)}
    return {"marks": marks, "rebar": rebars, "levels": levels, "units": units}


def _candidate_text_pool(f: dict) -> str:
    """Объединить problem + description finding'а для извлечения признаков."""
    parts = [
        str(f.get("problem") or ""),
        str(f.get("title") or ""),
        str(f.get("description") or ""),
    ]
    return " ".join(p for p in parts if p)


# Список «шумовых» слов для лексического сходства — после извлечения объектных
# признаков их можно безопасно отбросить.
_STOPWORDS = {
    "в", "на", "и", "по", "для", "из", "с", "к", "от", "не", "до", "при",
    "у", "о", "об", "за", "над", "под", "что", "это", "как", "так", "же",
    "только", "также", "или", "ни", "то", "а", "но", "лист", "листы", "стр",
    "страница", "page", "блок", "блоки", "block", "указан", "указано", "указана",
    "указаны", "ссылк", "ссылка", "ссылки", "норма", "нормы", "пункт", "пункта",
}


def _tokenize_problem(text: str) -> set[str]:
    """Слова для лексического сходства (после удаления стоп-слов)."""
    if not text:
        return set()
    s = text.lower()
    # Слова длиной >= 3 из букв или цифр (включая дефис и точку).
    tokens = re.findall(r"[a-zа-я0-9][a-zа-я0-9.\-]{2,}", s, re.IGNORECASE)
    return {t for t in tokens if t.lower() not in _STOPWORDS}


def _title_similarity(a: str, b: str) -> float:
    """Сходство problem'ов через Jaccard по токенам без стоп-слов."""
    sa = _tokenize_problem(a)
    sb = _tokenize_problem(b)
    if not sa or not sb:
        return 0.0
    inter = len(sa & sb)
    union = len(sa | sb)
    return inter / union if union else 0.0


def _features_match(
    a_text: str, b_text: str,
) -> tuple[float, dict[str, list[str]], dict[str, list[str]]]:
    """Сравнить «объектные» признаки между двумя текстами.

    Returns:
        (object_score, matched, different)
        - object_score: -0.3..+0.4. Положительный за совпадение марок/позиций;
          отрицательный, если у обоих признаков много, но они не пересекаются
          (это сильный негативный сигнал — разные объекты).
        - matched: словарь признаков с пересечением.
        - different: словарь признаков, явно различающихся.
    """
    fa = _extract_object_features(a_text)
    fb = _extract_object_features(b_text)

    matched: dict[str, list[str]] = {}
    different: dict[str, list[str]] = {}
    score = 0.0

    # Совпадение конкретной позиции арматуры / марки конструкции — очень сильный
    # сигнал «об одном и том же объекте». Текстовое сходство при разных
    # формулировках падает до 0.1-0.3, и без бустинга такие пары теряются.
    weights = {"marks": 0.25, "rebar": 0.30, "levels": 0.10, "units": 0.05}
    # Штраф за наличие признаков с обеих сторон без пересечения.
    penalty_w = {"marks": 0.20, "rebar": 0.15, "levels": 0.05}

    for key, w in weights.items():
        a, b = fa[key], fb[key]
        if a and b:
            inter = a & b
            if inter:
                matched[key] = sorted(inter)
                score += w
            else:
                different[key] = sorted(a | b)
                score -= penalty_w.get(key, 0.0)
        elif a or b:
            # Признак только с одной стороны — слабый штраф (мог не распознаться).
            if key == "marks":
                different[key] = sorted(a | b)
                score -= 0.05
    return score, matched, different


_CATEGORY_FAMILIES = {
    # Группы категорий — внутри одной группы нет штрафа, между группами штраф.
    "concrete": {"concrete_class", "concrete_protection", "frost_resistance"},
    "rebar": {"rebar_class", "rebar_geometry", "cover_thickness", "lap_length", "anchorage"},
    "openings": {"opening_reinforcement", "slab_detail", "stairs_detail"},
    "documentation": {"documentation", "marking", "normative_refs"},
    "embedded": {"embedded_parts", "corrosion_protection", "welding"},
    "coordination": {
        "km_kj_coordination", "ar_kj_coordination", "mep_coordination",
        "foundation_detail", "column_detail",
    },
    "spec": {"spec_mismatch", "dimension_mismatch"},
    "fire": {"fire_rating", "progressive_collapse"},
    "construction": {"construction_sequence"},
}


def _category_family(cat: str) -> Optional[str]:
    cat_lc = (cat or "").lower()
    for fam, cats in _CATEGORY_FAMILIES.items():
        if cat_lc in cats:
            return fam
    return None


def _category_signal(a: str, b: str) -> float:
    """Категория finding'а — слабый сигнал.

    + 0.10 если категории совпадают;
    + 0.00 если разные категории внутри одной family;
    - 0.10 если категории явно из разных family.
    """
    a_lc, b_lc = (a or "").lower(), (b or "").lower()
    if not a_lc or not b_lc:
        return 0.0
    if a_lc == b_lc:
        return 0.10
    fa, fb = _category_family(a_lc), _category_family(b_lc)
    if fa and fb and fa != fb:
        return -0.10
    return 0.0


def _score_pair(
    candidate: dict, current: dict,
) -> dict[str, Any]:
    """Оценить одну пару «v1 finding (candidate) ↔ v2 finding (current)».

    Возвращает dict со всеми компонентами скоринга — для прозрачности отчёта.
    """
    cand_refs = candidate.get("origin_norm_refs", [])
    cand_text = (candidate.get("origin_title") or "") + " " + (
        candidate.get("origin_description") or "")
    cand_pages = candidate.get("origin_page")
    cand_category = candidate.get("origin_category") or ""
    cand_evidence_blocks = {
        str(ev.get("block_id")) for ev in (candidate.get("origin_evidence") or [])
        if isinstance(ev, dict) and ev.get("block_id")
    }

    cur_refs = _extract_norm_refs(current)
    cur_text = _candidate_text_pool(current)
    cur_category = current.get("category") or ""
    cur_evidence_blocks = {
        str(ev.get("block_id")) for ev in (current.get("evidence") or [])
        if isinstance(ev, dict) and ev.get("block_id")
    } | {str(b) for b in (current.get("related_block_ids") or [])}

    norm_score, head_match, clause_match = _norm_overlap_signal(cand_refs, cur_refs)
    title_sim = _title_similarity(cand_text, cur_text)
    title_score = 0.50 * title_sim
    obj_score, matched_features, diff_features = _features_match(cand_text, cur_text)
    page_score = 0.10 if _pages_overlap(cand_pages, current.get("page")) else 0.0
    cat_score = _category_signal(cand_category, cur_category)
    evidence_score = 0.30 if (cand_evidence_blocks & cur_evidence_blocks) else 0.0

    # «Уникальные совпадения объектов» — бустер.
    # Конкретная позиция арматуры или марка конструкции — почти уникальный
    # идентификатор. Если они совпали, и при этом совпадает категория или
    # evidence-блок, это очень сильный сигнал одинакового нарушения.
    #
    # ВАЖНО: если категории явно из разных family (cat_score < 0), бустер
    # не применяем — общий объект может фигурировать в РАЗНЫХ нарушениях
    # на одной странице (например, плита Пм-25.2 имеет проблемы с бетоном
    # И с проёмами одновременно).
    has_unique_object = bool(matched_features.get("rebar") or matched_features.get("marks"))
    different_family = cat_score < 0
    positive_context = (cat_score > 0 or evidence_score > 0) and not different_family
    unique_match_bonus = 0.15 if (has_unique_object and positive_context) else 0.0

    # Штраф «общий объект, но разное описание».
    # Один и тот же элемент (например, плита Пм-25.2) может иметь несколько
    # независимых проблем (бетон / проёмы / толщина). Если объект совпал,
    # но title_similarity очень низкое (< 0.10) И нет общих evidence-блоков —
    # это два разных нарушения вокруг одного объекта, не дубль.
    semantic_divergence_penalty = 0.0
    if has_unique_object and title_sim < 0.10 and evidence_score == 0.0:
        semantic_divergence_penalty = -0.20

    raw_score = (
        norm_score + title_score + obj_score
        + page_score + cat_score + evidence_score
        + unique_match_bonus + semantic_divergence_penalty
    )
    confidence = _to_confidence(raw_score)

    return {
        # `score` (= raw_score) — открытая сумма всех слагаемых, может выходить
        # за пределы [0, 1] (например 1.29 при clause-match + bonus). Используется
        # для пороговых сравнений с BORDERLINE_LOW/HIGH.
        "score": raw_score,
        "raw_score": raw_score,
        # `confidence` — нормализованный 0..1, выводится в UI/отчёте как
        # «уверенность совпадения». Внутри логика по-прежнему сравнивает raw_score.
        "confidence": confidence,
        "norm_score": norm_score,
        "title_score": title_score,
        "object_score": obj_score,
        "page_score": page_score,
        "category_score": cat_score,
        "evidence_score": evidence_score,
        "unique_match_bonus": unique_match_bonus,
        "semantic_divergence_penalty": semantic_divergence_penalty,
        "title_similarity": title_sim,
        "head_match": head_match,
        "clause_match": clause_match,
        "matched_features": matched_features,
        "different_features": diff_features,
        "evidence_block_overlap": sorted(cand_evidence_blocks & cur_evidence_blocks),
    }


def _find_duplicate(
    candidate: dict, current_findings: list[dict],
) -> Optional[dict]:
    """Самый сильный дубль candidate-а среди current findings (или None)."""
    diag = find_duplicate_candidates(candidate, current_findings)
    top = diag[0] if diag else None
    if top is None:
        return None
    if top["score"] >= DUPLICATE_SCORE_THRESHOLD:
        return top["finding"]
    return None


def find_duplicate_candidates(
    candidate: dict, current_findings: list[dict],
) -> list[dict]:
    """Полная диагностика: топ-кандидаты v2 для одного v1 candidate.

    Возвращает список dict'ов вида `{"finding": <v2>, "score": float, ...}`,
    отсортированный по score по убыванию. Скоринг открытый — нужен для UI
    и для решения LLM-recheck'а.
    """
    scored: list[dict] = []
    for f in current_findings:
        s = _score_pair(candidate, f)
        s["finding"] = f
        s["finding_id"] = f.get("id")
        scored.append(s)
    scored.sort(key=lambda x: -x["score"])
    return scored


def _evidence_blocks_present_in_current(
    candidate: dict, current_findings: list[dict],
) -> bool:
    """Лёгкий source-check: есть ли упоминание origin-блоков среди evidence
    текущих findings (или их `related_block_ids`).
    """
    origin_blocks: set[str] = set()
    for ev in candidate.get("origin_evidence", []) or []:
        bid = (ev or {}).get("block_id") if isinstance(ev, dict) else None
        if bid:
            origin_blocks.add(str(bid))
    if not origin_blocks:
        return False
    for f in current_findings:
        for ev in f.get("evidence", []) or []:
            bid = (ev or {}).get("block_id") if isinstance(ev, dict) else None
            if bid and str(bid) in origin_blocks:
                return True
        for bid in f.get("related_block_ids", []) or []:
            if str(bid) in origin_blocks:
                return True
    return False


def _is_critical(candidate: dict) -> bool:
    sev = (candidate.get("origin_severity") or "").strip().lower()
    return sev in CRITICAL_SEVERITIES


def _build_diagnostic(top: dict) -> dict:
    """Вернуть «причину» совпадения для записи в отчёт."""
    return {
        "raw_score": round(top["raw_score"], 3),
        "confidence": round(top["confidence"], 3),
        "norm_score": round(top["norm_score"], 3),
        "title_similarity": round(top["title_similarity"], 3),
        "object_score": round(top["object_score"], 3),
        "page_score": round(top["page_score"], 3),
        "category_score": round(top["category_score"], 3),
        "evidence_score": round(top["evidence_score"], 3),
        "unique_match_bonus": round(top.get("unique_match_bonus", 0.0), 3),
        "semantic_divergence_penalty": round(top.get("semantic_divergence_penalty", 0.0), 3),
        "clause_match": top["clause_match"],
        "head_match": top["head_match"],
        "matched_features": top["matched_features"],
        "different_features": top["different_features"],
        "evidence_block_overlap": top["evidence_block_overlap"],
    }


def recheck_migration_candidate(
    project_id: str,
    current_version_id: str,
    candidate: dict,
    current_findings: list[dict],
    *,
    llm_recheck_enabled: bool = False,
) -> dict:
    """Определить судьбу одного origin-замечания в текущей версии.

    Логика:
    1. score >= BORDERLINE_HIGH → confident duplicate (без LLM).
    2. BORDERLINE_LOW <= score < BORDERLINE_HIGH → borderline:
       - если включён LLM recheck → отдать на семантическую проверку;
       - иначе → needs_manual_review.
    3. score < BORDERLINE_LOW:
       - есть evidence-перекрытие → still_relevant;
       - critical-замечание → possibly_resolved;
       - non-critical → not_found_in_new_version (НЕ "устранено" — автомат
         не вправе ставить такой вердикт без подтверждения).
    """
    candidates = find_duplicate_candidates(candidate, current_findings)
    top = candidates[0] if candidates else None
    top_score = top["score"] if top else 0.0

    base = {
        "origin_version_id": candidate.get("origin_version_id", ""),
        "origin_finding_id": candidate.get("origin_finding_id", ""),
        "top_candidate_id": top.get("finding_id") if top else None,
        "top_candidate_score": round(top_score, 3),  # raw_score (legacy field name)
        "top_candidate_raw_score": round(top_score, 3),
        "top_candidate_confidence": round(_to_confidence(top_score), 3),
        "diagnostic": _build_diagnostic(top) if top else None,
    }

    if top is not None and top_score >= BORDERLINE_HIGH:
        base.update(
            migration_status="duplicate_of_new_finding",
            linked_finding_id=top["finding"].get("id"),
            reason=(
                "Высокая уверенность дубля: score={:.2f}, "
                "совпали пункты норм/объекты/описание"
            ).format(top_score),
            llm_verified=False,
        )
        return base

    if top is not None and top_score >= BORDERLINE_LOW:
        # Borderline. Если разрешён LLM — вернём флаг, чтобы внешний слой
        # дёрнул LLM. Сам вызов делается в run_migrated_findings_check, чтобы
        # держать сетевые I/O вне «чистой» функции скоринга.
        if llm_recheck_enabled:
            base["migration_status"] = "needs_llm_recheck"
            base["reason"] = (
                "Borderline score={:.2f}. Требуется семантическая проверка LLM."
            ).format(top_score)
            base["llm_pending"] = True
            return base
        base["migration_status"] = "needs_manual_review"
        base["linked_finding_id"] = top["finding"].get("id")
        base["reason"] = (
            "Есть похожий кандидат (score={:.2f}), но уверенности недостаточно. "
            "Требуется ручная проверка."
        ).format(top_score)
        base["llm_verified"] = False
        return base

    # Score < BORDERLINE_LOW: уверенно НЕ совпадает с топ-кандидатом.
    if _evidence_blocks_present_in_current(candidate, current_findings):
        base.update(
            migration_status="still_relevant",
            reason="Origin-блоки замечания присутствуют в evidence v_current",
            llm_verified=False,
        )
        return base

    if not current_findings:
        base.update(
            migration_status="not_verifiable",
            reason="В текущей версии нет findings — recheck невозможен",
            llm_verified=False,
        )
        return base

    if _is_critical(candidate):
        base.update(
            migration_status="possibly_resolved",
            reason=(
                "Критичное замечание из v1 не нашло смыслового дубля в v_current "
                "(top_score={:.2f}). Возможно устранено — требуется подтверждение."
            ).format(top_score),
            llm_verified=False,
        )
        return base

    base.update(
        migration_status="not_found_in_new_version",
        reason=(
            "Не найдено корректного смыслового совпадения в текущей версии "
            "(top_score={:.2f}). Это НЕ автоматически означает «устранено» — "
            "требуется подтверждение экспертом."
        ).format(top_score),
        llm_verified=False,
    )
    return base


# ─── LLM Semantic Recheck (опциональный) ────────────────────────────────


def _build_llm_prompt(candidate: dict, top_v2: dict) -> str:
    """Промпт для семантической проверки одной пары v1↔v2."""
    v1_block = json.dumps({
        "id": candidate.get("origin_finding_id"),
        "severity": candidate.get("origin_severity"),
        "category": candidate.get("origin_category"),
        "page": candidate.get("origin_page"),
        "sheet": candidate.get("origin_sheet"),
        "problem": candidate.get("origin_title"),
        "description": candidate.get("origin_description"),
        "norm": candidate.get("origin_norm_refs"),
    }, ensure_ascii=False, indent=2)
    v2_block = json.dumps({
        "id": top_v2.get("id"),
        "severity": top_v2.get("severity"),
        "category": top_v2.get("category"),
        "page": top_v2.get("page"),
        "sheet": top_v2.get("sheet"),
        "problem": top_v2.get("problem"),
        "description": top_v2.get("description"),
        "norm": top_v2.get("norm") or _extract_norm_refs(top_v2),
    }, ensure_ascii=False, indent=2)
    return (
        "Ты — эксперт по проектной документации. Проверь, описывают ли два замечания "
        "ОДНО И ТО ЖЕ нарушение по сути (даже если формулировки разные), либо это РАЗНЫЕ "
        "нарушения, случайно попавшие на одну страницу/норму.\n\n"
        f"ЗАМЕЧАНИЕ V1 (предыдущая версия, согласовано экспертом):\n{v1_block}\n\n"
        f"ЗАМЕЧАНИЕ V2 (новая версия, лучший кандидат на дубль):\n{v2_block}\n\n"
        "Ответ — строго JSON в одной строке, без markdown и комментариев:\n"
        "{\n"
        '  "same_issue": true | false,\n'
        '  "confidence": 0.0..1.0,\n'
        '  "reason": "1-2 предложения почему",\n'
        '  "matched_aspects": ["что совпало"],\n'
        '  "different_aspects": ["что различается"]\n'
        "}\n"
        "Если общая норма/страница есть, но объект/числа/тип проблемы разные — "
        "same_issue:false. Если одно и то же нарушение описано другими словами — same_issue:true."
    )


def _run_claude_cli_sync(prompt: str, timeout: Optional[int] = None) -> Optional[dict]:
    """Синхронный вызов `claude -p` для семантической проверки.

    Возвращает распарсенный JSON или None, если LLM недоступен/упал.

    Не выбрасывает исключений — fail-soft. На любой сбой возвращает None;
    вызывающий код должен интерпретировать это как «не удалось проверить».

    Claude CLI работает по подписке (см. memory `feedback_subscription_only.md`),
    поэтому `paid_api_guard` НЕ срабатывает: подписка — не платный внешний API.
    Защита от злоупотребления через env-флаги:
      * `MIGRATED_FINDINGS_LLM_RECHECK=1` (default 0) — kill-switch;
      * `MIGRATED_FINDINGS_LLM_TIMEOUT_SEC` (default 120, clamp [10, 300]);
      * `MIGRATED_FINDINGS_LLM_MAX_PAIRS` (default 10, clamp [0, 50]) — лимит
        вызовов в одном `run_migrated_findings_check`.
    """
    if not _llm_recheck_enabled():
        # Дополнительная защита: даже если кто-то вызвал _run_claude_cli_sync
        # напрямую, без флага мы НИЧЕГО не вызываем.
        logger.info("migrated-findings LLM recheck: disabled (env flag off)")
        return None

    timeout_sec = timeout if timeout is not None else _llm_recheck_timeout_sec()

    try:
        # Импорт здесь, чтобы избежать heavy import в обычном пути.
        from backend.app.core.config import get_claude_cli, get_claude_model
    except Exception:
        logger.warning("migrated-findings LLM recheck: cannot import claude config")
        return None

    try:
        cli = get_claude_cli()
    except Exception:
        cli = None
    if not cli:
        logger.warning("migrated-findings LLM recheck: claude CLI not configured")
        return None

    try:
        model = get_claude_model()
    except Exception:
        model = "claude-sonnet-4-6"

    cmd = [
        cli, "-p",
        "--model", model,
        "--output-format", "json",
    ]
    # Минимальный env, без CLAUDE_* — повторяем паттерн discussion_service.
    env = {k: v for k, v in os.environ.items() if not k.startswith("CLAUDE")}

    logger.info(
        "migrated-findings LLM recheck: claude -p start model=%s timeout=%ds",
        model, timeout_sec,
    )

    try:
        proc = subprocess.run(
            cmd,
            input=prompt,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            env=env,
            check=False,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as e:
        logger.warning("migrated-findings LLM recheck: subprocess failed: %s", e)
        return None

    if proc.returncode != 0 and not proc.stdout:
        logger.warning(
            "migrated-findings LLM recheck: exit=%d stderr=%s",
            proc.returncode, (proc.stderr or "")[:200],
        )
        return None

    # Парсим CLI JSON-обёртку (см. claude_runner.parse_cli_json_output).
    try:
        cli_data = json.loads(proc.stdout)
        result_text = cli_data.get("result") or ""
    except (json.JSONDecodeError, KeyError, TypeError):
        result_text = proc.stdout

    if not result_text:
        return None

    # Внутри result_text — ожидаемый JSON. Достаём первый {...}.
    match = re.search(r"\{.*\}", result_text, re.DOTALL)
    if not match:
        logger.warning("migrated-findings LLM recheck: no JSON found in CLI output")
        return None
    try:
        parsed = json.loads(match.group(0))
    except json.JSONDecodeError:
        logger.warning("migrated-findings LLM recheck: malformed JSON in CLI output")
        return None

    if not isinstance(parsed, dict) or "same_issue" not in parsed:
        logger.warning("migrated-findings LLM recheck: response missing same_issue")
        return None
    return parsed


def _apply_llm_recheck(result_dict: dict, candidate: dict, top_v2: Optional[dict]) -> dict:
    """Дёрнуть LLM и интерпретировать ответ. Возвращает обновлённый result_dict.

    Стратегия:
    - same_issue=true → duplicate_of_new_finding (с пометкой llm_verified=true).
    - same_issue=false:
        * если v1 — critical → possibly_resolved + false_positive_rejected_for=<id>;
        * иначе → not_found_in_new_version + false_positive_rejected_for=<id>
          (НЕ "устранено" — автомат не вправе ставить такой вердикт).
    - LLM недоступен/неконсистентен → needs_manual_review.
    """
    if top_v2 is None:
        result_dict.update(
            migration_status="needs_manual_review",
            reason="LLM recheck невозможен: нет кандидата v2",
            llm_verified=False,
        )
        return result_dict

    prompt = _build_llm_prompt(candidate, top_v2)
    llm = _run_claude_cli_sync(prompt)
    if llm is None:
        result_dict.update(
            migration_status="needs_manual_review",
            reason="LLM recheck недоступен или вернул некорректный ответ",
            llm_verified=False,
        )
        return result_dict

    same = bool(llm.get("same_issue"))
    conf = float(llm.get("confidence") or 0.0)
    llm_reason = str(llm.get("reason") or "")[:300]
    result_dict["llm_response"] = {
        "same_issue": same,
        "confidence": conf,
        "reason": llm_reason,
        "matched_aspects": llm.get("matched_aspects") or [],
        "different_aspects": llm.get("different_aspects") or [],
    }

    if same:
        result_dict.update(
            migration_status="duplicate_of_new_finding",
            linked_finding_id=top_v2.get("id"),
            reason=f"LLM confirmed duplicate (confidence={conf:.2f}): {llm_reason}",
            llm_verified=True,
        )
        return result_dict

    # LLM сказал — это разные замечания.
    result_dict["false_positive_rejected_for"] = top_v2.get("id")
    if _is_critical(candidate):
        result_dict.update(
            migration_status="possibly_resolved",
            reason=(
                f"LLM отклонил локальный матч с {top_v2.get('id')} "
                f"(confidence={conf:.2f}): {llm_reason}. "
                "Critical-замечание из v1 не имеет дубля в v_current — возможно устранено."
            ),
            llm_verified=True,
        )
    else:
        result_dict.update(
            migration_status="not_found_in_new_version",
            reason=(
                f"LLM отклонил локальный матч с {top_v2.get('id')} "
                f"(confidence={conf:.2f}): {llm_reason}. "
                "Корректного смыслового совпадения в v_current не найдено — "
                "требуется проверка экспертом."
            ),
            llm_verified=True,
        )
    return result_dict


# ─── Запись отчёта и обновление 03_findings ────────────────────────────


def _report_path(project_id: str, version_id: str) -> Path:
    output_dir = version_service.resolve_version_output_dir(project_id, version_id)
    return output_dir / MIGRATED_REPORT_FILENAME


def write_migrated_findings_report(
    project_id: str,
    current_version_id: str,
    source_version_id: Optional[str],
    items: list[dict],
    *,
    current_findings_missing: bool = False,
    source_data_origin: Optional[dict] = None,
    llm_recheck_used: bool = False,
    llm_calls_made: int = 0,
    llm_skipped_reasons: Optional[list[str]] = None,
    id_mismatch_diagnostics: Optional[dict] = None,
) -> dict:
    """Сохранить migrated_findings_report.json и вернуть его содержимое."""
    summary = {
        "still_relevant": 0,
        "not_found_in_new_version": 0,
        "possibly_resolved": 0,
        "duplicate_of_new_finding": 0,
        "needs_manual_review": 0,
        "false_positive_rejected": 0,  # подсчёт false positives через флаг
        "not_verifiable": 0,
        "source_missing": 0,
        "id_mismatch_in_source": 0,
    }
    for it in items:
        st = it.get("migration_status")
        if st in summary:
            summary[st] += 1
        if it.get("false_positive_rejected_for"):
            summary["false_positive_rejected"] += 1

    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "project_id": project_id,
        "current_version_id": current_version_id,
        "source_version_id": source_version_id,
        "source_data_origin": source_data_origin,
        "llm_recheck_used": llm_recheck_used,
        "llm_calls_made": llm_calls_made,
        "llm_skipped_reasons": llm_skipped_reasons or [],
        "id_mismatch_diagnostics": id_mismatch_diagnostics or {
            "expert_accepted_count": 0,
            "matched_in_findings_count": 0,
            "missing_ids": [],
            "mismatch_detected": False,
        },
        "checked_at": _now_iso(),
        "total_previous_accepted_findings": len(items),
        "items": items,
        **summary,
    }
    if current_findings_missing:
        report["status"] = "current_findings_missing"

    path = _report_path(project_id, current_version_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8",
    )
    return report


def read_migrated_findings_report(
    project_id: str, version_id: str,
) -> Optional[dict]:
    """Вернуть существующий отчёт, либо None."""
    try:
        return _load_json(_report_path(project_id, version_id))
    except (version_service.VersionNotFoundError, FileNotFoundError):
        return None


def _stable_migrated_id(origin_version_id: str, origin_finding_id: str) -> str:
    """ID для migrated finding: стабильный, узнаваемый, не конфликтует с обычными."""
    safe = re.sub(r"[^A-Za-z0-9-]", "_", origin_finding_id or "X")
    return f"MIG-{origin_version_id.upper()}-{safe}"


def append_migrated_findings_to_current_findings(
    project_id: str,
    current_version_id: str,
    migration_results: list[dict],
    *,
    candidates_by_origin: Optional[dict[str, dict]] = None,
) -> dict:
    """Применить результаты recheck'а к 03_findings.json текущей версии.

    Делает:
    - добавляет migrated finding для каждого `still_relevant` (idempotent —
      если такой migrated finding уже есть, не дублируется);
    - для `duplicate_of_new_finding` обогащает существующий current-finding
      полями `has_origin_from_previous_version`/`origin_version_id`/...
    """
    output_dir = version_service.resolve_version_output_dir(project_id, current_version_id)
    findings_path = output_dir / "03_findings.json"
    data = _load_json(findings_path)
    if data is None:
        return {"updated": False, "reason": "current_findings_missing"}

    items_key = "findings" if "findings" in data else (
        "items" if "items" in data else "findings"
    )
    items = data.get(items_key) or []
    if not isinstance(items, list):
        return {"updated": False, "reason": "invalid_findings_structure"}

    by_id: dict[str, dict] = {
        str(f.get("id", "")): f for f in items if isinstance(f, dict)
    }
    # Idempotency: множество уже существующих migrated origin_finding_id.
    existing_origins: set[tuple[str, str]] = set()
    for f in items:
        if not isinstance(f, dict):
            continue
        if f.get("is_migrated") or f.get("has_origin_from_previous_version"):
            ov = str(f.get("origin_version_id") or "")
            of = str(f.get("origin_finding_id") or "")
            if ov and of:
                existing_origins.add((ov, of))

    added = 0
    linked = 0
    candidates_by_origin = candidates_by_origin or {}

    for res in migration_results:
        origin_v = res.get("origin_version_id") or ""
        origin_f = res.get("origin_finding_id") or ""
        status = res.get("migration_status")

        if status == "duplicate_of_new_finding":
            linked_id = res.get("linked_finding_id")
            if not linked_id or str(linked_id) not in by_id:
                continue
            target = by_id[str(linked_id)]
            if target.get("has_origin_from_previous_version"):
                continue  # уже был связан раньше
            target["has_origin_from_previous_version"] = True
            target["origin_version_id"] = origin_v
            target["origin_finding_id"] = origin_f
            target["llm_verified_match"] = bool(res.get("llm_verified"))
            linked += 1
            continue

        # Кроме still_relevant в 03_findings добавляем также possibly_resolved
        # и needs_manual_review — они должны быть видны в UI и не теряться.
        if status not in {"still_relevant", "possibly_resolved", "needs_manual_review"}:
            continue

        if (origin_v, origin_f) in existing_origins:
            continue  # idempotent skip

        cand = candidates_by_origin.get(origin_f, {})
        mig_id = _stable_migrated_id(origin_v, origin_f)
        # Подстраховка: не конфликтует с существующим id.
        if mig_id in by_id:
            continue

        if status == "still_relevant":
            note = (
                f"Замечание было согласовано экспертом в {origin_v.upper()} "
                f"и осталось актуальным в {current_version_id.upper()}"
            )
            sev = cand.get("origin_severity") or "ПРОВЕРИТЬ ПО СМЕЖНЫМ"
            source_finding_status = "still_relevant"
        elif status == "possibly_resolved":
            note = (
                f"Критичное замечание из {origin_v.upper()} не нашло смыслового "
                f"совпадения в {current_version_id.upper()}. Требуется проверка "
                f"экспертом: устранено реально или потерялось."
            )
            sev = "ПРОВЕРИТЬ ПО СМЕЖНЫМ"
            source_finding_status = "possibly_resolved"
        else:  # needs_manual_review
            note = (
                f"Из {origin_v.upper()} есть похожее замечание, но автоматическая проверка "
                f"не уверена. Требуется ручная проверка."
            )
            sev = "ПРОВЕРИТЬ ПО СМЕЖНЫМ"
            source_finding_status = "needs_manual_review"

        migrated = {
            "id": mig_id,
            # --- Маркеры виртуальной природы (важно для UI/экспорта/статистики).
            "is_virtual": True,
            "is_migrated": True,
            "origin": "migrated_findings_control",
            "should_count_as_new_finding": False,
            # --- Источник в предыдущей версии.
            "source_version_id": origin_v,
            "source_finding_id": origin_f,
            "source_finding_status": source_finding_status,
            # --- Совместимость с прежним форматом (origin_*).
            "source_type": "migrated_from_previous_version",
            "migration_status": status,
            "origin_version_id": origin_v,
            "origin_finding_id": origin_f,
            "origin_expert_status": "accepted",
            "migrated_from_label": origin_v.upper(),
            # --- Контент.
            "severity": sev,
            "category": cand.get("origin_category", ""),
            "problem": cand.get("origin_title", ""),
            "description": cand.get("origin_description", ""),
            "norm": (cand.get("origin_norm_refs") or [""])[0],
            "sheet": cand.get("origin_sheet", ""),
            "page": cand.get("origin_page"),
            "evidence": cand.get("origin_evidence", []) or [],
            # --- Диагностика.
            "migration_note": note,
            "migration_reason": res.get("reason", ""),
            "top_candidate_id": res.get("top_candidate_id"),
            "top_candidate_score": res.get("top_candidate_score"),
            "top_candidate_confidence": res.get("top_candidate_confidence"),
            "llm_verified": bool(res.get("llm_verified")),
        }
        items.append(migrated)
        by_id[mig_id] = migrated
        existing_origins.add((origin_v, origin_f))
        added += 1

    data[items_key] = items
    # Обновим meta-счётчик, если он был
    meta = data.get("meta")
    if isinstance(meta, dict) and "total_findings" in meta:
        meta["total_findings"] = len(items)

    findings_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8",
    )
    return {"updated": True, "migrated_added": added, "linked_duplicates": linked}


# ─── Главная entrypoint-функция ────────────────────────────────────────


def run_migrated_findings_check(
    project_id: str, current_version_id: str,
) -> dict:
    """Полный сценарий: candidates → recheck → report → update 03_findings.

    Returns:
        dict с полями:
            status, source_version_id, summary, report_path,
            migrated_added (если 03_findings обновлён).
    """
    if not current_version_id or current_version_id == "v1":
        raise MigratedFindingsError(
            "Migrated findings check работает только для V2+. "
            "Для V1 (legacy) нет более ранней проверенной версии."
        )

    # Валидируем version_id (бросает VersionNotFoundError если нет).
    project_dir = resolve_project_dir(project_id)
    version_service.get_version_entry(project_dir, project_id, current_version_id)

    llm_enabled = _llm_recheck_enabled()
    llm_max_pairs = _llm_recheck_max_pairs() if llm_enabled else 0
    llm_used = False
    llm_calls_made = 0
    llm_skipped_reasons: list[str] = []  # для отчёта

    prev = get_previous_checked_version(project_id, current_version_id)
    if not prev:
        # Пустой отчёт — нет предыдущей проверенной версии.
        report = write_migrated_findings_report(
            project_id, current_version_id, None, [],
            llm_recheck_used=False,
        )
        return {
            "status": "ok",
            "source_version_id": None,
            "reason": "no_previous_checked_version",
            "report": report,
        }

    # Диагностика: откуда взяты данные v1.
    source_info = describe_version_source(project_id, prev)
    if source_info.get("origin") == "backup_pre_enrichment":
        logger.info(
            "migrated-findings %s/%s: v1 data taken from pre_enrichment backup %s",
            project_id, current_version_id, source_info.get("backup_dir"),
        )

    candidates = build_migration_candidates(project_id, current_version_id)
    # Диагностика id mismatch — даже если candidates пуст, надо различать
    # «эксперт никого не принял» vs «принятые id потерялись при OCR/нормализации».
    mismatch_diag = load_id_mismatch_diagnostics(project_id, prev)

    if not candidates:
        if mismatch_diag["mismatch_detected"]:
            reason = "id_mismatch_in_source"
            logger.warning(
                "migrated-findings %s/%s: source v1 has %d accepted decisions but "
                "%d of them have no corresponding finding (missing ids: %s)",
                project_id, current_version_id,
                mismatch_diag["expert_accepted_count"],
                len(mismatch_diag["missing_ids"]),
                mismatch_diag["missing_ids"][:5],
            )
        else:
            reason = "no_accepted_findings_in_source"
        report = write_migrated_findings_report(
            project_id, current_version_id, prev, [],
            source_data_origin=source_info, llm_recheck_used=False,
            llm_skipped_reasons=["env_flag_off"] if not _llm_recheck_enabled() else [],
            id_mismatch_diagnostics=mismatch_diag,
        )
        return {
            "status": "ok",
            "source_version_id": prev,
            "reason": reason,
            "id_mismatch_diagnostics": mismatch_diag,
            "report": report,
        }

    # Берём текущие findings (если их нет — recheck невозможен, но отчёт
    # должен быть записан со статусом current_findings_missing).
    current_findings = _load_findings_from_version(
        project_dir, project_id, current_version_id,
    )
    current_missing = not current_findings

    results: list[dict] = []
    candidates_by_origin: dict[str, dict] = {}
    for c in candidates:
        candidates_by_origin[c["origin_finding_id"]] = c
        res = recheck_migration_candidate(
            project_id, current_version_id, c, current_findings,
            llm_recheck_enabled=llm_enabled,
        )
        # LLM recheck — только для borderline и только если флаг включён.
        if res.get("migration_status") == "needs_llm_recheck":
            if llm_calls_made >= llm_max_pairs:
                # Лимит на запуск исчерпан → fallback в needs_manual_review.
                res["migration_status"] = "needs_manual_review"
                res["reason"] = (
                    f"Borderline кандидат, но лимит LLM recheck'ов "
                    f"({llm_max_pairs} пар на запуск) исчерпан."
                )
                res["llm_verified"] = False
                if "llm_max_pairs_exceeded" not in llm_skipped_reasons:
                    llm_skipped_reasons.append("llm_max_pairs_exceeded")
            else:
                top_cands = find_duplicate_candidates(c, current_findings)
                top = top_cands[0]["finding"] if top_cands else None
                res = _apply_llm_recheck(res, c, top)
                llm_used = True
                llm_calls_made += 1
                # Если apply вернул manual_review с reason 'недоступен' — пометить.
                if not res.get("llm_verified") and res.get("migration_status") == "needs_manual_review":
                    if "llm_unavailable" not in llm_skipped_reasons:
                        llm_skipped_reasons.append("llm_unavailable")
        # Прокидываем origin-метаданные, чтобы отчёт был самодостаточным.
        res.setdefault("origin_title", c["origin_title"])
        res.setdefault("origin_severity", c["origin_severity"])
        res.setdefault("origin_category", c["origin_category"])
        res.setdefault("origin_page", c["origin_page"])
        res.setdefault("origin_sheet", c["origin_sheet"])
        results.append(res)

    if llm_enabled and llm_calls_made == 0 and not llm_skipped_reasons:
        llm_skipped_reasons.append("no_borderline_candidates")
    if not llm_enabled:
        llm_skipped_reasons.append("env_flag_off")

    report = write_migrated_findings_report(
        project_id, current_version_id, prev, results,
        current_findings_missing=current_missing,
        source_data_origin=source_info,
        llm_recheck_used=llm_used,
        llm_calls_made=llm_calls_made,
        llm_skipped_reasons=llm_skipped_reasons,
        id_mismatch_diagnostics=mismatch_diag,
    )
    apply_result: dict[str, Any] = {"updated": False}
    if not current_missing:
        apply_result = append_migrated_findings_to_current_findings(
            project_id, current_version_id, results,
            candidates_by_origin=candidates_by_origin,
        )

    return {
        "status": "ok",
        "source_version_id": prev,
        "source_data_origin": source_info,
        "llm_recheck_used": llm_used,
        "report": report,
        "apply": apply_result,
    }
