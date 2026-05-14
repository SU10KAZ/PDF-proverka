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
- resolved_in_new_version   — нарушение устранено в новой документации.
- not_verifiable            — недостаточно данных для recheck.
- source_missing            — соответствующий раздел/документ отсутствует в V2.
- current_findings_missing  — у V2 ещё нет 03_findings.json; recheck отложен.
"""
from __future__ import annotations

import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from backend.app.services.common import version_service
from backend.app.services.common.project_service import resolve_project_dir
from backend.app.services.findings.findings_service import _normalize_problem_pattern


SCHEMA_VERSION = 1
MIGRATED_REPORT_FILENAME = "migrated_findings_report.json"

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


# ─── Поиск предыдущей проверенной версии ────────────────────────────────


def _version_completed(project_dir: Path, project_id: str, version_id: str) -> bool:
    """Считать версию «проверенной», если в её `_output/` есть 03_findings.json
    или 03a_norms_verified.json с непустым `findings`/`items`."""
    try:
        version_dir = version_service.get_version_dir(project_dir, project_id, version_id)
    except version_service.VersionNotFoundError:
        return False
    output_dir = version_dir / "_output"
    for fname in ("03a_norms_verified.json", "03_findings.json"):
        data = _load_json(output_dir / fname)
        if not data:
            continue
        items = data.get("findings") or data.get("items") or []
        # Версия может быть «проверена с нулём замечаний» — тогда `findings_merge`
        # завершён, и мы считаем её проверенной. Главное — что файл валидный JSON.
        return True
    return False


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
) -> list[dict]:
    """Все findings из 03_findings.json указанной версии (без фильтра)."""
    try:
        version_dir = version_service.get_version_dir(project_dir, project_id, version_id)
    except version_service.VersionNotFoundError:
        return []
    output_dir = version_dir / "_output"
    # Источник правды — 03a_norms_verified.json (с verified norms), если есть.
    for fname in ("03a_norms_verified.json", "03_findings.json"):
        data = _load_json(output_dir / fname)
        if not data:
            continue
        items = data.get("findings") or data.get("items") or []
        return [it for it in items if isinstance(it, dict)]
    return []


def _load_expert_review(project_dir: Path, project_id: str, version_id: str) -> dict[str, dict]:
    """Карта `finding_id → decision dict` из `_output/expert_review.json`."""
    try:
        version_dir = version_service.get_version_dir(project_dir, project_id, version_id)
    except version_service.VersionNotFoundError:
        return {}
    review_path = version_dir / "_output" / "expert_review.json"
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


def _norm_refs_overlap(a: list[str], b: list[str]) -> bool:
    """Есть ли пересечение нормативных ссылок (по нормализованному номеру СП/ГОСТ)."""
    if not a or not b:
        return False
    norm_re = re.compile(
        r"(?:СП|ГОСТ|ПУЭ|СНиП|СанПиН|ФЗ|ТР\s*ТС|МДС)\s*[\d.\-]+",
        re.IGNORECASE,
    )

    def keys(refs: list[str]) -> set[str]:
        out: set[str] = set()
        for r in refs:
            for m in norm_re.finditer(r):
                token = re.sub(r"\s+", " ", m.group(0).strip().upper())
                out.add(token)
        return out

    return bool(keys(a) & keys(b))


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


def _title_similarity(a: str, b: str) -> float:
    """Сходство по нормализованному паттерну проблемы (0..1)."""
    if not a or not b:
        return 0.0
    pa = _normalize_problem_pattern(a)
    pb = _normalize_problem_pattern(b)
    if not pa or not pb:
        return 0.0
    if pa == pb:
        return 1.0
    sa = set(pa.split())
    sb = set(pb.split())
    if not sa or not sb:
        return 0.0
    inter = len(sa & sb)
    return (2 * inter) / (len(sa) + len(sb))


def _find_duplicate(
    candidate: dict, current_findings: list[dict],
) -> Optional[dict]:
    """Самый сильный дубль candidate-а среди current findings (или None)."""
    cand_refs = candidate.get("origin_norm_refs", [])
    cand_title = candidate.get("origin_title", "") or candidate.get("origin_description", "")
    cand_pages = candidate.get("origin_page")
    cand_category = (candidate.get("origin_category") or "").lower()

    best: Optional[dict] = None
    best_score: float = 0.0

    for f in current_findings:
        score = 0.0
        # 1) пересечение нормативных ссылок — сильный сигнал
        if _norm_refs_overlap(cand_refs, _extract_norm_refs(f)):
            score += 0.6
        # 2) сходство названия/описания
        title_b = f.get("problem") or f.get("title") or f.get("description") or ""
        sim = _title_similarity(cand_title, title_b)
        score += 0.4 * sim
        # 3) перекрытие страниц
        if _pages_overlap(cand_pages, f.get("page")):
            score += 0.15
        # 4) совпадение категории
        cat_b = (f.get("category") or "").lower()
        if cand_category and cat_b and cand_category == cat_b:
            score += 0.1

        if score > best_score:
            best_score = score
            best = f

    # Порог: либо norm-overlap + хотя бы что-то ещё, либо очень похожий title.
    if best is not None and best_score >= 0.7:
        return best
    return None


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


def recheck_migration_candidate(
    project_id: str,
    current_version_id: str,
    candidate: dict,
    current_findings: list[dict],
) -> dict:
    """Определить судьбу одного origin-замечания в текущей версии.

    LLM-recheck опционален и по умолчанию выключен; включается переменной
    окружения `MIGRATED_FINDINGS_LLM_RECHECK=1` — на данном проходе она
    зарезервирована и фактически не вызывает LLM (только меняет reason).
    """
    # 1) Дубль с уже найденным V2-finding-ом → duplicate_of_new_finding
    dup = _find_duplicate(candidate, current_findings)
    if dup is not None:
        return {
            "origin_version_id": candidate["origin_version_id"],
            "origin_finding_id": candidate["origin_finding_id"],
            "migration_status": "duplicate_of_new_finding",
            "linked_finding_id": dup.get("id"),
            "reason": "Сильное совпадение по нормам/заголовку с текущим finding",
        }

    # 2) Если в текущих findings есть evidence из origin-блоков, считаем
    # что нарушение присутствует — still_relevant.
    if _evidence_blocks_present_in_current(candidate, current_findings):
        return {
            "origin_version_id": candidate["origin_version_id"],
            "origin_finding_id": candidate["origin_finding_id"],
            "migration_status": "still_relevant",
            "reason": "Origin-блоки замечания присутствуют в evidence V_current",
        }

    # 3) Без evidence-блоков и без дубля: если у candidate есть norm_refs и
    # current_findings вообще не пусто, считаем resolved (если бы нарушение
    # осталось, dedup поднял бы его выше). Это безопасное допущение — alt
    # вариант помечается как not_verifiable.
    if candidate.get("origin_norm_refs") and current_findings:
        return {
            "origin_version_id": candidate["origin_version_id"],
            "origin_finding_id": candidate["origin_finding_id"],
            "migration_status": "resolved_in_new_version",
            "reason": "Не найдено совпадения с текущими findings — вероятно, устранено",
        }

    # 4) Иначе — недостаточно данных, ручная проверка.
    if os.environ.get("MIGRATED_FINDINGS_LLM_RECHECK") == "1":
        # Hook для будущего LLM-recheck. На этом этапе мы НЕ вызываем LLM,
        # только маркируем reason, чтобы оператор видел: enable detected.
        return {
            "origin_version_id": candidate["origin_version_id"],
            "origin_finding_id": candidate["origin_finding_id"],
            "migration_status": "not_verifiable",
            "reason": "Recheck отложен: LLM-recheck включён, но не реализован",
        }
    return {
        "origin_version_id": candidate["origin_version_id"],
        "origin_finding_id": candidate["origin_finding_id"],
        "migration_status": "not_verifiable",
        "reason": "Недостаточно данных для deterministic recheck",
    }


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
) -> dict:
    """Сохранить migrated_findings_report.json и вернуть его содержимое."""
    summary = {
        "still_relevant": 0,
        "resolved_in_new_version": 0,
        "duplicate_of_new_finding": 0,
        "not_verifiable": 0,
        "source_missing": 0,
    }
    for it in items:
        st = it.get("migration_status")
        if st in summary:
            summary[st] += 1

    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "project_id": project_id,
        "current_version_id": current_version_id,
        "source_version_id": source_version_id,
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
            linked += 1
            continue

        if status != "still_relevant":
            continue

        if (origin_v, origin_f) in existing_origins:
            continue  # idempotent skip

        cand = candidates_by_origin.get(origin_f, {})
        mig_id = _stable_migrated_id(origin_v, origin_f)
        # Подстраховка: не конфликтует с существующим id.
        if mig_id in by_id:
            continue

        migrated = {
            "id": mig_id,
            "is_migrated": True,
            "source_type": "migrated_from_previous_version",
            "migration_status": "still_relevant",
            "origin_version_id": origin_v,
            "origin_finding_id": origin_f,
            "origin_expert_status": "accepted",
            "migrated_from_label": origin_v.upper(),
            "severity": cand.get("origin_severity") or "ПРОВЕРИТЬ ПО СМЕЖНЫМ",
            "category": cand.get("origin_category", ""),
            "problem": cand.get("origin_title", ""),
            "description": cand.get("origin_description", ""),
            "norm": (cand.get("origin_norm_refs") or [""])[0],
            "sheet": cand.get("origin_sheet", ""),
            "page": cand.get("origin_page"),
            "evidence": cand.get("origin_evidence", []) or [],
            "migration_note": (
                f"Замечание было согласовано экспертом в {origin_v.upper()} "
                f"и осталось актуальным в {current_version_id.upper()}"
            ),
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

    prev = get_previous_checked_version(project_id, current_version_id)
    if not prev:
        # Пустой отчёт — нет предыдущей проверенной версии.
        report = write_migrated_findings_report(
            project_id, current_version_id, None, [],
        )
        return {
            "status": "ok",
            "source_version_id": None,
            "reason": "no_previous_checked_version",
            "report": report,
        }

    candidates = build_migration_candidates(project_id, current_version_id)
    if not candidates:
        report = write_migrated_findings_report(
            project_id, current_version_id, prev, [],
        )
        return {
            "status": "ok",
            "source_version_id": prev,
            "reason": "no_accepted_findings_in_source",
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
        )
        # Прокидываем origin-метаданные в reason, чтобы отчёт был самодостаточным.
        res.setdefault("origin_title", c["origin_title"])
        res.setdefault("origin_severity", c["origin_severity"])
        results.append(res)

    report = write_migrated_findings_report(
        project_id, current_version_id, prev, results,
        current_findings_missing=current_missing,
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
        "report": report,
        "apply": apply_result,
    }
