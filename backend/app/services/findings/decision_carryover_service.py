"""
Сервис «decision carryover» — перенос ВЕРДИКТА эксперта (согласовано/отклонено)
из предыдущей проверенной версии в текущую.

Отличие от `migrated_findings_service` (который переносит сами accepted-замечания
в список findings): здесь мы идём ПО КАЖДОМУ ТЕКУЩЕМУ замечанию, ищем его аналог
среди РЕШЁННЫХ (accepted И rejected) замечаний прошлой версии, и если Sonnet
подтверждает, что это то же нарушение, — проставляем вердикт в expert_review.json
текущей версии:

- повтор ранее отклонённого  → `rejected`  + «Замечание отклонено ... повторяется — замечание отменено.»
- повтор ранее согласованного → `accepted` + «Замечание согласовано ... повторяется — заказчик/проектировщик его не исправил.»

Принципы:
- решения человека (carried_over=False) НЕ перезаписываются;
- сверка batch-режимом: один Sonnet-вызов на замечание (все top-K кандидатов в
  одном промпте), параллельно DECISION_CARRYOVER_CONCURRENCY воркеров; вердикт
  переносим ТОЛЬКО при валидном match_origin_id из shortlist и confidence >= порога;
- fail-soft: Sonnet недоступен/timeout/не уверен → pending-пометка в
  expert_review.json БЕЗ вердикта («возможный повтор — проверьте»), эксперт решает сам;
- идемпотентно: повторный прогон не плодит дублей (merge по item_id);
- запись в expert_review.json + decisions_log.json идёт через save_expert_review
  под bind_version(current) и с stamp_schedule=False (pending в БЗ не попадают).
"""
from __future__ import annotations

import json
import logging
import os
import re
import subprocess
from pathlib import Path
from typing import Any, Optional

from backend.app.models.expert_review import ExpertDecision
from backend.app.services.common import version_service
from backend.app.services.common.project_service import resolve_project_dir
from backend.app.services.storage.projects_v2_source_resolver import (
    is_projects_v2_version_dir,
)
from backend.app.services.findings import migrated_findings_service as mfs

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1
REPORT_FILENAME = "decision_carryover_report.json"

# ─── Конфигурация (env, читается при каждом запуске — для тестов/оператора) ──

DEFAULT_CONF_THRESHOLD = 0.85
DEFAULT_TOP_K = 3
DEFAULT_MAX_LLM_CALLS = 80
DEFAULT_TIMEOUT_SEC = 120
DEFAULT_MODEL = "claude-sonnet-4-6"


def is_enabled() -> bool:
    """Kill-switch. Default ON (перенос вердиктов всегда в пайплайне для V2+)."""
    return os.environ.get("DECISION_CARRYOVER_ENABLED", "1").strip().lower() in {
        "1", "true", "yes", "on",
    }


def _conf_threshold() -> float:
    raw = os.environ.get("DECISION_CARRYOVER_CONF_THRESHOLD", "").strip()
    try:
        v = float(raw) if raw else DEFAULT_CONF_THRESHOLD
    except ValueError:
        v = DEFAULT_CONF_THRESHOLD
    return max(0.0, min(1.0, v))


def _top_k() -> int:
    raw = os.environ.get("DECISION_CARRYOVER_TOP_K", "").strip()
    try:
        v = int(raw) if raw else DEFAULT_TOP_K
    except ValueError:
        v = DEFAULT_TOP_K
    return max(1, min(v, 10))


def _max_llm_calls() -> int:
    raw = os.environ.get("DECISION_CARRYOVER_MAX_LLM_CALLS", "").strip()
    try:
        v = int(raw) if raw else DEFAULT_MAX_LLM_CALLS
    except ValueError:
        v = DEFAULT_MAX_LLM_CALLS
    return max(0, min(v, 500))


def _timeout_sec() -> int:
    raw = os.environ.get("DECISION_CARRYOVER_TIMEOUT_SEC", "").strip()
    try:
        v = int(raw) if raw else DEFAULT_TIMEOUT_SEC
    except ValueError:
        v = DEFAULT_TIMEOUT_SEC
    return max(10, min(v, 300))


def _model() -> str:
    return os.environ.get("DECISION_CARRYOVER_MODEL", "").strip() or DEFAULT_MODEL


DEFAULT_CONCURRENCY = 4


def _concurrency() -> int:
    """Сколько Sonnet-сверок гнать параллельно (по одному замечанию на вызов).

    3 параллельных `claude -p` проверены вживую (dry-run 2026-07-02); clamp 1..8.
    """
    raw = os.environ.get("DECISION_CARRYOVER_CONCURRENCY", "").strip()
    try:
        v = int(raw) if raw else DEFAULT_CONCURRENCY
    except ValueError:
        v = DEFAULT_CONCURRENCY
    return max(1, min(v, 8))


# ─── v2-aware чтение findings / expert_review нужной версии ──────────────

def _analysis_dirs(version_dir: Path) -> list[Path]:
    """Каталоги с артефактами анализа одной версии (v2 latest first, затем _output)."""
    if is_projects_v2_version_dir(version_dir):
        return [version_dir / "03_analysis" / "latest", version_dir / "_output"]
    return [version_dir / "_output"]


def _review_paths(version_dir: Path) -> list[Path]:
    """Пути к expert_review.json версии (v2 canonical 04_review, затем fallbacks)."""
    if is_projects_v2_version_dir(version_dir):
        return [
            version_dir / "04_review" / "expert_review.json",
            version_dir / "03_analysis" / "latest" / "expert_review.json",
            version_dir / "_output" / "expert_review.json",
        ]
    return [version_dir / "_output" / "expert_review.json"]


def _load_findings(project_dir: Path, project_id: str, version_id: str) -> list[dict]:
    """Findings указанной версии (v2-aware). Пусто, если файла нет."""
    try:
        version_dir = version_service.get_version_dir(project_dir, project_id, version_id)
    except version_service.VersionNotFoundError:
        return []
    for d in _analysis_dirs(version_dir):
        fpath = mfs._find_findings_file_in_dir(d)
        if fpath is not None:
            data = mfs._load_json(fpath)
            if data:
                items = data.get("findings") or data.get("items") or []
                return [it for it in items if isinstance(it, dict)]
    return []


def _load_review_map(project_dir: Path, project_id: str, version_id: str) -> dict[str, dict]:
    """Карта `finding_id → decision dict` из expert_review.json версии (v2-aware)."""
    try:
        version_dir = version_service.get_version_dir(project_dir, project_id, version_id)
    except version_service.VersionNotFoundError:
        return {}
    for p in _review_paths(version_dir):
        data = mfs._load_json(p)
        if not data:
            continue
        decisions = data.get("decisions", [])
        if not isinstance(decisions, list):
            continue
        out: dict[str, dict] = {}
        for d in decisions:
            if not isinstance(d, dict):
                continue
            if (d.get("item_type") or "finding").lower() != "finding":
                continue
            fid = d.get("item_id") or d.get("id")
            if fid:
                out[str(fid)] = d
        return out
    return {}


# ─── Кандидаты из решённых замечаний прошлой версии ──────────────────────

def _version_checked(project_dir: Path, project_id: str, version_id: str) -> bool:
    """Версия «проверена» = есть findings И ≥1 решение эксперта (v2-aware)."""
    if not _load_findings(project_dir, project_id, version_id):
        return False
    return bool(_load_review_map(project_dir, project_id, version_id))


def previous_checked_version(
    project_dir: Path, project_id: str, current_version_id: str,
) -> Optional[str]:
    """Ближайшая более ранняя ПРОВЕРЕННАЯ версия (v2-aware).

    В отличие от migrated_findings_service.get_previous_checked_version (который
    ищет только в `_output/`), учитывает v2-раскладку `04_review/` +
    `03_analysis/latest/` через _load_findings/_load_review_map.
    """
    try:
        manifest = version_service.read_project_versions(project_dir, project_id)
    except Exception:
        return None
    cur = next(
        (v for v in manifest.get("versions", []) if v.get("version_id") == current_version_id),
        None,
    )
    if cur is None:
        return None
    cur_no = int(cur.get("version_no") or 0)
    earlier = [v for v in manifest.get("versions", []) if int(v.get("version_no") or 0) < cur_no]
    earlier.sort(key=lambda v: int(v.get("version_no") or 0), reverse=True)
    for v in earlier:
        if _version_checked(project_dir, project_id, v["version_id"]):
            return v["version_id"]
    return None


def build_decided_candidates(
    project_dir: Path, project_id: str, source_version_id: str,
) -> list[dict]:
    """Решённые замечания прошлой версии (accepted И rejected) в origin_* форме.

    Каждый кандидат несёт `origin_expert_status` (`accepted`/`rejected`) и
    `origin_reason` (комментарий/причина из expert_review прошлой версии).
    """
    review = _load_review_map(project_dir, project_id, source_version_id)
    if not review:
        return []
    findings = _load_findings(project_dir, project_id, source_version_id)
    by_id = {str(f.get("id", "")): f for f in findings}

    candidates: list[dict] = []
    for fid, dec in review.items():
        f = by_id.get(fid)
        if not f:
            continue  # решение есть, а finding не нашёлся (id-mismatch) — пропускаем
        accepted = mfs._is_accepted_decision(dec.get("decision"))
        status = "accepted" if accepted else "rejected"
        candidates.append({
            "origin_version_id": source_version_id,
            "origin_finding_id": fid,
            "origin_title": f.get("problem") or f.get("title") or "",
            "origin_description": f.get("description", ""),
            "origin_severity": f.get("severity", ""),
            "origin_category": f.get("category", ""),
            "origin_norm_refs": mfs._extract_norm_refs(f),
            "origin_evidence": f.get("evidence", []) or [],
            "origin_sheet": f.get("sheet", ""),
            "origin_page": f.get("page"),
            "origin_expert_status": status,
            "origin_reason": str(dec.get("rejection_reason") or ""),
        })
    return candidates


# ─── Sonnet: подтверждение (batch — все кандидаты одного замечания за 1 вызов) ──

def _build_carryover_batch_prompt(current: dict, candidates: list[dict]) -> str:
    """Промпт для Sonnet: одно текущее замечание против ВСЕХ top-K кандидатов
    прошлой версии сразу. Модель выбирает лучшего (или «никто») — один вызов
    вместо K последовательных, и качество лучше: кандидаты сравниваются между собой.
    """
    cur_block = json.dumps({
        "id": current.get("id"),
        "severity": current.get("severity"),
        "category": current.get("category"),
        "page": current.get("page"),
        "sheet": current.get("sheet"),
        "problem": current.get("problem") or current.get("title"),
        "description": current.get("description"),
        "norm": current.get("norm") or mfs._extract_norm_refs(current),
    }, ensure_ascii=False, indent=2)
    cand_blocks = json.dumps([
        {
            "id": c.get("origin_finding_id"),
            "severity": c.get("origin_severity"),
            "category": c.get("origin_category"),
            "page": c.get("origin_page"),
            "sheet": c.get("origin_sheet"),
            "problem": c.get("origin_title"),
            "description": c.get("origin_description"),
            "norm": c.get("origin_norm_refs"),
            "expert_decision": c.get("origin_expert_status"),
            "expert_reason": c.get("origin_reason"),
        }
        for c in candidates
    ], ensure_ascii=False, indent=2)
    return (
        "Ты — эксперт по проектной документации МКД. Дано ОДНО замечание текущей "
        "версии проекта и НЕСКОЛЬКО замечаний-кандидатов из предыдущей версии (по "
        "каждому эксперт уже вынес вердикт). Определи, описывает ли КАКОЙ-ТО ОДИН "
        "из кандидатов ТО ЖЕ САМОЕ нарушение по сути (даже если формулировки разные), "
        "либо все кандидаты — разные нарушения, случайно совпавшие по норме/странице.\n\n"
        f"ЗАМЕЧАНИЕ ТЕКУЩЕЙ ВЕРСИИ:\n{cur_block}\n\n"
        f"КАНДИДАТЫ ИЗ ПРЕДЫДУЩЕЙ ВЕРСИИ:\n{cand_blocks}\n\n"
        "Если лучший кандидат имеет вердикт rejected, дополнительно оцени, применима "
        "ли прежняя причина отклонения к текущему замечанию.\n\n"
        "Ответ — строго JSON в одной строке, без markdown и комментариев:\n"
        "{\n"
        '  "match_origin_id": "<id совпавшего кандидата>" | null,\n'
        '  "confidence": 0.0..1.0,\n'
        '  "prior_verdict_applies": true | false,\n'
        '  "reason": "1-2 предложения почему"\n'
        "}\n"
        "Если норма/страница общие, но объект/числа/тип проблемы разные — "
        "match_origin_id:null. Если то же нарушение другими словами — верни id этого "
        "кандидата. Выбирай не более ОДНОГО кандидата — самого точного."
    )


def _run_sonnet_sync(prompt: str, required_key: str = "match_origin_id") -> Optional[dict]:
    """Синхронный `claude -p` (Sonnet). fail-soft: любой сбой → None.

    Работает по подписке (feedback_subscription_only) — paid_api_guard не нужен.
    Kill-switch — на уровне всего этапа (is_enabled).
    """
    try:
        from backend.app.core.config import get_claude_cli
    except Exception:
        logger.warning("decision-carryover: cannot import claude config")
        return None
    try:
        cli = get_claude_cli()
    except Exception:
        cli = None
    if not cli:
        logger.warning("decision-carryover: claude CLI not configured")
        return None

    cmd = [cli, "-p", "--model", _model(), "--output-format", "json"]
    env = {k: v for k, v in os.environ.items() if not k.startswith("CLAUDE")}
    try:
        proc = subprocess.run(
            cmd, input=prompt, capture_output=True, text=True,
            timeout=_timeout_sec(), env=env, check=False,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as e:
        logger.warning("decision-carryover: subprocess failed: %s", e)
        return None

    if proc.returncode != 0 and not proc.stdout:
        logger.warning(
            "decision-carryover: exit=%d stderr=%s",
            proc.returncode, (proc.stderr or "")[:200],
        )
        return None

    try:
        cli_data = json.loads(proc.stdout)
        result_text = cli_data.get("result") or ""
    except (json.JSONDecodeError, KeyError, TypeError):
        result_text = proc.stdout
    if not result_text:
        return None
    match = re.search(r"\{.*\}", result_text, re.DOTALL)
    if not match:
        return None
    try:
        parsed = json.loads(match.group(0))
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, dict) or required_key not in parsed:
        return None
    return parsed


# ─── Тексты комментариев переноса ────────────────────────────────────────

def _trim(text: str, limit: int = 400) -> str:
    s = " ".join(str(text or "").split())
    return s if len(s) <= limit else s[:limit].rstrip() + "…"


def _origin_tail(v: str, origin_id: str, origin_problem: str) -> str:
    """Хвост причины: суть замечания прошлой версии — чтобы инженер сравнил."""
    body = _trim(origin_problem)
    if body:
        return f" Замечание {v} ({origin_id}): «{body}»."
    return f" (исходное замечание {v}: {origin_id})."


def _carryover_comment(status: str, prev_version_id: str,
                       origin_id: str = "", origin_problem: str = "") -> str:
    v = str(prev_version_id or "").upper()
    if status == "rejected":
        head = (
            f"↩ Перенесено из предыдущей версии ({v}): в {v} это замечание было "
            f"ОТКЛОНЕНО и повторяется в текущей проверке — вердикт «отклонено»."
        )
    else:
        head = (
            f"↩ Перенесено из предыдущей версии ({v}): в {v} это замечание было "
            f"СОГЛАСОВАНО и снова присутствует — заказчик/проектировщик его не исправил."
        )
    return head + _origin_tail(v, origin_id, origin_problem)


def _manual_note(prev_version_id: str, origin_id: str, origin_status: str,
                 origin_problem: str = "") -> str:
    v = str(prev_version_id or "").upper()
    st = {"accepted": "было согласовано", "rejected": "было отклонено"}.get(origin_status, "")
    st = f", в {v} {st}" if st else ""
    head = (
        f"↩ Возможный повтор из предыдущей версии ({v}): похоже на замечание "
        f"{origin_id}{st}, но автоматически вердикт не проставлен — сверьте с текущим "
        f"и примите решение сами."
    )
    return head + _origin_tail(v, origin_id, origin_problem)


# ─── Отчёт ───────────────────────────────────────────────────────────────

def _report_path(project_id: str, version_id: str) -> Path:
    output_dir = version_service.resolve_version_output_dir(project_id, version_id)
    return output_dir / REPORT_FILENAME


def read_report(project_id: str, version_id: str) -> Optional[dict]:
    try:
        return mfs._load_json(_report_path(project_id, version_id))
    except (version_service.VersionNotFoundError, FileNotFoundError):
        return None


def _write_report(project_id: str, current_version_id: str, report: dict) -> Path:
    path = _report_path(project_id, current_version_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


# ─── Главный сценарий ────────────────────────────────────────────────────

def run_decision_carryover(
    project_id: str, current_version_id: str, *, dry_run: bool = False,
) -> dict:
    """Перенести вердикты из предыдущей проверенной версии в текущую.

    Returns dict: {status, source_version_id, summary, report_path, saved}.
    """
    if not is_enabled():
        return {"status": "skipped", "reason": "disabled"}
    if not current_version_id or current_version_id == "v1":
        return {"status": "skipped", "reason": "no_previous_version"}

    project_dir = resolve_project_dir(project_id)
    # v2-aware поиск предыдущей проверенной версии (04_review/03_analysis, не только _output).
    prev = previous_checked_version(project_dir, project_id, current_version_id)
    if not prev:
        report = {
            "schema_version": SCHEMA_VERSION,
            "project_id": project_id,
            "current_version_id": current_version_id,
            "source_version_id": None,
            "checked_at": mfs._now_iso(),
            "status": "no_previous_checked_version",
            "llm_model": _model(),
            "llm_calls_made": 0,
            "summary": {},
            "items": [],
        }
        if not dry_run:
            _write_report(project_id, current_version_id, report)
        return {"status": "ok", "source_version_id": None,
                "reason": "no_previous_checked_version", "saved": 0}

    candidates = build_decided_candidates(project_dir, project_id, prev)
    current_findings = _load_findings(project_dir, project_id, current_version_id)
    current_review = _load_review_map(project_dir, project_id, current_version_id)

    conf_threshold = _conf_threshold()
    top_k = _top_k()
    max_calls = _max_llm_calls()
    llm_calls = 0

    items: list[dict] = []
    decisions: list[ExpertDecision] = []

    # ── Фаза 1 (последовательно): статусы без Sonnet + shortlist для остальных ──
    # tasks: (index в items, current finding, shortlist) — по одному Sonnet-вызову
    # на замечание (batch-промпт со всеми top-K кандидатами сразу).
    tasks: list[tuple[int, dict, list]] = []

    for cur in current_findings:
        cur_id = str(cur.get("id", ""))
        row: dict[str, Any] = {
            "current_id": cur_id,
            "current_problem": cur.get("problem") or cur.get("title") or "",
        }

        # Не трогаем замечание, по которому уже есть ВЕРДИКТ (accepted/rejected):
        # ни решение человека, ни ранее перенесённый вердикт не перезаписываем.
        # Пустые pending-пометки (от прошлого прогона) перепроверяем — их вердикт
        # может проставиться в этот раз.
        existing = current_review.get(cur_id)
        if existing is not None and existing.get("decision"):
            row["status"] = ("already_human_decided"
                             if not existing.get("carried_over") else "already_carried")
            items.append(row)
            continue

        if not candidates:
            row["status"] = "no_candidate"
            items.append(row)
            continue

        # Детерминированный shortlist top-K кандидатов прошлой версии.
        scored = []
        for cand in candidates:
            s = mfs._score_pair(cand, cur)
            scored.append((s["score"], cand, s))
        scored.sort(key=lambda x: -x[0])
        shortlist = [t for t in scored[:top_k] if t[0] >= mfs.BORDERLINE_LOW]

        if not shortlist:
            row["status"] = "no_candidate"
            row["top_score"] = round(scored[0][0], 3) if scored else 0.0
            items.append(row)
            continue

        items.append(row)  # статус проставится в фазе 3
        tasks.append((len(items) - 1, cur, shortlist))

    # Лимит Sonnet-вызовов: 1 вызов = 1 замечание (batch); хвост сверх лимита
    # уходит в pending без вызова.
    if max_calls and len(tasks) > max_calls:
        overflow = tasks[max_calls:]
        tasks = tasks[:max_calls]
    else:
        overflow = []

    # ── Фаза 2 (параллельно): batch-сверки Sonnet ──
    # subprocess.run отпускает GIL — ThreadPoolExecutor подходит; порядок
    # результатов фиксируем по индексу задачи (детерминированная сборка).
    from concurrent.futures import ThreadPoolExecutor

    def _judge(task):
        _idx, cur, shortlist = task
        # fail-soft на уровне вызова: любое неожиданное исключение воркера — это
        # pending для одного замечания, а не крах всего прогона (P1 ревью).
        try:
            return _run_sonnet_sync(
                _build_carryover_batch_prompt(cur, [c for _, c, _ in shortlist])
            )
        except Exception:  # noqa: BLE001
            logger.warning("decision-carryover: judge worker failed", exc_info=True)
            return None

    llm_results: list[Optional[dict]] = []
    if tasks:
        with ThreadPoolExecutor(max_workers=_concurrency()) as pool:
            llm_results = list(pool.map(_judge, tasks))
        llm_calls = len(tasks)

    # ── Фаза 3 (последовательно, в исходном порядке): интерпретация ──
    for (idx, cur, shortlist), llm in zip(tasks, llm_results, strict=True):
        cur_id = str(cur.get("id", ""))
        row = items[idx]
        by_oid = {c["origin_finding_id"]: (score, c) for score, c, _ in shortlist}

        confirmed = None
        llm_info = None
        if llm is not None:
            match_id = llm.get("match_origin_id")
            try:
                conf = float(llm.get("confidence") or 0.0)
            except (TypeError, ValueError):  # модель вернула нечисло (P2 ревью)
                conf = 0.0
            prior_applies = llm.get("prior_verdict_applies")
            reason = str(llm.get("reason") or "")[:300]
            llm_info = {"match_origin_id": match_id, "confidence": conf,
                        "prior_verdict_applies": prior_applies, "reason": reason}
            # Валидация: выбранный id обязан быть из shortlist (анти-галлюцинация).
            picked = by_oid.get(str(match_id)) if match_id else None
            if picked is not None:
                score, cand = picked
                rejected_but_na = (
                    cand["origin_expert_status"] == "rejected"
                    and prior_applies is False
                )
                if conf >= conf_threshold and not rejected_but_na:
                    confirmed = (cand, score)

        if confirmed is None:
            # Похожий кандидат есть, но уверенности недостаточно. Записываем в
            # решения эксперта БЕЗ вердикта (decision="") — пометка «возможный
            # повтор из прошлой версии», чтобы эксперт САМ принял решение.
            # Если Sonnet выбрал валидного кандидата, но не добрал порог — берём
            # его для ноты; иначе топ по детерминированному скору.
            note_cand = None
            if llm_info and llm_info.get("match_origin_id"):
                picked = by_oid.get(str(llm_info["match_origin_id"]))
                if picked is not None:
                    note_cand = picked[1]
            top_cand = note_cand or shortlist[0][1]
            note = _manual_note(
                prev, top_cand["origin_finding_id"],
                top_cand.get("origin_expert_status", ""),
                top_cand.get("origin_title") or top_cand.get("origin_description", ""),
            )
            decisions.append(ExpertDecision(
                item_id=cur_id,
                item_type="finding",
                decision="",  # без вердикта — эксперт решает сам
                rejection_reason=note,
                reviewer=f"Авто-перенос из {str(prev).upper()}",
                timestamp=mfs._now_iso(),
                carried_over=True,
                carried_from_version=prev,
                carried_from_item_id=top_cand["origin_finding_id"],
            ))
            row.update(
                status="needs_manual_review",
                top_score=round(shortlist[0][0], 3),
                top_candidate_id=top_cand["origin_finding_id"],
                carried_comment=note,
            )
            if llm_info:
                row["llm"] = llm_info
            continue

        cand, score = confirmed
        status = cand["origin_expert_status"]  # accepted | rejected
        comment = _carryover_comment(
            status, prev, cand["origin_finding_id"],
            cand.get("origin_title") or cand.get("origin_description", ""),
        )
        decisions.append(ExpertDecision(
            item_id=cur_id,
            item_type="finding",
            decision=status,
            rejection_reason=comment,
            reviewer=f"Авто-перенос из {str(prev).upper()}",
            timestamp=mfs._now_iso(),
            carried_over=True,
            carried_from_version=prev,
            carried_from_item_id=cand["origin_finding_id"],
        ))
        row.update(
            status="carried_over",
            carried_decision=status,
            carried_comment=comment,
            origin_version_id=prev,
            origin_finding_id=cand["origin_finding_id"],
            origin_expert_status=status,
            top_score=round(score, 3),
            llm=llm_info,
        )

    # Хвост сверх лимита Sonnet-вызовов → pending без вызова (эксперт решит сам).
    for idx, cur, shortlist in overflow:
        cur_id = str(cur.get("id", ""))
        row = items[idx]
        top_cand = shortlist[0][1]
        note = _manual_note(
            prev, top_cand["origin_finding_id"],
            top_cand.get("origin_expert_status", ""),
            top_cand.get("origin_title") or top_cand.get("origin_description", ""),
        )
        decisions.append(ExpertDecision(
            item_id=cur_id, item_type="finding", decision="",
            rejection_reason=note,
            reviewer=f"Авто-перенос из {str(prev).upper()}",
            timestamp=mfs._now_iso(),
            carried_over=True, carried_from_version=prev,
            carried_from_item_id=top_cand["origin_finding_id"],
        ))
        row.update(
            status="needs_manual_review",
            top_score=round(shortlist[0][0], 3),
            top_candidate_id=top_cand["origin_finding_id"],
            carried_comment=note,
            llm={"reason": "лимит Sonnet-вызовов на прогон исчерпан"},
        )

    # Запись вердиктов: bind текущей версии + stamp_schedule=False.
    # dry_run: ничего не пишем (превью — что перенеслось бы).
    saved = 0
    if decisions and not dry_run:
        from backend.app.services.knowledge_base import knowledge_base_service as kb
        with version_service.pinned_version(current_version_id):
            result = kb.save_expert_review(
                project_id, decisions,
                reviewer=f"Авто-перенос из {str(prev).upper()}",
                stamp_schedule=False,
            )
            saved = int(result.get("saved", 0))

    summary = {
        "carried_over": sum(1 for i in items if i["status"] == "carried_over"),
        "carried_accepted": sum(
            1 for i in items
            if i["status"] == "carried_over" and i.get("carried_decision") == "accepted"),
        "carried_rejected": sum(
            1 for i in items
            if i["status"] == "carried_over" and i.get("carried_decision") == "rejected"),
        "needs_manual_review": sum(1 for i in items if i["status"] == "needs_manual_review"),
        # Pending-пометки без вердикта, записанные в expert_review для эксперта.
        "pending_written": sum(1 for d in decisions if not d.decision),
        "no_candidate": sum(1 for i in items if i["status"] == "no_candidate"),
        "already_human_decided": sum(1 for i in items if i["status"] == "already_human_decided"),
        "already_carried": sum(1 for i in items if i["status"] == "already_carried"),
    }
    report = {
        "schema_version": SCHEMA_VERSION,
        "project_id": project_id,
        "current_version_id": current_version_id,
        "source_version_id": prev,
        "checked_at": mfs._now_iso(),
        "status": "ok",
        "llm_model": _model(),
        "llm_calls_made": llm_calls,
        "total_current_findings": len(current_findings),
        "total_decided_candidates": len(candidates),
        "summary": summary,
        "items": items,
    }
    report_path = None if dry_run else _write_report(project_id, current_version_id, report)

    return {
        "status": "ok",
        "source_version_id": prev,
        "dry_run": dry_run,
        "summary": summary,
        "saved": saved,
        "report_path": str(report_path) if report_path else None,
        "items": items,
    }
