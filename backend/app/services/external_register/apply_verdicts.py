"""Применение вердиктов заказчика из реестра к замечаниям проектов.

Закрывает два пробела `external_register` относительно задачи импорта ответов
заказчика (King&Sons 2026-05-27):

  G3 — создание findings для замечаний реестра, которых нет на портале
       (unmatched-записи и неаудированные проекты);
  G4 — простановка экспертного вердикта (accepted/rejected) по решению
       заказчика — то, что видно в режиме эксперта (зелёный/красный).

Для каждой записи реестра:
  - решение заказчика → expert decision (`VERDICT_MAP`);
  - matched к нашему finding (auto/confirmed) → используем его id;
  - иначе → создаём новый finding `REG-*` в V1 `03_findings.json`;
  - `save_expert_review(...)` ставит вердикт (merge по item_id, идемпотентно).

Версии: всё пишется в V1 (корень проекта) — реестр относится к V1, даже если
у проекта есть V2 (`version_service.pinned_version("v1")`).

Мутации `03_findings.json` сопровождаются бэкапом `.bak`. Стабильные id и merge
делают повторный прогон идемпотентным.
"""
from __future__ import annotations

import json
import logging
import shutil
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from backend.app.models.expert_review import ExpertDecision
from backend.app.services.common import version_service
from backend.app.services.common.project_service import pinned_object
from backend.app.services.external_register import parser, section_map, service
from backend.app.services.external_register.models import (
    CustomerResponse,
    MatchStatus,
    RegisterEntry,
    RegisterFile,
)
from backend.app.services.knowledge_base import knowledge_base_service as kb_service

logger = logging.getLogger(__name__)


# ─── Таблицы соответствия ──────────────────────────────────────────────────

# Решение заказчика → экспертный вердикт. UNKNOWN → пропуск (нет вердикта).
VERDICT_MAP: dict[CustomerResponse, str] = {
    CustomerResponse.OTKLONENO: "rejected",
    CustomerResponse.TREBUET_VNESENIYA: "accepted",
    CustomerResponse.VNESENO: "accepted",
    CustomerResponse.PO_SOGLASOVANIYU: "accepted",
    CustomerResponse.UCHTENO: "accepted",
}

# Категория СУ-10 → severity finding'а.
SEV_MAP: dict[str, str] = {
    "Критическая": "КРИТИЧЕСКОЕ",
    "Экономическая": "ЭКОНОМИЧЕСКОЕ",
    "Эксплуатационная": "ЭКСПЛУАТАЦИОННОЕ",
    "Рекомендательная": "РЕКОМЕНДАТЕЛЬНОЕ",
    "Проверить по смежным": "ПРОВЕРИТЬ ПО СМЕЖНЫМ",
}

# Какие статусы матчинга считаем «замечание уже есть на портале».
_MATCHED_STATUSES = (MatchStatus.AUTO_MATCHED, MatchStatus.CONFIRMED)


# ─── Результат ──────────────────────────────────────────────────────────────


@dataclass
class ProjectPlan:
    project_id: str
    section_codes: set[str] = field(default_factory=set)
    had_findings_file: bool = False
    mark_existing: list[dict] = field(default_factory=list)   # {finding_id, decision, key}
    create_new: list[dict] = field(default_factory=list)      # {finding_id, decision, key, severity}
    needs_review_as_new: int = 0                              # сколько new пришло из needs_review

    def as_summary(self) -> dict:
        acc = sum(1 for x in self.mark_existing + self.create_new if x["decision"] == "accepted")
        rej = sum(1 for x in self.mark_existing + self.create_new if x["decision"] == "rejected")
        return {
            "project_id": self.project_id,
            "sections": sorted(self.section_codes),
            "had_findings_file": self.had_findings_file,
            "mark_existing": len(self.mark_existing),
            "create_new": len(self.create_new),
            "needs_review_as_new": self.needs_review_as_new,
            "accepted": acc,
            "rejected": rej,
        }


@dataclass
class ApplyReport:
    object_id: str
    register_id: str
    dry_run: bool
    projects: list[ProjectPlan] = field(default_factory=list)
    skipped_no_verdict: int = 0
    skipped_unmapped: list[str] = field(default_factory=list)   # section_codes

    def totals(self) -> dict:
        return {
            "projects": len(self.projects),
            "mark_existing": sum(len(p.mark_existing) for p in self.projects),
            "create_new": sum(len(p.create_new) for p in self.projects),
            "needs_review_as_new": sum(p.needs_review_as_new for p in self.projects),
            "skipped_no_verdict": self.skipped_no_verdict,
            "skipped_unmapped_sections": sorted(set(self.skipped_unmapped)),
        }


# ─── Создание finding'а ──────────────────────────────────────────────────────


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def _build_finding(entry: RegisterEntry, finding_id: str, register_id: str) -> dict:
    """Собрать finding-dict из записи реестра."""
    severity = SEV_MAP.get(entry.cat_su10, "ПРОВЕРИТЬ ПО СМЕЖНЫМ")
    return {
        "id": finding_id,
        "severity": severity,
        "category": "customer_registry",
        "sheet": entry.sheet_ref,
        "page": None,
        "problem": entry.problem,
        "description": entry.description,
        "norm": "",
        "norm_quote": None,
        "solution": entry.proposed_solution,
        "risk": entry.risk,
        "evidence": [],
        "origin": "customer_registry",
        "register_id": register_id,
        "register_key": entry.key,
        "external_register": {
            "register_id": register_id,
            "comment_key": entry.key,
            "customer_response": entry.customer_response.value,
            "customer_comment": entry.customer_comment,
            "confidence": 1.0,
            "auto_matched": False,
            "user_confirmed": False,
            "source": "customer_registry_import",
            "updated_at": _now_iso(),
        },
    }


def _new_findings_file(project_id: str) -> dict:
    return {
        "meta": {
            "project_id": project_id,
            "audit_completed": None,
            "source": "customer_registry",
            "total_findings": 0,
            "by_severity": {},
        },
        "findings": [],
    }


def _recompute_meta(data: dict) -> None:
    findings = data.get("findings", [])
    by_sev: dict[str, int] = {}
    for f in findings:
        sev = f.get("severity", "")
        by_sev[sev] = by_sev.get(sev, 0) + 1
    meta = data.setdefault("meta", {})
    meta["total_findings"] = len(findings)
    meta["by_severity"] = by_sev


# ─── Основной проход ─────────────────────────────────────────────────────────


def build_plan(
    register: RegisterFile,
    object_id: str,
) -> ApplyReport:
    """Построить план (без записи): что пометить, что создать, что пропустить."""
    report = ApplyReport(
        object_id=object_id,
        register_id=register.register_id,
        dry_run=True,
    )
    by_project: dict[str, ProjectPlan] = {}

    for entry in register.entries:
        decision = VERDICT_MAP.get(entry.customer_response)
        if decision is None:
            report.skipped_no_verdict += 1
            continue

        project_id = section_map.lookup(entry.section_code)
        if not project_id:
            report.skipped_unmapped.append(entry.section_code or "(empty)")
            continue

        plan = by_project.get(project_id)
        if plan is None:
            out_dir = service._findings_output_dir(object_id, project_id)
            plan = ProjectPlan(
                project_id=project_id,
                had_findings_file=(out_dir / "03_findings.json").exists(),
            )
            by_project[project_id] = plan
        plan.section_codes.add(entry.section_code)

        matched = (
            entry.match is not None
            and entry.match_status in _MATCHED_STATUSES
            and entry.match.project_id == project_id
        )
        if matched:
            plan.mark_existing.append({
                "finding_id": entry.match.finding_id,
                "decision": decision,
                "key": entry.key,
            })
        else:
            plan.create_new.append({
                "finding_id": parser.entry_key_to_finding_id(entry.key),
                "decision": decision,
                "key": entry.key,
                "severity": SEV_MAP.get(entry.cat_su10, "ПРОВЕРИТЬ ПО СМЕЖНЫМ"),
                "entry": entry,
            })
            if entry.match_status == MatchStatus.NEEDS_REVIEW:
                plan.needs_review_as_new += 1

    report.projects = [by_project[k] for k in sorted(by_project)]
    return report


def apply_register(
    register: RegisterFile,
    object_id: str,
    *,
    dry_run: bool = True,
    reviewer: str = "su10_registry",
) -> ApplyReport:
    """Построить план и (если не dry-run) применить: создать findings + вердикты.

    Возвращает ApplyReport со статистикой. Всё пишется в V1 (`pinned_version`).
    """
    report = build_plan(register, object_id)
    report.dry_run = dry_run
    if dry_run:
        return report

    with pinned_object(object_id), version_service.pinned_version("v1"):
        for plan in report.projects:
            out_dir = service._findings_output_dir(object_id, plan.project_id)
            findings_path = out_dir / "03_findings.json"

            # 1. Создать недостающие findings. Пишем И в master (03_findings.json),
            #    И в отображаемый файл (03a_norms_verified.json), который читает
            #    findings_service/UI — иначе REG-замечания не видны на портале.
            if plan.create_new:
                _write_new_findings(findings_path, plan, register.register_id,
                                    create_if_missing=True)
                display_path = out_dir / "03a_norms_verified.json"
                if display_path.exists():
                    _write_new_findings(display_path, plan, register.register_id,
                                        create_if_missing=False)

            # 2. Проставить экспертные вердикты (merge, идемпотентно).
            decisions = [
                ExpertDecision(
                    item_id=x["finding_id"],
                    item_type="finding",
                    decision=x["decision"],
                    rejection_reason=None,
                    reviewer=reviewer,
                    timestamp=_now_iso(),
                )
                for x in (plan.mark_existing + plan.create_new)
            ]
            # rejection_reason — комментарий заказчика для отклонённых
            reason_by_id = {
                x["finding_id"]: _entry_comment(x)
                for x in (plan.mark_existing + plan.create_new)
                if x["decision"] == "rejected"
            }
            for d in decisions:
                if d.decision == "rejected":
                    d.rejection_reason = reason_by_id.get(d.item_id) or None

            if decisions:
                kb_service.save_expert_review(plan.project_id, decisions, reviewer=reviewer)

    return report


def _entry_comment(plan_item: dict) -> str:
    entry = plan_item.get("entry")
    return entry.customer_comment if entry is not None else ""


def _write_new_findings(
    findings_path: Path,
    plan: ProjectPlan,
    register_id: str,
    *,
    create_if_missing: bool = True,
) -> None:
    """Добавить REG-* findings в findings-файл (с бэкапом), идемпотентно.

    create_if_missing=False — если файла нет, ничего не делаем (для 03a, который
    не выдумываем с нуля).
    """
    findings_path.parent.mkdir(parents=True, exist_ok=True)

    if findings_path.exists():
        try:
            data = json.loads(findings_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            logger.error("Битый findings %s: %s — пропуск проекта", findings_path, e)
            return
        # Бэкап перед первой мутацией.
        bak = findings_path.with_suffix(findings_path.suffix + ".bak")
        if not bak.exists():
            shutil.copy2(findings_path, bak)
    elif create_if_missing:
        data = _new_findings_file(plan.project_id)
    else:
        return

    findings = data.setdefault("findings", [])
    by_id = {f.get("id"): f for f in findings}

    for item in plan.create_new:
        fid = item["finding_id"]
        entry = item["entry"]
        finding = _build_finding(entry, fid, register_id)
        if fid in by_id:
            by_id[fid].update(finding)   # идемпотентное обновление тела
        else:
            findings.append(finding)
            by_id[fid] = finding

    _recompute_meta(data)
    findings_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
