"""Гейты готовности оптимизации раздела к инженерному пилоту."""
from __future__ import annotations


def build_pilot_readiness(snapshot: dict, replications: list[dict], packages: list[dict] | None = None) -> dict:
    rows = list(snapshot.get("specification_rows") or [])
    traceable = [row for row in rows if row.get("project_id") and row.get("version_id") and row.get("row_id")]
    latest_by_signal = {}
    for job in replications:
        if job.get("input_stale"):
            continue
        key = job.get("signal_id") or job.get("replication_id")
        previous = latest_by_signal.get(key)
        if previous is None or str(job.get("updated_at") or "") > str(previous.get("updated_at") or ""):
            latest_by_signal[key] = job
    current_jobs = list(latest_by_signal.values())
    decided = [
        job for job in current_jobs
        if job.get("expert_decisions")
        or (job.get("package_decision") or {}).get("decision") in {"economic_yes", "economic_no", "needs_data"}
    ]
    package_decisions = [item for item in (packages or []) if item.get("decision")]
    package_workflow_complete = bool(packages) and len(package_decisions) == len(packages)
    checks = [
        {"key": "snapshot", "passed": bool(snapshot), "message": "Сохранён актуальный снимок раздела"},
        {"key": "traceability", "passed": bool(rows) and len(traceable) == len(rows), "message": f"Адресуемые строки: {len(traceable)} из {len(rows)}"},
        {"key": "critic", "passed": bool(current_jobs) and all(job.get("critic", {}).get("status") == "pass" for job in current_jobs), "message": "Critic выполнен по текущим досье"},
        {"key": "expert_decisions", "passed": bool(decided) or package_workflow_complete, "message": f"Досье с решениями эксперта: {len(decided)}; пакетов закрыто: {len(package_decisions)}"},
        {"key": "no_stale_decisions", "passed": not any(
            job.get("input_stale") and (job.get("expert_decisions") or job.get("package_decision"))
            for job in replications
        ), "message": "Нет решений на устаревших входах"},
    ]
    if packages:
        checks.append({
            "key": "package_decisions",
            "passed": len(package_decisions) == len(packages),
            "message": f"Пакетных решений: {len(package_decisions)} из {len(packages)}",
        })
    blockers = [item for item in checks if not item["passed"]]
    return {
        "status": "ready_for_engineering_pilot" if not blockers else "in_progress",
        "checks": checks, "blockers": blockers,
        "metrics": {"specification_rows": len(rows), "current_dossiers": len(current_jobs), "decided_dossiers": len(decided), "stale_dossiers": sum(bool(job.get("input_stale")) for job in replications), "packages": len(packages or []), "decided_packages": len(package_decisions) if packages else 0},
        "manual_gate": "Полезность и инженерную корректность подтверждает инженер дисциплины на зафиксированном наборе.",
    }


__all__ = ["build_pilot_readiness"]
