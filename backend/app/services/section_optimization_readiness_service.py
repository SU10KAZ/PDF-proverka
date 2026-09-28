"""Гейты готовности оптимизации раздела к инженерному пилоту."""
from __future__ import annotations


def build_pilot_readiness(snapshot: dict, replications: list[dict]) -> dict:
    rows = list(snapshot.get("specification_rows") or [])
    traceable = [row for row in rows if row.get("project_id") and row.get("version_id") and row.get("row_id")]
    current_jobs = [job for job in replications if not job.get("input_stale")]
    decided = [job for job in current_jobs if job.get("expert_decisions")]
    checks = [
        {"key": "snapshot", "passed": bool(snapshot), "message": "Сохранён актуальный снимок раздела"},
        {"key": "traceability", "passed": bool(rows) and len(traceable) == len(rows), "message": f"Адресуемые строки: {len(traceable)} из {len(rows)}"},
        {"key": "critic", "passed": bool(current_jobs) and all(job.get("critic", {}).get("status") in {"pass", "blocked"} for job in current_jobs), "message": "Critic выполнен по текущим досье"},
        {"key": "expert_decisions", "passed": bool(decided), "message": f"Досье с решениями эксперта: {len(decided)}"},
        {"key": "no_stale_decisions", "passed": not any(job.get("input_stale") and job.get("expert_decisions") for job in replications), "message": "Нет решений на устаревших входах"},
    ]
    blockers = [item for item in checks if not item["passed"]]
    return {
        "status": "ready_for_engineering_pilot" if not blockers else "in_progress",
        "checks": checks, "blockers": blockers,
        "metrics": {"specification_rows": len(rows), "current_dossiers": len(current_jobs), "decided_dossiers": len(decided), "stale_dossiers": sum(bool(job.get("input_stale")) for job in replications)},
        "manual_gate": "Полезность и инженерную корректность подтверждает инженер дисциплины на зафиксированном наборе.",
    }


__all__ = ["build_pilot_readiness"]
