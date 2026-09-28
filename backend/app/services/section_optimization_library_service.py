"""Производная библиотека опыта из журналов решений уровня раздела."""
from __future__ import annotations


LIBRARY_VERSION = 1


def build_solution_library(replications: list[dict]) -> dict:
    entries: list[dict] = []
    for job in replications:
        decisions = {str(item.get("project_id") or ""): item for item in job.get("expert_decisions") or []}
        implementations = {str(item.get("project_id") or ""): item for item in job.get("implementation_checks") or []}
        passport = job.get("engineering_passport") or {}
        for project_id, decision in decisions.items():
            implementation = implementations.get(project_id) or {}
            entries.append({
                "library_entry_id": f"{job.get('replication_id')}:{decision.get('decision_id')}",
                "library_version": LIBRARY_VERSION,
                "case_id": job.get("signal_id"),
                "replication_id": job.get("replication_id"),
                "input_fingerprint": decision.get("input_fingerprint") or job.get("input_fingerprint"),
                "project_id": project_id,
                "title": job.get("title") or passport.get("title") or "",
                "action": passport.get("action") or "",
                "subjects": list(passport.get("subjects") or []),
                "decision": decision.get("decision"),
                "conditions": list(decision.get("conditions") or []),
                "reason": decision.get("note") or "",
                "reviewer": decision.get("reviewer") or "",
                "decided_at": decision.get("decided_at"),
                "implementation_status": implementation.get("status") or "not_tracked",
                "implementation_evidence_refs": list(implementation.get("evidence_refs") or []),
                "reuse_policy": "Новая цель требует отдельной проверки применимости.",
            })
    entries.sort(key=lambda item: str(item.get("decided_at") or ""), reverse=True)
    return {
        "library_version": LIBRARY_VERSION,
        "entries": entries,
        "counts": {
            "total": len(entries),
            "implemented": sum(item["implementation_status"] == "implemented" for item in entries),
            "rejected": sum(item["decision"] == "rejected" for item in entries),
        },
    }


__all__ = ["LIBRARY_VERSION", "build_solution_library"]
