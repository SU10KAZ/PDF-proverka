"""Оценка повторов стенда по правилам протокола (раздел 7) и сводка кампании.

Цели — итоговые замечания с `source_stage == alia_docnorm_audit`. Сопоставление —
`docnorm_match` с известными подтверждёнными отклонениями docnorm этой версии.
Тексты для сопоставления берутся после верификатора (до `norm_fix`): по контракту
`norm_fix` только правит формулировки и не добавляет и не удаляет замечания —
это проверяется (`norms_contract_ids_unchanged`).
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from backend.app.services.algorithm_improvement import docnorm_match as M

DOCNORM = "alia_docnorm_audit"


def _load(path: Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _findings(path: Path) -> list[dict]:
    if not Path(path).is_file():
        return []
    data = _load(path)
    return [f for f in (data.get("findings", []) if isinstance(data, dict) else data) if isinstance(f, dict)]


def score_run(run_dir: Path, known: dict) -> dict[str, Any]:
    run = _load(run_dir / "receipts" / "run.json")
    after_verify = _findings(run_dir / "result" / "after_verify.json")
    final = _findings(run_dir / "result" / "final.json")
    final_ids = {str(f.get("id")) for f in final}
    targets = [f for f in after_verify if f.get("source_stage") == DOCNORM and str(f.get("id")) in final_ids]
    known_rej = [{**k["finding"], "id": k["obs_id"]} for k in known["docnorm_rejected_verified"]]
    res = M.match(targets, known_rej)
    known_status = res["right"]
    base_texts = Counter(str(f.get("problem") or "").strip() for f in known.get("base_findings", []))
    non_target = [f for f in final if f.get("source_stage") != DOCNORM]
    final_texts = Counter(str(f.get("problem") or "").strip() for f in non_target)
    other_acc = known.get("other_accepted_verified", [])
    retained = sum(1 for k in other_acc if str(k["finding"].get("problem") or "").strip() in final_texts)
    raw_docnorm = run_dir / "data" / Path(known["version_path_in_sandbox"]) / "03_analysis" / "latest" / \
        "03_findings_targeted_alia_docnorm_audit.json"
    formal_notes = []
    if raw_docnorm.is_file():
        payload = _load(raw_docnorm)
        formal_notes = payload.get("formal_notes") or [] if isinstance(payload, dict) else []
    non_target_sig = hashlib.sha256("\n".join(sorted(
        str(f.get("problem") or "").strip() for f in non_target)).encode("utf-8")).hexdigest()
    return {
        "run": run_dir.name, "rules": M.RULES_VERSION, "status": run.get("status"), "error": run.get("error"),
        "non_target_signature": non_target_sig,
        "checks": run.get("checks"), "counts": run.get("counts"),
        "targets": len(targets),
        "known_rejected": len(known_rej),
        "KR_strong": sum(1 for s in known_status.values() if s == M.STRONG),
        "known_status": dict(Counter(known_status.values())),
        "per_known": known_status,
        "targets_status": dict(Counter(res["left"].values())),
        "new_unmatched_targets": sum(1 for s in res["left"].values() if s == M.UNMATCHED),
        "non_target_findings": len(non_target),
        "non_target_not_in_base": sum((final_texts - base_texts).values()),
        "base_not_in_final": sum((base_texts - final_texts).values()),
        "other_accepted_known": len(other_acc),
        "other_accepted_retained_exact": retained,
        "formal_notes": len(formal_notes),
    }


def summarize(campaign_dir: Path, scores: list[dict], plan: dict) -> dict[str, Any]:
    by_doc_arm: dict[tuple, list[dict]] = defaultdict(list)
    for s in scores:
        by_doc_arm[(s["doc_key"], s["arm"])].append(s)
    docs = {}
    for (doc, arm), rows in sorted(by_doc_arm.items()):
        rows.sort(key=lambda r: r["repeat"])
        stability = Counter()
        keys = set().union(*[r["per_known"].keys() for r in rows]) if rows else set()
        for k in keys:
            hits = sum(1 for r in rows if r["per_known"].get(k) == M.STRONG)
            stability[f"{hits}/{len(rows)}"] += 1
        docs.setdefault(doc, {})["non_target_identical_across_runs"] = None
        docs[doc][arm] = {
            "repeats": [r["repeat"] for r in rows],
            "KR_strong": [r["KR_strong"] for r in rows],
            "targets": [r["targets"] for r in rows],
            "new_unmatched": [r["new_unmatched_targets"] for r in rows],
            "formal_notes": [r["formal_notes"] for r in rows],
            "known_stability": dict(stability),
            "known_status_totals": dict(sum((Counter(r["known_status"]) for r in rows), Counter())),
            "other_accepted_retained": [f"{r['other_accepted_retained_exact']}/{r['other_accepted_known']}" for r in rows],
            "non_target_not_in_base": [r["non_target_not_in_base"] for r in rows],
        }
    for doc in docs:
        sigs = {s["non_target_signature"] for s in scores if s["doc_key"] == doc}
        docs[doc]["non_target_identical_across_runs"] = len(sigs) == 1
    calls = Counter()
    for s in scores:
        for kind, n in (s.get("counts") or {}).items():
            calls[kind] += n
    n_runs = len(scores)
    per_run = {k: round(v / n_runs, 2) for k, v in calls.items()} if n_runs else {}
    checks = Counter()
    for s in scores:
        for key, value in (s.get("checks") or {}).items():
            checks[f"{key}={value}"] += 1
    return {"campaign": campaign_dir.name, "mode": plan.get("mode"), "rules": M.RULES_VERSION, "runs": n_runs,
            "statuses": dict(Counter(s["status"] for s in scores)),
            "checks": dict(checks), "calls_total": dict(calls), "calls_per_run": per_run, "docs": docs}
