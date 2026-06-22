from __future__ import annotations

import re


def _artificial_series(labels: list[dict]) -> bool:
    values = [str(x.get("raw_text") or x.get("text") or "") for x in labels if isinstance(x, dict)]
    nums = []
    prefixes = set()
    for value in values:
        m = re.match(r"(.+?)(\d+)$", value)
        if m:
            prefixes.add(m.group(1))
            nums.append(int(m.group(2)))
    return len(nums) >= 8 and len(prefixes) == 1 and max(nums) - min(nums) >= len(nums) - 1


def score_result(result: dict) -> dict:
    penalties: dict[str, float | bool] = {}
    if result.get("status") != "done" or result.get("total_facts", 0) <= 0:
        return {"score": 0, "penalties": penalties, "hallucination_risk": False}
    facts = result.get("facts") or {}
    score = float(result.get("total_facts", 0)) * 10.0
    score += float(result.get("evidence_coverage", 0) or 0) * 25.0
    score += float(result.get("confidence", 0) or 0) * 10.0
    hallucination = False
    if _artificial_series(facts.get("labels") or []):
        penalties["artificial_series"] = True
        score -= 50.0
        hallucination = True
    if not result.get("json_valid", True):
        penalties["invalid_json"] = True
        score -= 10.0
    return {"score": max(0, round(score, 3)), "penalties": penalties, "hallucination_risk": hallucination}


def pick_best(results: list[dict]):
    best = None
    best_score = 0.0
    for result in results:
        scored = score_result(result)
        if scored["score"] > best_score:
            best_score = scored["score"]
            best = {"result": result, **scored}
    return best if best_score > 0 else None
