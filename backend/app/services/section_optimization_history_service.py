"""Read-only history of accepted ideas; no automatic approval or savings carryover."""
from __future__ import annotations

import hashlib
import json
import re
from typing import Callable


def _hash(value) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def _text(value) -> str:
    return " ".join(str(value or "").lower().replace("ё", "е").split())


def _identity(item: dict) -> tuple:
    # Only exact text duplicates. Similar wording is not proof of the same action.
    return (_text(item.get("current")), _text(item.get("proposed")),
            tuple(sorted(_text(x) for x in item.get("spec_items") or [])))


def _version_key(value) -> str:
    match = re.fullmatch(r"v(\d+)", str(value or ""))
    return str(int(match[1])) if match else str(value or "")


def read_previous_bundles(versions: list[dict], current_version: str, reader: Callable) -> dict:
    """Read strictly earlier manifest versions, never an unspecified latest version."""
    def number(entry):
        match = re.fullmatch(r"v(\d+)", str(entry.get("version_id") or ""))
        try:
            return int(entry.get("version_no") or (match[1] if match else 0))
        except (TypeError, ValueError):
            return 0

    current = next((v for v in versions if v.get("version_id") == current_version), None)
    if current is None or not number(current):
        return {"bundles": [], "warnings": ["Не удалось определить порядок версий для истории оптимизаций."]}
    bundles, warnings = [], []
    seen = set()
    for entry in sorted(versions, key=number):
        vid = str(entry.get("version_id") or "")
        if not re.fullmatch(r"v\d+", vid) or not number(entry):
            warnings.append("Не удалось прочитать историю версии с неизвестным идентификатором или порядком.")
            continue
        if number(entry) >= number(current) or vid in seen:
            continue
        seen.add(vid)
        try:
            bundle = reader(vid)
            if bundle.get("error") or bundle.get("version_id") != vid:
                raise ValueError("Не удалось прочитать точную версию")
            if not isinstance(bundle.get("optimization"), dict) or not isinstance(bundle.get("expert_review"), dict):
                warnings.append(f"{vid}: неполные результаты оптимизации или экспертные решения; история может быть неполной.")
            else:
                item_ids = {str(i.get("id")) for i in bundle["optimization"].get("items") or [] if isinstance(i, dict)}
                if any(isinstance(d, dict) and d.get("item_type") == "optimization"
                       and d.get("decision") == "accepted" and str(d.get("item_id")) not in item_ids
                       for d in bundle["expert_review"].get("decisions") or []):
                    warnings.append(f"{vid}: для части принятых решений отсутствует текст предложения.")
            bundles.append(bundle)
        except Exception:
            warnings.append(f"{vid}: история оптимизаций недоступна; требуется проверка источника.")
    return {"bundles": bundles, "warnings": warnings}


def build_historical_ideas(project_id: str, project_name: str, current: dict,
                           rows: list[dict], matcher: Callable, *, object_id: str = "") -> list[dict]:
    """Keep provenance and compare historical ideas with current rows and decisions."""
    grouped: dict[tuple, dict] = {}
    current_version = str(current.get("version_id") or "")
    history = (current.get("optimization_history") or {}).get("bundles") or []
    for bundle in history:
        vid = str(bundle.get("version_id") or "")
        if not vid or vid == current_version:
            continue
        decisions = {str(d.get("item_id")): d for d in (bundle.get("expert_review") or {}).get("decisions") or []
                     if isinstance(d, dict) and d.get("item_type") == "optimization"}
        for item in (bundle.get("optimization") or {}).get("items") or []:
            if not isinstance(item, dict) or not item.get("id"):
                continue
            decision = decisions.get(str(item["id"]), {})
            if decision.get("decision") != "accepted":
                continue
            identity = _identity(item)
            # Empty proposals must never merge unrelated historical items.
            key = identity if identity[1] else (vid, str(item["id"]))
            idea = grouped.setdefault(key, {
                "history_id": "HIST-" + _hash([object_id, project_id, key])[:20],
                "object_id": object_id, "project_id": project_id, "project_name": project_name,
                "current_version_id": current_version, "current": item.get("current") or "",
                "proposed": item.get("proposed") or "", "spec_items": list(item.get("spec_items") or []),
                "origins": [],
            })
            idea["origins"].append({
                "source_ref": f"{project_id}:{vid}:{item['id']}", "version_id": vid,
                "item_id": item["id"], "item": dict(item), "decision": dict(decision),
                "evidence_fingerprint": _hash([item, decision]),
            })

    current_items = [i for i in (current.get("optimization") or {}).get("items") or [] if isinstance(i, dict)]
    decisions = {str(d.get("item_id")): d for d in (current.get("expert_review") or {}).get("decisions") or []
                 if isinstance(d, dict) and d.get("item_type") == "optimization"}
    for idea in grouped.values():
        matches = [dict(row) for row in rows
                   if row.get("project_id") == project_id and row.get("version_id") == current_version
                   and matcher(idea, row) > 0]
        origin_ids = {(_version_key(o["version_id"]), str(o["item_id"])) for o in idea["origins"]}
        current_matches = []
        for item in current_items:
            decision = decisions.get(str(item.get("id")), {})
            carried_from = (_version_key(decision.get("carried_from_version")), str(decision.get("carried_from_item_id") or ""))
            if (idea["proposed"] and _identity(idea) == _identity(item)) or carried_from in origin_ids:
                current_matches.append({"item_id": item.get("id"), "decision": dict(decision)})
        statuses = {m["decision"].get("decision") for m in current_matches}
        past_rejections = []
        for bundle in history:
            same_ids = {str(i.get("id")) for i in (bundle.get("optimization") or {}).get("items") or []
                        if isinstance(i, dict) and idea["proposed"] and _identity(i) == _identity(idea)}
            for decision in (bundle.get("expert_review") or {}).get("decisions") or []:
                if (isinstance(decision, dict) and decision.get("item_type") == "optimization"
                        and decision.get("decision") == "rejected" and str(decision.get("item_id")) in same_ids):
                    past_rejections.append({"version_id": bundle["version_id"], "decision": dict(decision)})
        if "rejected" in statuses:
            status, reason = "current_rejected", "В текущей версии найдено отклонение этой идеи. Причину необходимо учесть перед повторным рассмотрением."
        elif any(m["decision"].get("decision") == "accepted" and not m["decision"].get("carried_over") for m in current_matches):
            status, reason = "current_accepted", "Идея принята экспертом в текущей версии. Внедрение и эффект проверяются отдельно."
        elif past_rejections:
            status, reason = "previously_rejected", "В истории есть отклонение этой идеи. Причину необходимо учесть перед повторным рассмотрением."
        elif any(m["decision"].get("decision") == "accepted" for m in current_matches):
            status, reason = "carried_requires_review", "Принятие перенесено из прошлой версии; требуется проверка актуальных условий."
        elif not rows:
            status, reason = "insufficient_data", "Спецификация текущей версии недоступна или не разобрана."
        elif not matches:
            status, reason = "subject_not_matched", "Сопоставимые позиции не найдены. Это не подтверждает внедрение или исчезновение предмета изменения."
        else:
            status, reason = "requires_review", "Найдены похожие позиции текущей версии; применимость и текущие объёмы требуют проверки."
        idea.update({
            "status": status, "reason": reason, "current_rows": matches,
            "current_decisions": current_matches, "past_rejections": past_rejections, "implementation_status": "unknown",
            "confirmed_savings": None,
            "next_step": "Проверить по текущим документам: внедрено ли решение, сохранились ли условия, какие объёмы и доказательства нужны.",
        })
        idea["input_fingerprint"] = _hash(idea)
    return sorted(grouped.values(), key=lambda idea: idea["history_id"])
