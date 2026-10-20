"""Поддельные исполнители для сухого прогона стенда. Ни одного вызова модели.

Ответы синтетические и помечены `_stand_fake: true`. Они нужны, чтобы прогнать
настоящую оркестровку конвейера (сборку промпта, объединение, дедупликацию,
верификатор, этап норм) и сопоставление/оценку. Как результат эксперимента
они не используются.

Поддельный docnorm строится из «затравки» — текстов известных замечаний
docnorm этой версии (снимок из журнала наблюдений): плечо A возвращает около
80 % затравки (детерминированно по номеру повтора), плечо B ту же часть, но
формальные по сути замечания переносит в `formal_notes`. Плюс одно
синтетическое новое замечание, чтобы проверить учёт новых находок.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any

from backend.app.services.algorithm_improvement import docnorm_match as M

FORMAL_ESSENCES = {"title", "changes", "duplicate", "unconfirmed", "not_profile", "doc_identity"}


def _keep(seed_id: str, repeat: int, share: int = 80) -> bool:
    digest = hashlib.sha256(f"{seed_id}|{repeat}".encode("utf-8")).hexdigest()
    return int(digest[:4], 16) % 100 < share


def fake_docnorm_payload(seeds: list[dict], *, arm: str, repeat: int) -> dict[str, Any]:
    findings, formal = [], []
    for n, seed in enumerate(seeds, start=1):
        if not _keep(str(seed.get("obs_id") or n), repeat):
            continue
        item = {
            "id": f"TGT-{n:03d}",
            "severity": seed.get("severity") or "РЕКОМЕНДАТЕЛЬНОЕ",
            "category": seed.get("category") or "normative_refs",
            "problem": seed.get("problem") or "",
            "description": seed.get("description") or "",
            "page": seed.get("page"),
            "norm": "",
            "norm_quote": None,
            "solution": "FAKE",
            "risk": "FAKE",
            "evidence": [{"type": "text", "block_id": None, "page": seed.get("page"), "md_lines": "1-1"}],
            "_stand_fake": True,
        }
        if arm == "B":
            essence = M.essence(f"{item['problem']} {item['description']}")
            if essence in FORMAL_ESSENCES:
                formal.append({"norm": ",".join(M.features(item)["norms"]), "issue": essence,
                               "significance": "formal", "reason": "FAKE", "md_lines": "1-1"})
                continue
            item["significance_basis"] = "a"
            item["requirement_quote"] = "FAKE"
        findings.append(item)
    findings.append({
        "id": f"TGT-{len(seeds) + 1:03d}", "severity": "РЕКОМЕНДАТЕЛЬНОЕ", "category": "normative_refs",
        "problem": f"FAKE новое: в ведомости ссылочных документов указан отменённый ГОСТ 99999-{repeat:02d} на плитку.",
        "description": "FAKE синтетическое замечание стенда", "page": None, "norm": "", "norm_quote": None,
        "solution": "FAKE", "risk": "FAKE", "evidence": [], "_stand_fake": True,
    })
    payload: dict[str, Any] = {"findings": findings}
    if arm == "B":
        payload["formal_notes"] = formal
    return payload


def fake_absence_verdicts(candidates: list[dict]) -> dict:
    """Каждый пятый кандидат «present» (понижение), остальные «absent»."""
    return {i: ("present" if i % 5 == 4 else "absent") for i in range(len(candidates))}


def fake_norm_fix_apply(findings_path, findings_to_fix_text: str) -> int:
    """Контракт norm_fix: обновить существующие замечания, не добавляя и не удаляя."""
    data = json.loads(findings_path.read_text(encoding="utf-8"))
    ids = set(re.findall(r"\bF-\d{3,}\b", findings_to_fix_text or ""))
    changed = 0
    for item in data.get("findings", []):
        if item.get("id") in ids:
            item["norm_verified"] = True
            item["norm_status"] = "revised"
            item["norm_revision"] = {"original_norm": item.get("norm"), "revised_norm": item.get("norm"),
                                     "original_text": None, "revised_text": None,
                                     "revision_reason": "FAKE стенд: сухой прогон"}
            changed += 1
    findings_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return changed
