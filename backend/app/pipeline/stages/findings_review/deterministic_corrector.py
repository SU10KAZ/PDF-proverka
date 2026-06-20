"""
Детерминированный corrector замечаний.

Зачем
-----
Corrector раньше был агентным `claude -p`, который читал
`03_findings.json` + `03_findings_review.json` + `02_blocks_analysis.json` +
`document_graph.json` и **переписывал** `03_findings.json` целиком. На крупных
проектах это нестабильно ровно так же, как агентный critic (таймаут/лимит
ходов), а главное — рискованно: агент перезаписывает мастер-файл замечаний.

Решение
-------
По вердиктам critic (см. docs/critic_corrector.md) применяем **детерминированные
консервативные** действия. Главный инвариант: **ни одно замечание не
удаляется** — потеря замечания недопустима, поэтому destructive-вариант
("удалить") заменён на понижение severity в `ПРОВЕРИТЬ_ПО_СМЕЖНЫМ`.

| Вердикт | Действие |
|---|---|
| `phantom_block` | удалить несуществующие block_id из evidence/related/source |
| `page_mismatch` | выставить page/sheet по страницам реальных evidence-блоков |
| `no_evidence` | критич./эконом. → `requires_human_review` (severity сохраняется); прочие → понизить в `ПРОВЕРИТЬ_ПО_СМЕЖНЫМ` |
| `contradicts_text` | то же правило (#31): критич./эконом. на ручную проверку, прочие — понизить |
| `weak_evidence` | оставить + пометка `corrector_note` (мягкий LLM-сигнал) |

Каждому исправленному замечанию проставляется `corrector_note` и
`corrected_by="deterministic"`. `norm_quote` и прочие поля сохраняются.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Awaitable, Callable, Optional

from backend.app.pipeline.stages.findings_review.deterministic_critic import (
    _Index,
    _referenced_block_ids,
    build_index,
    iter_findings,
)

logger = logging.getLogger(__name__)

CROSS_CHECK_SEVERITY = "ПРОВЕРИТЬ_ПО_СМЕЖНЫМ"
_DOWNGRADE_VERDICTS = {"no_evidence", "contradicts_text"}
# reserc.md #31: критичные/экономические замечания НЕ понижаем молча при
# no_evidence/contradicts_text — потеря такого замечания недопустима. Их
# помечаем requires_human_review и оставляем severity как есть.
_PROTECTED_SEVERITIES = {"КРИТИЧЕСКОЕ", "ЭКОНОМИЧЕСКОЕ"}


@dataclass
class DeterministicCorrectorResult:
    findings_total: int = 0
    corrected: int = 0
    phantom_cleaned: int = 0
    page_fixed: int = 0
    downgraded: int = 0
    flagged_human: int = 0
    notes_added: int = 0
    error: Optional[str] = None


def _verdict_map(review_data) -> dict:
    """{finding_id: review_dict} по 03_findings_review.json."""
    reviews = []
    if isinstance(review_data, dict):
        reviews = review_data.get("reviews") or review_data.get("verdicts") or []
    elif isinstance(review_data, list):
        reviews = review_data
    out = {}
    for r in reviews:
        if not isinstance(r, dict):
            continue
        fid = r.get("finding_id") or r.get("id")
        if fid is not None:
            out[str(fid)] = r
    return out


def _remove_phantom_blocks(finding: dict, idx: _Index) -> int:
    """Убрать несуществующие block_id из evidence/related/source. Возвращает
    число удалённых ссылок."""
    removed = 0

    def _exists_image(bid) -> bool:
        return str(bid) in idx.image_block_ids

    def _exists_text(bid) -> bool:
        b = str(bid)
        return b in idx.text_block_ids or b.endswith("_text")

    for key in ("related_block_ids", "source_block_ids"):
        lst = finding.get(key)
        if isinstance(lst, list):
            kept = [b for b in lst if _exists_image(b)]
            removed += len(lst) - len(kept)
            finding[key] = kept

    ev = finding.get("evidence")
    if isinstance(ev, list):
        kept_ev = []
        for e in ev:
            if not isinstance(e, dict):
                kept_ev.append(e)
                continue
            bid = e.get("block_id")
            if bid is None:
                kept_ev.append(e)
                continue
            etype = (e.get("type") or "").lower()
            ok = _exists_text(bid) if etype == "text" else _exists_image(bid)
            if ok:
                kept_ev.append(e)
            else:
                removed += 1
        finding["evidence"] = kept_ev

    refs = finding.get("evidence_text_refs")
    if isinstance(refs, list):
        kept_refs = []
        for ref in refs:
            bid = ref.get("block_id") if isinstance(ref, dict) else ref
            if bid is None or _exists_text(bid):
                kept_refs.append(ref)
            else:
                removed += 1
        finding["evidence_text_refs"] = kept_refs

    return removed


def _fix_page_sheet(finding: dict, idx: _Index) -> bool:
    """Выставить page/sheet по страницам реальных evidence-блоков."""
    image_refs, _text_refs, ev_pages = _referenced_block_ids(finding)
    pages = set(ev_pages)
    for b in image_refs:
        if b in idx.image_block_ids and b in idx.block_page:
            pages.add(idx.block_page[b])
    pages_sorted = sorted(p for p in pages if p is not None)
    if not pages_sorted:
        return False
    first = pages_sorted[0]
    finding["page"] = first if len(pages_sorted) == 1 else pages_sorted
    # sheet: сначала document_graph (page→sheet), затем sheet самого evidence-блока
    sheet = idx.page_to_sheet.get(first)
    if not sheet:
        for b in image_refs:
            if idx.block_page.get(b) == first and idx.block_sheet.get(b):
                sheet = idx.block_sheet[b]
                break
    if sheet:
        finding["sheet"] = sheet
    return True


def _severity_norm(finding: dict) -> str:
    return str(finding.get("severity") or "").strip().upper()


def _downgrade_or_flag(finding: dict) -> str:
    """no_evidence/contradicts_text (reserc.md #31).

    Критичные/экономические замечания НЕ понижаем молча — помечаем
    requires_human_review и сохраняем severity (инженер должен сам решить).
    Остальные — понижаем в ПРОВЕРИТЬ_ПО_СМЕЖНЫМ как раньше.
    Возвращает 'flagged' | 'downgraded'.
    """
    if _severity_norm(finding) in _PROTECTED_SEVERITIES:
        finding["requires_human_review"] = True
        return "flagged"
    finding["severity"] = CROSS_CHECK_SEVERITY
    return "downgraded"


def correct_findings(findings_data, review_data, blocks_analysis, doc_graph):
    """Применить детерминированные корректировки in-place. Возвращает
    (findings_data, result)."""
    idx = build_index(blocks_analysis, doc_graph)
    verdicts = _verdict_map(review_data)
    findings = iter_findings(findings_data)
    result = DeterministicCorrectorResult(findings_total=len(findings))

    for finding in findings:
        fid = finding.get("id") or finding.get("finding_id")
        rev = verdicts.get(str(fid))
        if not rev:
            continue
        verdict = rev.get("verdict", "pass")
        if verdict == "pass":
            continue

        changed = False
        reason = rev.get("reason") or verdict

        if verdict == "phantom_block":
            n = _remove_phantom_blocks(finding, idx)
            if n:
                result.phantom_cleaned += 1
                changed = True
            # если evidence не осталось — это уже no_evidence → понижаем
            # (критичные/экономические — помечаем на ручную проверку, #31)
            img, txt, _ = _referenced_block_ids(finding)
            if not img and not txt:
                if _downgrade_or_flag(finding) == "downgraded":
                    result.downgraded += 1
                else:
                    result.flagged_human += 1
                changed = True
        elif verdict == "page_mismatch":
            if _fix_page_sheet(finding, idx):
                result.page_fixed += 1
                changed = True
        elif verdict in _DOWNGRADE_VERDICTS:
            if _downgrade_or_flag(finding) == "downgraded":
                result.downgraded += 1
            else:
                result.flagged_human += 1
            changed = True
        elif verdict == "weak_evidence":
            changed = True  # только пометка ниже

        finding["corrector_note"] = f"[{verdict}] {reason}"
        finding["corrected_by"] = "deterministic"
        result.notes_added += 1
        if changed:
            result.corrected += 1

    return findings_data, result


async def run_deterministic_corrector(
    output_dir: Path,
    *,
    project_id: str = "",
    findings_filename: str = "03_findings.json",
    review_filename: str = "03_findings_review.json",
    on_log: Optional[Callable[[str], Awaitable[None]]] = None,
    write: bool = True,
) -> DeterministicCorrectorResult:
    """Прочитать findings + review, применить корректировки, записать findings."""
    output_dir = Path(output_dir)
    findings_data = _load_json(output_dir / findings_filename)
    if findings_data is None:
        return DeterministicCorrectorResult(error=f"{findings_filename} не найден/невалиден")
    review_data = _load_json(output_dir / review_filename)
    if review_data is None:
        return DeterministicCorrectorResult(error=f"{review_filename} не найден/невалиден")

    blocks_analysis = _load_json(output_dir / "02_blocks_analysis.json") or {}
    doc_graph = _load_json(output_dir / "document_graph.json") or {}

    findings_data, result = correct_findings(
        findings_data, review_data, blocks_analysis, doc_graph,
    )

    if write:
        (output_dir / findings_filename).write_text(
            json.dumps(findings_data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    if on_log:
        await on_log(
            f"Corrector готов: исправлено {result.corrected}/{result.findings_total} "
            f"(phantom: {result.phantom_cleaned}, page: {result.page_fixed}, "
            f"понижено: {result.downgraded}, на ручную проверку: {result.flagged_human})"
        )
    return result


def _load_json(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("deterministic_corrector: не прочитан %s: %s", path, exc)
        return None
