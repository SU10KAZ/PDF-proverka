"""Аудит отклонённых замечаний — «где ошибся эксперт» (read-only на live-данные).

Источник — per-version expert_review.json (version-корректный): пара
(finding ТОЙ версии ↔ rejection_reason) без F-ID-дрейфа. Никаких записей в live.

«Эксперт мог ошибиться» = верификатор уверенно говорит accept (замечание реальное)
на ОТКЛОНЁННОМ замечании, с конкретным доказательством. Это КАНДИДАТЫ на
человеческую перепроверку, не вердикты.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, Optional

ROOT = Path(__file__).resolve().parents[2]
ALIA_BASE = ROOT / "projects_v2" / "objects" / "214_Alia_ASTERUS" / "disciplines"


@dataclass
class RejRecord:
    discipline: str
    document: str
    version: str
    version_dir: Path
    output_dir: Path
    item_id: str
    rejection_reason: str
    timestamp: str = ""


def _output_dir_for(version_dir: Path) -> Optional[Path]:
    cand = version_dir / "03_analysis" / "latest"
    if cand.is_dir():
        return cand
    legacy = version_dir / "_output"
    return legacy if legacy.is_dir() else None


def iter_alia_rejected(discipline: Optional[str] = None) -> Iterator[RejRecord]:
    """Перебрать все отклонённые finding-замечания Алии из per-version expert_review."""
    disc_glob = discipline or "*"
    pattern = f"{disc_glob}/documents/*/versions/*/04_review/expert_review.json"
    for rev_path in sorted(ALIA_BASE.glob(pattern)):
        try:
            data = json.loads(rev_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        items = (data.get("reviews") or data.get("decisions") or data.get("items")
                 or (data if isinstance(data, list) else []))
        parts = rev_path.parts
        try:
            disc = parts[parts.index("disciplines") + 1]
            doc = parts[parts.index("documents") + 1]
            version = parts[parts.index("versions") + 1]
        except (ValueError, IndexError):
            continue
        version_dir = rev_path.parent.parent  # .../versions/vNNN
        output_dir = _output_dir_for(version_dir)
        if output_dir is None:
            continue
        for it in items:
            if not isinstance(it, dict):
                continue
            if str(it.get("decision") or it.get("expert_decision") or "").lower() != "rejected":
                continue
            if (it.get("item_type") or "finding") != "finding":
                continue
            yield RejRecord(
                discipline=disc, document=doc, version=version,
                version_dir=version_dir, output_dir=output_dir,
                item_id=str(it.get("item_id") or ""),
                rejection_reason=str(it.get("rejection_reason") or it.get("expert_reason") or ""),
                timestamp=str(it.get("timestamp") or ""),
            )


_findings_cache: dict = {}


def load_version_finding(output_dir: Path, item_id: str) -> Optional[dict]:
    """finding из 03_findings.json ИМЕННО этой версии (по item_id)."""
    key = str(output_dir)
    if key not in _findings_cache:
        fmap = {}
        for fname in ("03a_norms_verified.json", "03_findings.json"):
            p = output_dir / fname
            if p.is_file():
                try:
                    data = json.loads(p.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    continue
                for it in data.get("findings", data.get("items", [])):
                    if isinstance(it, dict) and it.get("id"):
                        fmap[str(it["id"])] = it
                if fmap:
                    break
        _findings_cache[key] = fmap
    return _findings_cache[key].get(item_id)


def _toks(s: str) -> set:
    return {w for w in re.sub(r"[^0-9a-zа-яё]+", " ", (s or "").lower()).split() if len(w) > 3}


# --- Фаза A: офлайн-триаж ---
def triage_offline(rec: RejRecord) -> dict:
    """Категоризировать отклонённое замечание офлайн (норма+кросс-блок), без нейросети."""
    from .context import load_context_from_dir
    from .norm_check import run_norm_check
    from .cross_block import run_cross_block

    finding = load_version_finding(rec.output_dir, rec.item_id)
    base = {"discipline": rec.discipline, "document": rec.document, "version": rec.version,
            "item_id": rec.item_id, "rejection_reason": rec.rejection_reason[:200]}
    if not finding:
        return {**base, "category": "no_finding", "reason": "finding по item_id в версии не найден"}

    finding = {**finding, "id": rec.item_id}
    base["problem"] = (finding.get("problem") or finding.get("description") or "")[:200]
    # мягкий сигнал дрейфа: пересечение rejection_reason с текстом finding
    base["reason_overlap"] = len(_toks(rec.rejection_reason) & _toks(base["problem"]))

    ctx = load_context_from_dir(rec.output_dir, finding, section=rec.discipline)
    norm = run_norm_check(finding)
    xb = run_cross_block(finding, ctx.graph if ctx else {})
    base["norm_kind"] = norm.kind
    base["xb_kind"] = xb.kind
    base["has_png"] = bool(ctx and ctx.has_png)

    if xb.kind == "xref_supports":
        cat = "offline_accept_candidate"   # смежный блок подтверждает проблему → сильный сигнал
    elif xb.kind == "xref_refutes":
        cat = "info_elsewhere_supported"   # отсутствующее реально есть в др. блоке → эксперт скорее прав
    elif norm.decision_hint == "accept_with_flag":
        cat = "offline_norm_superseded"    # норма заменена/устарела — НЕ обязательно ошибка эксперта
    elif ctx and ctx.has_png:
        cat = "needs_vision"
    else:
        cat = "text_only"                   # нет графики → EV2-зрение не применимо
    base["category"] = cat
    base["candidate_block_ids"] = xb.candidate_block_ids[:3]
    return base


# --- Фаза B: vision-аудит ---
def is_expert_error(fused) -> tuple[bool, str]:
    """(флаг, причина). Сильный сигнал «эксперт мог ошибиться»:
    верификатор ПОДТВЕРДИЛ замечание по чертежу/смежному блоку, или конфликт источников.
    norm_flag (норма заменена) сюда НЕ входит — это «норма устарела», не «эксперт неправ».
    """
    dec = getattr(fused, "decision", "")
    src = getattr(fused, "source", "")
    if dec == "accept" and src in ("visual_confirm", "cross_block_supports"):
        return True, f"верификатор подтвердил замечание по {('чертежу' if src=='visual_confirm' else 'смежному блоку')}"
    if src == "conflict":
        return True, "конфликт источников (что-то подтверждает замечание) — на перепроверку"
    return False, ""


_AUDIT_PROMPT = """Ты эксперт по строительной проектной документации. ИИ выдвинул замечание по чертежу, а ЧЕЛОВЕК-ЭКСПЕРТ его ОТКЛОНИЛ со своим обоснованием. Твоя задача — проверить, не ошибся ли ЭКСПЕРТ.

Замечание ИИ:
{problem}

Обоснование, по которому ЭКСПЕРТ отклонил замечание:
{reason}

OCR/описание блока:
{ocr}

Смотри на чертёж и оцени КОНКРЕТНО обоснование эксперта. Эксперт мог ошибиться ТОЛЬКО если на чертеже есть прямое доказательство, ОПРОВЕРГАЮЩЕЕ его обоснование (например: эксперт says «значение верное 900мм», а на чертеже видно «250мм»; эксперт says «есть в ведомости», а в ведомости этого нет).

ВАЖНО:
- Если обоснование эксперта согласуется с чертежом или правдоподобно — эксперт ПРАВ (expert_wrong="no").
- Если по чертежу нельзя проверить обоснование (нет нужного фрагмента, нормативное/контекстное суждение) — expert_wrong="cannot_tell".
- expert_wrong="yes" ТОЛЬКО при наличии дословной цитаты с чертежа, ПРЯМО противоречащей обоснованию эксперта.
- НЕ повторяй замечание ИИ; оценивай именно ОБОСНОВАНИЕ ЭКСПЕРТА.

Ответь ТОЛЬКО одним JSON-объектом:
{{"expert_reason_checkable":true,"contradicting_quote":"дословная цитата с чертежа или пусто","expert_wrong":"yes|no|cannot_tell","explanation":"кратко на русском"}}
"""


async def audit_vision_reasonaware_async(rec: RejRecord, *, model: str) -> Optional[dict]:
    """Reason-aware аудит: проверяет, опровергает ли чертёж ОБОСНОВАНИЕ эксперта.

    Высокая точность: «эксперт ошибся» только при дословной цитате, противоречащей причине.
    """
    import json as _json
    from pathlib import Path as _P
    from .context import load_context_from_dir
    from .extract import _parse

    finding = load_version_finding(rec.output_dir, rec.item_id)
    if not finding:
        return None
    finding = {**finding, "id": rec.item_id}
    ctx = load_context_from_dir(rec.output_dir, finding, section=rec.discipline)
    if not ctx or not ctx.primary_png:
        return None

    from backend.app.services.common.local_vision_provider import describe_image_local
    ocr = "\n".join(f"{b.block_id}: {b.gemma_text[:1500]}" for b in ctx.blocks if b.gemma_text) or "(нет OCR)"
    prompt = _AUDIT_PROMPT.format(
        problem=(finding.get("problem") or finding.get("description") or "")[:600],
        reason=rec.rejection_reason[:600], ocr=ocr)
    try:
        res = await describe_image_local(_P(ctx.primary_png), prompt, model=model)
    except Exception as exc:
        return {"item_id": rec.item_id, "document": rec.document, "error": str(exc)}
    raw = (res.full_raw_response or res.raw_response_excerpt or "").strip()
    obj = _parse(raw) or {}
    verdict = str(obj.get("expert_wrong", "cannot_tell")).strip().lower()
    quote = str(obj.get("contradicting_quote", "")).strip()
    # GUARD: «yes» только с непустой цитатой
    expert_wrong = verdict == "yes" and bool(quote)
    return {
        "discipline": rec.discipline, "document": rec.document, "version": rec.version,
        "item_id": rec.item_id,
        "problem": (finding.get("problem") or finding.get("description") or "")[:300],
        "rejection_reason": rec.rejection_reason[:300],
        "expert_wrong_verdict": verdict,
        "contradicting_quote": quote[:200],
        "explanation": str(obj.get("explanation", ""))[:300],
        "expert_maybe_wrong": expert_wrong,
        "why_flag": ("чертёж прямо опровергает обоснование эксперта" if expert_wrong else ""),
    }


async def audit_vision_async(rec: RejRecord, *, model: str, runs: int = 1) -> Optional[dict]:
    """Прогнать многоисточниковый верификатор на отклонённом замечании (с vision)."""
    from .context import load_context_from_dir
    from .verify import verify_finding_multi_async

    finding = load_version_finding(rec.output_dir, rec.item_id)
    if not finding:
        return None
    finding = {**finding, "id": rec.item_id}
    ctx = load_context_from_dir(rec.output_dir, finding, section=rec.discipline)
    fused = await verify_finding_multi_async(
        rec.document, finding, section=rec.discipline, model=model, runs=runs, ctx=ctx)
    err, why = is_expert_error(fused)
    return {
        "discipline": rec.discipline, "document": rec.document, "version": rec.version,
        "item_id": rec.item_id,
        "problem": (finding.get("problem") or finding.get("description") or "")[:300],
        "rejection_reason": rec.rejection_reason[:300],
        "verifier_decision": fused.decision, "source": fused.source,
        "confidence": fused.confidence, "taxonomy": fused.taxonomy,
        "norm_flags": fused.norm_flags, "reason": fused.reason,
        "evidence_quote": fused.evidence_quote,
        "expert_maybe_wrong": err, "why_flag": why,
    }
