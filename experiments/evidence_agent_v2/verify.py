"""EV2 verdict — детерминированная политика поверх K прогонов восприятия.

Голоса contradicts_finding (yes/no/cannot_tell) от K независимых vision-прогонов
агрегируются в 4-вердикт (accept/reject/borderline/needs_human) ЯВНОЙ политикой
на Python — её можно тюнить и аудировать, в отличие от вердикта, зашитого в
промпт (подход Cursor).

Консервативное смещение (главный принцип проекта — не удалять реальные замечания):
  - уверенный REJECT требует >= MIN_REJECT_VOTES согласных «yes» И большинства;
  - один-единственный «yes» (K=1 или 1 из K) НЕ даёт reject -> borderline;
  - при равенстве/раздрае -> borderline;
  - если доминирует cannot_tell / нечитаемо -> needs_human.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Optional

from .context import Context, load_context
from .extract import Perception, perceive_async

MIN_REJECT_VOTES = 2          # минимум согласных «yes» для уверенного reject
DEFAULT_RUNS = 2              # K прогонов восприятия (production); benchmark может 1


@dataclass
class Verdict:
    finding_id: str
    decision: str             # accept | reject | borderline | needs_human
    confidence: float
    verification_path: str    # graphic | text | weak | skipped
    reason: str               # короткое объяснение на русском
    votes: dict = field(default_factory=dict)   # {yes, no, cannot_tell}
    runs: int = 0
    model_used: str = ""
    block_ids_used: list = field(default_factory=list)
    perceptions: list = field(default_factory=list)


def _aggregate(fid: str, perceptions: list[Perception], path: str, block_ids: list) -> Verdict:
    valid = [p for p in perceptions if p.ok]
    yes = sum(1 for p in valid if p.contradicts == "yes")
    no = sum(1 for p in valid if p.contradicts == "no")
    cant = sum(1 for p in valid if p.contradicts == "cannot_tell")
    k = len(valid)
    model = next((p.model_used for p in perceptions if p.model_used), "")
    votes = {"yes": yes, "no": no, "cannot_tell": cant, "invalid": len(perceptions) - k}

    if k == 0:
        return Verdict(fid, "needs_human", 0.0, path,
                       "Модель не дала валидного ответа по блоку.", votes, len(perceptions),
                       model, block_ids, perceptions)

    # уверенный reject: согласных «yes» достаточно И они большинство
    if yes >= MIN_REJECT_VOTES and yes > no and yes >= cant:
        return Verdict(fid, "reject", round(yes / k, 2), path,
                       "Чертёж опровергает замечание (несколько прогонов согласны).",
                       votes, len(perceptions), model, block_ids, perceptions)

    # замечание подтверждается чертежом (строгое большинство «no»; ничья -> borderline)
    if no >= 1 and no > yes and no >= cant:
        conf = round(no / k, 2)
        return Verdict(fid, "accept", max(conf, 0.6), path,
                       "Чертёж подтверждает проблему из замечания.",
                       votes, len(perceptions), model, block_ids, perceptions)

    # один «yes» без поддержки -> не реджектим (консервативно)
    if yes >= 1 and yes < MIN_REJECT_VOTES and no == 0:
        return Verdict(fid, "borderline", round(yes / k, 2), path,
                       "Есть признак ошибки ИИ, но недостаточно согласия для отклонения.",
                       votes, len(perceptions), model, block_ids, perceptions)

    # доминирует «не видно»
    if cant >= yes and cant >= no:
        return Verdict(fid, "needs_human", round(cant / k, 2), path,
                       "По графическому блоку нельзя проверить — нужен эксперт.",
                       votes, len(perceptions), model, block_ids, perceptions)

    return Verdict(fid, "borderline", 0.5, path,
                   "Прогоны разошлись во мнении.",
                   votes, len(perceptions), model, block_ids, perceptions)


async def verify_graphic_async(
    ctx: Context, *, model: str, runs: int = DEFAULT_RUNS,
    extra_block_ids: Optional[list] = None,
) -> Verdict:
    fid = str(ctx.finding.get("id", "?"))
    block_ids = [b.block_id for b in ctx.blocks if b.png_path]

    # Фаза 4: коллаж основного блока + блоков-кандидатов кросс-блока (другие листы)
    extra_pngs, extra_ctx = [], ""
    if extra_block_ids:
        from .context import _find_block_png, _blocks_analysis_text, _load_json
        ba = _load_json(ctx.output_dir / "02_blocks_analysis.json") or {}
        ba_text = _blocks_analysis_text(ba)
        seen = {b.replace("block_", "") for b in block_ids}
        ctx_parts = []
        for ebid in extra_block_ids[:2]:
            nb = ebid.replace("block_", "")
            if nb in seen:
                continue
            p = _find_block_png(ctx.output_dir, ebid)
            if p:
                extra_pngs.append(p)
            t = ba_text.get(nb)
            if t:
                ctx_parts.append(f"[{nb}] {t[:400]}")
        extra_ctx = "\n".join(ctx_parts)

    perceptions = []
    for _ in range(max(1, runs)):
        perceptions.append(await perceive_async(
            ctx, model=model, extra_pngs=extra_pngs or None, extra_context=extra_ctx))
    return _aggregate(fid, perceptions, "graphic", block_ids)


def route(ctx: Context) -> str:
    has_img = bool(ctx.primary_png)
    has_text = bool(ctx.md_excerpt) or bool(ctx.text_block_ids)
    if has_img:
        return "graphic"
    if has_text:
        return "text"
    if ctx.grounding_level == "ungrounded":
        return "weak"
    return "text"


async def verify_finding_multi_async(
    project_id: str,
    finding: dict,
    *,
    section: str = "",
    version_id: Optional[str] = None,
    model: str,
    runs: int = DEFAULT_RUNS,
    use_norm: bool = True,
    use_cross_block: bool = True,
    ctx=None,
):
    """Async-ядро многоисточникового верификатора (норма+кросс-блок офлайн + зрение).

    Ранние выходы экономят vision. Инвариант: норм-сигнал не может породить reject.
    ctx можно передать готовым (аудит по точной версии через load_context_from_dir).
    """
    from .cross_block import run_cross_block
    from .fusion import fuse
    from .norm_check import run_norm_check

    fid = str(finding.get("id", "?"))
    if ctx is None:
        ctx = load_context(project_id, finding, version_id=version_id, section=section)
    if ctx is None:
        return fuse(None, None, None, finding_id=fid)

    norm_signal = run_norm_check(finding) if use_norm else None
    cross_block = run_cross_block(finding, ctx.graph) if use_cross_block else None
    xb_kind = getattr(cross_block, "kind", "none") if cross_block else "none"
    norm_hint = getattr(norm_signal, "decision_hint", "none") if norm_signal else "none"

    # --- ранние выходы без зрения ---
    if norm_hint == "accept_with_flag" and xb_kind != "xref_refutes":
        return fuse(None, norm_signal, cross_block, finding_id=fid)
    if xb_kind == "xref_supports":
        return fuse(None, norm_signal, cross_block, finding_id=fid)
    if route(ctx) != "graphic" or not ctx.primary_png:
        return fuse(None, norm_signal, cross_block, finding_id=fid)

    # --- дорогой визуал (Фаза 4: коллаж с блоками-кандидатами) ---
    extra = getattr(cross_block, "candidate_block_ids", None) if cross_block else None
    visual = await verify_graphic_async(ctx, model=model, runs=runs, extra_block_ids=extra)
    return fuse(visual, norm_signal, cross_block, finding_id=fid)


def verify_finding_multi(project_id: str, finding: dict, **kw):
    """Sync-обёртка для standalone-использования (НЕ внутри event loop)."""
    return asyncio.run(verify_finding_multi_async(project_id, finding, **kw))


def verify_finding(
    project_id: str,
    finding: dict,
    *,
    section: str = "",
    version_id: Optional[str] = None,
    model: str,
    runs: int = DEFAULT_RUNS,
) -> Verdict:
    fid = str(finding.get("id", "?"))
    ctx = load_context(project_id, finding, version_id=version_id, section=section)
    if ctx is None:
        return Verdict(fid, "needs_human", 0.0, "weak", "Не разрешился output_dir проекта.")
    path = route(ctx)
    if path == "graphic":
        return asyncio.run(verify_graphic_async(ctx, model=model, runs=runs))
    # текстовый/слабый путь EV2 пока помечает needs_human (фокус версии — графика)
    return Verdict(fid, "needs_human", 0.0, path,
                   "EV2 v1 проверяет только графические блоки; текст — на эксперта.")
