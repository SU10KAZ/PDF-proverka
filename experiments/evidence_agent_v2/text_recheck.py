"""Прогон ТЕКСТОВЫХ замечаний (без графики) reason-aware на локальной qwen.

Для 1265 `text_only` отклонённых: модель оценивает ОБОСНОВАНИЕ эксперта по нормам/тексту
(«опровергает ли документ/норма причину эксперта»), а не сам finding. Модель —
qwen/qwen3.6-35b-a3b (бесплатно, JSON 3/3 на смоуке). Read-only на live, 0 токенов подписки.

CLI:
  python -m experiments.evidence_agent_v2.text_recheck --mode all --disciplines EOM
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from .audit_rejected import iter_alia_rejected, triage_offline, load_version_finding

OUT_DIR = Path(__file__).resolve().parent / "results" / "audit_alia"

_SCHEMA = {"type": "json_schema", "json_schema": {"name": "verdict", "strict": True, "schema": {
    "type": "object", "additionalProperties": False,
    "properties": {"expert_wrong": {"type": "string", "enum": ["yes", "no", "cannot_tell"]},
                   "key_point": {"type": "string"}, "explanation": {"type": "string"}},
    "required": ["expert_wrong", "key_point", "explanation"]}}}

_SYS = "Ты эксперт по строительной проектной документации РФ. Отвечай строго одним JSON-объектом по схеме."


def _text_context(finding: dict) -> str:
    """Собрать текстовые свидетельства finding'а (без графики)."""
    parts = []
    for e in (finding.get("evidence") or []):
        if isinstance(e, dict) and (e.get("type") in (None, "text")):
            for k in ("text", "quote", "snippet", "content", "md_excerpt"):
                v = e.get(k)
                if isinstance(v, str) and v.strip():
                    parts.append(v.strip())
    for k in ("evidence_text", "md_excerpt", "context"):
        v = finding.get(k)
        if isinstance(v, str) and v.strip():
            parts.append(v.strip())
    seen, uniq = set(), []
    for p in parts:
        if p[:200] not in seen:
            seen.add(p[:200]); uniq.append(p)
    return "\n---\n".join(uniq)[:1800] or "(текст из документа не приложен к замечанию)"


def _prompt(finding: dict, reason: str) -> str:
    return (
        "ИИ выдвинул замечание, а ЧЕЛОВЕК-ЭКСПЕРТ его ОТКЛОНИЛ. Проверь, не ошибся ли ЭКСПЕРТ.\n\n"
        f"Замечание ИИ:\n{(finding.get('problem') or finding.get('description') or '')[:700]}\n\n"
        f"Норма (если указана): {str(finding.get('norm') or finding.get('normative_ref') or '—')[:300]}\n\n"
        f"Текст из документа / контекст замечания:\n{_text_context(finding)}\n\n"
        f"Обоснование, по которому эксперт отклонил:\n{reason[:700]}\n\n"
        "Оцени ОБОСНОВАНИЕ эксперта по нормам и тексту документа.\n"
        "expert_wrong=\"yes\" — только при конкретном нормативном/фактическом основании ПРОТИВ "
        "обоснования эксперта; \"no\" — если эксперт прав/правдоподобен; \"cannot_tell\" — если по "
        "тексту не проверить.\n"
        "КРИТИЧЕСКИ ВАЖНО: если эксперт ссылается на ОБЩИЕ УКАЗАНИЯ, другие листы/разделы, "
        "спецификации/ведомости, или что вопрос ПРИНЯТ ЭКСПЕРТИЗОЙ / является ФОРМАЛЬНЫМ "
        "некритичным/редакционным недочётом, или сделал обоснованное инженерное СУЖДЕНИЕ — это "
        "НЕ ошибка эксперта (expert_wrong=\"no\" или \"cannot_tell\", НЕ \"yes\"). Технической правоты "
        "по букве нормы НЕДОСТАТОЧНО, чтобы объявить эксперта ошибшимся, если его решение разумно.\n"
        "key_point — ключевой довод. Ответь ТОЛЬКО JSON."
    )


async def recheck_text_one(rec, *, model: str) -> Optional[dict]:
    from backend.app.services.llm import llm_runner as lr
    from .norm_check import run_norm_check

    finding = load_version_finding(rec.output_dir, rec.item_id)
    if not finding:
        return None
    finding = {**finding, "id": rec.item_id}
    msgs = [{"role": "system", "content": _SYS},
            {"role": "user", "content": _prompt(finding, rec.rejection_reason)}]
    try:
        r = await lr._run_local_chat_completions(
            model=model, messages=msgs, max_tokens=900, temperature=0.0,
            timeout=120, response_format=_SCHEMA, _allow_context_reload=True)
    except Exception as exc:
        return {"item_id": rec.item_id, "document": rec.document, "error": str(exc)[:120]}
    txt = r.text or ""
    try:
        obj = json.loads(txt)
    except Exception:
        obj = r.json_data if not r.is_error else None
    obj = obj if isinstance(obj, dict) else {}
    verdict = str(obj.get("expert_wrong", "cannot_tell")).strip().lower()
    key_point = str(obj.get("key_point", "")).strip()
    # офлайн норм-чек (пометка «норма заменена/устарела»)
    try:
        norm = run_norm_check(finding)
        norm_flag = norm.decision_hint if norm.decision_hint in ("accept_with_flag", "soft_human") else ""
    except Exception:
        norm_flag = ""
    # GUARD: «yes» только при непустом key_point
    expert_wrong = verdict == "yes" and bool(key_point)
    return {
        "discipline": rec.discipline, "document": rec.document, "version": rec.version,
        "item_id": rec.item_id,
        "problem": (finding.get("problem") or finding.get("description") or "")[:300],
        "norm": str(finding.get("norm") or finding.get("normative_ref") or "")[:200],
        "rejection_reason": rec.rejection_reason[:300],
        "expert_wrong_verdict": verdict, "key_point": key_point[:200],
        "explanation": str(obj.get("explanation", ""))[:300],
        "norm_flag": norm_flag,
        "expert_maybe_wrong": expert_wrong,
        "json_error": bool(r.is_error),
    }


async def _run_async(disciplines, *, model: str, limit: int = 0) -> int:
    import time
    from collections import Counter
    from . import ngrok_guard

    # собрать text_only кейсы
    recs = []
    for disc in disciplines:
        for r in iter_alia_rejected(disc):
            if triage_offline(r).get("category") == "text_only":
                recs.append(r)
    if limit:
        recs = recs[:limit]
    print(f"[text] к прогону text_only: {len(recs)} ({disciplines})", flush=True)

    ngrok_guard.preflight(require_idle=False)
    results = []
    with ngrok_guard.LocalLLMLock(owner="ev2", note="text_recheck"):
        t0 = time.time()
        for i, r in enumerate(recs, 1):
            try:
                res = await recheck_text_one(r, model=model)
            except Exception as exc:
                res = {"item_id": r.item_id, "document": r.document, "error": str(exc)[:120]}
            if res:
                results.append(res)
            if i % 20 == 0:
                kept = sum(1 for x in results if x.get("expert_maybe_wrong"))
                print(f"  …{i}/{len(recs)} ({time.time()-t0:.0f}с) кандидатов={kept}", flush=True)

    verdicts = Counter(x.get("expert_wrong_verdict") for x in results if "expert_wrong_verdict" in x)
    errs = sum(1 for x in results if x.get("error"))
    out = OUT_DIR / f"visionTEXT_{'_'.join(disciplines)}.json"
    out.write_text(json.dumps({
        "disciplines": disciplines, "model": model, "rechecked": len(results), "errors": errs,
        "verdicts": dict(verdicts),
        "candidates": [x for x in results if x.get("expert_maybe_wrong")],
        "all": results,
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n[text] прогнано {len(results)} | ошибок {errs} | вердикты {dict(verdicts)}")
    print(f"[text] кандидатов 'эксперт ошибся': {verdicts.get('yes',0)}")
    print(f"[text] отчёт: {out}")
    return 0


if __name__ == "__main__":
    import argparse, asyncio, sys
    ROOT = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(ROOT))
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    ap = argparse.ArgumentParser()
    ap.add_argument("--disciplines", default="EOM")
    ap.add_argument("--mode", choices=("all",), default="all")
    ap.add_argument("--model", default="qwen/qwen3.6-35b-a3b")
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()
    raise SystemExit(asyncio.run(_run_async(
        [d.strip() for d in a.disciplines.split(",") if d.strip()], model=a.model, limit=a.limit)))
