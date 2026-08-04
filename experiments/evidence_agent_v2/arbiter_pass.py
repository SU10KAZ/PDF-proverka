"""Финальный арбитр-проход по шорт-листу через Claude (claude -p, поодиночке).

Эксперименты показали: только ПООДИНОЧКЕ надёжно (батч/одна-сессия плывут), и Sonnet
заметно консервативнее Haiku (меньше ложных обвинений эксперта). Поэтому финал — Sonnet,
по одному кандидату на вызов. claude -p = токены подписки (не ngrok). Read-only на live.

Крэш-устойчиво: каждый вердикт сразу пишется в arbiter_<model>_progress.jsonl (resume по нему).

  python -m experiments.evidence_agent_v2.arbiter_pass --model sonnet
"""
from __future__ import annotations

import json
import re
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = Path(__file__).resolve().parent / "results" / "audit_alia"

try:
    import sys
    sys.path.insert(0, str(ROOT))
    from backend.app.core.config import get_claude_cli
    CLI = get_claude_cli()
except Exception:
    CLI = "/home/coder/.local/bin/claude"

_RULES = (
    "Правила (КОНСЕРВАТИВНО):\n"
    "- expert_wrong=\"yes\" ТОЛЬКО если довод ассистента конкретно и достоверно опровергает обоснование эксперта.\n"
    "- Если эксперт ссылается на общие указания / другие листы / спецификации / принято экспертизой / "
    "формальный недочёт / разумное инженерное суждение — expert_wrong=\"no\" (эксперт прав).\n"
    "- Если данных недостаточно — \"cannot_tell\"."
)


def _assistant_finding(x: dict) -> str:
    if x.get("kind") == "graphic":
        return f"прочитано с чертежа: {x.get('value_read','')}; вывод: {x.get('explanation','')}"
    return f"довод: {x.get('key_point','')}; пояснение: {x.get('explanation','')}"


def _prompt(x: dict) -> str:
    return (
        "Ты старший эксперт-арбитр по строительной проектной документации РФ.\n"
        "ИИ-замечание ОТКЛОНЕНО экспертом, а ассистент заподозрил, что эксперт ОШИБСЯ. Проверь.\n\n"
        f"Замечание ИИ: {(x.get('problem') or '')[:450]}\n"
        f"Обоснование эксперта: {(x.get('rejection_reason') or '')[:450]}\n"
        f"Что нашёл ассистент против эксперта: {_assistant_finding(x)[:450]}\n\n"
        f"{_RULES}\n\nОтветь ТОЛЬКО JSON: {{\"expert_wrong\":\"yes|no|cannot_tell\",\"reason\":\"кратко\"}}"
    )


def _call(prompt: str, model: str):
    t = time.time()
    proc = subprocess.run(
        [CLI, "-p", "--model", model, "--allowedTools", "none", "--output-format", "json", "--max-turns", "1"],
        input=prompt, capture_output=True, text=True, timeout=300)
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr or proc.stdout or "fail")[:200])
    env = json.loads(proc.stdout)
    u = env.get("usage", {}) or {}
    return env.get("result", ""), time.time() - t, env.get("total_cost_usd", 0.0), u.get("output_tokens", 0)


def _verdict(t: str):
    m = re.search(r"\{.*\}", t, re.DOTALL)
    try:
        o = json.loads(m.group(0)) if m else {}
    except Exception:
        o = {}
    return str(o.get("expert_wrong", "cannot_tell")).strip().lower(), str(o.get("reason", ""))[:240]


def main(model: str = "claude-sonnet-5") -> int:
    items = json.loads((OUT_DIR / "shortlist_ranked.json").read_text(encoding="utf-8"))
    prog = OUT_DIR / f"arbiter_{model}_progress.jsonl"
    done = {}
    if prog.is_file():
        for line in prog.read_text(encoding="utf-8").splitlines():
            try:
                r = json.loads(line); done[r["item_id"] + "|" + r.get("document", "")] = r
            except Exception:
                pass
    print(f"[arbiter] модель={model} | кандидатов={len(items)} | уже сделано={len(done)}", flush=True)

    from collections import Counter
    results = list(done.values())
    t0 = time.time(); cost = 0.0
    with prog.open("a", encoding="utf-8") as fh:
        for i, x in enumerate(items, 1):
            key = x["item_id"] + "|" + x.get("document", "")
            if key in done:
                continue
            try:
                raw, dt, c, otok = _call(_prompt(x), model)
                v, reason = _verdict(raw)
                cost += c
            except Exception as e:
                v, reason, dt, c = "error", str(e)[:120], 0, 0
            rec = {"discipline": x.get("discipline"), "document": x.get("document"),
                   "version": x.get("version"), "item_id": x["item_id"], "kind": x.get("kind"),
                   "_sev": x.get("_sev"), "_score": x.get("_score"),
                   "problem": x.get("problem"), "rejection_reason": x.get("rejection_reason"),
                   "assistant_finding": _assistant_finding(x),
                   "arbiter_verdict": v, "arbiter_reason": reason}
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n"); fh.flush()
            results.append(rec)
            if i % 10 == 0:
                kept = sum(1 for r in results if r.get("arbiter_verdict") == "yes")
                print(f"  …{i}/{len(items)} ({time.time()-t0:.0f}с) подтверждено yes={kept} ${cost:.2f}", flush=True)

    verd = Counter(r.get("arbiter_verdict") for r in results)
    survivors = [r for r in results if r.get("arbiter_verdict") == "yes"]
    survivors.sort(key=lambda r: -(r.get("_score") or 0))
    (OUT_DIR / f"arbiter_{model}_result.json").write_text(json.dumps({
        "model": model, "total": len(results), "verdicts": dict(verd),
        "confirmed": len(survivors), "survivors": survivors,
    }, ensure_ascii=False, indent=2), encoding="utf-8")

    L = [f"# Аудит Алии — ФИНАЛ (арбитр {model}): подтверждённые «эксперт мог ошибиться»", ""]
    L.append(f"Из {len(results)} кандидатов шорт-листа арбитр ({model}, поодиночке, консервативно) "
             f"подтвердил **{len(survivors)}**.")
    L.append(f"Вердикты: {dict(verd)}. Это ЛИДЫ на ручную перепроверку экспертом.\n")
    for i, r in enumerate(survivors, 1):
        L.append(f"### {i}. [{r.get('discipline')}/{r.get('document')} {r['item_id']}] "
                 f"({r.get('_sev','')}, {r.get('kind')})")
        L.append(f"- **Замечание:** {(r.get('problem') or '')[:220]}")
        L.append(f"- **Эксперт отклонил:** {(r.get('rejection_reason') or '')[:200]}")
        L.append(f"- **Против эксперта:** {(r.get('assistant_finding') or '')[:200]}")
        L.append(f"- **Вывод арбитра:** {(r.get('arbiter_reason') or '')[:220]}")
    (OUT_DIR / "EXPERT_AUDIT_ALIA_FINAL.md").write_text("\n".join(L), encoding="utf-8")
    print(f"\n[arbiter] вердикты: {dict(verd)} | подтверждено yes: {len(survivors)} | ~${cost:.2f}")
    print(f"[arbiter] финал: {OUT_DIR/'EXPERT_AUDIT_ALIA_FINAL.md'}")
    return 0


if __name__ == "__main__":
    import argparse, sys
    sys.path.insert(0, str(ROOT))
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default="claude-sonnet-5")
    a = ap.parse_args()
    raise SystemExit(main(a.model))
