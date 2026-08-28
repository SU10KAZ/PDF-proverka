"""Разбор результатов бенчмарка атомов.

Без эталонных ответов человека нельзя говорить о точности. Поэтому считаем
ровно то, что измеримо объективно:
  schema_ok        — модель вернула валидный JSON по схеме
  verifier_pass    — ответ прошёл детерминированную проверку заземления
  resolved_rate    — доля AI_RESOLVED
  human_rate       — доля честных отказов
  invented         — попытки выдать значение, которого нет в доказательствах
  agreement        — согласие конфигураций между собой (замена эталона)
"""
from __future__ import annotations

import collections
import json
import statistics
import sys
from pathlib import Path

RESULTS = Path(__file__).parent / "results"


def main() -> None:
    data = json.loads((RESULTS / "atom_benchmark.json").read_text(encoding="utf-8"))
    recs = data["records"]
    configs = data["configs"]

    print(f"выборка: {data['sample_size']} случаев × {len(configs)} конфигураций "
          f"= {len(recs)} вызовов\n")

    rows = []
    for cfg in configs:
        r = [x for x in recs if x["config"] == cfg]
        n = len(r)
        ok = [x for x in r if x.get("ok") and x.get("response")]
        ver = [x for x in ok if x.get("verifier", {}).get("ok")]
        resolved = [x for x in ok if x["response"].get("resolution_status") == "AI_RESOLVED"]
        resolved_valid = [x for x in ver if x["response"].get("resolution_status") == "AI_RESOLVED"]
        human = [x for x in ok if x["response"].get("resolution_status") == "HUMAN_REQUIRED"]
        durs = [x["duration_s"] for x in r if x.get("duration_s")]

        errs = collections.Counter()
        for x in ok:
            for e in x.get("verifier", {}).get("errors", []):
                errs[e.split(":")[0]] += 1
        invented = sum(v for k, v in errs.items() if k in ("grounding", "quote"))

        rows.append({
            "config": cfg, "n": n,
            "schema_ok": len(ok) / n if n else 0,
            "verifier_pass": len(ver) / len(ok) if ok else 0,
            "resolved": len(resolved) / len(ok) if ok else 0,
            "resolved_valid": len(resolved_valid) / n if n else 0,
            "human": len(human) / len(ok) if ok else 0,
            "invented": invented,
            "median_s": statistics.median(durs) if durs else 0,
            "p90_s": (sorted(durs)[int(0.9 * len(durs))] if durs else 0),
            "errs": errs,
        })

    hdr = (f"{'конфигурация':<34}{'схема':>7}{'верифик':>9}{'AI_RES':>8}"
           f"{'годн.реш':>10}{'человек':>9}{'выдумки':>9}{'медиана':>9}{'p90':>7}")
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        print(f"{r['config']:<34}{r['schema_ok']:>6.0%}{r['verifier_pass']:>9.0%}"
              f"{r['resolved']:>8.0%}{r['resolved_valid']:>10.0%}{r['human']:>9.0%}"
              f"{r['invented']:>9d}{r['median_s']:>8.1f}s{r['p90_s']:>6.0f}s")

    print("\n=== ПРИЧИНЫ ОТКАЗА ВЕРИФИКАТОРА ===")
    for r in rows:
        if r["errs"]:
            top = ", ".join(f"{k}={v}" for k, v in r["errs"].most_common(6))
            print(f"  {r['config']:<34}{top}")

    print("\n=== ЧТО МОДЕЛИ ГОВОРЯТ О ПРИРОДЕ РАСХОЖДЕНИЙ ===")
    reasons = collections.defaultdict(collections.Counter)
    for x in recs:
        if x.get("ok") and x.get("response"):
            reasons[x["config"]][x["response"].get("review_reason")] += 1
    for cfg in configs:
        c = reasons[cfg]
        tot = sum(c.values()) or 1
        parts = ", ".join(f"{k} {v} ({v / tot:.0%})" for k, v in c.most_common(5))
        print(f"  {cfg:<34}{parts}")

    print("\n=== СОГЛАСИЕ КОНФИГУРАЦИЙ (без эталона) ===")
    by_q = collections.defaultdict(dict)
    for x in recs:
        if x.get("ok") and x.get("response"):
            by_q[x["question_id"]][x["config"]] = x["response"]

    full_status = full_reason = full_dim = 0
    considered = 0
    for qid, per in by_q.items():
        if len(per) < 2:
            continue
        considered += 1
        st = {v.get("resolution_status") for v in per.values()}
        rs = {v.get("review_reason") for v in per.values()}
        dm = {v.get("dimension") for v in per.values()}
        full_status += len(st) == 1
        full_reason += len(rs) == 1
        full_dim += len(dm) == 1
    if considered:
        print(f"  случаев с ответом ≥2 конфигураций: {considered}")
        print(f"  полное согласие по статусу разрешения: {full_status / considered:.0%}")
        print(f"  полное согласие по причине отказа:     {full_reason / considered:.0%}")
        print(f"  полное согласие по измерению:          {full_dim / considered:.0%}")

    print("\n=== ПО СТРАТАМ: доля честных отказов ===")
    strat = collections.defaultdict(lambda: [0, 0])
    for x in recs:
        if x.get("ok") and x.get("response"):
            s = x["stratum"]
            key = f"{s['direction']}|{s['content_shape']}"
            strat[key][1] += 1
            if x["response"].get("resolution_status") != "AI_RESOLVED":
                strat[key][0] += 1
    for k, (h, t) in sorted(strat.items(), key=lambda kv: -kv[1][1]):
        print(f"  {k:<34}{h}/{t} = {h / t:.0%}")


if __name__ == "__main__":
    main()
