#!/usr/bin/env python3
"""visscore — сверка ответов слепых Vision-агентов с artifacts/vis_truth.json.

Вход: artifacts/vis_answers.json (5 партий, склеенные в один массив).
Выход: artifacts/vis_score.json  (S1 точность, S2 калибровка, S3 разбор ошибок, S4 цена).
"""
import json, os, math, statistics as st
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
ART = os.path.join(os.path.dirname(HERE), "artifacts")

OPT_SET1 = {"графика та же": "SAME", "есть графическое отличие": "DIFFERENT",
            "не могу определить": "IDK"}
OPT_SET2 = {"один и тот же чертёж": "SAME", "разные чертежи": "DIFFERENT",
            "не могу определить": "IDK"}


def norm_answer(ans, answer_set):
    m = OPT_SET1 if answer_set == 1 else OPT_SET2
    a = ans.strip()
    if a in m:
        return m[a]
    # ответ не из закрытого списка своего набора — проверим другой набор
    other = OPT_SET2 if answer_set == 1 else OPT_SET1
    if a in other:
        return other[a] + "|OFFLIST"
    return "UNPARSED"


def img_tokens(w, h):
    return min(1.2014 * math.ceil(w / 32) * math.ceil(h / 32) + 48.67, 3051.0)


def main():
    truth = {t["case_id"]: t for t in json.load(open(os.path.join(ART, "vis_truth.json")))["truth"]}
    cases = {c["case_id"]: c for c in json.load(open(os.path.join(ART, "vis_cases.json")))}
    answers = json.load(open(os.path.join(ART, "vis_answers.json")))

    rows = []
    for a in answers:
        cid = a["case_id"]
        t = truth[cid]
        c = cases[cid]
        pred = norm_answer(a["answer"], t["answer_set"])
        base = pred.split("|")[0]
        rows.append(dict(
            case_id=cid, role=t["role"], source_kind=t["source_kind"], answer_set=t["answer_set"],
            truth=t["truth"], raw_answer=a["answer"], pred=base, offlist=pred.endswith("OFFLIST"),
            correct=(base == t["truth"]), conf=a.get("confidence"), why=a.get("why", ""),
            pair_id=t.get("pair_id"), cf_id=t.get("cf_id"), cf_class=t.get("cf_class"),
            discipline=t.get("discipline"), rec_type=t.get("rec_type"), change_len=t.get("change_len"),
            structural=t["pixel_arbiter"]["structural"], raw_diff=t["pixel_arbiter"]["raw_diff"],
            size_mismatch=t["pixel_arbiter"]["size_mismatch"],
            tokens_case=t["tokens_left"] + t["tokens_right"],
            px_left=t["px_left"], px_right=t["px_right"],
            eye_note_ru=t.get("eye_note_ru"), why_uncertain_ru=t.get("why_uncertain_ru"),
            batch=a.get("batch"),
        ))
    rows.sort(key=lambda r: r["case_id"])

    def acc(sub):
        n = len(sub)
        k = sum(1 for r in sub if r["correct"])
        idk = sum(1 for r in sub if r["pred"] == "IDK")
        unp = sum(1 for r in sub if r["pred"] == "UNPARSED")
        return dict(n=n, correct=k, acc=(k / n if n else None), idk=idk, unparsed=unp)

    def majority_baseline(sub):
        """точность стратегии «всегда самый частый ответ по истине этого среза»"""
        cnt = defaultdict(int)
        for r in sub:
            cnt[r["truth"]] += 1
        if not sub:
            return None
        best = max(cnt.values())
        return dict(n=len(sub), best_label=[k for k, v in cnt.items() if v == best],
                    baseline_acc=best / len(sub), truth_counts=dict(cnt))

    slices = {"ALL": rows}
    for role in ("A", "B", "C"):
        slices[f"role_{role}"] = [r for r in rows if r["role"] == role]
    for sk in ("REAL", "CF"):
        slices[f"src_{sk}"] = [r for r in rows if r["source_kind"] == sk]
        for role in ("A", "B", "C"):
            s = [r for r in rows if r["source_kind"] == sk and r["role"] == role]
            if s:
                slices[f"src_{sk}_role_{role}"] = s
    for tr in ("SAME", "DIFFERENT"):
        slices[f"truth_{tr}"] = [r for r in rows if r["truth"] == tr]
        for sk in ("REAL", "CF"):
            slices[f"truth_{tr}_{sk}"] = [r for r in rows if r["truth"] == tr and r["source_kind"] == sk]
    slices["answer_set_1"] = [r for r in rows if r["answer_set"] == 1]
    slices["answer_set_2"] = [r for r in rows if r["answer_set"] == 2]

    S1 = {k: dict(**acc(v), **{"majority": majority_baseline(v)}) for k, v in slices.items()}

    # confusion (SAME/DIFFERENT/IDK)
    conf_mat = defaultdict(int)
    for r in rows:
        conf_mat[f"{r['truth']}->{r['pred']}"] += 1
    S1["confusion"] = dict(conf_mat)
    S1["by_batch"] = {b: acc([r for r in rows if r["batch"] == b])
                      for b in sorted({r["batch"] for r in rows if r["batch"] is not None})}
    S1["by_discipline"] = {d: acc([r for r in rows if r["discipline"] == d])
                           for d in sorted({r["discipline"] for r in rows if r["discipline"]})}
    S1["by_cf_id"] = {c: acc([r for r in rows if r["cf_id"] == c])
                      for c in sorted({r["cf_id"] for r in rows if r["cf_id"]})}

    # per-class recall/precision for DIFFERENT
    tp = sum(1 for r in rows if r["truth"] == "DIFFERENT" and r["pred"] == "DIFFERENT")
    fp = sum(1 for r in rows if r["truth"] == "SAME" and r["pred"] == "DIFFERENT")
    fn = sum(1 for r in rows if r["truth"] == "DIFFERENT" and r["pred"] != "DIFFERENT")
    tn = sum(1 for r in rows if r["truth"] == "SAME" and r["pred"] != "DIFFERENT")
    S1["DIFFERENT_as_positive"] = dict(tp=tp, fp=fp, fn=fn, tn=tn,
                                       precision=tp / (tp + fp) if tp + fp else None,
                                       recall=tp / (tp + fn) if tp + fn else None)
    # то же по источникам
    for sk in ("REAL", "CF"):
        s = [r for r in rows if r["source_kind"] == sk]
        tp = sum(1 for r in s if r["truth"] == "DIFFERENT" and r["pred"] == "DIFFERENT")
        fp = sum(1 for r in s if r["truth"] == "SAME" and r["pred"] == "DIFFERENT")
        fn = sum(1 for r in s if r["truth"] == "DIFFERENT" and r["pred"] != "DIFFERENT")
        tn = sum(1 for r in s if r["truth"] == "SAME" and r["pred"] != "DIFFERENT")
        S1[f"DIFFERENT_as_positive_{sk}"] = dict(tp=tp, fp=fp, fn=fn, tn=tn,
                                                 precision=tp / (tp + fp) if tp + fp else None,
                                                 recall=tp / (tp + fn) if tp + fn else None)

    # ---- S2 калибровка ----
    bands = [("<0.3", lambda c: c < 0.3), ("0.3-0.7", lambda c: 0.3 <= c <= 0.7), (">0.7", lambda c: c > 0.7)]
    S2 = {"bands": {}, "conf_stats": {}}
    for name, f in bands:
        sub = [r for r in rows if r["conf"] is not None and f(r["conf"])]
        S2["bands"][name] = dict(**acc(sub), conf_mean=(st.mean([r["conf"] for r in sub]) if sub else None),
                                 cases=[r["case_id"] for r in sub],
                                 wrong=[r["case_id"] for r in sub if not r["correct"]])
    cor = [r["conf"] for r in rows if r["correct"]]
    wro = [r["conf"] for r in rows if not r["correct"]]
    S2["conf_stats"] = dict(
        correct=dict(n=len(cor), mean=st.mean(cor) if cor else None, median=st.median(cor) if cor else None,
                     min=min(cor) if cor else None, max=max(cor) if cor else None),
        wrong=dict(n=len(wro), mean=st.mean(wro) if wro else None, median=st.median(wro) if wro else None,
                   min=min(wro) if wro else None, max=max(wro) if wro else None))
    # точечно-бисериальная корреляция conf ~ correct
    xs = [r["conf"] for r in rows]
    ys = [1.0 if r["correct"] else 0.0 for r in rows]
    mx, my = st.mean(xs), st.mean(ys)
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den = math.sqrt(sum((x - mx) ** 2 for x in xs) * sum((y - my) ** 2 for y in ys))
    S2["point_biserial_r"] = num / den if den else None
    # Brier score (уверенность как вероятность своего ответа быть верным)
    S2["brier"] = st.mean([(r["conf"] - (1.0 if r["correct"] else 0.0)) ** 2 for r in rows])
    # ECE в 3 полосах
    ece = 0.0
    for name, f in bands:
        sub = [r for r in rows if f(r["conf"])]
        if sub:
            ece += len(sub) / len(rows) * abs(st.mean([r["conf"] for r in sub]) -
                                              sum(1 for r in sub if r["correct"]) / len(sub))
    S2["ece_3bands"] = ece
    # AUC уверенности как детектора собственной правоты
    pos = [r["conf"] for r in rows if r["correct"]]
    neg = [r["conf"] for r in rows if not r["correct"]]
    if pos and neg:
        wins = sum(1 for p in pos for n in neg if p > n) + 0.5 * sum(1 for p in pos for n in neg if p == n)
        S2["auc_conf_vs_correct"] = wins / (len(pos) * len(neg))
    # порог отсечения: если отбрасывать ответы ниже порога — что остаётся
    S2["threshold_sweep"] = []
    for thr in (0.0, 0.55, 0.6, 0.62, 0.7, 0.78, 0.8, 0.85, 0.88, 0.9):
        kept = [r for r in rows if r["conf"] >= thr]
        S2["threshold_sweep"].append(dict(thr=thr, kept=len(kept),
                                          acc=(sum(1 for r in kept if r["correct"]) / len(kept)) if kept else None,
                                          dropped=len(rows) - len(kept)))

    # ---- S3 ошибки ----
    S3 = [dict(case_id=r["case_id"], role=r["role"], source_kind=r["source_kind"], truth=r["truth"],
               pred=r["pred"], raw_answer=r["raw_answer"], conf=r["conf"], why=r["why"],
               structural=r["structural"], raw_diff=r["raw_diff"], cf_id=r["cf_id"],
               rec_type=r["rec_type"], change_len=r["change_len"], discipline=r["discipline"],
               pair_id=r["pair_id"], eye_note_ru=r["eye_note_ru"], why_uncertain_ru=r["why_uncertain_ru"],
               tokens_case=r["tokens_case"])
          for r in rows if not r["correct"]]

    # ---- S4 цена ----
    tok = [r["tokens_case"] for r in rows]
    S4 = dict(sum_tokens=sum(tok), median_per_case=st.median(tok), min=min(tok), max=max(tok),
              mean=st.mean(tok))
    S4["per_crop"] = dict(
        median=st.median([t["tokens_left"] for t in truth.values()] + [t["tokens_right"] for t in truth.values()]))
    S4["by_source"] = {sk: dict(n=len([r for r in rows if r["source_kind"] == sk]),
                                sum=sum(r["tokens_case"] for r in rows if r["source_kind"] == sk),
                                median=st.median([r["tokens_case"] for r in rows if r["source_kind"] == sk]))
                       for sk in ("REAL", "CF")}
    json.dump(dict(rows=rows, S1=S1, S2=S2, S3=S3, S4=S4), open(os.path.join(ART, "vis_score.json"), "w"),
              ensure_ascii=False, indent=1)
    print(json.dumps({"S1_ALL": S1["ALL"], "S2_bands": {k: (v["n"], v["acc"]) for k, v in S2["bands"].items()},
                      "n_wrong": len(S3), "S4": {k: S4[k] for k in ("sum_tokens", "median_per_case")}},
                     ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
