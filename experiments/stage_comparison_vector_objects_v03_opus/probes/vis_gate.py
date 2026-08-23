#!/usr/bin/env python3
"""visscore S5 — детерминированный гейт «когда вектор обязан позвать Vision»
и его цена по корпусу.

Ничего не рендерит заново: пользуется уже собранными кропами (vis_cand/, vis_cases/),
реестром записей loc (`loc_real_pairs.json`), переписью блоков `cns_vector_eligibility.json`
и статусами выравнивания зонда `mov`.
"""
from __future__ import annotations
import json, math, sys, statistics as st
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import vis_common as V           # noqa: E402
from vis_check import compare    # noqa: E402

ART = V.ART
EPS = 0.002          # порог свободного пиксельного предфильтра (доля площади окна)
T_LO, T_HI = 2.0, 60.0   # полоса неоднозначности по длине краски записи, pt


def tokens(w, h):
    return min(1.2014 * math.ceil(w / 32) * math.ceil(h / 32) + 48.67, 3051.0)


def part_A_gate_on_answered_set():
    """Композитный конвейер: свободный пиксельный предфильтр -> Vision на выживших."""
    score = json.load(open(ART / "vis_score.json", encoding="utf-8"))["rows"]
    rows = []
    for r in score:
        prefilter_says_same = r["structural"] < EPS
        final = "SAME" if prefilter_says_same else r["pred"]
        rows.append(dict(case_id=r["case_id"], truth=r["truth"], vision=r["pred"],
                         structural=r["structural"], called_vision=not prefilter_says_same,
                         final=final, correct=final == r["truth"],
                         tokens=0.0 if prefilter_says_same else r["tokens_case"],
                         source_kind=r["source_kind"], conf=r["conf"]))
    n = len(rows)
    calls = sum(1 for r in rows if r["called_vision"])
    acc = sum(1 for r in rows if r["correct"]) / n
    vis_acc = sum(1 for r in score if r["correct"]) / n
    # чистая пиксельная линия: лучший порог, выбранный ЗАДНИМ ЧИСЛОМ (оракульная линия)
    ths = sorted({round(r["structural"], 6) for r in score} | {0.0, 1.0})
    best = max(((t, sum(1 for r in score if (r["structural"] >= t) == (r["truth"] == "DIFFERENT")) / n)
                for t in ths), key=lambda x: x[1])
    return dict(
        eps=EPS, n=n, vision_calls=calls, calls_share=round(calls / n, 3),
        tokens_total=round(sum(r["tokens"] for r in rows), 1),
        tokens_saved=round(sum(r["tokens_case"] for r in score) - sum(r["tokens"] for r in rows), 1),
        accuracy_pipeline=round(acc, 4), accuracy_vision_alone=round(vis_acc, 4),
        pixel_only_oracle_threshold=best[0], pixel_only_oracle_acc=round(best[1], 4),
        prefiltered_out=[r["case_id"] for r in rows if not r["called_vision"]],
        prefilter_errors=[r["case_id"] for r in rows if not r["called_vision"] and not r["correct"]],
        rows=rows)


def part_B_prefilter_on_real_ledger_windows():
    """Как часто свободный пиксельный предфильтр гасит запись реестра ДО Vision."""
    cands = json.load(open(ART / "vis_real_candidates.json", encoding="utf-8"))["candidates"]
    out = []
    for c in cands:
        cid = c["cand_id"]
        l, r = V.CAND_DIR / f"{cid}_L.png", V.CAND_DIR / f"{cid}_R.png"
        if not (l.exists() and r.exists()):
            continue
        m = compare(l, r)
        pxw, pxh = c["px"][0]
        out.append(dict(cand_id=cid, pair_id=c["pair_id"], pair_expected=c["pair_expected"],
                        rec_type=c["rec_type"], change_len=c["change_len"],
                        structural=m["structural"], raw_diff=m["raw_diff"],
                        killed_by_prefilter=m["structural"] < EPS,
                        tokens=round(tokens(*c["px"][0]) + tokens(*c["px"][1]), 1)))
    killed = [o for o in out if o["killed_by_prefilter"]]
    return dict(n=len(out), killed=len(killed), killed_share=round(len(killed) / len(out), 3) if out else None,
                killed_on_NO_CHANGE_pairs=sum(1 for o in killed if o["pair_expected"] == "NO_GRAPHIC_CHANGE"),
                killed_on_CHANGE_pairs=sum(1 for o in killed if o["pair_expected"] == "GRAPHIC_CHANGE"),
                tokens_if_all=round(sum(o["tokens"] for o in out), 1),
                tokens_after_prefilter=round(sum(o["tokens"] for o in out if not o["killed_by_prefilter"]), 1),
                rows=out)


def part_D_ordered_gate():
    """Правильный порядок: пиксельный предфильтр -> длинная запись отвечает сама ->
    короткая выжившая запись идёт в Vision.  Считано на 24 реальных окнах реестра."""
    B = part_B_prefilter_on_real_ledger_windows()["rows"]
    killed = [r for r in B if r["killed_by_prefilter"]]
    surv = [r for r in B if not r["killed_by_prefilter"]]
    auto = [r for r in surv if r["change_len"] >= T_HI]
    call = [r for r in surv if r["change_len"] < T_HI]
    band = [r for r in B if r["change_len"] < T_HI]
    return dict(
        n_windows=len(B), n_pairs=len({r["pair_id"] for r in B}),
        killed_free=len(killed),
        killed_free_max_change_len=max([r["change_len"] for r in killed], default=None),
        auto_reported=len(auto),
        auto_reported_on_NO_CHANGE_pairs=sum(1 for r in auto if r["pair_expected"] == "NO_GRAPHIC_CHANGE"),
        vision_calls=len(call), vision_call_ids=[r["cand_id"] for r in call],
        band_windows=len(band), band_survival_rate=round(len(call) / len(band), 3) if band else None,
        tokens_all_windows=round(sum(r["tokens"] for r in B), 1),
        tokens_ordered_gate=round(sum(r["tokens"] for r in call), 1),
        tokens_per_pair=round(sum(r["tokens"] for r in call) / len({r["pair_id"] for r in B}), 1))


def part_C_corpus_firing():
    """Сколько раз гейт сработает на корпусе и во что это обойдётся."""
    pairs = json.load(open(ART / "loc_real_pairs.json", encoding="utf-8"))["pairs"]
    elig = json.load(open(ART / "cns_vector_eligibility.json", encoding="utf-8"))
    per_pair = []
    for p in pairs:
        ri = p.get("rec_len_interior") or []
        band = [x for x in ri if T_LO <= x < T_HI]
        big = [x for x in ri if x >= T_HI]
        per_pair.append(dict(pair_id=p["pair_id"], expected=p.get("expected"),
                             n_rec=p.get("n_records"), n_int=p.get("n_records_interior"),
                             n_band=len(band), n_big=len(big),
                             n_small=len([x for x in ri if x < T_LO]),
                             classes=p["classes"]))
    fires = [q for q in per_pair if q["n_band"] > 0]
    band_counts = [q["n_band"] for q in per_pair]
    # цена: медиана токенов на один вызов из измеренного набора
    score = json.load(open(ART / "vis_score.json", encoding="utf-8"))["rows"]
    tok_med = st.median([r["tokens_case"] for r in score])
    capped = [min(q["n_band"], 5) for q in per_pair]
    return dict(
        band=[T_LO, T_HI], n_pairs=len(per_pair),
        pairs_firing_VA=len(fires), share_pairs_firing_VA=round(len(fires) / len(per_pair), 3),
        band_records_total=sum(band_counts),
        interior_records_total=sum((q["n_int"] or 0) for q in per_pair),
        big_records_total=sum(q["n_big"] for q in per_pair),
        small_records_total=sum(q["n_small"] for q in per_pair),
        band_per_pair_median=st.median(band_counts), band_per_pair_max=max(band_counts),
        band_per_pair_mean=round(st.mean(band_counts), 2),
        tokens_per_call_median=tok_med,
        tokens_per_pair_median_uncapped=round(st.median(band_counts) * tok_med, 1),
        tokens_per_pair_mean_uncapped=round(st.mean(band_counts) * tok_med, 1),
        tokens_per_pair_mean_cap5=round(st.mean(capped) * tok_med, 1),
        tokens_worst_pair_uncapped=round(max(band_counts) * tok_med, 1),
        corpus_VC=dict(vision_required_share=elig["vision_required_share"],
                       n_blocks_measured=elig["corpus_context"]["n_blocks_with_pdf_present"],
                       n_blocks_VC=round(elig["vision_required_share"] *
                                         elig["corpus_context"]["n_blocks_with_pdf_present"])),
        per_pair=per_pair)


def part_E_budget():
    """Ожидаемый бюджет токенов на ОДНУ пару подготовленных блоков при полном гейте."""
    g = json.load(open(ART / "vis_gate.json", encoding="utf-8")) if (ART / "vis_gate.json").exists() else None
    B = part_B_prefilter_on_real_ledger_windows()["rows"]
    C = part_C_corpus_firing()
    cost = json.load(open(ART / "vis_cost.json", encoding="utf-8"))
    band = [r for r in B if T_LO <= r["change_len"] < T_HI]
    surv_band = sum(1 for r in band if not r["killed_by_prefilter"])
    surv_rate = surv_band / len(band) if band else None
    small = [r for r in B if r["change_len"] < T_LO]
    tok_call = C["tokens_per_call_median"]
    tok_whole = cost["summary"]["tokens_whole_envelope_median"]
    calls_VA = C["band_per_pair_mean"] * surv_rate
    p_VC_pair = 1 - (1 - 0.02956) ** 2          # хотя бы одна сторона пары без вектора
    p_VD_pair = 10 / 68                          # mov: отказ выравнивания на случайных парах
    return dict(
        surv_rate_band=round(surv_rate, 3), n_band_windows=len(band),
        surv_rate_below_T_LO=round(sum(1 for r in small if not r["killed_by_prefilter"]) / len(small), 3) if small else None,
        n_below_T_LO=len(small),
        calls_VA_per_pair=round(calls_VA, 3), tokens_VA_per_pair=round(calls_VA * tok_call, 1),
        p_VC_pair=round(p_VC_pair, 4), tokens_VC_per_pair=round(p_VC_pair * tok_whole, 1),
        p_VD_pair=round(p_VD_pair, 4), tokens_VD_per_pair=round(p_VD_pair * tok_whole, 1),
        tokens_total_per_pair=round(calls_VA * tok_call + p_VC_pair * tok_whole + p_VD_pair * tok_whole, 1),
        baseline_whole_block_per_pair_envelope=tok_whole,
        baseline_whole_block_per_pair_same_zoom=cost["summary"]["tokens_whole_same_zoom_median"],
        tok_call_median=tok_call)


def main():
    A = part_A_gate_on_answered_set()
    D = part_D_ordered_gate()
    E = part_E_budget()
    B = part_B_prefilter_on_real_ledger_windows()
    C = part_C_corpus_firing()
    json.dump(dict(gate_eps=EPS, band=[T_LO, T_HI], A_pipeline=A, B_prefilter_real=B, C_corpus=C,
                   D_ordered_gate=D, E_budget=E),
              open(ART / "vis_gate.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    for k, d in (("A", A), ("B", B), ("C", C), ("D", D), ("E", E)):
        print("==", k)
        print(json.dumps({x: y for x, y in d.items() if x not in ("rows", "per_pair")},
                         ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
