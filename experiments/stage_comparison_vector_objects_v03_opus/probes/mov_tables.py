# -*- coding: utf-8 -*-
"""Regenerate every numeric table of mov_FINDINGS.md from the aggregated JSON, so the
report and the artifacts can never drift apart.

    python probes/mov_tables.py > artifacts/mov_tables.md
"""
from __future__ import annotations
import json, sys
from pathlib import Path

ART = Path(__file__).resolve().parents[1] / "artifacts"
CF = json.load(open(ART / "mov_cf_summary.json", encoding="utf-8"))
RE = json.load(open(ART / "mov_real_summary.json", encoding="utf-8"))
BR = json.load(open(ART / "mov_borderrule.json", encoding="utf-8"))
RG = json.load(open(ART / "mov_region.json", encoding="utf-8"))
out = []
P = out.append


def g(d, *ks, default=None):
    for k in ks:
        if d is None:
            return default
        d = d.get(k) if isinstance(d, dict) else None
    return default if d is None else d


P("### [T1] Объём набора [CF]")
P("")
P(f"строк {CF['n_rows']}, носителей {CF['n_carriers']}, дисциплин {CF['n_disciplines']}, "
  f"сравнений выполнено {CF['n_ok']}, пропущено движком {CF['n_skipped']}, "
  f"ошибок обвязки {CF['n_error']}")
P("")

P("### [T2] Матрица M2 «что было → что сказала система» [CF]")
P("")
cols = ["SILENT(block transform / no change)", "MOVED_OBJECT at the right place",
        "change at the right place, not named a move", "findings ONLY elsewhere",
        "findings (no truth bbox)", "UNKNOWN"]
names = {"SILENT(block transform / no change)": "молчание",
         "MOVED_OBJECT at the right place": "MOVED в нужном месте",
         "change at the right place, not named a move": "в нужном месте, не назван переносом",
         "findings ONLY elsewhere": "только в другом месте",
         "findings (no truth bbox)": "находки (bbox истины нет)",
         "UNKNOWN": "UNKNOWN"}
P("| истина ↓ / ответ → | " + " | ".join(names[c] for c in cols) + " | всего |")
P("|" + "---|" * (len(cols) + 2))
RU = {"A: representation rewritten": "A: переписано представление",
      "B: whole block transformed": "B: весь блок трансформирован",
      "C1: one object removed": "C1: один объект удалён",
      "C3: one object moved >= 0.5 pt": "C3: один объект сдвинут на ≥ 0.5 pt",
      "C3: one object moved < 0.5 pt": "C3: один объект сдвинут на < 0.5 pt",
      "C3+B: block AND object moved": "C3+B: блок И объект сдвинуты",
      "C3+B: below tolerance": "C3+B: сдвиг объекта ниже допуска",
      "D: text only": "D: изменён только текст"}
for k, v in CF["confusion_M2"].items():
    tot = sum(v.values())
    P(f"| **{RU.get(k, k)}** | " + " | ".join(str(v.get(c, 0)) for c in cols) + f" | {tot} |")
P("")

P("### [T3] Флаг BLOCK_TRANSFORMED [CF]")
P("")
P("| класс | инстансов | флаг выставлен |")
P("|---|---|---|")
for k, v in CF["block_transformed_flag"].items():
    P(f"| {k} | {v['n']} | {v['flagged']} |")
P("")

P("### [T4] Точность восстановления параметров [CF]")
P("")
import glob as _glob
_terr = {}
for _f in sorted(_glob.glob(str(ART / "mov_runs" / "cf_*.jsonl"))):
    for _l in open(_f, encoding="utf-8"):
        if '"B1_translate' not in _l and '"B2_scale' not in _l:
            continue
        _r = json.loads(_l)
        _t = (_r.get("transform") or {}).get("t_err_pt")
        if _t is not None and not _r.get("chained"):
            _terr.setdefault(_r["tag"], []).append(_t)
P("| контрфакт | n | ошибка сдвига, медиана pt | p90 | max | ≤ 0.01 pt | ошибка масштаба, медиана | угол верен |")
P("|---|---|---|---|---|---|---|---|")
for tag in sorted(CF["by_tag"]):
    if not tag.startswith(("B1", "B2", "B5")):
        continue
    v = CF["by_tag"][tag]
    e = _terr.get(tag, [])
    ok01 = f"{sum(1 for x in e if x <= 0.01)}/{len(e)}" if e else "—"
    mx = round(max(e), 3) if e else "—"
    P(f"| {tag} | {v['n_scored']} | {g(v,'t_err_pt','median')} | {g(v,'t_err_pt','p90')} | {mx} | {ok01} | "
      f"{g(v,'s_err','median')} | {v['theta_ok']}/{v['n_scored']} |")
P("")

P("### [T5] Кривая чувствительности к δ, абсолютная ось [CF]")
P("")
P("| δ, pt | n | recall (назван переносом) | локализовано |")
P("|---|---|---|---|")
for r in CF["curve_by_pt"]:
    if r["band"] != "all":
        continue
    hi = "∞" if r["hi_pt"] > 1e8 else r["hi_pt"]
    P(f"| {r['lo_pt']} – {hi} | {r['n']} | {r['recall']:.3f} | {r['localised_share']:.3f} |")
P("")

P("### [T5b] Самопроверка объяснения: δ, нормированная на допуск сопоставления [CF]")
P("")
tr = CF["tol_range"]
P(f"допуск `tol = max(0.5, 0.05·S)` в корпусе: медиана {tr['median']} pt, "
  f"диапазон {tr['min']}…{tr['max']} pt, выше 0.5 pt у {tr['share_above_0.5']:.3f} инстансов")
P("")
P("| δ / tol | n | recall |")
P("|---|---|---|")
for r in CF["curve_by_tol"]:
    hi = "∞" if r["hi"] > 1e8 else r["hi"]
    P(f"| {r['lo']} – {hi} | {r['n']} | {r['recall']:.3f} |")
P("")

P("### [T6] То же по плотности блока [CF]")
P("")
bands = ["<500", "500-5k", "5k-20k", ">20k"]
edges = sorted({(r["lo_pt"], r["hi_pt"]) for r in CF["curve_by_pt"] if r["band"] != "all"})
P("| плотность | " + " | ".join(f"{lo}–{'∞' if hi>1e8 else hi} pt" for lo, hi in edges) + " |")
P("|" + "---|" * (len(edges) + 1))
for b in bands:
    cells = []
    for lo, hi in edges:
        m = [r for r in CF["curve_by_pt"] if r["band"] == b and r["lo_pt"] == lo]
        cells.append(f"{m[0]['recall']:.3f} ({m[0]['n']})" if m else "—")
    P(f"| {b} | " + " | ".join(cells) + " |")
P("")

P("### [T7] Кривая по δ как доле диагонали блока, по размеру объекта [CF]")
P("")
P("| δ, доля диагонали | tiny (<0.1 % площади) | small (0.1–1 %) | large (>1 %) |")
P("|---|---|---|---|")
fr = sorted({r["frac"] for r in CF["curve_by_frac"]})
for f in fr:
    cells = []
    for b in ("tiny", "small", "large"):
        m = [r for r in CF["curve_by_frac"] if r["bucket"] == b and r["frac"] == f]
        cells.append(f"{m[0]['recall']:.3f} ({m[0]['n']})" if m else "—")
    med = [r for r in CF["curve_by_frac"] if r["bucket"] == "all" and r["frac"] == f]
    mp = g(med[0], "delta_pt", "median") if med else "?"
    P(f"| {f} (медиана {mp} pt) | " + " | ".join(cells) + " |")
P("")

P("### [T8] M3 — блок трансформирован И объект сдвинут [CF]")
P("")
P("| комбинация | n | recall | локализовано | ложных находок вне объекта (медиана / p90 / max) |")
P("|---|---|---|---|---|")
for k, v in CF["combo"].items():
    fp = v["fp_elsewhere"]
    P(f"| {k} | {v['n']} | {v['recall']:.3f} | {v['localised_share']:.3f} | "
      f"{fp.get('median')} / {fp.get('p90')} / {fp.get('max')} |")
P("")

P("### [T9] Ложные срабатывания по классам, правило v1 [CF]")
P("")
P("| класс | инстансов | с находками | доля |")
P("|---|---|---|---|")
for k, v in CF["silent_classes"]["by_class"].items():
    P(f"| {k} | {v['n']} | {v['fp']} | {v['fp']/max(1,v['n']):.4f} |")
P(f"")
P(f"всего «молчащих» инстансов {CF['silent_classes']['n']}, с находками "
  f"{CF['silent_classes']['n_with_findings']} ({CF['silent_classes']['fp_rate']:.4f})")
P("")

P("### [T10] M4b — правило границы v1 против v2 [CF]")
P("")
h = BR["headline"]
P(f"носителей {BR['n_carriers']}, сравнений {BR['n_scored']}, "
  f"«молчащих» {h['n_silent']}, «настоящих» {h['n_true']}; "
  f"доля сегментов, обрезанных рамкой, медиана {g(BR,'border_seg_share','median')}")
P("")
P("| правило | ложных на классах A/B (доля) | истинных найдено (доля) |")
P("|---|---|---|")
P(f"| v1 (пересечение кадров, без провенанса) | **{h['v1_fp_rate']:.4f}** | {h['v1_tp_rate']:.4f} |")
for pad in ("pad0.0", "pad0.5", "pad1.0", "pad2.0", "pad4.0"):
    P(f"| v2, отступ {pad[3:]} pt | **{h['v2_'+pad+'_fp_rate']:.4f}** | {h['v2_'+pad+'_tp_rate']:.4f} |")
P("")
P("| класс | n | v1 | v2 (отступ 0) | v1: нашёл истину | v2: нашёл истину |")
P("|---|---|---|---|---|---|")
for k, v in BR["by_class"].items():
    P(f"| {k} | {v['n']} | {v['v1']:.3f} | {v['v2_pad0.0']:.3f} | "
      f"{v.get('v1_on_truth','—')} | {v.get('v2_pad0.0_on_truth','—')} |")
P("")

PH = json.load(open(ART / "mov_phase.json", encoding="utf-8"))
P("### [T_PH] Слой объектов не инвариантен к чистому сдвигу блока [CF]")
P("")
P(f"{len(PH['rows'])} реальных блоков; сдвиг задан в долях характерного масштаба S блока, "
  f"рисунок не тронут — идеальный слой обязан вернуть ТО ЖЕ разбиение. "
  f"churn = доля длины штриха в объектах, состав которых изменился хотя бы одним сегментом")
P("")
P("| сдвиг, доли S | блоков | churn разбиения, медиана | max | блоков без единого изменения | n_obj / n_obj₀, медиана |")
P("|---|---|---|---|---|---|")
for f in PH["fracs"]:
    v = PH["agg"][str(f)]
    P(f"| {f} | {v['n']} | {v['churn_median']} | {v['churn_max']} | {v['n_zero']}/{v['n']} | "
      f"{v['nobj_ratio_median']} |")
P("")

P("### [T_AVB] Якоря против bbox блока [REAL]")
P("")
a = RE["anchor_vs_bbox"]
P(f"пар {a['anchor']['n']} (бенчмарк + запасная ось Р↔Р)")
P("")
P("| выравнивание | несопоставленная краска A, медиана | p90 | max | якоря лучше | якоря хуже | расхождение оценки сдвига, медиана pt |")
P("|---|---|---|---|---|---|---|")
P(f"| **якоря-объекты** | **{g(a,'anchor','median')}** | {g(a,'anchor','p90')} | {g(a,'anchor','max')} | — | — | — |")
P(f"| рамка кропа, совмещены начала (s=1) | {g(a,'bbox_org','median')} | {g(a,'bbox_org','p90')} | {g(a,'bbox_org','max')} | "
  f"{a['n_anchor_better_than_org']} | {a['n_anchor_worse_than_org']} | {g(a,'dt_org_pt','median')} |")
P(f"| рамка кропа, подогнана изотропно | {g(a,'bbox_fit','median')} | {g(a,'bbox_fit','p90')} | {g(a,'bbox_fit','max')} | "
  f"{a['n_anchor_better_than_fit']} | {a['n_anchor_worse_than_fit']} | {g(a,'dt_fit_pt','median')} |")
P("")

P("### [T10b] Ложные «объект сдвинулся» от рамки кропа: v1 против v2 [CF]")
P("")
P("| контрфакт | инстансов | инстансов с ложным MOVED, v1 | v2 | всего ложных находок, v1 | v2 |")
P("|---|---|---|---|---|---|")
for k, v in (BR.get("false_moved") or {}).items():
    P(f"| {k} | {v['n']} | **{v['v1_instances_with_MOVED']}** | **{v['v2_instances_with_MOVED']}** | "
      f"{v['v1_findings_total']} | {v['v2_findings_total']} |")
P("")

P("### [T11] Величина глобального преобразования на РЕАЛЬНЫХ парах [REAL]")
P("")
P("| набор | пар | выровнено | \\|t\\|, медиана pt | p90 | max | доля \\|t\\|>1 pt | доля \\|s−1\\|>0.01 | поворот ≠ 0 |")
P("|---|---|---|---|---|---|---|---|---|")
for key in ("transform_bench", "transform_fallback", "transform_border_sample"):
    v = RE[key]
    P(f"| {v['label']} | {v['n']} | {v['n_aligned']} | {g(v,'t_norm_pt','median')} | "
      f"{g(v,'t_norm_pt','p90')} | {g(v,'t_norm_pt','max')} | {v['share_t_gt_1pt']} | "
      f"{v['share_s_gt_0.01']} | {v['n_theta_nonzero']} |")
P("")

P("### [T12] Бенчмарк: вердикт против разметки [REAL]")
P("")
for k, v in RE["bench_confusion"].items():
    P(f"* `{k}` — {v}")
P("")
P("| пара | классы | ожидалось | статус | вердикт | находок | границ | \\|t\\|, pt |")
P("|---|---|---|---|---|---|---|---|")
for r in sorted(RE["bench_rows"], key=lambda r: r["pair_id"]):
    P(f"| {r['pair_id']} | {','.join(r['classes'] or [])} | {r['expected']} | {r['status']} | "
      f"{r['verdict']} | {r['n_findings']} | {r['n_border_entries']} | {r['t_norm_pt']} |")
P("")

P("### [T13] Сопоставимая область (доля краски внутри пересечения кадров) [REAL]")
P("")
P(f"пар {RG['n_ok']}; медиана A {g(RG,'comparable_share_a','median')}, "
  f"B {g(RG,'comparable_share_b','median')}; "
  f"min(A,B) < 0.95 у {RG['share_below_0.95']:.4f}, < 0.80 у {RG['share_below_0.80']:.4f}")
P("")
P("| пара | ожидалось | доля краски A | доля краски B | классы |")
P("|---|---|---|---|---|")
for r in RG["rows"][:14]:
    P(f"| {r['pair_id']} | {r['expected']} | {r['comparable_share_a']:.4f} | "
      f"{r['comparable_share_b']:.4f} | {','.join(r['classes'] or [])} |")
P("")

P("### [T14] M4b на реальном корпусе: «вся невязка на границе» против контроля [REAL]")
P("")
P("| группа | пар | выровнено | пар с находками | из них с MOVED | находок, медиана | "
  "граничных записей, медиана | краска на границе A, медиана |")
P("|---|---|---|---|---|---|---|---|")
for k, v in (RE.get("m4b_border") or {}).items():
    P(f"| {k} | {v['n']} | {v['n_aligned']} | {v['share_with_findings']:.3f} | "
      f"{v['share_with_moved']:.3f} | {g(v,'n_findings','median')} | "
      f"{g(v,'n_border_entries','median')} | {g(v,'border_ink_share_a','median')} |")
P("")

P("### [T15] M5 — когда выравнивание невозможно")
P("")
P(f"[CF] отказов {CF['unavailable']['n']} из {CF['n_rows']} "
  f"({CF['unavailable']['share']:.4f}); по контрфактам: {CF['unavailable']['by_tag'][:6]}")
P(f"[CF] неоднозначных (второй консенсус): {CF['ambiguous']['n']}")
P("")
P("[REAL] статусы по источникам:")
P("")
P("| источник | " + " | ".join(["ALIGNED", "ALIGNMENT_UNAVAILABLE", "NO_VECTOR",
                                "SKIPPED_TOO_DENSE", "ERROR"]) + " | всего |")
P("|---|---|---|---|---|---|---|")
for src, rows_ in RE["m5_real"]["by_source"].items():
    d = dict(rows_)
    tot = sum(d.values())
    P(f"| {src} | " + " | ".join(str(d.get(k, 0)) for k in
                                 ("ALIGNED", "ALIGNMENT_UNAVAILABLE", "NO_VECTOR",
                                  "SKIPPED_TOO_DENSE", "ERROR")) + f" | {tot} |")
P("")
P(f"причины отказа [REAL]: {RE['m5_real']['unavailable_reasons']}")
P("")
print("\n".join(out))
