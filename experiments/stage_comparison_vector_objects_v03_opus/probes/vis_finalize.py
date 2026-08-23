# -*- coding: utf-8 -*-
"""visprep V2/V3/V4 — assemble the 30 vision cases, price them, split truth away.

Output:
  artifacts/vis_cases/           left/right PNG of every case (no annotation on them)
  artifacts/vis_cases.json       what a BLIND agent gets: id, role, source, question,
                                 closed answer list, two PNG paths, token estimate
  artifacts/vis_truth.json       ground truth + provenance (evaluator only)

The case list below is explicit on purpose: every entry was looked at with my own eyes
before it became a case, and the eye verdict is what `truth` records.
"""
from __future__ import annotations

import json
import math
import random
import shutil
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import fitz                     # noqa: E402
import vis_common as V          # noqa: E402
import vis_check as CHK         # noqa: E402

Q1 = ("Слева и справа показан ОДИН И ТОТ ЖЕ участок графики из двух версий одного "
      "документа (левый кроп — версия A, правый — версия B). Детерминированный "
      "вектор-слой не смог решить, есть ли здесь изменение графики. "
      "Отличается ли ГРАФИКА на этом участке?\n"
      "НЕ считать изменением графики: другую оцифровку той же фигуры (дрожание "
      "координат, другое разбиение на примитивы, иная гладкость кривой), другую "
      "толщину линии, любые изменения текста, чисел и подписей.\n"
      "Считать изменением графики: элемент появился или исчез, элемент переехал, "
      "элемент заменён другим по форме.")
O1 = ["графика та же", "есть графическое отличие", "не могу определить"]

Q2 = ("Слева и справа — два подготовленных графических блока, которые сопоставитель "
      "связал как один и тот же чертёж (левый — версия A, правый — версия B). "
      "Пригодного вектор-представления для сравнения нет, решать нужно по картинке. "
      "Показан ли на обоих кропах ОДИН И ТОТ ЖЕ чертёж (масштаб, положение и обрезка "
      "могут отличаться)?")
O2 = ["один и тот же чертёж", "разные чертежи", "не могу определить"]

# cand_id, role, qset, truth, source_kind, why_uncertain, eye_note
CASES = [
    # ---------------- [REAL] same ----------------
    ("P_SS-6fc75e05_0", "A", 1, "SAME", "REAL",
     "окно, где стороны несут одну краску, но разное число примитивов: 200 против 1658 сегментов (×8.3) при равной длине штриха (отношение 1.016)",
     "подпись в штампе побитово та же картинка; расхождение только в разложении на примитивы"),
    ("R_EOM-0c86dfde_2", "A", 1, "SAME", "REAL",
     "запись реестра REMOVED_OBJECT (change_len 3.5 pt) внутри плотного плана",
     "глазами картинки не отличаются"),
    ("R_EOM-0c86dfde_0", "A", 1, "SAME", "REAL",
     "запись реестра ADDED_OBJECT (change_len 41.1 pt) на паре класса text_only_change",
     "различие только в тексте марки (K1.2.3п / K1.2.3п-12) и её подчёркивании; стены, штриховка, оси совпадают"),
    ("R_SS-392b7bd3_0", "A", 1, "SAME", "REAL",
     "запись реестра ADDED_OBJECT (change_len 0.1 pt) на паре класса bbox_boundary_artifact",
     "символ (диод + молния) идентичен"),
    ("R_SS-982f7f30_0", "B", 1, "SAME", "REAL",
     "запись реестра REMOVED_OBJECT (change_len 0.2 pt) на логотипе: контурный текст + заливка, вектор-описание бесполезно",
     "логотип идентичен"),
    ("S1", "B", 2, "SAME", "REAL",
     "блок класса vector_raster_mix; сопоставитель связал блоки, в которых предмет один, но масштаб и состав листа разные",
     "на обоих кропах узел В прохода кабеля в гильзе, разный масштаб"),
    ("S2", "C", 2, "SAME", "REAL",
     "оба блока чисто растровые (вектор-слоя нет вообще), сравнивать нечем кроме картинки",
     "тот же чертёж «Фундамент под оборудование Фпк-3, Фпк-4. Опалубка», другой масштаб"),
    # ---------------- [REAL] different ----------------
    ("R_EOM-c50e2170_1", "A", 1, "DIFFERENT", "REAL",
     "запись реестра CHANGED_OBJECT (change_len 113.2 pt) на том же плане, другое окно",
     "справа добавлена дверь: створка и пунктирная дуга открывания"),
    ("R_EOM-c50e2170_0", "A", 1, "DIFFERENT", "REAL",
     "запись реестра CHANGED_OBJECT (change_len 151.6 pt) на паре класса object_removed",
     "справа добавлены дуга открывания двери и прямоугольник, слева есть вертикальная серая линия"),
    ("R_AR-55eda7fb_1", "A", 1, "DIFFERENT", "REAL",
     "запись реестра ADDED_OBJECT (change_len 350.5 pt) на листе перемычек",
     "справа добавлены два анкера с кружками и размерная цепочка .80."),
    ("R_AR-5acaab0e_1", "A", 1, "DIFFERENT", "REAL",
     "запись реестра ADDED_OBJECT (change_len 50.1 pt) на плотном плане со штриховкой",
     "справа добавлена выноска-параллелограмм с цифрой 1"),
    ("R_EOM-14558cda_0", "A", 1, "DIFFERENT", "REAL",
     "запись реестра CHANGED_OBJECT (change_len 9 223 pt) на паре класса occluding_fill",
     "слева весь вид закрыт серой заливкой и оранжевой полосой, справа заливки нет"),
    ("R_VK-148ffe6c_1", "A", 1, "DIFFERENT", "REAL",
     "запись реестра CHANGED_OBJECT (change_len 1 452 pt) на аксонометрии трубопровода",
     "справа у тройника добавлена муфта и отметка -0,019 вместо -0,050"),
    ("S3", "C", 2, "DIFFERENT", "REAL",
     "правый блок содержит ДВА чертежа, левый один; сопоставитель связал блоки целиком, вектор-слоя нет (растр)",
     "слева «Опалубка», справа «Армирование» — разные чертежи"),
    ("S4", "B", 2, "DIFFERENT", "REAL",
     "пара, на которой компаратор отказался выравниваться (ALIGNMENT_UNAVAILABLE / no_consensus); зонд mine пометил её как block_match_failure",
     "слева «Условные обозначения», справа «Проход кабелей через перегородки» — разные чертежи"),
    ("R_AR-441907a2_0", "A", 1, "SAME", "REAL",
     "запись реестра REMOVED_OBJECT длиной 15 582 pt на паре класса object_removed — самая крупная запись реестра во всём наборе",
     "ПЕРЕСМОТРЕНО: мой первый глазной вердикт был «добавлен слой штриховки» и он ОШИБОЧЕН — кропы двух версий побитово идентичны (md5 совпал), пиксельный арбитр даёт структурную разницу 0.0"),
    # ---------------- [CF] same ----------------
    ("C_KJ_blk_fafe_A6_89", "A", 1, "SAME", "CF",
     "A6_round_0.5: координаты объекта округлены до 0.5 pt — точность экспорта CAD",
     "тот же символ арматурного стыка"),
    ("C_SS_blk_362a_A6_9", "A", 1, "SAME", "CF",
     "A6_round_0.5 на кружке соединения кабелей",
     "тот же символ, окружность стала слегка угловатой"),
    ("C_PT_blk_3521_A6_63", "A", 1, "SAME", "CF",
     "A6_round_0.5 на условном знаке дерева",
     "тот же знак дерева, чуть иная многоугольность"),
    ("C_VK_blk_f35e_A6_4", "A", 1, "SAME", "CF",
     "A6_round_0.5 на росчерке подписи (тонкая кривая)",
     "та же подпись, хвост росчерка стал ступенчатым"),
    ("C_KJ_blk_fafe_A1_22", "A", 1, "SAME", "CF",
     "A1_path_split: каждый путь разбит на односегментные пути (упаковка другая, краска та же)",
     "картинка идентична"),
    ("C_EOM_FT93-N34_A2_51", "A", 1, "SAME", "CF",
     "A2_path_merge: соседние сегменты слиты в полилинии",
     "картинка идентична"),
    ("C_GP_A3QG-4DH_A6_34", "A", 1, "SAME", "CF",
     "A6_round_0.5 внутри плотного фрагмента генплана",
     "тот же фрагмент"),
    # ---------------- [CF] different ----------------
    ("C_KJ_blk_fafe_C1", "A", 1, "DIFFERENT", "CF",
     "C1_remove_object: один объект слоя удалён целиком",
     "справа исчез символ арматурного стыка"),
    ("C_SS_blk_362a_C1", "A", 1, "DIFFERENT", "CF",
     "C1_remove_object",
     "справа исчезли кружок соединения и отрезок кабеля"),
    ("C_PT_blk_3521_C1", "A", 1, "DIFFERENT", "CF",
     "C1_remove_object",
     "справа исчез один знак дерева"),
    ("C_KJ_blk_fafe_C2", "A", 1, "DIFFERENT", "CF",
     "C2_add_object: объект продублирован в свободное место блока",
     "справа появился символ арматурного стыка"),
    ("C_KJ_MD36-6R6_C2", "A", 1, "DIFFERENT", "CF",
     "C2_add_object",
     "справа над надписью появились две толстые чёрточки"),
    ("C_ITP_blk_2d0c_C2", "A", 1, "DIFFERENT", "CF",
     "C2_add_object",
     "справа появилась синяя дуга"),
    ("C_OV_blk_23b7_C9", "A", 1, "DIFFERENT", "CF",
     "C9_add_branch: к линейной сети добавлена одна ветвь",
     "справа в таблице появилась вертикальная линия"),
]


def pixdiff(a: Path, b: Path):
    """How different the two crops are as PICTURES — recorded in the truth file only.
    It is the honest measure of how hard the case is for an eye; it must never reach
    the blind agent."""
    import numpy as np
    pa, pb = fitz.Pixmap(str(a)), fitz.Pixmap(str(b))
    if pa.width != pb.width or pa.height != pb.height:
        return None
    def m(p):
        arr = np.frombuffer(p.samples, dtype=np.uint8)
        arr = arr.reshape(p.height, p.stride)[:, : p.width * p.n].reshape(p.height, p.width, p.n)
        return arr[:, :, :3].mean(axis=2) < 200
    return round(float((m(pa) != m(pb)).mean()), 6)


def tokens(png: Path) -> float:
    p = fitz.Pixmap(str(png))
    return V.image_tokens(p.width, p.height), p.width, p.height


def main():
    src = V.CAND_DIR
    out = V.CASES_DIR
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    real = json.load(open(V.ART / "vis_real_candidates.json", encoding="utf-8"))["candidates"]
    scan = json.load(open(V.ART / "vis_real_scan.json", encoding="utf-8"))["candidates"]
    cf1 = json.load(open(V.ART / "vis_cf_candidates.json", encoding="utf-8"))["candidates"]
    cf2 = json.load(open(V.ART / "vis_cf_candidates2.json", encoding="utf-8"))["candidates"]
    meta = {c["cand_id"]: c for c in (real + scan + cf1 + cf2) if not c.get("reject")}
    # roles B/C cases are cut by fraction of the block (probes/vis_frac.py), so their
    # provenance is recorded here rather than coming out of a candidate file
    meta.update({
        "S1": {"pair_id": "SS-a369f492", "discipline": "SS",
               "note": "left = whole block A, right = frac 0.28,0.42,1.0,1.0 of block B"},
        "S2": {"pair_id": "AR-a32b30a6", "discipline": "AR",
               "note": "left = whole block A, right = frac 0.0,0.0,0.52,1.0 of block B"},
        "S3": {"pair_id": "AR-a32b30a6", "discipline": "AR",
               "note": "left = whole block A, right = frac 0.55,0.0,1.0,1.0 of block B "
                       "(the second drawing that block B carries)"},
        "S4": {"pair_id": "EOM-1db297d2", "discipline": "EOM",
               "note": "both sides = whole block; the pair the comparator refused to align"},
    })

    order = list(range(len(CASES)))
    random.Random(20260823).shuffle(order)
    cases, truth = [], []
    for n, k in enumerate(order, start=1):
        cand_id, role, qset, tr, kind, why, eye = CASES[k]
        cid = f"vis{n:02d}"
        lp, rp = out / f"{cid}_L.png", out / f"{cid}_R.png"
        shutil.copyfile(src / f"{cand_id}_L.png", lp)
        shutil.copyfile(src / f"{cand_id}_R.png", rp)
        tl, wl, hl = tokens(lp)
        tr_, wr, hr = tokens(rp)
        m = meta.get(cand_id, {})
        cases.append({
            "case_id": cid,
            "role": role,
            # NB: `source` is deliberately uniform inside an answer set — it must not
            # tell a blind agent whether the case is real or a counterfactual, nor hint
            # at the answer.  Full provenance lives in vis_truth.json.
            "source": ("участок подготовленного графического блока, где детерминированный "
                       "вектор-слой не смог решить сам (несопоставленная краска либо "
                       "разное векторное разложение одной и той же области)"
                       if qset == 1 else
                       "подготовленный графический блок без пригодного вектор-представления "
                       "(растр или отказ сопоставления)"),
            "question": Q1 if qset == 1 else Q2,
            "options": O1 if qset == 1 else O2,
            "left_png": str(lp),
            "right_png": str(rp),
            "tokens_est": round(tl + tr_, 1),
        })
        truth.append({
            "case_id": cid, "cand_id": cand_id, "truth": tr, "source_kind": kind,
            "role": role, "answer_set": qset,
            "truth_option": (O1 if qset == 1 else O2)[0 if tr == "SAME" else 1],
            "why_uncertain_ru": why, "eye_note_ru": eye,
            "pair_id": m.get("pair_id"), "discipline": m.get("discipline") or (m.get("carrier") or {}).get("discipline"),
            "cf_id": m.get("cf_id"), "cf_class": m.get("cf_class"),
            "block_id": m.get("block_id") or (m.get("carrier") or {}).get("block_id"),
            "doc_id": m.get("doc_id") or (m.get("carrier") or {}).get("doc_id"),
            "version": m.get("version") or (m.get("carrier") or {}).get("version"),
            "rect_a_pt": m.get("rect_a_pt") or m.get("window_pt"),
            "rect_b_pt": m.get("rect_b_pt"),
            "patch_pt": m.get("patch_pt"),
            "redraw_fidelity_diff": m.get("redraw_fidelity_diff"),
            "pair_expected": m.get("pair_expected"), "crop_note": m.get("note"),
            "rec_type": m.get("rec_type"), "change_len": m.get("change_len"),
            "n_seg_a": m.get("n_a"), "n_seg_b": m.get("n_b"),
            "px_left": [wl, hl], "px_right": [wr, hr],
            "pixel_arbiter": CHK.compare(lp, rp),
            "tokens_left": round(tl, 1), "tokens_right": round(tr_, 1),
        })
    tot = [c["tokens_est"] for c in cases]
    tot.sort()
    med = tot[len(tot) // 2] if len(tot) % 2 else (tot[len(tot) // 2 - 1] + tot[len(tot) // 2]) / 2
    summary = {
        "n_cases": len(cases),
        "by_source": {k: sum(1 for t in truth if t["source_kind"] == k) for k in ("REAL", "CF")},
        "by_truth": {k: sum(1 for t in truth if t["truth"] == k) for k in ("SAME", "DIFFERENT")},
        "balance_within_source": {
            k: {v: sum(1 for t in truth if t["source_kind"] == k and t["truth"] == v)
                for v in ("SAME", "DIFFERENT")} for k in ("REAL", "CF")},
        "by_role": {k: sum(1 for t in truth if t["role"] == k) for k in ("A", "B", "C")},
        "by_answer_set": {1: sum(1 for t in truth if t["answer_set"] == 1),
                          2: sum(1 for t in truth if t["answer_set"] == 2)},
        "tokens_sum": round(sum(tot), 1), "tokens_median_per_case": med,
        "tokens_min": tot[0], "tokens_max": tot[-1],
        "token_formula": "min(1.2014*ceil(w/32)*ceil(h/32)+48.67, 3051), измерено в v0.2",
    }
    with open(V.ART / "vis_cases.json", "w", encoding="utf-8") as fh:
        json.dump(cases, fh, ensure_ascii=False, indent=1)
    with open(V.ART / "vis_truth.json", "w", encoding="utf-8") as fh:
        json.dump({"summary": summary, "truth": truth}, fh, ensure_ascii=False, indent=1)
    print(json.dumps(summary, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
