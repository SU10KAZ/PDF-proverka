# -*- coding: utf-8 -*-
"""mine · step 7 — assemble the benchmark from the pairs confirmed BY EYE.

Every label below was written after looking at artifacts/mine_crops/<pair_id>.png
(three panels: side A, side B registered, residual overlay) and, where the change
was too small to read at block scale, at artifacts/mine_crops_zoom/<pair_id>_c*.png.
Labelling was done by ONE annotator (this probe) — see mine_FINDINGS.md.

Guards enforced here:
  * both sides must be real prepared blocks (block_type == "image") from result.json;
  * sha256(pdf_a) != sha256(pdf_b)  — no pair may be a file compared with itself;
  * coords come from result.json, never synthesised.

Writes artifacts/mine_pairs.json and artifacts/mine_corpus.md
"""
import json, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import v03_foundation as F  # noqa

ART = Path(__file__).resolve().parents[1] / "artifacts"

# pair_id: (classes, verdict, n_objects|None, why_hard_ru, human_expected_ru, confidence)
LABELS = {
 # ---------- provably unchanged -------------------------------------------------
 "AR-b38a7dbc": (["unchanged_control", "dense_block", "with_labels"], "NO_GRAPHIC_CHANGE", 0,
   "Плотный план (32 312 сегментов, 487 текстовых строк) из двух РАЗНЫХ файлов PDF; любой шум компаратора здесь — ложное срабатывание.",
   "Изменений нет: наложение даёт 0 пикселей несовпадения (diff=0.0), число сегментов и текстовых строк совпадает точно.", "high"),
 "TX-95031dbd": (["unchanged_control", "no_labels", "rotated_page"], "NO_GRAPHIC_CHANGE", 0,
   "Страница повёрнута (/Rotate 270), текст выведен кривыми: в текстовом слое 0 строк, значит текстового якоря нет вообще.",
   "Изменений нет: две схемы («Цепь освещения 220 В» и «Силовая цепь 380 В») совпадают попиксельно (diff=0.0).", "high"),
 # ---------- same graphics, other packaging -------------------------------------
 "VK-b71afe81": (["different_packaging", "rotated_page", "dense_block", "garbled_text_layer"], "NO_GRAPHIC_CHANGE", 0,
   "Растр совпадает побитно, но текстовый слой извлекается по-разному (битая кодировка шрифта): text_jaccard=0.123 при diff=0.0. Компаратор, опирающийся на текстовый якорь, увидит здесь «изменение».",
   "Графика идентична: узлы «Узел гиг. душа» и «Узел кранов с эл. приводом» совпадают попиксельно (diff=0.0, 16 205 сегментов с обеих сторон). Отличается только извлекаемый текст: «Ø161/2\"» против «Ø16*1/2\"» — дефект кодировки шрифта, не чертежа.", "high"),
 "SS-6fc75e05": (["different_packaging", "with_labels", "table_like"], "NO_GRAPHIC_CHANGE", 0,
   "Тот же штамп записан 5 845 сегментами в одной версии и 12 921 в другой (×2.21) — прямая проверка устойчивости к декомпозиции путей.",
   "Штамп тот же самый; видимое отличие — только полоска рамки на границе кропа (0.08 % площади). Число сегментов различается вдвое из-за другой упаковки путей.", "high"),
 # ---------- bbox / crop artefacts (NO change, but diff is non-zero) -------------
 "KJ-b5c82f7c": (["bbox_boundary_artifact", "with_labels"], "NO_GRAPHIC_CHANGE", 0,
   "Единственная разница — вертикальная линия рамки листа, попавшая в bbox одной версии и не попавшая в другой.",
   "Узлы армирования совпадают полностью; в версии B в кроп дополнительно попала линия рамки листа слева.", "high"),
 "AR-46f62636": (["bbox_boundary_artifact", "dense_block", "with_labels"], "NO_GRAPHIC_CHANGE", 0,
   "Плотный план 30 876 сегментов; вся «разница» — линии рамки на краю кропа, содержимое совпадает.",
   "План этажа не изменился; в A в кроп попала горизонтальная линия рамки сверху, в B — вертикальная слева.", "high"),
 "SS-392b7bd3": (["bbox_boundary_artifact", "with_labels"], "NO_GRAPHIC_CHANGE", 0,
   "Схема управления дверьми; наивный детектор объектов увидит «добавленную линию» длиной во всю ширину блока.",
   "Схема (EZ1, У1.1, «Входные двери помещения», ПУ-У1) идентична; в версии B сверху добавилась линия рамки листа.", "high"),
 # ---------- whole block moved / rescaled ---------------------------------------
 "EOM-36de2ce2": (["block_moved", "rotated_page", "few_labels"], "NO_GRAPHIC_CHANGE", 0,
   "Одна и та же схема вычерчена в другом масштабе, попиксельная разница 8.7 % — классический ложноположительный.",
   "«Схема размещения фрагмента» та же самая, в версии B крупнее (масштаб блока другой). Сегментов 2 207 и 2 207, текст совпадает.", "high"),
 "KJ-548814a7": (["block_moved", "few_labels"], "NO_GRAPHIC_CHANGE", 0,
   "Тот же чертёж в другом масштабе: сдвигом не совмещается, разница 3.1 %.",
   "«Схема отгиба стержня» одна и та же, в версии B вычерчена примерно в 1.25 раза крупнее.", "high"),
 "EOM-3306e907": (["block_moved", "rotated_page"], "NO_GRAPHIC_CHANGE", 0,
   "Версии хранят страницу с РАЗНЫМ /Rotate (90 против 0) И в разном масштабе — двойная ловушка систем координат.",
   "«Схема размещения фрагмента»: то же изображение, в версии B крупнее; после корректной дерotation обе стороны стоят вертикально.", "medium"),
 "SS-982f7f30": (["block_moved", "with_labels", "table_like"], "NO_GRAPHIC_CHANGE", 0,
   "Штамп совпадает по содержанию, но нарисован в чуть другом масштабе — 1 % площади уходит в невязку.",
   "Штамп («Книга 2. Автоматизация и диспетчеризация… Корпус 5») тот же; отличается только масштаб/положение блока.", "medium"),
 # ---------- text / table only ---------------------------------------------------
 "KJ-25717577": (["text_only_change", "table_only_change", "with_labels"], "NO_GRAPHIC_CHANGE", 0,
   "В таблице штампа изменились только значения ячеек (даты); сетка не тронута. Это работа текстового конвейера, а не графического.",
   "Изменились только даты во всех строках штампа: 25.03.26 → 22.05.26. Линии таблицы, подписи и логотип те же.", "high"),
 "SS-76640e11": (["text_only_change", "with_labels"], "NO_GRAPHIC_CHANGE", 0,
   "Удалён крупный текстовый блок (8 398 пикселей краски) — соблазн объявить «удалён объект», хотя графика плана не менялась.",
   "С плана удалён текстовый блок «Примечание:» из трёх пунктов внизу листа. Сам план (стены, оси, оборудование) не изменился.", "high"),
 "EOM-0c86dfde": (["text_only_change", "object_moved_label", "dense_block"], "NO_GRAPHIC_CHANGE", 0,
   "Сдвинута только колонка текстовых марок кабелей; геометрия плана не тронута — прямая проверка правила «подпись не объект».",
   "Столбец марок кабелей (K1.2.3n-12, K1.2.15, K1.2.21, K1.2.3n-6, K1.2.24, K1.2.20, …) сдвинут влево относительно плана; сам план тот же.", "high"),
 # ---------- real object changes -------------------------------------------------
 "EOM-c50e2170": (["object_removed", "dense_block", "with_labels"], "GRAPHIC_CHANGE", 30,
   "Удалён целый слой (координационные оси) — сотни примитивов, но семантически это одна правка; счётчик объектов должен не взорваться.",
   "В версии B исчезли координационные оси: штрихпунктирные линии и кружки с марками осей по всему периметру плана. Сегментов 8 027 → 4 502.", "high"),
 "AR-dbfd82b8": (["object_removed", "with_labels", "table_like"], "GRAPHIC_CHANGE", 1,
   "Удалён составной объект (легенда из ~25 строк с образцами штриховок) при полностью неизменной таблице выше.",
   "В версии B удалён блок «Условные обозначения» целиком (около 25 строк с образцами штриховок и подписями). Таблица «Ведомость полов 2-го этажа» не изменилась.", "high"),
 "VK-56115717": (["object_removed", "with_labels"], "GRAPHIC_CHANGE", 1,
   "Удалена целая аксонометрическая схема (~40 поэтажных отводов) — крупнейший вид на листе.",
   "В версии B удалена правая схема «Схема системы В2.3 (2 зона)» целиком; левая схема В2.2 осталась. Заодно исчез текст в штампе.", "high"),
 "AR-441907a2": (["object_removed", "with_labels"], "GRAPHIC_CHANGE", 2,
   "Из листа исчезли и чертёж-разрез, и содержимое штампа — два разных по природе объекта в одной паре.",
   "В версии B удалены разрез «Сеч. а–а» в правом верхнем углу и всё содержимое штампа (остались только линии таблицы).", "high"),
 "SS-c7aa8d26": (["object_added", "with_labels", "sparse_block"], "GRAPHIC_CHANGE", 2,
   "Разреженный блок: два новых узла добавлены рядом с единственным существующим.",
   "В версии B добавлены два узла: «Крепление горизонтальных кабельных трасс…» и «Крепление кабельных трасс в месте изгиба…». Первый узел («вертикальных трасс») не изменился.", "high"),
 "AR-a32b30a6": (["object_added", "raster_graphics", "no_labels"], "GRAPHIC_CHANGE", 1,
   "Оба блока — растровые вставки (0 векторных сегментов, 0 строк текста): векторный компаратор здесь слеп по построению.",
   "В версии B рядом с «Фундамент под оборудование Фпк-3, Фпк-4. Опалубка» добавлен второй чертёж «Фундамент под оборудование Фпк-2. Армирование».", "high"),
 "EOM-7fef43a3": (["object_added", "with_labels"], "GRAPHIC_CHANGE", 2,
   "Добавлены выноска с полочкой и сноска: графика (линия-выноска) и текст меняются вместе — граница между текстовой и графической правкой.",
   "В версии B добавлены выноска с подписью «Закладная гильза для кабеля обогрева» и сноска «* Зазор между трубой и гильзой необходимо заделать мягким водонепроницаемым материалом».", "high"),
 "EOM-46355862": (["object_added", "dense_block", "with_labels"], "GRAPHIC_CHANGE", 15,
   "Добавлена штриховка примерно в 15 помещениях: тысячи новых сегментов, но объектов — десятки.",
   "В версии B появилась штриховка (заливка) примерно в 15 помещениях плюс новые выноски с марками; контуры стен и осей те же.", "medium"),
 "SS-a369f492": (["object_added", "object_moved", "raster_graphics", "with_labels"], "GRAPHIC_CHANGE", 2,
   "Один узел добавлен, второй сдвинут вниз — смешанный случай; в блоке ещё и растровые вставки (6 → 24).",
   "В версии B добавлен «Узел А прохода группы кабелей в лотке через строительные конструкции», а существующий «Узел В прохода кабеля в гильзе…» сдвинут вниз.", "high"),
 "VK-148ffe6c": (["object_added", "object_moved", "rotated_page", "dense_block"], "GRAPHIC_CHANGE", 3,
   "Повёрнутая страница (/Rotate 90) + 106 552 сегмента + три разнородные правки одновременно.",
   "В версии B добавлены спецификация оборудования справа и узел «Узел кранов с эл. приводом»; узел «Узел гиг. душа» сдвинут вправо. Сегментов 106 552 → 115 081.", "medium"),
 "OV-93cc012f": (["object_added", "object_moved", "with_labels"], "GRAPHIC_CHANGE", 4,
   "Часть чертежа сдвинута относительно остальной (нежёсткое смещение) — глобальная регистрация одним сдвигом здесь принципиально не работает.",
   "В версии B добавлены выноски-«шарики» 5 (33) и 1 (33) (три штуки), а верхняя часть трассы теплоснабжения сдвинута относительно нижней.", "medium"),
 "AR-55eda7fb": (["small_local_change", "table_only_change", "with_labels"], "GRAPHIC_CHANGE", 1,
   "Изменение занимает ≈0.5 % площади блока и спрятано в одной строке таблицы; число и эскиз изменились согласованно (D6/D7 из брифа контрфактов, но настоящий).",
   "В таблице «Перемычки 39 этаж» изменён эскиз строки ПР-14: было L=850 (поз. 10) с размерами 150/550/150, стало L=1000 (поз. 8) со штриховкой стены справа. Остальные строки не изменились.", "high"),
 "AR-577a293f": (["small_local_change", "table_only_change", "with_labels"], "GRAPHIC_CHANGE", 3,
   "Три строки таблицы изменены локально при неизменной сетке — проверка локализации, а не факта изменения.",
   "В «Ведомости полов 17-го этажа» изменены эскизы и составы строк 2.1, 2.4 и 2.7 (число слоёв и толщины). Сетка таблицы и остальные строки не изменились.", "medium"),
 "EOM-14558cda": (["object_added", "occluding_fill", "with_labels"], "GRAPHIC_CHANGE", 1,
   "В одной версии весь вид закрыт сплошной чёрной заливкой при почти том же числе сегментов (7 932 / 7 767): различается ровно один перекрывающий объект, а попиксельная разница 14 %.",
   "В версии A вид «Внешний вид УЭРМ» полностью закрыт сплошной чёрной заливкой; в версии B тот же вид виден нормально. Подписи и отметки вокруг совпадают.", "medium"),
 "AR-5acaab0e": (["object_added", "dense_block", "hatch_noise", "with_labels"], "GRAPHIC_CHANGE", 3,
   "111 161 сегмент, штриховка сдвинута по фазе и даёт красно-синий шум по всей площади, поверх которого лежит настоящая правка.",
   "В версии B достроен объём стен в правой верхней части (штриховка расширена вправо) и добавлен заголовок сверху; остальная штриховка совпадает по смыслу, но сдвинута по фазе.", "medium"),
 # ---------- prepared-block SCOPE differs between versions (A is contained in B) ---
 "AR-490254e9": (["block_scope_change", "with_labels"], "NOT_COMPARABLE", None,
   "Границы подготовленного блока разъехались: в v002 блок покрывает только «Вид 1», в v003 — «Вид 1»…«Вид 4». Сравнивать как «один и тот же регион» нельзя, наивный компаратор объявит три добавленных чертежа.",
   "Содержимое стороны A целиком присутствует в стороне B («Вид 1»), но блок B охватывает ещё три вида. Это разница нарезки блоков, а не доказанная правка листа.", "high"),
 "OV-2cc2a382": (["block_scope_change", "raster_graphics", "no_labels"], "NOT_COMPARABLE", None,
   "Крайний случай той же болезни: A — узкая полоска с эскизом установки (0 текстовых строк, 1 растровая вставка), B — целый опросный лист поставщика с тем же эскизом внутри (65 строк, 7 вставок).",
   "Эскиз установки из A присутствует в B; сторона B дополнительно содержит весь бланк «ТЕХНИЧЕСКИЕ ДАННЫЕ № РА26-005271-05» с таблицами. Разница нарезки блоков, а не правка.", "high"),

 # ---------- deliberately kept as NOT ground truth --------------------------------
 "EOM-1db297d2": (["uncertain", "block_match_failure"], "UNCERTAIN", None,
   "Сопоставитель блоков связал РАЗНЫЕ блоки: слева «Условные обозначения», справа «Проход кабелей через перегородки и перекрытия». Пара показывает отказ сопоставления, а не изменение чертежа.",
   "Содержимое сторон не соответствует друг другу — сравнивать нечего; как эталон не использовать.", "high"),
 "GP-6bc1c029": (["uncertain", "dense_block"], "UNCERTAIN", None,
   "Генплан на 378 441 сегмент: разница 1.5 % рассыпана мелкими пятнами, на экране блока не читается.",
   "Что именно изменилось — по картинке блока определить не удалось; нужна поэлементная сверка. Как эталон не использовать.", "medium"),
}


def main():
    rows = {}
    for name in ("mine_shortlist2.jsonl", "mine_extract.jsonl", "mine_extract2.jsonl", "mine_align2.jsonl"):
        p = ART / name
        if not p.exists():
            continue
        for l in open(p, encoding="utf-8"):
            r = json.loads(l)
            pid = r.get("pair_id")
            if not pid:
                continue
            if pid in rows and "EA" in rows[pid] and "EA" not in r:
                continue
            if pid in rows and "interior_components" in rows[pid] and "interior_components" not in r:
                continue
            rows[pid] = r
    out = []
    problems = []
    for pid, (classes, verdict, nobj, why, human, conf) in LABELS.items():
        r = rows.get(pid)
        if r is None:
            problems.append(f"{pid}: not found in artifacts")
            continue
        sha_a = F.pdf_sha256(r["pdf_a"]); sha_b = F.pdf_sha256(r["pdf_b"])
        if sha_a == sha_b:
            problems.append(f"{pid}: SAME PDF sha256 — dropped")
            continue
        # re-verify the blocks are real prepared graphic blocks
        ok_a = any(b.block_id == r["block_a"] for b in F.iter_prepared_blocks(
            str(Path(r["pdf_a"]).with_name("result.json"))))
        ok_b = any(b.block_id == r["block_b"] for b in F.iter_prepared_blocks(
            str(Path(r["pdf_b"]).with_name("result.json"))))
        if not (ok_a and ok_b):
            problems.append(f"{pid}: block id not found among prepared image blocks (a={ok_a} b={ok_b})")
            continue
        a2 = r["align2"]
        EA, EB = r.get("EA") or {}, r.get("EB") or {}
        out.append({
            "pair_id": pid,
            "classes": classes,
            "expected_verdict": verdict,
            "expected_changed_objects": nobj,
            "why_hard_ru": why,
            "human_expected_ru": human,
            "label_confidence": conf,
            "discipline": r["discipline"], "obj_id": r["obj_id"], "doc_id": r["doc_id"],
            "side_a": {
                "pdf": r["pdf_a"], "result_json": str(Path(r["pdf_a"]).with_name("result.json")),
                "version": r["ver_a"], "page_number": r["page_a"], "page_index": r["page_index_a"],
                "block_id": r["block_a"], "coords_px": r["coords_a"], "page_px": r["page_px_a"],
                "rotation": r["rot_a"], "category_code": r["cat_a"], "shape_type": r["shape_a"],
                "clip_pt": r["clip_pt_a"], "sha256": sha_a,
                "segments": EA.get("segments"), "text_lines": EA.get("n_text_lines"),
                "raster_inserts": EA.get("n_images"), "quality": EA.get("quality"),
            },
            "side_b": {
                "pdf": r["pdf_b"], "result_json": str(Path(r["pdf_b"]).with_name("result.json")),
                "version": r["ver_b"], "page_number": r["page_b"], "page_index": r["page_index_b"],
                "block_id": r["block_b"], "coords_px": r["coords_b"], "page_px": r["page_px_b"],
                "rotation": r["rot_b"], "category_code": r["cat_b"], "shape_type": r["shape_b"],
                "clip_pt": r["clip_pt_b"], "sha256": sha_b,
                "segments": EB.get("segments"), "text_lines": EB.get("n_text_lines"),
                "raster_inserts": EB.get("n_images"), "quality": EB.get("quality"),
            },
            "same_pdf_file": False,
            "screen_signals": {
                "iou_coords_norm": r["iou_norm"],
                "diff_frac_block_equal_scale": a2["diff_frac_block"],
                "iou_dil_equal_scale": a2["iou_dil"],
                "registration_shift_pt": a2["shift_pt"],
                "n_components_big": a2["n_components_big"],
                "interior_components_big": r.get("int_n_big"),
                "interior_diff_frac_block": r.get("int_frac_block"),
                "only_a_px": a2["only_a"], "only_b_px": a2["only_b"],
                "text_jaccard": r.get("text_jaccard"),
                "seg_ratio": r.get("seg_ratio"),
                "sheet_confirmed_by_stamp": r.get("sheet_confirmed"),
            },
            "crop_png": f"artifacts/mine_crops/{pid}.png",
        })
    doc = {
        "schema_version": "mine_pairs/1",
        "research_only": True,
        "track": "VECTOR 0.3 / probe mine",
        "unit": "already prepared graphic block (block_type == 'image') from result.json",
        "pair_source": "cross-revision (same document, consecutive versions)",
        "cross_stage_P_to_RD_slots": {
            "n_slots_filled": 0,
            "status": "НЕТ ПАР",
            "source_checked": "artifacts/pd_block_pairs.json (зонд pd того же трека)",
            "reason": ("pd проверил 124 кандидата визуально и подтвердил 0 кросс-стадийных пар блоков: "
                       "единственный документ корпуса со стадией П (133/23-ГК.ЭС) не имеет читаемого РД-двойника "
                       "(у РД-альбомов того же предмета нет document.pdf, облачные кропы отдают 404). "
                       "pd подтвердил 7 пар на запасной оси cross_document_same_stage (Р↔Р) — они принадлежат pd, "
                       "здесь не дублируются."),
        },
        "guards": {
            "no_self_pairs": "sha256(pdf_a) != sha256(pdf_b) verified for every pair",
            "no_synthetic_bbox": "coords_px copied from result.json, verified present among prepared image blocks",
            "single_annotator": True,
        },
        "n_pairs": len(out),
        "problems": problems,
        "pairs": out,
    }
    (ART / "mine_pairs.json").write_text(json.dumps(doc, ensure_ascii=False, indent=1), encoding="utf-8")
    print("pairs:", len(out), "problems:", problems)


if __name__ == "__main__":
    main()
