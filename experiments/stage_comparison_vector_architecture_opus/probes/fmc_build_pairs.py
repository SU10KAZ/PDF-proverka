#!/usr/bin/env python3
"""FMC probe step 8 — build the failure-mode corpus manifest and render crops.

Every pair below was FOUND by the mining steps 1-7 (fmc_page_scan / fmc_candidates /
fmc_batch_diff / fmc_page_profile / fmc_broken_text / fmc_rotation_candidates), then looked at.
The manifest has the same shape as
experiments/stage_comparison_vector_blocks/artifacts/block_pairs.json so it is directly runnable by
`python -m experiments.stage_comparison_vector_blocks.run_research --manifest <this file>`.

    python -m experiments.stage_comparison_vector_architecture_opus.probes.fmc_build_pairs
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import fitz

ROOT = Path(__file__).resolve().parents[3]
ART = Path(__file__).resolve().parents[1] / "artifacts"
CROPS = ART / "fmc_crops"

OBJ = "projects_v2/objects/214_Alia_ASTERUS/disciplines"


def pdf(disc: str, doc: str, ver: str) -> str:
    return f"{OBJ}/{disc}/documents/{doc}/versions/{ver}/02_work/document.pdf"


# (pair_id, discipline, type, change_class, why_hard, human_expected_ru, human_expected_status,
#  left(doc,ver,page,bbox), right(doc,ver,page,bbox))
CASES: list[dict] = [
    dict(
        pair_id="fmc_eom_text_as_paths",
        discipline="EOM",
        type="общие указания; текст в кривых на одной стороне",
        change_class="TEXT_LAYER_ABSENT_ONE_SIDE",
        why_hard="в v001 весь текст листа выведен кривыми (0 текстовых спанов, 92135 команд c), "
        "в v002 — настоящий текстовый слой; любая текстовая метрика сравнивает пустое множество с 1796 спанами, "
        "а геометрия сравнивает контуры букв с линиями рамки",
        human_expected="Лист «Общие данные (окончание)» тот же; в новой версии текст стал выбираемым (в старой — в кривых), "
        "текст переверстан, в п.2 добавлена ссылка на отдельный том 13АВ-РД-ГРЩ2-ПА, в правом нижнем углу добавлен QR-код.",
        human_status="STRUCTURE_SAME_VALUES_CHANGED",
        left=("EOM", "13АВ-РД-ЭМ2-ПА V1", "v001", 5, [0.055, 0.015, 0.545, 0.985]),
        right=("EOM", "13АВ-РД-ЭМ2-ПА V1", "v002", 5, [0.055, 0.015, 0.545, 0.985]),
    ),
    dict(
        pair_id="fmc_kj_spec_table_reflow",
        discipline="KJ",
        type="таблица спецификации арматуры",
        change_class="LAYOUT_ONLY_NO_ENGINEERING_CHANGE",
        why_hard="текст листа побайтово одинаков (text_sha совпал), но строка «12-Г-1» подтянута вплотную к первой, "
        "то есть вся сетка таблицы и её содержимое сдвинуты по вертикали; покрытие сегментов обязано просесть, "
        "хотя инженерного изменения нет",
        human_expected="Изменений по существу нет: в спецификации элементов армирования убран разрыв между строками, "
        "позиции 12-поз.м., 12-Г-1, 16-П-1, их диаметры, длины, количества и массы прежние.",
        human_status="NEAR_IDENTICAL",
        left=("KJ", "13АВ-РД-КЖ6-К7", "v001", 13, [0.030, 0.030, 0.510, 0.290]),
        right=("KJ", "13АВ-РД-КЖ6-К7", "v002", 14, [0.030, 0.030, 0.510, 0.290]),
    ),
    dict(
        pair_id="fmc_kj_steel_table_shift",
        discipline="KJ",
        type="таблица «Общая ведомость расхода стали»",
        change_class="PURE_TRANSLATION",
        why_hard="та же таблица целиком сдвинута вверх примерно на 0.09 высоты листа; внутри блока с фиксированным "
        "bbox нормализация сдвиг НЕ убирает — это чистая проверка на инвариантность к переносу объекта",
        human_expected="Ведомость расхода стали не изменилась (Ø12 3506,33 кг, Ø16 935,90 кг, всего 4442,23 кг) — таблица только смещена по листу.",
        human_status="NEAR_IDENTICAL",
        left=("KJ", "13АВ-РД-КЖ6-К7", "v001", 13, [0.040, 0.320, 0.360, 0.630]),
        right=("KJ", "13АВ-РД-КЖ6-К7", "v002", 14, [0.040, 0.320, 0.360, 0.630]),
    ),
    dict(
        pair_id="fmc_eom_room_schedule_values",
        discipline="EOM",
        type="экспликация помещений (таблица) на листе плана",
        change_class="VALUES_CHANGED",
        why_hard="изменились три числа из сотен (17,68→17,37; 2,67→2,65; 497,57→497,24); сетка таблицы идентична, "
        "поэтому геометрическое покрытие ≈1.0, а нужное предложение эксперта состоит именно из этих чисел",
        human_expected="В экспликации помещений уточнены площади: лестничная клетка 17,68→17,37 м², кладовая 3.К.8 2,67→2,65 м², итог по этажу 497,57→497,24 м².",
        human_status="STRUCTURE_SAME_VALUES_CHANGED",
        left=("EOM", "13АВ-РД-ЭМ-К3", "v001", 20, [0.715, 0.030, 0.995, 0.830]),
        right=("EOM", "13АВ-РД-ЭМ-К3", "v002", 21, [0.715, 0.030, 0.995, 0.830]),
    ),
    dict(
        pair_id="fmc_eom_tray_plan_geometry",
        discipline="EOM",
        type="план кабельных лотков; плотный CAD-план",
        change_class="GEOMETRY_CHANGED_PLUS_NEW_MARKS",
        why_hard="перестроены участки стен и воздуховодов и добавлены отметки h=2650 и h=3350; "
        "на плотном плане это тонет в фоне — 3 000+ линий одинакового вида",
        human_expected="На плане кабельных лотков −1 этажа перестроен участок в осях 3.И–3.Л (изменены контуры венткамеры и короба) и проставлены отметки высоты h=2650 и h=3350.",
        human_status="STRUCTURE_CHANGED",
        left=("EOM", "13АВ-РД-ЭМ-К3", "v001", 20, [0.080, 0.220, 0.450, 0.850]),
        right=("EOM", "13АВ-РД-ЭМ-К3", "v002", 21, [0.080, 0.220, 0.450, 0.850]),
    ),
    dict(
        pair_id="fmc_eom_notes_reflow",
        discipline="EOM",
        type="лист общих указаний, сплошной текст",
        change_class="TEXT_REFLOW_NO_ENGINEERING_CHANGE",
        why_hard="абзацы переверстаны: каждый спан сместился, а содержательно исправлена только опечатка; "
        "text_sha страницы совпал, но координаты всех 600+ спанов разные",
        human_expected="Содержание указаний не изменилось — абзацы переверстаны, исправлена ссылка «ПУЭ, гл. 1.7, п. 1.7.76».",
        human_status="NEAR_IDENTICAL",
        left=("EOM", "13АВ-РД-ЭМ-К6", "v001", 6, [0.370, 0.090, 0.670, 0.890]),
        right=("EOM", "13АВ-РД-ЭМ-К6", "v002", 7, [0.370, 0.090, 0.670, 0.890]),
    ),
    dict(
        pair_id="fmc_eom_qr_stamp_only",
        discipline="EOM",
        type="угол листа со штампом",
        change_class="ADDED_NON_ENGINEERING_MARK",
        why_hard="единственное отличие всего листа — добавленный QR-код (одна область 0.9-0.98 x 0.60-0.71); "
        "это заметное геометрическое изменение, которое эксперту сообщать НЕ нужно",
        human_expected="Инженерных изменений нет: на листе появился QR-код системы документооборота.",
        human_status="NEAR_IDENTICAL",
        left=("EOM", "13АВ-РД-ЭМ-К5", "v001", 39, [0.860, 0.540, 0.995, 0.760]),
        right=("EOM", "13АВ-РД-ЭМ-К5", "v002", 40, [0.860, 0.540, 0.995, 0.760]),
    ),
    dict(
        pair_id="fmc_ov_raster_retile",
        discipline="OV",
        type="растровый блок (скан вставлен в лист)",
        change_class="RASTER_REPACK_NO_CHANGE",
        why_hard="слева содержимое собрано из 274 растровых плиток, справа — из 1 изображения; "
        "векторных примитивов в блоке нет вообще, поэтому вся модель VectorBlockDescription пуста с обеих сторон",
        human_expected="Инженерных изменений нет: тот же вставленный растровый лист, изменена только его нарезка на изображения внутри PDF.",
        human_status="IDENTICAL",
        left=("OV", "13АВ-РД-ОВ1.1-К2 V1", "v001", 71, [0.050, 0.050, 0.850, 0.700]),
        right=("OV", "13АВ-РД-ОВ1.1-К2 V1", "v002", 73, [0.050, 0.050, 0.850, 0.700]),
    ),
    dict(
        pair_id="fmc_tx_raster_scan",
        discipline="TX",
        type="полностью растровый лист (0 текстовых спанов, 0 шрифтов)",
        change_class="RASTER_ONLY_UNCHANGED",
        why_hard="лист не содержит ни векторной графики, ни текста — только 2 изображения; "
        "любой векторный дескриптор здесь пустой и обязан честно сказать «нет данных», а не «идентично»",
        human_expected="Лист — растровый скан, изменений нет.",
        human_status="IDENTICAL",
        left=("TX", "13АВ-РД-ТХ3 V1", "v001", 10, [0.050, 0.050, 0.950, 0.900]),
        right=("TX", "13АВ-РД-ТХ3 V1", "v002", 10, [0.050, 0.050, 0.950, 0.900]),
    ),
    dict(
        pair_id="fmc_ss_a4_to_a3_reissue",
        discipline="SS",
        type="лист, перевыпущенный с A4 на A3, и переставленный по порядку",
        change_class="FORMAT_AND_PAGE_ORDER_CHANGE",
        why_hard="ширина листа 595→1191 pt при полностью совпадающем НАБОРЕ слов (word-set Jaccard = 1.0), лист "
        "переехал со стр. 31 на стр. 44, таблица переставлена из-под текста вправо и выросла на 7 строк; "
        "нормализация по bbox не убирает смену пропорций листа",
        human_expected="Лист «Расчёт сечения кабеля» перевыпущен с A4 на A3: таблица расчёта линий оповещения расширена "
        "с 14 до 21 линии (добавлены V15–V21) и все значения пересчитаны (V1: длина 390→230 м, мощность 120→84 Вт, "
        "расчётное сечение 0,819→0,338 мм²); таблица перенесена из-под текста вправо; лист переставлен с 31-й на 44-ю страницу.",
        human_status="STRUCTURE_CHANGED",
        left=("SS", "13АВ-РД-СОУЭ-ПА", "v001", 30, [0.050, 0.030, 0.960, 0.960]),
        right=("SS", "13АВ-РД-СОУЭ-ПА", "v002", 43, [0.050, 0.030, 0.960, 0.960]),
    ),
    dict(
        pair_id="fmc_ov_block_split_widened",
        discipline="OV",
        type="лист узлов ОВ, расширен с A1 до A0; блок разрезан на несколько",
        change_class="ONE_TO_N_BLOCK_SPLIT",
        why_hard="в v001 весь чертёж — один image-блок, в v002 — три блока на более широком листе (1684→2526 pt); "
        "сопоставление 1↔1 в принципе не может выразить такое соответствие",
        human_expected="Лист плана фреонопроводов перевыпущен на более широком формате (1684→2526 pt) и разбит на три блока; "
        "часть участков увеличена с ø15,9х0,89 до ø19,1х0,89, добавлены подписи «Подъём на 3 этаж» / «Подъём на 4 этаж», "
        "проставлены привязки (120, 250, 270, 390, 430, 480, 1130, 1820) и к огнезащитному коробу добавлена ссылка «см. раздел АР».",
        human_status="STRUCTURE_CHANGED",
        left=("OV", "13АВ-РД-ОВ1.2-К1_V1", "v001", 16, [0.100, 0.080, 0.400, 0.560]),
        right=("OV", "13АВ-РД-ОВ1.2-К1_V1", "v002", 19, [0.100, 0.080, 0.400, 0.560]),
    ),
    dict(
        pair_id="fmc_eom_drawing_list_rows",
        discipline="EOM",
        type="ведомость рабочих чертежей (таблица), формат A4x3→A4x4",
        change_class="TABLE_ROWS_ADDED",
        why_hard="в таблицу добавлены 4 строки (листы 43–46), из-за чего сдвинулись все нижележащие строки, "
        "и одновременно вырос формат листа; правильный ответ — «добавлено 4 листа», а не «сдвинулась сетка»",
        human_expected="В ведомость рабочих чертежей добавлены листы 43–46 (компоновка УЭРВ и наполняемость гильз, планы кабельных лотков на −1 этаже, в техпространстве и на кровле); формат листа изменён с A4x3 (630х297) на A4x4 (840х297).",
        human_status="STRUCTURE_CHANGED",
        left=("EOM", "13АВ-РД-ЭМ-К1", "v001", 5, [0.030, 0.030, 0.970, 0.970]),
        right=("EOM", "13АВ-РД-ЭМ-К1", "v002", 5, [0.030, 0.030, 0.970, 0.970]),
    ),
    dict(
        pair_id="fmc_eom_layout_reorg_mismatch",
        discipline="EOM",
        type="один и тот же лист однолинейной схемы, полностью перекомпонованный",
        change_class="LAYOUT_REORGANISED_COORD_MATCH_INVALID",
        why_hard="лист перекомпонован, поэтому одни и те же нормализованные координаты блока показывают РАЗНЫЕ объекты: "
        "слева таблица «Потребность кабелей и проводов» и схемы сигнализации, справа «План расстановки панелей ВРУ-НП6»; "
        "правильный ответ — «блоки не сопоставимы», а не «структура изменилась»",
        human_expected="Сравнивать нечего: в этих границах листа слева находится таблица потребности кабелей и схемы "
        "сигнализации, справа — план расстановки панелей ВРУ-НП6. Требуется сопоставление по объектам, а не по координатам.",
        human_status="STRUCTURE_CHANGED",
        left=("EOM", "13АВ-РД-ЭМ-К7 V1", "v001", 11, [0.640, 0.490, 0.830, 0.800]),
        right=("EOM", "13АВ-РД-ЭМ-К7 V1", "v002", 12, [0.640, 0.490, 0.830, 0.800]),
    ),
    dict(
        pair_id="fmc_eom_cable_table_values",
        discipline="EOM",
        type="таблица «Потребность кабелей и проводов», выровненная по содержимому",
        change_class="ENGINEERING_VALUES_CHANGED",
        why_hard="та же таблица найдена на обеих сторонах по её заголовку и обрезана по содержимому (bbox сдвинут на 0.155 "
        "по X, потому что таблица переехала); сетка идентична, изменилась одна строка марки кабеля — "
        "ровно та фраза, ради которой строится весь конвейер",
        human_expected="В потребности кабелей марка питающего кабеля изменена с 4х(1х120)+1х70 на 4х(1х185)+1х95 "
        "(объём 140 м прежний); остальные строки и итоги 520 / 35 м не изменились.",
        human_status="STRUCTURE_SAME_VALUES_CHANGED",
        left=("EOM", "13АВ-РД-ЭМ-К7 V1", "v001", 11, [0.680, 0.620, 0.825, 0.995]),
        right=("EOM", "13АВ-РД-ЭМ-К7 V1", "v002", 12, [0.823, 0.628, 0.968, 0.999]),
    ),
    dict(
        pair_id="fmc_gp_section_hatch_dims",
        discipline="GP",
        type="сечение лестничного схода на генплане + условные обозначения",
        change_class="DIMENSIONS_AND_HATCH_ADDED_PLUS_LEGEND_SHIFT",
        why_hard="в одном блоке сразу три разнородных изменения: подпись типа тротуара, две новые размерные "
        "цепочки 0,12 и новая штриховая зона; плюс легенда генплана въехала в границы блока, потому что "
        "весь лист сместился — размерная правка тонет в 12 575 примитивах",
        human_expected="На сечении 1–1 лестничного схода тип тротуара изменён с Р4.3 на Р4.2, проставлены высоты "
        "ступеней 0,12 м, справа добавлена зона существующего асфальтового покрытия (новая штриховка); "
        "условные обозначения генплана сместились и попали в границы блока.",
        human_status="STRUCTURE_SAME_VALUES_CHANGED",
        left=("GP", "13АВ-РД-ГП2", "v001", 7, [0.270, 0.550, 0.500, 0.985]),
        right=("GP", "13АВ-РД-ГП2", "v002", 7, [0.270, 0.550, 0.500, 0.985]),
    ),
    dict(
        pair_id="fmc_ov_page_shift_geometry",
        discipline="OV",
        type="лист ОВ, переехавший со стр. 186 на стр. 134; текст листа идентичен",
        change_class="PAGE_ORDER_SHIFT_PLUS_GEOMETRY",
        why_hard="блок — аэродинамическая характеристика вентилятора, ВСТАВЛЕННАЯ РАСТРОМ; внутри рамки блока "
        "всего 2 векторных примитива и 0 текстовых спанов, поэтому подобранный вентилятор и рабочая точка "
        "не видны ни геометрии, ни тексту; страница вдобавок переехала на 52 позиции",
        human_expected="Подобран другой вентилятор: VO-PatAIR-Kp-5-6/9-5,5-2 заменён на VO-PatAIR-Kp-5-6/9-3-2-V1; "
        "рабочая точка сместилась Pv 795→757 Па, Ps 633→692 Па (кривая характеристики другая). Лист переставлен "
        "с 186-й на 134-ю страницу.",
        human_status="STRUCTURE_CHANGED",
        left=("OV", "13АВ-РД-ОВ2-К1 V1", "v001", 185, [0.290, 0.120, 0.820, 0.400]),
        right=("OV", "13АВ-РД-ОВ2-К1 V1", "v002", 133, [0.290, 0.120, 0.820, 0.400]),
    ),
    dict(
        pair_id="fmc_ar_hatch_sections",
        discipline="AR",
        type="план кровли АР с плотной штриховкой и облаками ревизии",
        change_class="DENSE_HATCH_CHANGED",
        why_hard="на листе 32 675 линейных команд, из них ~4 700 под 135° — это штриховка; "
        "hatch_like_structures упирается в свой предел и не различает «поменялся материал» и «сдвинулся контур»",
        human_expected="На плане кровли пристройки К4/К5 исправлена отметка +59,935 → +4,935; уклоны кровли переназначены "
        "(1,63 %→2 %, 2,54 %→3 %, 3,72 %→4 %); добавлены ходовые дорожки и две шахты ОВ с привязками; в условные "
        "обозначения добавлены утеплители У2 (100 мм), У4 (50 мм) и штукатурка Ш1 (10 мм); размеры фрагмента 1 "
        "уточнены (1070→1080, 770→740, 530→500); изменения обведены облаками ревизии.",
        human_status="STRUCTURE_CHANGED",
        left=("AR", "13АВ-РД-АР3-К4-К5", "v001", 5, [0.035, 0.020, 0.470, 0.970]),
        right=("AR", "13АВ-РД-АР3-К4-К5", "v002", 5, [0.035, 0.020, 0.470, 0.970]),
    ),
    dict(
        pair_id="fmc_vk_spec_positions",
        discipline="VK",
        type="спецификация ВК: проставлены номера позиций",
        change_class="SMALL_REAL_TEXT_CHANGE",
        why_hard="изменение занимает 0.09 % площади листа — шесть номеров позиций 81–86 в пустых ячейках; "
        "любая метрика, усредняющая по блоку, назовёт это шумом",
        human_expected="В спецификации проставлены номера позиций 81–86 для тройников 45°/67,3° и заглушек DN50/DN110; сами изделия не изменились.",
        human_status="STRUCTURE_SAME_VALUES_CHANGED",
        left=("VK", "13АВ-РД-ВК.КВ-К4_V1", "v001", 22, [0.030, 0.080, 0.350, 0.350]),
        right=("VK", "13АВ-РД-ВК.КВ-К4_V1", "v002", 22, [0.030, 0.080, 0.350, 0.350]),
    ),
    dict(
        pair_id="fmc_km_broken_text_swap",
        discipline="KM",
        type="лист КМ с нераскодируемыми глифами; листы 6 и 7 поменялись местами",
        change_class="UNDECODABLE_TEXT_PLUS_PAGE_SWAP",
        why_hard="в тексте листа есть символы, которые PyMuPDF не может отобразить в Unicode "
        "(U+FFFD / PUA), поэтому текстовый слой частично ложный; вдобавок листы 6 и 7 в новой версии переставлены",
        human_expected="Лист переставлен (был 7-м, стал 8-м); отметка верха конструкций изменена с +55,850 на +52,400, "
        "переразмерены траверсы Тр-1/Тр-2/Тр-3 (600→85, 165→55, 35→225, 565→190) и добавлена ссылка «см. л. 2».",
        human_status="STRUCTURE_SAME_VALUES_CHANGED",
        left=("KM", "13АВ-РД-КМ-К4", "v001", 6, [0.040, 0.040, 0.700, 0.900]),
        right=("KM", "13АВ-РД-КМ-К4", "v002", 7, [0.040, 0.040, 0.700, 0.900]),
    ),
    dict(
        pair_id="fmc_eom_rotated_labels",
        discipline="EOM",
        type="принципиальная схема этажного щита; вертикальные подписи развёрнуты",
        change_class="ORIENTATION_FLIPPED",
        why_hard="263 текстовых спана шли под +90°, стали идти под −90° (развёрнуты на 180°); "
        "мультимножество строк почти не меняется, а bbox и rotation каждого спана меняются полностью",
        human_expected="Схема этажного щита перевыпущена на большем формате и перекомпонована: вертикальные подписи развёрнуты, добавлены сведения по автоматам C25 и УЗО 40А/100 мА.",
        human_status="STRUCTURE_CHANGED",
        left=("EOM", "13АВ-РД-ЭМ-К4", "v001", 8, [0.030, 0.050, 0.520, 0.950]),
        right=("EOM", "13АВ-РД-ЭМ-К4", "v002", 10, [0.030, 0.050, 0.520, 0.950]),
    ),
    dict(
        pair_id="fmc_crop_mismatch_same_sheet",
        discipline="EOM",
        type="один и тот же лист, но разные границы блока (несовпадение кропа)",
        change_class="CROP_MISMATCH_NO_CHANGE",
        why_hard="контент одинаков побайтово (тот же PDF, та же страница), но правая рамка блока сдвинута на +0.06 по X "
        "и +0.04 по Y и уменьшена (0.320→0.290 по ширине, 0.550→0.540 по высоте); правильный ответ — «изменений нет», "
        "а нормализация по bbox превращает сдвиг в изменение масштаба",
        human_expected="Изменений нет: это один и тот же фрагмент листа, взятый с другими границами блока.",
        human_status="IDENTICAL",
        left=("EOM", "13АВ-РД-ЭМ-К3", "v002", 21, [0.100, 0.250, 0.420, 0.800]),
        right=("EOM", "13АВ-РД-ЭМ-К3", "v002", 21, [0.160, 0.290, 0.450, 0.830]),
    ),
]


def block_id(side: tuple, pair_id: str, name: str) -> str:
    key = f"{pair_id}|{name}|{side[1]}|{side[2]}|{side[3]}|{side[4]}"
    return "fmcblk_" + hashlib.sha256(key.encode()).hexdigest()[:24]


def main() -> None:
    CROPS.mkdir(parents=True, exist_ok=True)
    pairs = []
    docs: dict[str, fitz.Document] = {}
    for case in CASES:
        entry = {
            "pair_id": case["pair_id"],
            "discipline": case["discipline"],
            "type": case["type"],
            "change_class": case["change_class"],
            "why_hard": case["why_hard"],
            "reason": case["why_hard"],
            "human_expected_ru": case["human_expected"],
            "human_expected": case["human_status"],
        }
        for name in ("left", "right"):
            disc, doc, ver, page, bbox = case[name]
            rel = pdf(disc, doc, ver)
            if rel not in docs:
                docs[rel] = fitz.open(ROOT / rel)
            d = docs[rel]
            p = d[page]
            entry[name] = {
                "version": ver,
                "pdf": rel,
                "page_index": page,
                "block_id": block_id(case[name], case["pair_id"], name),
                "bbox_norm": bbox,
            }
            clip = fitz.Rect(bbox[0] * p.rect.width, bbox[1] * p.rect.height,
                             bbox[2] * p.rect.width, bbox[3] * p.rect.height)
            scale = min(3.0, 1400.0 / max(clip.width, clip.height))
            pm = p.get_pixmap(matrix=fitz.Matrix(scale, scale), clip=clip)
            pm.save(str(CROPS / f"{case['pair_id']}_{name}.png"))
        pairs.append(entry)
    manifest = {
        "schema_version": "vector-block-benchmark-v0.1",
        "research_only": True,
        "selection_method": (
            "Mined from projects_v2: 210 PDFs of 98 documents with 2+ versions scanned for cheap page "
            "descriptors (fmc_page_scan), pages matched across versions by word-set Jaccard (fmc_candidates), "
            "3096 matched page pairs raster-diffed at 900 px (fmc_batch_diff), plus font/ToUnicode/image "
            "profiling (fmc_page_profile, fmc_find_broken_text) and text-orientation deltas "
            "(fmc_find_rotation_hatch). Each surviving case was rendered and read by hand."
        ),
        "pairs": pairs,
    }
    (ART / "fmc_pairs.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {ART/'fmc_pairs.json'} with {len(pairs)} pairs; crops in {CROPS}")


if __name__ == "__main__":
    main()
