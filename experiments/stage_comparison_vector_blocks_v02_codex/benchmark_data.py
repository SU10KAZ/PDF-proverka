"""Explicit manually reviewed benchmark pairs and human ground truth.

Candidate metadata was shortlisted by page/bbox, but every pair below was
visually inspected side-by-side.  This module does not perform block matching.
"""
from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any


EXPERIMENT_DIR = Path(__file__).resolve().parent
REPOSITORY_ROOT = EXPERIMENT_DIR.parents[1]
BASELINE_MANIFEST = REPOSITORY_ROOT / "experiments/stage_comparison_vector_blocks/artifacts/block_pairs.json"

DOCS = {
    "SS": "projects_v2/objects/214_Alia_ASTERUS/disciplines/SS/documents/13AB-РД-СОТ-К7 V1",
    "VK": "projects_v2/objects/214_Alia_ASTERUS/disciplines/VK/documents/13АВ-РД-ВК.КВ-К4_V1",
    "AR": "projects_v2/objects/256_Primavera_K14_Spartak/disciplines/AR/documents/СТ26_01-14-АР0-АС-1-РД_V1",
    "OV": "projects_v2/objects/214_Alia_ASTERUS/disciplines/OV/documents/13АВ-РД-ОВ2-К7_V1",
}


def _side(discipline: str, version: str, page: int, block_id: str, bbox: list[float]) -> dict[str, Any]:
    return {
        "version": version,
        "pdf": f"{DOCS[discipline]}/versions/{version}/02_work/document.pdf",
        "page_index": page - 1,
        "block_id": block_id,
        "bbox_norm": bbox,
    }


def _pair(
    pair_id: str,
    discipline: str,
    page: int | tuple[int, int],
    left_id: str,
    right_id: str,
    left_bbox: list[float],
    right_bbox: list[float],
    *,
    kind: str,
    expected_route: str,
    expected_verdict: str,
    facts: list[str],
) -> dict[str, Any]:
    pages = (page, page) if isinstance(page, int) else page
    versions = ("v002", "v003") if discipline == "SS" else ("v001", "v002")
    return {
        "pair_id": pair_id, "discipline": discipline, "type": kind,
        "selection": "Manual same-semantic-block pairing after side-by-side crop inspection.",
        "left": _side(discipline, versions[0], pages[0], left_id, left_bbox),
        "right": _side(discipline, versions[1], pages[1], right_id, right_bbox),
        "ground_truth": {
            "expected_route": expected_route, "expected_verdict": expected_verdict,
            "important_factual_changes": facts,
        },
    }


BASE_GROUND_TRUTH = {
    "ss_scheme_text_changed": ("VECTOR_WITH_VISION", "STRUCTURE_SAME_VALUES_CHANGED", ["ОСПД2.1 → ОСПД1.1", "Удалено слово «выезда» в подписи контроля паркинга."]),
    "ss_plan_dense": ("VECTOR_WITH_VISION", "NEAR_IDENTICAL", ["Смысловая геометрия плана совпадает; высота crop немного различается."]),
    "ss_simple_node": ("VECTOR_OK", "IDENTICAL", ["Графика и подписи узла совпадают точно."]),
    "ss_table_graphic": ("VECTOR_WITH_VISION", "NEAR_IDENTICAL", ["Узел крепления совпадает; границы crop и разбиение текста различаются."]),
    "ar_plan": ("VECTOR_WITH_VISION", "NEAR_IDENTICAL", ["План и ревизионные отметки совпадают; блок плотный и capped."]),
    "ar_wall_sections": ("VECTOR_WITH_VISION", "NEAR_IDENTICAL", ["Повторяющиеся разрезы стен и подписи визуально совпадают; segment/topology caps требуют Hybrid."]),
    "vk_plan": ("VECTOR_WITH_VISION", "NEAR_IDENTICAL", ["Геометрия плана совпадает; embedded-font text не читается надёжно."]),
    "vk_nodes": ("VECTOR_WITH_VISION", "STRUCTURE_SAME_VALUES_CHANGED", ["Справа добавлены примечания и отметка −0,034; основные узлы совпадают."]),
    "vk_node_plan": ("VECTOR_WITH_VISION", "NEAR_IDENTICAL", ["Геометрия узлов совпадает; текст требует Vision из-за font mapping."]),
    "eom_singleline_changed": ("VISION_ONLY", "CROP_MISMATCH", ["Semantic extents crop различаются, поэтому Vector verdict должен остановиться на CROP_MISMATCH.", "По Vision две обобщённые ветви заменены четырьмя явными QD/Wh/QF ветвями.", "Расчётные нагрузки 13/14/16/18 кВт заменены повторяющимся вариантом 13 кВт."]),
}


EXTRA_PAIRS = [
    _pair("ss_crop_mismatch_page07", "SS", 7, "blk_fbcc7cf80170404892bedddc7f5cc68e", "blk_634dd5b79f9346b5b9e4d01a5e59726f", [.015247285366,.003040969372,.804989531648,.989119887352], [.011965870857,.004747629166,.838002026081,.989980876446], kind="real dense plan; semantic crop mismatch", expected_route="VISION_ONLY", expected_verdict="CROP_MISMATCH", facts=["Версии показывают разные смысловые экстенты одного большого плана; прямое block-normalized сравнение недопустимо."]),
    _pair("ss_plan_page09", "SS", 9, "blk_3b3aec40658045fbb98ddb801e115b41", "blk_552c36eb68f949eeb46ca7b578f3283f", [.028638452291,.00943377614,.645064383745,.990422099829], [.028638452291,.00943377614,.645064383745,.990422099829], kind="dense plan; same crop", expected_route="VECTOR_WITH_VISION", expected_verdict="IDENTICAL", facts=["План, оси, подписи и exact vector signature совпадают; topology cap сохраняет Hybrid route."]),
    _pair("ss_plan_page11", "SS", 11, "blk_e1d399f4675849219328e15935bad316", "blk_bca79355a8d940258f6e13e33b450608", [.033347070217,.010010570288,.687705278397,.797078877687], [.033388227224,.0100068748,.687898606062,.796978324652], kind="curved plan; repeated labels", expected_route="VECTOR_OK", expected_verdict="NEAR_IDENTICAL", facts=["Криволинейный план и трасса совпадают; все шесть evidence gates проходят без caps."]),
    _pair("ss_plan_page12", "SS", 12, "blk_adc391730ffc4d2586f0006020d048ec", "blk_790d85eb4d304084abc9de2165787783", [.037466198206,.019195258617,.496481031179,.924373865128], [.037466198206,.019195258617,.496481031179,.924373865128], kind="tall plan", expected_route="VECTOR_WITH_VISION", expected_verdict="IDENTICAL", facts=["План, синяя трасса и exact vector signature совпадают; topology cap сохраняет Hybrid route."]),
    _pair("ss_plan_page13", "SS", 13, "blk_f99e8e966cea44aea69979480dabcba6", "blk_7680e3e8f14f457ba5f99af006a21af4", [.005198448896,.002940684557,.740782244072,.985914677382], [.005198448896,.002940684557,.745220520099,.985914677382], kind="long dense plan", expected_route="VECTOR_WITH_VISION", expected_verdict="NEAR_IDENTICAL", facts=["Длинный план и цветные трассы совпадают; правый crop немного шире."]),
    _pair("ss_plan_page14", "SS", 14, "blk_7f0f3c77addf44fc8656d6d8397ef376", "blk_5cf2e6a664ca4027b705c14f73735ead", [.015422077922,.00689331932,.835227272727,.992637982088], [.013230429989,.009363361163,.836824696803,.985493762426], kind="angled plan", expected_route="VECTOR_WITH_VISION", expected_verdict="NEAR_IDENTICAL", facts=["Наклонный план и синяя трасса совпадают; padding crop различается."]),
    _pair("ss_detail_page17", "SS", 17, "blk_546083b72a744614ac9dd0090cfb213a", "blk_14d0b1e2de3e48b38cd2e708d2d84b43", [.057725250721,.015084996819,.984821617603,.368088796735], [.057725250721,.015084996819,.984821617603,.368088796735], kind="simple details; hatches", expected_route="VECTOR_OK", expected_verdict="IDENTICAL", facts=["Три узла и их подписи визуально и по crop совпадают."]),
    _pair("ss_table_page19", "SS", 19, "blk_1e6b496d718740deb5bbc73ec0ca3306", "blk_26e345ae1c614eecad4b894e4d5193ba", [.045557409525,.014387667179,.994608968496,.787125170231], [.045557409525,.014387667179,.994608968496,.787125170231], kind="equipment table; text-heavy", expected_route="VECTOR_OK", expected_verdict="IDENTICAL", facts=["Строки спецификации и сетка таблицы визуально совпадают."]),

    _pair("vk_plan_page07", "VK", 7, "blk_4bf7cb4b5c8745e4b84c1765a798ba13", "blk_38634c774ac341408511ce0d252846e3", [.038392484188,.009733885527,.669744431973,.852668017149], [.035954266787,.009103685617,.669747918844,.854747205973], kind="plumbing plan", expected_route="VECTOR_WITH_VISION", expected_verdict="NEAR_IDENTICAL", facts=["Красные и синие трассы, помещения и оси совпадают."]),
    _pair("vk_plan_page08", "VK", 8, "blk_67633b17aa9d46dea652c99bad9743a6", "blk_e566e7f21b2140c98e7652f3eab75900", [.054941415787,.009542196989,.679214000702,.828260689974], [.053095191717,.008923023939,.678584843874,.83082357049], kind="plumbing plan", expected_route="VECTOR_WITH_VISION", expected_verdict="NEAR_IDENTICAL", facts=["Трассы и инженерные узлы плана совпадают."]),
    _pair("vk_plan_page10", "VK", 10, "blk_6a5de03a4afa40c7a7747adbf9c62109", "blk_63a6c0ccfcf94fb79c40ddf7e83ca94b", [.021403091558,.006732119656,.536266349584,.671528935705], [.02476683259,.011115521193,.536024821487,.64959409833], kind="two riser diagrams", expected_route="VECTOR_WITH_VISION", expected_verdict="NEAR_IDENTICAL", facts=["Две схемы стояков совпадают; вертикальный padding crop различается."]),
    _pair("vk_nodes_page11", "VK", 11, "blk_994c950db379410b86c701fd60e36e56", "blk_2cb30408c82044caa1fdce62b5c96a96", [.027095064521,.007588088512,.405974722027,.988243758678], [.026232466102,.007474780083,.410966481958,.986751258373], kind="multiple engineering nodes", expected_route="VECTOR_WITH_VISION", expected_verdict="NEAR_IDENTICAL", facts=["Набор схем и узлов совпадает; мелкий текст требует Vision."]),
    _pair("vk_diagrams_page16", "VK", 16, "blk_ac23f3fc7f7f42e7a61b4efe7bd2d4fc", "blk_d344edef80a8489fad5677986ef975fc", [.015643802647,.010219698728,.521058965102,.840570220391], [.013513513514,.005216565557,.518427518428,.860733316856], kind="riser diagrams", expected_route="VECTOR_WITH_VISION", expected_verdict="NEAR_IDENTICAL", facts=["Компоновка стояков и труб совпадает; crop height различается."]),
    _pair("vk_axono_page17", "VK", 17, "blk_ea2bc6b4de4b406d9fef6ea5ff9eebee", "blk_972098b0570344e49e54c396ea55d3c3", [.431934493347,.002173124142,.891504605937,.995290856962], [.435729367536,.004488673046,.8765327502,.996485416128], kind="axonometric diagram", expected_route="VECTOR_WITH_VISION", expected_verdict="STRUCTURE_SAME_VALUES_CHANGED", facts=["Основная аксонометрия совпадает; в RIGHT добавлен многострочный блок примечаний у нижней границы."]),
    _pair("vk_diagrams_page18", "VK", 18, "blk_e6921c281fb040439a5ffacdfb818d55", "blk_6a452addf6134686831a50ee2dd17379", [.013582438231,.005307883024,.87853577733,.563086711179], [.014287441969,.007499247789,.878169447184,.555263426159], kind="multiple diagrams", expected_route="VECTOR_WITH_VISION", expected_verdict="NEAR_IDENTICAL", facts=["Три инженерные схемы и узлы совпадают."]),
    _pair("vk_axono_page20", "VK", 20, "blk_f3952b21781b408dbda489fc25845e05", "blk_9d7bfa92dbd3476aaf0f2d7a194c1297", [.386559802713,.005865033704,.843232044199,.985325662285], [.385749385749,.005216565695,.842751842752,.944198390816], kind="axonometric diagram; crop padding", expected_route="VECTOR_WITH_VISION", expected_verdict="NEAR_IDENTICAL", facts=["Аксонометрия совпадает; снизу различается включённый в crop фрагмент таблицы."]),

    _pair("ar_plan_page05", "AR", 5, "blk_5ef204dbfe96487da0692e278679a81f", "blk_268725917f604096be71a1ee6d4f0ccf", [.015809713716,.007046506946,.837487537388,.995772095832], [.011321097612,0,.838389320925,.994469195604], kind="large dense architecture plan", expected_route="VECTOR_WITH_VISION", expected_verdict="NEAR_IDENTICAL", facts=["Большой план и ревизионные отметки совпадают."]),
    _pair("ar_plan_page07", "AR", 7, "blk_f808177d1ca141a684ce53a4a5c94580", "blk_528240b7b24c4460a032144842731cab", [.023152808536,.317274800456,.599355747936,.986031927024], [.023684293032,.313269571709,.592018157244,.983285933733], kind="dense apartment plan", expected_route="VECTOR_WITH_VISION", expected_verdict="NEAR_IDENTICAL", facts=["План помещений и плотные аннотации совпадают."]),
    _pair("ar_plan_page08", "AR", 8, "blk_0ce1019e612345b48fe5db00037023fa", "blk_46ce98f3f24b4cc0a0075926b417cf6d", [.05154016509,.001710376283,.569156432454,.934150513113], [.026860624552,.00513240695,.57000147979,.958884984255], kind="dense apartment plan; crop padding", expected_route="VECTOR_WITH_VISION", expected_verdict="NEAR_IDENTICAL", facts=["План совпадает; левый padding заметно различается, но semantic extent сохранён."]),
    _pair("ar_plan_page10", "AR", 10, "blk_2e4fcac867994d36988bf5efad0d15b0", "blk_0b196bf490e04861893a35859f883ad4", [.023958123616,.007981755986,.388765854641,.959806157355], [.021873325109,.005778491497,.366597729147,.987143874168], kind="tall apartment plan", expected_route="VECTOR_WITH_VISION", expected_verdict="NEAR_IDENTICAL", facts=["Вертикальный фрагмент плана совпадает; crop width различается."]),
    _pair("ar_plan_page11", "AR", 11, "blk_c1072b23eb2f4d408aec5696d1c5af68", "blk_52c37a67088c4779924d4210ba9b8f80", [.01599247413,.004561003421,.449267571563,.55330672748], [.019934371114,.007294774055,.459769800305,.54314750433], kind="wide apartment plan", expected_route="VECTOR_WITH_VISION", expected_verdict="NEAR_IDENTICAL", facts=["Широкий план помещений совпадает."]),
    _pair("ar_plan_page12", "AR", 12, "blk_dce28beee72d40028cb5bf61c856a661", "blk_303ff7cecbbd4bdd91e949dea1f13648", [.024159452386,.007126567845,.617877994765,.551596351197], [.020189225674,.012357413769,.623842775822,.533009946346], kind="wide apartment plan", expected_route="VECTOR_WITH_VISION", expected_verdict="NEAR_IDENTICAL", facts=["Геометрия и ревизионные пометки совпадают."]),
    _pair("ar_plan_page13", "AR", 13, "blk_080fac3e518047229caf432e7178a4f8", "blk_5154285adb8d43e5911c5a83cd1bc85b", [.013976616046,.008551881414,.241365407875,.999429874572], [.014584980905,.009525895119,.246118582785,.989805817604], kind="narrow tall plan", expected_route="VECTOR_WITH_VISION", expected_verdict="NEAR_IDENTICAL", facts=["Узкий вертикальный план и подписи совпадают."]),
    _pair("ar_plan_page16", "AR", 16, "blk_55c46b96b32c42bcb1f65a385201c02c", "blk_4e1a412900e64600b2f01fa87dfd5fec", [.01509756445,.008858465875,.838769406068,.984296355949], [.031004846096,0,.838874578476,.996325880289], kind="large courtyard plan", expected_route="VECTOR_WITH_VISION", expected_verdict="NEAR_IDENTICAL", facts=["Контур двора, помещения и ревизионные отметки совпадают."]),

    _pair("ov_plan_floor04", "OV", (9,10), "blk_75fa57b1daea4023915bf824f2eac458", "blk_2cbc90bfff2e4cf3ac01b1573cfbd5c7", [.019841269841,.011221643992,.993386243386,.824790833443], [.017690390348,.018894642591,.999505788088,.803808420897], kind="ventilation floor plan", expected_route="VECTOR_WITH_VISION", expected_verdict="NEAR_IDENTICAL", facts=["План этажа и цветная трасса вентиляции совпадают; лист сдвинут на одну страницу."]),
    _pair("ov_plan_floor05", "OV", (10,11), "blk_7a93d50b432641eda7fe5aa726452100", "blk_70eca0411db047eb86e0f374d45be47f", [.017195767196,.011221643992,.993386243386,.807958367454], [.018988192081,.012599945068,1,.805625617504], kind="ventilation floor plan", expected_route="VECTOR_WITH_VISION", expected_verdict="NEAR_IDENTICAL", facts=["План и магистраль совпадают; лист сдвинут на одну страницу."]),
    _pair("ov_plan_floor06", "OV", (11,12), "blk_2212f35196ec4143ad0db68029a2ccb3", "blk_515c6ef44da248c4bd0928ac1a5c194a", [.025132275132,.008416232994,.994708994709,.847234121428], [.017172962427,.01264795661,.998101204634,.855030046804], kind="ventilation floor plan", expected_route="VECTOR_WITH_VISION", expected_verdict="NEAR_IDENTICAL", facts=["Протяжённый план и вентиляционные трассы совпадают."]),
    _pair("ov_plan_floor07", "OV", (12,13), "blk_28ef636509184e52b6709241f6189393", "blk_447b14f9866c41c7b4ecd5078bfd8066", [.056878306878,.01496748302,.82671957672,.905532722699], [.046208530806,.011731030769,.900473933649,.926751430783], kind="curved ventilation plan", expected_route="VECTOR_WITH_VISION", expected_verdict="STRUCTURE_CHANGED", facts=["В RIGHT из нескольких розовых зон удалены внутренние контуры/символы оборудования.", "Crop справа шире, поэтому точная локализация требует Hybrid, но общая область достаточна для подтверждения удаления."]),
    _pair("ov_equipment_table", "OV", (13,14), "blk_21446c486e0f453890207c46506b5945", "blk_a56f604b46114e28841e0af4a8425d86", [.045454545455,.011539615156,.986013986014,.51104009978], [.045246809721,.012170508504,.994272977114,.508374914527], kind="equipment table; text-heavy", expected_route="VECTOR_OK", expected_verdict="NEAR_IDENTICAL", facts=["Состав таблицы оборудования и количества визуально совпадают; экспортная упаковка/границы слегка различаются."]),
]


def benchmark_pairs() -> list[dict[str, Any]]:
    baseline = json.loads(BASELINE_MANIFEST.read_text(encoding="utf-8"))["pairs"]
    result = []
    for row in baseline:
        pair = copy.deepcopy(row)
        route, verdict, facts = BASE_GROUND_TRUTH[pair["pair_id"]]
        pair.pop("human_expected", None)
        pair["selection"] = "Baseline manual pairing from commit 1619fc3f; crop revalidated for v02."
        pair["ground_truth"] = {
            "expected_route": route, "expected_verdict": verdict,
            "important_factual_changes": facts,
        }
        result.append(pair)
    result.extend(copy.deepcopy(EXTRA_PAIRS))
    assert len(result) == 39
    assert len({pair["pair_id"] for pair in result}) == len(result)
    return result


def benchmark_manifest() -> dict[str, Any]:
    return {
        "schema_version": "vector-block-benchmark-v0.2-codex",
        "research_only": True,
        "baseline_commit": "1619fc3f",
        "selection_method": "39 explicit real block pairs. Metadata shortlist only; every final crop pair was manually inspected. No automatic block matcher.",
        "discipline_limit": "Available active corpus supplied paired block manifests for AR/OV/SS/VK; EOM was manually page-shift paired. KJ/KM/GP lacked usable paired versions.",
        "pairs": benchmark_pairs(),
    }


def ground_truth_artifact() -> dict[str, Any]:
    return {
        "schema_version": "vector-block-ground-truth-v0.2-codex",
        "judge": "Manual side-by-side raster inspection by the researcher; no model output used as ground truth.",
        "pairs": [{"pair_id": pair["pair_id"], **pair["ground_truth"]} for pair in benchmark_pairs()],
    }
