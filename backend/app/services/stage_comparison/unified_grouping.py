"""Deterministic post-processing слой поверх unified_findings.json.

Цель — превратить сырой evidence-layer (346 raw findings в session
ba413a93c5754f6c, например) в **сгруппированный реестр значимых отличий**,
которые могут влиять на стоимость, объёмы, материалы, оборудование, топологию
и т.п. Без LLM, без Qwen, без Opus — чистый Python.

Что делает модуль (v2 — двухуровневая группировка):
    classify_formality(finding)            — formal / escalated / significant
    assign_theme(finding)                  — тематическая категория
    infer_change_direction(finding)        — complication / simplification / neutral
    infer_cost_impact_direction(finding)   — increase / decrease / unknown
    extract_semantic_subject(finding)      — tile/pump/valve/route/etc.
    extract_semantic_action(finding)       — replaced/added/removed/increased/...
    extract_discipline_or_system(finding)  — АР/КР/ИОС/ОВ/ВК и т.п.
    normalize_value(text)                  — нормализация для group_key
    build_group_key(finding)               — strict ключ группы (точная пара old/new)
    build_semantic_group_key(finding)      — semantic ключ кластера
    group_findings(flat_items)             — двухпроходная сборка

Архитектурные принципы:
    * группировка ВСЕГДА выполняется в рамках одного списка items, т.е. одной
      session_id; cross-session merge невозможен по построению API;
    * сырой `unified_findings.json` остаётся неизменным;
    * grouped результат — отдельный артефакт
      `unified_findings_grouped.json`;
    * каждый source_finding попадает либо в `groups`, либо в
      `hidden_formal_groups` — потерь evidence не должно быть;
    * `significance` строится из cost_impact + theme + direction, а
      НЕ из inherited severity (severity сохраняется как max только для UI).

Safety guards для semantic merge:
    * НЕ объединяем strict-группы, если у них конфликт по cost_impact_direction
      (`increase` ↔ `decrease`);
    * НЕ объединяем strict-группы с разным change_direction
      (`complication` ↔ `simplification`);
    * НЕ объединяем, если semantic_subject == "unknown";
    * если объединили N strict-групп с разными old/new — каждой паре
      (old, new) выделяется запись в `value_variants[]`; группа помечается
      `requires_human_review=true` и получает `review_reason="multiple_value_variants"`.
"""
from __future__ import annotations

import hashlib
import logging
import re
import threading
from datetime import datetime
from typing import Any, Iterable, Optional

from . import paths as paths_mod
from . import unified_findings as unified_findings_mod

logger = logging.getLogger(__name__)

VERSION = 1
_lock = threading.RLock()


# ─── Configuration tables ────────────────────────────────────────────────


# Темы и их веса для significance scoring. Если finding попадает в одну из
# high-impact тем, ему повышается significance даже при cost_impact=possible.
THEMES = {
    "geometry",
    "quantities",
    "materials",
    "equipment",
    "system_topology",
    "routes_lengths",
    "power_load_perf",
    "construction_scope",
    "finishing_scope",
    "fire_safety",
    "commissioning_tests",
    "exclusions_additions",
    "simplifications",
    "documentation_formal",
    "norms",
    "other",
}

HIGH_IMPACT_THEMES = {
    "equipment",
    "materials",
    "geometry",
    "quantities",
    "system_topology",
    "routes_lengths",
    "power_load_perf",
    "construction_scope",
    "finishing_scope",
    "fire_safety",
    "exclusions_additions",
}


# Регулярки для распознавания формальных stamp-only / header-footer изменений.
# Применяются к title + summary в нижнем регистре.
_FORMAL_DESIGNER_RE = re.compile(
    r"\b("
    r"проектировщик|проектная организация|организац\S* разработчик|"
    r"генпроектировщик|разработчик(\s+проекта|\s+штампа)?"
    r")\b",
    re.IGNORECASE,
)
_FORMAL_GIP_RE = re.compile(
    r"\b(гип|гап|главный инженер проекта|главный архитектор проекта)\b",
    re.IGNORECASE,
)
_FORMAL_DATE_RE = re.compile(
    r"^\s*(дата|год|выпуска|дата выпуска|год выпуска|изменена дата)\b",
    re.IGNORECASE,
)
_FORMAL_REVISION_RE = re.compile(
    r"\b(шифр|индекс корректировки|номер корректировки|суффикс\s+-?\s*корр|"
    r"добавлен\s+суффикс|изменён\s+шифр)\b",
    re.IGNORECASE,
)
_FORMAL_PAGE_NUM_RE = re.compile(
    r"\b(номер(\s+(листа|страницы))|перенумерац\S*|нумерац\S*\s+стр\S*|"
    r"номер листа|порядок листов)\b",
    re.IGNORECASE,
)
_FORMAL_FORMATTING_RE = re.compile(
    r"\b(оформлени\S*|шрифт|колонтитул|рамка|подпис\S*(\s+только)?|"
    r"типографик\S*|кавычк\S*|форматирование|стиль)\b",
    re.IGNORECASE,
)
_FORMAL_NORM_COSMETIC_RE = re.compile(
    r"\b(актуализаци\S* ссыл\S*\s+на\s+(нормы|сп)|"
    r"замен(а|ены)? редакц\S*|замен(а|ены)? сп\b|"
    r"актуализир\S*\s+редакц\S*\s+сп)\b",
    re.IGNORECASE,
)
_FORMAL_HEADER_FOOTER_RE = re.compile(
    r"\b(колонтитул|содержание тома|обложка|титульный лист\s+(только|оформление))\b",
    re.IGNORECASE,
)


# Escalation: ситуации, когда формальное на первый взгляд изменение всё же
# скрывать нельзя.
_ESC_STAGE_RE = re.compile(
    r"\b(стадия\s*[:=]?\s*(п|пд|рд|р|рабоч\S*|проектн\S*)|"
    r"п\s*[→\->]+\s*рд|пд\s*[→\->]+\s*рд|"
    r"смена стадии|изменена стадия|стадия проектирования)\b",
    re.IGNORECASE,
)
_ESC_EXPERT_RE = re.compile(
    r"\b(положительное заключение|экспертиз\S*|госэкспертиз\S*|"
    r"негосударственн\S* экспертиз\S*)\b",
    re.IGNORECASE,
)
_ESC_COMPOSITION_RE = re.compile(
    r"\b(состав документац\S*|изменён состав|состав тома|"
    r"изменение состава|новый том|новый раздел)\b",
    re.IGNORECASE,
)
_ESC_GPZU_RE = re.compile(r"\bгпзу\b", re.IGNORECASE)
_ESC_NORM_SUBSTANTIVE_RE = re.compile(
    r"\b(новое требование|новые требования|изменены требования|"
    r"добавлено требование|ужесточ\S*\s+требован\S*)\b",
    re.IGNORECASE,
)


# Theme keyword detectors (по title+summary+old+new в нижнем регистре).
_THEME_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("fire_safety", re.compile(
        r"\b(пожарн\S*|противопожар\S*|спринклер\S*|"
        r"эвакуа\S*\s+(путь|выход)|fire|sprinkler|противодым\S*|"
        r"огнестойк\S*|категори\S*\s+(пожароопасн|по\s+пожар))\b",
        re.IGNORECASE,
    )),
    ("equipment", re.compile(
        r"\b(оборудовани\S*|насос\S*|вентилятор\S*|щит\S*|шкаф\S*|"
        r"кондиционер\S*|преобразовател\S*|трансформатор\S*|"
        r"источник\s+бесперебойного|ибп\b|увос|чувствительн\S*\s+элемент\S*|"
        r"извещател\S*|оповещател\S*|задвижк\S*|клапан\S*\s+противопожар\S*|"
        r"датчик\S*|контроллер\S*|плк\b|приточн\S*\s+установк\S*|"
        r"теплообменник\S*|насосн\S*\s+агрегат\S*)\b",
        re.IGNORECASE,
    )),
    ("materials", re.compile(
        r"\b(материал\S*|плитк\S*|кирпич\S*|бетон\S*|кабель\S*|"
        r"провод\S*|изоляц\S*|утеплител\S*|керамогранит\S*|"
        r"гипсокартон\S*|металл\S*|стеклоф\S*|травертин\S*|"
        r"доломит\S*|штукатурк\S*|краск\S*|облицов\S*|"
        r"асфальт\S*|щебен\S*|раствор\S*)\b",
        re.IGNORECASE,
    )),
    ("routes_lengths", re.compile(
        r"\b(трасс\S*|длин\S*\s+(кабел|трасс|трубопровод)|"
        r"кабельн\S*\s+трасс\S*|маршрут\S*\s+прокладк\S*|"
        r"протяж\S*|сечени\S*\s+кабел\S*|трубопровод\S*)\b",
        re.IGNORECASE,
    )),
    ("power_load_perf", re.compile(
        r"\b(нагрузк\S*|мощност\S*|квт\b|вт\b|тепловая\s+нагрузк\S*|"
        r"расход\S*\s+(воды|воздух|тепл)|производительн\S*|"
        r"электронагрузк\S*|тепловыделени\S*)\b",
        re.IGNORECASE,
    )),
    ("system_topology", re.compile(
        r"\b(структурн\S*\s+схем\S*|принципиальн\S*\s+схем\S*|"
        r"топологи\S*|иерархи\S*\s+(систем|схем)|"
        r"схема\s+(подключен|присоединен|сопряжен)|"
        r"стояк\S*|магистрал\S*\s+схем)\b",
        re.IGNORECASE,
    )),
    ("geometry", re.compile(
        r"\b(размер\S*|габарит\S*|этажност\S*|высот\S*\s+(этаж|здани)|"
        r"площад\S*\s+(застройк|здани|помещен|объект)|"
        r"объём\S*\s+(здани|помещен)|координат\S*|пятно\s+застройк\S*)\b",
        re.IGNORECASE,
    )),
    ("quantities", re.compile(
        r"\b(количеств\S*|шт\b|штук\S*|число\S*\s+(квартир|секций|"
        r"мест|парковочн)|объём\S*\s+(работ|материал|бетон)|"
        r"парковочн\S*\s+мест\S*|машиномест\S*)\b",
        re.IGNORECASE,
    )),
    ("construction_scope", re.compile(
        r"\b(объём\S*\s+работ|состав\s+работ|строительн\S*\s+работ|"
        r"монтажн\S*\s+работ|демонтаж\S*|новое\s+строительств)\b",
        re.IGNORECASE,
    )),
    ("finishing_scope", re.compile(
        r"\b(отделк\S*|фасад\S*|облицов\S*|финиш\S*|чистов\S*\s+отделк\S*|"
        r"декоратив\S*|декор\S*\s+элемент\S*)\b",
        re.IGNORECASE,
    )),
    ("commissioning_tests", re.compile(
        r"\b(пнр\b|пусконаладочн\S*|испытани\S*|проверк\S*\s+оборуд\S*|"
        r"балансировк\S*|опрессов\S*)\b",
        re.IGNORECASE,
    )),
    ("norms", re.compile(
        r"\b(нормативн\S*\s+(ссыл|документ)|перечен\S*\s+(норм|сп)|"
        r"требовани\S*\s+(норм|сп)|новая редакц\S*|"
        r"переход на\s+сп)\b",
        re.IGNORECASE,
    )),
    ("documentation_formal", re.compile(
        r"\b(штамп|шифр|номер листа|перенумерац\S*|оформлени\S*|"
        r"проектировщик|гип\b|гап\b|подпис\S*|содержание тома|"
        r"титульн\S*\s+лист)\b",
        re.IGNORECASE,
    )),
]


# ─── Semantic-cluster vocabularies ───────────────────────────────────────


# Каждый subject — список регулярок (или плейн-подстрок), которые должны
# найтись в title+summary+old+new. Порядок важен: первый matched subject
# побеждает (от specific к generic).
_SEMANTIC_SUBJECTS: list[tuple[str, re.Pattern]] = [
    # Materials / finishing
    ("tile", re.compile(r"\b(плитк\S*|плиточн\S*|керамогранит\S*)\b", re.IGNORECASE)),
    ("facade_material", re.compile(
        r"\b(фасад\S*|облицов\S*\s+(фасад|наружн)|стеклоф\S*|"
        r"травертин\S*|доломит\S*|облицовочн\S*\s+(панел|плит))\b",
        re.IGNORECASE,
    )),
    ("floor_covering", re.compile(
        r"\b(покрытие\s+пол\S*|напольн\S*\s+(покрыт|материал)|"
        r"паркет\S*|ламинат\S*|линолеум\S*)\b",
        re.IGNORECASE,
    )),
    ("roof_covering", re.compile(
        r"\b(кровл\S*\s+(покрыт|материал)|кровельн\S*\s+ковёр|"
        r"мембран\S*\s+кровл\S*|ограждени\S*\s+кровл\S*)\b",
        re.IGNORECASE,
    )),
    ("insulation", re.compile(r"\b(утеплител\S*|теплоизоляц\S*)\b", re.IGNORECASE)),
    ("paint_plaster", re.compile(r"\b(краск\S*|штукатурк\S*|покрасочн\S*)\b", re.IGNORECASE)),
    # Equipment
    ("pump", re.compile(r"\b(насос\S*|насосн\S*\s+агрегат\S*)\b", re.IGNORECASE)),
    ("fan", re.compile(r"\b(вентилятор\S*|приточн\S*\s+установк\S*|воздушно-?тепл\S*\s+завес\S*)\b", re.IGNORECASE)),
    ("valve", re.compile(r"\b(клапан\S*|задвижк\S*|вентил\S*\s+трубопровод)\b", re.IGNORECASE)),
    ("switchboard", re.compile(r"\b(щит\S*|шкаф\S*\s+(электр|управл|автоматик))\b", re.IGNORECASE)),
    ("ups", re.compile(r"\b(ибп\b|источник\S*\s+бесперебойн\S*\s+питан)\b", re.IGNORECASE)),
    ("transformer", re.compile(r"\b(трансформатор\S*|тп\b|подстанц\S*)\b", re.IGNORECASE)),
    ("lighting_fixture", re.compile(r"\b(светильник\S*|осветительн\S*\s+приб)\b", re.IGNORECASE)),
    ("detector_sensor", re.compile(r"\b(извещател\S*|оповещател\S*|датчик\S*|детектор\S*)\b", re.IGNORECASE)),
    ("controller", re.compile(r"\b(контроллер\S*|плк\b|шкаф\S*\s+автоматики)\b", re.IGNORECASE)),
    ("heat_exchanger", re.compile(r"\b(теплообменник\S*)\b", re.IGNORECASE)),
    ("ahu_ac", re.compile(r"\b(кондиционер\S*|чиллер\S*|вру\b)\b", re.IGNORECASE)),
    # Routes / lengths / sections
    ("cable_line", re.compile(r"\b(кабел\S*\s+лини|кабельн\S*\s+(лини|сет|трасс)|линия\S*\s+связи)\b", re.IGNORECASE)),
    ("cable_route", re.compile(r"\b(кабельн\S*\s+трасс\S*|трасс\S*\s+кабел|маршрут\S*\s+прокладк\S*)\b", re.IGNORECASE)),
    ("pipe_run", re.compile(r"\b(трубопровод\S*|стояк\S*|магистрал\S*\s+труб)\b", re.IGNORECASE)),
    ("air_duct", re.compile(r"\b(воздуховод\S*|вентканал\S*)\b", re.IGNORECASE)),
    ("cable_tray", re.compile(r"\b(лоток\S*\s+кабел|кабельн\S*\s+лоток)\b", re.IGNORECASE)),
    # System topology
    ("water_supply_zone", re.compile(
        r"\b((одно|двух|двухзон|однозон)\S*\s+(систем|зон)\s+водоснабж|"
        r"зон\S*\s+водоснабж|зональ\S*\s+(хвс|гвс))\b",
        re.IGNORECASE,
    )),
    ("fire_water_system", re.compile(
        r"\b(противопожарн\S*\s+водопровод|внутр\S*\s+противопожарн\S*|"
        r"спринклер\S*\s+(систем|сеть)|систем\S*\s+пожароту)\b",
        re.IGNORECASE,
    )),
    ("drainage", re.compile(r"\b(водосток\S*|водоотвод\S*|канализац\S*|дренаж\S*)\b", re.IGNORECASE)),
    ("ventilation_system", re.compile(r"\b(систем\S*\s+вентиляц|вытяжн\S*\s+вентиляц|приточно-?вытяжн\S*)\b", re.IGNORECASE)),
    ("heating_system", re.compile(r"\b(систем\S*\s+отоплен|итп\b|тепловой\s+пункт|тепл\S*\s+узел)\b", re.IGNORECASE)),
    ("dispatching_asud", re.compile(r"\b(асуд\b|диспетчер\S*\s+(систем|схем)|структурн\S*\s+схем\S*\s+асуд)\b", re.IGNORECASE)),
    # Geometry
    ("floor_height", re.compile(r"\b(высот\S*\s+(этаж|потолк)|этажн\S*\s+высот)\b", re.IGNORECASE)),
    ("building_height", re.compile(r"\b(высот\S*\s+(зданий|здани|корпус)|строительн\S*\s+высот)\b", re.IGNORECASE)),
    ("floor_count", re.compile(r"\b(этажност\S*|колич\S*\s+этаж|число\s+этаж)\b", re.IGNORECASE)),
    ("area", re.compile(r"\b(площад\S*\s+(застройк|здани|помещен|объект|общ|квартир))\b", re.IGNORECASE)),
    # Quantities / scope
    ("equipment_count", re.compile(r"\b(колич\S*\s+(оборудов|щит|насос|вентил|светильник|шкаф))\b", re.IGNORECASE)),
    ("parking_count", re.compile(r"\b(парковочн\S*\s+мест|машиномест\S*|колич\S*\s+(парков|маш))\b", re.IGNORECASE)),
    ("apartment_count", re.compile(r"\b(колич\S*\s+квартир|число\s+квартир)\b", re.IGNORECASE)),
    # Loads / performance
    ("power_load", re.compile(r"\b(электронагрузк\S*|расчётн\S*\s+нагрузк|потребл\S*\s+мощност)\b", re.IGNORECASE)),
    ("thermal_load", re.compile(r"\b(тепловая\s+нагрузк|теплопотер\S*|тепловыдел\S*|расход\S*\s+тепл)\b", re.IGNORECASE)),
    ("water_flow", re.compile(r"\b(расход\S*\s+(вод|сточн)|объём\S*\s+вод)\b", re.IGNORECASE)),
    ("air_flow", re.compile(r"\b(расход\S*\s+воздух|объём\S*\s+воздух|производительн\S*\s+вент)\b", re.IGNORECASE)),
    # Documentation / formal
    ("gpzu", re.compile(r"\bгпзу\b", re.IGNORECASE)),
    ("stage_change", re.compile(r"\b(стадия\s+(п|пд|рд)|переход\S*\s+на\s+рд|п\s*[→\->]\s*рд)\b", re.IGNORECASE)),
    ("expertise", re.compile(r"\b(положительн\S*\s+заключени|экспертиз\S*|госэкспертиз\S*)\b", re.IGNORECASE)),
    ("designer_org", re.compile(r"\b(проектировщик\S*|организаци\S*\s+(разработчик|проектант))\b", re.IGNORECASE)),
    ("gip_gap", re.compile(r"\b(гип\b|гап\b|главн\S*\s+(инженер|архитектор)\s+проекта)\b", re.IGNORECASE)),
    ("project_code", re.compile(r"\b(шифр|номер\s+проекта|корр\.?\b|индекс\s+корректировк)\b", re.IGNORECASE)),
    ("document_date", re.compile(r"\b(дата\s+выпуска|год\s+выпуска|изменена\s+дата|изменён\s+год)\b", re.IGNORECASE)),
    ("page_numbering", re.compile(r"\b(номер\S*\s+(листа|страницы)|перенумерац\S*\s+стр)\b", re.IGNORECASE)),
    # Composition / scope-of-doc
    ("document_composition", re.compile(
        r"\b(состав\S*\s+(документац|тома|раздел)|изменён\S*\s+состав|"
        r"новый\s+(том|раздел)|том\S*\s+(добавлен|удал))\b",
        re.IGNORECASE,
    )),
    ("natural_lighting", re.compile(r"\b(кео\b|естественн\S*\s+освещен|инсоляц\S*)\b", re.IGNORECASE)),
    ("fire_safety_meta", re.compile(r"\b(пожарн\S*\s+безопасн|противодым\S*|категор\S*\s+пожароопас)\b", re.IGNORECASE)),
]


# Semantic action: что произошло с subject. Применяется к type+title+summary.
_SEMANTIC_ACTIONS: list[tuple[str, re.Pattern]] = [
    ("added", re.compile(r"\b(добавлен\S*|новый|введён\S*|появил\S*|внесен\S*|включён)\b", re.IGNORECASE)),
    ("removed", re.compile(r"\b(удалён\S*|исключен\S*|аннулирован\S*|аннул\.|снят\S*|демонтир\S*)\b", re.IGNORECASE)),
    ("split_system", re.compile(r"\b(раздел\S*\s+(на|систем)|раздельн\S*\s+систем|разделить|разнесены)\b", re.IGNORECASE)),
    ("merged_system", re.compile(r"\b(объединён\S*\s+систем|объединить|объединение)\b", re.IGNORECASE)),
    ("increased", re.compile(r"\b(увеличен\S*|выросл\S*|повышен\S*|расширен\S*)\b", re.IGNORECASE)),
    ("decreased", re.compile(r"\b(уменьш\S*|сокращ\S*|снижен\S*|упрощ\S*)\b", re.IGNORECASE)),
    ("route_changed", re.compile(r"\b(изменён\S*\s+(трасс|маршрут)|перенесен\S*\s+трасс)\b", re.IGNORECASE)),
    ("topology_changed", re.compile(r"\b(изменён\S*\s+(топологи|схем|структур)|перерасчёт\S*\s+схем)\b", re.IGNORECASE)),
    ("calculation_changed", re.compile(r"\b(перерасчёт\S*|пересчёт\S*|изменён\S*\s+расчёт|перерасчитан\S*)\b", re.IGNORECASE)),
    ("requirement_changed", re.compile(r"\b(изменён\S*\s+требован|новое\s+требован|ужесточ\S*\s+требован)\b", re.IGNORECASE)),
    ("material_changed", re.compile(r"\b(замен\S*\s+(материал|плитк|покрыт|облицов)|изменён\S*\s+материал)\b", re.IGNORECASE)),
    ("equipment_changed", re.compile(r"\b(замен\S*\s+(оборудов|насос|вентил|клапан|щит)|изменён\S*\s+оборудован)\b", re.IGNORECASE)),
    ("quantity_changed", re.compile(r"\b(колич\S*\s+изменен|изменён\S*\s+колич|число\s+изменен)\b", re.IGNORECASE)),
    ("replaced", re.compile(r"\b(замен\S*|заменён|замена)\b", re.IGNORECASE)),
    ("changed", re.compile(r"\b(изменён\S*|изменена|изменено|изменены)\b", re.IGNORECASE)),
]

# Direction detectors. Apply on summary+title+old+new.
_COMPLICATION_RE = re.compile(
    r"\b(добавл\S*|новое|дополнительн\S*|увеличен\S*|выросл\S*|"
    r"введён\S*|появил\S*|расширен\S*|усложн\S*|вырос\S*|"
    r"больше|более\s+(дорог|сложн))\b",
    re.IGNORECASE,
)
_SIMPLIFICATION_RE = re.compile(
    r"\b(уменьш\S*|сокращ\S*|исключен\S*|удалён\S*|"
    r"аннулирован\S*|аннул\.\b|упрощ\S*|снижен\S*|"
    r"меньше|более\s+простой|более\s+дешев|демонтиров\S*|"
    r"замен\S*\s+на\s+более\s+(прост|деш))\b",
    re.IGNORECASE,
)

# Cost-impact direction: more nuanced — applied AFTER theme classification.
_COST_INCREASE_RE = re.compile(
    r"\b(добавлен(а|о|ы)?|новое\s+оборудовани|увеличен(а|о|ы)?|"
    r"дополнительн\S*\s+(оборудовани|материал|объём)|расширен\S*)\b",
    re.IGNORECASE,
)
_COST_DECREASE_RE = re.compile(
    r"\b(исключен(а|о|ы)?|удалён(а|о|ы)?|демонтиров\S*|аннул\S*|"
    r"уменьшен\S*|сокращ\S*|упрощ\S*|снижен\S*)\b",
    re.IGNORECASE,
)


# Normalize: page/sheet/coord patterns to strip.
_PAGE_REF_RE = re.compile(
    r"\b(стр\.?|страниц\S*|page|p\.|лист\S*|sheet)\s*\.?\s*\d+(?:\s*[-–]\s*\d+)?",
    re.IGNORECASE,
)
_COORDINATE_RE = re.compile(
    r"\b(?:оси?\s+)?[А-ЯA-Z]{1,2}\s*[-/]\s*[А-ЯA-Z]{1,2}\b"
    r"|\b\d{1,3}(?:[\.,]\d{1,3})?\s*[xх×]\s*\d{1,3}(?:[\.,]\d{1,3})?\b",
)
_WHITESPACE_RE = re.compile(r"\s+")
_QUOTES_RE = re.compile(r"[«»\"'`„""''‛]")
# Word-level stripping for stamp text.
_STAMP_NOISE_WORDS = [
    "том", "редакция", "корр", "корр.", "ред.", "арх.", "арх",
    "москва", "санкт-петербург", "г.", "ооо", "оао", "пао", "зао",
]

# Unit normalization map.
_UNIT_MAP = {
    "м³": "м3", "м2": "м2", "м²": "м2",
    "шт.": "шт", "штук": "шт", "штуки": "шт",
    "мм.": "мм", "миллиметр": "мм", "миллиметров": "мм",
    "квт.": "квт", "квт ": "квт ",
}


# ─── Pure classification functions ───────────────────────────────────────


def _lower_concat(finding: dict) -> str:
    parts = [
        finding.get("title") or "",
        finding.get("summary") or "",
        finding.get("type") or "",
        finding.get("category") or "",
        finding.get("source_layer") or "",
        finding.get("construction_impact") or "",
    ]
    return " ".join(p.strip() for p in parts).lower()


def _full_text(finding: dict) -> str:
    parts = [
        finding.get("title") or "",
        finding.get("summary") or "",
        finding.get("old_value") or "",
        finding.get("new_value") or "",
        finding.get("construction_impact") or "",
    ]
    return " ".join(p.strip() for p in parts)


def classify_formality(finding: dict) -> dict:
    """Определить, является ли finding формальным (штамп/проектировщик/дата).

    Возвращает `{is_formal, formal_reason, escalation_reason}`. Если
    escalation_reason не None, finding всё равно considered formal=True,
    но НЕ скрывается полностью — UI должен показать его (медленно) с пометкой.
    """
    src_layer = (finding.get("source_layer") or "").lower()
    f_type = (finding.get("type") or "").lower()
    lc = _lower_concat(finding)
    full = _full_text(finding)

    formal_reason: Optional[str] = None

    # Layer/type-based heuristics.
    if src_layer == "stamp" or f_type == "stamp_changed":
        # У stamp-source-layer почти всегда formal. Дальше определяем причину.
        if _FORMAL_DESIGNER_RE.search(lc) or "проектировщик" in lc or "разработчик" in lc:
            formal_reason = "designer_only"
        elif _FORMAL_GIP_RE.search(lc):
            formal_reason = "gip_gap_only"
        elif _FORMAL_DATE_RE.search(lc) or "год выпуска" in lc or "дата выпуска" in lc:
            formal_reason = "date_only"
        elif _FORMAL_REVISION_RE.search(lc) or "корр" in lc and "шифр" in lc:
            formal_reason = "revision_code_only"
        elif _FORMAL_PAGE_NUM_RE.search(lc):
            formal_reason = "page_order_only"
        else:
            formal_reason = "stamp_only"

    if formal_reason is None:
        if _FORMAL_DESIGNER_RE.search(lc):
            formal_reason = "designer_only"
        elif _FORMAL_GIP_RE.search(lc):
            formal_reason = "gip_gap_only"
        elif _FORMAL_PAGE_NUM_RE.search(lc):
            formal_reason = "page_order_only"
        elif _FORMAL_FORMATTING_RE.search(lc) and not _is_substantive_text(full):
            formal_reason = "formatting_only"
        elif _FORMAL_HEADER_FOOTER_RE.search(lc):
            formal_reason = "header_footer_repeat"
        elif _FORMAL_NORM_COSMETIC_RE.search(lc) and not _ESC_NORM_SUBSTANTIVE_RE.search(full):
            formal_reason = "norm_reference_cosmetic"

    is_formal = formal_reason is not None

    # Escalation: даже у formal может быть содержательная причина не скрывать.
    escalation_reason: Optional[str] = None
    if is_formal:
        if _ESC_STAGE_RE.search(full):
            escalation_reason = "stage_change"
        elif _ESC_EXPERT_RE.search(full):
            escalation_reason = "expert_review"
        elif _ESC_COMPOSITION_RE.search(full):
            escalation_reason = "composition_change"
        elif _ESC_GPZU_RE.search(full):
            escalation_reason = "gpzu_impact"
        elif _ESC_NORM_SUBSTANTIVE_RE.search(full):
            escalation_reason = "substantive_norm_change"

    return {
        "is_formal": is_formal,
        "formal_reason": formal_reason,
        "escalation_reason": escalation_reason,
    }


def _is_substantive_text(text: str) -> bool:
    """Проверка: содержит ли текст явно содержательные термины, которые
    подавляют formatting-only классификацию."""
    if not text:
        return False
    t = text.lower()
    return any(kw in t for kw in (
        "оборудование", "материал", "трасса", "нагрузк", "мощност",
        "система", "схема", "плитк", "кабель", "пожарн",
    ))


def assign_theme(finding: dict) -> str:
    """Определить тему finding'а по эвристикам (без LLM)."""
    src_layer = (finding.get("source_layer") or "").lower()
    f_type = (finding.get("type") or "").lower()
    f_category = (finding.get("category") or "").lower()
    full = _full_text(finding).lower()

    # Source/type fast paths.
    if src_layer == "stamp" or f_type == "stamp_changed":
        # Если в тексте есть substantive признаки — пропускаем дальше
        if not (_ESC_STAGE_RE.search(full) or _ESC_EXPERT_RE.search(full)
                or _ESC_COMPOSITION_RE.search(full)):
            return "documentation_formal"

    if f_type == "material_changed":
        return "materials"
    if f_type == "equipment_changed":
        return "equipment"
    if f_type == "scheme_sequence_changed":
        return "system_topology"
    if f_type == "calculation_changed":
        # Нагрузки vs геометрия vs прочее
        if any(kw in full for kw in ("нагрузк", "мощност", "квт", "тепл", "расход")):
            return "power_load_perf"
        if any(kw in full for kw in ("этаж", "высот", "площад", "размер")):
            return "geometry"
        return "quantities"
    if f_type == "table_changed":
        if any(kw in full for kw in ("оборудовани", "позиц", "марк")):
            return "equipment"
        if any(kw in full for kw in ("материал", "плитк", "облицовк")):
            return "materials"

    if f_type in ("added", "removed"):
        if any(kw in full for kw in ("оборудовани", "насос", "вентилятор", "щит",
                                      "извещател", "клапан", "трансформатор")):
            return "equipment"
        if any(kw in full for kw in ("лист", "схема", "том", "раздел")):
            return "exclusions_additions"

    # Category fallbacks.
    if f_category == "fire_safety":
        # Уточняем — оборудование пожарки vs нормы пожарки.
        for theme, pat in _THEME_PATTERNS:
            if theme == "fire_safety":
                continue
            if pat.search(full):
                return theme
        return "fire_safety"

    # Pattern-based theme detection.
    for theme, pat in _THEME_PATTERNS:
        if pat.search(full):
            return theme

    # Category broad fallback.
    if f_category in ("engineering_systems", "electrical", "water_supply",
                       "low_voltage", "hvac"):
        return "system_topology"
    if f_category == "architecture":
        return "finishing_scope"
    if f_category == "structures":
        return "construction_scope"

    return "other"


def infer_change_direction(finding: dict) -> str:
    """complication / simplification / neutral / unknown."""
    f_type = (finding.get("type") or "").lower()
    if f_type == "added":
        return "complication"
    if f_type == "removed":
        return "simplification"

    full = _full_text(finding).lower()
    if not full.strip():
        return "unknown"
    comp = bool(_COMPLICATION_RE.search(full))
    simp = bool(_SIMPLIFICATION_RE.search(full))
    if comp and not simp:
        return "complication"
    if simp and not comp:
        return "simplification"
    if comp and simp:
        return "neutral"
    return "unknown"


def infer_cost_impact_direction(finding: dict) -> str:
    """increase / decrease / unknown — направление денежного эффекта."""
    cost = (finding.get("cost_impact") or "").lower()
    if cost == "none":
        return "unknown"

    direction = infer_change_direction(finding)
    if direction == "complication":
        return "increase"
    if direction == "simplification":
        return "decrease"

    # Дополнительные эвристики на cost-specific words.
    full = _full_text(finding).lower()
    inc = bool(_COST_INCREASE_RE.search(full))
    dec = bool(_COST_DECREASE_RE.search(full))
    if inc and not dec:
        return "increase"
    if dec and not inc:
        return "decrease"
    return "unknown"


# ─── Semantic extractors ─────────────────────────────────────────────────


def _primary_subject_text(finding: dict) -> str:
    """Текст для extraction subject — primary fields only.

    `construction_impact` исключён, потому что он часто описывает
    вторичные эффекты («влияет на выбор насосного оборудования» для
    finding'а про этажность) и приводит к ложным subject-merges.
    """
    parts = [
        finding.get("title") or "",
        finding.get("summary") or "",
        finding.get("old_value") or "",
        finding.get("new_value") or "",
    ]
    return " ".join(p.strip() for p in parts)


def extract_semantic_subject(finding: dict) -> str:
    """Достать «о чём идёт изменение» (плитка / насос / кабельная трасса / ...).

    Возвращает `unknown` если ни один паттерн не сработал. В этом случае
    semantic merge для этой группы отключён (safety guard).

    Анализирует ТОЛЬКО title+summary+old+new. `construction_impact` намеренно
    исключён — иначе secondary-эффекты типа «влияет на выбор насосов» втянут
    finding в чужой subject-кластер.
    """
    text = _primary_subject_text(finding)
    if not text.strip():
        return "unknown"
    for subject, pat in _SEMANTIC_SUBJECTS:
        if pat.search(text):
            return subject
    return "unknown"


def extract_semantic_action(finding: dict) -> str:
    """Что произошло с subject. fallback на change_type."""
    f_type = (finding.get("type") or "").lower()
    if f_type == "added":
        return "added"
    if f_type == "removed":
        return "removed"
    if f_type == "material_changed":
        return "material_changed"
    if f_type == "equipment_changed":
        return "equipment_changed"
    if f_type == "scheme_sequence_changed":
        return "topology_changed"
    if f_type == "calculation_changed":
        return "calculation_changed"
    if f_type == "requirement_changed":
        return "requirement_changed"
    if f_type == "stamp_changed":
        return "changed"

    full = _full_text(finding)
    for action, pat in _SEMANTIC_ACTIONS:
        if pat.search(full):
            return action
    return "changed"


_DISCIPLINE_RE = re.compile(
    r"\b(АР\d*|КР\d*|ИОС[\d\.\-]*|ОВ\d*|ВК\d*|ЭО[М\d]*|"
    r"ПЗУ|ПОС|ОДИ|ИКЕО|ООС\d*|ЭЭ|АК\d*|СС|ТХ|ГП|АИ)\b",
    re.IGNORECASE,
)


def extract_discipline_or_system(finding: dict) -> Optional[str]:
    """Достать дисциплину из pair_label / pdf-имён.

    Используется как опциональный компонент semantic-ключа: разные дисциплины
    могут иметь те же subject+action, но это, как правило, разные физические
    сущности (плитка в АР vs плитка в КР).
    """
    label = (finding.get("pair_label") or "")
    if not label:
        # Попытка достать из pdf имён.
        label = " ".join(filter(None, [
            finding.get("left_pdf_name") or "",
            finding.get("right_pdf_name") or "",
        ]))
    if not label:
        return None
    m = _DISCIPLINE_RE.search(label)
    if m:
        return m.group(1).upper()
    return None


# ─── Normalization & key building ────────────────────────────────────────


def normalize_value(text: Any) -> str:
    """Нормализация для построения group_key.

    Шаги: lower → удалить page/sheet refs → удалить координаты →
    нормализовать единицы → удалить кавычки → схлопнуть whitespace.
    """
    if text is None:
        return ""
    s = str(text)
    if not s.strip():
        return ""
    s = s.lower()
    # Strip page references.
    s = _PAGE_REF_RE.sub(" ", s)
    # Strip coordinate references.
    s = _COORDINATE_RE.sub(" ", s)
    # Strip quotes / typographical markers.
    s = _QUOTES_RE.sub("", s)
    # Strip number punctuation.
    s = re.sub(r"[«»‹›]", "", s)
    # Normalize units.
    for u, repl in _UNIT_MAP.items():
        s = s.replace(u, repl)
    # Stamp noise.
    for w in _STAMP_NOISE_WORDS:
        s = re.sub(rf"\b{re.escape(w)}\b", " ", s)
    # Strip empty parens left over after page-ref removal: "( )" / "()".
    s = re.sub(r"\(\s*\)", " ", s)
    # Collapse whitespace.
    s = _WHITESPACE_RE.sub(" ", s).strip()
    # Trim trailing punctuation noise.
    s = re.sub(r"[\s,;:.\-]+$", "", s)
    return s


def build_group_key(finding: dict) -> str:
    """Strict ключ группы: только идентичные нормализованные old/new
    объединяются. Это первый, безопасный, уровень.

    Priority:
        1. theme + change_type + normalize(old_value) + normalize(new_value)
        2. if old/new пустые → theme + normalize(title) + normalize(summary)
    """
    theme = finding.get("__theme") or assign_theme(finding)
    ctype = (finding.get("type") or "changed").lower()

    nold = normalize_value(finding.get("old_value"))
    nnew = normalize_value(finding.get("new_value"))

    if nold or nnew:
        raw = f"{theme}|{ctype}|{nold}|{nnew}"
    else:
        ntitle = normalize_value(finding.get("title"))
        nsummary = normalize_value(finding.get("summary"))
        raw = f"{theme}|{ctype}|t:{ntitle}|s:{nsummary}"
    return raw


def build_semantic_group_key(finding: dict) -> str:
    """Мягкий semantic ключ — объединяет strict-группы по смыслу.

    Состав: theme + semantic_subject + semantic_action + change_direction.

    Дисциплина НЕ включается в ключ. Это позволяет cross-pair rollup для
    однотипных изменений в разных корпусах/разделах того же проекта
    (например, фасадный материал в АР1 и АР2). При этом subject и action
    уже достаточно специфичны, чтобы не склеить разные физические сущности.

    Если subject=unknown — возвращаем уникальный per-finding ключ, чтобы
    safety guard в `_cluster_semantic` не объединял такие группы между собой.
    """
    theme = finding.get("__theme") or assign_theme(finding)
    subject = finding.get("__semantic_subject") or extract_semantic_subject(finding)
    if subject == "unknown":
        # Каждый unknown остаётся в собственном кластере — без агрегации.
        return f"{theme}|__unknown__|{finding.get('id') or id(finding)}"
    action = finding.get("__semantic_action") or extract_semantic_action(finding)
    direction = finding.get("__change_direction") or infer_change_direction(finding)
    return f"{theme}|{subject}|{action}|{direction}"


def _short_hash(text: str, length: int = 10) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:length]


# ─── Significance scoring ────────────────────────────────────────────────


def _significance_for_group(group: dict) -> str:
    """Из cost_impact + theme + direction + formal status."""
    if group["is_formal"] and not group["escalation_reason"]:
        return "formal"

    cost = (group["cost_impact"] or "unknown").lower()
    theme = group["theme"]
    direction = group["change_direction"]

    if cost == "likely":
        return "high"
    if theme in HIGH_IMPACT_THEMES and direction == "complication":
        return "high"
    if theme in HIGH_IMPACT_THEMES and direction in ("simplification", "neutral"):
        return "medium"
    if cost == "possible":
        return "medium"
    if group["is_formal"] and group["escalation_reason"]:
        return "medium"
    if cost == "none" and not group["is_formal"]:
        return "low"
    return "low"


def _aggregate_cost_impact(items: list[dict]) -> str:
    """likely > possible > unknown > none."""
    rank = {"likely": 3, "possible": 2, "unknown": 1, "none": 0}
    best = -1
    best_label = "unknown"
    for it in items:
        c = (it.get("cost_impact") or "unknown").lower()
        r = rank.get(c, 1)
        if r > best:
            best = r
            best_label = c
    return best_label


def _max_severity(items: list[dict]) -> str:
    rank = {"high": 2, "medium": 1, "low": 0}
    best = -1
    best_label = "low"
    for it in items:
        s = (it.get("severity") or "low").lower()
        r = rank.get(s, 0)
        if r > best:
            best = r
            best_label = s
    return best_label


def _build_evidence(items: list[dict]) -> list[dict]:
    """Сборка evidence из source findings (left + right quote/section)."""
    out: list[dict] = []
    seen = set()
    for it in items:
        for side in ("left", "right"):
            ev = it.get(f"evidence_{side}") or {}
            if not isinstance(ev, dict):
                continue
            quote = (ev.get("quote") or "").strip()
            if not quote:
                continue
            key = (it.get("pair_id"), side, quote[:80])
            if key in seen:
                continue
            seen.add(key)
            out.append({
                "pair_id": it.get("pair_id"),
                "side": side,
                "section": (ev.get("section") or "").strip(),
                "approx_location": (ev.get("approx_location") or "").strip(),
                "quote": quote,
                "page": it.get("page"),
                "sheet": it.get("sheet"),
                "source_finding_id": it.get("id"),
            })
    return out


def _build_locations(items: list[dict]) -> list[dict]:
    """Where the group applies: pair_id + sheet + page."""
    out: list[dict] = []
    seen = set()
    for it in items:
        pid = it.get("pair_id")
        page = it.get("page")
        key = (pid, page)
        if key in seen:
            continue
        seen.add(key)
        out.append({
            "pair_id": pid,
            "pair_label": it.get("pair_label"),
            "sheet": it.get("sheet"),
            "page": page,
            "left_page": it.get("left_page"),
            "right_page": it.get("right_page"),
        })
    return out


def _discipline_from_pair_label(label: str) -> Optional[str]:
    """Попытаться вытащить дисциплину (АР, КР, ИОС1.1, и т.п.) из имени файла."""
    if not label:
        return None
    m = re.search(
        r"\b(АР\d*|КР\d*|ИОС[\d\.\-]*|ОВ\d*|ВК\d*|ЭО\d*|"
        r"ПЗУ|ПОС|ОДИ|ИКЕО|ООС\d*|ЭЭ|АК|СС|ТХ|ГП|АИ|ЭОМ)\b",
        label,
        re.IGNORECASE,
    )
    if m:
        return m.group(1).upper()
    return None


def _build_group(group_key: str, items: list[dict]) -> dict:
    """Собрать одну group из 1+ source findings.

    Все items должны иметь одинаковый group_key.
    """
    if not items:
        raise ValueError("empty items")

    # Первый item задаёт «канонический» title/old/new.
    head = items[0]
    is_formal_flag = bool(head.get("__is_formal"))
    formal_reason = head.get("__formal_reason")
    escalation = head.get("__escalation_reason")
    theme = head.get("__theme")
    direction = head.get("__change_direction")
    cost_direction = head.get("__cost_impact_direction")

    pair_ids = {it.get("pair_id") for it in items}
    scope_level = "session_rollup" if len(pair_ids) > 1 else "pair"

    cost_impact = _aggregate_cost_impact(items)
    severity_max = _max_severity(items)

    # Build single-strict-group value_variants list (one entry by default).
    nold = normalize_value(head.get("old_value"))
    nnew = normalize_value(head.get("new_value"))
    variant_ids = [it.get("id") for it in items if it.get("id")]
    value_variants = [{
        "old_value": (head.get("old_value") or "")[:400],
        "new_value": (head.get("new_value") or "")[:400],
        "source_finding_ids": variant_ids,
    }] if (nold or nnew or items) else []

    group = {
        "id": f"grp_{_short_hash(group_key, 12)}",
        "group_key": group_key,
        "semantic_subject": head.get("__semantic_subject"),
        "semantic_action": head.get("__semantic_action"),
        "title": (head.get("title") or "").strip()[:200],
        "theme": theme,
        "change_type": head.get("type") or "changed",
        "change_direction": direction,
        "significance": "low",  # будет переопределено ниже
        "is_formal": is_formal_flag,
        "formal_reason": formal_reason,
        "escalation_reason": escalation,
        "discipline": head.get("__discipline") or _discipline_from_pair_label(head.get("pair_label") or ""),
        "scope_level": scope_level,
        "old_value": (head.get("old_value") or "")[:400],
        "new_value": (head.get("new_value") or "")[:400],
        "value_variants": value_variants,
        "quantity_delta": {"unit": None, "old": None, "new": None, "delta": None},
        "cost_impact": cost_impact,
        "cost_impact_direction": cost_direction,
        "construction_impact": (head.get("construction_impact") or "")[:400],
        "affected_locations": _build_locations(items),
        "affected_pages": sorted({it.get("page") for it in items if it.get("page") is not None}),
        "affected_pair_ids": sorted(pair_ids),
        "affected_count": len(items),
        "evidence": _build_evidence(items),
        "source_finding_ids": [it.get("id") for it in items if it.get("id")],
        "confidence": round(max(float(it.get("confidence") or 0.0) for it in items), 3),
        "severity_max": severity_max,
        "requires_human_review": any(bool(it.get("requires_human_review")) for it in items),
        "review_reason": f"merged_from_{len(items)}" if len(items) > 1 else "single_source",
        "merge_info": {
            "strict_group_count": 1,
            "variant_count": 1,
        },
    }
    group["significance"] = _significance_for_group(group)
    return group


# ─── Top-level group_findings ────────────────────────────────────────────


def _annotate_items(items: list[dict]) -> list[dict]:
    """Добавить computed-поля прямо в items (in-place safe, items уже копии)."""
    out: list[dict] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        annotated = dict(it)
        cls = classify_formality(annotated)
        annotated["__is_formal"] = cls["is_formal"]
        annotated["__formal_reason"] = cls["formal_reason"]
        annotated["__escalation_reason"] = cls["escalation_reason"]
        annotated["__theme"] = assign_theme(annotated)
        annotated["__change_direction"] = infer_change_direction(annotated)
        annotated["__cost_impact_direction"] = infer_cost_impact_direction(annotated)
        annotated["__semantic_subject"] = extract_semantic_subject(annotated)
        annotated["__semantic_action"] = extract_semantic_action(annotated)
        annotated["__discipline"] = extract_discipline_or_system(annotated)
        out.append(annotated)
    return out


# ─── Two-tier grouping ───────────────────────────────────────────────────


def _can_merge_strict_groups(a: dict, b: dict) -> tuple[bool, Optional[str]]:
    """Safety guard для semantic merge двух strict-групп.

    Возвращает (allowed, reason). Если allowed=False — strict-группы остаются
    раздельно.
    """
    # 1. Конфликт по cost_impact_direction.
    a_cd = (a.get("cost_impact_direction") or "unknown")
    b_cd = (b.get("cost_impact_direction") or "unknown")
    if a_cd in ("increase", "decrease") and b_cd in ("increase", "decrease") and a_cd != b_cd:
        return False, "cost_direction_conflict"

    # 2. Разные change_direction (complication vs simplification).
    a_dir = a.get("change_direction") or "unknown"
    b_dir = b.get("change_direction") or "unknown"
    if (a_dir == "complication" and b_dir == "simplification") or \
       (a_dir == "simplification" and b_dir == "complication"):
        return False, "change_direction_conflict"

    return True, None


def _cluster_semantic(strict_groups: list[dict]) -> list[dict]:
    """Объединить strict-группы в semantic-кластеры по
    `semantic_group_key` с safety guards.

    Каждый semantic кластер собирает все source items всех вошедших strict
    групп. Разные old/new сохраняются в `value_variants[]`.
    """
    buckets: dict[str, list[dict]] = {}
    order: list[str] = []
    for g in strict_groups:
        key = g.get("__semantic_key") or "__none__"
        if key not in buckets:
            buckets[key] = []
            order.append(key)
        buckets[key].append(g)

    out: list[dict] = []
    for key in order:
        group_list = buckets[key]
        if len(group_list) == 1 or key.startswith("__") or "__unknown__" in key:
            # Single или unknown subject — semantic merge не делаем.
            out.extend(group_list)
            continue
        # Попробуем последовательно сливать, отсекая по safety guard.
        merged_anchor = group_list[0]
        rejected: list[dict] = []
        compatible = [merged_anchor]
        for cand in group_list[1:]:
            ok, _reason = _can_merge_strict_groups(merged_anchor, cand)
            # Дополнительно: соответствие cand vs все уже подсоединённые.
            if ok:
                for existing in compatible[1:]:
                    ok2, _r2 = _can_merge_strict_groups(existing, cand)
                    if not ok2:
                        ok = False
                        break
            if ok:
                compatible.append(cand)
            else:
                rejected.append(cand)
        if len(compatible) > 1:
            out.append(_merge_into_semantic(compatible))
        else:
            out.append(merged_anchor)
        # Rejected strict groups остаются как есть.
        out.extend(rejected)
    return out


def _merge_into_semantic(strict_groups: list[dict]) -> dict:
    """Слить N strict-групп в одну semantic с value_variants[]."""
    # Берём anchor (первый — с наибольшим affected_count после сортировки
    # в _cluster_semantic-ниже не делаем; используем как есть).
    anchor = strict_groups[0]
    # Собрать все source items (для evidence/locations/value variants).
    all_items: list[dict] = []
    for g in strict_groups:
        all_items.extend(g.get("__items") or [])

    # value_variants: уникальные нормализованные пары (old, new).
    variants_map: dict[tuple[str, str], dict] = {}
    for it in all_items:
        nold = normalize_value(it.get("old_value"))
        nnew = normalize_value(it.get("new_value"))
        key = (nold, nnew)
        if key not in variants_map:
            variants_map[key] = {
                "old_value": (it.get("old_value") or "")[:400],
                "new_value": (it.get("new_value") or "")[:400],
                "source_finding_ids": [],
            }
        sid = it.get("id")
        if sid and sid not in variants_map[key]["source_finding_ids"]:
            variants_map[key]["source_finding_ids"].append(sid)

    pair_ids = sorted({it.get("pair_id") for it in all_items if it.get("pair_id")})
    scope_level = "session_rollup" if len(pair_ids) > 1 else "pair"

    cost_impact = _aggregate_cost_impact(all_items)
    severity_max = _max_severity(all_items)

    # title: берём самый «осмысленный» — самый длинный из anchor + других, но
    # не длиннее 200 символов.
    title_candidates = [g.get("title") or "" for g in strict_groups]
    title = max(title_candidates, key=len)[:200]

    construction_impact = max(
        (g.get("construction_impact") or "" for g in strict_groups), key=len
    )[:400]

    main_old = anchor.get("old_value") or ""
    main_new = anchor.get("new_value") or ""
    if len(variants_map) > 1:
        # Если variant > 1 — заменяем main old/new на summary-метку.
        main_old = f"см. value_variants ({len(variants_map)} вариантов)"
        main_new = f"см. value_variants ({len(variants_map)} вариантов)"

    key_str = anchor.get("__semantic_key") or "merged"
    merged = {
        "id": f"grp_{_short_hash(key_str + '|' + '|'.join(pair_ids), 12)}",
        "group_key": key_str,
        "semantic_group_key": key_str,
        "semantic_subject": anchor.get("semantic_subject") or anchor.get("__semantic_subject"),
        "semantic_action": anchor.get("semantic_action") or anchor.get("__semantic_action"),
        "title": title,
        "theme": anchor.get("theme"),
        "change_type": anchor.get("change_type") or "changed",
        "change_direction": anchor.get("change_direction"),
        "significance": "low",
        "is_formal": anchor.get("is_formal"),
        "formal_reason": anchor.get("formal_reason"),
        "escalation_reason": anchor.get("escalation_reason"),
        "discipline": anchor.get("discipline"),
        "scope_level": scope_level,
        "old_value": main_old[:400],
        "new_value": main_new[:400],
        "value_variants": list(variants_map.values()),
        "quantity_delta": {"unit": None, "old": None, "new": None, "delta": None},
        "cost_impact": cost_impact,
        "cost_impact_direction": anchor.get("cost_impact_direction"),
        "construction_impact": construction_impact,
        "affected_locations": _build_locations(all_items),
        "affected_pages": sorted({it.get("page") for it in all_items if it.get("page") is not None}),
        "affected_pair_ids": pair_ids,
        "affected_count": len(all_items),
        "evidence": _build_evidence(all_items),
        "source_finding_ids": [it.get("id") for it in all_items if it.get("id")],
        "confidence": round(max(float(it.get("confidence") or 0.0) for it in all_items), 3),
        "severity_max": severity_max,
        "requires_human_review": any(bool(it.get("requires_human_review")) for it in all_items)
                                or len(variants_map) > 1,
        "review_reason": (
            "multiple_value_variants" if len(variants_map) > 1
            else f"semantic_cluster_from_{len(strict_groups)}_strict_groups"
        ),
        "merge_info": {
            "strict_group_count": len(strict_groups),
            "variant_count": len(variants_map),
        },
    }
    merged["significance"] = _significance_for_group(merged)
    return merged


def group_findings(flat_items: list[dict]) -> dict:
    """Двухпроходная группировка.

    Pass 1: strict — точная пара normalize(old) / normalize(new).
    Pass 2: semantic cluster — theme+subject+action+direction+discipline,
            с safety guards (cost_direction conflict, change_direction conflict,
            unknown subject).

    Каждый source_finding попадает либо в visible group, либо в hidden_formal.
    Evidence сохраняется полностью.
    """
    if flat_items is None:
        flat_items = []
    annotated = _annotate_items(flat_items)

    # ── Pass 1: strict bucketing ────────────────────────────────────────
    strict_buckets: dict[str, list[dict]] = {}
    strict_order: list[str] = []
    for it in annotated:
        key = build_group_key(it)
        if key not in strict_buckets:
            strict_buckets[key] = []
            strict_order.append(key)
        strict_buckets[key].append(it)

    strict_groups_visible: list[dict] = []
    strict_groups_hidden: list[dict] = []
    for key in strict_order:
        grp = _build_group(key, strict_buckets[key])
        # Прикрепляем raw items + semantic key для второго прохода.
        grp["__items"] = strict_buckets[key]
        anchor = strict_buckets[key][0]
        grp["__semantic_key"] = build_semantic_group_key(anchor)
        if grp["significance"] == "formal":
            strict_groups_hidden.append(grp)
        else:
            strict_groups_visible.append(grp)

    # ── Pass 2: semantic clustering (только visible) ────────────────────
    clustered_visible = _cluster_semantic(strict_groups_visible)
    # Hidden formal — semantic clustering применяем тоже, но с тем же guard'ом.
    # Это позволит склеить «Изменён ГИП» × N pair в одну hidden group.
    clustered_hidden = _cluster_semantic(strict_groups_hidden)

    # Cleanup private fields.
    for g in clustered_visible + clustered_hidden:
        g.pop("__items", None)
        g.pop("__semantic_key", None)

    # Сортировка: significance high → low → formal; theme; affected_count desc.
    sig_rank = {"high": 0, "medium": 1, "low": 2}
    clustered_visible.sort(key=lambda g: (
        sig_rank.get(g["significance"], 9),
        g["theme"] or "",
        -g["affected_count"],
    ))
    clustered_hidden.sort(key=lambda g: (g["theme"] or "", -g["affected_count"]))

    summary = _build_summary(annotated, clustered_visible, clustered_hidden)
    return {
        "version": VERSION,
        "summary": summary,
        "groups": clustered_visible,
        "hidden_formal_groups": clustered_hidden,
    }


def _build_summary(annotated: list[dict], visible: list[dict], hidden: list[dict]) -> dict:
    from collections import Counter

    raw_count = len(annotated)
    grouped_count = len(visible)
    hidden_count = len(hidden)

    by_theme = Counter(g["theme"] for g in visible)
    by_discipline = Counter(g.get("discipline") or "—" for g in visible)
    by_significance = Counter(g["significance"] for g in visible)
    by_significance["formal"] = hidden_count
    by_change_direction = Counter(g["change_direction"] for g in visible)
    by_cost_impact_direction = Counter(g["cost_impact_direction"] for g in visible)

    high = by_significance.get("high", 0)
    medium = by_significance.get("medium", 0)
    low = by_significance.get("low", 0)

    return {
        "raw_findings_count": raw_count,
        "grouped_findings_count": grouped_count,
        "hidden_formal_count": hidden_count,
        "high_value_count": high,
        "medium_value_count": medium,
        "low_value_count": low,
        "formal_count": hidden_count,
        "by_theme": dict(by_theme),
        "by_discipline": dict(by_discipline),
        "by_significance": dict(by_significance),
        "by_change_direction": dict(by_change_direction),
        "by_cost_impact_direction": dict(by_cost_impact_direction),
    }


# ─── IO + endpoint helpers ───────────────────────────────────────────────


def _utc_now() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _read_grouped(session_id: str) -> Optional[dict]:
    import json

    p = paths_mod.unified_findings_grouped_path(session_id)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _write_grouped(session_id: str, payload: dict) -> dict:
    import json

    p = paths_mod.unified_findings_grouped_path(session_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)
    return payload


def build_unified_grouped(
    session_id: str,
    *,
    force: bool = False,
    persist: bool = True,
) -> dict:
    """Собрать grouped JSON для сессии и опционально записать на диск.

    `force=True` → пересобрать даже если файл существует.
    `persist=False` → не писать на диск (для in-memory smoke).

    Источник данных — `unified_findings.json` (persisted). Если он
    отсутствует, пробуем построить flat через `build_unified_flat`.
    """
    with _lock:
        if not force:
            existing = _read_grouped(session_id)
            if existing is not None:
                return existing

        # Источник 1: persisted unified_findings.json.
        persisted = unified_findings_mod.get_unified_findings(session_id)
        items = persisted.get("items") or []
        # Источник 2: live build (если на диске пусто).
        if not items:
            try:
                flat = unified_findings_mod.build_unified_flat(session_id)
                items = flat.get("items") or []
            except KeyError:
                # session not found → пустая structure, но не падаем
                items = []

        result = group_findings(items)
        payload = {
            "version": VERSION,
            "session_id": session_id,
            "created_at": _utc_now(),
            "source": "unified_findings.json",
            **result,
        }
        if persist:
            _write_grouped(session_id, payload)
        return payload


def _apply_query_filters(
    payload: dict,
    *,
    include_formal: bool = False,
    pair_id: Optional[str] = None,
    significance: Optional[str] = None,
    theme: Optional[str] = None,
) -> dict:
    """Применить query-фильтры к persisted grouped payload.

    Read-only, не пишет на диск.
    """
    groups: list[dict] = list(payload.get("groups") or [])
    hidden: list[dict] = list(payload.get("hidden_formal_groups") or [])

    if pair_id:
        groups = [
            g for g in groups
            if pair_id in (g.get("affected_pair_ids") or [])
        ]
        hidden = [
            g for g in hidden
            if pair_id in (g.get("affected_pair_ids") or [])
        ]

    if significance:
        groups = [g for g in groups if g.get("significance") == significance]

    if theme:
        groups = [g for g in groups if g.get("theme") == theme]

    out = dict(payload)
    out["groups"] = groups
    if include_formal:
        out["hidden_formal_groups"] = hidden
    else:
        # Сохраняем счётчик, но саму выборку убираем для compact ответа.
        out["hidden_formal_groups"] = []
    out["_filters_applied"] = {
        "pair_id": pair_id,
        "include_formal": include_formal,
        "significance": significance,
        "theme": theme,
    }
    return out


def get_unified_grouped(
    session_id: str,
    *,
    pair_id: Optional[str] = None,
    include_formal: bool = False,
    force_rebuild: bool = False,
    significance: Optional[str] = None,
    theme: Optional[str] = None,
) -> dict:
    """Endpoint-friendly accessor. Lazy build при отсутствии файла."""
    payload = build_unified_grouped(session_id, force=force_rebuild, persist=True)
    return _apply_query_filters(
        payload,
        pair_id=pair_id,
        include_formal=include_formal,
        significance=significance,
        theme=theme,
    )


__all__ = [
    "VERSION",
    "classify_formality",
    "assign_theme",
    "infer_change_direction",
    "infer_cost_impact_direction",
    "extract_semantic_subject",
    "extract_semantic_action",
    "extract_discipline_or_system",
    "normalize_value",
    "build_group_key",
    "build_semantic_group_key",
    "group_findings",
    "build_unified_grouped",
    "get_unified_grouped",
]
