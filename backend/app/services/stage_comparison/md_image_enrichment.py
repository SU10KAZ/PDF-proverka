"""MD enrichment pipeline для раздела «Сравнение стадий».

Подготавливает enriched MD для последующего смыслового сравнения стадий.
Ключевое отличие от старого подхода (см. `text_llm_input.prepare_text_only_markdown`):
изображения теперь не удаляются, а заменяются на улучшенное описание от
локальной VLM (Qwen 35B через LM Studio + ngrok). Сам исходный MD остаётся
неизменным — enriched-версия пишется в `comparison/sessions/<sid>/pairs/<pid>/text_enrichment/`.

Никаких внешних paid API (OpenRouter / OpenAI / Gemini / Anthropic) этот
модуль не использует — только `local_openai_compatible` provider.

Flow:
    1. Прочитать MD одной стороны (left/right).
    2. Найти text-блоки и image/imagine-блоки + их связь с реальной картинкой
       (block_id / page → result.json или render_block_crop).
    3. Для каждого image-блока:
         a. Прокатать кеш по sha256(image_bytes + model + prompt_version);
         b. Если кеш-промах и `run_model=True`, дернуть `describe_image_local`;
         c. Иначе оставить status=pending (dry-run).
    4. Собрать enriched MD: текст не трогаем, image-блоки оборачиваем
       `original_imagine_start/end` + добавляем `#### QWEN_IMAGE_DESCRIPTION`.
    5. Записать enriched MD + image_descriptions.json + сохранить prompts/raw.

Контракт enrichment(run_model=False) — dry-run:
    * никаких сетевых вызовов;
    * MD парсится; обнаруженные image/imagine-блоки попадают в counts;
    * enriched MD НЕ перезаписывается (если уже существует, его не трогаем);
    * `summary.described == summary.from_cache + успешные блоки в этой сессии`.

Контракт enrichment(run_model=True):
    * для каждого image-блока с найденной картинкой — вызвать describe_image_local;
    * результат пишется в кеш и в image_descriptions.json;
    * enriched MD пересобирается полностью.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

from . import graphic_llm_local as graphic_local_mod
from . import paths as paths_mod
from . import problem_block_retry as problem_block_retry_mod

logger = logging.getLogger(__name__)


# Версия prompt'а — входит в cache-key. При изменении prompt'а — инкрементируем.
# v1                       → базовое описание + structured-поля
# v2_scheme_analysis       → + structural/single-line schemes (электро, ОВиК, гидравлика,
#                             автоматика, слаботочка, технологические процессы)
# v3_no_ellipsis_chunking  → анти-ellipsis + continues/next_chunk_hint/coverage_notes
#                             (борьба с массовыми json_parse_failed)
# v4_compact               → короткий prompt без агрессивных "ВНИМАНИЕ"/"ПОВТОРЯЮ",
#                             explicit limits на массивы, scheme_analysis сохранён.
#                             Benchmark 2026-05-26 на 4 heavy HVAC блоках:
#                             4/4 success (vs 2/4 на v2), avg 27s vs 96s.
PROMPT_VERSION = "v4_compact"


# Версия формата enriched MD (left_enriched.md / right_enriched.md):
#   "append_v0"               — legacy: <!-- original_imagine_start --> wrapper +
#                                #### QWEN_IMAGE_DESCRIPTION рядом со старым блоком.
#                                В таком формате Opus видел и старое OCR-описание,
#                                и Qwen-описание одного и того же блока — это
#                                раздувало enriched MD и могло конфликтовать.
#   "replace_image_blocks_v1" — НЫНЕШНИЙ default: image/imagine-блок ПОЛНОСТЬЮ
#                                заменяется на структурированное Qwen-описание
#                                в HTML-обёртке <!-- QWEN_IMAGE_DESCRIPTION_START
#                                ... QWEN_IMAGE_DESCRIPTION_END -->. Старое OCR
#                                из исходного MD физически отсутствует в основном
#                                enriched.md (debug-метаданные сохранены в
#                                image_descriptions.json).
ENRICHED_MD_FORMAT_VERSION = "replace_image_blocks_v1"

# Маркеры старого формата — используются для детекции outdated enriched.md
# при rebuild без повторного Qwen.
_LEGACY_ENRICHED_MARKER = "<!-- original_imagine_start -->"
# Маркер нового формата
_REPLACE_ENRICHED_MARKER = "QWEN_IMAGE_DESCRIPTION_START"


# Compact-mode лимиты прописаны прямо в prompt'е — модель видит верхнюю границу
# и не пытается перечислить «всё» (что приводило к truncation'у на max_tokens).
# Эти числа также используются для документации.
COMPACT_PROMPT_LIMITS = {
    "nodes": 30,
    "connections": 30,
    "numeric_parameters": 40,
    "visible_text": 25,
    "comparison_relevant_facts": 8,
    "comparison_relevant_scheme_facts": 8,
    "uncertainties": 5,
}


QWEN_IMAGE_DESCRIPTION_PROMPT = """Ты анализируешь изображение из проектной/рабочей документации в строительстве.

Задача — извлечь информацию для сравнения стадий проекта.

ПРАВИЛА ВЫВОДА:

1. Возвращай только валидный, полностью закрытый JSON. Никакого markdown, никакого текста до или после JSON, никаких комментариев `//`.
2. Не используй многоточие (`…`, `...`, `etc.`, `и т.д.`, `и др.`, `и тому подобное`) нигде в JSON.
3. Не обрывай ключи или значения «на полуслове». Сократи формулировку, но всегда закрывай кавычку и ставь `,` или `}` где надо.
4. Лимиты массивов (не превышай):
   - `nodes`: до 30
   - `connections`: до 30
   - `numeric_parameters`: до 40
   - `visible_text`: до 25 строк (только значимый текст: штампы, маркировка, отметки)
   - `comparison_relevant_facts`: до 8
   - `comparison_relevant_scheme_facts`: до 8
   - `uncertainties`: до 5
5. Поля `coverage_notes`, `continues`, `next_chunk_hint` идут В САМОМ КОНЦЕ JSON, после `confidence`, в указанном порядке. Не пиши их в начале.
   - Если не помещаешься: `continues=true`, `next_chunk_hint` = что осталось.
   - Если помещаешься полностью: `continues=false`, `next_chunk_hint=""`.

Опиши: что изображено (чертёж, схема, таблица, штамп, спецификация); проектные решения, материалы, оборудование, числовые параметры (размеры, отметки, мощности, расходы, марки, количества), требования, таблицы; видимый значимый текст (штампы, маркировка); что важно отслеживать при сравнении со следующей стадией.

Если это штамп — извлеки организацию, стадию, шифр, лист, год, разработчика/проверяющего.
Если читается плохо — пиши явно («не читается», «низкое разрешение»), не многоточие.

ДОПОЛНИТЕЛЬНЫЙ АНАЛИЗ СХЕМЫ:
Если изображение — структурная/однолинейная схема, схема воздуха/воды/электричества/сигналов/автоматики/слаботочка/технологический процесс — заполни `scheme_analysis`:

- `scheme_type`: electrical_single_line | hvac_air_flow | water_or_liquid_flow | automation_signal | low_voltage_system | process_scheme | structural_scheme | unknown_scheme
- `flow_medium`: electricity | air | water | liquid | heat_carrier | signal | control | data | unknown
- `nodes`: узлы (источник, ввод, оборудование, щит, автомат, насос, клапан, датчик, контроллер, потребитель и т.п.)
- `connections`: связи `{from,to,direction,line_label,parameters,evidence,confidence}`
- `sequence_summary`: последовательности типа «Ввод → ВРУ → АВР → нагрузка»
- `independent_circuits`: независимые контуры, если их несколько
- `comparison_relevant_scheme_facts`: байпасы, резервные линии, изменения порядка/маркировки/параметров
- `uncertainties`: что не читается, где направление непонятно

Не выдумывай связи. Если направление не определено — пиши «направление не определено».
Если изображение НЕ является схемой — `scheme_analysis.is_scheme=false`, массивы пустые.

Верни JSON по такой схеме (заполнители вида `<...>` — это шаблон, не пиши их в ответе):

{
"status": "done",
"image_kind": "drawing|table|scheme|plan|facade|section|node|stamp|specification|unknown",
"summary": "<краткое описание>",
"design_solutions": ["<решение_1>"],
"materials": ["<материал_1>"],
"equipment": ["<оборудование_1>"],
"numeric_parameters": [{"name":"<имя>","value":"<значение>","unit":"<единица>","context":"<контекст>"}],
"requirements": ["<требование_1>"],
"tables": ["<табличный_факт_1>"],
"visible_text": ["<видимый_текст_1>"],
"comparison_relevant_facts": ["<существенный_факт_1>"],
"uncertainties": ["<неопределённость_1>"],
"scheme_analysis": {
"is_scheme": true,
"scheme_type": "electrical_single_line|hvac_air_flow|water_or_liquid_flow|automation_signal|low_voltage_system|process_scheme|structural_scheme|unknown_scheme",
"flow_medium": "electricity|air|water|liquid|heat_carrier|signal|control|data|unknown",
"nodes": [{"id":"node_1","label":"ВРУ","type":"source|input|panel|breaker|meter|equipment|valve|pump|fan|filter|heater|sensor|controller|actuator|consumer|junction|line|unknown","visible_mark":"...","parameters":["..."],"confidence":0.0}],
"connections": [{"from":"node_1","to":"node_2","direction":"left_to_right|right_to_left|top_to_bottom|bottom_to_top|bidirectional|unknown","line_label":"...","parameters":["..."],"evidence":"стрелка/линия/подпись","confidence":0.0}],
"sequence_summary": ["Ввод → ВРУ → АВР → ГРЩ → нагрузка"],
"independent_circuits": [{"name":"Контур 1","sequence":"Источник → элемент → потребитель","notes":"..."}],
"comparison_relevant_scheme_facts": ["В цепочке присутствует байпасная линия"],
"uncertainties": ["Направление потока между узлами X и Y не читается"]
},
"confidence": 0.0,
"coverage_notes": "<охвачено: ...; осталось: ...>",
"continues": false,
"next_chunk_hint": ""
}

Если изображение не схема — оставь:
"scheme_analysis": {"is_scheme": false, "scheme_type": "unknown_scheme", "flow_medium": "unknown", "nodes": [], "connections": [], "sequence_summary": [], "independent_circuits": [], "comparison_relevant_scheme_facts": [], "uncertainties": []}

Никакого markdown вне JSON. Все скобки и кавычки закрыты.
"""


# ─── v5 prompt семейство ──────────────────────────────────────────────────
#
# v5_scheme_diff_anchors — специализированный prompt для электрических/
# инженерных однолинейных схем. Главное отличие от v4_compact: модель
# обязана выдать буквальные diff-якоря (raw_text маркировок щитов,
# кабелей, автоматов), а не «общее описание». Эти якоря потом попадают
# в IMAGE_DIFF_INDEX и помогают Opus реально увидеть, что в правой
# стадии появились/исчезли позиции вроде «ЩР-1а», «ВРУ-2 с.ш.1», «QF3».
PROMPT_VERSION_GENERAL = "v4_compact"
PROMPT_VERSION_SCHEME = "v5_scheme_diff_anchors"


QWEN_SCHEME_DIFF_ANCHORS_PROMPT = """Ты анализируешь однолинейную/структурную схему из проектной документации (электрика, ОВиК, водопровод, автоматика, слаботочка).

Главная задача — извлечь БУКВАЛЬНЫЕ diff-якоря: ровно те маркировки, кабели, номиналы, связи, которые видны на ЭТОМ изображении. Это нужно, чтобы при сравнении двух стадий проекта можно было сказать, какие позиции реально появились/исчезли/поменялись.

ТЫ ДОЛЖЕН ВЕРНУТЬ КОМПАКТНЫЙ JSON. Лучше меньше anchors и литеральные, чем много и нагалюциированные. Не пытайся заполнить max_tokens. Если всё важное извлечено — заверши JSON.

КРИТИЧЕСКИЕ ПРАВИЛА (нарушать НЕЛЬЗЯ):

1. Возвращай только валидный JSON. Никакого markdown, никакого текста до или после JSON, никаких комментариев `//`.
2. `raw_text` ВСЕГДА буквальная видимая надпись. НЕ нормализуй:
   - «ЩР-1а» НЕ становится «Щит 1»;
   - «ВРУ-2 с.ш.1» НЕ становится «вводное устройство»;
   - «QF3» НЕ становится «автоматический выключатель»;
   - «4х185» НЕ становится «кабель сечением 185»;
   - «1000А» НЕ становится «номинал тока».
3. Если виден тип объекта, но конкретная маркировка не читается — пиши `raw_text = "[маркировка не читается]"` и поясняй в comment, какой это тип.
4. Если не уверен в распознавании — клади запись в `diff_anchors.uncertain_text` с альтернативами, а не выдумывай определённое значение.
5. НЕ добавляй типовые номиналы/сечения, если они не видны на изображении (напр. «100А, 160А, 250А, 400А, 630А» — это каталог, а не наблюдение).
6. **АНТИ-ЭКСТРАПОЛЯЦИЯ РЯДОВ** (главный source галлюцинаций):
   - Если на схеме виден ряд однотипных позиций («ЩА-1.1», «ЩА-1.2», «ЩА-1.3»), и отдельные номера читаются плохо/не все — НЕ ДОСТРАИВАЙ ряд до 10/20/40 элементов.
   - НЕЛЬЗЯ выводить «ЩА-1.4, ЩА-1.5, ..., ЩА-1.40» только потому, что есть номера 1-3 и «дом на 40 квартир».
   - НЕЛЬЗЯ перечислять искусственные ряды вроде «ВРП-1, ВРП-2, ..., ВРП-50» / «QF1, QF2, ..., QF50».
   - Если видишь только 3 чётких номера в серии — пиши только эти 3 в `labels`.
   - Если виден паттерн, но не каждый номер — добавь ОДИН элемент в `uncertain_text`:
     `{"possible_text": "ЩА-1.N", "alternatives": ["ЩА-1.1", "ЩА-1.2", "ЩА-1.3"], "confidence": 0.4, "why_uncertain": "видна серия похожих маркировок, отдельные номера читаются не полностью"}`.
     Это ОДИН элемент, не сорок.
7. **АНТИ-ЦЕПОЧКА**: запрещены последовательные connections между членами одной серии («ЩА-1.1 → ЩА-1.2», «ЩА-1.2 → ЩА-1.3»…), если ты не видишь явно нарисованной линии соединения между этими двумя точками.
   - В МКД квартирные/апартаментные щиты по умолчанию — параллельные потребители от общего щитка, а не цепочка. Если на чертеже не нарисована явная серия линий «1→2→3» — НЕ выводи их.
   - Типичная корректная связь: `ВРУ-2 → ЩА-1.1`, `ВРУ-2 → ЩА-1.2`, ... (звезда от ввода к квартирам).
   - Запрещено: `ЩА-1.1 → ЩА-1.2 → ЩА-1.3 → ...` (цепочка между потребителями).
8. **АНТИ-ДУБЛИКАТНЫЕ КОММЕНТАРИИ**: если у тебя 10+ labels — comment должен либо отсутствовать (пустая строка), либо быть индивидуальным для каждой позиции. Запрещено повторять одинаковый comment (напр. «читается в левой части схемы») для десятков labels.
9. Не используй текст из предыдущих/соседних изображений. Только то, что есть НА ЭТОЙ картинке.
10. Не добавляй организацию/адрес/номер листа, если они не видны на этом изображении.
11. `summary` — 1-2 коротких предложения (≤200 символов), без фактов, которых нет в anchors / visible_text / scheme_analysis.
12. **ЖЁСТКИЕ ЛИМИТЫ МАССИВОВ** (не превышай — лучше меньше, чем добитьcя catalog-fill):
    - `diff_anchors.labels` ≤ **25** (только реально читаемые индивидуально маркировки)
    - `diff_anchors.ratings` ≤ **20** (только видимые значения)
    - `diff_anchors.connections` ≤ **15** (только явно нарисованные соединения)
    - `diff_anchors.uncertain_text` ≤ **10**
    - `visible_text` ≤ 20
    - `numeric_parameters` ≤ 25
    - `nodes` ≤ 20, `connections` (scheme_analysis) ≤ 15
    - `comparison_relevant_facts` ≤ 6, `comparison_relevant_scheme_facts` ≤ 6
    - `uncertainties` ≤ 5
13. Поля `coverage_notes`, `continues`, `next_chunk_hint` идут в самом конце JSON, после `confidence`. **continues=false** в подавляющем большинстве случаев — модель должна успеть вместить compact diff в один chunk. Если не помещаешься — это сигнал, что в массивах был catalog-fill, сократи labels/ratings/connections.

Если читается плохо — пиши явно («не читается», «низкое разрешение»), не многоточие.

Верни JSON по такой схеме (плейсхолдеры `<...>` — шаблон, не пиши их в ответе):

{
"status": "done",
"image_kind": "scheme",
"diff_anchors": {
"labels": [{"raw_text":"ЩР-1а","normalized_type":"panel","confidence":0.0,"comment":"<откуда взято>"}],
"ratings": [{"raw_text":"1000А","value_type":"current_rating","related_to":"<raw маркировка соседней позиции или \"\">","confidence":0.0}],
"connections": [{"from_raw":"ВРУ-2 с.ш.1","to_raw":"ЩР-1а","relation":"питает","confidence":0.0}],
"uncertain_text": [{"possible_text":"ЩР-1?","alternatives":["ЩО-1?","ЩР-1а?"],"confidence":0.0,"why_uncertain":"<почему>"}]
},
"summary": "<краткое описание ≤350 символов, без новых фактов>",
"visible_text": ["<буквальный видимый текст_1>"],
"numeric_parameters": [{"name":"<имя>","value":"<значение>","unit":"<ед>","context":"<контекст>"}],
"scheme_analysis": {
"is_scheme": true,
"scheme_type": "electrical_single_line|hvac_air_flow|water_or_liquid_flow|automation_signal|low_voltage_system|process_scheme|structural_scheme|unknown_scheme",
"flow_medium": "electricity|air|water|liquid|heat_carrier|signal|control|data|unknown",
"nodes": [{"id":"node_1","label":"<raw маркировка>","type":"source|input|panel|breaker|meter|equipment|valve|pump|fan|filter|heater|sensor|controller|actuator|consumer|junction|line|unknown","visible_mark":"<raw>","parameters":["<raw>"],"confidence":0.0}],
"connections": [{"from":"node_1","to":"node_2","direction":"left_to_right|right_to_left|top_to_bottom|bottom_to_top|bidirectional|unknown","line_label":"<raw>","parameters":["<raw>"],"evidence":"стрелка/линия/подпись","confidence":0.0}],
"sequence_summary": ["<последовательность с raw маркировками>"],
"comparison_relevant_scheme_facts": ["<что важно для diff>"],
"uncertainties": ["<что не читается>"]
},
"comparison_relevant_facts": ["<буквальный факт для diff_1>"],
"uncertainties": ["<неопределённость_1>"],
"confidence": 0.0,
"coverage_notes": "<охвачено: ...; осталось: ...>",
"continues": false,
"next_chunk_hint": ""
}

Допустимые `normalized_type` для labels:
panel — щит (ЩР, ЩО, ЩАО, ГРЩ, ЩС и т.п.)
switchgear — ВРУ, ВРП, РУ
breaker — QF, QS, KM, F
line — фидерная линия
cable — кабель/провод
room — помещение
stamp — штамп
other — остальное (с указанием в comment)

Допустимые `value_type` для ratings:
current_rating — A
cable_section — мм² (4х10, 5х95 и т.п.)
power — кВт/Вт/кВА
voltage — В/кВ
quantity — количество (шт., n=2)
other — прочее (с указанием в comment)

Связи (connections):
relation — «питает» / «резервирует» / «подключён к» / «через» / иное короткое словосочетание. from_raw/to_raw — буквальные маркировки видимых узлов.

Никакого markdown вне JSON. Все скобки и кавычки закрыты.
"""


# ─── Block-type классификатор ────────────────────────────────────────────
#
# Классификатор image-блока по эвристикам из заголовка/окружения MD.
# Используется enrich_side(), чтобы выбрать правильный prompt и параметры
# рендера/inference для каждого блока:
#   * scheme/dense_scheme  → v5_scheme_diff_anchors + высокий long_side
#   * table_legend          → v4_compact + средний long_side + чуть больше токенов
#   * stamp                 → v4_compact + маленький long_side
#   * plan                  → v4_compact + средний long_side
#   * photo_or_general      → текущие default'ы (v4_compact, 1100)

BLOCK_TYPE_SCHEME = "scheme"
BLOCK_TYPE_DENSE_SCHEME = "dense_scheme"
BLOCK_TYPE_TABLE_LEGEND = "table_legend"
BLOCK_TYPE_STAMP = "stamp"
BLOCK_TYPE_PLAN = "plan"
BLOCK_TYPE_GENERAL = "photo_or_general"


_SCHEME_STRONG_MARKERS = (
    "ВРУ",
    "ВРП",
    "ЩР",
    "ЩО",
    "ЩАО",
    "ГРЩ",
    "ЩС-",
    "QF",
    "QS",
    "KM",
    "АВР",
    "РУ-",
    "с.ш.",
)

_SCHEME_WEAK_MARKERS = (
    "однолинейн",
    "схема",
    "кабел",
    "автомат",
    "линия",
)

# Полный список для backward-compat (некоторые тесты могут считать общее
# число попаданий).
_SCHEME_MARKERS = _SCHEME_STRONG_MARKERS + _SCHEME_WEAK_MARKERS

_TABLE_LEGEND_MARKERS = (
    "таблица",
    "спецификация",
    "экспликация",
    "ведомость",
    "условные обозначения",
    "перечень",
)

_STAMP_MARKERS = (
    "стадия",
    "лист",
    "изм.",
    "подп.",
    "шифр",
    "лит.",
    "разраб.",
    "пров.",
)

_PLAN_MARKERS = (
    "план",
    "этаж",
    "помещени",
    "ось ",
    "оси ",
    "трасса",
)


def _count_marker_hits(text: str, markers: tuple[str, ...]) -> int:
    if not text:
        return 0
    low = text.lower()
    return sum(low.count(m.lower()) for m in markers)


def classify_image_block(
    mb: "MdBlock",
    side_block: Optional[dict] = None,
    surrounding_context: Optional[str] = None,
) -> str:
    """Эвристически определить тип image-блока.

    Возвращает один из BLOCK_TYPE_* (scheme / dense_scheme / plan /
    table_legend / stamp / photo_or_general). Используется для выбора
    per-type prompt'а и render/inference настроек.

    Сигналы:
      * текст заголовка MD-блока (`mb.text`);
      * surrounding_context — окружающий MD-текст той же страницы;
      * side_block — запись о картинке из result.json (page_width/page_height,
        bbox, area_ratio), если доступна. Большой относительный размер +
        scheme markers → dense_scheme.
    """
    block_text = (getattr(mb, "text", "") or "")
    excerpt = (block_text + "\n" + (surrounding_context or ""))[:2000]

    stamp_hits = _count_marker_hits(excerpt, _STAMP_MARKERS)
    table_hits = _count_marker_hits(excerpt, _TABLE_LEGEND_MARKERS)
    scheme_strong_hits = _count_marker_hits(excerpt, _SCHEME_STRONG_MARKERS)
    scheme_weak_hits = _count_marker_hits(excerpt, _SCHEME_WEAK_MARKERS)
    scheme_total = scheme_strong_hits + scheme_weak_hits
    plan_hits = _count_marker_hits(excerpt, _PLAN_MARKERS)

    # Штамп почти всегда узнаваем сразу: 3+ маркера (стадия/лист/изм/подп/шифр).
    # Если штамп-markers сильно перевешивают остальные — это штамп.
    if stamp_hits >= 3 and stamp_hits > scheme_strong_hits and stamp_hits > table_hits:
        return BLOCK_TYPE_STAMP

    # Таблица/спецификация/ведомость: хотя бы один очевидный маркер и нет
    # явного перевеса в сторону схемы (по strong-маркерам).
    if table_hits >= 1 and table_hits >= scheme_strong_hits:
        return BLOCK_TYPE_TABLE_LEGEND

    # Плоский план без strong-scheme-маркеров: «план этажа», «оси», «трасса».
    # Если есть хоть 2 plan-маркера и нет сильных scheme-маркеров — это план,
    # даже если в окружающем тексте упомянуто слово «кабел» (weak scheme).
    if plan_hits >= 2 and scheme_strong_hits == 0:
        return BLOCK_TYPE_PLAN

    # Схема: нужно хотя бы один strong-маркер, либо очень явный weak-сигнал
    # «однолинейн» + минимум одна доп. подсказка.
    looks_like_scheme = (
        scheme_strong_hits >= 1
        or ("однолинейн" in excerpt.lower() and scheme_total >= 2)
    )
    if looks_like_scheme:
        # Признаки плотной схемы:
        #   - суммарно ≥6 strong-маркеров (например, длинная цепочка ВРУ/ЩР/QF);
        #   - суммарно ≥10 любых scheme-маркеров;
        #   - известный area_ratio ≥ 0.35 относительно страницы.
        dense = False
        if scheme_strong_hits >= 6 or scheme_total >= 10:
            dense = True
        if isinstance(side_block, dict):
            try:
                area_ratio = float(side_block.get("area_ratio") or 0.0)
            except (TypeError, ValueError):
                area_ratio = 0.0
            if area_ratio >= 0.35:
                dense = True
            bbox = side_block.get("bbox") or side_block.get("crop_bbox")
            pw = side_block.get("page_width")
            ph = side_block.get("page_height")
            try:
                if (
                    isinstance(bbox, (list, tuple)) and len(bbox) == 4
                    and isinstance(pw, (int, float)) and isinstance(ph, (int, float))
                    and pw > 0 and ph > 0
                ):
                    bw = float(bbox[2]) - float(bbox[0])
                    bh = float(bbox[3]) - float(bbox[1])
                    if (bw * bh) / float(pw * ph) >= 0.35:
                        dense = True
            except (TypeError, ValueError):
                pass
        return BLOCK_TYPE_DENSE_SCHEME if dense else BLOCK_TYPE_SCHEME

    # План: «план этажа», «оси», «помещение», «трасса». Стараемся не путать
    # с обычной фотографией: нужен хотя бы один маркер.
    if plan_hits >= 1:
        return BLOCK_TYPE_PLAN

    return BLOCK_TYPE_GENERAL


# Per-type render/inference конфигурация. Используется enrich_side(), чтобы
# для схемы поднять long_side и max_tokens, не задирая их глобально.
#
# render_target_long_side — параметр store.render_block_crop (масштаб PNG).
# image_input_long_side   — параметр graphic_llm_local._resize_png_to_long_side
#                           (то, что реально уходит в Qwen).
# max_tokens              — override для cfg.max_tokens в этом вызове.
# max_continuations       — override для cfg.max_continuations в этом вызове.
# prompt_version          — какой prompt семейства использовать.
#
# v5 production tuning (после validation report 2026-05-27):
# Изначально предложенные значения (dense_scheme: 3000/2800/10000/cont=4)
# на реальном Qwen deployment приводили к ~4 минутам на блок и catalog-fill
# галлюцинациям до max_tokens. Понижены до production-safe defaults.
# Smoke-test показал, что image_long_side=1100 уже даёт буквальные anchors —
# больше нет смысла подключать огромные картинки и тратить токены на
# бесконечное достраивание. Override через env переменные
# STAGE_COMPARISON_<TYPE>_<PARAM> (см. ниже).
import os as _os


def _env_int(name: str, default: int) -> int:
    """Прочитать целое из env с fallback на default. None default не допускается."""
    raw = _os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = _os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


BLOCK_TYPE_CONFIG: dict[str, dict[str, Any]] = {
    BLOCK_TYPE_GENERAL: {
        "render_target_long_side": _env_int("STAGE_COMPARISON_GENERAL_RENDER_LONG_SIDE", 1200),
        "image_input_long_side": _env_int("STAGE_COMPARISON_GENERAL_IMAGE_LONG_SIDE", 1100),
        "max_tokens": None,  # использовать cfg.max_tokens (env default)
        "max_continuations": None,
        "prompt_version": PROMPT_VERSION_GENERAL,
    },
    BLOCK_TYPE_SCHEME: {
        "render_target_long_side": _env_int("STAGE_COMPARISON_SCHEME_RENDER_LONG_SIDE", 1800),
        "image_input_long_side": _env_int("STAGE_COMPARISON_SCHEME_IMAGE_LONG_SIDE", 1600),
        "max_tokens": _env_int("STAGE_COMPARISON_SCHEME_MAX_TOKENS", 3500),
        "max_continuations": _env_int("STAGE_COMPARISON_SCHEME_MAX_CONTINUATIONS", 1),
        "prompt_version": PROMPT_VERSION_SCHEME,
    },
    BLOCK_TYPE_DENSE_SCHEME: {
        "render_target_long_side": _env_int("STAGE_COMPARISON_DENSE_SCHEME_RENDER_LONG_SIDE", 2000),
        "image_input_long_side": _env_int("STAGE_COMPARISON_DENSE_SCHEME_IMAGE_LONG_SIDE", 1800),
        "max_tokens": _env_int("STAGE_COMPARISON_DENSE_SCHEME_MAX_TOKENS", 4000),
        "max_continuations": _env_int("STAGE_COMPARISON_DENSE_SCHEME_MAX_CONTINUATIONS", 1),
        "prompt_version": PROMPT_VERSION_SCHEME,
    },
    BLOCK_TYPE_TABLE_LEGEND: {
        "render_target_long_side": _env_int("STAGE_COMPARISON_TABLE_RENDER_LONG_SIDE", 1800),
        "image_input_long_side": _env_int("STAGE_COMPARISON_TABLE_IMAGE_LONG_SIDE", 1600),
        "max_tokens": _env_int("STAGE_COMPARISON_TABLE_MAX_TOKENS", 3500),
        "max_continuations": _env_int("STAGE_COMPARISON_TABLE_MAX_CONTINUATIONS", 1),
        "prompt_version": PROMPT_VERSION_GENERAL,
    },
    BLOCK_TYPE_STAMP: {
        "render_target_long_side": _env_int("STAGE_COMPARISON_STAMP_RENDER_LONG_SIDE", 1600),
        "image_input_long_side": _env_int("STAGE_COMPARISON_STAMP_IMAGE_LONG_SIDE", 1400),
        "max_tokens": _env_int("STAGE_COMPARISON_STAMP_MAX_TOKENS", 2500),
        "max_continuations": _env_int("STAGE_COMPARISON_STAMP_MAX_CONTINUATIONS", 0),
        "prompt_version": PROMPT_VERSION_GENERAL,
    },
    BLOCK_TYPE_PLAN: {
        "render_target_long_side": _env_int("STAGE_COMPARISON_PLAN_RENDER_LONG_SIDE", 1800),
        "image_input_long_side": _env_int("STAGE_COMPARISON_PLAN_IMAGE_LONG_SIDE", 1600),
        "max_tokens": _env_int("STAGE_COMPARISON_PLAN_MAX_TOKENS", 3500),
        "max_continuations": _env_int("STAGE_COMPARISON_PLAN_MAX_CONTINUATIONS", 1),
        "prompt_version": PROMPT_VERSION_GENERAL,
    },
}


def get_block_type_config(block_type: str) -> dict[str, Any]:
    """Безопасный доступ к BLOCK_TYPE_CONFIG: на неизвестный тип возвращает
    конфигурацию photo_or_general (backward-compat default)."""
    return dict(BLOCK_TYPE_CONFIG.get(block_type) or BLOCK_TYPE_CONFIG[BLOCK_TYPE_GENERAL])


def get_prompt_for_block_type(block_type: str) -> tuple[str, str]:
    """Вернуть (prompt_text, prompt_version) для заданного block_type.

    Используется enrich_side() — каждый блок может уйти в разный prompt.
    """
    cfg = get_block_type_config(block_type)
    version = str(cfg.get("prompt_version") or PROMPT_VERSION_GENERAL)
    if version == PROMPT_VERSION_SCHEME:
        return QWEN_SCHEME_DIFF_ANCHORS_PROMPT, PROMPT_VERSION_SCHEME
    return QWEN_IMAGE_DESCRIPTION_PROMPT, PROMPT_VERSION_GENERAL


# ─── Парсер MD ────────────────────────────────────────────────────────────


_HEADING_RE = re.compile(r"^(\s{0,3})(#{1,6})\s+(.*?)\s*$")

# Image markers (см. text_llm_input.py — оставляем совместимыми с одним
# набором правил).
_IMG_TOKENS = (
    r"\bBLOCK\s*\[\s*IMAGE\s*\]",
    r"\[\s*IMAGE\s*\]",
    r"\bIMAGE\b",
    r"\bIMAGEN\b",
    r"\bIMAGINE\b",
    r"Изображени[ея]",
    r"Графический\s+блок",
    r"Графика",
    r"Иллюстрация",
)
_IMG_TOKEN_RE = re.compile(r"(?:" + r"|".join(_IMG_TOKENS) + r")", re.IGNORECASE)

_TEXT_BLOCK_HEADING_RE = re.compile(
    r"^\s{0,3}#{1,6}\s+.*?(?:BLOCK\s*\[\s*TEXT\s*\]|\[\s*TEXT\s*\])",
    re.IGNORECASE,
)

_PAGE_HEADING_RE = re.compile(
    r"^\s{0,3}#{1,6}\s+(?:СТРАНИЦА|Страница|PAGE|Лист)\b",
    re.IGNORECASE,
)

# Извлечь block_id из заголовка вида `### BLOCK [IMAGE]: <id>` или `[IMAGE] id`
_BLOCK_ID_FROM_HEADING_RE = re.compile(
    r"\[\s*(?:IMAGE|IMAGEN|IMAGINE)\s*\]\s*[:\-]?\s*([A-Za-z0-9_\-]+)",
    re.IGNORECASE,
)

# Извлечь номер страницы из заголовка-страницы Chandra
_PAGE_NUM_RE = re.compile(
    r"^\s{0,3}#{1,6}\s+(?:СТРАНИЦА|Страница|PAGE|Лист)\s*[:№#]?\s*(\d+)",
    re.IGNORECASE,
)


def _is_image_heading(stripped: str) -> bool:
    m = _HEADING_RE.match(stripped)
    if not m:
        return False
    if _TEXT_BLOCK_HEADING_RE.match(stripped):
        return False
    if _PAGE_HEADING_RE.match(stripped):
        return False
    return bool(_IMG_TOKEN_RE.search(m.group(3) or ""))


def _heading_level(stripped: str) -> Optional[int]:
    m = _HEADING_RE.match(stripped)
    if not m:
        return None
    return len(m.group(2))


def _extract_block_id(stripped_heading: str) -> Optional[str]:
    m = _BLOCK_ID_FROM_HEADING_RE.search(stripped_heading)
    if not m:
        return None
    raw = m.group(1).strip()
    return raw or None


def _extract_page_from_heading(stripped: str) -> Optional[int]:
    m = _PAGE_NUM_RE.match(stripped)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


@dataclass
class MdBlock:
    """Логический блок MD после парсинга.

    Не привязан к нумерации страниц/листов проектной документации; это
    просто отрезок исходного текста между двумя image/text boundary'ами.
    """

    kind: str               # "text" | "image"
    text: str               # исходный текст блока, как он был в MD (с переводами строк)
    page: Optional[int]     # текущая открытая `### СТРАНИЦА N`/`### Лист N`, если известна
    block_id: Optional[str] = None  # для image-блока — id из заголовка, если он там был
    order: int = 0          # порядковый номер блока этого kind среди блоков того же типа
    image_order_on_page: Optional[int] = None  # для image-блока — индекс среди image-блоков той же страницы

    @property
    def is_image(self) -> bool:
        return self.kind == "image"


def parse_md_blocks(md_text: str) -> list[MdBlock]:
    """Разбить MD на упорядоченный список text/image-блоков.

    Парсер совместим с Chandra MD-форматом (`### BLOCK [IMAGE]: ...`,
    `### СТРАНИЦА N`, `<image>...</image>`). Никаких lossy-преобразований
    с самим текстом не делается — оригинал собирается обратно конкатенацией
    `block.text`.
    """
    if not isinstance(md_text, str):
        md_text = "" if md_text is None else str(md_text)

    blocks: list[MdBlock] = []
    cur_page: Optional[int] = None
    page_image_counter: dict[int, int] = {}
    block_image_counter = 0
    block_text_counter = 0

    # Буфер для накапливания обычного текста
    text_buf: list[str] = []

    def flush_text():
        nonlocal block_text_counter
        if not text_buf:
            return
        joined = "".join(text_buf)
        # Полностью пустой буфер из одних переводов строк — оставляем для
        # точного восстановления, но не плодим лишний text-блок.
        if joined.strip() == "" and not blocks:
            text_buf.clear()
            return
        if joined.strip() == "":
            # Пустота между блоками — приклеим к последнему блоку, чтобы при
            # сборке enriched MD не терялись разделители.
            if blocks:
                blocks[-1].text += joined
            text_buf.clear()
            return
        block_text_counter += 1
        blocks.append(MdBlock(
            kind="text",
            text=joined,
            page=cur_page,
            block_id=None,
            order=block_text_counter,
        ))
        text_buf.clear()

    def append_image(text: str, block_id: Optional[str]):
        nonlocal block_image_counter
        page_image_counter[cur_page or 0] = page_image_counter.get(cur_page or 0, 0) + 1
        block_image_counter += 1
        blocks.append(MdBlock(
            kind="image",
            text=text,
            page=cur_page,
            block_id=block_id,
            order=block_image_counter,
            image_order_on_page=page_image_counter[cur_page or 0],
        ))

    lines = md_text.splitlines(keepends=True)
    i = 0
    in_image_fence = False
    fence_buf: list[str] = []

    in_image_tag = False
    tag_buf: list[str] = []

    in_image_block_by_heading = False
    image_heading_level: Optional[int] = None
    image_heading_buf: list[str] = []
    image_heading_block_id: Optional[str] = None

    while i < len(lines):
        raw_line = lines[i]
        stripped = raw_line.rstrip("\n").rstrip("\r")

        # ── Внутри ```image fence ─────────────────────────────────────
        if in_image_fence:
            fence_buf.append(raw_line)
            m_close = re.match(r"^(\s*)```\s*(\S+)?", stripped)
            if m_close and (m_close.group(2) is None or m_close.group(2) == ""):
                in_image_fence = False
                flush_text()
                append_image("".join(fence_buf), None)
                fence_buf = []
            i += 1
            continue

        # ── Внутри <image>...</image> / [IMAGE]...[/IMAGE] ─────────────
        if in_image_tag:
            tag_buf.append(raw_line)
            if re.match(
                r"^\s*(?:<\s*/\s*(?:image|imagen|imagine)\s*>|\[\s*/\s*(?:IMAGE|IMAGEN|IMAGINE)\s*\])\s*$",
                stripped, flags=re.IGNORECASE,
            ):
                in_image_tag = False
                flush_text()
                append_image("".join(tag_buf), None)
                tag_buf = []
            i += 1
            continue

        # ── Внутри image-блока, открытого заголовком ─────────────────
        if in_image_block_by_heading:
            # Что-то заканчивает блок?
            new_level = _heading_level(stripped)
            if new_level is not None:
                ends = False
                if image_heading_level is not None and new_level <= image_heading_level:
                    ends = True
                if _TEXT_BLOCK_HEADING_RE.match(stripped) or _PAGE_HEADING_RE.match(stripped):
                    ends = True
                if ends:
                    # Закрыть текущий image-блок и продолжить общую обработку строки
                    in_image_block_by_heading = False
                    image_heading_level = None
                    flush_text()
                    append_image("".join(image_heading_buf), image_heading_block_id)
                    image_heading_buf = []
                    image_heading_block_id = None
                    # не делаем i += 1 — fall-through
                else:
                    image_heading_buf.append(raw_line)
                    i += 1
                    continue
            else:
                image_heading_buf.append(raw_line)
                i += 1
                continue

        # ── Открывающий image-fence ─────────────────────────────────
        m_fence = re.match(r"^(\s*)```\s*(\S+)?", stripped)
        if m_fence:
            lang = (m_fence.group(2) or "").strip().lower()
            if lang in ("image", "imagen", "imagine"):
                in_image_fence = True
                fence_buf.append(raw_line)
                i += 1
                continue
            # обычный fence — fall-through (он попадёт в text buffer)

        # ── Открывающий image-tag ────────────────────────────────────
        if re.match(
            r"^\s*(?:<\s*(?:image|imagen|imagine)\b[^>]*>|\[\s*(?:IMAGE|IMAGEN|IMAGINE)\s*\])\s*$",
            stripped, flags=re.IGNORECASE,
        ):
            in_image_tag = True
            tag_buf.append(raw_line)
            i += 1
            continue

        # ── Page heading? ───────────────────────────────────────────
        page_num = _extract_page_from_heading(stripped)
        if page_num is not None:
            cur_page = page_num
            # Не делаем flush — page heading это часть текста, идёт в text-блок
            text_buf.append(raw_line)
            i += 1
            continue

        # ── Image heading? ──────────────────────────────────────────
        if _is_image_heading(stripped):
            in_image_block_by_heading = True
            image_heading_level = _heading_level(stripped)
            image_heading_buf.append(raw_line)
            image_heading_block_id = _extract_block_id(stripped)
            i += 1
            continue

        # ── Standalone markdown image-line (![](...)) ────────────────
        if re.match(r"^\s*(?:!\[[^\]]*\]\([^)]*\)\s*)+[\s.,;:!?]*$", stripped) and stripped.strip().startswith("!"):
            flush_text()
            append_image(raw_line, None)
            i += 1
            continue

        # ── Обычная строка → в text buffer ───────────────────────────
        text_buf.append(raw_line)
        i += 1

    # Хвосты — на случай неполных блоков в конце файла
    if in_image_fence and fence_buf:
        flush_text()
        append_image("".join(fence_buf), None)
    elif in_image_tag and tag_buf:
        flush_text()
        append_image("".join(tag_buf), None)
    elif in_image_block_by_heading and image_heading_buf:
        flush_text()
        append_image("".join(image_heading_buf), image_heading_block_id)

    flush_text()
    return blocks


# ─── Связь image-блока с реальной картинкой ──────────────────────────────


def _normalize_block_id(value: Any) -> str:
    return re.sub(r"[^A-Za-z0-9_\-]", "_", str(value or "").strip())


@dataclass
class ImageResolution:
    """Результат попытки найти реальную картинку для image-блока MD."""

    status: str                  # "ok" | "no_image" | "render_failed"
    image_path: Optional[Path] = None
    side_block_id: Optional[str] = None
    matched_by: Optional[str] = None  # "block_id" | "page_order" | "manual_crop" | ...
    note: str = ""


# ─── Кеш ──────────────────────────────────────────────────────────────────


def compute_image_cache_key(image_bytes: bytes, model: str, prompt_version: str = PROMPT_VERSION) -> str:
    h = hashlib.sha256()
    h.update(image_bytes)
    h.update(b"|")
    h.update((model or "").encode("utf-8", errors="replace"))
    h.update(b"|")
    h.update((prompt_version or "").encode("utf-8", errors="replace"))
    return h.hexdigest()


def read_cache(session_id: str, pair_id: str, key: str) -> Optional[dict]:
    p = paths_mod.text_enrichment_cache_dir(session_id, pair_id) / f"{key}.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def write_cache(session_id: str, pair_id: str, key: str, payload: dict) -> None:
    p = paths_mod.text_enrichment_cache_dir(session_id, pair_id) / f"{key}.json"
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


# ─── Сборка enriched MD ───────────────────────────────────────────────────


def _utc_now() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _format_qwen_description_md(desc_payload: dict, *, model: str, page: Optional[int], block_id: Optional[str]) -> str:
    """Сформировать тело markdown-блока Qwen-описания для enriched MD (без HTML-обёртки).

    Используется новым builder'ом `build_enriched_md`: HTML-обёртка
    `<!-- QWEN_IMAGE_DESCRIPTION_START ... -->` строится наружу, а это тело —
    структурированное описание (заголовок «Графический блок / схема», секции
    «Краткое описание», «Видимый текст», «Оборудование», «Материалы»,
    «Числовые параметры», «Схема», «Неопределённости»).

    `desc_payload` — это либо `{"status": "done", ...}` (parsed JSON-ответ
    модели), либо `{"status": "error", "error": "..."}`.
    """
    lines: list[str] = []
    status = (desc_payload.get("status") or "").strip()
    if status == "error":
        err = (desc_payload.get("error") or "unknown").strip()
        lines.append("### Графический блок не распознан")
        lines.append("")
        lines.append("Описание графического блока отсутствует из-за ошибки распознавания.")
        lines.append(f"Этот блок требует повторного Qwen-enrichment / ручной проверки.")
        lines.append(f"Причина: {err}")
        lines.append("")
        return "\n".join(lines)

    lines.append("### Графический блок / схема")
    lines.append("")
    if model:
        lines.append(f"Модель: {model}")
    if page is not None:
        lines.append(f"Страница: {page}")
    if block_id:
        lines.append(f"Block ID: {block_id}")
    if desc_payload.get("_salvaged"):
        lines.append("Salvaged: yes (partial JSON, восстановлен с пропусками — модель оборвалась многоточием)")
    chunks_count = desc_payload.get("chunks_count")
    continued = desc_payload.get("continued")
    if isinstance(chunks_count, int) and chunks_count > 1:
        lines.append(f"Chunks: {chunks_count}")
    if continued is True:
        lines.append("Continued: yes (Qwen вернул несколько chunk'ов с continuation_prompt)")
    elif isinstance(chunks_count, int):
        lines.append("Continued: no")
    cont_warnings = desc_payload.get("continuation_warnings")
    if isinstance(cont_warnings, list) and cont_warnings:
        # Любая запись `*_cap_reached*` означает: модель ещё хотела продолжать,
        # но мы упёрлись в лимит → явное предупреждение.
        cap_hit = any("cap_reached" in str(w) for w in cont_warnings)
        if cap_hit:
            lines.append("⚠ Continuation cap reached — описание может быть неполным, увеличьте STAGE_COMPARISON_GRAPHIC_LLM_MAX_CONTINUATIONS")
        for w in cont_warnings:
            lines.append(f"  · continuation warning: {w}")
    continues_flag = desc_payload.get("continues")
    if continues_flag is True:
        nxt = (desc_payload.get("next_chunk_hint") or "").strip()
        lines.append(f"Продолжение требуется: yes — {nxt}" if nxt else "Продолжение требуется: yes")
    cov = (desc_payload.get("coverage_notes") or "").strip()
    if cov:
        lines.append(f"Покрытие: {cov}")
    lines.append("")

    # ── DIFF_ANCHORS: буквальные маркировки/номиналы/связи для diff'а ──
    # Эта секция идёт ДО summary, чтобы Opus видел сырые ЩР-1а / ВРУ-2 с.ш.1
    # / QF3 / 4х185 раньше, чем плавный текст. Это критично для схем —
    # текст summary часто нормализован, anchors — нет.
    diff_anchors = desc_payload.get("diff_anchors")
    if isinstance(diff_anchors, dict):
        labels = diff_anchors.get("labels")
        ratings = diff_anchors.get("ratings")
        connections = diff_anchors.get("connections")
        uncertain = diff_anchors.get("uncertain_text")

        if isinstance(labels, list) and labels:
            lines.append("DIFF_ANCHORS — буквальные маркировки:")
            for lab in labels:
                if not isinstance(lab, dict):
                    continue
                raw = (lab.get("raw_text") or "").strip()
                if not raw:
                    continue
                ntype = (lab.get("normalized_type") or "").strip()
                comment = (lab.get("comment") or "").strip()
                lconf = lab.get("confidence")
                parts = [f"- {raw}"]
                if ntype:
                    parts.append(f" [{ntype}]")
                if isinstance(lconf, (int, float)):
                    try:
                        parts.append(f" (уверенность: {float(lconf):.2f})")
                    except (TypeError, ValueError):
                        pass
                if comment:
                    parts.append(f" — {comment}")
                lines.append("".join(parts))
            lines.append("")

        if isinstance(ratings, list) and ratings:
            lines.append("DIFF_ANCHORS — кабели, номиналы, мощности:")
            for r in ratings:
                if not isinstance(r, dict):
                    continue
                raw = (r.get("raw_text") or "").strip()
                if not raw:
                    continue
                vtype = (r.get("value_type") or "").strip()
                related = (r.get("related_to") or "").strip()
                rconf = r.get("confidence")
                parts = [f"- {raw}"]
                if vtype:
                    parts.append(f" [{vtype}]")
                if related:
                    parts.append(f" → {related}")
                if isinstance(rconf, (int, float)):
                    try:
                        parts.append(f" (уверенность: {float(rconf):.2f})")
                    except (TypeError, ValueError):
                        pass
                lines.append("".join(parts))
            lines.append("")

        if isinstance(connections, list) and connections:
            lines.append("DIFF_ANCHORS — связи:")
            for c in connections:
                if not isinstance(c, dict):
                    continue
                f_raw = (c.get("from_raw") or "?").strip()
                t_raw = (c.get("to_raw") or "?").strip()
                relation = (c.get("relation") or "").strip()
                cconf = c.get("confidence")
                parts = [f"- {f_raw} → {t_raw}"]
                if relation:
                    parts.append(f" ({relation})")
                if isinstance(cconf, (int, float)):
                    try:
                        parts.append(f" [уверенность: {float(cconf):.2f}]")
                    except (TypeError, ValueError):
                        pass
                lines.append("".join(parts))
            lines.append("")

        if isinstance(uncertain, list) and uncertain:
            lines.append("Неуверенно прочитанные надписи:")
            for u in uncertain:
                if not isinstance(u, dict):
                    continue
                possible = (u.get("possible_text") or "").strip()
                if not possible:
                    continue
                alts = u.get("alternatives") or []
                alts_clean = [str(a).strip() for a in alts if str(a).strip()]
                why = (u.get("why_uncertain") or "").strip()
                uconf = u.get("confidence")
                parts = [f"- {possible}"]
                if alts_clean:
                    parts.append(f" (варианты: {', '.join(alts_clean)})")
                if isinstance(uconf, (int, float)):
                    try:
                        parts.append(f" [уверенность: {float(uconf):.2f}]")
                    except (TypeError, ValueError):
                        pass
                if why:
                    parts.append(f" — {why}")
                lines.append("".join(parts))
            lines.append("")

    summary = (desc_payload.get("summary") or "").strip()
    if summary:
        lines.append("Краткое описание:")
        lines.append(summary)
        lines.append("")

    def _bullets(label: str, items: Any):
        if not isinstance(items, list) or not items:
            return
        lines.append(label)
        for it in items:
            if isinstance(it, str) and it.strip():
                lines.append(f"- {it.strip()}")
        lines.append("")

    _bullets("Видимый текст:", desc_payload.get("visible_text"))
    _bullets("Оборудование и элементы:", desc_payload.get("equipment"))
    _bullets("Материалы:", desc_payload.get("materials"))
    _bullets("Проектные решения:", desc_payload.get("design_solutions"))

    nums = desc_payload.get("numeric_parameters")
    if isinstance(nums, list) and nums:
        lines.append("Числовые параметры:")
        for n in nums:
            if not isinstance(n, dict):
                continue
            name = (n.get("name") or "").strip()
            value = (n.get("value") or "").strip()
            unit = (n.get("unit") or "").strip()
            context = (n.get("context") or "").strip()
            entry = f"- {name}: {value}".rstrip(": ").rstrip()
            if unit:
                entry += f" {unit}"
            if context:
                entry += f"  ({context})"
            if entry.strip() == "-":
                continue
            lines.append(entry)
        lines.append("")

    _bullets("Требования / примечания:", desc_payload.get("requirements"))
    _bullets("Таблицы:", desc_payload.get("tables"))
    _bullets("Существенно для сравнения стадий:", desc_payload.get("comparison_relevant_facts"))

    image_kind = (desc_payload.get("image_kind") or "").strip()
    if image_kind:
        lines.append(f"Тип изображения: {image_kind}")
    conf = desc_payload.get("confidence")
    if isinstance(conf, (int, float)):
        try:
            lines.append(f"Уверенность модели: {float(conf):.2f}")
        except (TypeError, ValueError):
            pass

    scheme = desc_payload.get("scheme_analysis")
    if isinstance(scheme, dict):
        is_scheme = bool(scheme.get("is_scheme"))
        if not is_scheme:
            # Кратко отметим, чтобы было видно в enriched MD, что модель проверила.
            lines.append("")
            lines.append("Схемный анализ: не применимо (изображение не является схемой)")
        else:
            lines.append("")
            lines.append("Схемный анализ:")
            scheme_type = (scheme.get("scheme_type") or "").strip()
            if scheme_type:
                lines.append(f"- Тип схемы: {scheme_type}")
            flow_medium = (scheme.get("flow_medium") or "").strip()
            if flow_medium:
                lines.append(f"- Среда / поток: {flow_medium}")
            lines.append("")

            nodes = scheme.get("nodes")
            if isinstance(nodes, list) and nodes:
                lines.append("Узлы:")
                for n in nodes:
                    if not isinstance(n, dict):
                        continue
                    nid = (n.get("id") or "").strip() or "?"
                    label = (n.get("label") or "").strip()
                    ntype = (n.get("type") or "").strip()
                    mark = (n.get("visible_mark") or "").strip()
                    parts = [f"- {nid}"]
                    if label:
                        parts.append(f": {label}")
                    if ntype:
                        parts.append(f", тип: {ntype}")
                    if mark:
                        parts.append(f", маркировка: {mark}")
                    params = n.get("parameters")
                    if isinstance(params, list) and params:
                        params_clean = [str(p).strip() for p in params if str(p).strip()]
                        if params_clean:
                            parts.append(f", параметры: {', '.join(params_clean)}")
                    nconf = n.get("confidence")
                    if isinstance(nconf, (int, float)):
                        try:
                            parts.append(f", уверенность: {float(nconf):.2f}")
                        except (TypeError, ValueError):
                            pass
                    lines.append("".join(parts))
                lines.append("")

            conns = scheme.get("connections")
            if isinstance(conns, list) and conns:
                lines.append("Связи:")
                for c in conns:
                    if not isinstance(c, dict):
                        continue
                    src = (c.get("from") or "?").strip()
                    dst = (c.get("to") or "?").strip()
                    direction = (c.get("direction") or "unknown").strip()
                    line_label = (c.get("line_label") or "").strip()
                    evidence = (c.get("evidence") or "").strip()
                    parts = [f"- {src} → {dst}", f", направление: {direction}"]
                    if line_label:
                        parts.append(f", линия: {line_label}")
                    params = c.get("parameters")
                    if isinstance(params, list) and params:
                        params_clean = [str(p).strip() for p in params if str(p).strip()]
                        if params_clean:
                            parts.append(f", параметры: {', '.join(params_clean)}")
                    if evidence:
                        parts.append(f", основание: {evidence}")
                    cconf = c.get("confidence")
                    if isinstance(cconf, (int, float)):
                        try:
                            parts.append(f", уверенность: {float(cconf):.2f}")
                        except (TypeError, ValueError):
                            pass
                    lines.append("".join(parts))
                lines.append("")

            sequence = scheme.get("sequence_summary")
            if isinstance(sequence, list) and sequence:
                lines.append("Последовательность:")
                for s in sequence:
                    if isinstance(s, str) and s.strip():
                        lines.append(f"- {s.strip()}")
                lines.append("")

            circuits = scheme.get("independent_circuits")
            if isinstance(circuits, list) and circuits:
                lines.append("Независимые контуры:")
                for circ in circuits:
                    if not isinstance(circ, dict):
                        continue
                    name = (circ.get("name") or "").strip()
                    seq = (circ.get("sequence") or "").strip()
                    notes = (circ.get("notes") or "").strip()
                    entry = "- "
                    if name:
                        entry += f"{name}: "
                    if seq:
                        entry += seq
                    if notes:
                        entry += f"  ({notes})"
                    if entry.strip() != "-":
                        lines.append(entry)
                lines.append("")

            _bullets("Существенно для сравнения (схема):", scheme.get("comparison_relevant_scheme_facts"))
            _bullets("Неопределённости (схема):", scheme.get("uncertainties"))

    # Top-level Неопределённости — в самом конце, после scheme_analysis,
    # по новому формату replace_image_blocks_v1.
    _bullets("Неопределённости:", desc_payload.get("uncertainties"))

    return "\n".join(lines).rstrip() + "\n"


def _format_image_block_header(
    *,
    status: str,
    source_kind: str,
    block_id: Optional[str],
    page: Optional[int],
    desc_item: Optional[dict] = None,
    error: Optional[str] = None,
) -> str:
    """Сформировать HTML-комментарий <!-- QWEN_IMAGE_DESCRIPTION_START ... -->.

    Используется как обёртка вокруг тела Qwen-описания в новом формате
    `replace_image_blocks_v1`. В метаданных сохраняется:
      - format_version (как маркер для preflight);
      - block_id / page (из исходного MD-блока);
      - source (image / imagine);
      - status (done / done_with_salvage / error / pending / no_image);
      - prompt_version / model / confidence — если описание есть;
      - original_block_id (для debug).
    """
    lines = ["<!-- QWEN_IMAGE_DESCRIPTION_START"]
    lines.append(f"format_version: {ENRICHED_MD_FORMAT_VERSION}")
    if block_id:
        lines.append(f"block_id: {block_id}")
    if page is not None:
        lines.append(f"page: {page}")
    lines.append(f"source: {source_kind or 'image'}")
    lines.append(f"status: {status}")
    if desc_item:
        prompt_version = desc_item.get("used_prompt_version") or desc_item.get("prompt_version")
        if prompt_version:
            lines.append(f"prompt_version: {prompt_version}")
        model = (desc_item.get("model_used") or desc_item.get("model") or "").strip()
        if model:
            lines.append(f"model: {model}")
        # Per-block metadata: тип, рендер/inference sizing, usable_for_diff и
        # warnings — это нужно Opus'у, чтобы судить, насколько верить блоку.
        block_type = (desc_item.get("block_type") or "").strip()
        if block_type:
            lines.append(f"block_type: {block_type}")
        for fk in ("prompt_family",):
            fv = (desc_item.get(fk) or "").strip()
            if fv:
                lines.append(f"{fk}: {fv}")
        for nk in ("render_target_long_side", "image_input_long_side"):
            nv = desc_item.get(nk)
            if isinstance(nv, (int, float)) and int(nv) > 0:
                lines.append(f"{nk}: {int(nv)}")
        if "usable_for_diff" in desc_item:
            lines.append(f"usable_for_diff: {'true' if desc_item.get('usable_for_diff') else 'false'}")
        warnings_list = desc_item.get("warnings")
        if isinstance(warnings_list, list) and warnings_list:
            safe_w = [str(w).replace("\n", " ").replace("--", "—") for w in warnings_list if w]
            if safe_w:
                # Чтобы HTML-комментарий не сломался от слишком длинных значений
                lines.append("warnings: " + ", ".join(safe_w)[:600])
        payload = desc_item.get("description")
        if isinstance(payload, dict):
            conf = payload.get("confidence")
            if isinstance(conf, (int, float)):
                try:
                    lines.append(f"confidence: {float(conf):.2f}")
                except (TypeError, ValueError):
                    pass
        if block_id:
            lines.append(f"original_block_id: {block_id}")
    if error:
        # экранируем переводы строк, чтобы HTML-комментарий не сломался
        safe_err = str(error).replace("\n", " ").replace("--", "—")
        lines.append(f"error: {safe_err[:200]}")
    lines.append("-->")
    return "\n".join(lines) + "\n"


def build_enriched_md(blocks: list[MdBlock], descriptions: list[dict]) -> str:
    """Собрать enriched MD из блоков + сопоставленных описаний.

    Формат `replace_image_blocks_v1`:
      - text-блоки переносятся как есть, под заголовком `### BLOCK [TEXT]`;
      - image/imagine-блоки ПОЛНОСТЬЮ ЗАМЕНЯЮТСЯ структурированным
        Qwen-описанием в обёртке
        `<!-- QWEN_IMAGE_DESCRIPTION_START … --> … <!-- QWEN_IMAGE_DESCRIPTION_END -->`.
        Старое OCR из исходного MD физически отсутствует в основном
        enriched.md (debug сохранён в image_descriptions.json: original_block_id,
        original_page, original_kind, original_order).

    `descriptions` — список dict'ов по индексу, соответствующему `block.order`
    для image-блоков. Каждый элемент: один блок image, со всеми полями.
    """
    desc_by_image_order: dict[int, dict] = {}
    for d in descriptions:
        order = d.get("order")
        if isinstance(order, int):
            desc_by_image_order[order] = d

    out_parts: list[str] = []
    # Документ-уровневый header — упрощает детекцию формата в preflight.
    out_parts.append(f"<!-- ENRICHED_MD_FORMAT: {ENRICHED_MD_FORMAT_VERSION} -->\n\n")

    # IMAGE_DIFF_INDEX: компактный список буквальных diff-якорей. Ставим
    # сразу после format-header'а, чтобы Opus при сравнении стадий видел
    # raw маркировки/номиналы РАНЬШЕ длинных markdown-блоков.
    try:
        diff_index = build_image_diff_index(descriptions)
    except Exception:  # noqa: BLE001
        logger.debug("build_enriched_md: build_image_diff_index failed", exc_info=True)
        diff_index = (
            _IMAGE_DIFF_INDEX_START
            + "\nimage_diff_index_parse_failed: yes\n"
            + _IMAGE_DIFF_INDEX_END
            + "\n"
        )
    out_parts.append(diff_index)
    out_parts.append("\n")

    for block in blocks:
        if block.kind == "text":
            out_parts.append("### BLOCK [TEXT]\n")
            out_parts.append(block.text)
            if not block.text.endswith("\n"):
                out_parts.append("\n")
            out_parts.append("\n")
            continue

        # ── image-блок: REPLACE (не append) ─────────────────────────
        d = desc_by_image_order.get(block.order)

        def _wrap(body: str, *, status: str, error: Optional[str] = None) -> None:
            header = _format_image_block_header(
                status=status,
                source_kind="image",
                block_id=block.block_id,
                page=block.page,
                desc_item=d,
                error=error,
            )
            out_parts.append(header)
            out_parts.append("\n")
            out_parts.append(body)
            if not body.endswith("\n"):
                out_parts.append("\n")
            out_parts.append("\n<!-- QWEN_IMAGE_DESCRIPTION_END -->\n\n")

        if d is None:
            body = (
                "### Графический блок не распознан\n\n"
                "Описание ещё не сформировано (dry-run или модель не запущена).\n"
            )
            _wrap(body, status="pending")
            continue

        item_status = (d.get("status") or "").lower()

        # Large Sheet Enrichment: вставляем готовую компактную сводку вместо
        # обычного Qwen-описания (тело уже сформировано в large_sheet_md).
        if d.get("source") == "large_sheet_enrichment":
            body = d.get("large_sheet_md") or "### Большой лист\n\n(сводка отсутствует)\n"
            _wrap(body, status=("done" if item_status == "done" else item_status or "pending"))
            continue

        if item_status in ("pending", "no_image"):
            note = (
                d.get("error")
                or (
                    "Описание ещё не сформировано (dry-run)."
                    if item_status == "pending"
                    else "Для блока не найдено изображения."
                )
            )
            body = (
                "### Графический блок не распознан\n\n"
                f"{note}\n"
            )
            _wrap(body, status=item_status, error=str(note))
            continue

        if item_status == "error":
            err_msg = str(d.get("error") or "unknown")
            body = (
                "### Графический блок не распознан\n\n"
                "Описание графического блока отсутствует из-за ошибки распознавания.\n"
                "Этот блок требует повторного Qwen-enrichment / ручной проверки.\n"
                f"Причина: {err_msg}\n"
            )
            _wrap(body, status="error", error=err_msg)
            continue

        payload = d.get("description") or {"status": "error", "error": d.get("error") or "unknown"}
        model = (d.get("model_used") or d.get("model") or "").strip()
        body = _format_qwen_description_md(
            payload,
            model=model,
            page=block.page,
            block_id=block.block_id,
        )
        # «status» отражает реальный per-item статус (done / partial → done_with_salvage).
        wrap_status = "done_with_salvage" if (item_status == "partial" or d.get("salvaged")) else (
            item_status or "done"
        )
        _wrap(body, status=wrap_status)

    return "".join(out_parts)


def detect_enriched_md_format(text: str | bytes | None) -> str:
    """Определить формат enriched MD: `replace_image_blocks_v1` / `append_v0` / `unknown`.

    Используется preflight'ом для решения «можно ли запускать Opus» и
    «нужна ли пересборка enriched.md без повторного Qwen».
    """
    if not text:
        return "unknown"
    sample = text if isinstance(text, str) else text.decode("utf-8", errors="replace")
    sample = sample[:4096]  # достаточно для header'а
    if _REPLACE_ENRICHED_MARKER in sample or ENRICHED_MD_FORMAT_VERSION in sample:
        return ENRICHED_MD_FORMAT_VERSION
    if _LEGACY_ENRICHED_MARKER in sample:
        return "append_v0"
    # Edge case: совсем пустой файл / без image-блоков. Считаем legacy, чтобы
    # rebuild пересобрал в новом формате (это безопасно — image-блоков нет).
    return "append_v0"


# ─── Quality-эвристики (hallucination / usable_for_diff) ─────────────────


# Маркеры искусственных рядов: модели иногда выдумывают «ВРП-1 ... ВРП-50»
# или «ЩА-1.1 ... ЩА-1.40» (subindex format).
# Считаем подозрительным ряд из ≥6 идущих по возрастанию маркировок одной
# из этих серий, если нет локальных доказательств (другой evidence в
# original_md_excerpt / scheme).
#
# Префиксы для top-level номеров: ЩР-1, ЩР-2, ВРП-N, QF-N etc.
_SEQ_PREFIXES = ("ВРП", "ВРУ", "ЩР", "ЩО", "ЩАО", "ГРЩ", "ЩС", "ЩА", "ЩАВР", "QF", "QS", "KM", "F")

# Каталожно-генерики сечения и номиналы. Если ratings полностью совпадают
# с этим списком (или очень близко) и при этом labels пустые/generic, это
# почти наверняка не наблюдение, а каталог.
_GENERIC_CABLE_SECTIONS = {
    "4x6", "4x10", "4x16", "4x25", "4x35", "4x50", "4x70", "4x95",
    "4x120", "4x150", "4x185", "4x240",
    "5x6", "5x10", "5x16", "5x25", "5x35", "5x50", "5x70", "5x95",
    "5x120", "5x150", "5x185", "5x240",
}
_GENERIC_CURRENT_RATINGS = {
    "16А", "25А", "32А", "40А", "63А", "80А", "100А", "125А",
    "160А", "200А", "250А", "315А", "400А", "500А", "630А",
    "800А", "1000А", "1250А", "1600А",
}

_RU_X_RE = re.compile(r"\s*[xх×Х]\s*")


def _normalize_anchor_value(text: str) -> str:
    if not isinstance(text, str):
        return ""
    s = text.strip().replace(" ", "")
    s = _RU_X_RE.sub("x", s)
    return s


# Универсальный парсер маркировки серии: ЩР-1, ЩА-1.5, ЩР-2.10, ЩО-1-12, QF12, QF-3.7.
# Возвращает (series_key, seq_num) или None.
#
#   - "ЩР-1"      → ("ЩР", 1)            # top-level номер
#   - "ЩР-2.10"   → ("ЩР-2", 10)         # subindex
#   - "ЩА-1.5"    → ("ЩА-1", 5)
#   - "ЩО-1-12"   → ("ЩО-1", 12)
#   - "QF-3.7"    → ("QF-3", 7)
#   - "QF12"      → ("QF", 12)
#   - "ВРУ-2"     → ("ВРУ", 2)
#   - "ВРУ-2 с.ш.1" → ("ВРУ", 2)         # игнорируем суффиксы вроде "с.ш.1"
#
# Без literal-text fallback: если raw не совпадает с pref → не парсится.
_ANCHOR_SERIES_PATTERNS = [
    re.compile(
        rf"^\s*(?P<prefix>{pref})\s*[-]?\s*(?P<major>\d+)(?:\s*[.\-]\s*(?P<minor>\d+))?\b",
        flags=re.IGNORECASE,
    )
    for pref in _SEQ_PREFIXES
]


def parse_anchor_series_key(raw: str) -> Optional[tuple[str, int]]:
    """Разобрать raw_text label на (series_key, seq_num).

    Поддерживает:
    - top-level: ЩР-N → ("ЩР", N)
    - subindex: ЩА-1.N → ("ЩА-1", N)
    - dash subindex: ЩО-1-N → ("ЩО-1", N)
    - bare digits: QFN → ("QF", N)

    Возвращает None, если raw не похож на маркировку серии.
    """
    if not isinstance(raw, str) or not raw.strip():
        return None
    for pat in _ANCHOR_SERIES_PATTERNS:
        m = pat.match(raw)
        if not m:
            continue
        prefix = m.group("prefix")
        try:
            major = int(m.group("major"))
        except (TypeError, ValueError):
            continue
        minor_grp = m.group("minor")
        if minor_grp:
            try:
                minor = int(minor_grp)
            except (TypeError, ValueError):
                continue
            return (f"{prefix.upper()}-{major}", minor)
        return (prefix.upper(), major)
    return None


def _detect_artificial_sequences(labels: list[Any]) -> list[str]:
    """Найти подозрительные ряды вида ЩР-1, ЩР-2, ЩР-3, ..., ≥6 подряд.

    Поддерживает subindex format: ЩА-1.1 ... ЩА-1.8 распознаётся через
    series_key="ЩА-1" и seq_num=1..8. Раньше regex видел только major
    номер и считал, что у всех 40 ЩА-1.X номер=1 (один уникальный → нет ряда).
    Теперь возвращается список series_keys, для которых есть либо ≥6
    подряд номеров, либо ≥10 уникальных номеров (плотный ряд).
    """
    if not isinstance(labels, list) or len(labels) < 6:
        return []
    series: dict[str, list[int]] = {}
    for lab in labels:
        if isinstance(lab, dict):
            raw = (lab.get("raw_text") or "").strip()
        else:
            raw = str(lab or "").strip()
        parsed = parse_anchor_series_key(raw)
        if parsed is None:
            continue
        series_key, seq_num = parsed
        series.setdefault(series_key, []).append(seq_num)
    suspicious: list[str] = []
    for key, nums in series.items():
        if len(nums) < 6:
            continue
        nums_sorted = sorted(set(nums))
        # Длиннейший идущий подряд run
        longest = 1
        cur = 1
        for a, b in zip(nums_sorted, nums_sorted[1:]):
            if b - a == 1:
                cur += 1
                longest = max(longest, cur)
            else:
                cur = 1
        # Триггер: ≥6 подряд ИЛИ ≥10 уникальных номеров в одной серии.
        if longest >= 6 or len(nums_sorted) >= 10:
            suspicious.append(key)
    return suspicious


def _detect_serial_chain_connections(connections: list[Any]) -> list[str]:
    """Найти подозрительные «цепочки» вида ЩА-1.1 → ЩА-1.2 → ЩА-1.3 → ...

    В электрических схемах однотипные потребители (квартирные щитки,
    апартаментные щиты) питаются ПАРАЛЛЕЛЬНО от одного ввода («звезда»),
    а не последовательно. Цепочка `A.N → A.N+1` практически всегда —
    галлюцинация модели, которая «соединила» элементы серии в порядке нумерации,
    хотя в реальности они независимы.

    Возвращает список series_key, в которых найдена цепочка ≥5 шагов.
    Не флагает:
    - звезду: ВРУ-2 → ЩА-1.1, ВРУ-2 → ЩА-1.2 (разные series_key источников/получателей);
    - короткие цепочки (<5);
    - связи с не-series labels (Ввод → ЩР-1 — допустимо).

    Учитывает relation: если в JSON прямо написано relation="питает"/"feeds"/
    "connected_to"/"подключён", это сильный сигнал, что модель утверждает
    топологию, а не описывает соседство.
    """
    if not isinstance(connections, list) or len(connections) < 5:
        return []
    # Сгруппируем шаги per-series_key.
    steps_by_series: dict[str, list[tuple[int, int, str]]] = {}
    chain_relations = ("питает", "feeds", "connected_to", "подключ", "подключён", "подключен", "соедин", "→")
    for c in connections:
        if not isinstance(c, dict):
            continue
        f = str(c.get("from_raw") or "").strip()
        t = str(c.get("to_raw") or "").strip()
        rel = str(c.get("relation") or "").strip().lower()
        f_parsed = parse_anchor_series_key(f)
        t_parsed = parse_anchor_series_key(t)
        if not f_parsed or not t_parsed:
            continue
        f_key, f_num = f_parsed
        t_key, t_num = t_parsed
        # Серия должна совпадать (от ЩА-1.X к ЩА-1.X+1).
        if f_key != t_key:
            continue
        # Шаг по номеру = 1 (последовательная цепочка).
        if t_num - f_num != 1:
            continue
        # Relation может быть пустым (модель не указала). Если указан — он
        # должен быть «топологическим» (питает/feeds/connected). Иначе скорее
        # это просто описательная связь, не топология.
        if rel and not any(k in rel for k in chain_relations):
            continue
        steps_by_series.setdefault(f_key, []).append((f_num, t_num, rel))
    chains: list[str] = []
    for key, steps in steps_by_series.items():
        if len(steps) < 5:
            continue
        # Проверим, что эти шаги формируют сплошную цепочку (1→2, 2→3, 3→4, …).
        # Если 5+ шагов с возрастающим начальным номером без пропусков —
        # это цепочка.
        starts = sorted(s[0] for s in steps)
        longest = 1
        cur = 1
        for a, b in zip(starts, starts[1:]):
            if b - a == 1:
                cur += 1
                longest = max(longest, cur)
            else:
                cur = 1
        if longest >= 5:
            chains.append(key)
    return chains


def _comments_mostly_identical(labels: list[Any], threshold_ratio: float = 0.6) -> bool:
    """True, если ≥threshold_ratio labels имеют один и тот же comment.

    Сильный признак catalog-fill: модель проставила одинаковую отметку
    «читается в левой части схемы» на 40 элементов подряд.
    """
    if not isinstance(labels, list) or len(labels) < 10:
        return False
    comment_counts: dict[str, int] = {}
    total_with_comment = 0
    for lab in labels:
        if not isinstance(lab, dict):
            continue
        cmt = (lab.get("comment") or "").strip().lower()
        if not cmt:
            continue
        total_with_comment += 1
        comment_counts[cmt] = comment_counts.get(cmt, 0) + 1
    if total_with_comment < 10:
        return False
    top_count = max(comment_counts.values(), default=0)
    return top_count / float(total_with_comment) >= threshold_ratio


def _is_low_label_recall(labels: list[Any], block_type: str) -> bool:
    """True, если у схемы нет ни одной полезной буквальной маркировки.

    «Полезная» — это не пустота, не «[маркировка не читается]» и не
    обобщение вида «Щит 1» без раскрытия.
    """
    if block_type not in (BLOCK_TYPE_SCHEME, BLOCK_TYPE_DENSE_SCHEME):
        return False
    if not isinstance(labels, list) or not labels:
        return True
    useful = 0
    for lab in labels:
        if isinstance(lab, dict):
            raw = (lab.get("raw_text") or "").strip()
        else:
            raw = str(lab or "").strip()
        if not raw:
            continue
        if raw.startswith("[") and raw.endswith("]"):
            continue
        # Совсем общие наклейки «Щит 1» / «панель 1» — это generic.
        if re.match(r"^(щит|панель|шкаф|устройство)\s*\d+$", raw, flags=re.IGNORECASE):
            continue
        useful += 1
    return useful == 0


def _detect_generic_rating_list(ratings: list[Any]) -> bool:
    """True, если ratings выглядят как каталог, а не как наблюдение."""
    if not isinstance(ratings, list) or len(ratings) < 5:
        return False
    norm_set: set[str] = set()
    for r in ratings:
        if isinstance(r, dict):
            raw = (r.get("raw_text") or "").strip()
        else:
            raw = str(r or "").strip()
        if not raw:
            continue
        norm_set.add(_normalize_anchor_value(raw))
    if not norm_set:
        return False
    catalog = _GENERIC_CABLE_SECTIONS | _GENERIC_CURRENT_RATINGS
    matches = sum(1 for v in norm_set if v in catalog)
    return matches >= 5 and matches / max(1, len(norm_set)) >= 0.7


_ORG_KEYWORDS = (
    "ООО ",
    "АО ",
    "ОАО ",
    "ЗАО ",
    "ПАО ",
    "Общество с ограниченной",
    "г. ",
    "ул. ",
    "проспект",
    "пр-кт",
    "проезд",
    "д. ",
)


def _detect_unexpected_org_or_address(desc_payload: dict, item_context: dict) -> bool:
    """Best-effort: если в anchors/visible_text есть org/address-like строки,
    а в окружающем MD таких маркеров нет — добавляем warning.
    Не претендует на 100% точность.
    """
    surrounding = (
        item_context.get("original_md_excerpt") or ""
        + " "
        + (item_context.get("surrounding_context") or "")
    )
    surrounding_low = surrounding.lower()

    def _has_org(text: str) -> bool:
        low = text.lower()
        return any(k.lower() in low for k in _ORG_KEYWORDS)

    def _collect_text_pool(payload: dict) -> list[str]:
        pool: list[str] = []
        for k in ("summary", "visible_text", "comparison_relevant_facts"):
            v = payload.get(k)
            if isinstance(v, list):
                pool.extend(str(x) for x in v if isinstance(x, (str, int, float)))
            elif isinstance(v, str):
                pool.append(v)
        diff = payload.get("diff_anchors") or {}
        if isinstance(diff, dict):
            for k in ("labels",):
                arr = diff.get(k) or []
                for x in arr:
                    if isinstance(x, dict):
                        pool.append(str(x.get("raw_text") or ""))
                    else:
                        pool.append(str(x))
        return [p for p in pool if isinstance(p, str)]

    if _has_org(surrounding_low):
        # В соседнем MD есть org/address — не считаем подозрительным.
        return False
    for s in _collect_text_pool(desc_payload):
        if _has_org(s):
            return True
    return False


def analyze_qwen_description_quality(
    desc_payload: Optional[dict],
    item_context: dict,
) -> dict:
    """Эвристически оценить, насколько результат Qwen-описания пригоден для
    участия в diff-сравнении стадий.

    Возвращает:
      * ``usable_for_diff`` — bool. True = блок можно использовать как
        evidence; False = в одиночку не должен порождать change.
      * ``warnings`` — list[str] коротких маркеров, что не так.
      * ``adjusted_confidence`` — Optional[float], если confidence уместно
        снизить (например, при hallucination).

    Эвристики:
      1. Искусственный ряд ЩР-1 ... ЩР-50 или ЩА-1.1 ... ЩА-1.40 →
         repeated_pattern_detected. Эскалируется до hallucination_suspected
         только если есть дополнительные сигналы (см. ниже).
      2. Серийная цепочка связей ЩА-1.1 → ЩА-1.2 → ЩА-1.3 → ... →
         serial_chain_connection_detected. ≥5 шагов → hallucination_suspected.
      3. Одинаковые comment у десятков labels → identical_comments_detected.
         В сочетании с repeated_pattern → hallucination_suspected.
      4. Generic catalog ratings без labels → generic_rating_list_without_labels.
      5. Не привязанные к контексту org/address-like строки →
         unexpected_org_or_address_text.
      6. continuation issues (continuation_warnings содержит «cap_reached»,
         «hint_repeated», salvage и т.п.) → continuation_salvaged /
         continuation_repeated / truncated_output. Также эскалирует repeated
         → hallucination_suspected (truncated + ряд = почти гарантированный
         catalog-fill).
      7. block_type ∈ {scheme, dense_scheme} и labels пусты/generic →
         low_literal_label_recall.

    Все warnings собираются. usable_for_diff становится False, если есть
    serious warnings.
    """
    warnings: list[str] = []
    adjusted_confidence: Optional[float] = None
    usable = True

    payload = desc_payload if isinstance(desc_payload, dict) else {}
    block_type = (item_context.get("block_type") or "").strip()

    diff_anchors = payload.get("diff_anchors")
    labels: list[Any] = []
    ratings: list[Any] = []
    connections: list[Any] = []
    if isinstance(diff_anchors, dict):
        labels = diff_anchors.get("labels") or []
        ratings = diff_anchors.get("ratings") or []
        connections = diff_anchors.get("connections") or []

    # 1) Искусственные ряды (top-level или subindex).
    seq_series = _detect_artificial_sequences(labels)
    if seq_series:
        warnings.append("repeated_pattern_detected")

    # 2) Серийная цепочка связей (топология ЩА-1.1 → ЩА-1.2 → ...).
    chain_series = _detect_serial_chain_connections(connections)
    if chain_series:
        warnings.append("serial_chain_connection_detected")

    # 3) Одинаковые comment у многих labels (catalog-fill подсказка).
    identical_comments = _comments_mostly_identical(labels)
    if identical_comments:
        warnings.append("identical_comments_detected")

    # 4) Generic ratings list без labels (или labels generic'и).
    generic_ratings = _detect_generic_rating_list(ratings)
    low_label_recall = _is_low_label_recall(labels, block_type)
    if generic_ratings and (low_label_recall or not labels):
        warnings.append("generic_rating_list_without_labels")

    # 5) Org/address-like вне контекста
    if _detect_unexpected_org_or_address(payload, item_context):
        warnings.append("unexpected_org_or_address_text")

    # 6) Continuation issues — это берём из item_context (enrich_side
    #    туда складывает continuation_warnings/salvaged).
    cont_warns = item_context.get("continuation_warnings") or []
    salvaged = bool(item_context.get("salvaged"))
    if cont_warns:
        joined = " ".join(str(c) for c in cont_warns)
        if "cap_reached" in joined:
            warnings.append("truncated_output")
        if "hint_repeated" in joined or "stuck" in joined:
            warnings.append("continuation_repeated")
    if salvaged:
        warnings.append("continuation_salvaged")
    pe_detail = (item_context.get("parse_error_detail") or "").strip()
    if pe_detail in ("truncated_json", "salvage_no_safe_boundary"):
        warnings.append("truncated_output")

    # 7) Low literal label density (только для схем)
    if low_label_recall:
        warnings.append("low_literal_label_recall")

    # ── Эскалация до hallucination_suspected ─────────────────────────
    # Простой повторяющийся ряд (без других сигналов) — это «подозрительно»,
    # но в МКД-проекте 6 квартирных щитов реально могут быть. НЕ помечаем
    # автоматически как галлюцинацию. Помечаем как hallucination_suspected
    # только если есть СУПЕРПОЗИЦИЯ сигналов.
    #
    # 2026-05-28 follow-up: убран сигнал `len(labels) >= 23`. С prompt cap=25
    # legitimate cap-fill (25 реальных labels) попадал в эту планку и
    # давал false-positive hallucination. Под cap-bound prompt'ом близкое
    # к cap число labels — это нормальное поведение, не аномалия.
    hallucination_signals = 0
    if seq_series:
        hallucination_signals += 1
    if chain_series:
        hallucination_signals += 2  # сильный сигнал, считаем за два
    if identical_comments:
        hallucination_signals += 1
    if "truncated_output" in warnings:
        hallucination_signals += 1
    if generic_ratings:
        hallucination_signals += 1
    if hallucination_signals >= 2:
        if "hallucination_suspected" not in warnings:
            warnings.append("hallucination_suspected")

    # Сборка результата.
    # `truncated_output` НЕ в serious: graceful truncation на prompt cap
    # с валидными anchors должна оставаться usable. С dense_scheme prompt
    # cap=25/20/15 + max_tokens=4000 модель штатно упирается в cap и
    # truncated_json — это ожидаемое поведение, не катастрофа. Опасно
    # только в комбинации с другими сигналами — ловится composite scoring.
    serious = {
        "hallucination_suspected",
        "unexpected_org_or_address_text",
        "low_literal_label_recall",
        "serial_chain_connection_detected",
    }
    if any(w in serious for w in warnings):
        usable = False
        # Корректируем confidence: -0.2 на каждое serious warning, не ниже 0.0
        try:
            base_conf = float(payload.get("confidence") or 0.0)
        except (TypeError, ValueError):
            base_conf = 0.0
        seriouses = sum(1 for w in warnings if w in serious)
        adjusted_confidence = max(0.0, min(base_conf, 1.0) - 0.2 * seriouses)
    elif "repeated_pattern_detected" in warnings:
        # Repeated alone — info-level. confidence чуть снижаем, но
        # usable_for_diff остаётся True. Блок может быть валиден (реальная
        # серия в МКД), но Opus должен относиться скептически.
        try:
            base_conf = float(payload.get("confidence") or 0.0)
        except (TypeError, ValueError):
            base_conf = 0.0
        adjusted_confidence = max(0.0, min(base_conf, 1.0) - 0.1)

    return {
        "usable_for_diff": usable,
        "warnings": warnings,
        "adjusted_confidence": adjusted_confidence,
    }


# ─── IMAGE_DIFF_INDEX builder ────────────────────────────────────────────


_IMAGE_DIFF_INDEX_START = "<!-- IMAGE_DIFF_INDEX_START -->"
_IMAGE_DIFF_INDEX_END = "<!-- IMAGE_DIFF_INDEX_END -->"


def _extract_anchors_from_description(d: dict) -> dict[str, list[str]]:
    """Извлечь labels/ratings/connections из item.description.

    Сначала пытаемся diff_anchors (v5 prompt). Если их нет — fallback на
    visible_text/numeric_parameters/scheme_analysis.nodes/connections от
    v4-блоков. Результат всегда плоский: list[str].
    """
    out = {"labels": [], "ratings": [], "connections": []}
    if not isinstance(d, dict):
        return out
    payload = d.get("description")
    if not isinstance(payload, dict):
        return out

    da = payload.get("diff_anchors")
    if isinstance(da, dict):
        for raw in (da.get("labels") or []):
            if isinstance(raw, dict):
                txt = (raw.get("raw_text") or "").strip()
                if txt:
                    out["labels"].append(txt)
        for raw in (da.get("ratings") or []):
            if isinstance(raw, dict):
                txt = (raw.get("raw_text") or "").strip()
                if txt:
                    out["ratings"].append(txt)
        for raw in (da.get("connections") or []):
            if isinstance(raw, dict):
                f = (raw.get("from_raw") or "").strip()
                t = (raw.get("to_raw") or "").strip()
                if f or t:
                    out["connections"].append(f"{f or '?'} -> {t or '?'}")
        if out["labels"] or out["ratings"] or out["connections"]:
            return out

    # Fallback для v4-блоков: пытаемся вытащить хоть что-то.
    vt = payload.get("visible_text") or []
    if isinstance(vt, list):
        for x in vt:
            if isinstance(x, str) and x.strip():
                out["labels"].append(x.strip())
    np = payload.get("numeric_parameters") or []
    if isinstance(np, list):
        for x in np:
            if isinstance(x, dict):
                val = (x.get("value") or "").strip()
                unit = (x.get("unit") or "").strip()
                if val:
                    out["ratings"].append((val + " " + unit).strip())
    scheme = payload.get("scheme_analysis") or {}
    if isinstance(scheme, dict):
        for n in (scheme.get("nodes") or []):
            if isinstance(n, dict):
                mark = (n.get("visible_mark") or n.get("label") or "").strip()
                if mark:
                    out["labels"].append(mark)
        for c in (scheme.get("connections") or []):
            if isinstance(c, dict):
                f = (c.get("from") or "").strip()
                t = (c.get("to") or "").strip()
                if f or t:
                    out["connections"].append(f"{f or '?'} -> {t or '?'}")
    # Deduplicate, сохраняя порядок.
    for k in out:
        seen: set[str] = set()
        uniq: list[str] = []
        for v in out[k]:
            key = v.strip()
            if key and key not in seen:
                seen.add(key)
                uniq.append(key)
        out[k] = uniq
    return out


def build_image_diff_index(descriptions: list[dict]) -> str:
    """Сформировать компактный IMAGE_DIFF_INDEX для enriched MD.

    Индекс ставится в начало enriched MD сразу после
    `<!-- ENRICHED_MD_FORMAT: replace_image_blocks_v1 -->`. Opus получает
    плоский список:

        ## Page 24 / block ... / scheme / confidence 0.74 / usable_for_diff=true
        labels:
        - ЩР-1а
        ratings:
        - 1000А
        connections:
        - ВРУ-2 с.ш.1 -> ЩР-1а
        warnings:
        - none

    Это нужно, чтобы при сравнении двух стадий буквальные маркировки были
    видны Opus'у ДО любого markdown'а — снижает риск, что image_enrichment
    источник вообще не сработает.
    """
    if not descriptions:
        return _IMAGE_DIFF_INDEX_START + "\n_no image blocks_\n" + _IMAGE_DIFF_INDEX_END + "\n"

    lines = [_IMAGE_DIFF_INDEX_START]
    for d in descriptions:
        if not isinstance(d, dict):
            continue
        item_status = (d.get("status") or "").lower()
        if item_status in ("pending", "no_image"):
            continue  # пустые блоки — не индексируем
        try:
            anchors = _extract_anchors_from_description(d)
        except Exception:  # noqa: BLE001
            logger.debug("build_image_diff_index: extract failed", exc_info=True)
            continue

        page = d.get("page") or d.get("original_page")
        block_id = (d.get("md_block_id") or d.get("original_block_id")
                    or d.get("side_block_id") or "").strip()
        block_type = (d.get("block_type") or "photo_or_general").strip()
        usable = bool(d.get("usable_for_diff", True))
        warnings_list = list(d.get("warnings") or [])

        # confidence из description.confidence, не из item.
        conf_text = ""
        payload = d.get("description")
        if isinstance(payload, dict):
            try:
                conf_text = f" / confidence {float(payload.get('confidence') or 0.0):.2f}"
            except (TypeError, ValueError):
                pass

        header_parts = [
            f"## Page {page if page is not None else '?'}",
            f"block {block_id or '?'}",
            block_type,
        ]
        header = " / ".join(header_parts) + conf_text + f" / usable_for_diff={'true' if usable else 'false'}"
        lines.append(header)
        lines.append("")

        # labels / ratings / connections (если есть хотя бы по одной строке)
        for section_name, key in (("labels", "labels"), ("ratings", "ratings"), ("connections", "connections")):
            arr = anchors.get(key) or []
            if not arr:
                continue
            lines.append(f"{section_name}:")
            # Ограничиваем размер на блок, чтобы index оставался компактным.
            for v in arr[:30]:
                lines.append(f"- {v}")
            lines.append("")

        # warnings: всегда показываем хотя бы «none», чтобы Opus понимал.
        lines.append("warnings:")
        if warnings_list:
            for w in warnings_list[:8]:
                lines.append(f"- {w}")
        else:
            lines.append("- none")
        lines.append("")

    lines.append(_IMAGE_DIFF_INDEX_END)
    return "\n".join(lines) + "\n"


def compute_image_diff_index_summary(descriptions: list[dict]) -> dict:
    """Метрики IMAGE_DIFF_INDEX для записи в image_descriptions.json summary."""
    summary = {
        "total_anchor_labels": 0,
        "total_anchor_ratings": 0,
        "total_anchor_connections": 0,
        "blocks_with_diff_anchors": 0,
        "usable_for_diff_true": 0,
        "usable_for_diff_false": 0,
    }
    for d in descriptions or []:
        if not isinstance(d, dict):
            continue
        anchors = _extract_anchors_from_description(d)
        labels_n = len(anchors.get("labels") or [])
        ratings_n = len(anchors.get("ratings") or [])
        conns_n = len(anchors.get("connections") or [])
        summary["total_anchor_labels"] += labels_n
        summary["total_anchor_ratings"] += ratings_n
        summary["total_anchor_connections"] += conns_n
        if labels_n + ratings_n + conns_n > 0:
            summary["blocks_with_diff_anchors"] += 1
        if bool(d.get("usable_for_diff", True)):
            summary["usable_for_diff_true"] += 1
        else:
            summary["usable_for_diff_false"] += 1
    return summary


# ─── Подготовка карты блоков из result.json ──────────────────────────────


def load_image_blocks_index_from_result_json(result_json_path: Optional[str | Path]) -> list[dict]:
    """Прочитать result.json и вернуть только image-блоки в порядке встречи.

    Используется как fallback для связи MD image-блока с реальной картинкой
    (когда block_id из MD не указан явно). Если result.json нет — возвращаем
    пустой список.
    """
    if not result_json_path:
        return []
    from . import blocks as blocks_mod
    try:
        all_blocks, _meta = blocks_mod.normalize_blocks_from_result_json(result_json_path)
    except Exception:  # noqa: BLE001
        return []
    return [b for b in all_blocks if (b.get("type") or "").lower() == "image"]


# ─── Image resolution ────────────────────────────────────────────────────


def _invoke_render_crop(
    render_crop: Optional[Callable[..., Optional[Path]]],
    side_block_id: str,
    target_long_side: Optional[int] = None,
) -> Optional[Path]:
    """Backward-compatible wrapper над user-supplied render_crop.

    Старый контракт: ``render_crop(block_id) -> Path``. Новый контракт:
    ``render_crop(block_id, target_long_side=...) -> Path``. Используется,
    чтобы per-type render-конфиг (например, dense_scheme требует крупного
    PNG) дошёл до store.render_block_crop, но тестовые fake-callable'ы
    с сигнатурой ``def render(block_id)`` продолжали работать.
    """
    if render_crop is None:
        return None
    if target_long_side is not None:
        try:
            return render_crop(side_block_id, target_long_side=int(target_long_side))
        except TypeError:
            pass
    try:
        return render_crop(side_block_id)
    except TypeError:
        return None


def resolve_image_for_block(
    md_block: MdBlock,
    side_image_blocks: list[dict],
    used_block_ids: set[str],
    *,
    render_crop: Optional[Callable[..., Optional[Path]]] = None,
    target_long_side: Optional[int] = None,
) -> ImageResolution:
    """Связать image/imagine-блок MD с реальной картинкой.

    Стратегия:
      1. Если md_block.block_id явно совпадает с каким-либо block_id из
         `side_image_blocks` — берём его.
      2. Если у нас есть номер страницы — пытаемся сопоставить по порядку
         image-блоков на этой странице.
      3. Если render_crop коллбэк задан и нашли side_block_id — рендерим crop
         с заданным target_long_side (per-type config).
      4. Иначе возвращаем status=no_image (резюме с warning'ом).
    """
    side_by_id = {str(b.get("id") or "").strip(): b for b in side_image_blocks if b.get("id")}
    side_by_id_norm = {_normalize_block_id(k): k for k in side_by_id.keys()}

    side_block_id: Optional[str] = None
    matched_by: Optional[str] = None

    if md_block.block_id:
        if md_block.block_id in side_by_id and md_block.block_id not in used_block_ids:
            side_block_id = md_block.block_id
            matched_by = "block_id"
        else:
            norm_id = _normalize_block_id(md_block.block_id)
            real = side_by_id_norm.get(norm_id)
            if real and real not in used_block_ids:
                side_block_id = real
                matched_by = "block_id_normalized"

    if side_block_id is None and md_block.page is not None and md_block.image_order_on_page is not None:
        same_page = [b for b in side_image_blocks if (b.get("page") or 0) == md_block.page]
        idx = md_block.image_order_on_page - 1
        if 0 <= idx < len(same_page):
            cand = same_page[idx]
            cand_id = str(cand.get("id") or "")
            if cand_id and cand_id not in used_block_ids:
                side_block_id = cand_id
                matched_by = "page_order"

    if side_block_id is None:
        return ImageResolution(status="no_image", note="no_matching_image_block_in_result_json")

    if render_crop is None:
        return ImageResolution(
            status="no_image",
            side_block_id=side_block_id,
            matched_by=matched_by,
            note="renderer_unavailable",
        )

    try:
        path = _invoke_render_crop(render_crop, side_block_id, target_long_side)
    except Exception as exc:  # noqa: BLE001
        logger.warning("md_enrichment: render_crop failed for %s: %s", side_block_id, exc)
        return ImageResolution(
            status="render_failed",
            side_block_id=side_block_id,
            matched_by=matched_by,
            note=f"render_error:{type(exc).__name__}:{exc}",
        )

    if not path or not Path(path).exists():
        return ImageResolution(
            status="render_failed",
            side_block_id=side_block_id,
            matched_by=matched_by,
            note="render_returned_empty_path",
        )

    return ImageResolution(
        status="ok",
        image_path=Path(path),
        side_block_id=side_block_id,
        matched_by=matched_by,
    )


# ─── Высокоуровневый enrich для одной стороны ────────────────────────────


@dataclass
class EnrichSideSummary:
    """Сводка одной стороны (left/right) для UI/API."""

    side: str
    status: str = "not_run"   # not_run | done | done_with_salvage | partial | error
    md_path: Optional[str] = None
    md_exists: bool = False
    enriched_md_path: Optional[str] = None
    image_blocks: int = 0
    described: int = 0
    from_cache: int = 0
    errors: int = 0
    pending: int = 0
    salvaged: int = 0    # сколько блоков спасены из обрезанного JSON
    warnings: list[str] = field(default_factory=list)
    items: list[dict] = field(default_factory=list)


def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def _read_image_descriptions(session_id: str, pair_id: str, side: str) -> Optional[dict]:
    p = paths_mod.text_enrichment_descriptions_path(session_id, pair_id, side)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _write_image_descriptions(session_id: str, pair_id: str, side: str, payload: dict) -> Path:
    p = paths_mod.text_enrichment_descriptions_path(session_id, pair_id, side)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)
    return p


def _save_prompt_and_raw(
    session_id: str,
    pair_id: str,
    side: str,
    md_block: MdBlock,
    prompt: str,
    raw_excerpt: str,
    raw_full: str = "",
) -> Optional[Path]:
    """Сохранить prompt + raw для одного image-блока.

    `raw_excerpt` (≤1500 chars) пишется в `<side>_<order>.txt` (backward
    compatible). `raw_full` (полный content_text от модели, может быть >100КБ)
    пишется отдельно в `<side>_<order>.full.txt`, чтобы forensics не были
    обрезаны при сохранении.

    Возвращает Path к полному raw'у (или к excerpt'у, если raw_full пустой)
    для записи в item["raw_response_path"]. None — если ничего не записалось.
    """
    prompts_dir = paths_mod.text_enrichment_prompts_dir(session_id, pair_id)
    raw_dir = paths_mod.text_enrichment_raw_dir(session_id, pair_id)
    safe_suffix = f"{side}_{md_block.order:04d}"
    try:
        (prompts_dir / f"{safe_suffix}.txt").write_text(prompt, encoding="utf-8")
    except OSError:
        pass
    excerpt_path = raw_dir / f"{safe_suffix}.txt"
    try:
        excerpt_path.write_text(raw_excerpt or "", encoding="utf-8")
    except OSError:
        pass
    full_path: Optional[Path] = None
    if raw_full and raw_full != raw_excerpt:
        # Полный raw обычно длиннее excerpt'а — пишем отдельно, чтобы не
        # ломать инструменты, которые читают <side>_<order>.txt как короткий
        # excerpt.
        full_path = raw_dir / f"{safe_suffix}.full.txt"
        try:
            full_path.write_text(raw_full, encoding="utf-8")
        except OSError:
            full_path = None
    return full_path or (excerpt_path if (raw_excerpt or "") else None)


def _read_side_md(md_path: Optional[str | Path]) -> Optional[str]:
    if not md_path:
        return None
    p = Path(md_path)
    if not p.exists():
        return None
    try:
        return p.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None


def discover_image_blocks_for_side(md_text: Optional[str]) -> tuple[list[MdBlock], list[MdBlock]]:
    """Распарсить MD и разбить на (все_блоки, image_only_блоки)."""
    if not md_text:
        return [], []
    blocks = parse_md_blocks(md_text)
    image_blocks = [b for b in blocks if b.is_image]
    return blocks, image_blocks


def _read_json_file(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _maybe_large_sheet_block(
    session_id: str, pair_id: str, side: str, mb: "MdBlock", block_type: str,
) -> Optional[dict]:
    """Gated large-sheet path. Возвращает dict item-обновлений, если блок надо
    обслужить через Large Sheet Enrichment, иначе None (обычный поток).

    НИКОГДА не вызывает Qwen / job / LM Studio. По умолчанию ВЫКЛЮЧЕНО
    (``STAGE_COMPARISON_LARGE_SHEET_ENRICHMENT_ENABLED=false``) → всегда None.
    """
    from . import large_sheet_enrichment as ls_mod

    if not ls_mod.large_sheet_enabled():
        return None
    page = getattr(mb, "page", None)
    if not page or int(page) < 1:
        return None
    page = int(page)

    summary = ls_mod.read_large_sheet_summary(session_id, pair_id, side, page)
    has_artifact = (summary.get("status") not in (None, "not_run"))
    is_candidate = ls_mod.should_route_to_large_sheet(block_type=block_type) or has_artifact
    if not is_candidate:
        return None

    pe_path = paths_mod.large_sheet_artifact_path(session_id, pair_id, side, page, "page_enriched.json")
    md_art = paths_mod.large_sheet_artifact_path(session_id, pair_id, side, page, "page_enriched.md")
    diag_path = paths_mod.large_sheet_artifact_path(session_id, pair_id, side, page, "diagnostics.json")

    use_existing = _env_bool("STAGE_COMPARISON_LARGE_SHEET_USE_EXISTING_ARTIFACTS", True)
    if use_existing and has_artifact and pe_path.exists():
        pe = _read_json_file(pe_path) or {}
        diag = _read_json_file(diag_path) or (summary.get("diagnostics") or {})
        body = ls_mod.build_large_sheet_embed_summary(
            pe, diag, json_path=str(pe_path), md_path=str(md_art))
        return {
            "status": "done",
            "source": "large_sheet_enrichment",
            "large_sheet": True,
            "large_sheet_md": body,
            "page_enriched_json_path": str(pe_path),
            "page_enriched_md_path": str(md_art),
            "diagnostics": diag,
            "usable_for_diff": True,
        }

    # артефакт отсутствует — НЕ запускаем модель в этой задаче
    auto = _env_bool("STAGE_COMPARISON_LARGE_SHEET_AUTO_RUN_MODEL", False)
    note = (
        "### Большой лист обнаружен\n\n"
        "Большой лист обнаружен, но page_enriched.md ещё не сформирован. "
        "Запустите Large Sheet Enrichment.\n"
    )
    warns = ["large_sheet_not_prepared"]
    if auto:
        warns.append("auto_run_model_not_implemented_use_job")
    return {
        "status": "large_sheet_not_prepared",
        "source": "large_sheet_enrichment",
        "large_sheet": True,
        "large_sheet_md": note,
        "page_enriched_json_path": str(pe_path),
        "page_enriched_md_path": str(md_art),
        "large_sheet_warnings": warns,
        "usable_for_diff": False,
    }


async def enrich_side(
    session_id: str,
    pair_id: str,
    side: str,
    *,
    md_path: Optional[str | Path],
    result_json_path: Optional[str | Path] = None,
    render_crop: Optional[Callable[[str], Optional[Path]]] = None,
    describe_fn: Optional[
        Callable[[Path, str], Awaitable[graphic_local_mod.DescribeResult]]
    ] = None,
    run_model: bool = False,
    force: bool = False,
    cfg: Optional[graphic_local_mod.LocalGraphicLLMConfig] = None,
    on_block_progress: Optional[Callable[[dict], Any]] = None,
) -> EnrichSideSummary:
    """Обработать одну сторону пары — собрать enriched MD.

    Параметры:
      md_path:           путь к исходному MD стороны;
      result_json_path:  путь к result.json (для поиска image-блоков);
      render_crop:       коллбэк side_block_id → Path с PNG. Обычно
                          functools.partial(store.render_block_crop, ...);
      describe_fn:       коллбэк (image_path, prompt) → DescribeResult.
                          Если None, используется graphic_local.describe_image_local.
      run_model:         False → dry-run, никаких сетевых вызовов;
      force:             True → перезаписать enriched MD даже если есть кеш;
      cfg:               предзагруженный config; по умолчанию читаем env.
      on_block_progress: optional sync/async callback, вызывается ПОСЛЕ обработки
                          каждого image-блока с dict
                          ``{block_index, total, block_id, page, status}``
                          (block_index — 1-based, включает текущий). Caller может
                          использовать для обновления job.json после каждого блока.
                          Исключения из коллбэка глотаются (не валят enrich_side).
    """
    if side not in ("left", "right"):
        raise ValueError("side must be 'left' or 'right'")

    cfg = cfg or graphic_local_mod.load_local_graphic_llm_config()
    _retry_cfg = problem_block_retry_mod.ProblemBlockRetryConfig.from_env()
    user_supplied_describe_fn = describe_fn  # сохранить, чтобы не делать override на тестовых fake'ах

    async def _call_describe(
        image_path: Path, prompt_text: str, *, cfg_override: Optional[graphic_local_mod.LocalGraphicLLMConfig] = None,
    ) -> graphic_local_mod.DescribeResult:
        if user_supplied_describe_fn is not None:
            # backward-compat: тесты передают fake_describe(image_path, prompt)
            # без поддержки cfg override — зовём как раньше.
            return await user_supplied_describe_fn(image_path, prompt_text)
        return await graphic_local_mod.describe_image_local(
            image_path, prompt_text, cfg=(cfg_override or cfg),
        )

    summary = EnrichSideSummary(side=side, md_path=str(md_path) if md_path else None)

    md_text = _read_side_md(md_path)
    if md_text is None:
        summary.status = "error"
        summary.warnings.append("md_not_found")
        return summary
    summary.md_exists = True

    blocks, image_blocks = discover_image_blocks_for_side(md_text)
    summary.image_blocks = len(image_blocks)

    side_image_blocks_idx = load_image_blocks_index_from_result_json(result_json_path)
    side_block_by_id: dict[str, dict] = {
        str(b.get("id") or ""): b for b in side_image_blocks_idx if b.get("id")
    }

    # Контекст по страницам: для каждой страницы агрегируем текст соседних
    # text-блоков, чтобы classify_image_block мог использовать локальные
    # сигналы (ВРУ/QF/ЩР/«спецификация»/«штамп» и т.п.). Это особенно важно
    # для image-блоков без явной маркировки в заголовке.
    page_text_context: dict[Optional[int], str] = {}
    for _blk in blocks:
        if _blk.kind != "text":
            continue
        key = _blk.page
        prev = page_text_context.get(key, "")
        page_text_context[key] = (prev + "\n" + (_blk.text or ""))[-4000:]

    descriptions: list[dict] = []
    used_side_block_ids: set[str] = set()

    _total_blocks = len(image_blocks)

    async def _notify_progress(idx_one_based: int, mb_obj, item_obj):
        if on_block_progress is None:
            return
        try:
            payload = {
                "block_index": idx_one_based,
                "total": _total_blocks,
                "block_id": getattr(mb_obj, "block_id", None),
                "page": getattr(mb_obj, "page", None),
                "status": item_obj.get("status"),
            }
            ret = on_block_progress(payload)
            if asyncio.iscoroutine(ret):
                await ret
        except Exception:  # noqa: BLE001
            logger.debug("on_block_progress callback raised; ignored", exc_info=True)

    for _block_idx, mb in enumerate(image_blocks, start=1):
        # Debug-метаданные оригинального image/imagine-блока. Они не уходят в
        # основной enriched.md (там только Qwen-описание), но сохраняются в
        # image_descriptions.json для трассировки.
        original_md_excerpt = (mb.text or "")[:400].replace("\n", " ").strip()

        # ── Per-block classification & config ─────────────────────────
        side_block_hint = None  # будет уточнено после resolve, см. ниже
        block_type = classify_image_block(
            mb,
            side_block=side_block_hint,
            surrounding_context=page_text_context.get(mb.page) or page_text_context.get(None),
        )
        type_cfg = get_block_type_config(block_type)
        prompt_text, prompt_version_for_block = get_prompt_for_block_type(block_type)
        render_target_long_side = int(type_cfg.get("render_target_long_side") or 1200)
        image_input_long_side = int(type_cfg.get("image_input_long_side") or cfg.image_long_side)
        per_call_max_tokens = type_cfg.get("max_tokens")
        per_call_max_continuations = type_cfg.get("max_continuations")
        prompt_family = "scheme" if prompt_version_for_block == PROMPT_VERSION_SCHEME else "general"

        item: dict[str, Any] = {
            "order": mb.order,
            "page": mb.page,
            "image_order_on_page": mb.image_order_on_page,
            "md_block_id": mb.block_id,
            "source": "qwen_local_openai_compatible",
            "model": cfg.model,
            "prompt_version": prompt_version_for_block,
            "used_prompt_version": prompt_version_for_block,
            "prompt_family": prompt_family,
            "block_type": block_type,
            "render_target_long_side": render_target_long_side,
            "image_input_long_side": image_input_long_side,
            "usable_for_diff": True,
            "from_cache": False,
            "status": "pending",
            "side_block_id": None,
            "matched_by": None,
            "warnings": [],
            "created_at": _now_iso(),
            # Debug-only метаданные исходного MD-блока (не отображаются в enriched.md).
            "original_block_id": mb.block_id,
            "original_page": mb.page,
            "original_kind": "image",
            "original_order": mb.order,
            "original_md_excerpt": original_md_excerpt,
        }

        resolution = resolve_image_for_block(
            mb, side_image_blocks_idx, used_side_block_ids,
            render_crop=render_crop,
            target_long_side=render_target_long_side,
        )
        if resolution.side_block_id and resolution.matched_by:
            used_side_block_ids.add(resolution.side_block_id)
            item["side_block_id"] = resolution.side_block_id
            item["matched_by"] = resolution.matched_by

        # После того, как мы знаем реальный side_block, можно уточнить
        # классификацию по area_ratio/bbox (например, обычная schema может
        # стать dense_scheme если занимает >35% страницы).
        if resolution.side_block_id and resolution.side_block_id in side_block_by_id:
            refined_block_type = classify_image_block(
                mb,
                side_block=side_block_by_id.get(resolution.side_block_id),
                surrounding_context=page_text_context.get(mb.page)
                or page_text_context.get(None),
            )
            if refined_block_type != block_type:
                block_type = refined_block_type
                type_cfg = get_block_type_config(block_type)
                prompt_text, prompt_version_for_block = get_prompt_for_block_type(block_type)
                render_target_long_side = int(type_cfg.get("render_target_long_side") or 1200)
                image_input_long_side = int(type_cfg.get("image_input_long_side") or cfg.image_long_side)
                per_call_max_tokens = type_cfg.get("max_tokens")
                per_call_max_continuations = type_cfg.get("max_continuations")
                prompt_family = "scheme" if prompt_version_for_block == PROMPT_VERSION_SCHEME else "general"
                item["block_type"] = block_type
                item["prompt_version"] = prompt_version_for_block
                item["used_prompt_version"] = prompt_version_for_block
                item["prompt_family"] = prompt_family
                item["render_target_long_side"] = render_target_long_side
                item["image_input_long_side"] = image_input_long_side
                # render_crop для dense_scheme может требовать большего масштаба
                # — пере-рендерим, если изменился target_long_side. Иначе
                # переиспользуем уже отрендеренный crop.
                try:
                    new_path = _invoke_render_crop(
                        render_crop, resolution.side_block_id, render_target_long_side,
                    )
                    if new_path and Path(new_path).exists():
                        resolution.image_path = Path(new_path)
                except Exception:  # noqa: BLE001
                    logger.debug("re-render after classify refinement failed", exc_info=True)

        # ── Large Sheet Enrichment gated path (default OFF) ───────────
        # Если включён и блок относится к большому/плотному листу, обслуживаем
        # его готовым page_enriched.md вместо single-image Qwen. Не зависит от
        # crop-resolution (большому листу single crop не нужен). Qwen НЕ зовём.
        try:
            ls_updates = _maybe_large_sheet_block(session_id, pair_id, side, mb, block_type)
        except Exception:  # noqa: BLE001 — large-sheet ветка не должна валить enrich
            logger.debug("large-sheet gated path failed; falling back to normal flow",
                         exc_info=True)
            ls_updates = None
        if ls_updates is not None:
            item.update(ls_updates)
            for w in ls_updates.get("large_sheet_warnings", []) or []:
                if w not in item["warnings"]:
                    item["warnings"].append(w)
            descriptions.append(item)
            st = item.get("status")
            if st == "done":
                summary.described += 1
            elif st == "large_sheet_not_prepared":
                summary.pending += 1
            else:
                summary.errors += 1
            await _notify_progress(_block_idx, mb, item)
            continue

        if resolution.status != "ok":
            item["status"] = "error" if resolution.status == "render_failed" else "no_image"
            item["error"] = resolution.note
            item["warnings"].append(resolution.note)
            descriptions.append(item)
            summary.errors += 1 if resolution.status == "render_failed" else 0
            await _notify_progress(_block_idx, mb, item)
            continue

        # Кешируем по контенту картинки
        try:
            img_bytes = Path(resolution.image_path).read_bytes()
        except OSError as exc:
            item["status"] = "error"
            item["error"] = f"read_image_failed:{type(exc).__name__}:{exc}"
            summary.errors += 1
            descriptions.append(item)
            await _notify_progress(_block_idx, mb, item)
            continue

        # Cache-key включает per-block prompt_version. На блок схемы под v5
        # получится отдельный кеш — старый v4-кеш для photo_or_general остаётся
        # валидным.
        cache_key = compute_image_cache_key(img_bytes, cfg.model, prompt_version_for_block)
        item["cache_key"] = cache_key

        cached = read_cache(session_id, pair_id, cache_key) if not force else None
        if cached and not force:
            item["from_cache"] = True
            item["status"] = cached.get("status") or "done"
            item["description"] = cached.get("description")
            item["model_used"] = cached.get("model_used") or cfg.model
            item["raw_response_excerpt"] = cached.get("raw_response_excerpt", "")
            descriptions.append(item)
            if item["status"] == "done":
                summary.described += 1
                summary.from_cache += 1
            else:
                summary.errors += 1
            await _notify_progress(_block_idx, mb, item)
            continue

        if not run_model:
            item["status"] = "pending"
            item["warnings"].append("dry_run_no_model_call")
            summary.pending += 1
            descriptions.append(item)
            await _notify_progress(_block_idx, mb, item)
            continue

        # ── Real call ─────────────────────────────────────────────
        # Per-block cfg override: меняем image_long_side / max_tokens /
        # max_continuations под тип блока, не задирая глобальные env.
        try:
            from dataclasses import replace as _dc_replace
            cfg_override = _dc_replace(
                cfg,
                image_long_side=image_input_long_side,
                max_tokens=int(per_call_max_tokens) if per_call_max_tokens else cfg.max_tokens,
                max_continuations=(
                    int(per_call_max_continuations) if per_call_max_continuations is not None
                    else cfg.max_continuations
                ),
            )
        except Exception:  # noqa: BLE001
            cfg_override = cfg

        started = time.monotonic()
        try:
            result = await _call_describe(
                Path(resolution.image_path), prompt_text, cfg_override=cfg_override,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("md_enrichment: describe_fn raised for block order=%s", mb.order)
            item["status"] = "error"
            item["error"] = f"describe_exception:{type(exc).__name__}:{exc}"
            item["duration_sec"] = round(time.monotonic() - started, 3)
            summary.errors += 1
            descriptions.append(item)
            await _notify_progress(_block_idx, mb, item)
            continue

        item["duration_sec"] = round(time.monotonic() - started, 3)
        item["model_used"] = (result.model_used or cfg.model)
        item["fallback_used"] = bool(result.fallback_used)
        item["raw_response_excerpt"] = result.raw_response_excerpt or ""
        # Расширенная диагностика для тюнинга prompt/max_tokens без чтения raw'а.
        item["finish_reason"] = result.finish_reason
        item["usage"] = result.usage or None
        item["response_char_count"] = int(result.response_char_count or 0)
        item["parse_error_detail"] = result.parse_error_detail
        # Production tracking: какая версия prompt'а реально применена.
        # Полезно при rolling cache invalidation, чтобы оператор мог
        # увидеть, какие items описаны под какой prompt-версией.
        item["used_prompt_version"] = prompt_version_for_block
        item["compact_mode_used"] = (
            prompt_version_for_block.startswith("v4")
            or prompt_version_for_block.startswith("v5")
        )
        # Сохраняем полный raw отдельным файлом, чтобы forensics не были
        # обрезаны excerpt'ом до 1500 символов.
        raw_full = getattr(result, "full_raw_response", "") or ""
        full_path = _save_prompt_and_raw(
            session_id, pair_id, side, mb, prompt_text, result.raw_response_excerpt or "",
            raw_full=raw_full,
        )
        if full_path is not None:
            try:
                item["raw_response_path"] = str(full_path)
            except (TypeError, ValueError):
                pass

        # Continuation accounting: chunks_count лежит внутри parsed,
        # вытащим в item для удобного агрегирования на session-level.
        parsed_for_diag = result.parsed if isinstance(result.parsed, dict) else {}
        cc = parsed_for_diag.get("chunks_count")
        item["chunks_count"] = int(cc) if isinstance(cc, int) and cc > 0 else 1
        item["continuation_count"] = max(0, item["chunks_count"] - 1)
        item["continued"] = bool(parsed_for_diag.get("continued"))
        cont_warns = parsed_for_diag.get("continuation_warnings")
        if isinstance(cont_warns, list) and cont_warns:
            item["continuation_warnings"] = list(cont_warns)

        if result.status == "done" and isinstance(result.parsed, dict):
            payload = dict(result.parsed)
            payload.setdefault("status", "done")
            item["status"] = "done"
            item["description"] = payload
            item["final_status_reason"] = "primary_done"
            # Quality analysis: hallucination/repeated_pattern/generic_rating —
            # сюда добавляем дополнительные warnings и поправку confidence.
            try:
                quality = analyze_qwen_description_quality(payload, item)
                item["usable_for_diff"] = bool(quality.get("usable_for_diff", True))
                for w in quality.get("warnings") or []:
                    if w and w not in item["warnings"]:
                        item["warnings"].append(w)
                adj = quality.get("adjusted_confidence")
                if isinstance(adj, (int, float)):
                    item["adjusted_confidence"] = round(float(adj), 3)
            except Exception:  # noqa: BLE001
                logger.debug("analyze_qwen_description_quality failed", exc_info=True)
            cache_payload = {
                "status": "done",
                "description": payload,
                "model_used": item["model_used"],
                "raw_response_excerpt": item["raw_response_excerpt"],
                "created_at": _now_iso(),
                "cache_key": cache_key,
                "prompt_version": prompt_version_for_block,
            }
            try:
                write_cache(session_id, pair_id, cache_key, cache_payload)
            except OSError:
                item["warnings"].append("cache_write_failed")
            summary.described += 1
        elif result.status == "partial" and isinstance(result.parsed, dict):
            # Salvage из оборванного JSON: данные частично восстановлены, но
            # это НЕ полноценный success — не кешируем, чтобы следующий прогон
            # мог попытаться получить полный ответ.
            payload = dict(result.parsed)
            payload["status"] = "salvaged_partial"
            payload["_salvaged"] = True
            item["status"] = "partial"
            item["salvaged"] = True
            item["description"] = payload
            item["error"] = result.error or "salvaged_partial_json"
            item["warnings"].append("salvaged_partial_json")
            # Human-readable explanation: труд оператора в Stage 02/Opus
            # начинается с диагностики, почему конкретный блок попал в partial.
            item["final_status_reason"] = (
                "salvaged_with_continuation" if item["continued"] else "salvaged_partial"
            )
            try:
                quality = analyze_qwen_description_quality(payload, item)
                # Для salvage'а usable_for_diff почти всегда False (есть как
                # минимум warning continuation_salvaged), но не убираем блок —
                # он остаётся как weak confirmation.
                item["usable_for_diff"] = bool(quality.get("usable_for_diff", False))
                for w in quality.get("warnings") or []:
                    if w and w not in item["warnings"]:
                        item["warnings"].append(w)
                adj = quality.get("adjusted_confidence")
                if isinstance(adj, (int, float)):
                    item["adjusted_confidence"] = round(float(adj), 3)
            except Exception:  # noqa: BLE001
                logger.debug("analyze_qwen_description_quality failed (partial)", exc_info=True)
            summary.described += 1
            summary.salvaged += 1
        else:
            item["status"] = "error"
            err_payload: dict[str, Any] = {
                "status": "error",
                "error": result.error or result.status,
            }
            item["error"] = result.error or result.status
            item["description"] = err_payload
            # Категория сбоя из describe_image_local (markdown_reasoning,
            # truncated_json, empty_content, ctx_mismatch, ...).
            item["final_status_reason"] = (
                result.parse_error_detail or result.status or "unknown_error"
            )
            summary.errors += 1

        # ── Problem-block tiled high-res retry (feature-flagged, default OFF) ──
        # Runs only AFTER the baseline pass, only for problem blocks; preserves
        # baseline output; never raises into the pipeline.
        if run_model and _retry_cfg.enabled and _retry_cfg.after_main:
            try:
                import dataclasses as _dc_retry

                _tile_cfg = _dc_retry.replace(
                    cfg,
                    timeout_sec=int(_retry_cfg.tile_timeout_sec),
                    max_continuations=1,
                    image_long_side=int(max(_retry_cfg.tile_width, _retry_cfg.tile_height)),
                )

                async def _tile_describe(_img, _prompt, __cfg=_tile_cfg):
                    return await _call_describe(_img, _prompt, cfg_override=__cfg)

                _side_block = side_block_by_id.get(item.get("side_block_id") or "")
                _pre_status = item.get("status")
                item = await problem_block_retry_mod.maybe_run_problem_block_retry(
                    item=item,
                    side_block=_side_block,
                    error=None,
                    render_crop=render_crop,
                    describe_fn=_tile_describe,
                    cfg=_retry_cfg,
                    session_id=session_id,
                    pair_id=pair_id,
                    side=side,
                    model=cfg.model,
                    cache_read=lambda k: read_cache(session_id, pair_id, k),
                    cache_write=lambda k, v: write_cache(session_id, pair_id, k, v),
                )
                if (item.get("method_used") == "tiled_retry"
                        and _pre_status in ("error", "partial", "no_image")
                        and item.get("status") == "done"):
                    summary.described += 1
                    if _pre_status == "error":
                        summary.errors = max(0, summary.errors - 1)
            except Exception:  # noqa: BLE001 — retry must never break enrichment
                logger.debug("problem_block_retry hook failed (ignored)", exc_info=True)

        descriptions.append(item)
        await _notify_progress(_block_idx, mb, item)

    # ── Записать enriched MD + JSON ─────────────────────────────────
    enriched_md = build_enriched_md(blocks, descriptions)
    md_out = paths_mod.text_enrichment_md_path(session_id, pair_id, side)
    if force or run_model or not md_out.exists():
        try:
            md_out.write_text(enriched_md, encoding="utf-8")
            summary.enriched_md_path = str(md_out)
        except OSError as exc:
            summary.warnings.append(f"enriched_md_write_failed:{type(exc).__name__}:{exc}")
    else:
        summary.enriched_md_path = str(md_out) if md_out.exists() else None

    # Replacement-mode counters: сколько image/imagine блоков в исходном MD,
    # сколько было реально заменено Qwen-описанием (любой usable status:
    # done / done_with_salvage), и сколько Qwen-описаний попало в enriched.md.
    original_image_blocks = summary.image_blocks
    replaced_image_blocks = sum(
        1 for d in descriptions
        if (d.get("status") or "").lower() in ("done", "partial", "no_image", "error", "pending")
    )
    qwen_description_blocks = sum(
        1 for d in descriptions
        if (d.get("status") or "").lower() in ("done", "partial")
    )

    # IMAGE_DIFF_INDEX summary: total anchors, blocks_with_diff_anchors,
    # usable_for_diff_true/false. Используется UI/aggregate_job_progress.
    try:
        diff_summary = compute_image_diff_index_summary(descriptions)
    except Exception:  # noqa: BLE001
        logger.debug("compute_image_diff_index_summary failed", exc_info=True)
        diff_summary = {}

    # Block-type / quality breakdown — для diagnostics.
    blocks_by_type: dict[str, int] = {}
    hallucination_warns = 0
    continuation_warns = 0
    done_with_salvage_count = 0
    avg_conf_sum = 0.0
    avg_conf_n = 0
    for d in descriptions:
        if not isinstance(d, dict):
            continue
        bt = (d.get("block_type") or "photo_or_general").strip()
        blocks_by_type[bt] = blocks_by_type.get(bt, 0) + 1
        warns = d.get("warnings") or []
        if any(w in (
            "repeated_pattern_detected",
            "hallucination_suspected",
            "unexpected_org_or_address_text",
            "generic_rating_list_without_labels",
            "low_literal_label_recall",
        ) for w in warns):
            hallucination_warns += 1
        if any(w in (
            "continuation_salvaged",
            "continuation_repeated",
            "truncated_output",
            "salvaged_partial_json",
        ) for w in warns):
            continuation_warns += 1
        if d.get("salvaged"):
            done_with_salvage_count += 1
        payload = d.get("description")
        if isinstance(payload, dict):
            try:
                c = float(payload.get("confidence") or 0.0)
                avg_conf_sum += c
                avg_conf_n += 1
            except (TypeError, ValueError):
                pass

    enrichment_metrics = {
        "qwen_blocks_by_type": blocks_by_type,
        "hallucination_warnings_count": hallucination_warns,
        "continuation_warnings_count": continuation_warns,
        "done_with_salvage_count": done_with_salvage_count,
        "avg_confidence": round(avg_conf_sum / avg_conf_n, 3) if avg_conf_n else 0.0,
    }
    enrichment_metrics.update(diff_summary)

    payload_json = {
        "version": 1,
        "enriched_md_format_version": ENRICHED_MD_FORMAT_VERSION,
        "replacement_mode": True,
        "session_id": session_id,
        "pair_id": pair_id,
        "side": side,
        "model": cfg.model,
        "fallback_model": cfg.fallback_model,
        "provider": cfg.provider,
        # Сохраняем legacy global prompt_version для backward-compat (UI),
        # но реальный per-block prompt лежит в items[].used_prompt_version.
        "prompt_version": PROMPT_VERSION,
        "md_path": str(md_path) if md_path else None,
        "result_json_path": str(result_json_path) if result_json_path else None,
        "enriched_md_path": str(md_out),
        "image_blocks_total": summary.image_blocks,
        "described": summary.described,
        "from_cache": summary.from_cache,
        "errors": summary.errors,
        "pending": summary.pending,
        "salvaged": summary.salvaged,
        "original_image_blocks": original_image_blocks,
        "replaced_image_blocks": replaced_image_blocks,
        "qwen_description_blocks": qwen_description_blocks,
        "enrichment_metrics": enrichment_metrics,
        "problem_block_retry": problem_block_retry_mod.summarize_problem_block_retry(
            descriptions, _retry_cfg
        ),
        "updated_at": _now_iso(),
        "items": descriptions,
        "run_model": bool(run_model),
        "force": bool(force),
    }
    try:
        _write_image_descriptions(session_id, pair_id, side, payload_json)
    except OSError as exc:
        summary.warnings.append(f"descriptions_json_write_failed:{type(exc).__name__}:{exc}")

    if summary.image_blocks == 0:
        summary.status = "done"
    elif (summary.described == summary.image_blocks
          and summary.errors == 0
          and summary.pending == 0
          and summary.salvaged == 0):
        summary.status = "done"
    elif (summary.described == summary.image_blocks
          and summary.errors == 0
          and summary.pending == 0
          and summary.salvaged > 0):
        # Все блоки получили usable description, но часть восстановлена
        # salvage'ом / continuation'ом. Это готовое состояние для pipeline:
        # enriched MD создан, ready_for_unified_analysis истинно, но в
        # диагностике сохраняется метка "восстановлено", чтобы оператор
        # мог при желании дёрнуть quality-retry.
        summary.status = "done_with_salvage"
    elif summary.described == 0 and summary.errors == 0 and summary.pending > 0 and not run_model:
        summary.status = "not_run"
    elif summary.errors > 0:
        summary.status = "partial" if summary.described > 0 else "error"
    else:
        summary.status = "partial"

    summary.items = descriptions
    return summary


def read_summary_only(session_id: str, pair_id: str, side: str) -> dict:
    """Лёгкое read-only представление для GET md-enrichment.

    Не запускает парсер и не читает MD — только подхватывает существующий
    JSON, чтобы быстро отрисовать статус в UI.
    """
    data = _read_image_descriptions(session_id, pair_id, side)
    md_path_resolved = paths_mod.text_enrichment_md_path(session_id, pair_id, side)
    md_format = "unknown"
    if md_path_resolved.exists():
        try:
            head = md_path_resolved.read_text(encoding="utf-8", errors="replace")[:4096]
            md_format = detect_enriched_md_format(head)
        except OSError:
            md_format = "unknown"
    if not data:
        return {
            "side": side,
            "status": "not_run",
            "image_blocks": 0,
            "described": 0,
            "from_cache": 0,
            "errors": 0,
            "pending": 0,
            "salvaged": 0,
            "enriched_md_path": None,
            "enriched_md_format_version": md_format,
            "replacement_mode": md_format == ENRICHED_MD_FORMAT_VERSION,
            "original_image_blocks": 0,
            "replaced_image_blocks": 0,
            "qwen_description_blocks": 0,
        }
    salvaged = int(data.get("salvaged") or 0)
    described = int(data.get("described") or 0)
    image_blocks_total = int(data.get("image_blocks_total") or 0)
    errors_n = int(data.get("errors") or 0)
    pending_n = int(data.get("pending") or 0)
    if image_blocks_total and described == image_blocks_total and errors_n == 0 and pending_n == 0 and salvaged == 0:
        status = "done"
    elif (image_blocks_total
          and described == image_blocks_total
          and errors_n == 0
          and pending_n == 0
          and salvaged > 0):
        # Все блоки описаны, часть восстановлена salvage'ом — это
        # backward-совместимое представление старых artifact'ов,
        # которые писали status="partial" по той же ситуации.
        status = "done_with_salvage"
    elif described > 0:
        status = "partial"
    else:
        status = "not_run"
    # Replacement-mode metadata. JSON может быть legacy (без полей) — тогда
    # source-of-truth — формат enriched.md на диске.
    json_format = (data.get("enriched_md_format_version") or "").strip() or None
    if json_format:
        format_version = json_format
    else:
        format_version = md_format
    return {
        "side": side,
        "status": status,
        "image_blocks": image_blocks_total,
        "described": described,
        "from_cache": int(data.get("from_cache") or 0),
        "errors": errors_n,
        "pending": pending_n,
        "salvaged": salvaged,
        "enriched_md_path": data.get("enriched_md_path"),
        "model": data.get("model"),
        "provider": data.get("provider"),
        "updated_at": data.get("updated_at"),
        "enriched_md_format_version": format_version,
        "replacement_mode": format_version == ENRICHED_MD_FORMAT_VERSION,
        "original_image_blocks": int(data.get("original_image_blocks") or image_blocks_total),
        "replaced_image_blocks": int(data.get("replaced_image_blocks") or 0),
        "qwen_description_blocks": int(data.get("qwen_description_blocks") or described),
        "md_format_on_disk": md_format,
    }


def rebuild_enriched_md_from_descriptions(
    session_id: str,
    pair_id: str,
    side: str,
    *,
    md_path: Optional[str | Path] = None,
) -> dict:
    """Пересобрать `<side>_enriched.md` из существующего image_descriptions.json,
    не вызывая Qwen повторно.

    Используется когда:
      - Qwen descriptions уже готовы и валидны;
      - но enriched.md лежит в старом `append_v0` формате (с
        `<!-- original_imagine_start -->` обёрткой).

    Возвращает dict с counts: `original_image_blocks`,
    `replaced_image_blocks`, `qwen_description_blocks`, `enriched_md_path`,
    `enriched_md_format_version`, `status`.

    Cache key включает PROMPT_VERSION (не ENRICHED_MD_FORMAT_VERSION) — потому
    что при rebuild мы не дёргаем модель, мы только пересобираем enriched.md
    из уже готовых items. Cache-инвалидация формата не нужна.
    """
    if side not in ("left", "right"):
        raise ValueError("side must be 'left' or 'right'")

    data = _read_image_descriptions(session_id, pair_id, side)
    if not data:
        return {"status": "no_descriptions", "side": side}

    items = data.get("items") if isinstance(data.get("items"), list) else []

    md_resolved: Optional[str | Path] = md_path or data.get("md_path")
    md_text = _read_side_md(md_resolved)
    if md_text is None:
        return {"status": "md_not_found", "side": side, "md_path": str(md_resolved) if md_resolved else None}

    blocks, image_blocks = discover_image_blocks_for_side(md_text)

    enriched_md = build_enriched_md(blocks, items)
    md_out = paths_mod.text_enrichment_md_path(session_id, pair_id, side)
    try:
        md_out.write_text(enriched_md, encoding="utf-8")
    except OSError as exc:
        return {"status": f"write_failed:{type(exc).__name__}", "side": side, "error": str(exc)[:200]}

    original_image_blocks = len(image_blocks)
    replaced_image_blocks = sum(
        1 for d in items
        if (d.get("status") or "").lower() in ("done", "partial", "no_image", "error", "pending")
    )
    qwen_description_blocks = sum(
        1 for d in items
        if (d.get("status") or "").lower() in ("done", "partial")
    )

    data["enriched_md_format_version"] = ENRICHED_MD_FORMAT_VERSION
    data["replacement_mode"] = True
    data["enriched_md_path"] = str(md_out)
    data["original_image_blocks"] = original_image_blocks
    data["replaced_image_blocks"] = replaced_image_blocks
    data["qwen_description_blocks"] = qwen_description_blocks
    data["updated_at"] = _now_iso()
    try:
        _write_image_descriptions(session_id, pair_id, side, data)
    except OSError:
        pass

    return {
        "status": "rebuilt",
        "side": side,
        "enriched_md_path": str(md_out),
        "enriched_md_format_version": ENRICHED_MD_FORMAT_VERSION,
        "original_image_blocks": original_image_blocks,
        "replaced_image_blocks": replaced_image_blocks,
        "qwen_description_blocks": qwen_description_blocks,
        "size_bytes": len(enriched_md.encode("utf-8")),
        "size_chars": len(enriched_md),
    }


__all__ = [
    "PROMPT_VERSION",
    "ENRICHED_MD_FORMAT_VERSION",
    "QWEN_IMAGE_DESCRIPTION_PROMPT",
    "MdBlock",
    "ImageResolution",
    "EnrichSideSummary",
    "parse_md_blocks",
    "discover_image_blocks_for_side",
    "compute_image_cache_key",
    "read_cache",
    "write_cache",
    "build_enriched_md",
    "detect_enriched_md_format",
    "rebuild_enriched_md_from_descriptions",
    "resolve_image_for_block",
    "load_image_blocks_index_from_result_json",
    "enrich_side",
    "read_summary_only",
]
