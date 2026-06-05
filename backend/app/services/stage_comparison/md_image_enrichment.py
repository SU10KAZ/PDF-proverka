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

from . import block_pdf_source as block_pdf_source_mod
from . import graphic_llm_local as graphic_local_mod
from . import graphic_profiles as graphic_profiles_mod
from . import grsh_feeder_extraction as grsh_feeder_mod
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
# r1 (flag-gated): схемный prompt + фиксированные доменные поля (domain_fields)
# с явным «не указано» для отсутствующих. Отдельная версия → отдельный cache-key,
# старый v5-кеш не задевается, активируется только при включённом флаге.
PROMPT_VERSION_SCHEME_DOMAIN = "v6_scheme_domain_fields"
# GRSH single-shot prompt (controlled experiment 2026-06-04, attempt_05):
# плотные однолинейные схемы ГРЩ получают отдельный prompt с инъекцией
# Chandra-OCR-словаря буквальных маркировок + жёсткий анти-ряд / анти-переименование.
# Single-shot (max_continuations=0), без tile mode. Отдельная prompt-версия →
# отдельный cache-key (старый v5-кеш этих блоков не подхватывается, нужен
# re-enrichment). v8 — GRSH + domain_fields (flag-gated).
PROMPT_VERSION_GRSH = "v7_grsh_singleline"
PROMPT_VERSION_GRSH_DOMAIN = "v8_grsh_domain_fields"
# v9 — GRSH tiled пофидерное извлечение (contour B, grsh_feeder_extraction).
# Отдельная prompt-версия → отдельный cache-key (не подхватывает v7 single-shot).
PROMPT_VERSION_GRSH_FEEDER = "v9_grsh_feeder_tiled"

# Все «схемные» prompt-версии (для prompt_family-метки и diagnostics).
_SCHEME_FAMILY_VERSIONS = frozenset({
    PROMPT_VERSION_SCHEME,
    PROMPT_VERSION_SCHEME_DOMAIN,
    PROMPT_VERSION_GRSH,
    PROMPT_VERSION_GRSH_DOMAIN,
    PROMPT_VERSION_GRSH_FEEDER,
})


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


# ─── GRSH single-line prompt семейство (controlled experiment 2026-06-04) ──
#
# Для плотных однолинейных схем ГРЩ (главный распределительный щит) обычный
# v5-prompt стабильно галлюцинировал ряды (ТП1…ТП22, ГРЩ1-РП1-8…40) и подменял
# трансформаторы Т1/Т2 на ТП1/ТП2. Эксперимент (attempt_05) показал рабочий
# рецепт:
#   * single-shot @ image_long_side≈2000, max_tokens≈9000 (continuation выкл);
#   * инъекция Chandra-OCR-словаря буквальных маркировок ПЕРЕД prompt'ом —
#     модель строит СТРУКТУРУ/СВЯЗИ, опираясь на словарь, а не выдумывает;
#   * жёсткий анти-ряд («вводов всего ДВА, не достраивай ряды») + анти-rename
#     («если в словаре Т1/Т2 — пиши Т1/Т2, не ТП1/ТП2»);
#   * раздельные взаимоисключающие бакеты verified / visual_unverified / rejected;
#   * tile mode НЕ применять (он РЕинтродуцирует ложные ряды — attempt_06).
#
# Словарь Chandra инжектится per-block в enrich_side (build_grsh_anchor_vocab_block),
# поэтому здесь — статический шаблон правил и схемы JSON.
QWEN_GRSH_SINGLELINE_PROMPT = """Перед тобой ОДНОЛИНЕЙНАЯ СХЕМА ГРЩ (главный распределительный щит) из проектной документации.

Твоя задача — восстановить СТРУКТУРУ и СВЯЗИ (кто кого питает: ввод → секция ГРЩ → автомат QF → кабель → потребитель ВРУ/ШУ), опираясь на словарь буквальных маркировок Chandra-OCR (он подан ВЫШЕ как РЕФЕРЕНС, а не как полное описание).

ПРАВИЛА (нарушение = брак):
1. Возвращай ТОЛЬКО валидный, полностью закрытый JSON. Никакого markdown, текста до/после JSON, комментариев `//`. Не используй многоточие (`…`, `...`, «и т.д.»).
2. В `verified_anchors` клади ТОЛЬКО маркировки, которые есть в словаре Chandra ИЛИ буквально читаются на картинке и совпадают по форме со словарём.
3. Если на картинке видно что-то, чего НЕТ в словаре — это `visual_unverified_anchors` (НЕ verified). Каждая маркировка живёт ровно в ОДНОМ списке (не дублируй в verified/visual/rejected/uncertain одновременно).
4. ЗАПРЕЩЕНО придумывать числовые ряды (ТП1…ТП22, ВРУ1…ВРУ20, ГРЩ1-РП1…РП40, QF1…QF50). В этой схеме вводов всего ДВА. Отходящих ВРУ — единицы (см. словарь). Не достраивай ряды до «дом на N квартир».
5. `uncertainties` — ТОЛЬКО для нечитаемых/сомнительных надписей. Если видишь намёк на ряд, но не уверен — добавь ОДИН элемент в `uncertainties` с перечислением читаемого, НЕ плоди элементы.
6. Не переноси маркировки с соседних листов/таблиц. Только то, что на этой схеме.
7. Числовые номиналы (А, кВт, кА, мм²) бери из словаря или из чётко читаемого текста рядом с элементом. Не добавляй типовые номиналы «из каталога» без видимого основания.
8. НЕ переименовывай элементы. Если в словаре трансформатор Т1/Т2 — пиши Т1/Т2, НЕ заменяй на ТП1/ТП2. Если в словаре ШУ-ХЦ / ШУ-АПТ — так и пиши. Используй ровно ту форму маркировки, что в словаре/на картинке.
9. Не считай разными потребителями те, что отличаются лишь формой записи (ШУ-ХЦ ↔ ВРУ-ХЦ, ШУ-АПТ ↔ ВРУ-АПТ) — если сомневаешься, отметь в `uncertainties`.

Верни ТОЛЬКО JSON по схеме (заполнители `<...>` — шаблон, не пиши их в ответе):

{
"status": "done",
"sheet_kind": "electrical_single_line",
"summary": "<1-2 предложения о структуре схемы, без новых фактов>",
"verified_anchors": {
"labels": ["<буквальные маркировки, подтверждённые словарём Chandra>"],
"cables": ["<кабели>"],
"ratings": ["<номиналы A/кВт/кА/мм²>"],
"equipment": ["<оборудование: счётчики, АУКРМ, АВР, ТТ>"]
},
"visual_unverified_anchors": ["<видно на картинке, но НЕТ в словаре Chandra>"],
"rejected_anchors": ["<что ты НЕ стал выписывать: предполагаемый ряд, нечитаемое>"],
"panels": [
{"name": "<ГРЩ1 РП1>", "type": "main_switchboard_section", "fed_from": "<Т1|ТП1>", "input": {"label": "<Ввод 1>", "busbar": "<3L/PEN Al 3200А>"}}
],
"circuits": [
{"id": "<1ГРЩ-ВРУ1>", "source": "<ГРЩ1 РП1>", "breaker": "<1QF6>", "breaker_params": "<3P 800A>", "cable": "<ППГнг(А)-HF 3х(5х120)>", "consumer": "<ВРУ1>", "load": {"p_calc_kw": null, "i_calc_a": null}, "validation": {"label_status": "verified_by_chandra|visual_only|rejected", "cable_status": "verified_by_chandra|visual_only|rejected"}, "confidence": 0.0}
],
"connections": [
{"from": "<Т1>", "to": "<ГРЩ1 РП1>", "via": "<шинопровод 3200А>", "status": "verified_by_chandra|visual_only", "confidence": 0.0}
],
"uncertainties": ["<нечитаемое/сомнительное>"],
"warnings": [],
"confidence": 0.0
}

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
# Плотная однолинейная схема ГРЩ — особый подтип dense_scheme. Имеет приоритет
# выше dense_scheme: использует GRSH single-shot prompt + Chandra-словарь и
# запрещает tile mode. См. QWEN_GRSH_SINGLELINE_PROMPT.
BLOCK_TYPE_DENSE_GRSH = "dense_grsh_singleline"
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

# ─── GRSH (главный распределительный щит) — детекция плотной однолинейной ──
# Заголовок/лист должен явно говорить о ГРЩ (однолинейная/принципиальная схема
# ГРЩ). Без этого обычная схема с упоминанием «ГРЩ» в сноске не становится GRSH.
_GRSH_HEADING_MARKERS = (
    "однолинейная схема грщ",
    "схема электрическая принципиальная грщ",
    "принципиальная схема грщ",
    "схема грщ",
    "однолинейная схема главного распределительного щита",
)

# Группы маркеров Chandra-raw: для подтверждения, что это реально плотная
# однолинейная ГРЩ, требуется несколько РАЗНЫХ групп (≥ _GRSH_MIN_MARKER_GROUPS).
# Каждая группа — кортеж вариантов (любого достаточно для попадания группы).
_GRSH_CHANDRA_MARKER_GROUPS: tuple[tuple[str, ...], ...] = (
    ("грщ",),
    ("вру",),
    ("тп1", "тп2", "т1", "т2"),     # вводы от ТП / трансформаторов
    ("qf", "qs"),                    # автоматы / разъединители
    ("ппгнг", "кппгнг", "пугпнг"),  # кабели ППГнг(А)-HF
    ("шинопровод",),
    ("аукрм", "акврм", "укрм"),     # компенсация реактивной мощности
)
_GRSH_MIN_MARKER_GROUPS = 3


def _count_marker_hits(text: str, markers: tuple[str, ...]) -> int:
    if not text:
        return 0
    low = text.lower()
    return sum(low.count(m.lower()) for m in markers)


def _count_marker_groups(text: str, groups: tuple[tuple[str, ...], ...]) -> int:
    """Число РАЗНЫХ групп маркеров, у которых сработал хотя бы один вариант."""
    if not text:
        return 0
    low = text.lower()
    hit = 0
    for group in groups:
        if any(variant in low for variant in group):
            hit += 1
    return hit


def _is_dense_grsh_singleline(excerpt: str, block_text: str) -> bool:
    """Эвристика: это ли плотная однолинейная схема ГРЩ?

    Требует ОДНОВРЕМЕННО:
      * явное упоминание ГРЩ в заголовке/окружении (`_GRSH_HEADING_MARKERS`);
      * ≥ `_GRSH_MIN_MARKER_GROUPS` РАЗНЫХ групп электро-маркеров в
        Chandra-тексте блока (ГРЩ / ВРУ / ТП-Т / QF-QS / кабели / шинопровод /
        компенсация).

    Вызывается только когда блок уже распознан как электрическая схема
    (`looks_like_scheme`), поэтому имеет приоритет над dense_scheme.
    """
    low_excerpt = (excerpt or "").lower()
    heading_grsh = any(h in low_excerpt for h in _GRSH_HEADING_MARKERS)
    if not heading_grsh:
        # Запасной сигнал: «грщ» рядом с «однолинейн»/«принципиальн» в заголовке.
        if "грщ" in low_excerpt and ("однолинейн" in low_excerpt
                                     or "принципиальн" in low_excerpt):
            heading_grsh = True
    if not heading_grsh:
        return False
    # Chandra-маркеры считаем по тексту самого блока + окружению.
    marker_source = (block_text or "") + "\n" + (excerpt or "")
    groups = _count_marker_groups(marker_source, _GRSH_CHANDRA_MARKER_GROUPS)
    return groups >= _GRSH_MIN_MARKER_GROUPS


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
        # GRSH плотная однолинейная схема имеет приоритет над dense_scheme:
        # отдельный single-shot prompt + Chandra-словарь, без tile mode.
        if _is_dense_grsh_singleline(excerpt, block_text):
            return BLOCK_TYPE_DENSE_GRSH
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
    # GRSH single-shot профиль (attempt_05): крупнее картинка, больше токенов,
    # БЕЗ continuation (continuation-merge на этом классе листов ломается —
    # модель не склеивает фрагмент, salvage откатывается к началу).
    BLOCK_TYPE_DENSE_GRSH: {
        "render_target_long_side": _env_int("STAGE_COMPARISON_GRSH_RENDER_LONG_SIDE", 2200),
        "image_input_long_side": _env_int("STAGE_COMPARISON_GRSH_IMAGE_LONG_SIDE", 2000),
        "max_tokens": _env_int("STAGE_COMPARISON_GRSH_MAX_TOKENS", 9000),
        "max_continuations": _env_int("STAGE_COMPARISON_GRSH_MAX_CONTINUATIONS", 0),
        "prompt_version": PROMPT_VERSION_GRSH,
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


# ─── r1: фиксированная доменная схема (flag-gated, default OFF) ────────────
#
# Проблема: generic-схема Qwen переменной длины — отсутствующее поле молча
# выпадает, и Opus не отличает «убрали поле» от «Qwen не описал». Доменный слой
# навязывает фиксированный набор слотов с явным «не указано» для отсутствующих,
# ОДИНАКОВЫЙ для обеих сторон. По умолчанию ВЫКЛЮЧЕН: prompt/cache/поведение
# идентичны прежним. Включается STAGE_COMPARISON_DOMAIN_FIELDS_ENABLED=true
# (после включения нужен re-enrichment пары — cache-key меняется на v6).

_DOMAIN_FIELD_ABSENT = "не указано"

# Фиксированные доменные слоты по block_type. Сейчас покрыты схемы электрики
# (однолинейные); список расширяется добавлением ключей.
DOMAIN_FIXED_SLOTS: dict[str, list[str]] = {
    BLOCK_TYPE_SCHEME: [
        "feeders", "main_breakers", "sectional", "metering",
        "compensation", "cts", "busbars", "earthing", "notes",
    ],
    BLOCK_TYPE_DENSE_SCHEME: [
        "feeders", "main_breakers", "sectional", "metering",
        "compensation", "cts", "busbars", "earthing", "notes",
    ],
    BLOCK_TYPE_DENSE_GRSH: [
        "feeders", "main_breakers", "sectional", "metering",
        "compensation", "cts", "busbars", "earthing", "notes",
    ],
}

_DOMAIN_FIELDS_PROMPT_SUFFIX = """

ДОПОЛНИТЕЛЬНО — ФИКСИРОВАННЫЕ ДОМЕННЫЕ ПОЛЯ (электрические схемы):
Добавь в JSON объект "domain_fields" со ВСЕМИ перечисленными ключами, даже если
поля нет на чертеже — это нужно, чтобы отличить «поля нет» от «не описано».
Правила:
  - заполняй ТОЛЬКО тем, что реально видно (как и для остального JSON);
  - если поля/раздела на схеме нет или не читается — поставь строку "не указано"
    (НЕ выдумывай, НЕ достраивай ряды, НЕ копируй из соседних блоков);
  - значение слота — короткая строка ИЛИ список коротких строк (видимые
    значения/маркировки).
Ключи domain_fields:
  "feeders"        — отходящие линии/потребители (имя + номинал/сечение, если видно);
  "main_breakers"  — вводные/главные автоматы (обозначение + номинал);
  "sectional"      — секционный аппарат / АВР;
  "metering"       — учёт (счётчики, ТТ-учёт);
  "compensation"   — компенсация реактивной мощности (АУКРМ/УКМ, kvar);
  "cts"            — трансформаторы тока (коэффициенты);
  "busbars"        — шины/шинопровод (материал, сечение);
  "earthing"       — заземление/ГЗШ/ДСУП;
  "notes"          — примечания.
Пример: "domain_fields": {"feeders": ["ЩР-1а 5х10"], "compensation": "не указано", ...}
"""


def _domain_fields_enabled() -> bool:
    return (_os.environ.get("STAGE_COMPARISON_DOMAIN_FIELDS_ENABLED") or "").strip().lower() in (
        "1", "true", "yes", "on")


def _coerce_domain_fields(payload: dict, block_type: str) -> dict:
    """Гарантировать фиксированный набор слотов в payload['domain_fields'].

    Детерминированно (не доверяя полноте модели) проставляет недостающие/пустые
    слоты = «не указано». No-op, если флаг выключен или block_type не схемный.
    """
    if not _domain_fields_enabled() or not isinstance(payload, dict):
        return payload
    slots = DOMAIN_FIXED_SLOTS.get(block_type)
    if not slots:
        return payload
    df = payload.get("domain_fields")
    if not isinstance(df, dict):
        df = {}
    for slot in slots:
        v = df.get(slot)
        empty = (
            v is None
            or (isinstance(v, str) and not v.strip())
            or (isinstance(v, (list, dict)) and not v)
        )
        if empty:
            df[slot] = _DOMAIN_FIELD_ABSENT
    payload["domain_fields"] = df
    return payload


# ─── Chandra OCR anchor extractor (GRSH vocabulary) ───────────────────────
#
# Извлекает из исходного Chandra-MD блока БУКВАЛЬНЫЕ OCR-якоря: labels,
# equipment, cables, ratings, loads, connection hints, raw_tokens. Это
# СЛОВАРЬ, а не полное описание — Qwen строит структуру/связи, опираясь на
# словарь. Перенесено из controlled-эксперимента (exp_lib.extract_chandra_anchors).
# OCR Chandra часто путает кириллическую Н и латинскую H/F в суффиксе HF.

_CHANDRA_ANCHOR_RE: dict[str, "re.Pattern[str]"] = {
    # panels / switchboards / inputs (labels)
    "panel_grsh":   re.compile(r"\b[12]?ГРЩ\d?(?:[ -](?:РП|ПСВ|ВП|КУ)\d?)?\b"),
    "panel_vru":    re.compile(r"\bВРУ(?:-[А-Яа-я]{1,4}|[-\s]?[0-9а]{1,2})\b"),
    "panel_shu":    re.compile(r"\b[12]?Ш?ЩУ[-.\s]?(?:[А-Яа-я]{1,4}|\d{1,2})\b|\bШУ[-.]?[А-Яа-я]{1,4}\b"),
    "panel_tp":     re.compile(r"\bТП\d{1,2}\b"),
    "panel_t":      re.compile(r"\bТ\d\b"),
    "panel_shchr":  re.compile(r"\bЩРа?-\d\b"),
    # equipment
    "qf":           re.compile(r"\b[12]?QF[D]?\d{1,2}\b"),
    "qs":           re.compile(r"\bQS\d?\b"),
    "wh":           re.compile(r"\bWh\d?\b"),
    "merk":         re.compile(r"Меркурий\s?\d{3}(?:\.\d+)?"),
    "ttk":          re.compile(r"\b[12]?ТТ\d?(?:\.\.\.\d?ТТ\d)?\s?\d{0,4}(?:/5)?\b"),
    "ukrm":         re.compile(r"\b(?:АУКРМ|АКВРМ|УКРМ|АУКРМ-\d|АУКРМ №?\d)\b"),
    "avr":          re.compile(r"\bАВР\b"),
    "vn":           re.compile(r"\bВН\s?\d{2}\b"),
    # cables — OCR mixes Cyrillic Н and Latin H/F in the HF suffix
    "cable":        re.compile(r"(?:К?ППГнг|ПуГПнг)\(А\)-(?:FR)?[НHнh][FFфf][\s\-]*(?:\d?х?\(?\d?х?\d+(?:[.,]\d+)?\)?(?:\s?мм²?)?)?"),
    # ratings
    "current":      re.compile(r"\b\d{2,4}\s?А\b"),
    "voltage":      re.compile(r"\b\d{3}(?:/\d{3})?\s?В\b"),
    "ka":           re.compile(r"\b\d{1,3}\s?кА\b"),
    "kva":          re.compile(r"\b\d{3,4}\s?кВА\b"),
    # loads
    "power_kw":     re.compile(r"\b(?:Py|Pp|Рр|Pр|Ру|Рp)\s?=?\s?\d+[.,]?\d*\s?кВт"),
    "current_calc": re.compile(r"\b(?:Iр|Iрасч|Ip)\.?\s?=?\s?\d+[.,]?\d*\s?А"),
    "cosf":         re.compile(r"\bcos[fφ]\s?=?\s?\d[.,]\d+"),
    # connection hints (literal line names)
    "feeder_line":  re.compile(r"\b[12]ГРЩ-[А-ЯA-Z0-9.\-]+"),
    "input_line":   re.compile(r"Ввод\s?[№]?\d(?:\s?к\s?ТП\d)?|Ввод\s?\d\s?к\s?ТП\d"),
    "busbar":       re.compile(r"Шинопровод[^\n]{0,40}"),
}


def _uniq_anchors(seq: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for s in seq:
        s = re.sub(r"\s+", " ", s).strip()
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def extract_chandra_anchors(raw_text: str) -> dict[str, list[str]]:
    """Извлечь буквальные OCR-якоря из Chandra-MD блока (СЛОВАРЬ, не описание).

    Возвращает dict с ключами labels / equipment / cables / ratings / loads /
    connections_hint / raw_tokens. Используется для GRSH-режима: словарь
    инжектится в prompt и применяется в validation (Chandra grounding).
    """
    t = raw_text or ""
    labels: list[str] = []
    for key in ("panel_grsh", "panel_vru", "panel_shu", "panel_tp", "panel_t", "panel_shchr"):
        labels += _CHANDRA_ANCHOR_RE[key].findall(t)
    equipment: list[str] = []
    for key in ("qf", "qs", "wh", "merk", "ttk", "ukrm", "avr", "vn"):
        equipment += _CHANDRA_ANCHOR_RE[key].findall(t)
    cables = _CHANDRA_ANCHOR_RE["cable"].findall(t)
    ratings: list[str] = []
    for key in ("current", "voltage", "ka", "kva"):
        ratings += _CHANDRA_ANCHOR_RE[key].findall(t)
    loads: list[str] = []
    for key in ("power_kw", "current_calc", "cosf"):
        loads += _CHANDRA_ANCHOR_RE[key].findall(t)
    conn: list[str] = []
    for key in ("feeder_line", "input_line", "busbar"):
        conn += _CHANDRA_ANCHOR_RE[key].findall(t)
    raw_tokens = re.findall(r"[A-ZА-Я0-9][A-Za-zА-Яа-я0-9().\-/]{1,}", t)
    return {
        "labels": _uniq_anchors(labels),
        "equipment": _uniq_anchors(equipment),
        "cables": _uniq_anchors(cables),
        "ratings": _uniq_anchors(ratings),
        "loads": _uniq_anchors(loads),
        "connections_hint": _uniq_anchors(conn),
        "raw_tokens": _uniq_anchors(raw_tokens),
    }


def build_grsh_anchor_vocab_block(chandra_raw: str) -> str:
    """Сформировать текст СЛОВАРЯ Chandra-OCR для инъекции в GRSH prompt.

    Пустой/без якорей Chandra → пустая строка (vocab не добавляется, fail-soft).
    """
    anchors = extract_chandra_anchors(chandra_raw or "")
    keep = ("labels", "equipment", "cables", "ratings", "connections_hint")
    lines = [
        "СЛОВАРЬ Chandra-OCR (буквальные надписи с ЭТОГО чертежа — РЕФЕРЕНС, "
        "не полное описание; строй структуру/связи, опираясь на эти маркировки):"
    ]
    has_any = False
    for k in keep:
        vals = anchors.get(k) or []
        if vals:
            has_any = True
            lines.append(f"- {k}: {json.dumps(vals, ensure_ascii=False)}")
    if not has_any:
        return ""
    return "\n".join(lines)


def get_prompt_for_block_type(
    block_type: str, chandra_raw: Optional[str] = None,
) -> tuple[str, str]:
    """Вернуть (prompt_text, prompt_version) для заданного block_type.

    Используется enrich_side() — каждый блок может уйти в разный prompt.
    Для GRSH-блоков (`dense_grsh_singleline`) при наличии `chandra_raw`
    инжектится словарь буквальных OCR-якорей ПЕРЕД prompt'ом.
    """
    cfg = get_block_type_config(block_type)
    version = str(cfg.get("prompt_version") or PROMPT_VERSION_GENERAL)

    if version == PROMPT_VERSION_GRSH:
        prompt = QWEN_GRSH_SINGLELINE_PROMPT
        vocab = build_grsh_anchor_vocab_block(chandra_raw) if chandra_raw else ""
        if vocab:
            prompt = vocab + "\n\n" + prompt
        if _domain_fields_enabled() and block_type in DOMAIN_FIXED_SLOTS:
            return prompt + _DOMAIN_FIELDS_PROMPT_SUFFIX, PROMPT_VERSION_GRSH_DOMAIN
        return prompt, PROMPT_VERSION_GRSH

    if version == PROMPT_VERSION_SCHEME:
        if _domain_fields_enabled() and block_type in DOMAIN_FIXED_SLOTS:
            return (
                QWEN_SCHEME_DIFF_ANCHORS_PROMPT + _DOMAIN_FIELDS_PROMPT_SUFFIX,
                PROMPT_VERSION_SCHEME_DOMAIN,
            )
        return QWEN_SCHEME_DIFF_ANCHORS_PROMPT, PROMPT_VERSION_SCHEME
    return QWEN_IMAGE_DESCRIPTION_PROMPT, PROMPT_VERSION_GENERAL


# ─── block-PDF source (crop_url) для всех image-блоков (flag-gated, OFF) ───
#
# Архитектура: для любого image/imagine-блока приоритетный источник —
# block-PDF из `crop_url` (по факту есть всегда). Перед Qwen-рендером
# извлекаем его текст-слой (pdfplumber_text) как словарь буквальных значений,
# затем рендерим этот же block-PDF в PNG. Page-crop — только fallback.
# По умолчанию ВЫКЛЮЧЕНО (STAGE_COMPARISON_BLOCK_PDF_SOURCE_ENABLED=false):
# поведение идентично прежнему (render_block_crop по странице). Helper-функции
# самого block_pdf_source доступны всем режимам независимо от флага.


def block_pdf_source_enabled() -> bool:
    return _env_bool("STAGE_COMPARISON_BLOCK_PDF_SOURCE_ENABLED", False)


def resolve_block_pdf_for_enrichment(
    session_id: str, pair_id: str, side: str, side_block: Optional[dict],
    *, render_target_long_side: int,
) -> Optional[dict]:
    """Приоритетный block-PDF путь для одного блока (fail-soft).

    Возвращает dict {image_path, text_layer_text, text_layer_usable, ocr_anchors,
    diagnostics} либо None (caller остаётся на page-crop). Любая ошибка → None.
    """
    if not isinstance(side_block, dict):
        return None
    raw = side_block.get("raw") if isinstance(side_block.get("raw"), dict) else side_block
    if not (raw.get("crop_url") or raw.get("image_file") or raw.get("pdfplumber_text")):
        return None
    try:
        bid = str(side_block.get("id") or "block")
        cache_dir = paths_mod.text_enrichment_cache_dir(session_id, pair_id) / "block_pdf" / side
        src = block_pdf_source_mod.resolve_block_pdf_source(side_block, cache_dir=cache_dir)
        text_layer = block_pdf_source_mod.extract_block_text_layer(
            src.pdf_path, result_json_text=raw.get("pdfplumber_text"))
        rendered = None
        if src.ok and src.pdf_path is not None:
            out_png = cache_dir / f"{block_pdf_source_mod._safe_block_id(bid)}_{render_target_long_side}.png"
            rendered = block_pdf_source_mod.render_block_pdf(
                src.pdf_path, long_side=int(render_target_long_side), out_path=out_png)
        anchors = block_pdf_source_mod.build_ocr_literal_anchors(text_layer) if text_layer.usable else {"tokens": []}
        diag = block_pdf_source_mod.build_block_source_diagnostics(src, text_layer, rendered)
        return {
            "image_path": (rendered.png_path if (rendered and rendered.ok) else None),
            "text_layer_text": text_layer.text if text_layer.usable else None,
            "text_layer_usable": text_layer.usable,
            "ocr_anchors": anchors.get("tokens", []),
            "diagnostics": diag,
        }
    except Exception:  # noqa: BLE001 — block-PDF путь не должен валить enrich
        logger.debug("resolve_block_pdf_for_enrichment failed; staying on page-crop", exc_info=True)
        return None


# ─── Контур B: tiled GRSH feeder extraction (flag-gated, default OFF) ──────
#
# Для плотного ГРЩ/ВРУ single-shot Qwen сжимает схему в бедный текст. Режим:
# block-PDF (crop_url) → текст-слой (words+bbox) → high-res render → tiles
# (concurrency=1) → per-tile Qwen feeder-JSON (tile-local OCR vocab) →
# детерминированный merge + recall. Qwen зовётся реально (это живой путь),
# но локализован в этом helper'е; любая ошибка → None → fallback на single-shot.


async def _run_grsh_feeder_extraction_for_block(
    session_id: str, pair_id: str, side: str, side_block: Optional[dict],
    *, cfg: "graphic_local_mod.LocalGraphicLLMConfig",
) -> Optional[dict]:
    """Прогнать tiled feeder extraction для одного GRSH-блока (fail-soft).

    Возвращает {"desc_payload": <renderable>, "diagnostics": <dict>} или None
    (caller остаётся на single-shot v7 GRSH).
    """
    if not isinstance(side_block, dict):
        return None
    try:
        from dataclasses import replace as _dc_replace
        gcfg = grsh_feeder_mod.load_grsh_feeder_config()
        raw = side_block.get("raw") if isinstance(side_block.get("raw"), dict) else side_block
        bid = str(side_block.get("id") or "block")
        cache_dir = paths_mod.text_enrichment_cache_dir(session_id, pair_id) / "grsh_feeder" / side

        src = block_pdf_source_mod.resolve_block_pdf_source(side_block, cache_dir=cache_dir)
        if not (src.ok and src.pdf_path is not None):
            return None  # нет block-PDF → single-shot

        # текст-слой С КООРДИНАТАМИ (PyMuPDF words) для tile-local vocabulary
        tl = block_pdf_source_mod.extract_block_text_layer(src.pdf_path, result_json_text=None)
        if not tl.usable and raw.get("pdfplumber_text"):
            tl = block_pdf_source_mod.extract_block_text_layer(
                src.pdf_path, result_json_text=raw.get("pdfplumber_text"))
        anchors = grsh_feeder_mod.extract_text_layer_anchors(tl.text)

        render_png = cache_dir / f"{block_pdf_source_mod._safe_block_id(bid)}_{gcfg.render_long_side}.png"
        rb = block_pdf_source_mod.render_block_pdf(
            src.pdf_path, long_side=int(gcfg.render_long_side), out_path=render_png)
        if not rb.ok or rb.png_path is None:
            return None
        render_bytes = Path(rb.png_path).read_bytes()

        # размер страницы block-PDF в точках (для проекции word bbox → render px)
        page_pt = (float(rb.width), float(rb.height))
        try:
            import fitz
            _doc = fitz.open(str(src.pdf_path))
            page_pt = (float(_doc[0].rect.width), float(_doc[0].rect.height))
            _doc.close()
        except Exception:  # noqa: BLE001
            pass

        grsh_call_cfg = _dc_replace(
            cfg, max_tokens=int(gcfg.max_tokens), image_long_side=int(gcfg.tile_long_side),
            max_continuations=0)

        async def _describe(png_bytes: bytes, prompt: str) -> dict:
            url = graphic_local_mod._png_bytes_to_data_url(png_bytes)
            res, content = await graphic_local_mod._describe_image_once(
                img_url=url, prompt=prompt, cfg=grsh_call_cfg,
                use_model=grsh_call_cfg.model, primary_model=grsh_call_cfg.model,
                fallback_used=False)
            parsed = res.parsed
            if parsed is None and content:
                parsed = graphic_local_mod.salvage_partial_json(content)
            return {"parsed": parsed, "status": res.status}

        tile_results = await grsh_feeder_mod.extract_feeders_for_block(
            render_png_bytes=render_bytes, text_layer_words=tl.words, pdf_page_size=page_pt,
            describe_fn=_describe, cfg=gcfg, image_size=(rb.width, rb.height))
        merged = grsh_feeder_mod.merge_tile_feeders(tile_results, anchors, cfg=gcfg)
        diag = merged["diagnostics"]
        table = grsh_feeder_mod.render_feeder_table_md(merged)
        # Универсальный structured output профиля electrical_singleline/grsh.
        structured = graphic_profiles_mod.build_electrical_singleline_structured(
            merged, subtype="grsh")

        desc_payload = {
            "status": "done",
            "image_kind": "scheme",
            "graphic_profile": graphic_profiles_mod.ELECTRICAL_SINGLELINE,
            "profile_subtype": "grsh",
            "structured": structured,
            "grsh_feeder_table": table,
            "grsh_feeders": merged["feeders"],
            "grsh_connections": merged["connections"],
            "summary": (
                f"GRSH пофидерное извлечение (tiled): {diag['feeders_extracted']} фидеров, "
                f"designation_recall={diag['designation_recall']}, "
                f"consumer_recall={diag['consumer_recall']}, "
                f"connections={diag['connections_count']}, "
                f"искусственных рядов={len(diag['rejected_artificial_series'])}."
            ),
            "coverage_notes": (
                f"tiles={tile_results['n_tiles']}, recall_ok={diag['meets_min_recall']}, "
                f"text_layer={tl.source}"
            ),
        }
        diagnostics = {
            "method": "grsh_feeder_tiled",
            "graphic_profile": graphic_profiles_mod.ELECTRICAL_SINGLELINE,
            "profile_subtype": "grsh",
            "block_source": src.source,
            "text_layer_source": tl.source,
            "n_tiles": tile_results["n_tiles"],
            "field_state_audit": graphic_profiles_mod.structured_field_state_audit(structured),
        }
        diagnostics.update({f"grsh_{k}": v for k, v in diag.items()})
        return {"desc_payload": desc_payload, "diagnostics": diagnostics}
    except Exception:  # noqa: BLE001 — никогда не валим enrich
        logger.warning("GRSH feeder extraction failed; falling back to single-shot",
                       exc_info=True)
        return None


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


def _format_grsh_sections(lines: list[str], desc_payload: dict) -> None:
    """Отрендерить GRSH-секции (verified/ocr_only/visual/rejected + структура).

    rejected_anchors помечены «НЕ evidence», чтобы Opus не строил по ним diff.
    """
    va = desc_payload.get("verified_anchors")
    if isinstance(va, dict):
        verified_labels = [str(x).strip() for x in (va.get("labels") or []) if str(x).strip()]
        if verified_labels:
            lines.append("GRSH_VERIFIED_ANCHORS — подтверждены словарём Chandra (evidence):")
            for x in verified_labels:
                lines.append(f"- {x}")
            lines.append("")
        for title, key in (("Кабели", "cables"), ("Номиналы", "ratings"),
                           ("Оборудование", "equipment")):
            arr = [str(x).strip() for x in (va.get(key) or []) if str(x).strip()]
            if arr:
                lines.append(f"GRSH_VERIFIED — {title}:")
                for x in arr:
                    lines.append(f"- {x}")
                lines.append("")

    ocr_only = [str(x).strip() for x in (desc_payload.get("ocr_only_anchors") or []) if str(x).strip()]
    if ocr_only:
        lines.append("GRSH_OCR_ONLY_ANCHORS — есть в Chandra-OCR, Qwen не описал (слабое evidence):")
        for x in ocr_only:
            lines.append(f"- {x}")
        lines.append("")

    visual = [_grsh_anchor_text(x).strip() for x in (desc_payload.get("visual_unverified_anchors") or [])]
    visual = [x for x in visual if x]
    if visual:
        lines.append("GRSH_VISUAL_UNVERIFIED — видно на картинке, нет в Chandra (НЕ evidence в одиночку):")
        for x in visual:
            lines.append(f"- {x}")
        lines.append("")

    rejected = [_grsh_anchor_text(x).strip() for x in (desc_payload.get("rejected_anchors") or [])]
    rejected = [x for x in rejected if x]
    if rejected:
        lines.append("GRSH_REJECTED — отброшены как достроенный ряд / нечитаемое (НЕ evidence, НЕ использовать):")
        for x in rejected:
            lines.append(f"- {x}")
        lines.append("")

    panels = desc_payload.get("panels")
    if isinstance(panels, list) and panels:
        lines.append("GRSH_PANELS — секции ГРЩ / вводы:")
        for p in panels:
            if not isinstance(p, dict):
                continue
            name = str(p.get("name") or "").strip()
            fed = str(p.get("fed_from") or "").strip()
            inp = p.get("input")
            busbar = str((inp or {}).get("busbar") or "").strip() if isinstance(inp, dict) else ""
            seg = f"- {name or '?'}"
            if fed:
                seg += f" ← {fed}"
            if busbar:
                seg += f" [{busbar}]"
            lines.append(seg)
        lines.append("")

    circuits = desc_payload.get("circuits")
    if isinstance(circuits, list) and circuits:
        lines.append("GRSH_CIRCUITS — отходящие линии (источник → автомат → кабель → потребитель):")
        for c in circuits:
            if not isinstance(c, dict):
                continue
            src = str(c.get("source") or "").strip()
            br = str(c.get("breaker") or "").strip()
            cab = str(c.get("cable") or "").strip()
            cons = str(c.get("consumer") or "").strip()
            chain = " → ".join(x for x in (src, br, cab, cons) if x)
            if chain:
                lines.append(f"- {chain}")
        lines.append("")

    connections = desc_payload.get("connections")
    if isinstance(connections, list) and connections:
        lines.append("GRSH_CONNECTIONS — связи:")
        for c in connections:
            if not isinstance(c, dict):
                continue
            f_ = str(c.get("from") or "?").strip()
            t_ = str(c.get("to") or "?").strip()
            via = str(c.get("via") or c.get("relation") or "").strip()
            seg = f"- {f_} → {t_}"
            if via:
                seg += f" ({via})"
            lines.append(seg)
        lines.append("")

    uncertainties = [_grsh_anchor_text(x).strip() for x in (desc_payload.get("uncertainties") or [])]
    uncertainties = [x for x in uncertainties if x]
    if uncertainties:
        lines.append("GRSH_UNCERTAIN — нечитаемое / сомнительное:")
        for x in uncertainties:
            lines.append(f"- {x}")
        lines.append("")


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

    # ── DOMAIN_FIELDS (r1): фиксированные доменные слоты с явным «не указано» ──
    # Рендерятся ВСЕГДА, когда присутствуют (включая «не указано»), чтобы Opus
    # механически отличал «поля нет» от «не описано». Пустые слоты не скрываются.
    domain_fields = desc_payload.get("domain_fields")
    if isinstance(domain_fields, dict) and domain_fields:
        lines.append("DOMAIN_FIELDS — фиксированные доменные поля (отсутствующее = «не указано»):")
        for slot, val in domain_fields.items():
            if isinstance(val, list):
                rendered = "; ".join(str(x) for x in val if str(x).strip()) or _DOMAIN_FIELD_ABSENT
            elif isinstance(val, dict):
                rendered = json.dumps(val, ensure_ascii=False) if val else _DOMAIN_FIELD_ABSENT
            else:
                rendered = str(val).strip() or _DOMAIN_FIELD_ABSENT
            lines.append(f"- {slot}: {rendered}")
        lines.append("")

    # ── GRSH (dense_grsh_singleline): verified / ocr_only / visual_unverified /
    #    rejected якоря + panels/circuits/connections. rejected помечены явно
    #    «НЕ evidence», чтобы Opus не использовал их как доказательство. ──
    if is_grsh_payload(desc_payload):
        _format_grsh_sections(lines, desc_payload)

    # ── GRSH_FEEDERS (контур B): пофидерная таблица из tiled-извлечения ──
    # Идёт ДО summary — Opus читает буквальные пофидерные строки раньше прозы.
    grsh_feeder_table = desc_payload.get("grsh_feeder_table")
    if isinstance(grsh_feeder_table, str) and grsh_feeder_table.strip():
        lines.append(grsh_feeder_table.strip())
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


# ─── GRSH validation / dedup layer ────────────────────────────────────────
#
# Qwen на GRSH-схеме сам не партиционирует бакеты (копирует реальные labels
# и в verified, и в visual_unverified, и в rejected) и иногда достраивает
# числовые ряды. Этот детерминированный слой:
#   * сверяет каждую verified-маркировку со словарём Chandra (grounding);
#   * отбрасывает достроенные ряды (ТП3…ТП22, ГРЩ1-РП1-8…15) в rejected_anchors;
#   * negrounded не-серии → visual_unverified_anchors;
#   * не теряет важные Chandra-only маркировки → ocr_only_anchors;
#   * делает бакеты взаимоисключающими (verified > ocr_only > visual_unverified
#     > rejected > uncertainties).
# Перенесено из controlled-эксперимента (exp_qwen.dedup_buckets / detect_artificial_series).

# Серия = «префикс + хвостовое число»: ТП1, ВРУ2, ГРЩ1-РП1-8 → (key, num).
_GRSH_SERIES_RE = re.compile(r"^(?P<key>.*?)[-.\s]?(?P<num>\d{1,3})$")


def _norm_grsh(s: Any) -> str:
    return re.sub(r"\s+", "", (str(s) if s is not None else "").lower().replace("ё", "е"))


def _grsh_anchor_text(x: Any) -> str:
    if isinstance(x, str):
        return x
    if isinstance(x, dict):
        for k in ("raw_text", "label", "text", "possible_text", "name"):
            if x.get(k):
                return str(x[k])
    return str(x)


def is_grsh_payload(payload: Any) -> bool:
    """True, если payload похож на GRSH-описание (verified_anchors+бакеты)."""
    if not isinstance(payload, dict):
        return False
    if isinstance(payload.get("verified_anchors"), dict):
        return True
    return (payload.get("sheet_kind") == "electrical_single_line"
            and ("panels" in payload or "circuits" in payload))


def _grsh_parse_series(raw: str) -> tuple[Optional[str], Optional[int]]:
    """Разобрать маркировку на (series_key, seq_num). ВРУа/ГРЩ → (None, None)."""
    raw = (raw or "").strip()
    m = _GRSH_SERIES_RE.match(raw)
    if not m:
        return None, None
    key = (m.group("key") or "").strip(" -.")
    if not key:
        return None, None
    try:
        num = int(m.group("num"))
    except (TypeError, ValueError):
        return None, None
    return key, num


def _grsh_rejected_series_labels(label_texts: list[str], chandra_raw: str) -> set[str]:
    """Нормализованные тексты labels, которые надо отбросить как достроенный ряд.

    Серия отбрасывается (только её absent-члены), если в ней ≥4 номеров и
    отсутствующих в Chandra членов «подавляюще много» (явная экстраполяция).
    Члены, реально присутствующие в Chandra, НЕ трогаются.
    """
    chandra_norm = _norm_grsh(chandra_raw)
    groups: dict[str, list[tuple[int, str]]] = {}
    for raw in label_texts:
        key, num = _grsh_parse_series(raw)
        if key is None or num is None:
            continue
        groups.setdefault(_norm_grsh(key), []).append((num, raw))
    reject: set[str] = set()
    for _key, members in groups.items():
        nums = sorted({n for n, _ in members})
        if len(nums) < 4:
            continue
        present = [(n, r) for (n, r) in members if _norm_grsh(r) in chandra_norm]
        absent = [(n, r) for (n, r) in members if _norm_grsh(r) not in chandra_norm]
        # Экстраполированный ряд: много отсутствующих И их явно больше присутствующих.
        if len(absent) >= 3 or (len(absent) >= 2 and len(absent) > len(present)):
            for _n, r in absent:
                reject.add(_norm_grsh(r))
    return reject


def detect_chandra_artificial_series(labels: list[Any], chandra_raw: str) -> list[str]:
    """Найти достроенные ряды в Qwen labels с номерами, отсутствующими в Chandra.

    Возвращает человекочитаемые маркеры (для diagnostics/тестов): напр.
    ``"hallucinated_TP_series: ТП[3, 4, ...]"`` или ``"artificial_sequence:..."``.
    """
    issues: list[str] = []
    chandra = chandra_raw or ""
    flat = [_grsh_anchor_text(x) for x in (labels or [])]
    joined = " ".join(flat)
    tp_nums = sorted({int(m) for m in re.findall(r"ТП(\d{1,2})", joined)})
    bad_tp = [n for n in tp_nums if n >= 3 and f"ТП{n}" not in chandra]
    if bad_tp:
        issues.append(f"hallucinated_TP_series: ТП{bad_tp}")
    vru_nums = sorted({int(m) for m in re.findall(r"ВРУ[-\s]?(\d{1,2})\b", joined)})
    bad_vru = [n for n in vru_nums if n >= 5 and f"ВРУ{n}" not in chandra and f"ВРУ-{n}" not in chandra]
    if bad_vru:
        issues.append(f"hallucinated_VRU_series: ВРУ{bad_vru}")
    reject = _grsh_rejected_series_labels(flat, chandra_raw)
    if reject:
        issues.append(f"artificial_sequence_rejected:{len(reject)}")
    return issues


def _grsh_chandra_anchor_set(chandra_raw: str) -> tuple[set[str], list[str]]:
    """(нормализованный набор всех Chandra-якорей, упорядоченные важные labels)."""
    anchors = extract_chandra_anchors(chandra_raw or "")
    important = list(anchors.get("labels") or []) + list(anchors.get("equipment") or [])
    norm_set: set[str] = set()
    for bucket in ("labels", "equipment", "cables", "ratings", "raw_tokens"):
        for v in anchors.get(bucket) or []:
            n = _norm_grsh(v)
            if n:
                norm_set.add(n)
    # Полный нормализованный текст для substring-match (надёжнее токенов).
    norm_set.add(_norm_grsh(chandra_raw))
    return norm_set, important


def _dedup_anchor_buckets(payload: dict) -> dict:
    """Сделать бакеты якорей взаимоисключающими.

    Приоритет: verified > ocr_only > visual_unverified > rejected > uncertainties.
    Каждая нормализованная маркировка остаётся только в самом приоритетном бакете.
    """
    if not isinstance(payload, dict):
        return payload
    verified = {
        _norm_grsh(_grsh_anchor_text(x))
        for x in ((payload.get("verified_anchors") or {}).get("labels") or [])
    }
    seen = set(verified)
    report: dict[str, list[str]] = {}

    def _filter(bucket_key: str):
        out: list[Any] = []
        removed: list[str] = []
        for x in (payload.get(bucket_key) or []):
            n = _norm_grsh(_grsh_anchor_text(x))
            if not n:
                continue
            if n in seen:
                removed.append(_grsh_anchor_text(x))
            else:
                seen.add(n)
                out.append(x)
        payload[bucket_key] = out
        if removed:
            report[bucket_key] = removed

    for bk in ("ocr_only_anchors", "visual_unverified_anchors", "rejected_anchors", "uncertainties"):
        _filter(bk)
    payload.setdefault("_grsh_validation", {})["dedup_removed"] = report
    return payload


def apply_grsh_validation(payload: dict, chandra_raw: str) -> dict:
    """Провалидировать GRSH-описание против словаря Chandra (детерминированно).

    Мутирует и возвращает payload:
      * verified_anchors.labels → только Chandra-grounded;
      * достроенные ряды → rejected_anchors;
      * negrounded не-серии → visual_unverified_anchors;
      * важные Chandra-only маркировки → ocr_only_anchors;
      * бакеты взаимоисключающие (dedup).
    Fail-soft: на не-GRSH payload или ошибке — возвращает payload как есть.
    """
    if not is_grsh_payload(payload):
        return payload
    try:
        va = payload.get("verified_anchors")
        if not isinstance(va, dict):
            va = {}
            payload["verified_anchors"] = va
        verified_labels = [x for x in (va.get("labels") or [])]
        visual = list(payload.get("visual_unverified_anchors") or [])
        rejected = list(payload.get("rejected_anchors") or [])
        uncertain_norm = {
            _norm_grsh(_grsh_anchor_text(u)) for u in (payload.get("uncertainties") or [])
        }

        chandra_set, important_chandra = _grsh_chandra_anchor_set(chandra_raw)
        all_label_texts = [_grsh_anchor_text(x) for x in verified_labels] + \
                          [_grsh_anchor_text(x) for x in visual]
        reject_set = _grsh_rejected_series_labels(all_label_texts, chandra_raw)

        kept_verified: list[Any] = []
        moved_visual: list[Any] = []
        moved_rejected: list[Any] = []
        for lab in verified_labels:
            txt = _grsh_anchor_text(lab)
            n = _norm_grsh(txt)
            if not n:
                continue
            if n in reject_set:
                moved_rejected.append(txt)
            elif n in chandra_set:
                # Chandra match: verified — даже если модель ещё и в uncertainties.
                kept_verified.append(lab)
            else:
                # negrounded (в т.ч. label, продублированный в uncertainties) → downgrade.
                _ = uncertain_norm  # downgrade одинаков с/без uncertainties-дубля
                moved_visual.append(txt)

        # Существующие visual labels: серии-достройки тоже в rejected.
        kept_visual: list[Any] = list(moved_visual)
        for lab in visual:
            txt = _grsh_anchor_text(lab)
            n = _norm_grsh(txt)
            if not n:
                continue
            if n in reject_set:
                moved_rejected.append(txt)
            else:
                kept_visual.append(lab)

        # ocr_only: важные Chandra-маркировки, не попавшие ни в один бакет Qwen.
        seen_norm = {_norm_grsh(_grsh_anchor_text(x)) for x in kept_verified}
        seen_norm |= {_norm_grsh(_grsh_anchor_text(x)) for x in kept_visual}
        seen_norm |= {_norm_grsh(x) for x in moved_rejected}
        ocr_only: list[str] = []
        for c in important_chandra:
            n = _norm_grsh(c)
            if n and n not in seen_norm:
                seen_norm.add(n)
                ocr_only.append(c)

        va["labels"] = kept_verified
        payload["visual_unverified_anchors"] = kept_visual
        payload["rejected_anchors"] = _uniq_anchors(
            [_grsh_anchor_text(x) for x in rejected] + moved_rejected
        )
        payload["ocr_only_anchors"] = ocr_only

        payload.setdefault("_grsh_validation", {})
        payload["_grsh_validation"].update({
            "chandra_anchor_count": len(chandra_set),
            "verified_count": len(kept_verified),
            "visual_unverified_count": len(kept_visual),
            "rejected_count": len(payload["rejected_anchors"]),
            "ocr_only_count": len(ocr_only),
            "series_rejected": sorted(reject_set),
        })
        payload["_grsh_validated"] = True
        _dedup_anchor_buckets(payload)
    except Exception:  # noqa: BLE001 — validation must never break enrichment
        logger.debug("apply_grsh_validation failed (ignored)", exc_info=True)
    return payload


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
    elif is_grsh_payload(payload):
        # GRSH-форма: detector'ы читают verified_anchors + connections.
        va = payload.get("verified_anchors") or {}
        labels = list(va.get("labels") or [])
        ratings = list(va.get("ratings") or [])
        for c in (payload.get("connections") or []):
            if isinstance(c, dict):
                connections.append({
                    "from_raw": c.get("from"),
                    "to_raw": c.get("to"),
                    "relation": c.get("via") or c.get("relation"),
                })

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


def _extract_grsh_anchors_from_payload(payload: dict) -> dict[str, list[str]]:
    """GRSH-форма → {labels (verified+ocr_only), ratings, connections,
    visual_unverified, rejected}. rejected отдаётся отдельно — НЕ как evidence."""
    out: dict[str, list[str]] = {
        "labels": [], "ratings": [], "connections": [],
        "visual_unverified": [], "rejected": [],
    }
    va = payload.get("verified_anchors") or {}
    if isinstance(va, dict):
        out["labels"].extend(str(x).strip() for x in (va.get("labels") or []) if str(x).strip())
        out["ratings"].extend(str(x).strip() for x in (va.get("ratings") or []) if str(x).strip())
    # ocr_only — Chandra-grounded, считаем verified-уровнем evidence.
    out["labels"].extend(str(x).strip() for x in (payload.get("ocr_only_anchors") or []) if str(x).strip())
    out["visual_unverified"].extend(
        _grsh_anchor_text(x).strip() for x in (payload.get("visual_unverified_anchors") or [])
        if _grsh_anchor_text(x).strip()
    )
    out["rejected"].extend(
        _grsh_anchor_text(x).strip() for x in (payload.get("rejected_anchors") or [])
        if _grsh_anchor_text(x).strip()
    )
    for c in (payload.get("connections") or []):
        if isinstance(c, dict):
            f = str(c.get("from") or "").strip()
            t = str(c.get("to") or "").strip()
            if f or t:
                out["connections"].append(f"{f or '?'} -> {t or '?'}")
    # dedup сохраняя порядок
    for k in out:
        seen: set[str] = set()
        uniq: list[str] = []
        for v in out[k]:
            if v and v not in seen:
                seen.add(v)
                uniq.append(v)
        out[k] = uniq
    return out


def _extract_anchors_from_description(d: dict) -> dict[str, list[str]]:
    """Извлечь labels/ratings/connections из item.description.

    Сначала GRSH-форма (verified_anchors + ocr_only/visual/rejected), затем
    diff_anchors (v5 prompt), иначе fallback на
    visible_text/numeric_parameters/scheme_analysis.nodes/connections от
    v4-блоков. Результат всегда плоский: list[str]. Для GRSH дополнительно
    возвращаются ключи visual_unverified / rejected.
    """
    out = {"labels": [], "ratings": [], "connections": []}
    if not isinstance(d, dict):
        return out
    payload = d.get("description")
    if not isinstance(payload, dict):
        return out

    if is_grsh_payload(payload):
        return _extract_grsh_anchors_from_payload(payload)

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

        # labels / ratings / connections (если есть хотя бы по одной строке).
        # Для GRSH labels = verified+ocr_only (это evidence).
        for section_name, key in (("labels", "labels"), ("ratings", "ratings"), ("connections", "connections")):
            arr = anchors.get(key) or []
            if not arr:
                continue
            lines.append(f"{section_name}:")
            # Ограничиваем размер на блок, чтобы index оставался компактным.
            for v in arr[:30]:
                lines.append(f"- {v}")
            lines.append("")

        # GRSH: visual_unverified и rejected — РАЗДЕЛЬНО. rejected явно помечены
        # «(NOT evidence)» и НЕ должны использоваться Opus как доказательство.
        visual_unverified = anchors.get("visual_unverified") or []
        if visual_unverified:
            lines.append("visual_unverified (weak, not evidence alone):")
            for v in visual_unverified[:30]:
                lines.append(f"- {v}")
            lines.append("")
        rejected = anchors.get("rejected") or []
        if rejected:
            lines.append("rejected (NOT evidence — hallucinated/unreadable, do not use):")
            for v in rejected[:30]:
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


def _atomic_write_text(path: Path, text: str) -> None:
    """Атомарно записать текст: сначала во временный файл, затем replace.

    Гарантирует, что читатели enriched MD никогда не видят полузаписанный
    файл (важно для production left/right_enriched.md, которые читает
    Opus-сравнение)."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


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
        # Синтетический `description` с diff_anchors — чтобы буквальные
        # large-sheet маркировки попали в IMAGE_DIFF_INDEX через общий
        # _extract_anchors_from_description (у large-sheet нет Qwen-описания).
        # build_enriched_md рендерит тело из large_sheet_md (проверка source
        # идёт раньше description), так что на тело это не влияет.
        try:
            anchors = ls_mod.build_large_sheet_diff_anchors(pe)
        except Exception:  # noqa: BLE001 — anchors не должны валить enrich
            logger.debug("large-sheet diff_anchors build failed", exc_info=True)
            anchors = None
        det = pe.get("detection") if isinstance(pe.get("detection"), dict) else {}
        description = {"diff_anchors": anchors} if anchors else None
        if description is not None:
            try:
                description["confidence"] = float(det.get("confidence") or 0.0)
            except (TypeError, ValueError):
                description["confidence"] = 0.0
        out = {
            "status": "done",
            "source": "large_sheet_enrichment",
            "large_sheet": True,
            "large_sheet_md": body,
            "page_enriched_json_path": str(pe_path),
            "page_enriched_md_path": str(md_art),
            "diagnostics": diag,
            "usable_for_diff": True,
        }
        if description is not None:
            out["description"] = description
        return out

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
        # GRSH-блок: словарь Chandra инжектится из исходного MD-текста блока.
        prompt_text, prompt_version_for_block = get_prompt_for_block_type(
            block_type, chandra_raw=mb.text,
        )
        render_target_long_side = int(type_cfg.get("render_target_long_side") or 1200)
        image_input_long_side = int(type_cfg.get("image_input_long_side") or cfg.image_long_side)
        per_call_max_tokens = type_cfg.get("max_tokens")
        per_call_max_continuations = type_cfg.get("max_continuations")
        prompt_family = "scheme" if prompt_version_for_block in _SCHEME_FAMILY_VERSIONS else "general"

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
                prompt_text, prompt_version_for_block = get_prompt_for_block_type(
                    block_type, chandra_raw=mb.text,
                )
                render_target_long_side = int(type_cfg.get("render_target_long_side") or 1200)
                image_input_long_side = int(type_cfg.get("image_input_long_side") or cfg.image_long_side)
                per_call_max_tokens = type_cfg.get("max_tokens")
                per_call_max_continuations = type_cfg.get("max_continuations")
                prompt_family = "scheme" if prompt_version_for_block in _SCHEME_FAMILY_VERSIONS else "general"
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

        # ── block-PDF source (crop_url) preferred path (default OFF) ──
        # Приоритетный источник для image-блока — block-PDF из crop_url:
        # извлекаем текст-слой как словарь буквальных значений и рендерим
        # ИМЕННО block-PDF (page-crop остаётся fallback'ом). Default OFF.
        if (
            block_pdf_source_enabled()
            and resolution.status == "ok"
            and resolution.side_block_id
        ):
            bps_override = resolve_block_pdf_for_enrichment(
                session_id, pair_id, side,
                side_block_by_id.get(resolution.side_block_id),
                render_target_long_side=render_target_long_side,
            )
            if bps_override:
                # block-PDF render предпочтительнее page-crop'а
                if bps_override.get("image_path"):
                    resolution.image_path = Path(bps_override["image_path"])
                # текст-слой block-PDF → словарь буквальных значений в prompt
                tl_text = bps_override.get("text_layer_text")
                if tl_text:
                    if prompt_version_for_block == PROMPT_VERSION_GRSH:
                        prompt_text, _pv_ignored = get_prompt_for_block_type(
                            block_type, chandra_raw=tl_text)
                    else:
                        vocab = bps_override.get("ocr_anchors") or []
                        if vocab:
                            prompt_text = (
                                "OCR_VOCAB (буквальные надписи из текст-слоя block-PDF — "
                                "референс, не считай verified отсутствующие здесь маркировки): "
                                + ", ".join(str(v) for v in vocab[:80])
                                + "\n\n" + prompt_text
                            )
                # диагностика block-PDF источника
                for dk, dv in (bps_override.get("diagnostics") or {}).items():
                    item[dk] = dv

        # ── Graphic Structured Extraction: классификация в профиль ────
        # Универсальный слой: block_type → (profile, subtype). Рабочий extractor
        # сейчас только у electrical_singleline/grsh (контур B). Остальные
        # профили классифицируются, но извлекаются fallback'ом (single-shot).
        graphic_profile_id, graphic_profile_subtype = graphic_profiles_mod.classify_graphic_profile(block_type)
        item["graphic_profile"] = graphic_profile_id
        item["graphic_profile_subtype"] = graphic_profile_subtype
        use_structured = (
            graphic_profiles_mod.graphic_structured_extraction_enabled()
            and graphic_profiles_mod.profile_production_ready(graphic_profile_id, graphic_profile_subtype)
            and resolution.status == "ok"
            and resolution.side_block_id is not None
        )
        _single_shot_prompt_version = prompt_version_for_block  # для fail-soft отката
        if use_structured:
            prompt_version_for_block = PROMPT_VERSION_GRSH_FEEDER
            item["prompt_version"] = prompt_version_for_block
            item["used_prompt_version"] = prompt_version_for_block

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

        # ── Graphic Structured Extraction: run profile extractor (Real call) ─
        # electrical_singleline/grsh → tiled feeder extraction (контур B).
        if use_structured:
            started = time.monotonic()
            grsh_out = await _run_grsh_feeder_extraction_for_block(
                session_id, pair_id, side,
                side_block_by_id.get(resolution.side_block_id), cfg=cfg)
            if grsh_out:
                item["duration_sec"] = round(time.monotonic() - started, 3)
                item["description"] = grsh_out["desc_payload"]
                item["model_used"] = cfg.model
                item["grsh_feeder_extraction"] = grsh_out["diagnostics"]
                recall_ok = bool(grsh_out["diagnostics"].get("grsh_meets_min_recall", True))
                item["usable_for_diff"] = recall_ok
                if not recall_ok:
                    item["warnings"].append("grsh_feeder_recall_below_min")
                item["status"] = "done"
                try:
                    write_cache(session_id, pair_id, cache_key, {
                        "status": "done", "description": item["description"],
                        "model_used": cfg.model, "raw_response_excerpt": "",
                    })
                except Exception:  # noqa: BLE001
                    logger.debug("grsh feeder cache write failed", exc_info=True)
                descriptions.append(item)
                summary.described += 1
                await _notify_progress(_block_idx, mb, item)
                continue
            # grsh_out is None → fail-soft: откатываем prompt-версию к single-shot
            # (контур B не отработал — записываем реально применённую версию).
            item["warnings"].append("grsh_feeder_fallback_to_single_shot")
            prompt_version_for_block = _single_shot_prompt_version
            item["prompt_version"] = prompt_version_for_block
            item["used_prompt_version"] = prompt_version_for_block

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
            _coerce_domain_fields(payload, block_type)  # r1: фикс. слоты (flag-gated)
            if block_type == BLOCK_TYPE_DENSE_GRSH:
                apply_grsh_validation(payload, mb.text)  # Chandra grounding + dedup
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
            _coerce_domain_fields(payload, block_type)  # r1: фикс. слоты (flag-gated)
            if block_type == BLOCK_TYPE_DENSE_GRSH:
                apply_grsh_validation(payload, mb.text)  # Chandra grounding + dedup
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
        #
        # GRSH: tile mode для плотных однолинейных схем ГРЩ РЕинтродуцирует
        # ложные ряды (эксперимент attempt_06). Поэтому для dense_grsh_singleline
        # tiled-retry в обычном production flow НЕ запускается. Разрешён только
        # явным debug-override STAGE_COMPARISON_QWEN_TILE_ALLOW_GRSH=true.
        _grsh_tile_blocked = (
            block_type == BLOCK_TYPE_DENSE_GRSH
            and not _env_bool("STAGE_COMPARISON_QWEN_TILE_ALLOW_GRSH", False)
        )
        if _grsh_tile_blocked and item.get("status") in ("done", "partial", "error", "no_image"):
            if "grsh_tile_retry_skipped" not in item["warnings"]:
                item["warnings"].append("grsh_tile_retry_skipped")
        if (run_model and _retry_cfg.enabled and _retry_cfg.after_main
                and not _grsh_tile_blocked):
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

    # Large-sheet write-gate: компактная large-sheet сводка собирается из уже
    # готового page_enriched.json (Qwen НЕ вызывается). При обычном dry-run
    # (run_model=False, force=False) enriched MD не перезаписывался, поэтому
    # сводка не попадала в production left/right_enriched.md. Если в этом
    # прогоне встроен хотя бы один large-sheet item со status=done — считаем,
    # что содержимое enriched MD могло измениться, и перезаписываем его.
    large_sheet_embedded = any(
        (d.get("source") == "large_sheet_enrichment"
         and (d.get("status") or "").lower() == "done")
        for d in descriptions
    )
    enriched_md_write_reason: Optional[str] = None
    should_write = force or run_model or not md_out.exists()
    if should_write:
        enriched_md_write_reason = (
            "missing" if not md_out.exists() else ("force" if force else "run_model")
        )
    elif large_sheet_embedded:
        # Idempotency / «не переписывать зря»: пишем только если содержимое
        # реально отличается от того, что уже на диске. Повторный прогон с тем
        # же артефактом даст идентичный MD → записи не будет.
        try:
            current_md = md_out.read_text(encoding="utf-8")
        except OSError:
            current_md = None
        if current_md != enriched_md:
            should_write = True
            enriched_md_write_reason = "large_sheet_embedded"

    enriched_md_written = False
    if should_write:
        try:
            _atomic_write_text(md_out, enriched_md)
            summary.enriched_md_path = str(md_out)
            enriched_md_written = True
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
        "large_sheet_embedded": bool(large_sheet_embedded),
        "enriched_md_written": bool(enriched_md_written),
        "enriched_md_write_reason": enriched_md_write_reason,
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
