"""Транспортная адаптация этапа `findings_merge` под ProviderAdapter (этап 11E).

Разделение то же, что и на 11D у `text_analysis` (§13 задания), но предмет
другой: свод не читает документ, он читает ДВА ГОТОВЫХ АРТЕФАКТА предыдущих
этапов и сводит их замечания в один список.

    A. ИНЖЕНЕРНОЕ СОДЕРЖАНИЕ          B. ТРАНСПОРТНАЯ ОБОЛОЧКА
    роль дисциплины                   «прочитай 02_text_analysis.json»
    кросс-страничная сверка           «прочитай 01_blocks_analysis.json»
    правила дедупликации              «прочитай MD-файл»
    правила объединения               «запиши через Write tool»
    обработка detector_comparison     «не выводи в чат»
    повышение/понижение severity      путь выходного файла
    трассировка источников
    правила sheet/page
    сохранение norm_quote
    JSON-схема
    сами замечания T-NNN и G-NNN

A обязано дойти до модели дословно. B в provider-режиме вредно: адаптер
запускает CLI с `--tools=` и полным `--disallowed-tools`, инструментов нет
вовсе, и инструкция «прочитай файл» адресована тому, чего не существует.

Откуда берётся A. Из УЖЕ БОЕВОГО сборщика
`prompt_builder.build_findings_merge_messages` — того самого, которым сегодня
работает ветка OpenRouter. Он уже читает оба артефакта силами конвейера, уже
вкладывает их inline в user-сообщение и уже снимает CLI-инструкции через
`_clean_template_for_api`. Задача этого модуля — привести его двухсообщенный вид
к одному тексту для stdin и дочистить то, чего сборщик не делает.

Чего этот модуль НЕ делает и почему. Он не переписывает правила свода. 11E —
перенос транспорта, а не улучшение качества merge (§15 задания): любое «как
кажется лучше» здесь означало бы два расходящихся свода — один на центре, другой
на воркере.

Тело payload (user-часть) НЕ зачищается никогда: это данные аудита — замечания
предыдущих этапов со своими `evidence`, `block_id` и координатами. Правка внутри
них была бы искажением входа, который свод обязан обработать дословно.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

from backend.app.pipeline.stages.text_analysis.provider_transport import (
    FILESYSTEM_PLACEHOLDER,
    SEVERITY_SEMANTICS,
    count_absolute_paths,
    split_messages,
    strip_filesystem_references,
)
from backend.app.services.storage.stage_artifacts import (
    BLOCKS_ANALYSIS_FILENAME,
    TEXT_ANALYSIS_FILENAME,
    resolve_existing,
)

__all__ = [
    "FILESYSTEM_PLACEHOLDER",
    "SEVERITY_SEMANTICS",
    "INPUT_DATA_NOTE",
    "TRANSPORT_CONTRACT",
    "REQUIRED_RESULT_FIELDS",
    "FIELD_TYPES",
    "EXPECTED_SEMANTICS",
    "SOFT_RESULT_FIELDS",
    "MergeInputs",
    "MergeInputError",
    "resolve_merge_inputs",
    "build_provider_prompt",
    "input_coverage_report",
    "engineering_markers_present",
    "transport_markers_present",
    "semantic_preservation_report",
    "soft_contract_report",
    "count_absolute_paths",
    "strip_filesystem_references",
]


#: Справка «что подано на вход В ЭТОМ прогоне».
#:
#: Нужна ровно по той же причине, что и на 11D: `_clean_template_for_api`
#: удаляет СТРОКИ, содержащие «Read tool», а в шаблоне свода это строки-заголовки
#: пунктов входных данных («**Text analysis** — READ via Read tool: …»,
#: «**Block analysis** — READ via Read tool: …», «**MD file** (for context) — READ
#: via Read tool: …»). Заголовки уезжают целиком, вместе с инженерной
#: квалификацией источников, и от секции `## Input Data` остаются висячие
#: подпункты про `text_findings` и `block_analyses` без указания, откуда они.
#: Для ветки OpenRouter это давняя данность; на боевом этапе воркера — потеря.
#:
#: Пункт про MD говорит правду и ничего не обещает. Заменяемая CLI-ветка давала
#: модели путь к MD и надеялась на `Read`; ветка API его не вкладывает вовсе. В
#: provider-режиме файла нет — и молчать об этом нельзя: свод, считающий, что
#: документ «где-то есть», начал бы ссылаться на то, чего не видел.
#:
#: Пункт про нормативную базу сформулирован как ЯВНОЕ ОПРОВЕРЖЕНИЕ строки
#: шаблона, а не как отдельное утверждение рядом с ней. Причина найдена
#: состязательным разбором до боевого вызова: шаблон свода говорит
#: «**Normative reference** — provided in system context», и это неправда ни на
#: одной ветке — в EN-шаблоне есть только `{DISCIPLINE_ROLE}`, справочник норм
#: не вкладывается ни ветке API, ни CLI-ветке (пре-существующий дефект, KI-2).
#: Пока справка стояла рядом молча, промпт содержал два соседних
#: противоположных утверждения об одном и том же, причём ложное — ПОЗЖЕ, то
#: есть с большей рецентностью. Правка касается транспортной правды о составе
#: входа, а не правил свода: продовый шаблон не тронут ни на символ.
INPUT_DATA_NOTE = """## Input Data (this run)

1. **Text analysis output** — inlined below under `## 02_text_analysis.json`.
   Contains `text_findings` (T-001…), `normative_refs_found`, `project_params`
   and, when the text stage ran after blocks, `items_verified_from_blocks`.
2. **Block analysis output** — inlined below under `## 01_blocks_analysis.json`.
   Contains `block_analyses` with their findings (G-001…), block `label`,
   `page`, `sheet`, coverage metadata and `highlight_regions`.
3. **Project MD file — NOT available in this run.** Use the two artifacts above
   as the only source of facts. Do not claim to have read the source document,
   and do not invent block labels or sheet numbers that are not in them.
4. **Normative reference — NOT available in this run.** The task specification
   below states that it is "provided in system context". In this run it is not:
   no normative reference file is attached. Keep the norm references that the
   source T-/G-findings already carry, and do not invent clause numbers or cite
   a reference you were not given.

The section below is the full task specification. Where it refers to reading a
file, the content is already inlined as described above."""


#: Транспортный контракт, заменяющий блок B.
#:
#: Оговорка «TOOL ACCESS ONLY» перенесена с 11D.1 сознательно и здесь значима не
#: меньше: свод обязан уметь сказать «на чертеже нет того, что заявлено в
#: спецификации». Запрет, прочитанный шире, чем «у тебя нет инструментов»,
#: заглушил бы ровно этот класс замечаний — а он в шаге 1 шаблона прямо
#: предписан («Is there equipment on drawings missing from the specification?»).
#:
#: Про полноту сказано отдельно и намеренно: единственный настоящий риск
#: транспорта — молчаливая потеря входного замечания. Дедупликация и объединение
#: — законная работа этапа; «не заметил» — нет.
TRANSPORT_CONTRACT = """## OUTPUT TRANSPORT

You have NO tools in this run: no file reading, no file writing, no shell, no
web search. Do not ask for files and do not complain that you cannot open one.
This restriction is about TOOL ACCESS ONLY. A defect that the project
documentation itself fails to state is a normal audit finding and must be
reported as usual.

Everything this task needs is already inside this message: the complete text
analysis output and the complete block analysis output are inlined above.

Every T-NNN and G-NNN finding in them must be accounted for: merged into an
F-finding, deduplicated into one, or deliberately dropped by the rules above.
Silently skipping an input finding is not one of the options.

Return your result as ONE JSON object in your reply, matching the schema above.
- no markdown code fences,
- no explanation before or after the JSON,
- no summary text.

The pipeline itself parses your reply, validates it and persists it to the
output file. Your only job in this run is the consolidation and the JSON."""


#: Куда ставится блок смысла severity — лестница якорей по убыванию точности.
#:
#: Первый якорь — `### Finding Fields`: именно там шаблон свода перечисляет пять
#: значений шкалы. Определения обязаны стоять рядом с перечнем, а не в хвосте.
#:
#: Почему блок вообще нужен на этом этапе (а не только на 01). Свод не просто
#: ВЫБИРАЕТ severity — он его МЕНЯЕТ: «Severity elevation» поднимает уровень при
#: подтверждении чертежом, «Severity reduction» понижает при опровержении, а
#: объединение сводит замечания разных уровней в одно. Делать это без определений
#: значений — то же самое, что было вскрыто на 11D.1: единственная копия
#: определений живёт в корневом CLAUDE.md, который ProviderAdapter намеренно
#: подавляет (`--setting-sources=`). Константа импортируется из модуля этапа 01 —
#: второй экземпляр тех же формулировок разошёлся бы с первым.
_SEVERITY_ANCHORS: tuple[str, ...] = (
    "### Finding Fields",
    "## Output JSON Schema",
    "## Rules",
)


#: Поля, отсутствие которых делает артефакт непригодным дальше по конвейеру.
#:
#: Только `findings`. Боевой раннер этапа читает из артефакта ровно его
#: (`len(...get("findings", []))`), и все post-merge проходы (провенанс, дедуп,
#: нумерация, подписи блоков) работают с этим ключом. `meta` в жёсткую часть не
#: берётся намеренно: `merge_similar_findings` и `apply_phase0_dedup` создают его
#: сами, если модель ключ опустила, — превращать это в отказ этапа значило бы
#: убить исправный свод из-за отсутствующей шапки.
REQUIRED_RESULT_FIELDS: tuple[str, ...] = ("findings",)

FIELD_TYPES: dict[str, Any] = {
    "findings": list,
    "meta": dict,
}

#: Смысловых ожиданий у свода нет. У `text_analysis` их источник был явный —
#: правило платформы «production-аудит принимает только md» и прямая инструкция
#: модели вернуть `text_source: "md"`. У свода ни одного поля с заранее
#: известным ЗНАЧЕНИЕМ контракт не содержит, и придумывать его здесь было бы
#: изобретением требования, которого продовый этап не предъявляет.
EXPECTED_SEMANTICS: dict[str, Any] = {}

#: Поля, чьё отсутствие фиксируется, но НЕ роняет этап.
SOFT_RESULT_FIELDS: tuple[str, ...] = ("meta",)


# ─── Вход этапа ──────────────────────────────────────────────────────────────

class MergeInputError(RuntimeError):
    """Обязательный вход свода отсутствует или нечитаем. Тихой подмены не бывает."""


class MergeInputs:
    """Разрешённые и ПРОЧИТАННЫЕ КОНВЕЙЕРОМ входы свода.

    Класс существует ради одного свойства, которого у боевого сборщика нет:
    `prompt_builder._read_json_file` при отсутствии файла возвращает СТРОКУ
    «(файл … не найден)» и кладёт её в промпт как ни в чём не бывало. На центре
    это давняя данность — свод отработает по одному источнику и запишет результат
    как полноценный. На воркере это оплаченный вызов, который выглядит успешным и
    молча теряет половину аудита. Поэтому вход разрешается ОТДЕЛЬНО и до сборки
    промпта, а нечитаемый вход — отказ этапа (§23 E/F задания).
    """

    __slots__ = ("text_path", "blocks_path", "text_data", "blocks_data")

    def __init__(
        self,
        *,
        text_path: Path,
        blocks_path: Path,
        text_data: dict,
        blocks_data: dict,
    ) -> None:
        self.text_path = text_path
        self.blocks_path = blocks_path
        self.text_data = text_data
        self.blocks_data = blocks_data

    # ── Счётчики и идентификаторы ────────────────────────────────────────────

    @property
    def text_finding_ids(self) -> list[str]:
        return [
            str(item.get("id"))
            for item in (self.text_data.get("text_findings") or [])
            if isinstance(item, dict) and item.get("id")
        ]

    @property
    def block_finding_ids(self) -> list[str]:
        ids: list[str] = []
        for block in self.blocks_data.get("block_analyses") or []:
            if not isinstance(block, dict):
                continue
            for item in block.get("findings") or []:
                if isinstance(item, dict) and item.get("id"):
                    ids.append(str(item["id"]))
        return ids

    @property
    def block_count(self) -> int:
        return len(self.blocks_data.get("block_analyses") or [])

    def as_facts(self) -> dict[str, Any]:
        """Факты о входе — БЕЗ содержимого. Ровно это уезжает в отчёты."""
        text_ids = self.text_finding_ids
        block_ids = self.block_finding_ids
        return {
            "text_analysis": {
                "filename": self.text_path.name,
                "bytes": self.text_path.stat().st_size,
                "sha256": _sha256_file(self.text_path),
                "text_findings": len(text_ids),
                "normative_refs_found": len(
                    self.text_data.get("normative_refs_found") or []
                ),
                "items_verified_from_blocks": len(
                    self.text_data.get("items_verified_from_blocks") or []
                ),
                "stage": self.text_data.get("stage"),
                "text_source": self.text_data.get("text_source"),
            },
            "blocks_analysis": {
                "filename": self.blocks_path.name,
                "bytes": self.blocks_path.stat().st_size,
                "sha256": _sha256_file(self.blocks_path),
                "block_analyses": self.block_count,
                "block_findings": len(block_ids),
                "preliminary_findings": len(
                    self.blocks_data.get("preliminary_findings") or []
                ),
                "stage": self.blocks_data.get("stage"),
            },
            "expected_input_finding_ids": sorted(set(text_ids) | set(block_ids)),
            "text_finding_ids": text_ids,
            "block_finding_ids": block_ids,
        }


def _sha256_file(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def resolve_merge_inputs(output_dir: Path) -> MergeInputs:
    """Найти и прочитать ОБА обязательных входа свода. Отсутствие — исключение.

    Резолв идёт через `resolve_existing`, то есть тем же путём, что и боевой
    сборщик промпта: канон сначала, legacy-имя как fallback. Иначе проект с ещё
    не мигрированными именами файлов проходил бы проверку и падал бы на сборке.
    """
    output_dir = Path(output_dir)
    resolved: dict[str, Path] = {}
    for filename in (TEXT_ANALYSIS_FILENAME, BLOCKS_ANALYSIS_FILENAME):
        path = resolve_existing(output_dir, filename)
        if path is None or not Path(path).is_file():
            raise MergeInputError(
                f"обязательный вход свода отсутствует: {filename} в {output_dir}. "
                "Сборщик промпта подставил бы строку «файл не найден» и вызов "
                "прошёл бы как успешный, потеряв половину аудита"
            )
        resolved[filename] = Path(path)

    payloads: dict[str, dict] = {}
    for filename, path in resolved.items():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise MergeInputError(
                f"обязательный вход свода нечитаем: {path.name} — {exc}"
            ) from None
        if not isinstance(data, dict):
            raise MergeInputError(
                f"обязательный вход свода не является JSON-объектом: {path.name}"
            )
        payloads[filename] = data

    return MergeInputs(
        text_path=resolved[TEXT_ANALYSIS_FILENAME],
        blocks_path=resolved[BLOCKS_ANALYSIS_FILENAME],
        text_data=payloads[TEXT_ANALYSIS_FILENAME],
        blocks_data=payloads[BLOCKS_ANALYSIS_FILENAME],
    )


# ─── Сборка промпта ──────────────────────────────────────────────────────────

def _insert_severity_semantics(system_text: str) -> tuple[str, str]:
    """Поставить блок смысла severity рядом с перечнем значений.

    Возвращает (текст, сработавший якорь). Якорь пишется в карту сборки: если
    шаблон переименуют, отчёт покажет `end_of_instructions`, а не промолчит о
    том, что блок уехал в хвост.
    """
    for anchor in _SEVERITY_ANCHORS:
        if anchor in system_text:
            head, tail = system_text.split(anchor, 1)
            return f"{head}{SEVERITY_SEMANTICS}\n\n{anchor}{tail}", anchor
    return f"{system_text}\n\n{SEVERITY_SEMANTICS}", "end_of_instructions"


def build_provider_prompt(messages: Iterable[dict]) -> dict[str, Any]:
    """Собрать один текст для stdin `claude -p` из боевых messages этапа.

    Возвращает не только промпт, но и КАРТУ сборки: сколько символов пришло из
    инструкций, сколько из полезной нагрузки, сколько путей вычищено. Карта
    уезжает в артефакт прогона — без неё «промпт собран правильно» пришлось бы
    принимать на слово.
    """
    system_raw, payload_text = split_messages(messages)
    system_text, stripped = strip_filesystem_references(system_raw)

    marker = "## Input Data"
    if marker in system_text:
        head, tail = system_text.split(marker, 1)
        system_text = f"{head}{INPUT_DATA_NOTE}\n\n{marker}{tail}"
    else:
        system_text = f"{INPUT_DATA_NOTE}\n\n{system_text}"
    system_text, severity_anchor = _insert_severity_semantics(system_text)

    prompt = (
        f"{system_text}\n\n"
        "===== STAGE OUTPUTS TO CONSOLIDATE (inlined by the pipeline) =====\n\n"
        f"{payload_text}\n\n"
        "===== END OF STAGE OUTPUTS =====\n\n"
        f"{TRANSPORT_CONTRACT}\n"
    )
    return {
        "prompt": prompt,
        # `map` — то, что МОЖНО класть в отчёт. Сам `prompt` содержит замечания
        # по документу заказчика целиком, и место ему только в stdin
        # подпроцесса: отчёт о прогоне уезжает центру в пакете результата.
        "map": {
            "system_chars": len(system_text),
            "payload_chars": len(payload_text),
            "prompt_chars": len(prompt),
            "filesystem_refs_stripped": stripped,
            "absolute_paths_remaining_in_instructions": count_absolute_paths(system_text),
            "input_data_note_applied": INPUT_DATA_NOTE.splitlines()[0] in prompt,
            "transport_contract_applied": "OUTPUT TRANSPORT" in prompt,
            "severity_semantics_applied": SEVERITY_SEMANTICS.splitlines()[0] in prompt,
            "severity_semantics_anchor": severity_anchor,
        },
        "system_chars": len(system_text),
        "payload_chars": len(payload_text),
        "prompt_chars": len(prompt),
        "filesystem_refs_stripped": stripped,
        "absolute_paths_remaining_in_instructions": count_absolute_paths(system_text),
    }


# ─── Полнота входа ДО вызова модели (§11 задания) ────────────────────────────

def input_coverage_report(
    prompt: str, expected_ids: Sequence[str],
) -> dict[str, Any]:
    """Каждое ли входное замечание физически доехало до промпта.

    Проверяется ТРАНСПОРТ, а не работа модели. Сохранять каждое замечание в
    выходе свод не обязан — дедупликация и объединение и есть его смысл. Но
    потерять вход ДО вызова транспорт права не имеет: такую утрату уже не видно
    ни по одному артефакту, потому что выход выглядит нормально.

    Идентификатор ищется в кавычках (`"T-001"`) — так он записан в JSON payload.
    Голая подстрока `T-001` совпала бы с `T-0010` и дала бы ложное «на месте».
    """
    raw = str(prompt or "")
    expected = [str(value) for value in expected_ids if str(value)]
    seen: dict[str, int] = {}
    for value in expected:
        seen[value] = seen.get(value, 0) + 1
    duplicates = sorted(name for name, count in seen.items() if count > 1)

    encoded = sorted({value for value in expected if f'"{value}"' in raw})
    missing = sorted(set(expected) - set(encoded))
    return {
        "expected_count": len(expected),
        "expected_unique_count": len(set(expected)),
        "encoded_count": len(encoded),
        "expected_ids": sorted(set(expected)),
        "encoded_ids": encoded,
        "missing_before_inference": missing,
        "duplicate_input_ids": duplicates,
        "passed": not missing,
    }


# ─── Сверка «инженерное сохранено, транспортное снято» (§13 задания) ─────────

#: Опорные признаки инженерного содержания свода. Сравнивать тексты целиком
#: нельзя: они и обязаны различаться транспортной частью; сравнивать нужно то,
#: что различаться НЕ имеет права.
ENGINEERING_MARKERS: tuple[tuple[str, str], ...] = (
    ("json_schema_findings", '"findings"'),
    ("json_schema_meta", '"by_severity"'),
    ("severity_enum", "РЕКОМЕНДАТЕЛЬНОЕ"),
    ("adjacent_discipline_rule", "ПРОВЕРИТЬ ПО СМЕЖНЫМ"),
    ("cross_page_verification", "Cross-Page and Cross-Block Verification"),
    ("merge_rules", "### Merge Rules"),
    ("dedup_rule", "**Deduplication**"),
    ("severity_elevation_rule", "**Severity elevation**"),
    ("severity_reduction_rule", "**Severity reduction**"),
    ("renumbering_rule", "**Renumbering**"),
    ("detector_comparison_rule", "detector_comparison"),
    ("disputed_rule", "`disputed`"),
    ("coverage_warning_rule", "Coverage Warning Sections"),
    ("verification_processing_rule", "Processing text↔block verification"),
    ("source_tracing_rule", "source_finding_ids"),
    ("block_linkage_rule", "related_block_ids"),
    ("evidence_rule", "evidence_text_refs"),
    ("highlight_rule", "highlight_regions"),
    ("no_internal_ids_rule", "No internal identifiers in human-readable text"),
    ("sheet_page_rule", "Sheet and Page Rules"),
    ("norm_quote_rule", "norm_quote"),
    ("output_language_rule", "OUTPUT LANGUAGE"),
)

#: Признаки транспортной оболочки, которых в provider-промпте быть НЕ должно.
FORBIDDEN_TRANSPORT_MARKERS: tuple[tuple[str, str], ...] = (
    ("read_tool", "Read tool"),
    ("write_tool", "Write tool"),
    ("write_via", "WRITE via"),
    ("read_via", "READ via"),
    ("no_chat_output", "DO NOT output to chat"),
    ("brief_summary", "After writing, output a brief summary"),
)


def engineering_markers_present(text: str) -> dict[str, bool]:
    """Какие опорные признаки инженерной части присутствуют в тексте."""
    raw = str(text or "")
    return {name: (needle in raw) for name, needle in ENGINEERING_MARKERS}


def transport_markers_present(text: str) -> dict[str, bool]:
    """Какие признаки транспортной оболочки присутствуют в тексте."""
    raw = str(text or "")
    return {name: (needle in raw) for name, needle in FORBIDDEN_TRANSPORT_MARKERS}


def semantic_preservation_report(
    *, api_prompt: str, provider_prompt: str
) -> dict[str, Any]:
    """Сверка двух промптов по опорным признакам.

    База сравнения — боевой API-промпт (ветка OpenRouter): он уже прошёл
    `_clean_template_for_api`, поэтому разница показывает вклад ровно 11E, а не
    давно принятое решение о ветке API. Сверка с сырым CLI-шаблоном делается
    отдельно и офлайн (`scripts/verify_11e_prompt_semantics.py`) — там она
    построчная и отвечает на другой вопрос: что потерял переход CLI → API.
    """
    api_markers = engineering_markers_present(api_prompt)
    provider_markers = engineering_markers_present(provider_prompt)
    lost = sorted(
        name for name, present in api_markers.items()
        if present and not provider_markers.get(name)
    )
    transport = transport_markers_present(provider_prompt)
    leaked = sorted(name for name, present in transport.items() if present)
    head = provider_prompt.split("===== STAGE OUTPUTS TO CONSOLIDATE", 1)[0]
    return {
        "engineering_markers_api": api_markers,
        "engineering_markers_provider": provider_markers,
        "engineering_lost": lost,
        "transport_markers_leaked": leaked,
        "absolute_paths_in_provider_instructions": count_absolute_paths(head),
        "passed": not lost and not leaked,
    }


def soft_contract_report(payload: Optional[dict]) -> dict[str, Any]:
    """Мягкая часть контракта: что есть, чего нет. Ничего не роняет."""
    data = payload if isinstance(payload, dict) else {}
    return {
        "present": sorted(name for name in SOFT_RESULT_FIELDS if name in data),
        "missing": sorted(name for name in SOFT_RESULT_FIELDS if name not in data),
    }
