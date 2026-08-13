"""Реестры плана маршрутизации: провайдеры, способности, роли, условия.

Почему реестр, а не свободные строки.

До 11I контракт задания нёс одну пару «провайдер + способность» на ВЕСЬ
worker-участок, и различить в нём «сильная модель для свода» и «дешёвая для
критика» было нечем: способность была одна (`strong_audit`). Инвентаризация
показала, что фактический прогон одного пресета состоит из вызовов к трём
разным провайдерам, шести классам моделей и четырнадцати ролям — и что две
строки таблицы UI не соответствуют рантайму вовсе.

Свободная строка в такой конструкции — это молчаливая ошибка: опечатка в
способности превратилась бы в «нет записи в политике воркера», а опечатка в
роли — в действие, которое исполнитель тихо не выполнит. Поэтому каждое
измерение плана закрыто кортежем, и всё, чего в кортеже нет, отвергается на
валидации плана, то есть ДО создания задания.

Что здесь НЕТ и быть не может: идентификаторов моделей. Способность —
логический КЛАСС модели («сильная модель аудита»), а какая строка ему
соответствует на конкретном VPS, знает только локальная политика воркера
(`audit_worker/providers/model_policy.py`). Это тот же рубеж I-P5, что и в
`provider_requirement.py`, просто выраженный богаче.
"""
from __future__ import annotations

import re
from typing import Any

# ─── Провайдеры ──────────────────────────────────────────────────────────────
#: Клиент подписки Claude Code (CLI).
PROVIDER_CLAUDE = "claude"
#: Клиент подписки Codex (CLI).
PROVIDER_CODEX = "codex"
#: Внешний платный шлюз. Отдельный провайдер, а НЕ разновидность Codex.
#:
#: Инвентаризация показала, что первая нога этапа 01 идёт в OpenRouter
#: (`gemma_findings_only.DEFAULT_MODEL = "openai/gpt-5.4"`, HTTP-клиент, ключ
#: `OPENROUTER_API_KEY`), а не в Codex CLI. Замаскировать её под Codex значило
#: бы получить воркер, который «совместим» с пресетом, не имея ни ключа, ни
#: канала — и узнать об этом уже в середине оплаченного прогона.
PROVIDER_OPENROUTER = "openrouter"

KNOWN_PROVIDERS: tuple[str, ...] = (PROVIDER_CLAUDE, PROVIDER_CODEX, PROVIDER_OPENROUTER)

# ─── Способности (логические классы моделей) ─────────────────────────────────
#: Сильная модель общего аудита: текст, свод, привязка норм, пересмотр,
#: основная нога оптимизации. На Claude-пути — Opus-класс, на Codex-пути —
#: рабочая модель Codex. Имя СОХРАНЕНО с 11G намеренно: политики воркеров,
#: написанные до 11I, продолжают работать без правки файла.
CAP_STRONG_AUDIT = "strong_audit"
#: Дешёвая/быстрая модель проверки: страж отсутствия, критик оптимизации на
#: Claude-пути. Sonnet-класс. Отдельная способность нужна именно затем, чтобы
#: центр мог заказать «дешёвую», не называя модель.
CAP_CHEAP_REVIEW = "cheap_review"
#: Детектор графического блока, стандартный класс (GPT-5.4-класс).
CAP_BLOCK_DETECTOR = "block_detector"
#: Детектор графического блока, усиленный класс (третья нога ансамбля).
CAP_BLOCK_DETECTOR_STRONG = "block_detector_strong"
#: Судья ансамбля блоков + gap-search одним обращением.
CAP_BLOCK_JUDGE = "block_judge"
#: Визуальное рассуждение по чертежам с повышенным reasoning effort
#: (нога оптимизации, получающая PNG блоков).
CAP_VISUAL_REASONING = "visual_reasoning"

KNOWN_CAPABILITIES: tuple[str, ...] = (
    CAP_STRONG_AUDIT,
    CAP_CHEAP_REVIEW,
    CAP_BLOCK_DETECTOR,
    CAP_BLOCK_DETECTOR_STRONG,
    CAP_BLOCK_JUDGE,
    CAP_VISUAL_REASONING,
)

#: Какие способности вообще осмысленны у какого провайдера. Пара, которой здесь
#: нет, — ошибка компиляции плана, а не «воркер как-нибудь разберётся».
#:
#: Набор описывает ДОСТИЖИМОЕ, а не желаемое. Ни одной пары «на будущее» здесь
#: нет: каждая — обязательство, которое воркер обязан подтвердить ДО выдачи
#: задания. OpenRouter получает не только ногу детектора, потому что `openai/…`
#: сегодня доступен в таблице моделей для текстовых этапов тоже
#: (`config.AVAILABLE_MODELS`, ограничения стоят только на `block_batch` и
#: `optimization`) — и раскладка, где текст идёт на внешний шлюз, обязана
#: требовать от воркера ИМЕННО этот шлюз, а не молча уехать на Codex.
#:
#: Обратное неверно: у Claude ноги детектора блоков нет, потому что
#: `STAGE_MODEL_RESTRICTIONS["block_batch"]` физически не допускает Claude.
PROVIDER_CAPABILITIES: dict[str, tuple[str, ...]] = {
    PROVIDER_CLAUDE: (
        CAP_STRONG_AUDIT,
        CAP_CHEAP_REVIEW,
    ),
    PROVIDER_CODEX: (
        CAP_STRONG_AUDIT,
        CAP_CHEAP_REVIEW,
        CAP_BLOCK_DETECTOR,
        CAP_BLOCK_DETECTOR_STRONG,
        CAP_BLOCK_JUDGE,
        CAP_VISUAL_REASONING,
    ),
    PROVIDER_OPENROUTER: (
        CAP_STRONG_AUDIT,
        CAP_CHEAP_REVIEW,
        CAP_BLOCK_DETECTOR,
    ),
}

# ─── Роли ────────────────────────────────────────────────────────────────────
#: Роль отвечает на вопрос «ЧТО делает это обращение», способность — «модель
#: какого класса нужна». Пара «провайдер + способность» их не заменяет: две
#: codex-ноги этапа 01 отличаются именно ролью и способностью одновременно, а
#: судья от детектора — ролью при совпадающем классе модели.
ROLE_DETECTOR = "detector"
ROLE_JUDGE_GAP_SEARCH = "judge_gap_search"
ROLE_TEXT_AUDIT = "text_audit"
ROLE_MERGE = "merge"
ROLE_TARGETED_DISCIPLINE = "targeted_discipline"
ROLE_TARGETED_DOCNORM = "targeted_docnorm"
ROLE_TARGETED_MARK_SYSTEM = "targeted_mark_system"
ROLE_ABSENCE_GUARD = "absence_guard"
ROLE_NORM_BINDING = "norm_binding"
ROLE_NORM_REVIEW_FINDINGS = "norm_review_findings"
ROLE_NORM_REVIEW_OPTIMIZATION = "norm_review_optimization"
ROLE_OPTIMIZATION_PRIMARY = "optimization_primary"
ROLE_OPTIMIZATION_VISUAL = "optimization_visual"
ROLE_OPTIMIZATION_CRITIC = "optimization_critic"

#: Детерминированные роли. Модель здесь не вызывается НИКОГДА, и это свойство
#: проверяется валидатором: у такого действия не может быть ни провайдера, ни
#: способности, ни reasoning effort.
ROLE_DETECTOR_COMBINE = "detector_combine"
ROLE_STRUCTURAL_CRITIC = "structural_critic"
ROLE_OPTIMIZATION_MERGE = "optimization_merge"
ROLE_DETERMINISTIC_FIX = "deterministic_fix"
ROLE_NORM_PARAGRAPH_VERIFICATION = "norm_paragraph_verification"
ROLE_NORM_REQUOTE = "norm_requote"
ROLE_CRITIC_AUGMENT = "critic_augment"

MODEL_ROLES: tuple[str, ...] = (
    ROLE_DETECTOR,
    ROLE_JUDGE_GAP_SEARCH,
    ROLE_TEXT_AUDIT,
    ROLE_MERGE,
    ROLE_TARGETED_DISCIPLINE,
    ROLE_TARGETED_DOCNORM,
    ROLE_TARGETED_MARK_SYSTEM,
    ROLE_ABSENCE_GUARD,
    ROLE_NORM_BINDING,
    ROLE_NORM_REVIEW_FINDINGS,
    ROLE_NORM_REVIEW_OPTIMIZATION,
    ROLE_OPTIMIZATION_PRIMARY,
    ROLE_OPTIMIZATION_VISUAL,
    ROLE_OPTIMIZATION_CRITIC,
)

DETERMINISTIC_ROLES: tuple[str, ...] = (
    ROLE_DETECTOR_COMBINE,
    ROLE_STRUCTURAL_CRITIC,
    ROLE_OPTIMIZATION_MERGE,
    ROLE_DETERMINISTIC_FIX,
    ROLE_NORM_PARAGRAPH_VERIFICATION,
    ROLE_NORM_REQUOTE,
    ROLE_CRITIC_AUGMENT,
)

KNOWN_ROLES: tuple[str, ...] = MODEL_ROLES + DETERMINISTIC_ROLES

# ─── Вид действия ────────────────────────────────────────────────────────────
KIND_MODEL = "model"
KIND_DETERMINISTIC = "deterministic"
KNOWN_KINDS: tuple[str, ...] = (KIND_MODEL, KIND_DETERMINISTIC)

# ─── Область исполнения ──────────────────────────────────────────────────────
#: Действие исполняется на удалённом воркере.
SCOPE_WORKER = "worker"
#: Действие исполняется на центре — либо потому, что этап центральный
#: (нормативный хвост, E-19), либо потому, что оно идёт уже после возврата
#: результата.
SCOPE_CENTER = "center"
KNOWN_SCOPES: tuple[str, ...] = (SCOPE_WORKER, SCOPE_CENTER)

# ─── Reasoning effort ────────────────────────────────────────────────────────
EFFORT_LOW = "low"
EFFORT_MEDIUM = "medium"
EFFORT_HIGH = "high"
EFFORT_XHIGH = "xhigh"
KNOWN_EFFORTS: tuple[str, ...] = (EFFORT_LOW, EFFORT_MEDIUM, EFFORT_HIGH, EFFORT_XHIGH)

#: Какие провайдеры вообще принимают reasoning effort как параметр вызова.
#:
#: Claude CLI такого параметра не имеет: попытка передать его означала бы либо
#: молчаливое игнорирование (и тогда план врёт о том, что произойдёт), либо
#: мусор в argv. Валидатор отвергает effort у Claude-действия.
EFFORT_CAPABLE_PROVIDERS: tuple[str, ...] = (PROVIDER_CODEX, PROVIDER_OPENROUTER)

# ─── Мультипликативность ─────────────────────────────────────────────────────
#: Один вызов на весь документ.
MULT_PER_DOCUMENT = "per_document"
#: Один вызов на КАЖДЫЙ графический блок, дошедший до этапа 01.
MULT_PER_GRAPHIC_BLOCK = "per_graphic_block"
#: Вызов на чанк входа (текст Codex, страж отсутствия по MD).
MULT_PER_CHUNK = "per_chunk"
#: Вызов на батч целей (привязка пунктов норм — по 25 целей за раз).
MULT_PER_BATCH = "per_batch"
KNOWN_MULTIPLICITIES: tuple[str, ...] = (
    MULT_PER_DOCUMENT,
    MULT_PER_GRAPHIC_BLOCK,
    MULT_PER_CHUNK,
    MULT_PER_BATCH,
)

#: Разрешённые параметры каждой мультипликативности. Свободный словарь здесь
#: означал бы, что оценщик бюджета молча не заметит новый ключ.
MULTIPLICITY_PARAMS: dict[str, tuple[str, ...]] = {
    MULT_PER_DOCUMENT: (),
    MULT_PER_GRAPHIC_BLOCK: (),
    MULT_PER_CHUNK: ("chunk_source", "max_chunks"),
    MULT_PER_BATCH: ("batch_size", "max_rounds", "target_source"),
}

# ─── Условия ─────────────────────────────────────────────────────────────────
#: Условие — ТИПИЗИРОВАННЫЙ идентификатор с параметрами, а не выражение.
#:
#: Строка кода, пришедшая из нагрузки задания и попавшая в `eval`, — это
#: удалённое исполнение произвольного кода на чужом VPS, и никакая песочница не
#: делает такой канал приемлемым. Поэтому условий ровно столько, сколько
#: перечислено, и вычисляет их детерминированный версионированный вычислитель.
COND_ALWAYS = "always"
#: Флаги, замороженные в снимке.
COND_FEATURE_ENABLED = "feature_enabled"
#: Свойства данных, известные УЖЕ при создании задания (дисциплина, наличие MD).
COND_DISCIPLINE_IN = "discipline_in"
#: Свойства, которые станут известны только по ходу прогона.
COND_HAS_ABSENCE_CANDIDATES = "has_absence_candidates"
COND_HAS_CLAUSE_BINDING_TARGETS = "has_clause_binding_targets"
COND_HAS_FINDINGS_NEEDING_REVISION = "has_findings_needing_revision"
COND_HAS_OPTIMIZATION_ARTIFACT = "has_optimization_artifact"
COND_HAS_MD_FILE = "has_md_file"
COND_HAS_CRITIC_ISSUES = "has_critic_issues"
COND_DETECTORS_COMPLETE = "detectors_complete"

KNOWN_CONDITIONS: tuple[str, ...] = (
    COND_ALWAYS,
    COND_FEATURE_ENABLED,
    COND_DISCIPLINE_IN,
    COND_HAS_ABSENCE_CANDIDATES,
    COND_HAS_CLAUSE_BINDING_TARGETS,
    COND_HAS_FINDINGS_NEEDING_REVISION,
    COND_HAS_OPTIMIZATION_ARTIFACT,
    COND_HAS_MD_FILE,
    COND_HAS_CRITIC_ISSUES,
    COND_DETECTORS_COMPLETE,
)

CONDITION_PARAMS: dict[str, tuple[str, ...]] = {
    COND_ALWAYS: (),
    COND_FEATURE_ENABLED: ("flag",),
    COND_DISCIPLINE_IN: ("disciplines",),
    COND_HAS_ABSENCE_CANDIDATES: (),
    COND_HAS_CLAUSE_BINDING_TARGETS: (),
    COND_HAS_FINDINGS_NEEDING_REVISION: (),
    COND_HAS_OPTIMIZATION_ARTIFACT: (),
    COND_HAS_MD_FILE: (),
    COND_HAS_CRITIC_ISSUES: (),
    COND_DETECTORS_COMPLETE: (),
}

#: Условия, ответ на которые известен ЦЕНТРУ в момент создания задания.
#:
#: Разделение нужно совместимости воркера (§25 задания): условие, вычислимое
#: заранее, снимает или подтверждает требование к способности ДО выдачи; всё
#: остальное обязано требоваться заранее — иначе воркер узнает о нехватке
#: провайдера в середине прогона.
RESOLVABLE_AT_CREATION: frozenset[str] = frozenset(
    {COND_ALWAYS, COND_FEATURE_ENABLED, COND_DISCIPLINE_IN}
)

# ─── Флаги, влияющие на маршрутизацию ────────────────────────────────────────
#: Замораживаются в план. Список ЗАКРЫТ: в снимок плана попадает только то, что
#: реально меняет число ног, провайдера, ветку «детерминированно/модель» или
#: набор targeted-проходов. Копировать `.env` целиком нельзя — там секреты.
ROUTING_FEATURE_FLAGS: tuple[str, ...] = (
    "STAGE01_THIRD_LEG_ENABLED",
    "STAGE01_DUAL_REVIEW_ENABLED",
    "STAGE01_DUAL_GAP_SEARCH_ENABLED",
    "STAGE01_PROTECTION_TABLE_CHECK_ENABLED",
    "OPTIMIZATION_CRITIC_DETERMINISTIC",
    "NORM_CLAUSE_BINDING_ENABLED",
    "AUDIT_CODEX_TARGETED_FINDINGS",
    #: Гейт ТРЕТЬЕГО targeted-прохода свода (`alia_mark_system_audit`).
    #: Имя историческое и о проходе ничего не говорит — но именно оно решает,
    #: будет ли на Codex-пути свода два обращения к модели или три
    #: (`codex_targeted_findings.py:721-733`).
    "FINDING_EVIDENCE_OCR_OBSERVER_ENABLED",
    "AUDIT_CODEX_OPTIMIZATION_IMAGES",
    "PIPELINE_VERIFIER_ENABLED",
    "PIPELINE_NORMS_AFTER_MERGE_ENABLED",
    "PIPELINE_BLOCKS_BEFORE_TEXT_ENABLED",
)

#: Глобальные ручки ПРОЦЕССА, меняющие маршрут, но не являющиеся переменными
#: окружения. Их тоже нужно замораживать, и по той же причине.
#:
#: `CLAUDE_DEFAULT_MODEL_CLASS` — снимок `config.get_claude_model()`, глобальной
#: мутабельной ручки, которую переключает `POST /api/audit/model`. Именно она, а
#: не строка таблицы, определяет модель стража отсутствия
#: (`absence_guard.py:272-275`). Ручка не привязана ни к заданию, ни к проекту:
#: оператор, переключивший её между этапами, менял бы модель уже идущего
#: аудита. В снимок кладётся КЛАСС (`strong`/`cheap`), а не идентификатор —
#: точной модели в контракте центра не место.
GLOBAL_CLAUDE_DEFAULT_MODEL_CLASS = "CLAUDE_DEFAULT_MODEL_CLASS"

ROUTING_RUNTIME_GLOBALS: tuple[str, ...] = (GLOBAL_CLAUDE_DEFAULT_MODEL_CLASS,)

#: Классы модели, которыми центр вправе оперировать.
MODEL_CLASS_STRONG = "strong"
MODEL_CLASS_CHEAP = "cheap"
KNOWN_MODEL_CLASSES: tuple[str, ...] = (MODEL_CLASS_STRONG, MODEL_CLASS_CHEAP)

#: Класс модели → способность общего назначения.
CLASS_TO_CAPABILITY: dict[str, str] = {
    MODEL_CLASS_STRONG: CAP_STRONG_AUDIT,
    MODEL_CLASS_CHEAP: CAP_CHEAP_REVIEW,
}

# ─── Запрет точной модели в контракте центра ─────────────────────────────────
#: Строки, похожие на идентификатор модели. Проверяются ВСЕ значения плана.
#:
#: Это не паранойя, а единственный способ удержать инвариант при живой правке:
#: достаточно один раз положить `claude-opus-5` в описание действия «для
#: наглядности», чтобы следующий читатель счёл поле авторитетным и начал
#: выбирать по нему модель. Запрет должен быть машинным.
_EXACT_MODEL_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bclaude-(opus|sonnet|haiku)\b", re.IGNORECASE),
    re.compile(r"\bgpt-\d", re.IGNORECASE),
    re.compile(r"\bcodex/", re.IGNORECASE),
    re.compile(r"\bopenai/", re.IGNORECASE),
    re.compile(r"\bo\d-(mini|preview)\b", re.IGNORECASE),
    re.compile(r"\bgemini-\d", re.IGNORECASE),
    re.compile(r"\bqwen", re.IGNORECASE),
    re.compile(r"\bgemma", re.IGNORECASE),
)


def looks_like_exact_model(value: Any) -> bool:
    """Похоже ли значение на точный идентификатор модели."""
    if not isinstance(value, str):
        return False
    return any(pattern.search(value) for pattern in _EXACT_MODEL_PATTERNS)


def capability_allowed(provider: str, capability: str) -> bool:
    """Осмысленна ли пара «провайдер + способность»."""
    return capability in PROVIDER_CAPABILITIES.get(str(provider), ())


def effort_allowed(provider: str) -> bool:
    """Принимает ли провайдер reasoning effort как параметр вызова."""
    return str(provider) in EFFORT_CAPABLE_PROVIDERS
