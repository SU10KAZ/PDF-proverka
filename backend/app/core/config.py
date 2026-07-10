"""
Audit Manager — конфигурация приложения (backend).
Пути, константы, настройки.
"""
import json
import os
import shutil
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_csv(name: str, default: list[str]) -> list[str]:
    raw = os.environ.get(name)
    if raw is None:
        return list(default)
    return [part.strip() for part in raw.split(",") if part.strip()]


# Корневая папка проекта (где лежат projects/, prompts/, knowledge_base/, reports/, docs/)
# Приоритет: env AUDIT_ROOT_DIR / AUDIT_BASE_DIR → автодетекция (backend/../../)
def _find_root_dir() -> Path:
    if os.environ.get("AUDIT_ROOT_DIR"):
        return Path(os.environ["AUDIT_ROOT_DIR"]).resolve()
    if os.environ.get("AUDIT_BASE_DIR"):
        return Path(os.environ["AUDIT_BASE_DIR"]).resolve()
    # backend/app/core/config.py → backend/app/core → backend/app → backend → root
    return Path(__file__).resolve().parent.parent.parent.parent


ROOT_DIR = _find_root_dir()
BACKEND_DIR = ROOT_DIR / "backend"
FRONTEND_DIR = ROOT_DIR / "frontend"

# Папки данных (env-переменные для кастомного расположения)
def _data_dir() -> Path:
    if os.environ.get("AUDIT_DATA_DIR"):
        return Path(os.environ["AUDIT_DATA_DIR"]).resolve()
    return ROOT_DIR

DATA_DIR = _data_dir()

# Папка с проектами
PROJECTS_DIR = Path(os.environ["AUDIT_PROJECTS_DIR"]).resolve() if os.environ.get("AUDIT_PROJECTS_DIR") else DATA_DIR / "projects"

# Папка промптов
PROMPTS_DIR = Path(os.environ["AUDIT_PROMPTS_DIR"]).resolve() if os.environ.get("AUDIT_PROMPTS_DIR") else DATA_DIR / "prompts"

# Папка для итоговых отчётов
REPORTS_DIR = DATA_DIR / "отчет"

# Нормативный справочник
NORMS_FILE = ROOT_DIR / "norms_reference.md"
NORMS_PARAGRAPHS_FILE = DATA_DIR / "norms" / "norms_paragraphs.json"

# База знаний (экспертные решения, паттерны)
KNOWLEDGE_BASE_DIR = DATA_DIR / "knowledge_base"
DECISIONS_LOG_FILE = KNOWLEDGE_BASE_DIR / "decisions_log.json"
PATTERNS_FILE = KNOWLEDGE_BASE_DIR / "patterns.json"

# Профили дисциплин
DISCIPLINES_DIR = PROMPTS_DIR / "disciplines"

# Шаблоны задач Claude (RU-мастер в prompts/pipeline/ru/, EN для LLM в prompts/pipeline/en/)
_PIPELINE_RU = PROMPTS_DIR / "pipeline" / "ru"
NORM_VERIFY_TASK_TEMPLATE = _PIPELINE_RU / "norm_verify_task.md"
NORM_FIX_TASK_TEMPLATE = _PIPELINE_RU / "norm_fix_task.md"
NORM_REQUOTE_TASK_TEMPLATE = _PIPELINE_RU / "norm_requote_task.md"
OPTIMIZATION_TASK_TEMPLATE = _PIPELINE_RU / "optimization_task.md"
TEXT_ANALYSIS_TASK_TEMPLATE = _PIPELINE_RU / "text_analysis_task.md"
BLOCK_ANALYSIS_TASK_TEMPLATE = _PIPELINE_RU / "block_analysis_task.md"
FINDINGS_MERGE_TASK_TEMPLATE = _PIPELINE_RU / "findings_merge_task.md"
# findings_critic/corrector-шаблоны удалены: проверку замечаний делает детерминированный
# этап «Верификатор» (stages/findings_verify), не читающий шаблоны задач.
OPTIMIZATION_CRITIC_TASK_TEMPLATE = _PIPELINE_RU / "optimization_critic_task.md"
OPTIMIZATION_CORRECTOR_TASK_TEMPLATE = _PIPELINE_RU / "optimization_corrector_task.md"

# Скрипты — ссылаются на wrapper-файлы в корне (для subprocess-запуска)
PROCESS_PROJECT_SCRIPT = ROOT_DIR / "process_project.py"
BLOCKS_SCRIPT = ROOT_DIR / "blocks.py"          # субкоманды: crop, batches, merge
GEMMA_ENRICH_SCRIPT = ROOT_DIR / "gemma_enrich.py"
NORMS_SCRIPT = ROOT_DIR / "norms" / "_core.py"    # субкоманды: verify, update
GENERATE_EXCEL_SCRIPT = ROOT_DIR / "generate_excel_report.py"
# Legacy aliases (для обратной совместимости)
CROP_BLOCKS_SCRIPT = BLOCKS_SCRIPT
GENERATE_BLOCK_BATCHES_SCRIPT = BLOCKS_SCRIPT
MERGE_BLOCK_RESULTS_SCRIPT = BLOCKS_SCRIPT
GENERATE_BATCHES_SCRIPT = BLOCKS_SCRIPT
MERGE_RESULTS_SCRIPT = BLOCKS_SCRIPT
VERIFY_NORMS_SCRIPT = NORMS_SCRIPT
DEFAULT_TILE_QUALITY = "standard"

# Legacy aliases for tools (используются в claude_runner.py)
TILE_AUDIT_TOOLS = "Read,Write,Grep,Glob,WebSearch,WebFetch"
MAIN_AUDIT_TOOLS = "Read,Write,Edit,Bash,Grep,Glob,WebSearch,WebFetch"
TRIAGE_TOOLS = "Read,Write,Grep,Glob"
SMART_MERGE_TOOLS = "Read,Write,Grep,Glob,WebSearch,WebFetch"

# Legacy aliases for timeouts
CLAUDE_BATCH_TIMEOUT = 600
CLAUDE_AUDIT_TIMEOUT = 3600
CLAUDE_TRIAGE_TIMEOUT = 300
CLAUDE_SMART_MERGE_TIMEOUT = 600

# Название объекта (отображается в заголовке дашборда)
OBJECT_NAME = '213. Мосфильмовская 31А "King&Sons"'

# Порт веб-приложения
APP_HOST = "0.0.0.0"
APP_PORT = 8081

# Claude CLI — на Windows нужен полный путь, т.к. asyncio.create_subprocess_exec
# не находит .cmd файлы по PATH (в отличие от subprocess с shell=True)
def _is_usable_cli(path) -> bool:
    """Путь существует, разрешается (не битый симлинк), является исполняемым файлом."""
    if not path:
        return False
    try:
        resolved = Path(path).resolve(strict=True)
    except (FileNotFoundError, OSError):
        return False
    if not resolved.is_file():
        return False
    if resolved.suffix.lower() in (".cmd", ".bat", ".exe"):
        return True
    return os.access(str(resolved), os.X_OK)


def _scan_vscode_claude() -> str | None:
    """Найти свежайший claude-бинарь среди установленных расширений VSCode."""
    home = Path.home()
    ext_dirs = [
        home / ".vscode-server" / "extensions",
        home / ".vscode" / "extensions",
    ]
    candidates: list[tuple[float, str]] = []
    for ext_dir in ext_dirs:
        if not ext_dir.exists():
            continue
        for d in ext_dir.glob("anthropic.claude-code-*"):
            binary = d / "resources" / "native-binary" / "claude"
            if _is_usable_cli(binary):
                try:
                    mtime = d.stat().st_mtime
                except OSError:
                    mtime = 0.0
                candidates.append((mtime, str(binary)))
    if not candidates:
        return None
    candidates.sort(reverse=True)
    return candidates[0][1]


def _find_claude_cli() -> str:
    """Найти полный путь к Claude CLI (только usable-кандидаты)."""
    found = shutil.which("claude")
    if _is_usable_cli(found):
        return found
    extended_path = os.environ.get("PATH", "") + os.pathsep + str(Path.home() / ".local" / "bin")
    found = shutil.which("claude", path=extended_path)
    if _is_usable_cli(found):
        return found
    found = _scan_vscode_claude()
    if found:
        return found
    linux_paths = [
        Path.home() / ".local" / "bin" / "claude",
        Path("/usr/local/bin/claude"),
    ]
    for p in linux_paths:
        if _is_usable_cli(p):
            return str(p)
    npm_paths = [
        Path.home() / "AppData" / "Roaming" / "npm" / "claude.cmd",
        Path(r"C:\Program Files\nodejs\claude.cmd"),
    ]
    for p in npm_paths:
        if _is_usable_cli(p):
            return str(p)
    return "claude"


CLAUDE_CLI = _find_claude_cli()


def get_claude_cli() -> str:
    """Вернуть рабочий путь к Claude CLI; перерешить, если кешированный битый."""
    global CLAUDE_CLI
    if _is_usable_cli(CLAUDE_CLI):
        return CLAUDE_CLI
    CLAUDE_CLI = _find_claude_cli()
    return CLAUDE_CLI

# Timeout для Claude-сессий (секунды)
CLAUDE_NORM_VERIFY_TIMEOUT = 600
CLAUDE_NORM_FIX_TIMEOUT = 600
CLAUDE_NORM_REQUOTE_TIMEOUT = 600
CLAUDE_OPTIMIZATION_TIMEOUT = 3600
CLAUDE_TEXT_ANALYSIS_TIMEOUT = 1800
CLAUDE_BLOCK_BATCH_CLEAN_CWD = True

CLAUDE_BLOCK_ANALYSIS_TIMEOUT = 1800
CLAUDE_FINDINGS_MERGE_TIMEOUT = 1800
# findings_critic/corrector-таймауты и chunk-size удалены вместе с LLM-критиком
# (детерминированный этап «Верификатор» не чанкует и не запускает агентную сессию).
CLAUDE_OPTIMIZATION_CRITIC_TIMEOUT = 600
CLAUDE_OPTIMIZATION_CORRECTOR_TIMEOUT = 600

# Инструменты для Claude CLI сессий
NORM_VERIFY_TOOLS = (
    "Read,Write,Grep,Glob,"
    "mcp__norms__get_norm_status,"
    "mcp__norms__get_paragraph_json,"
    "mcp__norms__semantic_search_json"
)
TEXT_ANALYSIS_TOOLS = "Read,Write,Grep,Glob,WebSearch,WebFetch"
BLOCK_ANALYSIS_TOOLS = "Read,Write,Grep,Glob,WebSearch,WebFetch"
FINDINGS_MERGE_TOOLS = "Read,Write,Grep,Glob,WebSearch,WebFetch"
OPTIMIZATION_REVIEW_TOOLS = "Read,Write,Grep,Glob"

# Модель Claude CLI (sonnet = экономит лимит All models)
CLAUDE_MODEL_DEFAULT = "claude-sonnet-4-6"
CLAUDE_MODEL_OPTIONS = ["claude-sonnet-4-6", "claude-opus-4-7"]

_current_model = CLAUDE_MODEL_DEFAULT

_stage_models: dict[str, str | None] = {
    "text_analysis":   None,
    "block_batch":     None,
    "findings_merge":  "claude-opus-4-7",
    "findings_critic": None,
    "findings_corrector": None,
    "norm_verify":     None,
    "norm_fix":        None,
    "norm_requote":    None,
    "optimization":    "claude-opus-4-7",
    "optimization_critic": None,
    "optimization_corrector": None,
}

_STAGE_MODEL_DEFAULTS: dict[str, str] = {
    "text_analysis":          "claude-opus-4-7",
    "block_batch":            "openai/gpt-5.4",
    "findings_merge":         "claude-opus-4-7",
    "findings_critic":        "claude-opus-4-7",
    "findings_corrector":     "claude-opus-4-7",
    "norm_verify":            "claude-opus-4-7",
    "norm_fix":               "claude-opus-4-7",
    "norm_requote":           "claude-sonnet-4-6",
    "optimization":           "claude-opus-4-7",
    "optimization_critic":    "claude-sonnet-4-6",
    "optimization_corrector": "claude-sonnet-4-6",
}

# ─── Runtime data directory ─────────────────────────────────────────────────
# Все персистентные JSON-файлы (очереди, объекты, usage) хранятся здесь.
# Env AUDIT_APP_DATA_DIR переопределяет расположение.
def _app_data_dir() -> Path:
    if os.environ.get("AUDIT_APP_DATA_DIR"):
        return Path(os.environ["AUDIT_APP_DATA_DIR"]).resolve()
    return Path(__file__).resolve().parent.parent / "data"


APP_DATA_DIR = _app_data_dir()

# Обратная совместимость: _BACKEND_DATA_DIR → APP_DATA_DIR
_BACKEND_DATA_DIR = APP_DATA_DIR

# Runtime data file paths
BATCH_QUEUE_FILE             = APP_DATA_DIR / "batch_queue.json"
PREPARE_QUEUE_FILE           = APP_DATA_DIR / "prepare_queue.json"
MISSING_NORMS_VAULT_FILE     = APP_DATA_DIR / "missing_norms_vault.json"
OBJECTS_FILE_PATH            = APP_DATA_DIR / "objects.json"
USERS_FILE_PATH              = APP_DATA_DIR / "users.json"
PROJECT_GROUPS_FILE          = APP_DATA_DIR / "project_groups.json"
USAGE_DATA_FILE              = APP_DATA_DIR / "usage_data.json"
USAGE_OFFSETS_FILE           = APP_DATA_DIR / "usage_offsets.json"
STAGE_MODELS_FILE            = APP_DATA_DIR / "stage_models.json"
STAGE_BATCH_MODES_FILE_PATH  = APP_DATA_DIR / "stage_batch_modes.json"
HIDDEN_PROJECTS_FILE         = APP_DATA_DIR / "hidden_projects.json"

# Реестры внешних замечаний (письма заказчика / контрагентов с уже-отправленными findings).
# Ключ файла: {object_id}__{register_id}.json. Хранит структурированный список
# с per-entry match'ами на наши findings + ответ заказчика.
EXTERNAL_REGISTERS_DIR        = APP_DATA_DIR / "external_registers"
EXTERNAL_REGISTER_MATCH_THRESHOLD = 0.8   # confidence >= → auto-mark finding
EXTERNAL_REGISTER_REVIEW_THRESHOLD = 0.5  # confidence in [0.5, 0.8) → needs_review

# ─── Paid API guard ─────────────────────────────────────────────────────────
# Глобальный kill-switch. Default = False (fail-closed): чтобы реально
# пользоваться платными моделями (Stage 02 GPT-5.4, Gemini, OpenRouter etc.),
# нужно явно выставить PAID_API_ENABLED=true. При true pipeline имеет право
# вызывать платные модели автоматически без ручного подтверждения.
PAID_API_ENABLED              = _env_bool("PAID_API_ENABLED", False)

def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError:
        return default

# 0 = без лимита; >0 = жёсткий потолок $/день; превышение → блок до конца суток.
PAID_API_DAILY_LIMIT_USD      = _env_float("PAID_API_DAILY_LIMIT_USD", 0.0)

# Append-only журналы (НЕ truncate'ятся при clear_project_usage).
PAID_COST_EVENTS_FILE         = APP_DATA_DIR / "paid_cost_events.jsonl"
PAID_API_BLOCKED_EVENTS_FILE  = APP_DATA_DIR / "paid_api_blocked_events.jsonl"

_STAGE_MODELS_FILE = STAGE_MODELS_FILE


def _load_stage_model_config() -> dict[str, str]:
    """Загрузить конфиг моделей из файла, fallback на дефолты."""
    config = dict(_STAGE_MODEL_DEFAULTS)
    if _STAGE_MODELS_FILE.exists():
        try:
            with open(_STAGE_MODELS_FILE, "r", encoding="utf-8") as f:
                saved = json.load(f)
            for stage, model in saved.items():
                if stage in config and isinstance(model, str) and model:
                    config[stage] = model
            print(f"[config] Stage models loaded from {_STAGE_MODELS_FILE.name}")
        except Exception as e:
            print(f"[config] Failed to load stage_models.json: {e}")
    return config


def _save_stage_model_config():
    """Сохранить текущий конфиг моделей в файл."""
    try:
        _STAGE_MODELS_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(_STAGE_MODELS_FILE, "w", encoding="utf-8") as f:
            json.dump(STAGE_MODEL_CONFIG, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[config] Failed to save stage_models.json: {e}")


STAGE_MODEL_CONFIG: dict[str, str] = _load_stage_model_config()

_STAGE_BATCH_MODE_DEFAULTS: dict[str, str] = {
    "block_batch": "findings_only_gemma_pair",
}

STAGE_BATCH_MODE_CHOICES: dict[str, list[str]] = {
    "block_batch": ["findings_only_gemma_pair"],
}

_STAGE_BATCH_MODES_FILE = STAGE_BATCH_MODES_FILE_PATH


def _load_stage_batch_modes() -> dict[str, str]:
    config = dict(_STAGE_BATCH_MODE_DEFAULTS)
    if _STAGE_BATCH_MODES_FILE.exists():
        try:
            with open(_STAGE_BATCH_MODES_FILE, "r", encoding="utf-8") as f:
                saved = json.load(f)
            for stage, mode in saved.items():
                if stage in config and mode in STAGE_BATCH_MODE_CHOICES.get(stage, []):
                    config[stage] = mode
            print(f"[config] Stage batch modes loaded from {_STAGE_BATCH_MODES_FILE.name}")
        except Exception as e:
            print(f"[config] Failed to load stage_batch_modes.json: {e}")
    return config


def _save_stage_batch_modes() -> None:
    try:
        _STAGE_BATCH_MODES_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(_STAGE_BATCH_MODES_FILE, "w", encoding="utf-8") as f:
            json.dump(STAGE_BATCH_MODES, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[config] Failed to save stage_batch_modes.json: {e}")


STAGE_BATCH_MODES: dict[str, str] = _load_stage_batch_modes()


def get_stage_batch_mode(stage: str) -> str:
    return STAGE_BATCH_MODES.get(stage, _STAGE_BATCH_MODE_DEFAULTS.get(stage, "findings_only_gemma_pair"))


def set_stage_batch_mode(stage: str, mode: str) -> bool:
    """Возвращает True если режим установлен (валиден), иначе False."""
    if stage not in STAGE_BATCH_MODE_CHOICES:
        return False
    if mode not in STAGE_BATCH_MODE_CHOICES[stage]:
        return False
    STAGE_BATCH_MODES[stage] = mode
    _save_stage_batch_modes()
    return True


# Локальная модель enrichment/анализа. Раньше был хардкод; теперь env-управляемо
# (дефолт = прежнее значение), чтобы при миграции на новый LM Studio задать id новой
# модели (напр. chandra-ocr-2 / qwen36-27b-mtp) без правки кода. LOCAL_LLM_MODELS и
# AVAILABLE_MODELS ниже привязаны к переменной → is_local_llm_model() подхватит новый id.
CHANDRA_GEMMA_MODEL = os.environ.get("CHANDRA_GEMMA_MODEL", "google/gemma-4-26b-a4b")
LOCAL_LLM_MODELS = {CHANDRA_GEMMA_MODEL}

# Codex exec transport for classic agent stages. Stage model IDs use the
# `codex/<model>` namespace so they do not collide with OpenRouter model IDs.
CODEX_MODEL_DEFAULT = os.environ.get("AUDIT_CODEX_MODEL", "gpt-5.4").strip() or "gpt-5.4"
CODEX_STAGE_MODEL_ID = os.environ.get("AUDIT_CODEX_STAGE_MODEL", f"codex/{CODEX_MODEL_DEFAULT}").strip() or f"codex/{CODEX_MODEL_DEFAULT}"
# Диспетчеризация стадий идёт строго по префиксу "codex/" (is_codex_model). Значение
# без префикса (перепутали с AUDIT_CODEX_MODEL) попадало в AVAILABLE_MODELS как
# «Codex exec», но в рантайме молча уходило в OpenRouter-ветку с несуществующим id.
if not CODEX_STAGE_MODEL_ID.startswith("codex/"):
    CODEX_STAGE_MODEL_ID = f"codex/{CODEX_STAGE_MODEL_ID}"

AVAILABLE_MODELS = [
    {"id": "claude-opus-4-7",            "label": "Opus 4.7 (CLI)",        "provider": "claude_cli"},
    {"id": "claude-sonnet-4-6",          "label": "Sonnet (CLI)",           "provider": "claude_cli"},
    {"id": "openai/gpt-5.4",             "label": "GPT-5.4",                "provider": "openrouter"},
    {"id": CODEX_STAGE_MODEL_ID,          "label": "Codex exec",             "provider": "codex_cli"},
    {"id": "google/gemini-3.1-pro-preview", "label": "Gemini 3.1 Pro",      "provider": "openrouter"},
    {"id": CHANDRA_GEMMA_MODEL,           "label": "Gemma 3.6 35B (local)",   "provider": "chandra_local"},
]

STAGE_MODEL_RESTRICTIONS = {
    "block_batch": [
        "openai/gpt-5.4",
    ],
}

CRITICAL_STAGE_MODEL_STAGES: set[str] = {
    "text_analysis",
    "block_batch",
    "findings_merge",
    "findings_critic",
    "findings_corrector",
    "norm_verify",
    "norm_fix",
    "norm_requote",
    "optimization",
    "optimization_critic",
    "optimization_corrector",
}


def validate_stage_model_choice(stage: str, model: str) -> str | None:
    """Return rejection reason for a stage model choice, or None when valid."""
    if stage not in STAGE_MODEL_CONFIG:
        return "unknown stage"
    if not isinstance(model, str) or not model:
        return "model must be a non-empty string"
    valid_model_ids = {m["id"] for m in AVAILABLE_MODELS}
    if model not in valid_model_ids:
        return "unknown model"
    allowed = STAGE_MODEL_RESTRICTIONS.get(stage)
    if allowed and model not in allowed:
        return "model is not allowed for this stage"
    return None


def validate_current_stage_model_config(
    stages: set[str] | None = None,
) -> dict[str, str]:
    """Validate persisted runtime stage model config."""
    target_stages = stages or CRITICAL_STAGE_MODEL_STAGES
    rejected: dict[str, str] = {}
    for stage in sorted(target_stages):
        if stage not in STAGE_MODEL_CONFIG:
            continue
        reason = validate_stage_model_choice(stage, STAGE_MODEL_CONFIG.get(stage, ""))
        if reason:
            rejected[stage] = reason
    return rejected

STAGE_MODEL_HINTS: dict[str, str] = {
    "text_analysis": "Opus CLI рекомендуется. Sonnet допустим.",
    "block_batch": "Production: GPT-5.4 (OpenRouter), findings_only_gemma_pair, single-block. Gemma выполняется отдельным обязательным этапом enrichment.",
    "findings_merge": "Минимум Opus CLI — межблочная сверка требует сильной модели.",
    "findings_critic": "GPT-5.4 оптимален: быстро и дёшево.",
    "findings_corrector": "Минимум Opus CLI. Sonnet не успевает (таймаут). GPT-5.4 — альтернатива.",
    "norm_verify": "Opus CLI обязателен: MCP norms — единственный источник. WebSearch запрещён.",
    "norm_fix": "Opus CLI обязателен: MCP norms для поиска замены. WebSearch запрещён.",
    "optimization": "Opus CLI или GPT-5.4. Gemini находит мало предложений.",
    "optimization_critic": "GPT-5.4 или Sonnet CLI.",
    "optimization_corrector": "GPT-5.4 или Sonnet CLI.",
}

def get_stage_model(stage: str) -> str:
    """Получить модель для этапа из унифицированного конфига."""
    stage_key = stage
    if stage.startswith("block_batch"):
        stage_key = "block_batch"
    return STAGE_MODEL_CONFIG.get(stage_key, "openai/gpt-5.4")

def is_claude_stage(stage: str) -> bool:
    """Проверить, должен ли этап выполняться через Claude CLI."""
    model = get_stage_model(stage)
    return model.startswith("claude-")


def is_codex_model(model: str | None) -> bool:
    """True для модели classic pipeline, запускаемой через `codex exec`."""
    return bool((model or "").strip().startswith("codex/"))


def resolve_codex_model(model: str | None) -> str:
    """Преобразовать stage model id `codex/<model>` в реальный model id Codex CLI."""
    raw = (model or "").strip()
    if raw.startswith("codex/"):
        raw = raw.split("/", 1)[1]
    return raw or CODEX_MODEL_DEFAULT


def is_codex_stage(stage: str) -> bool:
    """Проверить, должен ли этап выполняться через Codex exec."""
    return is_codex_model(get_stage_model(stage))


def is_local_llm_model(model: str) -> bool:
    """True для локальных моделей через Chandra/LM Studio."""
    return model in LOCAL_LLM_MODELS

def get_claude_model() -> str:
    """Модель по умолчанию (для обратной совместимости)."""
    return _current_model

def get_model_for_stage(stage: str) -> str:
    """Модель для конкретного этапа конвейера."""
    stage_key = stage
    if stage.startswith("block_batch"):
        stage_key = "block_batch"
    model = _stage_models.get(stage_key)
    return model if model else _current_model

def set_claude_model(model: str):
    global _current_model
    if model in CLAUDE_MODEL_OPTIONS:
        _current_model = model

def set_stage_model(stage: str, model: str | None):
    """Установить модель для конкретного этапа (None = default)."""
    if model is not None and model not in CLAUDE_MODEL_OPTIONS:
        return
    _stage_models[stage] = model

def get_stage_models() -> dict[str, str | None]:
    """Текущие настройки per-stage моделей."""
    return dict(_stage_models)

MAX_PARALLEL_BATCHES = 2

CLAUDE_BLOCK_BATCH_PARALLELISM_DEFAULT = 3
CLAUDE_BLOCK_BATCH_PARALLELISM_CAP = 3
LOCAL_BLOCK_BATCH_PARALLELISM_DEFAULT = 1


def get_block_batch_parallelism(stage: str = "block_batch", model: str | None = None) -> int:
    """Параллелизм для stage 02 block_batch в зависимости от модели/провайдера."""
    if model is None:
        model = get_stage_model(stage)

    is_claude = isinstance(model, str) and model.startswith("claude-")
    if is_claude:
        value = CLAUDE_BLOCK_BATCH_PARALLELISM_DEFAULT
        env_val = os.environ.get("CLAUDE_BLOCK_BATCH_PARALLELISM")
        if env_val:
            try:
                parsed = int(env_val)
                if parsed >= 1:
                    value = parsed
            except ValueError:
                pass
        return min(max(1, value), CLAUDE_BLOCK_BATCH_PARALLELISM_CAP)
    if is_local_llm_model(model):
        value = LOCAL_BLOCK_BATCH_PARALLELISM_DEFAULT
        env_val = os.environ.get("LOCAL_BLOCK_BATCH_PARALLELISM")
        if env_val:
            try:
                parsed = int(env_val)
                if parsed >= 1:
                    value = parsed
            except ValueError:
                pass
        return max(1, value)
    return MAX_PARALLEL_BATCHES

RATE_LIMIT_THRESHOLD_PCT = 90
RATE_LIMIT_CHECK_INTERVAL = 60
RATE_LIMIT_MAX_WAIT = 5 * 3600
RATE_LIMIT_MAX_RETRIES = 5

ANTHROPIC_PLAN = "Max 20x"
WINDOW_5H_TOKEN_LIMIT = 12_000_000
WEEKLY_TOKEN_LIMIT = 17_000_000
CLAUDE_SESSIONS_DIR = Path.home() / ".claude" / "projects"

WEEKLY_RESET_WEEKDAY = 4    # пятница
WEEKLY_RESET_HOUR_UTC = 16  # 16:00 UTC = 19:00 MSK

SEVERITY_CONFIG = {
    "КРИТИЧЕСКОЕ":        {"color": "#e74c3c", "bg": "#fdecea", "icon": "\U0001f534", "order": 1},
    "ЭКОНОМИЧЕСКОЕ":      {"color": "#e67e22", "bg": "#fef5e7", "icon": "\U0001f7e0", "order": 2},
    "ЭКСПЛУАТАЦИОННОЕ":   {"color": "#f1c40f", "bg": "#fef9e7", "icon": "\U0001f7e1", "order": 3},
    "РЕКОМЕНДАТЕЛЬНОЕ":   {"color": "#3498db", "bg": "#eaf2f8", "icon": "\U0001f535", "order": 4},
    "ПРОВЕРИТЬ ПО СМЕЖНЫМ": {"color": "#95a5a6", "bg": "#f2f3f4", "icon": "⚪", "order": 5},
}

OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
OPENROUTER_SITE_URL = "http://localhost:8081"
OPENROUTER_SITE_NAME = "BIM Audit Pipeline"

def _normalize_local_base_url(url: str) -> str:
    """Нормализовать base URL локального LM Studio.

    Принимает обе формы — `https://host` и `https://host/v1` — и возвращает базу
    БЕЗ хвостового `/v1` (и без хвостового слэша), т.к. код сам добавляет суффиксы
    `/v1/chat/completions`, `/v1/models`, `/api/v1/*`. Оператору не нужно помнить,
    где URL с `/v1`, а где без.
    """
    u = (url or "").strip().rstrip("/")
    if u.endswith("/v1"):
        u = u[:-3].rstrip("/")
    return u


# base URL читаем из CHANDRA_BASE_URL, fallback на LMSTUDIO_BASE_URL (имя из инструкции сервера)
CHANDRA_BASE_URL = _normalize_local_base_url(
    os.environ.get("CHANDRA_BASE_URL") or os.environ.get("LMSTUDIO_BASE_URL", "")
)
CHANDRA_API_BASE_URL = f"{CHANDRA_BASE_URL}/v1" if CHANDRA_BASE_URL else ""
CHANDRA_BASIC_USER = os.environ.get("NGROK_AUTH_USER", "")
CHANDRA_BASIC_PASS = os.environ.get("NGROK_AUTH_PASS", "")
# Тип auth для локального LM Studio: "basic" (ngrok legacy) | "bearer" (новый сервер).
# Дефолт basic → прод/тесты не меняются, пока .env не переключат на bearer.
CHANDRA_AUTH_MODE = os.environ.get("CHANDRA_AUTH_MODE", "basic").strip().lower()
# Bearer-токен нового сервера. Читаем с fallback на LMSTUDIO_API_KEY (имя из инструкции).
# Никогда не логировать.
CHANDRA_BEARER_TOKEN = os.environ.get("CHANDRA_BEARER_TOKEN") or os.environ.get(
    "LMSTUDIO_API_KEY", ""
)
# Транспорт мультимодального/freeform локального пути:
#   "native"            → POST /api/v1/chat  (legacy ngrok Chandra)
#   "openai_completions"→ POST /v1/chat/completions с OpenAI vision (стандартный LM Studio)
CHANDRA_CHAT_TRANSPORT = os.environ.get("CHANDRA_CHAT_TRANSPORT", "native").strip().lower()

LMSTUDIO_AUTO_RELOAD_ENABLED = _env_bool("LMSTUDIO_AUTO_RELOAD_ENABLED", False)
GEMMA_ADAPTIVE_RELOAD_ENABLED = _env_bool("GEMMA_ADAPTIVE_RELOAD_ENABLED", False)
# Value Grounding (усиление предобработки графики): сверка значений gemma с векторным
# текст-слоем (pdfplumber) и фиксация глифовых ошибок (В4.0→В40). Phase 1 — офлайн, 0 токенов.
# OFF по умолчанию: стадия становится no-op (полная обратная совместимость).
BLOCK_VALUE_GROUNDING_ENABLED = _env_bool("BLOCK_VALUE_GROUNDING_ENABLED", False)
# Сохранение экспертных вердиктов при переаудите ТОЙ ЖЕ версии: снапшот решённых
# перед удалением 03_findings.json + детерминированная перепривязка на новые F-ID
# после findings_merge (exact fingerprint → fuzzy; carried_over=True, только пустые
# слоты). Fail-soft: любая ошибка не влияет на пайплайн. ON по умолчанию —
# требование: разметка эксперта не должна теряться из-за перенумерации F-NNN.
VERDICT_PRESERVATION_ENABLED = _env_bool("VERDICT_PRESERVATION_ENABLED", True)
# Shadow-режим: снапшот, матчинг и отчёт verdict_preservation_report.json —
# полные, но ЗАПИСЬ вердиктов в expert_review/decisions_log выключена.
# Для наблюдения за качеством матчинга без влияния на живую разметку.
VERDICT_PRESERVATION_SHADOW = _env_bool("VERDICT_PRESERVATION_SHADOW", False)
# Порядок пост-findings: вывести norm_verify из параллельного блока и запускать его
# ПОСЛЕ финализации findings (Верификатор → debt_control merge/stable-id → нормы).
# Так нормы всегда верифицируются против финальных, стабильных F-ID (убирает
# рассинхронизацию: сейчас нормы крутятся параллельно Верификатору и ДО merge/перенумерации).
# optimization остаётся параллельным (его review по-прежнему ждёт corrector_done).
# OFF по умолчанию → прод-порядок не меняется. Действует ТОЛЬКО на полный аудит
# (_run_ocr_pipeline); resume-путь остаётся на легаси-порядке (параллельные нормы).
PIPELINE_NORMS_AFTER_MERGE_ENABLED = _env_bool("PIPELINE_NORMS_AFTER_MERGE_ENABLED", False)
# Stage 02: для СХЕМНЫХ (однолинейных) блоков подавать в промпт полную rich-разметку графа
# (render_graph_etalon_markdown: расчёты панелей + ТТ + примечания + отходящие линии) вместо
# базового enrichment+page_text. OFF по умолчанию — прод не меняется. Влияет и на реальный
# Stage 02 (call_gpt_for_block), и на превью /blocks/llm-text (чтобы совпадали). Детерминированно,
# без OCR/Qwen. Переключать после замера «до/после» (размер промпта + качество findings).
SINGLELINE_RICH_PROMPT_ENABLED = _env_bool("SINGLELINE_RICH_PROMPT_ENABLED", False)
# OCR-подмена («зеркало»): для ГРАФИЧЕСКИХ блоков с вектор-слоем добавлять в промпт Stage 02
# ТОЧНЫЙ вектор-текст блока (вырезанный по полигону), помеченный как приоритетный над OCR.
# Мотив: Gemma/Chandra-OCR путает значения на CAD-чертежах (замер спеки ЭМ-К1: единственное
# расхождение вектор vs Chandra = OCR-ошибка 3х1.5→3x15). Вектор-слой этих ошибок не делает.
# Бьёт по классу «нейронка не так прочитала графику» (~66% брака). Аддитивно: НЕ удаляет
# enrichment (там структура/сущности), а ДОБАВЛЯет чистый текст-слой для сверки чисел.
# Только где есть вектор-слой (сканы → без изменений, fail-soft). Влияет и на call_gpt_for_block,
# и на превью /blocks/llm-text (чтобы совпадали). OFF по умолчанию — прод не меняется.
MIRROR_OCR_ENABLED = _env_bool("MIRROR_OCR_ENABLED", False)
# Сверка МД↔вектор-слой для ТЕКСТ-блоков на Этапе 01 (text_analysis): аддитивная ВРЕЗКА-подсветка
# в задачу «В MD: X / В вектор: Y» там, где Chandra-OCR разошёлся с точным вектор-слоем PDF
# (замер: ~97% значений совпадают; расхождения = 2 системных OCR-паттерна: HF→НФ, потеря точки).
# Нормализатор гасит стиль (кир/лат, ,/., ², пробел) → подсвечиваем только реальное. МД-файл
# НЕ редактируем (аддитивно в промпт). OFF по умолчанию — прод не меняется. fail-soft.
MD_MIRROR_RECONCILE_ENABLED = _env_bool("MD_MIRROR_RECONCILE_ENABLED", False)
# Роутер источника блока для Stage 02 (решение Андрея 2026-07-07 «вместо Gemma везде сырые
# данные, однолинейки — вектографом»). Единая развилка на блок, ЗАМЕНЯЕТ Gemma-описание в
# промпте (не аддитивно): (1) однолинейка + гейт Вектографа → полный структурированный рендер;
# (2) есть вектор-слой → сырой вектор-текст блока (полигон-клип); (3) скан/растр без слоя → None
# → Gemma+изображение остаются (обязательный fallback). Источник — полигон-клип по
# document_graph (не пустой pdfplumber_text). Когда ON — заменяет ad-hoc инъекции
# SINGLELINE_RICH_PROMPT_ENABLED/MIRROR_OCR_ENABLED (единый авторитет). Влияет на
# call_gpt_for_block и превью /blocks/llm-text. OFF по умолчанию — прод не меняется. fail-soft.
BLOCK_SOURCE_ROUTER_ENABLED = _env_bool("BLOCK_SOURCE_ROUTER_ENABLED", False)
# Пропуск СТАДИИ Gemma для блоков с годным вектор-слоем (оптимизация к роутеру выше). Такие
# блоки роутер и так отдаёт из вектор-слоя на Stage 02 → гонять по ним Gemma незачем (экономит
# токены/часы). Пропущенным блокам ставится синтетический ok-результат с ЧИСТЫМ вектор-текстом в
# enrichment (coverage остаётся "ok", summary валиден, MD получает вектор-текст вместо Gemma-OCR).
# ИНВАРИАНТ БЕЗОПАСНОСТИ: пропуск действует ТОЛЬКО когда BLOCK_SOURCE_ROUTER_ENABLED тоже ON —
# иначе Stage 02 не подаст вектор-текст и блок останется слепым. Общий предикат
# vector_covered_block_ids (тот же порог/клип, что у роутера). Скан/растр без слоя → Gemma как
# обычно. OFF по умолчанию. fail-soft.
GEMMA_SKIP_VECTOR_BLOCKS_ENABLED = _env_bool("GEMMA_SKIP_VECTOR_BLOCKS_ENABLED", False)
# ПОЛНОЕ отключение OCR-прогона стадии Gemma (решение Андрея 2026-07-10: «описание блоков —
# из текст-слоя PDF, Gemma выключить»). Расширяет пропуск выше со «только блоки с годным
# вектор-слоем» на ВСЕ image-блоки: covered-блоки получают синтетический enrichment с чистым
# вектор-текстом (как при GEMMA_SKIP_VECTOR_BLOCKS_ENABLED), сканы/растры без слоя —
# placeholder «OCR отключён, анализируй изображение» (Stage 02 анализирует их по PNG, а не
# скипает как enrichment=None). Стадия становится «сухой»: НИ ОДНОГО обращения к LM Studio
# (ни adaptive reload, ни preflight; CHANDRA_BASE_URL не требуется) — полная независимость
# аудита от ngrok/локальной модели. Кропы 100 DPI и summary (schema v2) пишутся как обычно →
# все гейты/resume/Stage 02 проходят без правок. ИНВАРИАНТ БЕЗОПАСНОСТИ: как и пропуск выше,
# действует ТОЛЬКО при BLOCK_SOURCE_ROUTER_ENABLED=true (иначе Stage 02 не подаст covered-блокам
# вектор-текст). Приоритетнее GEMMA_SKIP_VECTOR_BLOCKS_ENABLED (тот можно не выставлять).
# OFF по умолчанию — прод не меняется.
GEMMA_STAGE_DISABLED = _env_bool("GEMMA_STAGE_DISABLED", False)
# «Вектограф» shadow-режим (observe-only): на стадии gemma_enrichment прогоняет гейт качества
# по image-блокам и пишет _output/vectograf_shadow.json — «какие блоки Вектограф взял бы вместо
# Gemma-описания» + метрики/причины. Поведение пайплайна НЕ меняет; телеметрия для решения о
# реальной замене. Дёшево: не-схемы отсеиваются структурером за мс (PDF не открывается),
# однолинейка ~1.2 с. ON по умолчанию (observe-only), env — kill-switch.
VECTOGRAF_SHADOW_ENABLED = _env_bool("VECTOGRAF_SHADOW_ENABLED", True)
# «Вектограф» bbox-клип: геометрия строит топологию ТОЛЬКО по словам внутри области выделения
# блока (coords_norm из result.json), а не по ВСЕМ словам листа. Раньше build_singleline_graph
# читал get_text("words") со всей страницы → на листе с двумя схемами/таблицей чужие QF/коды/
# колонки «протекали» в топологию блока (замер ЭМ-К1: 3% утечки, но только штамп/рамка — хрупко
# к раскладке). Клип по центру слова в bbox (page-normalized) + margin; fail-soft (bbox нет/битый
# или клип оставил <3 слов при многословном листе → откат на весь лист). ON по умолчанию
# (correctness), env — kill-switch. На полностраничных однолинейках — no-op.
VECTOGRAF_BBOX_CLIP_ENABLED = _env_bool("VECTOGRAF_BBOX_CLIP_ENABLED", True)
# «Вектограф» полигон-текст: не только геометрия, но и ТЕКСТ-разделы графа строятся только по
# тексту ВНУТРИ полигона блока. Строки pdfplumber-текста фильтруются по принадлежности полигону
# (по токенам клипнутых слов, порядок pdfplumber сохраняется — формулы фидеров целы). Примечания
# и таблица ТТ, лежащие в «вырезах» контура, уходят из графа (у них СВОИ text-блоки + MD → Stage 01,
# не теряются), фидеры и расчёты панелей (внутри контура) остаются. Правило «вектограф = только
# текст внутри полигона» (запрос Андрея 2026-07-05). ON по умолчанию; env — kill-switch. Работает
# только при VECTOGRAF_BBOX_CLIP_ENABLED. fail-soft: если фильтр рушит фидеры (<3) — исходный текст.
VECTOGRAF_POLYGON_TEXT_ONLY_ENABLED = _env_bool("VECTOGRAF_POLYGON_TEXT_ONLY_ENABLED", True)
# Дедуп соседних текст-блоков против текст-слоя блока: /blocks/llm-text отдаёт поле
# neighbor_text_blocks {send, dropped} — какие соседние text-блоки той же страницы УЖЕ есть в
# текст-слое блока (не слать повторно) и какие уникальны. Аддитивное поле, разметку не трогает.
# ON по умолчанию (безопасно: только доп. инфо в ответе, поведение Stage 02 не меняется).
NEIGHBOR_TEXT_BLOCKS_ENABLED = _env_bool("NEIGHBOR_TEXT_BLOCKS_ENABLED", True)
# Phase 2 (qwen тайлинг/точечный кроп для блоков без вектор-слоя) — отдельный флаг, дорого/ngrok.
BLOCK_VALUE_GROUNDING_QWEN_ENABLED = _env_bool("BLOCK_VALUE_GROUNDING_QWEN_ENABLED", False)
# Разворот порядка конвейера: блоки (Stage 02, GPT) идут ПЕРЕД текстом (Stage 01, Opus).
# Новый порядок: gemma → block_analysis → block_retry → text_analysis → findings_merge.
# Текст становится финальным синтезатором: читает компактный view 02 (02_blocks_for_text.json)
# и сверяет свои T-замечания с блоками (items_verified_from_blocks) вместо обратной сверки.
# OFF по умолчанию — прод (порядок text→block) не меняется. Выкатка через A/B на реальном проекте.
PIPELINE_BLOCKS_BEFORE_TEXT_ENABLED = _env_bool("PIPELINE_BLOCKS_BEFORE_TEXT_ENABLED", False)
# «Страж отсутствия» (absence guard): анти-ложное правило для замечаний вида
# «нет / не указано / отсутствует». Исследование браков (03.07) показало, что ~32%
# отклонений эксперта — это «данные ЕСТЬ, ИИ не увидел» (на другом листе/в тексте).
# Флаг ON вставляет в текстовый промпт (Stage 01) правило: перед утверждением об
# отсутствии просканировать ВЕСЬ документ и заполнить `absence_checked`; иначе —
# понизить до «ПРОВЕРИТЬ ПО СМЕЖНЫМ». OFF по умолчанию — прод-промпт не меняется.
PIPELINE_ABSENCE_GUARD_ENABLED = _env_bool("PIPELINE_ABSENCE_GUARD_ENABLED", False)
# Этап «Верификатор» (findings_verify) — отдельный этап поверх слитого 03_findings.json:
#   1) детерминированные структурные проверки (перенос из критика: evidence_presence /
#      phantom_block / page_sheet_correct) + консервативный корректор (ничего не удаляет);
#   2) LLM-проверка присутствия («страж отсутствия»): подтверждённо-ложные «нет» → мягко
#      «ПРОВЕРИТЬ ПО СМЕЖНЫМ». Заменил бесполезный LLM-критик (recall 17%).
# Килсвитч: default TRUE («всегда включён» по решению). Поглощает PIPELINE_ABSENCE_GUARD_ENABLED
# (absence-часть работает внутри этапа). LLM-верификатор инъектируем — под замену на локальную модель.
PIPELINE_VERIFIER_ENABLED = _env_bool("PIPELINE_VERIFIER_ENABLED", True)
# (Подсистема Evidence-Verify удалена как мёртвая: флаги EVIDENCE_VERIFY_IN_PIPELINE_ENABLED
# и EV_PRECEDENT_* убраны вместе с ней.)
# Детерминированный корректор оптимизаций. Замер 07-07 (92 проекта): агентный
# optimization_corrector МОЛЧА ТЕРЯЕТ предложения — критик обрывается на больших
# входах (reviews < items), корректор перезаписывает файл только отрецензированной
# частью (ЭО1: 14 → 7, потеряно 7 валидных, включая устранявшее КРИТИЧЕСКОЕ). Плюс
# корректор УДАЛЯЕТ item'ы (41 удаление, 11 — вообще без вердикта). ON → корректор
# заменяется детерминированным: НИЧЕГО не удаляет (понижение/пометка вместо delete),
# неотрецензированные item'ы сохраняются как pass (guard против потери данных). Критик
# пока остаётся агентным (его вердикты по замеру качественные). OFF по умолчанию —
# прод-путь не меняется.
OPTIMIZATION_CRITIC_DETERMINISTIC = _env_bool("OPTIMIZATION_CRITIC_DETERMINISTIC", False)
# Потолок savings_pct для вердикта unrealistic_savings: корректор режет до него,
# сохраняя исходное значение в savings_pct_original + corrector_note.
OPTIMIZATION_SAVINGS_CAP_PCT = int(os.environ.get("OPTIMIZATION_SAVINGS_CAP_PCT", "50"))
# Жёсткий gate Phase 2: только КРУПНЫЕ no-vector блоки (тайлинг оправдан), с cap на прогон.
BLOCK_VALUE_GROUNDING_QWEN_MIN_WIDTH = int(os.environ.get("BLOCK_VALUE_GROUNDING_QWEN_MIN_WIDTH", "6000"))
# Точечный high-res кроп для СРЕДНИХ no-vector блоков (ниже порога тайлинга, но не мелочь).
# 0 = режим crop выключен (только тайлинг крупных). Общий бюджет — MAX_BLOCKS на оба режима.
BLOCK_VALUE_GROUNDING_QWEN_CROP_MIN_WIDTH = int(os.environ.get("BLOCK_VALUE_GROUNDING_QWEN_CROP_MIN_WIDTH", "0"))
BLOCK_VALUE_GROUNDING_QWEN_MAX_BLOCKS = int(os.environ.get("BLOCK_VALUE_GROUNDING_QWEN_MAX_BLOCKS", "12"))
BLOCK_VALUE_GROUNDING_QWEN_MODEL = os.environ.get(
    "BLOCK_VALUE_GROUNDING_QWEN_MODEL",
    os.environ.get("STAGE_COMPARISON_GRAPHIC_LLM_MODEL", "qwen/qwen3.6-35b-a3b"))
# 8192 — практический минимум для 100 DPI image-блока с page_text:
# на 4096 в проде стабильно падают блоки 800×500 с "Context size has been exceeded".
GEMMA_BASE_CONTEXT_LENGTH = int(os.environ.get("GEMMA_BASE_CONTEXT_LENGTH", "8192"))
GEMMA_HIGH_DETAIL_CONTEXT_LENGTH = int(os.environ.get("GEMMA_HIGH_DETAIL_CONTEXT_LENGTH", "16000"))
# #16: параллелизм base 100 DPI прохода Gemma. Default 1 (= прежний хардкод,
# gemma3.6-35b не тянет параллель), но конфигурируемо для будущих моделей/железа.
# High-detail 300 DPI всегда остаётся 1.
GEMMA_BASE_PARALLELISM = max(1, int(os.environ.get("GEMMA_BASE_PARALLELISM", "1") or "1"))
LMSTUDIO_UNLOAD_AFTER_QUEUE = _env_bool("LMSTUDIO_UNLOAD_AFTER_QUEUE", True)
LMSTUDIO_UNLOAD_GRACE_SECONDS = int(os.environ.get("LMSTUDIO_UNLOAD_GRACE_SECONDS", "60"))
LMSTUDIO_UNLOAD_MODEL_ALLOWLIST = _env_csv(
    "LMSTUDIO_UNLOAD_MODEL_ALLOWLIST",
    [
        "gemma/gemma3.5-35b-a3b",
        "gemma/gemma3.6-35b-a3b",
        "google/gemma-4-26b-a4b",
    ],
)
LMSTUDIO_UNLOAD_MODEL_DENYLIST = _env_csv(
    "LMSTUDIO_UNLOAD_MODEL_DENYLIST",
    [
        "chandra-ocr-2",
    ],
)

GEMINI_MODEL = "google/gemini-3.1-pro-preview"
GPT_MODEL = "openai/gpt-5.4"
LOCAL_GEMMA_CONTEXT_LENGTH = int(os.environ.get("LOCAL_GEMMA_CONTEXT_LENGTH", "98304"))
LOCAL_GEMMA_MAX_OUTPUT_TOKENS = int(os.environ.get("LOCAL_GEMMA_MAX_OUTPUT_TOKENS", "8192"))
LOCAL_GEMMA_FINDINGS_MAX_OUTPUT_TOKENS = int(
    os.environ.get("LOCAL_GEMMA_FINDINGS_MAX_OUTPUT_TOKENS", "16384")
)

GEMINI_DIRECT_API_KEY: str = (
    os.environ.get("GEMINI_DIRECT_API_KEY", "")
    or os.environ.get("GOOGLE_API_KEY", "")
)

GEMINI_DIRECT_MODEL_MAP: dict[str, str] = {
    "google/gemini-2.5-flash":       "gemini-2.5-flash",
    "google/gemini-2.5-flash-lite":  "gemini-2.5-flash-lite",
    "google/gemini-3.1-pro-preview": "gemini-3.1-pro-preview",
    "gemini-2.5-flash":              "gemini-2.5-flash",
    "gemini-2.5-flash-lite":         "gemini-2.5-flash-lite",
    "gemini-3.1-pro-preview":        "gemini-3.1-pro-preview",
}

GEMINI_DIRECT_DEFAULT_MODEL: str = os.environ.get("GEMINI_DIRECT_MODEL", "gemini-2.5-flash")
GEMINI_DIRECT_MAX_OUTPUT_TOKENS: int = 65536

STAGE_MODELS_OPENROUTER: dict[str, str] = {
    "text_analysis":          GPT_MODEL,
    "block_batch":            GEMINI_MODEL,
    "findings_merge":         GPT_MODEL,
    "findings_critic":        GPT_MODEL,
    "findings_corrector":     GPT_MODEL,
    "norm_verify":            GPT_MODEL,
    "norm_fix":               GPT_MODEL,
    "optimization":           GEMINI_MODEL,
    "optimization_critic":    GPT_MODEL,
    "optimization_corrector": GPT_MODEL,
}

GEMINI_MAX_OUTPUT_TOKENS = 65536
GPT_MAX_OUTPUT_TOKENS = 128000
DEFAULT_TEMPERATURE = 0.2

GEMINI_MAX_IMAGES = 3600
GPT_MAX_IMAGES = 500
OPENROUTER_MAX_BLOCKS_PER_BATCH = 80

SCHEMAS_DIR = Path(__file__).resolve().parent.parent / "schemas"

DISCUSSION_MODELS = [
    {"id": "claude-cli", "label": "Claude CLI", "provider": "claude_cli"},
    {"id": "openai/gpt-4.1-mini", "label": "GPT-4.1 mini", "provider": "openrouter"},
    {"id": "google/gemini-3.1-pro-preview", "label": "Gemini 3.1 Pro", "provider": "openrouter"},
]
DISCUSSION_DEFAULT_MODEL = "claude-cli"
DISCUSSION_CLI_TIMEOUT = 120
DISCUSSION_MAX_OUTPUT_TOKENS = 16384
DISCUSSION_TEMPERATURE = 0.3
DISCUSSION_TIMEOUT = 120
DISCUSSION_SUMMARY_THRESHOLD = 10

OPENROUTER_STAGE02_HARD_CAP_BLOCKS = 12
OPENROUTER_STAGE02_RAW_BYTE_CAP_KB = 9000
OPENROUTER_STAGE02_TIMEOUT_SEC = 600
OPENROUTER_STAGE02_MAX_OUTPUT_TOKENS = 32768

# ─── Critic v2 post-processing stage (experimental, OFF by default) ──────────
# Read-only re-triage поверх готовых findings. Не заменяет legacy critic,
# не меняет 03_findings_review.json, не запускает LLM по умолчанию.
# Все artifacts пишутся в <project>/_output/<CRITIC_V2_OUTPUT_SUBDIR>/.
# По умолчанию stage НЕ подключён к manager pipeline — запускается только
# через backfill script.
CRITIC_V2_ENABLED = _env_bool("CRITIC_V2_ENABLED", False)
CRITIC_V2_PROFILE = os.environ.get("CRITIC_V2_PROFILE", "conservative").strip() or "conservative"
CRITIC_V2_LLM_ENABLED = _env_bool("CRITIC_V2_LLM_ENABLED", False)
CRITIC_V2_FAILS_PIPELINE = _env_bool("CRITIC_V2_FAILS_PIPELINE", False)
CRITIC_V2_OUTPUT_SUBDIR = (
    os.environ.get("CRITIC_V2_OUTPUT_SUBDIR", "critic_v2").strip() or "critic_v2"
)

# ─── Stage 01 Phase 0 post-merge dedup (OFF by default, safe to enable) ──────
# Post-process applied at the tail of findings_merge after merge_similar_findings.
# On A0 baseline outputs this is a provable no-op (validated 8-case dataset,
# see experiments/md_analysis_comparison/production_preparation/rollout/phase0_rollout.md).
# Adds meta.dedup_report to 03_findings.json. Findings schema is additive.
# Fail-open: any exception → log + skip + return original findings.
STAGE01_DEDUP_ENABLED = _env_bool("STAGE01_DEDUP_ENABLED", False)
try:
    STAGE01_DEDUP_FUZZY_THRESHOLD = float(
        os.environ.get("STAGE01_DEDUP_FUZZY_THRESHOLD", "0.7")
    )
except (TypeError, ValueError):
    STAGE01_DEDUP_FUZZY_THRESHOLD = 0.7
if not (0.0 <= STAGE01_DEDUP_FUZZY_THRESHOLD <= 1.0):
    STAGE01_DEDUP_FUZZY_THRESHOLD = 0.7

# ─── Stage 01 Phase 1 completeness lens (SCAFFOLDING ONLY — all OFF) ─────────
# All vars below are scaffolding for the upcoming completeness-lens rollout
# documented in
#   experiments/md_analysis_comparison/production_preparation/rollout/phase1_rollout.md
# They are NOT yet read by any pipeline code. Adding them here so future
# sub-tasks can wire them up incrementally without touching this module again.
# Every flag defaults OFF; per-doc-type map is empty (no doc_type opted in);
# discipline allowlist is empty; fallback-to-A0 is the safe default (ON).
#
# Production guarantee: with these defaults, behaviour is identical to a
# build without these vars — they are inert until referenced by a runner.

STAGE01_COMPLETENESS_LENS_ENABLED          = _env_bool("STAGE01_COMPLETENESS_LENS_ENABLED", False)
STAGE01_COMPLETENESS_SHADOW                = _env_bool("STAGE01_COMPLETENESS_SHADOW", False)
STAGE01_SHADOW_ON_DISABLED_DOCTYPE         = _env_bool("STAGE01_SHADOW_ON_DISABLED_DOCTYPE", False)
STAGE01_FALLBACK_TO_A0_ON_LENS_FAILURE     = _env_bool("STAGE01_FALLBACK_TO_A0_ON_LENS_FAILURE", True)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError:
        return default


STAGE01_COMPLETENESS_MAX_FINDINGS          = _env_int("STAGE01_COMPLETENESS_MAX_FINDINGS", 10)
STAGE01_COMPLETENESS_MAX_FINDINGS_FULL_RD  = _env_int("STAGE01_COMPLETENESS_MAX_FINDINGS_FULL_RD", 6)

STAGE01_DOCUMENT_TYPE_CONFIDENCE_MIN       = _env_float("STAGE01_DOCUMENT_TYPE_CONFIDENCE_MIN", 0.7)
if not (0.0 <= STAGE01_DOCUMENT_TYPE_CONFIDENCE_MIN <= 1.0):
    STAGE01_DOCUMENT_TYPE_CONFIDENCE_MIN   = 0.7

# Per-doc-type opt-in for the completeness lens. JSON dict; empty default
# means no document_type is opted in, so even if LENS_ENABLED=true the lens
# only runs in shadow mode (controlled by STAGE01_COMPLETENESS_SHADOW).
#
# Expected shape:
#   {"audit_comparison": true, "specification_only": true,
#    "tz_vs_rd": false, "full_rd": false}
def _env_json_dict(name: str, default: dict) -> dict:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return dict(default)
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return parsed
    except (TypeError, ValueError, json.JSONDecodeError):
        pass
    return dict(default)


STAGE01_COMPLETENESS_BY_DOC_TYPE           = _env_json_dict(
    "STAGE01_COMPLETENESS_BY_DOC_TYPE", {}
)

# Per-discipline allowlist (CSV, e.g. "AR,EOM"). Empty = no discipline opted
# in. Used by Step 4 of phase1_rollout when expanding to full_rd within a
# given discipline.
STAGE01_COMPLETENESS_DISCIPLINE_ALLOWLIST  = _env_csv(
    "STAGE01_COMPLETENESS_DISCIPLINE_ALLOWLIST", []
)

# ─── Portal auth (простая защита веб-портала логином/паролем) ────────────────
# Лёгкая session-cookie аутентификация для 3-4 сотрудников. Без БД, ролей,
# регистрации. По умолчанию ВЫКЛЮЧЕНА (поведение портала не меняется).
#   PORTAL_AUTH_ENABLED=true                  → включить защиту
#   PORTAL_AUTH_USERS='ivan:HASH,petr:HASH'   → логин:pbkdf2-хеш, через запятую
#                                               (одинарные кавычки обязательны: хеш содержит $)
#   PORTAL_SESSION_SECRET=<длинная случайная строка>  → подпись session-cookie
#   PORTAL_SESSION_TTL_HOURS=24               → срок жизни сессии (часы)
#   PORTAL_COOKIE_SECURE=auto|true|false      → Secure-флаг cookie (auto = по схеме запроса)
PORTAL_AUTH_ENABLED        = _env_bool("PORTAL_AUTH_ENABLED", False)
PORTAL_AUTH_USERS_RAW      = os.environ.get("PORTAL_AUTH_USERS", "")
PORTAL_SESSION_SECRET      = os.environ.get("PORTAL_SESSION_SECRET", "")
PORTAL_SESSION_TTL_HOURS   = _env_int("PORTAL_SESSION_TTL_HOURS", 24)
PORTAL_COOKIE_SECURE       = (os.environ.get("PORTAL_COOKIE_SECURE", "auto").strip().lower() or "auto")
PORTAL_SESSION_COOKIE_NAME = os.environ.get("PORTAL_SESSION_COOKIE_NAME", "portal_session").strip() or "portal_session"

# Обратная совместимость: BASE_DIR → ROOT_DIR
BASE_DIR = ROOT_DIR
