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
FINDINGS_CRITIC_TASK_TEMPLATE = _PIPELINE_RU / "findings_critic_task.md"
FINDINGS_CORRECTOR_TASK_TEMPLATE = _PIPELINE_RU / "findings_corrector_task.md"
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
CLAUDE_FINDINGS_CRITIC_TIMEOUT = 1200
CRITIC_CHUNK_SIZE = 50
CLAUDE_FINDINGS_CORRECTOR_TIMEOUT = 1200
CORRECTOR_CHUNK_SIZE = 5
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
FINDINGS_REVIEW_TOOLS = "Read,Write,Grep,Glob"
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
PROJECT_GROUPS_FILE          = APP_DATA_DIR / "project_groups.json"
USAGE_DATA_FILE              = APP_DATA_DIR / "usage_data.json"
USAGE_OFFSETS_FILE           = APP_DATA_DIR / "usage_offsets.json"
STAGE_MODELS_FILE            = APP_DATA_DIR / "stage_models.json"
STAGE_BATCH_MODES_FILE_PATH  = APP_DATA_DIR / "stage_batch_modes.json"
HIDDEN_PROJECTS_FILE         = APP_DATA_DIR / "hidden_projects.json"

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


CHANDRA_GEMMA_MODEL = "google/gemma-4-26b-a4b"
LOCAL_LLM_MODELS = {CHANDRA_GEMMA_MODEL}

AVAILABLE_MODELS = [
    {"id": "claude-opus-4-7",            "label": "Opus 4.7 (CLI)",        "provider": "claude_cli"},
    {"id": "claude-sonnet-4-6",          "label": "Sonnet (CLI)",           "provider": "claude_cli"},
    {"id": "openai/gpt-5.4",             "label": "GPT-5.4",                "provider": "openrouter"},
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

WEEKLY_RESET_WEEKDAY = 4
WEEKLY_RESET_HOUR_UTC = 6

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

CHANDRA_BASE_URL = os.environ.get("CHANDRA_BASE_URL", "").rstrip("/")
CHANDRA_API_BASE_URL = f"{CHANDRA_BASE_URL}/v1" if CHANDRA_BASE_URL else ""
CHANDRA_BASIC_USER = os.environ.get("NGROK_AUTH_USER", "")
CHANDRA_BASIC_PASS = os.environ.get("NGROK_AUTH_PASS", "")

LMSTUDIO_AUTO_RELOAD_ENABLED = _env_bool("LMSTUDIO_AUTO_RELOAD_ENABLED", False)
GEMMA_ADAPTIVE_RELOAD_ENABLED = _env_bool("GEMMA_ADAPTIVE_RELOAD_ENABLED", False)
GEMMA_BASE_CONTEXT_LENGTH = int(os.environ.get("GEMMA_BASE_CONTEXT_LENGTH", "4000"))
GEMMA_HIGH_DETAIL_CONTEXT_LENGTH = int(os.environ.get("GEMMA_HIGH_DETAIL_CONTEXT_LENGTH", "16000"))
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

# Обратная совместимость: BASE_DIR → ROOT_DIR
BASE_DIR = ROOT_DIR
