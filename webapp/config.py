"""
Audit Manager — конфигурация приложения.
Пути, константы, настройки.
"""
import json
import os
import shutil
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

# Корневая папка проекта (где лежат process_project.py и т.д.)
# Приоритет: env AUDIT_BASE_DIR → автодетекция (webapp/../)
BASE_DIR = Path(os.environ["AUDIT_BASE_DIR"]) if os.environ.get("AUDIT_BASE_DIR") else Path(__file__).resolve().parent.parent

# Папка с проектами
PROJECTS_DIR = BASE_DIR / "projects"

# Папка для итоговых отчётов
REPORTS_DIR = BASE_DIR / "отчет"

# Нормативный справочник
NORMS_FILE = BASE_DIR / "norms_reference.md"
NORMS_PARAGRAPHS_FILE = BASE_DIR / "norms" / "norms_paragraphs.json"

# База знаний (экспертные решения, паттерны)
KNOWLEDGE_BASE_DIR = BASE_DIR / "knowledge_base"
DECISIONS_LOG_FILE = KNOWLEDGE_BASE_DIR / "decisions_log.json"
PATTERNS_FILE = KNOWLEDGE_BASE_DIR / "patterns.json"

# Профили дисциплин
DISCIPLINES_DIR = BASE_DIR / "prompts" / "disciplines"

# Шаблоны задач Claude (RU-мастер в prompts/pipeline/ru/, EN для LLM в prompts/pipeline/en/)
_PIPELINE_RU = BASE_DIR / "prompts" / "pipeline" / "ru"
NORM_VERIFY_TASK_TEMPLATE = _PIPELINE_RU / "norm_verify_task.md"
NORM_FIX_TASK_TEMPLATE = _PIPELINE_RU / "norm_fix_task.md"
OPTIMIZATION_TASK_TEMPLATE = _PIPELINE_RU / "optimization_task.md"
TEXT_ANALYSIS_TASK_TEMPLATE = _PIPELINE_RU / "text_analysis_task.md"
BLOCK_ANALYSIS_TASK_TEMPLATE = _PIPELINE_RU / "block_analysis_task.md"
FINDINGS_MERGE_TASK_TEMPLATE = _PIPELINE_RU / "findings_merge_task.md"
FINDINGS_CRITIC_TASK_TEMPLATE = _PIPELINE_RU / "findings_critic_task.md"
FINDINGS_CORRECTOR_TASK_TEMPLATE = _PIPELINE_RU / "findings_corrector_task.md"
OPTIMIZATION_CRITIC_TASK_TEMPLATE = _PIPELINE_RU / "optimization_critic_task.md"
OPTIMIZATION_CORRECTOR_TASK_TEMPLATE = _PIPELINE_RU / "optimization_corrector_task.md"

# Скрипты
PROCESS_PROJECT_SCRIPT = BASE_DIR / "process_project.py"
BLOCKS_SCRIPT = BASE_DIR / "blocks.py"          # субкоманды: crop, batches, merge
NORMS_SCRIPT = BASE_DIR / "norms" / "_core.py"    # субкоманды: verify, update
GENERATE_EXCEL_SCRIPT = BASE_DIR / "generate_excel_report.py"
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
APP_PORT = 8080

# Claude CLI — на Windows нужен полный путь, т.к. asyncio.create_subprocess_exec
# не находит .cmd файлы по PATH (в отличие от subprocess с shell=True)
def _find_claude_cli() -> str:
    """Найти полный путь к Claude CLI."""
    # 1. Через PATH
    found = shutil.which("claude")
    if found:
        return found
    # 2. Стандартные расположения npm global на Windows
    import pathlib
    npm_paths = [
        pathlib.Path.home() / "AppData" / "Roaming" / "npm" / "claude.cmd",
        pathlib.Path(r"C:\Program Files\nodejs\claude.cmd"),
    ]
    for p in npm_paths:
        if p.exists():
            return str(p)
    # 3. Fallback
    return "claude"

CLAUDE_CLI = _find_claude_cli()

# Timeout для Claude-сессий (секунды)
CLAUDE_NORM_VERIFY_TIMEOUT = 600  # 10 мин на верификацию норм
CLAUDE_NORM_FIX_TIMEOUT = 600     # 10 мин на пересмотр замечаний
CLAUDE_OPTIMIZATION_TIMEOUT = 3600  # 60 мин на оптимизацию
CLAUDE_TEXT_ANALYSIS_TIMEOUT = 1800   # 30 мин на анализ текста MD
CLAUDE_BLOCK_ANALYSIS_TIMEOUT = 1800  # 30 мин на пакет блоков (Opus CLI Vision медленнее GPT/Gemini)
CLAUDE_FINDINGS_MERGE_TIMEOUT = 1800  # 30 мин на свод замечаний (02_blocks может быть >800KB)
CLAUDE_FINDINGS_CRITIC_TIMEOUT = 1200  # 20 мин — critic чанк (до 50 findings) через CLI может занять 8-15 мин
CRITIC_CHUNK_SIZE = 50                 # макс. замечаний на 1 запуск Critic
CLAUDE_FINDINGS_CORRECTOR_TIMEOUT = 1200  # 20 мин — Sonnet CLI может быть медленнее Opus
CORRECTOR_CHUNK_SIZE = 5                 # макс. замечаний на 1 запуск Corrector
CLAUDE_OPTIMIZATION_CRITIC_TIMEOUT = 600   # 10 мин — critic проверяет оптимизацию
CLAUDE_OPTIMIZATION_CORRECTOR_TIMEOUT = 600  # 10 мин — corrector исправляет оптимизацию

# Инструменты для Claude CLI сессий
NORM_VERIFY_TOOLS = "Read,Write,Grep,Glob,WebSearch,WebFetch"
TEXT_ANALYSIS_TOOLS = "Read,Write,Grep,Glob,WebSearch,WebFetch"
BLOCK_ANALYSIS_TOOLS = "Read,Write,Grep,Glob,WebSearch,WebFetch"
FINDINGS_MERGE_TOOLS = "Read,Write,Grep,Glob,WebSearch,WebFetch"
FINDINGS_REVIEW_TOOLS = "Read,Write,Grep,Glob"
OPTIMIZATION_REVIEW_TOOLS = "Read,Write,Grep,Glob"

# Модель Claude CLI (sonnet = экономит лимит All models)
# Варианты: "claude-sonnet-4-6", "claude-opus-4-6", "claude-haiku-4-5-20251001"
CLAUDE_MODEL_DEFAULT = "claude-sonnet-4-6"
CLAUDE_MODEL_OPTIONS = ["claude-sonnet-4-6", "claude-opus-4-6"]

# Текущая модель (изменяемая в рантайме через API)
_current_model = CLAUDE_MODEL_DEFAULT

# Гибридный режим: per-stage модели (Opus для сложных рассуждений)
# None = использовать _current_model (по умолчанию)
_stage_models: dict[str, str | None] = {
    "text_analysis":   None,           # Sonnet — структурная задача
    "block_batch":     None,           # Sonnet — чтение чертежей, заполнение JSON
    "findings_merge":  "claude-opus-4-6",  # Opus — межблочная сверка, дедупликация
    "findings_critic": None,           # Sonnet — проверка grounding+evidence
    "findings_corrector": None,        # Sonnet — исправление по вердиктам критика
    "norm_verify":     None,           # Sonnet — поиск и сверка норм
    "norm_fix":        None,           # Sonnet — пересмотр по нормам
    "optimization":    "claude-opus-4-6",  # Opus — глубокий анализ оптимизаций
    "optimization_critic": None,           # Sonnet — проверка оптимизаций
    "optimization_corrector": None,        # Sonnet — корректировка оптимизаций
}

# ═══════════════════════════════════════════════════════════════════════════
# Унифицированная конфигурация моделей по этапам (UI Stage Model Config)
# Объединяет Claude CLI и OpenRouter модели в единый маппинг.
# Персистится в webapp/data/stage_models.json — переживает рестарт сервера.
# ═══════════════════════════════════════════════════════════════════════════

_STAGE_MODEL_DEFAULTS: dict[str, str] = {
    "text_analysis":          "claude-opus-4-6",
    "block_batch":            "claude-opus-4-6",
    "findings_merge":         "claude-opus-4-6",
    "findings_critic":        "openai/gpt-5.4",
    "findings_corrector":     "claude-opus-4-6",
    "norm_verify":            "claude-opus-4-6",
    "norm_fix":               "claude-opus-4-6",
    "optimization":           "claude-opus-4-6",
    "optimization_critic":    "claude-sonnet-4-6",
    "optimization_corrector": "claude-sonnet-4-6",
}

_STAGE_MODELS_FILE = Path(__file__).resolve().parent / "data" / "stage_models.json"


def _load_stage_model_config() -> dict[str, str]:
    """Загрузить конфиг моделей из файла, fallback на дефолты."""
    config = dict(_STAGE_MODEL_DEFAULTS)
    if _STAGE_MODELS_FILE.exists():
        try:
            with open(_STAGE_MODELS_FILE, "r", encoding="utf-8") as f:
                saved = json.load(f)
            # Применяем только известные этапы с валидными значениями
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

AVAILABLE_MODELS = [
    {"id": "claude-opus-4-6", "label": "Opus (CLI)", "provider": "claude_cli"},
    {"id": "claude-sonnet-4-6", "label": "Sonnet (CLI)", "provider": "claude_cli"},
    {"id": "openai/gpt-5.4", "label": "GPT-5.4", "provider": "openrouter"},
    {"id": "google/gemini-3.1-pro-preview", "label": "Gemini", "provider": "openrouter"},
]

# Этапы с ограничениями на выбор модели
# block_batch: OpenRouter (GPT/Gemini) + экспериментально Claude CLI (Opus/Sonnet)
# Claude CLI читает PNG через Read tool (Vision поддержка).
STAGE_MODEL_RESTRICTIONS = {
    "block_batch": [
        "openai/gpt-5.4",
        "google/gemini-3.1-pro-preview",
        "claude-opus-4-6",        # экспериментально — CLI + Vision
        "claude-sonnet-4-6",      # экспериментально — CLI + Vision
    ],
}

# Подсказки при выборе модели для этапа (отображаются в UI)
STAGE_MODEL_HINTS: dict[str, str] = {
    "text_analysis": "Opus CLI рекомендуется. Sonnet допустим.",
    "block_batch": "GPT-5.4 / Gemini (OpenRouter) или Opus / Sonnet (Claude CLI, Vision через Read). Opus CLI — эксперимент, медленнее и жрёт CLI-лимит.",
    "findings_merge": "Минимум Opus CLI — межблочная сверка требует сильной модели.",
    "findings_critic": "GPT-5.4 оптимален: быстро и дёшево.",
    "findings_corrector": "Минимум Opus CLI. Sonnet не успевает (таймаут). GPT-5.4 — альтернатива.",
    "norm_verify": "Opus CLI рекомендуется (WebSearch + анализ норм).",
    "norm_fix": "Opus CLI рекомендуется.",
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

def get_claude_model() -> str:
    """Модель по умолчанию (для обратной совместимости)."""
    return _current_model

def get_model_for_stage(stage: str) -> str:
    """Модель для конкретного этапа конвейера."""
    # Нормализация: block_batch_001 → block_batch
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

# Параллельная обработка батчей блоков
MAX_PARALLEL_BATCHES = 5  # параллельных батчей

# ─── Rate Limit: пауза вместо ошибки ───
RATE_LIMIT_THRESHOLD_PCT = 90   # при 90% лимита — предварительная проверка перед запуском
RATE_LIMIT_CHECK_INTERVAL = 60  # сек между проверками во время ожидания
RATE_LIMIT_MAX_WAIT = 5 * 3600  # макс. ожидание = 5 часов (полное окно)
RATE_LIMIT_MAX_RETRIES = 5      # макс. повторов одного батча после rate limit

# Уровни критичности замечаний (порядок и цвета)
# ─── Лимиты потребления токенов (Max 20x план, $200/мес) ───
# Лимиты рассчитаны по данным дашборда: input+output токены (без cache)
# Калибруйте через POST /api/usage/global/limits
ANTHROPIC_PLAN = "Max 20x"
WINDOW_5H_TOKEN_LIMIT = 12_000_000    # ~12M токенов на 5ч окно (оценка для Max 20x)
WEEKLY_TOKEN_LIMIT = 17_000_000       # ~17M токенов в неделю (оценка для Max 20x)
CLAUDE_SESSIONS_DIR = Path.home() / ".claude" / "projects"

# Еженедельный сброс лимитов (как на дашборде Anthropic Settings → Usage)
# Сайт показывает "Resets Fri 9:00 AM" → пятница = weekday 4
# 9:00 MSK = 06:00 UTC
WEEKLY_RESET_WEEKDAY = 4   # 0=пн, 1=вт, 2=ср, 3=чт, 4=пт, 5=сб, 6=вс
WEEKLY_RESET_HOUR_UTC = 6   # UTC час сброса (MSK-3)

SEVERITY_CONFIG = {
    "КРИТИЧЕСКОЕ":        {"color": "#e74c3c", "bg": "#fdecea", "icon": "\U0001f534", "order": 1},
    "ЭКОНОМИЧЕСКОЕ":      {"color": "#e67e22", "bg": "#fef5e7", "icon": "\U0001f7e0", "order": 2},
    "ЭКСПЛУАТАЦИОННОЕ":   {"color": "#f1c40f", "bg": "#fef9e7", "icon": "\U0001f7e1", "order": 3},
    "РЕКОМЕНДАТЕЛЬНОЕ":   {"color": "#3498db", "bg": "#eaf2f8", "icon": "\U0001f535", "order": 4},
    "ПРОВЕРИТЬ ПО СМЕЖНЫМ": {"color": "#95a5a6", "bg": "#f2f3f4", "icon": "\u26aa", "order": 5},
}

# ═══════════════════════════════════════════════════════════════════════════
# OpenRouter API — параллельный бэкенд для LLM (Gemini, GPT через единый API)
# Старый Claude CLI бэкенд выше остаётся рабочим до полной миграции.
# ═══════════════════════════════════════════════════════════════════════════

# === OpenRouter ===
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
OPENROUTER_SITE_URL = "http://localhost:8080"
OPENROUTER_SITE_NAME = "BIM Audit Pipeline"

# === Модели OpenRouter ===
GEMINI_MODEL = "google/gemini-3.1-pro-preview"
GPT_MODEL = "openai/gpt-5.4"

# === Per-stage модели (OpenRouter) ===
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

# === Параметры генерации ===
GEMINI_MAX_OUTPUT_TOKENS = 65536   # default 8192 — ОБЯЗАТЕЛЬНО задавать!
GPT_MAX_OUTPUT_TOKENS = 128000
DEFAULT_TEMPERATURE = 0.2

# === Лимиты изображений ===
GEMINI_MAX_IMAGES = 3600
GPT_MAX_IMAGES = 500
OPENROUTER_MAX_BLOCKS_PER_BATCH = 80

# === JSON Schema для structured output ===
SCHEMAS_DIR = Path(__file__).resolve().parent / "schemas"

# ═══════════════════════════════════════════════════════════════════════════
# Обсуждения (Discussions) — чат по замечаниям/оптимизациям через OpenRouter
# ═══════════════════════════════════════════════════════════════════════════
DISCUSSION_MODELS = [
    {"id": "claude-cli", "label": "Claude CLI", "provider": "claude_cli"},
    {"id": "openai/gpt-4.1-mini", "label": "GPT-4.1 mini", "provider": "openrouter"},
    {"id": "google/gemini-3.1-pro-preview", "label": "Gemini 3.1 Pro", "provider": "openrouter"},
]
DISCUSSION_DEFAULT_MODEL = "claude-cli"
DISCUSSION_CLI_TIMEOUT = 120  # секунд на один вызов Claude CLI для чата
DISCUSSION_MAX_OUTPUT_TOKENS = 16384
DISCUSSION_TEMPERATURE = 0.3
DISCUSSION_TIMEOUT = 120  # секунд на один запрос чата
DISCUSSION_SUMMARY_THRESHOLD = 10  # после скольких сообщений сжимать историю
