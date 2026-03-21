"""
Audit Manager — конфигурация приложения.
Пути, константы, настройки.
"""
import os
import shutil
from pathlib import Path

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

# Профили дисциплин
DISCIPLINES_DIR = BASE_DIR / "disciplines"

# Шаблоны задач Claude
NORM_VERIFY_TASK_TEMPLATE = BASE_DIR / ".claude" / "norm_verify_task.md"
NORM_FIX_TASK_TEMPLATE = BASE_DIR / ".claude" / "norm_fix_task.md"
OPTIMIZATION_TASK_TEMPLATE = BASE_DIR / ".claude" / "optimization_task.md"
TEXT_ANALYSIS_TASK_TEMPLATE = BASE_DIR / ".claude" / "text_analysis_task.md"
BLOCK_ANALYSIS_TASK_TEMPLATE = BASE_DIR / ".claude" / "block_analysis_task.md"
FINDINGS_MERGE_TASK_TEMPLATE = BASE_DIR / ".claude" / "findings_merge_task.md"
FINDINGS_CRITIC_TASK_TEMPLATE = BASE_DIR / ".claude" / "findings_critic_task.md"
FINDINGS_CORRECTOR_TASK_TEMPLATE = BASE_DIR / ".claude" / "findings_corrector_task.md"
OPTIMIZATION_CRITIC_TASK_TEMPLATE = BASE_DIR / ".claude" / "optimization_critic_task.md"
OPTIMIZATION_CORRECTOR_TASK_TEMPLATE = BASE_DIR / ".claude" / "optimization_corrector_task.md"

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
CLAUDE_BLOCK_ANALYSIS_TIMEOUT = 600   # 10 мин на пакет блоков
CLAUDE_FINDINGS_MERGE_TIMEOUT = 1800  # 30 мин на свод замечаний (02_blocks может быть >800KB)
CLAUDE_FINDINGS_CRITIC_TIMEOUT = 600   # 10 мин — critic проверяет готовые замечания
CRITIC_CHUNK_SIZE = 50                 # макс. замечаний на 1 запуск Critic
CLAUDE_FINDINGS_CORRECTOR_TIMEOUT = 600  # 10 мин — corrector исправляет по вердиктам
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
MAX_PARALLEL_BATCHES = 3  # одновременных Claude CLI сессий

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
