"""
Audit Manager — конфигурация приложения.
Пути, константы, настройки.
"""
import os
import shutil
from pathlib import Path

# Корневая папка проекта (где лежат process_project.py и т.д.)
BASE_DIR = Path(r"D:\Отедел Системного Анализа\1. Calude code")

# Папка с проектами
PROJECTS_DIR = BASE_DIR / "projects"

# Папка для итоговых отчётов
REPORTS_DIR = BASE_DIR / "отчет"

# Нормативный справочник
NORMS_FILE = BASE_DIR / "norms_reference.md"

# Профили дисциплин
DISCIPLINES_DIR = BASE_DIR / "disciplines"

# Шаблоны задач Claude
AUDIT_TASK_TEMPLATE = BASE_DIR / ".claude" / "audit_task.md"
TILE_BATCH_TASK_TEMPLATE = BASE_DIR / ".claude" / "tile_batch_task.md"
NORM_VERIFY_TASK_TEMPLATE = BASE_DIR / ".claude" / "norm_verify_task.md"
NORM_FIX_TASK_TEMPLATE = BASE_DIR / ".claude" / "norm_fix_task.md"
TRIAGE_TASK_TEMPLATE = BASE_DIR / ".claude" / "triage_task.md"
SMART_MERGE_TASK_TEMPLATE = BASE_DIR / ".claude" / "smart_merge_task.md"
OPTIMIZATION_TASK_TEMPLATE = BASE_DIR / ".claude" / "optimization_task.md"

# Скрипты
PROCESS_PROJECT_SCRIPT = BASE_DIR / "process_project.py"
GENERATE_BATCHES_SCRIPT = BASE_DIR / "generate_tile_batches.py"
MERGE_RESULTS_SCRIPT = BASE_DIR / "merge_tile_results.py"
GENERATE_EXCEL_SCRIPT = BASE_DIR / "generate_excel_report.py"
VERIFY_NORMS_SCRIPT = BASE_DIR / "verify_norms.py"

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
TILE_AUDIT_TOOLS = "Read,Write,Edit,Bash(python *),Bash(powershell *),Bash(cmd *),Grep,Glob,WebSearch,WebFetch"
MAIN_AUDIT_TOOLS = "Read,Write,Edit,Bash(python *),Bash(powershell *),Bash(cmd *),Bash(ls *),Bash(mkdir *),Grep,Glob,WebSearch,WebFetch"

# Timeout для Claude-сессий (секунды)
CLAUDE_BATCH_TIMEOUT = 600   # 10 мин на один пакет тайлов
CLAUDE_AUDIT_TIMEOUT = 5400  # 90 мин на основной аудит (большие проекты 60+ стр.)
CLAUDE_TRIAGE_TIMEOUT = 600       # 10 мин на триаж страниц
CLAUDE_SMART_MERGE_TIMEOUT = 900  # 15 мин на свод замечаний + отчёт
CLAUDE_NORM_VERIFY_TIMEOUT = 600  # 10 мин на верификацию норм
CLAUDE_NORM_FIX_TIMEOUT = 600     # 10 мин на пересмотр замечаний
CLAUDE_OPTIMIZATION_TIMEOUT = 3600  # 60 мин на оптимизацию

# Инструменты для триажа (чтение текста + запись JSON)
TRIAGE_TOOLS = "Read,Write,Grep,Glob,WebSearch,WebFetch"
# Инструменты для свода замечаний (чтение JSON + запись результатов)
SMART_MERGE_TOOLS = "Read,Write,Edit,Grep,Glob,WebSearch,WebFetch"
# Инструменты для верификации норм (WebSearch обязателен)
NORM_VERIFY_TOOLS = "Read,Write,Grep,Glob,WebSearch,WebFetch"

# Модель Claude CLI (sonnet = экономит лимит All models)
# Варианты: "claude-sonnet-4-6", "claude-opus-4-6", "claude-haiku-4-5-20251001"
CLAUDE_MODEL_DEFAULT = "claude-sonnet-4-6"
CLAUDE_MODEL_OPTIONS = ["claude-sonnet-4-6", "claude-opus-4-6"]

# Текущая модель (изменяемая в рантайме через API)
_current_model = CLAUDE_MODEL_DEFAULT

def get_claude_model() -> str:
    return _current_model

def set_claude_model(model: str):
    global _current_model
    if model in CLAUDE_MODEL_OPTIONS:
        _current_model = model

# Качество тайлов по умолчанию
DEFAULT_TILE_QUALITY = "speed"

# Параллельная обработка батчей тайлов
MAX_PARALLEL_BATCHES = 3  # одновременных Claude CLI сессий

# ─── Rate Limit: пауза вместо ошибки ───
RATE_LIMIT_THRESHOLD_PCT = 90   # при 90% лимита — предварительная проверка перед запуском
RATE_LIMIT_CHECK_INTERVAL = 60  # сек между проверками во время ожидания
RATE_LIMIT_MAX_WAIT = 5 * 3600  # макс. ожидание = 5 часов (полное окно)
RATE_LIMIT_MAX_RETRIES = 3      # макс. повторов одного батча после rate limit

# Уровни критичности замечаний (порядок и цвета)
# ─── Лимиты потребления токенов (Max 20x план, $200/мес) ───
# Лимиты рассчитаны по данным дашборда: input+output токены (без cache)
# Калибруйте через POST /api/usage/global/limits
ANTHROPIC_PLAN = "Max 20x"
WINDOW_5H_TOKEN_LIMIT = 12_000_000    # ~12M токенов на 5ч окно (оценка для Max 20x)
WEEKLY_TOKEN_LIMIT = 17_000_000       # ~17M токенов в неделю (оценка для Max 20x)
CLAUDE_SESSIONS_DIR = Path.home() / ".claude" / "projects"

# Еженедельный сброс лимитов (как на дашборде Anthropic)
# Пользователь видит "Сброс в четверг в 20:00" → четверг = weekday 3
# 20:00 MSK = 17:00 UTC
WEEKLY_RESET_WEEKDAY = 3   # 0=пн, 1=вт, 2=ср, 3=чт, 4=пт, 5=сб, 6=вс
WEEKLY_RESET_HOUR_UTC = 17  # UTC час сброса (MSK-3)

SEVERITY_CONFIG = {
    "КРИТИЧЕСКОЕ":        {"color": "#e74c3c", "bg": "#fdecea", "icon": "\U0001f534", "order": 1},
    "ЭКОНОМИЧЕСКОЕ":      {"color": "#e67e22", "bg": "#fef5e7", "icon": "\U0001f7e0", "order": 2},
    "ЭКСПЛУАТАЦИОННОЕ":   {"color": "#f1c40f", "bg": "#fef9e7", "icon": "\U0001f7e1", "order": 3},
    "РЕКОМЕНДАТЕЛЬНОЕ":   {"color": "#3498db", "bg": "#eaf2f8", "icon": "\U0001f535", "order": 4},
    "ПРОВЕРИТЬ ПО СМЕЖНЫМ": {"color": "#95a5a6", "bg": "#f2f3f4", "icon": "\u26aa", "order": 5},
}
