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
    """Найти свежайший claude-бинарь среди установленных расширений VSCode.

    Расширение Claude Code автообновляется, и симлинк ~/.local/bin/claude может
    указывать на удалённую старую версию. Этот скан подхватывает актуальную папку.
    """
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
    # 1. Через PATH
    found = shutil.which("claude")
    if _is_usable_cli(found):
        return found
    # 2. Через расширенный PATH (включая ~/.local/bin которого нет у webapp)
    extended_path = os.environ.get("PATH", "") + os.pathsep + str(Path.home() / ".local" / "bin")
    found = shutil.which("claude", path=extended_path)
    if _is_usable_cli(found):
        return found
    # 3. Расширение VSCode (актуальная версия после автообновления)
    found = _scan_vscode_claude()
    if found:
        return found
    # 4. Стандартные расположения Linux
    linux_paths = [
        Path.home() / ".local" / "bin" / "claude",
        Path("/usr/local/bin/claude"),
    ]
    for p in linux_paths:
        if _is_usable_cli(p):
            return str(p)
    # 5. Стандартные расположения npm global на Windows
    npm_paths = [
        Path.home() / "AppData" / "Roaming" / "npm" / "claude.cmd",
        Path(r"C:\Program Files\nodejs\claude.cmd"),
    ]
    for p in npm_paths:
        if _is_usable_cli(p):
            return str(p)
    # 6. Fallback
    return "claude"


CLAUDE_CLI = _find_claude_cli()


def get_claude_cli() -> str:
    """Вернуть рабочий путь к Claude CLI; перерешить, если кешированный битый.

    CLAUDE_CLI вычисляется один раз при import'е. Если расширение VSCode обновилось
    за время жизни webapp-процесса, симлинк может стать битым — здесь мы ловим это
    и находим новый путь без рестарта.
    """
    global CLAUDE_CLI
    if _is_usable_cli(CLAUDE_CLI):
        return CLAUDE_CLI
    CLAUDE_CLI = _find_claude_cli()
    return CLAUDE_CLI

# Timeout для Claude-сессий (секунды)
CLAUDE_NORM_VERIFY_TIMEOUT = 600  # 10 мин на верификацию норм
CLAUDE_NORM_FIX_TIMEOUT = 600     # 10 мин на пересмотр замечаний
CLAUDE_OPTIMIZATION_TIMEOUT = 3600  # 60 мин на оптимизацию
CLAUDE_TEXT_ANALYSIS_TIMEOUT = 1800   # 30 мин на анализ текста MD
# Stage 02 (block_batch) запускает `claude -p` из чистой папки /tmp/sonnet_clean/ + stripped env,
# чтобы не подгружать project CLAUDE.md, hooks, memory, skills (~47K input/блок harness'а).
# Эмпирически (КЖ5.1, 25 блоков): −42% input/блок, −36% cli_cost, +35% findings.
# Подробности — ideas.md (Идея 6) и memory/feedback_subscription_only.md.
# Если на других дисциплинах (EOM/OV/AR) увидишь регрессии — выставь False.
CLAUDE_BLOCK_BATCH_CLEAN_CWD = True

CLAUDE_BLOCK_ANALYSIS_TIMEOUT = 1800  # 30 мин на пакет блоков (Opus CLI Vision медленнее GPT/Gemini)
CLAUDE_FINDINGS_MERGE_TIMEOUT = 1800  # 30 мин на свод замечаний (02_blocks может быть >800KB)
CLAUDE_FINDINGS_CRITIC_TIMEOUT = 1200  # 20 мин — critic чанк (до 50 findings) через CLI может занять 8-15 мин
CRITIC_CHUNK_SIZE = 50                 # макс. замечаний на 1 запуск Critic
CLAUDE_FINDINGS_CORRECTOR_TIMEOUT = 1200  # 20 мин — Sonnet CLI может быть медленнее Opus
CORRECTOR_CHUNK_SIZE = 5                 # макс. замечаний на 1 запуск Corrector
CLAUDE_OPTIMIZATION_CRITIC_TIMEOUT = 600   # 10 мин — critic проверяет оптимизацию
CLAUDE_OPTIMIZATION_CORRECTOR_TIMEOUT = 600  # 10 мин — corrector исправляет оптимизацию

# Инструменты для Claude CLI сессий
# norm_verify / norm_fix работают через MCP norms (единственный источник истины).
# WebSearch и WebFetch намеренно исключены: внешних источников быть не должно.
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
# Варианты: "claude-sonnet-4-6", "claude-opus-4-7", "claude-haiku-4-5-20251001"
CLAUDE_MODEL_DEFAULT = "claude-sonnet-4-6"
CLAUDE_MODEL_OPTIONS = ["claude-sonnet-4-6", "claude-opus-4-7"]

# Текущая модель (изменяемая в рантайме через API)
_current_model = CLAUDE_MODEL_DEFAULT

# Гибридный режим: per-stage модели (Opus для сложных рассуждений)
# None = использовать _current_model (по умолчанию)
_stage_models: dict[str, str | None] = {
    "text_analysis":   None,           # Sonnet — структурная задача
    "block_batch":     None,           # Sonnet — чтение чертежей, заполнение JSON
    "findings_merge":  "claude-opus-4-7",  # Opus — межблочная сверка, дедупликация
    "findings_critic": None,           # Sonnet — проверка grounding+evidence
    "findings_corrector": None,        # Sonnet — исправление по вердиктам критика
    "norm_verify":     None,           # Sonnet — поиск и сверка норм
    "norm_fix":        None,           # Sonnet — пересмотр по нормам
    "optimization":    "claude-opus-4-7",  # Opus — глубокий анализ оптимизаций
    "optimization_critic": None,           # Sonnet — проверка оптимизаций
    "optimization_corrector": None,        # Sonnet — корректировка оптимизаций
}

# ═══════════════════════════════════════════════════════════════════════════
# Унифицированная конфигурация моделей по этапам (UI Stage Model Config)
# Объединяет Claude CLI и OpenRouter модели в единый маппинг.
# Персистится в webapp/data/stage_models.json — переживает рестарт сервера.
# ═══════════════════════════════════════════════════════════════════════════

_STAGE_MODEL_DEFAULTS: dict[str, str] = {
    # Пресет "Классический": Opus 4.7 для всех этапов кроме opt_critic/corrector.
    # Sonnet-corrector не применяет вердикты критика (проверено на КЖ 5.1 — 0 правок
    # против 4 удалённых formal findings у Opus при идентичных вердиктах критика).
    "text_analysis":          "claude-opus-4-7",
    "block_batch":            "claude-opus-4-7",
    "findings_merge":         "claude-opus-4-7",
    "findings_critic":        "claude-opus-4-7",
    "findings_corrector":     "claude-opus-4-7",
    "norm_verify":            "claude-opus-4-7",
    "norm_fix":               "claude-opus-4-7",
    "optimization":           "claude-opus-4-7",
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

# ═══════════════════════════════════════════════════════════════════════════
# Stage batch modes — расширенные режимы конкретных этапов.
# Сейчас используется только для block_batch:
#   "classic"                  — стандартный batched stage 02 (5-12 блоков на запрос)
#   "findings_only_qwen_pair"  — single-block через GPT-5.4 + qwen-enrichment + extended categories
# Персистится в webapp/data/stage_batch_modes.json.
# ═══════════════════════════════════════════════════════════════════════════

_STAGE_BATCH_MODE_DEFAULTS: dict[str, str] = {
    "block_batch": "classic",
}

STAGE_BATCH_MODE_CHOICES: dict[str, list[str]] = {
    "block_batch": ["classic", "findings_only_qwen_pair"],
}

_STAGE_BATCH_MODES_FILE = Path(__file__).resolve().parent / "data" / "stage_batch_modes.json"


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
    return STAGE_BATCH_MODES.get(stage, _STAGE_BATCH_MODE_DEFAULTS.get(stage, "classic"))


def set_stage_batch_mode(stage: str, mode: str) -> bool:
    """Возвращает True если режим установлен (валиден), иначе False."""
    if stage not in STAGE_BATCH_MODE_CHOICES:
        return False
    if mode not in STAGE_BATCH_MODE_CHOICES[stage]:
        return False
    STAGE_BATCH_MODES[stage] = mode
    _save_stage_batch_modes()
    return True


FLASH_PRO_TRIAGE_MODEL = "pair/gemini-2.5-flash+gemini-3.1-pro"
FLASH_PRO_TRIAGE_MODELS = (
    "google/gemini-2.5-flash",
    "google/gemini-3.1-pro-preview",
)

CHANDRA_QWEN_MODEL = "qwen/qwen3.6-35b-a3b"
LOCAL_LLM_MODELS = {CHANDRA_QWEN_MODEL}

AVAILABLE_MODELS = [
    {"id": "claude-opus-4-7",            "label": "Opus 4.7 (CLI)",        "provider": "claude_cli"},
    {"id": "claude-sonnet-4-6",          "label": "Sonnet (CLI)",           "provider": "claude_cli"},
    {"id": "openai/gpt-5.4",             "label": "GPT-5.4",                "provider": "openrouter"},
    {"id": "google/gemini-3.1-pro-preview", "label": "Gemini 3.1 Pro",      "provider": "openrouter"},
]

# Этапы с ограничениями на выбор модели
# block_batch: OpenRouter (GPT/Gemini) + экспериментально Claude CLI (Opus/Sonnet)
# Claude CLI читает PNG через Read tool (Vision поддержка).
STAGE_MODEL_RESTRICTIONS = {
    "block_batch": [
        "openai/gpt-5.4",
        "google/gemini-3.1-pro-preview",
        "claude-opus-4-7",           # production — CLI + Vision
        "claude-sonnet-4-6",         # экспериментально — CLI + Vision
    ],
}

# Подсказки при выборе модели для этапа (отображаются в UI)
STAGE_MODEL_HINTS: dict[str, str] = {
    "text_analysis": "Opus CLI рекомендуется. Sonnet допустим.",
    "block_batch": "Opus 4.7 CLI — production (r800 + baseline_p3, parallelism=3). Альтернативы: GPT-5.4 (OpenRouter) или Gemini 3.1 Pro. Direct Gemini API скрыт из UI из-за гео-ограничений.",
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

# Параллельная обработка батчей (общая — findings critic, tile batches и т.д.)
MAX_PARALLEL_BATCHES = 2  # параллельных батчей

# ─── Stage 02 block_batch: параллелизм по провайдеру ─────────────────────────
# Claude CLI Vision (Opus 4.7): production winner = baseline_p3 (parallelism=3).
# Закреплено экспериментами 20.04.2026, КЖ5.17, 215 блоков:
#   parallelism=3 + baseline batching → 100% coverage, 0 failures на обоих full-runs.
# Safe fallback: baseline_p2 (parallelism=2) при проблемах с rate-limit.
# OpenRouter/Gemini/GPT остаются на общем MAX_PARALLEL_BATCHES.
# ENV override: CLAUDE_BLOCK_BATCH_PARALLELISM=N — всё равно clamp до CAP.
CLAUDE_BLOCK_BATCH_PARALLELISM_DEFAULT = 3  # production winner: baseline_p3
CLAUDE_BLOCK_BATCH_PARALLELISM_CAP = 3
LOCAL_BLOCK_BATCH_PARALLELISM_DEFAULT = 1


def get_block_batch_parallelism(stage: str = "block_batch", model: str | None = None) -> int:
    """Параллелизм для stage 02 block_batch в зависимости от модели/провайдера.

    - Claude CLI (claude-*): default=3, hard cap=3 (production winner: baseline_p3).
      Safe fallback: parallelism=2 при rate-limit проблемах.
      ENV override CLAUDE_BLOCK_BATCH_PARALLELISM clamp до cap.
    - OpenRouter / прочие: общий MAX_PARALLEL_BATCHES.
    """
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

# === Chandra / LM Studio (локальные модели через OpenAI-compatible API) ===
CHANDRA_BASE_URL = os.environ.get("CHANDRA_BASE_URL", "").rstrip("/")
CHANDRA_API_BASE_URL = f"{CHANDRA_BASE_URL}/v1" if CHANDRA_BASE_URL else ""
CHANDRA_BASIC_USER = os.environ.get("NGROK_AUTH_USER", "")
CHANDRA_BASIC_PASS = os.environ.get("NGROK_AUTH_PASS", "")

# === Модели OpenRouter ===
GEMINI_MODEL = "google/gemini-3.1-pro-preview"
GPT_MODEL = "openai/gpt-5.4"
# Эмпирически безопасный потолок для текущей Chandra-машины:
# 98_304 загружается стабильно, 100_352+ уже режется guardrails LM Studio.
LOCAL_QWEN_CONTEXT_LENGTH = int(os.environ.get("LOCAL_QWEN_CONTEXT_LENGTH", "98304"))
LOCAL_QWEN_MAX_OUTPUT_TOKENS = int(os.environ.get("LOCAL_QWEN_MAX_OUTPUT_TOKENS", "8192"))
LOCAL_QWEN_FINDINGS_MAX_OUTPUT_TOKENS = int(
    os.environ.get("LOCAL_QWEN_FINDINGS_MAX_OUTPUT_TOKENS", "16384")
)

# ═══════════════════════════════════════════════════════════════════════════
# Direct Gemini Developer API (параллельный путь, только для block_batch)
# Активируется через env GEMINI_DIRECT_API_KEY.
# НЕ подменяет OpenRouter/Claude CLI автоматически.
# ═══════════════════════════════════════════════════════════════════════════

GEMINI_DIRECT_API_KEY: str = (
    os.environ.get("GEMINI_DIRECT_API_KEY", "")
    or os.environ.get("GOOGLE_API_KEY", "")
)

# Маппинг внутренних/OpenRouter model IDs → Gemini Developer API native IDs
GEMINI_DIRECT_MODEL_MAP: dict[str, str] = {
    "google/gemini-2.5-flash":       "gemini-2.5-flash",
    "google/gemini-2.5-flash-lite":  "gemini-2.5-flash-lite",
    "google/gemini-3.1-pro-preview": "gemini-3.1-pro-preview",
    "gemini-2.5-flash":              "gemini-2.5-flash",
    "gemini-2.5-flash-lite":         "gemini-2.5-flash-lite",
    "gemini-3.1-pro-preview":        "gemini-3.1-pro-preview",
}

# Mainline candidate for direct Gemini (set after Phase A quality gate).
# Default: unset — production path remains unchanged until experiment completes.
GEMINI_DIRECT_DEFAULT_MODEL: str = os.environ.get("GEMINI_DIRECT_MODEL", "gemini-2.5-flash")

# Max output tokens for direct Gemini calls
GEMINI_DIRECT_MAX_OUTPUT_TOKENS: int = 65536

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

# ═══════════════════════════════════════════════════════════════════════════
# OpenRouter stage 02 (block_batch) experimental knobs.
# Используются ТОЛЬКО экспериментальным runner'ом (scripts/run_gemini_openrouter_stage02_experiment.py).
# Production OpenRouter path (llm_runner.run_llm) не переключается автоматически.
# ═══════════════════════════════════════════════════════════════════════════

# Hard cap блоков в одном OpenRouter batch (stage 02). Абсолютный предел
# независимо от профиля.
OPENROUTER_STAGE02_HARD_CAP_BLOCKS = 12

# Raw PNG payload cap per batch (KB). Если суммарный размер PNG превышает
# лимит — deterministic split. 9000 KB — consensus safe для Gemini vision.
OPENROUTER_STAGE02_RAW_BYTE_CAP_KB = 9000

# Таймаут одного OpenRouter stage 02 запроса (секунды)
OPENROUTER_STAGE02_TIMEOUT_SEC = 600

# Максимум output токенов для stage 02 OpenRouter запросов
OPENROUTER_STAGE02_MAX_OUTPUT_TOKENS = 32768
