"""
Audit Manager — конфигурация приложения (backend).
Пути, константы, настройки.
"""
import json
import os
import shutil
from pathlib import Path

from dotenv import load_dotenv

# AUDIT_DISABLE_DOTENV=1 — единственный способ запустить конвейер с окружением
# ИЗ БЕЛОГО СПИСКА и никаким другим. Нужен удалённому исполнению на воркере:
# `load_dotenv()` ищет `.env` вверх от этого файла и находит его в корне
# установленного кода платформы, восстанавливая всё, что воркер намеренно не
# передал — включая ключи платных API и `PAID_API_ENABLED`. Обычный запуск
# центра переменную не выставляет, поэтому его поведение не меняется.
if os.environ.get("AUDIT_DISABLE_DOTENV", "").strip().lower() not in {
    "1", "true", "yes", "on",
}:
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
# Пересмотр ОПТИМИЗАЦИЙ после верификации норм. Отдельный шаблон, а не общий с
# norm_fix: у предложения другой исход — не «поправить ссылку», а переосмыслить
# саму замену под актуальную норму (still_valid / revised / obsolete).
OPTIMIZATION_NORM_FIX_TASK_TEMPLATE = _PIPELINE_RU / "optimization_norm_fix_task.md"
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
# 1800 с оказалось не запасом, а потолком: свод ДОО (44 находки по блокам + 27 по
# тексту) дважды подряд убивался ровно на 1801 с, теряя уже проделанную работу
# целиком (04.08.2026). Час — страховка на крупные комплекты; штатный свод
# укладывается в 10-25 мин и таймаута не касается.
CLAUDE_FINDINGS_MERGE_TIMEOUT = 3600
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
OPTIMIZATION_TOOLS = (
    "Read,Write,Grep,Glob,"
    "mcp__norms__get_norm_status,"
    "mcp__norms__get_paragraph_json,"
    "mcp__norms__semantic_search_json"
)
OPTIMIZATION_REVIEW_TOOLS = "Read,Write,Grep,Glob"

# Модель Claude CLI (sonnet = экономит лимит All models).
# 2026-08-04: пайплайн переведён на поколение 5 (claude-opus-5 / claude-sonnet-5).
# Важно: алиасы "opus"/"sonnet" СПЕЦИАЛЬНО не используются — CLI резолвит их в
# текущее поколение по своему усмотрению (замер 2026-08-03: opus→4.8, sonnet→5),
# поэтому модель везде задаётся явным id.
CLAUDE_MODEL_DEFAULT = "claude-sonnet-5"
CLAUDE_MODEL_OPTIONS = ["claude-sonnet-5", "claude-opus-5"]

_current_model = CLAUDE_MODEL_DEFAULT

_stage_models: dict[str, str | None] = {
    "text_analysis":   None,
    "block_batch":     None,
    "findings_merge":  "claude-opus-5",
    "findings_critic": None,
    "findings_corrector": None,
    "norm_verify":     None,
    "norm_fix":        None,
    "norm_requote":    None,
    "optimization":    "claude-opus-5",
    "optimization_critic": None,
    "optimization_corrector": None,
}

_STAGE_MODEL_DEFAULTS: dict[str, str] = {
    "text_analysis":          "claude-opus-5",
    "block_batch":            "ensemble/gpt-codex",
    "findings_merge":         "claude-opus-5",
    "findings_critic":        "claude-opus-5",
    "findings_corrector":     "claude-opus-5",
    "norm_verify":            "claude-opus-5",
    "norm_fix":               "claude-opus-5",
    "norm_requote":           "claude-opus-5",
    "optimization":           "ensemble/claude-codex-opt",
    "optimization_critic":    "claude-sonnet-5",
    "optimization_corrector": "claude-sonnet-5",
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
OBJECTS_FILE_PATH            = (
    Path(os.environ["AUDIT_OBJECTS_FILE"]).resolve()
    if os.environ.get("AUDIT_OBJECTS_FILE")
    else APP_DATA_DIR / "objects.json"
)
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

# Календарный месячный бюджет платных API. Это отчётный лимит для dashboard:
# в отличие от дневного guard он не блокирует вызовы, а показывает остаток и
# перерасход. Переопределяется через env для разных бюджетов окружений.
PAID_API_MONTHLY_LIMIT_USD    = _env_float("PAID_API_MONTHLY_LIMIT_USD", 250.0)

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


def reload_stage_model_config() -> dict[str, str]:
    """Перечитать `stage_models.json` В ТОТ ЖЕ словарь.

    Конфиг моделей читается ОДИН РАЗ на импорте модуля, и это верно для
    центра: файл там появляется задолго до старта backend. Для удалённой ноги
    это не так — снимок `stage_models.json` кладётся в `AUDIT_APP_DATA_DIR`
    уже ПОСЛЕ старта процесса, и попадёт он в конфигурацию только если модуль
    ещё не был импортирован. То есть применение снимка зависело от порядка
    импортов: добавление любого нового шага, который трогает конфигурацию
    раньше, молча возвращало прогон на дефолты кода — в том числе на
    `ensemble/gpt-codex`, который ходит в OpenRouter по HTTPS.

    Обновление идёт МУТАЦИЕЙ существующего словаря: часть модулей держит на
    него прямую ссылку, и переприсваивание имени их бы не затронуло.
    """
    fresh = _load_stage_model_config()
    STAGE_MODEL_CONFIG.clear()
    STAGE_MODEL_CONFIG.update(fresh)
    return dict(STAGE_MODEL_CONFIG)

BLOCK_BATCH_MODE_FINDINGS_ONLY = "findings_only_block_context"
_LEGACY_STAGE_BATCH_MODES = {
    "block_batch": {"findings_only_gemma_pair": BLOCK_BATCH_MODE_FINDINGS_ONLY},
}

_STAGE_BATCH_MODE_DEFAULTS: dict[str, str] = {
    "block_batch": BLOCK_BATCH_MODE_FINDINGS_ONLY,
}

STAGE_BATCH_MODE_CHOICES: dict[str, list[str]] = {
    "block_batch": [BLOCK_BATCH_MODE_FINDINGS_ONLY],
}

_STAGE_BATCH_MODES_FILE = STAGE_BATCH_MODES_FILE_PATH


def normalize_stage_batch_mode(stage: str, mode: str) -> str:
    """Map persisted legacy mode IDs to the canonical runtime ID."""
    return _LEGACY_STAGE_BATCH_MODES.get(stage, {}).get(mode, mode)


def _load_stage_batch_modes() -> dict[str, str]:
    config = dict(_STAGE_BATCH_MODE_DEFAULTS)
    if _STAGE_BATCH_MODES_FILE.exists():
        try:
            with open(_STAGE_BATCH_MODES_FILE, "r", encoding="utf-8") as f:
                saved = json.load(f)
            for stage, mode in saved.items():
                normalized = normalize_stage_batch_mode(stage, mode)
                if stage in config and normalized in STAGE_BATCH_MODE_CHOICES.get(stage, []):
                    config[stage] = normalized
            print(f"[config] Stage batch modes loaded from {_STAGE_BATCH_MODES_FILE.name}")
        except Exception as e:
            print(f"[config] Failed to load stage_batch_modes.json: {e}")
    return config


def _save_stage_batch_modes() -> None:
    try:
        _STAGE_BATCH_MODES_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(_STAGE_BATCH_MODES_FILE, "w", encoding="utf-8") as f:
            json.dump(STAGE_BATCH_MODES, f, ensure_ascii=False, indent=2)
            f.write("\n")
    except Exception as e:
        print(f"[config] Failed to save stage_batch_modes.json: {e}")


STAGE_BATCH_MODES: dict[str, str] = _load_stage_batch_modes()


def get_stage_batch_mode(stage: str) -> str:
    return STAGE_BATCH_MODES.get(
        stage,
        _STAGE_BATCH_MODE_DEFAULTS.get(stage, BLOCK_BATCH_MODE_FINDINGS_ONLY),
    )


def set_stage_batch_mode(stage: str, mode: str) -> bool:
    """Возвращает True если режим установлен (валиден), иначе False."""
    if stage not in STAGE_BATCH_MODE_CHOICES:
        return False
    mode = normalize_stage_batch_mode(stage, mode)
    if mode not in STAGE_BATCH_MODE_CHOICES[stage]:
        return False
    STAGE_BATCH_MODES[stage] = mode
    _save_stage_batch_modes()
    return True


# Codex exec transport for classic agent stages. Stage model IDs use the
# `codex/<model>` namespace so they do not collide with OpenRouter model IDs.
CODEX_MODEL_DEFAULT = os.environ.get("AUDIT_CODEX_MODEL", "gpt-5.4").strip() or "gpt-5.4"
CODEX_STAGE_MODEL_ID = os.environ.get("AUDIT_CODEX_STAGE_MODEL", f"codex/{CODEX_MODEL_DEFAULT}").strip() or f"codex/{CODEX_MODEL_DEFAULT}"
STAGE02_DUAL_MODEL_ID = "ensemble/gpt-codex"
OPTIMIZATION_DUAL_MODEL_ID = "ensemble/claude-codex-opt"
OPTIMIZATION_ENSEMBLE_CLAUDE_MODEL = (
    os.environ.get("AUDIT_OPTIMIZATION_ENSEMBLE_CLAUDE_MODEL", "claude-opus-5").strip()
    or "claude-opus-5"
)
OPTIMIZATION_ENSEMBLE_CODEX_MODEL = (
    os.environ.get("AUDIT_OPTIMIZATION_ENSEMBLE_CODEX_MODEL", "codex/gpt-5.6-sol").strip()
    or "codex/gpt-5.6-sol"
)
if not OPTIMIZATION_ENSEMBLE_CODEX_MODEL.startswith("codex/"):
    OPTIMIZATION_ENSEMBLE_CODEX_MODEL = f"codex/{OPTIMIZATION_ENSEMBLE_CODEX_MODEL}"
OPTIMIZATION_ENSEMBLE_CODEX_REASONING_EFFORT = (
    os.environ.get("AUDIT_OPTIMIZATION_ENSEMBLE_CODEX_REASONING_EFFORT", "xhigh").strip().lower()
    or "xhigh"
)
# Диспетчеризация стадий идёт строго по префиксу "codex/" (is_codex_model). Значение
# без префикса (перепутали с AUDIT_CODEX_MODEL) попадало в AVAILABLE_MODELS как
# «Codex exec», но в рантайме молча уходило в OpenRouter-ветку с несуществующим id.
if not CODEX_STAGE_MODEL_ID.startswith("codex/"):
    CODEX_STAGE_MODEL_ID = f"codex/{CODEX_STAGE_MODEL_ID}"

# Post-review for the explicit Stage 01 dual detector. The reviewer compares
# both independent finding sets and can inspect the block once more for issues
# missed by both detectors. These switches never affect single-model modes.
STAGE01_DUAL_REVIEW_ENABLED = _env_bool("STAGE01_DUAL_REVIEW_ENABLED", True)
STAGE01_DUAL_GAP_SEARCH_ENABLED = _env_bool(
    "STAGE01_DUAL_GAP_SEARCH_ENABLED", False
)
STAGE01_PROTECTION_TABLE_CHECK_ENABLED = _env_bool(
    "STAGE01_PROTECTION_TABLE_CHECK_ENABLED", False
)
STAGE01_DUAL_REVIEW_MODEL = (
    os.environ.get("STAGE01_DUAL_REVIEW_MODEL", CODEX_STAGE_MODEL_ID).strip()
    or CODEX_STAGE_MODEL_ID
)
if not STAGE01_DUAL_REVIEW_MODEL.startswith("codex/"):
    STAGE01_DUAL_REVIEW_MODEL = CODEX_STAGE_MODEL_ID

# ── Остановка этапа 01 при выпадении ноги ансамбля ────────────────────
# Требование Андрея Ивановича от 06.08.2026: «если хоть одна нога отвалилась —
# завершаем проверку и не продолжаем, выводим комментарий о том, что нога упала».
#
# Зачем. Блок считается успешным, если ответила ХОТЬ ОДНА нога
# (gemma_findings_only.combine_detector_results: `ok = bool(ok_models)`), а
# стадия — если уцелел хоть один блок. Поэтому исчерпание лимита провайдера
# (у codex-пути НЕТ ни распознавания usage limit, ни ретрая — в отличие от
# claude-пути с `_wait_for_rate_limit`) даёт аудит со статусом «выполнено» и
# молча урезанным рекаллом. Исторический пример: 19/22/29.07.2026 GPT-нога
# падала на 37/59/85% блоков из-за исчерпания кредитов OpenRouter — и ни один
# прогон об этом не сообщил.
#
# Признак уже вычисляется, новую детекцию городить не надо: непустой
# `detectors_failed` (он же `partial` в сводке блока).
#
# Дефолт OFF, как у всех флагов проекта: код сохраняет прежнее поведение,
# включение — через .env. THRESHOLD = сколько блоков с выпавшей ногой
# терпим, прежде чем остановиться (1 = буквально «хоть одна»).
STAGE01_ABORT_ON_LEG_FAILURE_ENABLED = _env_bool(
    "STAGE01_ABORT_ON_LEG_FAILURE_ENABLED", False
)
# Инлайн-разбор: _parse_int_env объявлен ниже по файлу и здесь ещё не виден.
try:
    STAGE01_LEG_FAILURE_THRESHOLD = max(
        1, int(os.environ.get("STAGE01_LEG_FAILURE_THRESHOLD", "1") or "1")
    )
except ValueError:
    STAGE01_LEG_FAILURE_THRESHOLD = 1

# Блоки в codex/ensemble режимах идут строго по одному. Блоки независимы, поэтому
# ограничение не смысловое, а страховка от лимитов подписки Codex: один блок в
# ensemble = три вызова (GPT + Codex + review). Дефолт 1 сохраняет прежнее
# поведение; значение >1 включает параллельную обработку блоков.
try:
    STAGE01_CODEX_PARALLELISM = max(
        1, int(os.environ.get("AUDIT_STAGE02_CODEX_PARALLELISM", "1") or "1")
    )
except ValueError:
    STAGE01_CODEX_PARALLELISM = 1

AVAILABLE_MODELS = [
    {"id": "claude-opus-5",              "label": "Opus 5 (CLI)",           "provider": "claude_cli"},
    {"id": "claude-sonnet-5",            "label": "Sonnet 5 (CLI)",         "provider": "claude_cli"},
    {"id": "openai/gpt-5.4",             "label": "GPT-5.4",                "provider": "openrouter"},
    {"id": CODEX_STAGE_MODEL_ID,          "label": "Codex",                  "provider": "codex_cli"},
    {"id": STAGE02_DUAL_MODEL_ID,         "label": "GPT + Codex",            "provider": "ensemble"},
    {"id": OPTIMIZATION_DUAL_MODEL_ID,     "label": "Claude + Codex (OPT)",   "provider": "optimization_ensemble"},
]

STAGE_MODEL_RESTRICTIONS = {
    "block_batch": [
        "openai/gpt-5.4",
        CODEX_STAGE_MODEL_ID,
        STAGE02_DUAL_MODEL_ID,
    ],
    "optimization": [
        "claude-opus-5",
        "claude-sonnet-5",
        "openai/gpt-5.4",
        CODEX_STAGE_MODEL_ID,
        OPTIMIZATION_DUAL_MODEL_ID,
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
    "block_batch": "Stage 01: GPT-5.4, Codex или независимый двойной проход GPT + Codex с единым контекстом PDF/Vectograph.",
    "findings_merge": "Минимум Opus CLI — межблочная сверка требует сильной модели.",
    "findings_critic": "GPT-5.4 оптимален: быстро и дёшево.",
    "findings_corrector": "Минимум Opus CLI. Sonnet не успевает (таймаут). GPT-5.4 — альтернатива.",
    "norm_verify": "Opus CLI обязателен: MCP norms — единственный источник. WebSearch запрещён.",
    "norm_fix": "Opus CLI обязателен: MCP norms для поиска замены. WebSearch запрещён.",
    "optimization": "Opus CLI, Codex exec или параллельный Claude + Codex с объединением и дедупликацией.",
    "optimization_critic": "GPT-5.4 или Sonnet CLI.",
    "optimization_corrector": "GPT-5.4 или Sonnet CLI.",
}

def get_stage_model(stage: str) -> str:
    """Получить модель для этапа: сперва ЗАМОРОЖЕННЫЙ план, потом конфиг.

    Порядок появился на 11J и закрывает KI-11I-3. `STAGE_MODEL_CONFIG` —
    ГЛОБАЛЬНОЕ мутабельное состояние процесса: оператор, переключивший пресет
    из интерфейса, менял маршрут уже идущего задания, потому что таблица
    читалась в момент старта КАЖДОГО этапа, а не в момент запуска аудита.

    Если у текущего прогона есть замороженный план (привязан к задаче на
    центре либо к процессу на воркере) и он однозначно называет пару
    «провайдер + способность» для этого этапа — маршрут берётся оттуда, и
    переключение пресета на него уже не влияет. Плана нет — поведение
    прежнее, дословно.

    Отказ плана здесь fail-soft НАМЕРЕННО. `get_stage_model` зовут из
    десятков мест, включая пути, где `backend.app.services` может быть не
    импортируем (офлайн-скрипты); падение здесь означало бы, что план,
    задуманный как уточнение, ломает работавшие сценарии.
    """
    stage_key = stage
    if stage.startswith("block_batch"):
        stage_key = "block_batch"
    try:
        from backend.app.services.audit_routing import center_models

        planned = center_models.stage_model_from_plan(stage_key)
    except Exception:                                   # noqa: BLE001 — см. докстринг
        planned = ""
    if planned:
        return planned
    return STAGE_MODEL_CONFIG.get(stage_key, "openai/gpt-5.4")

def is_claude_stage(stage: str) -> bool:
    """Проверить, должен ли этап выполняться через Claude CLI."""
    model = get_stage_model(stage)
    return model.startswith("claude-")


def is_codex_model(model: str | None) -> bool:
    """True для модели classic pipeline, запускаемой через `codex exec`."""
    return bool((model or "").strip().startswith("codex/"))


def is_optimization_ensemble_model(model: str | None) -> bool:
    """True для параллельного Claude + Codex режима этапа OPT."""
    return (model or "").strip() == OPTIMIZATION_DUAL_MODEL_ID


def resolve_codex_model(model: str | None) -> str:
    """Преобразовать stage model id `codex/<model>` в реальный model id Codex CLI."""
    raw = (model or "").strip()
    if raw.startswith("codex/"):
        raw = raw.split("/", 1)[1]
    return raw or CODEX_MODEL_DEFAULT


def is_codex_stage(stage: str) -> bool:
    """Проверить, должен ли этап выполняться через Codex exec."""
    return is_codex_model(get_stage_model(stage))


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
    if model == STAGE02_DUAL_MODEL_ID or is_codex_model(model):
        return 1
    return MAX_PARALLEL_BATCHES

RATE_LIMIT_THRESHOLD_PCT = 90
RATE_LIMIT_CHECK_INTERVAL = 60
RATE_LIMIT_MAX_WAIT = 5 * 3600

# Разбежка пробуждений после rate limit между параллельными проектами.
# Без неё все ждущие проекты стартуют в одну секунду и мгновенно вылетают
# в лимит снова. При одном проекте не влияет ни на что (waiters=0).
RATE_LIMIT_STAGGER_SEC = int(os.environ.get("RATE_LIMIT_STAGGER_SEC", "30") or "30")
RATE_LIMIT_MAX_RETRIES = 5

ANTHROPIC_PLAN = "Max 20x"
WINDOW_5H_TOKEN_LIMIT = 12_000_000
WEEKLY_TOKEN_LIMIT = 17_000_000
CLAUDE_SESSIONS_DIR = Path.home() / ".claude" / "projects"

WEEKLY_RESET_WEEKDAY = 6    # воскресенье UTC = понедельник МСК
WEEKLY_RESET_HOUR_UTC = 21  # 21:00 UTC вс = 00:00 MSK пн («с утра понедельника»)

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
# Гуманизация ссылок на блоки в текстах замечаний: после findings_merge
# внутренние block_id («6L97-3VTH-XTC») в problem/description/solution/risk
# заменяются подписями «Название» (лист N, стр. PDF M) из 01_blocks_analysis /
# document_graph; найденные в тексте ID переносятся в related_block_ids.
# Детерминированно, офлайн, идемпотентно, fail-soft. ON по умолчанию —
# сторонний эксперт не должен видеть внутренние идентификаторы.
FINDINGS_BLOCK_CAPTIONS_ENABLED = _env_bool("FINDINGS_BLOCK_CAPTIONS_ENABLED", True)
# Stage 01: вернуть блоку КОНТЕКСТ ЛИСТА (условные обозначения, примечания,
# спецификации, ведомость) + анти-FP оговорку про границы фрагмента.
# Причина: build_block_user_text подавал page_text, но source router (канонический
# путь, 72/73 блока на АИ2) затирает user_text целиком своим вектор-текстом блока —
# page_text молча терялся, хотя system-промпт обещает модели «текстовый контекст
# страницы». Замер на 133-23-ГК-АИ2: 27/35 страниц (77%) имеют непустой page_text,
# и ВСЕ 27 содержат легенду/примечания/спецификацию — ровно то, на отсутствие чего
# блоки репортили 63% documentation-шума («расшифровка отсутствует», «звёздочка не
# расшифрована»), при том что «Условные обозначения → Размер, обязательный к
# выполнению» лежит на том же листе. Цена возврата ~222 токена/блок (~16K на прогон,
# 0.7% от 2.4М). Асимметрия-улика: единственный image_only-блок контекст ПОЛУЧАЛ.
# A/B на 133-23-ГК-АИ2: полный контекст снизил эвристический documentation-шум
# с 84 до 36 кандидатов. Поэтому безопасный контекст листа теперь штатный.
STAGE01_PAGE_CONTEXT_ENABLED = _env_bool("STAGE01_PAGE_CONTEXT_ENABLED", True)
# Stage 01: evidence-first publication gate. Кандидаты без достаточного контекста
# не теряются, а сохраняются в deferred_findings с детерминированными причинами.
STAGE01_EVIDENCE_GATE_ENABLED = _env_bool("STAGE01_EVIDENCE_GATE_ENABLED", True)
# Stage 01: максимум опубликованных находок на один блок (ПОСЛЕ evidence-фильтров и
# дедупа). 0 или отрицательное = БЕЗ ограничения. Кап снят 2026-07-24: замер показал,
# что прежний потолок 3 отсекал реальные высокоуверенные находки (10 находок conf
# 0.85–0.97 на 7 блоках уходили в deferred с reason block_finding_cap). Quality-гейт
# (confidence≥0.80, claim_type, evidence) и дедуп по (problem_class, affected_entity)
# продолжают работать — снимается только числовой потолок. (_env_int определён ниже,
# поэтому парсим напрямую.)
def _parse_int_env(_name: str, _default: int) -> int:
    _raw = os.environ.get(_name)
    if _raw is None or not _raw.strip():
        return _default
    try:
        return int(_raw.strip())
    except ValueError:
        return _default


STAGE01_BLOCK_MAX_FINDINGS = _parse_int_env("STAGE01_BLOCK_MAX_FINDINGS", 0)
# Детерминированный shadow/observe-only поиск OCR-гомоглифов и ложных
# «не указано» по точному PDF-векторному слою. Ничего не удаляет и не меняет
# решение evidence gate; только добавляет аудируемый receipt. До замера OFF.
FINDING_EVIDENCE_OCR_OBSERVER_ENABLED = _env_bool(
    "FINDING_EVIDENCE_OCR_OBSERVER_ENABLED", False
)
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
# «Вектограф» shadow-режим (observe-only): на стадии gemma_enrichment прогоняет гейт качества
# по image-блокам и пишет _output/vectograf_shadow.json — «какие блоки Вектограф взял бы вместо
# Gemma-описания» + метрики/причины. Поведение пайплайна НЕ меняет; телеметрия для решения о
# реальной замене. Дёшево: не-схемы отсеиваются структурером за мс (PDF не открывается),
# однолинейка ~1.2 с. ON по умолчанию (observe-only), env — kill-switch.
VECTOGRAF_SHADOW_ENABLED = _env_bool("VECTOGRAF_SHADOW_ENABLED", True)
# Детерминированная подсветка цитируемых обозначений по fitz text layer.
# Главный switch OFF: прод не меняется. При первом включении остаётся shadow —
# считаются coverage/IoU и пишется textlayer_highlights_shadow.json, но
# 03_findings.json не меняется. Live заполняет только пустые highlight_regions;
# перезапись LLM-регионов требует отдельного явного switch.
PIPELINE_TEXTLAYER_HIGHLIGHTS_ENABLED = _env_bool(
    "PIPELINE_TEXTLAYER_HIGHLIGHTS_ENABLED", False
)
PIPELINE_TEXTLAYER_HIGHLIGHTS_SHADOW = _env_bool(
    "PIPELINE_TEXTLAYER_HIGHLIGHTS_SHADOW", True
)
PIPELINE_TEXTLAYER_HIGHLIGHTS_OVERRIDE_EXISTING = _env_bool(
    "PIPELINE_TEXTLAYER_HIGHLIGHTS_OVERRIDE_EXISTING", False
)
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
# Разворот порядка конвейера: блоки (Stage 01, GPT) идут ПЕРЕД текстом (Stage 02, Opus).
# Новый порядок: gemma → block_analysis → block_retry → text_analysis → findings_merge.
# Текст становится финальным синтезатором: читает компактный view блоков (01_blocks_for_text.json)
# и сверяет свои T-замечания с блоками (items_verified_from_blocks) вместо обратной сверки.
# ДЕФОЛТ True: нумерация артефактов (блоки=01, текст=02) отражает именно этот порядок.
# Флаг оставлен как временный escape-hatch; при False (старый порядок text→block) нумерация
# 01/02 становится несогласованной с фактическим порядком (legacy-режим).
PIPELINE_BLOCKS_BEFORE_TEXT_ENABLED = _env_bool("PIPELINE_BLOCKS_BEFORE_TEXT_ENABLED", True)
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
GPT_MODEL = "openai/gpt-5.4"

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
    "block_batch":            GPT_MODEL,
    "findings_merge":         GPT_MODEL,
    "findings_critic":        GPT_MODEL,
    "findings_corrector":     GPT_MODEL,
    "norm_verify":            GPT_MODEL,
    "norm_fix":               GPT_MODEL,
    "optimization":           GPT_MODEL,
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

# ─── Журнал действий (action log) ────────────────────────────────────────────
# Сквозной журнал всех действий в системе для последующего анализа ошибок:
#   * HTTP-запросы портала (кто из инженеров что сделал, статус, длительность);
#   * переходы этапов конвейера (запущен/завершён/упал/прерван);
#   * WARNING/ERROR из стандартного logging всех модулей backend.
# Пишется в суточные append-only JSONL-файлы ACTION_LOG_DIR/actions-YYYY-MM-DD.jsonl.
# Чтение: GET /api/action-log и scripts/analyze_action_log.py.
# Default ON: журнал fail-soft, ошибки записи не ломают основной поток.
ACTION_LOG_ENABLED          = _env_bool("ACTION_LOG_ENABLED", True)
# Куда писать: default — logs/actions в DATA_DIR (в prod-deploy данные прибиты
# к MAIN через AUDIT_DATA_DIR, журнал — тоже данные, не код).
ACTION_LOG_DIR = Path(os.environ["AUDIT_ACTION_LOG_DIR"]).resolve() if os.environ.get("AUDIT_ACTION_LOG_DIR") else DATA_DIR / "logs" / "actions"
# Сколько дней хранить суточные файлы (старше — удаляются при смене дня).
ACTION_LOG_RETENTION_DAYS   = _env_int("ACTION_LOG_RETENTION_DAYS", 180)
# Kill-switch на каждый источник событий отдельно.
ACTION_LOG_HTTP_ENABLED     = _env_bool("ACTION_LOG_HTTP_ENABLED", True)
ACTION_LOG_PIPELINE_ENABLED = _env_bool("ACTION_LOG_PIPELINE_ENABLED", True)
ACTION_LOG_APPLOG_ENABLED   = _env_bool("ACTION_LOG_APPLOG_ENABLED", True)
# Потолок объёма суточного файла (байт): при превышении события дропаются до
# следующего дня (пишется одно маркер-событие day_cap_reached). Защита диска
# от штормов (например, поллинг с протухшей сессией = 401 каждую секунду).
ACTION_LOG_MAX_DAY_BYTES    = _env_int("ACTION_LOG_MAX_DAY_BYTES", 256 * 1024 * 1024)
# Потолок событий app_log (мост logging) в минуту: сверх — дроп, по закрытии
# окна пишется одно агрегированное событие о числе подавленных. Защита от
# логгера, заголосившего WARNING в цикле.
ACTION_LOG_APPLOG_MAX_PER_MIN = _env_int("ACTION_LOG_APPLOG_MAX_PER_MIN", 600)
# Дополнительные шумовые пути (CSV из regex) — исключаются из HTTP-журнала
# в дополнение к встроенному списку поллинговых GET (см. action_log.py).
ACTION_LOG_NOISE_EXTRA      = _env_csv("ACTION_LOG_NOISE_EXTRA", [])

# ─── Эфемерные кропы блоков (block crop store) ─────────────────────────────
# Кропы блоков — крупнейшая устранимая статья на диске (замер 2026-08-03:
# 12.2 ГБ / 64764 PNG при диске на 98%). Они полностью воспроизводимы из
# локального 02_work/document.pdf по crop_px: контрольный ре-рендер дал ровно
# render_size из index.json при 99.52% совпадения пикселей с облачным кропом.
#
# Порядок восстановления НАМЕРЕННО local-first: облачные crop_url живут
# per-generation (решение от 13-14.07.2026, crop_cache.py), и замер показал,
# что 15% ссылок в корпусе уже мертвы. Локальный PDF не протухает.
#
# ВКЛЮЧАТЬ СТРОГО В ПОРЯДКЕ RESTORE → EVICTION: пока восстановление не
# проверено в бою, удалять кропы нельзя.
BLOCK_CROP_RESTORE_ENABLED   = _env_bool("BLOCK_CROP_RESTORE_ENABLED", False)
# Разрешён ли сетевой рунд (crop_url) как ЗАПАСНОЙ источник после локального.
BLOCK_CROP_RESTORE_ALLOW_NETWORK = _env_bool("BLOCK_CROP_RESTORE_ALLOW_NETWORK", True)
# Порядок источников восстановления: local_pdf | crop_url.
BLOCK_CROP_RESTORE_ORDER     = _env_csv("BLOCK_CROP_RESTORE_ORDER", ["local_pdf", "crop_url"])
# Параллелизм восстановления и бюджет на один HTTP-запрос (сек).
# fitz CPU-bound и не потокобезопасен — держим низким.
BLOCK_CROP_RESTORE_CONCURRENCY = _env_int("BLOCK_CROP_RESTORE_CONCURRENCY", 2)
BLOCK_CROP_RESTORE_BUDGET_S  = _env_int("BLOCK_CROP_RESTORE_BUDGET_S", 2)
BLOCK_CROP_RESTORE_TIMEOUT_S = _env_int("BLOCK_CROP_RESTORE_TIMEOUT_S", 30)
# LRU-кэш восстановленных кропов. ВНЕ деревьев проектов: один общий потолок
# удержим только над одним пулом, и кэш не должен уезжать в бэкапы версий.
BLOCK_CROP_CACHE_DIR = (
    Path(os.environ["AUDIT_BLOCK_CROP_CACHE_DIR"]).resolve()
    if os.environ.get("AUDIT_BLOCK_CROP_CACHE_DIR")
    else DATA_DIR / "cache" / "block_crops"
)
BLOCK_CROP_CACHE_MAX_BYTES      = _env_int("BLOCK_CROP_CACHE_MAX_BYTES", 1_500_000_000)
BLOCK_CROP_CACHE_MAX_FILE_BYTES = _env_int("BLOCK_CROP_CACHE_MAX_FILE_BYTES", 64 * 1024 * 1024)
# Пол свободного места: ниже него не пишем в кэш вовсе (диск живёт на пределе).
BLOCK_CROP_CACHE_MIN_FREE_BYTES = _env_int("BLOCK_CROP_CACHE_MIN_FREE_BYTES", 2_000_000_000)
# Запись моложе этого возраста не вытесняется: агент, восстановивший пачку
# кропов, иначе потеряет первые до того, как их прочитает codex_runner.
BLOCK_CROP_CACHE_MIN_AGE_S      = _env_int("BLOCK_CROP_CACHE_MIN_AGE_S", 900)
# Как часто запускать вытеснение (раз в N вставок).
BLOCK_CROP_CACHE_SWEEP_EVERY    = _env_int("BLOCK_CROP_CACHE_SWEEP_EVERY", 50)

# Эвакуация кропов после ПОЛНОГО завершения пайплайна.
# Оба флага нужны одновременно: включённый EVICTION при DRY_RUN=True (default)
# только пишет в лог, что было бы удалено.
BLOCK_CROP_EVICTION_ENABLED  = _env_bool("BLOCK_CROP_EVICTION_ENABLED", False)
BLOCK_CROP_EVICTION_DRY_RUN  = _env_bool("BLOCK_CROP_EVICTION_DRY_RUN", True)
# 03_analysis/latest — тёплая локальная копия для UI; трогать только осознанно.
BLOCK_CROP_EVICT_LATEST      = _env_bool("BLOCK_CROP_EVICT_LATEST", False)

# ─── Распределённые audit-worker (этап 0: вертикальный срез) ────────────────
# Подсистема выдачи заданий сторонним VPS. Этап 0 умеет ТОЛЬКО безопасное
# тестовое задание `test_pipeline_v1` — реальный аудит, Claude/Codex и
# нормативный этап не подключены (см. docs/distributed_audit_workers/).
#
# Kill-switch. При false: роутеры не регистрируются, SQLite-база НЕ создаётся,
# фоновых задач нет, экран отдаёт «функция отключена». Существующий конвейер
# при любом значении флага не затрагивается — точек врезки в PipelineManager
# на этом этапе нет вовсе.
DISTRIBUTED_WORKERS_ENABLED = _env_bool("DISTRIBUTED_WORKERS_ENABLED", False)

# Корень состояния подсистемы: workers.db + пакеты + логи заданий.
# ВНЕ projects_v2 и вне деревьев проектов — база не должна уезжать в архивы.
DISTRIBUTED_WORKERS_DATA_DIR = (
    Path(os.environ["DISTRIBUTED_WORKERS_DATA_DIR"]).resolve()
    if os.environ.get("DISTRIBUTED_WORKERS_DATA_DIR")
    else Path("/var/lib/auditmanager/distributed_workers")
)

# Пороги оси СВЯЗИ (не исполнения!). Молчание воркера меняет только
# connection_status и никогда не переводит задание в failed.
DISTRIBUTED_WORKERS_HEARTBEAT_STALE_SEC   = _env_int("DISTRIBUTED_WORKERS_HEARTBEAT_STALE_SEC", 90)
DISTRIBUTED_WORKERS_HEARTBEAT_OFFLINE_SEC = _env_int("DISTRIBUTED_WORKERS_HEARTBEAT_OFFLINE_SEC", 600)

# Потолок размера пакета в любую сторону (защита диска и от «бомбы»).
DISTRIBUTED_WORKERS_MAX_PACKAGE_BYTES = _env_int(
    "DISTRIBUTED_WORKERS_MAX_PACKAGE_BYTES", 2 * 1024 * 1024 * 1024
)
# Размер чанка загрузки результата. Держать НИЖЕ nginx client_max_body_size
# (на проде 200M), иначе загрузка результата не пролезет вовсе.
DISTRIBUTED_WORKERS_UPLOAD_CHUNK_BYTES = _env_int(
    "DISTRIBUTED_WORKERS_UPLOAD_CHUNK_BYTES", 32 * 1024 * 1024
)
# Потолок ожидания в long-poll выдачи задания.
DISTRIBUTED_WORKERS_LONG_POLL_SEC = _env_int("DISTRIBUTED_WORKERS_LONG_POLL_SEC", 25)
# Потолок суммарной длительности тестового задания (валидируется воркером).
DISTRIBUTED_WORKERS_TEST_JOB_MAX_SEC = _env_int("DISTRIBUTED_WORKERS_TEST_JOB_MAX_SEC", 300)

# Операторский контур /api/workers/* защищён ТОЛЬКО портальной авторизацией:
# собственной у него нет. При PORTAL_AUTH_ENABLED=false он оказался бы открыт
# всем, а ручка rotate-token отдаёт живой токен воркера открытым текстом.
# Поэтому по умолчанию такое сочетание запрещено; для локального пилота есть
# явный выключатель — как и для http:// на стороне воркера.
DISTRIBUTED_WORKERS_ALLOW_INSECURE_ADMIN = _env_bool(
    "DISTRIBUTED_WORKERS_ALLOW_INSECURE_ADMIN", False
)

# ─── Удалённое исполнение аудита (этап ExecutionBackend) ────────────────────
# Отдельный флаг: включение подсистемы воркеров НЕ включает реальный аудит.
# Пока он false, единственный активный backend — LocalExecutionBackend, и
# поведение платформы полностью прежнее.
DISTRIBUTED_AUDIT_EXECUTION_ENABLED = _env_bool(
    "DISTRIBUTED_AUDIT_EXECUTION_ENABLED", False
)
# Профиль пилотного удалённого аудита. Фиксированный, один: несколько почти
# одинаковых профилей — верный способ получить расхождение поведения.
REMOTE_AUDIT_PROFILE = "remote_audit_pilot_v1"
# Ревизия кода конвейера. Центр и воркер обязаны совпасть, иначе одинаковые
# входные данные дадут разные артефакты. Пусто = «не объявлена», и тогда
# удалённый запуск запрещён.
AUDIT_PIPELINE_REVISION = os.environ.get("AUDIT_PIPELINE_REVISION", "").strip()
# Сколько НАСТОЯЩИХ аудитов один воркер выполняет одновременно. Доказанный
# максимум этапа — 1. Значение больше зажимается: два тестовых задания на
# одном VPS ничего не говорят о двух реальных аудитах.
AUDIT_WORKER_REAL_AUDIT_MAX_SLOTS = min(
    1, max(0, _env_int("AUDIT_WORKER_REAL_AUDIT_MAX_SLOTS", 1))
)
# Разрешены ли НАСТОЯЩИЕ Claude/Codex на воркере. Центр этот флаг только
# читает из capability воркера и показывает оператору; включается он на самом
# воркере. Здесь — значение для собственных проверок и тестов.
AUDIT_WORKER_ALLOW_REAL_LLM = _env_bool("AUDIT_WORKER_ALLOW_REAL_LLM", False)

# ─── Ограничение частоты заявок на регистрацию ──────────────────────────────
# Эндпоинт /api/v1/worker/register публичный (воркер приходит сам), и до этого
# этапа перебор bootstrap-секрета не ограничивался ничем. Счётчики живут в
# workers.db, а не в памяти: рестарт backend делает вотчдог, и защита, которая
# обнуляется рестартом, — это её отсутствие.
DISTRIBUTED_WORKERS_REGISTRATION_RATE_WINDOW_SEC = _env_int(
    "DISTRIBUTED_WORKERS_REGISTRATION_RATE_WINDOW_SEC", 3600
)
# На пару (IP, instance_id). Крэш-луп агента под systemd Restart=always с
# RestartSec=10 даёт 360 попыток в час — но register вызывается один раз при
# установке, а не на каждом старте, поэтому 10 хватает с запасом.
DISTRIBUTED_WORKERS_REGISTRATION_RATE_MAX_PER_INSTANCE = _env_int(
    "DISTRIBUTED_WORKERS_REGISTRATION_RATE_MAX_PER_INSTANCE", 10
)
# На IP целиком: защита от перебора секрета с меняющимся instance_id.
DISTRIBUTED_WORKERS_REGISTRATION_RATE_MAX_PER_IP = _env_int(
    "DISTRIBUTED_WORKERS_REGISTRATION_RATE_MAX_PER_IP", 30
)

# Identity-preserving re-enrollment is deliberately shorter-lived than an
# operator portal session.  Five minutes is enough for the explicitly
# coordinated hand-off to one installation, while keeping a copied token's
# useful lifetime small.  The domain validates the effective value to the
# documented 30..3600 second interval and fails closed outside it.
DISTRIBUTED_WORKERS_IDENTITY_REENROLLMENT_TTL_SEC = _env_int(
    "DISTRIBUTED_WORKERS_IDENTITY_REENROLLMENT_TTL_SEC", 300
)

# ─── Provider auth & quota gate (этап 11) ────────────────────────────────────
# Порог «мало осталось». Значение по умолчанию КОНСЕРВАТИВНОЕ и намеренно
# высокое: 25 % пятичасового окна Codex — это уже мало для полного аудита
# раздела, и лучше предупредить рано, чем показать «готов» за десять минут до
# упора в лимит. Состояние `low` без настроенного порога не вычисляется вовсе
# (§12 задания): порог живёт здесь и только здесь.
DISTRIBUTED_WORKERS_QUOTA_LOW_THRESHOLD_PCT = _env_int(
    "DISTRIBUTED_WORKERS_QUOTA_LOW_THRESHOLD_PCT", 25
)
# Через сколько снимок квоты на ЦЕНТРЕ считается протухшим. Больше воркерского
# `stale_after`: heartbeat мог не дойти, а данные ещё не устарели по существу.
DISTRIBUTED_WORKERS_QUOTA_STALE_SEC = _env_int(
    "DISTRIBUTED_WORKERS_QUOTA_STALE_SEC", 3600
)
# История квот. Ограничена и по времени, и по числу строк: без второго предела
# сбойный воркер, шлющий меняющиеся значения, раздул бы таблицу за сутки.
DISTRIBUTED_WORKERS_QUOTA_HISTORY_RETENTION_DAYS = _env_int(
    "DISTRIBUTED_WORKERS_QUOTA_HISTORY_RETENTION_DAYS", 120
)
DISTRIBUTED_WORKERS_QUOTA_HISTORY_MAX_ROWS_PER_ACCOUNT = _env_int(
    "DISTRIBUTED_WORKERS_QUOTA_HISTORY_MAX_ROWS_PER_ACCOUNT", 5000
)
# Минимальный интервал между записями истории для одной пары воркер+провайдер.
# Запись всё равно происходит при СМЕНЕ состояния — интервал ограничивает
# только повторы одного и того же (§24: не хранить каждую 30-секундную запись).
DISTRIBUTED_WORKERS_QUOTA_HISTORY_MIN_INTERVAL_SEC = _env_int(
    "DISTRIBUTED_WORKERS_QUOTA_HISTORY_MIN_INTERVAL_SEC", 900
)

# Версия протокола центр↔воркер. Целое; растёт при несовместимом изменении API.
DISTRIBUTED_WORKERS_PROTOCOL_VERSION = 1
# Версия схемы package_manifest.json.
DISTRIBUTED_WORKERS_MANIFEST_VERSION = 1

# Обратная совместимость: BASE_DIR → ROOT_DIR
BASE_DIR = ROOT_DIR


def clean_cli_cwd_root() -> str:
    """Корень «чистых» рабочих каталогов для запуска `claude -p` вне репозитория.

    Раньше путь был литералом `/tmp/sonnet_clean` в ТРЁХ местах. На центральном
    хосте это безобидно, а на воркере — запись мимо каталога попытки: общий
    `/tmp` виден всем заданиям и переживает попытку целиком.

    `tempfile.gettempdir()` читает `TMPDIR`, который воркер уже уводит внутрь
    каталога попытки, поэтому изоляция получается без нового рубежа. На центре
    `TMPDIR` обычно не задан → `/tmp/sonnet_clean`, то есть прежний путь
    буквально. `AUDIT_CLEAN_CWD_ROOT` оставлен явным override для случаев,
    когда `TMPDIR` менять нельзя.
    """
    import tempfile

    raw = (os.environ.get("AUDIT_CLEAN_CWD_ROOT") or "").strip()
    if raw:
        return raw
    # `tempfile.gettempdir()` КЭШИРУЕТ результат первого вызова в
    # `tempfile.tempdir`, поэтому один только он читал бы `TMPDIR`, каким тот
    # был на импорте, — а воркер выставляет его позже.
    base = (os.environ.get("TMPDIR") or "").strip() or tempfile.gettempdir()
    return os.path.join(base, "sonnet_clean")


def codex_workdir() -> str:
    """Рабочий каталог (`-C` и `cwd`) процесса `codex exec`.

    Значение по умолчанию сохраняет прежнее поведение — корень установленного
    кода. Но песочница Codex по умолчанию `workspace-write`, то есть рабочий
    каталог ЗАПИСЫВАЕМ агентом; на чужом VPS это означало бы право писать в
    каталог установленного кода. `AUDIT_CODEX_WORKDIR` уводит его внутрь
    каталога попытки, ничего не меняя на центре.
    """
    raw = (os.environ.get("AUDIT_CODEX_WORKDIR") or "").strip()
    return raw or str(ROOT_DIR)
