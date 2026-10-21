"""Настройки ИИ-описаний (читаются из окружения при каждом вызове).

Стартовые значения — план §13. Всё, что влияет на содержимое пакета или
ответа, входит в ``digest()`` и тем самым в ключ кэша.
"""
from __future__ import annotations

import hashlib
import json
import os

# Версии входят в ключ кэша. Номер — намерение автора («изменил смысл»),
# отпечаток содержимого (промпт, схема) добавляется отдельно.
BUILDER_VERSION = "vcai-b2"
PROMPT_VERSION = "vcai-p2"
SCHEMA_VERSION = "vcai-s1"


def _int(name: str, default: int, low: int, high: int) -> int:
    try:
        value = int((os.environ.get(name) or "").strip() or default)
    except ValueError:
        value = default
    return max(low, min(high, value))


def _float(name: str, default: float) -> float:
    try:
        return float((os.environ.get(name) or "").strip() or default)
    except ValueError:
        return default


def _bool(name: str, default: bool) -> bool:
    raw = (os.environ.get(name) or "").strip().lower()
    if not raw:
        return default
    return raw not in {"0", "false", "no", "off"}


def switch_file():
    """Переключатель модуля в каталоге данных «Сравнения версий» (``{"enabled": true}``).

    Нужен, потому что окружение боевой службы правит только root. Читается при каждом
    вызове: включение и выключение — без перезапуска. Переменная окружения главнее файла.
    """
    from backend.app.services.version_comparison import paths
    return paths.root() / "ai_settings.json"


def enabled() -> bool:
    """Передача документов внешнему ИИ. Выключено, пока не включено явно."""
    if (os.environ.get("VERSION_COMPARISON_AI_ENABLED") or "").strip():
        return _bool("VERSION_COMPARISON_AI_ENABLED", False)
    try:
        return json.loads(switch_file().read_text(encoding="utf-8")).get("enabled") is True
    except (OSError, ValueError, AttributeError):
        return False


def enabled_source() -> str:
    if (os.environ.get("VERSION_COMPARISON_AI_ENABLED") or "").strip():
        return "переменная окружения VERSION_COMPARISON_AI_ENABLED"
    return f"файл {switch_file()}" if switch_file().exists() else "по умолчанию (выключено)"


def executor() -> str:
    return (os.environ.get("VC_AI_EXECUTOR") or "codex_exec").strip()


def model() -> str:
    return (os.environ.get("VC_AI_MODEL") or "gpt-6-astra").strip()


def effort() -> str:
    return (os.environ.get("VC_AI_EFFORT") or "medium").strip()


def concurrency() -> int:
    """Одновременных запросов модуля. Свой семафор, не общий с аудитами."""
    return _int("VC_AI_CONCURRENCY", 1, 1, 8)


def call_timeout_s() -> int:
    return _int("VC_AI_CALL_TIMEOUT_S", 900, 60, 7200)


def json_retries() -> int:
    """Повторы при ответе не по схеме. Квота и авторизация не повторяются никогда."""
    return _int("VC_AI_JSON_RETRIES", 2, 0, 5)


def chunk_records() -> int:
    return _int("VC_AI_CHUNK_RECORDS", 40, 5, 400)


def chunk_images() -> int:
    return _int("VC_AI_CHUNK_IMAGES", 12, 2, 40)


def crop_margin_pt() -> float:
    return _float("VC_AI_CROP_MARGIN_PT", 18.0)


def cluster_gap_pt() -> float:
    """Записи ближе этого расстояния попадают в одну вырезку (если вместе влезают в её предел).

    16.10.2026: 24 → 400 pt. На ЭМ2 V11→V12 при 24 pt типичная вырезка была 16 × 3 мм —
    941 изображение и 89 обращений на 9 листов; при 400 pt — 153 и 37 (план §16).
    """
    return _float("VC_AI_CLUSTER_GAP_PT", 400.0)


def max_image_side() -> int:
    return _int("VC_AI_MAX_IMAGE_SIDE", 2000, 400, 6000)


def text_px() -> float:
    """Желаемая высота строки текста на вырезке, px."""
    return _float("VC_AI_TEXT_PX", 14.0)


def dpi_range() -> tuple[int, int]:
    return _int("VC_AI_DPI_MIN", 150, 72, 600), _int("VC_AI_DPI_MAX", 400, 72, 800)


# Формат листа (длинная × короткая сторона, мм), который уходит модели целиком: лист A, лист B
# и наложение — по одному изображению, записи указаны рамками. Решение Андрея Ивановича 16.10.2026:
# A3 и меньше — одним листом. Риск (план §16): на A3 при стороне 2000 px надпись 2,5 мм ≈ 12 px.
WHOLE_SHEET_FORMATS = {"off": None, "A4": (297.0, 210.0), "A3": (420.0, 297.0)}
WHOLE_SHEET_TOLERANCE = 1.03


def whole_sheet_format() -> str:
    raw = (os.environ.get("VC_AI_WHOLE_SHEET") or "A3").strip()
    return raw if raw in WHOLE_SHEET_FORMATS else "A3"


def whole_sheet(width_pt: float, height_pt: float) -> bool:
    """Помещается ли страница в формат «целиком» (ориентация любая)."""
    lim = WHOLE_SHEET_FORMATS[whole_sheet_format()]
    if not lim:
        return False
    long_mm, short_mm = (v * 25.4 / 72.0 for v in sorted((width_pt, height_pt), reverse=True))
    return long_mm <= lim[0] * WHOLE_SHEET_TOLERANCE and short_mm <= lim[1] * WHOLE_SHEET_TOLERANCE


def xref_hits() -> int:
    """Совпадений межлистового поиска на одну марку."""
    return _int("VC_AI_XREF_HITS", 3, 0, 20)


def include_engine_classified() -> bool:
    """Передавать ли ИИ сдвиги и «оформление линий» (по умолчанию — нет)."""
    return _bool("VC_AI_INCLUDE_ENGINE_CLASSIFIED", False)


def text_beyond_pt() -> float:
    """Регион «текст» с рамкой шире связанных надписей на эту величину содержит графику."""
    return _float("VC_AI_TEXT_BEYOND_PT", 2.0)


def packet_settings() -> dict:
    lo, hi = dpi_range()
    return {
        "builder": BUILDER_VERSION, "chunk_records": chunk_records(), "chunk_images": chunk_images(),
        "crop_margin_pt": crop_margin_pt(), "cluster_gap_pt": cluster_gap_pt(),
        "max_image_side": max_image_side(), "text_px": text_px(), "dpi": [lo, hi], "xref_hits": xref_hits(),
        "whole_sheet": whole_sheet_format(),
        "include_engine_classified": include_engine_classified(), "text_beyond_pt": text_beyond_pt(),
    }


# ------------------------------------------------------------------ способ «Изменения в листе»
# Версии способа sheet_changes входят в ключ его результатов; прежний способ (records) их не видит.
SC_BUILDER_VERSION = "vcsc-b1"
SC_PROMPT_VERSION = "vcsc-p1"
SC_SCHEMA_VERSION = "vcsc-s1"
SC_VALIDATOR_VERSION = "vcsc-v1"
SC_DEFAULT_MODEL = "gpt-6.1-sol"     # выбор Андрея Ивановича 16.10.2026; подмены моделью нет
SC_DEFAULT_EFFORT = "high"


def sc_versions() -> dict:
    return {"name": "sheet_changes", "builder": SC_BUILDER_VERSION, "prompt": SC_PROMPT_VERSION,
            "schema": SC_SCHEMA_VERSION, "validator": SC_VALIDATOR_VERSION}


def _sc_file() -> dict:
    """Секция ``sheet_changes`` файла ``ai_settings.json`` (окружение службы правит только root)."""
    try:
        data = json.loads(switch_file().read_text(encoding="utf-8")).get("sheet_changes")
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError, AttributeError):
        return {}


def _sc(name: str, env: str, default: str = "") -> str:
    raw = (os.environ.get(env) or "").strip()
    if raw:
        return raw
    value = _sc_file().get(name)
    return str(value).strip() if value not in (None, "") else default


def sc_model() -> str:
    return _sc("model", "VC_SC_MODEL", SC_DEFAULT_MODEL)


def sc_effort() -> str:
    return _sc("effort", "VC_SC_EFFORT", SC_DEFAULT_EFFORT)


def sc_codex_bin() -> str:
    """Путь CLI только для этого способа; пусто — общий выбор шлюза (он не меняется)."""
    return _sc("codex_bin", "VC_SC_CODEX_BIN", "")


def sc_parallel() -> int:
    """Листов способа «Изменения в листе» одновременно: освободилось место — сразу берётся следующий лист."""
    try:
        return max(1, min(50, int(_sc("parallel", "VC_SC_PARALLEL", "1"))))
    except ValueError:
        return 1


# 20.10.2026: 10 переходов АР4 разом — 54–73 % обращений «429 Too Many Requests»; три запуска листа
# сгорали за секунды. Отказ провайдера пережидается паузой и не расходует запуски листа.
# 21.10.2026: 429 у провайдера длились дольше 3 мин — лист сгорал (ОВ3); теперь пережидается до ~18 мин,
# после пауз повторы тоже идут с паузой последнего значения, а не через секунды.
SC_DEFAULT_RETRY_DELAYS = (20, 60, 120, 300, 600)


def sc_retry_delays() -> tuple[int, ...]:
    """Паузы (с) перед повтором обращения листа после временного отказа провайдера (TRANSIENT, в т. ч. 429)."""
    raw = _sc("retry_delays_s", "VC_SC_RETRY_DELAYS", ",".join(map(str, SC_DEFAULT_RETRY_DELAYS)))
    try:
        return tuple(max(0, min(600, int(x))) for x in raw.strip("[]() ").split(",") if x.strip())[:5]
    except ValueError:
        return SC_DEFAULT_RETRY_DELAYS


def sc_context_rounds() -> int:
    try:
        return max(0, min(1, int(_sc("context_rounds", "VC_SC_CONTEXT_ROUNDS", "1"))))
    except ValueError:
        return 1


def sc_snapshot() -> dict:
    return sc_versions() | {"model": sc_model(), "effort": sc_effort(), "codex_bin": sc_codex_bin() or None,
                            "context_rounds": sc_context_rounds(), "parallel": sc_parallel(), "service_tier": None,
                            "retry_delays_s": list(sc_retry_delays())}


# ------------------------------------------------------------------ «Проверить сомнительные пары через Луну»
# Сопоставление спорных листов моделью (version_comparison_ai/sheet_match.py). Задание — вариант v3 пилота
# 18.10.2026: смена шифра не довод, ответ = номер кандидата + страница, подпись впечатана в картинку.
SM_PROMPT_VERSION = "vcsm-p3"
SM_DEFAULT_MODEL = "gpt-6-luna"
SM_DEFAULT_EFFORT = "high"
SM_DEFAULT_PARALLEL = 30        # решение Андрея Ивановича 18.10.2026: 30 обращений одновременно
SM_IMAGE_WIDTH = 1800           # ширина картинки листа, px (как в пилоте и пробном прогоне)


def _sm_file() -> dict:
    """Секция ``sheet_match`` файла ``ai_settings.json``; чего нет — берётся из ``sheet_changes`` (тот же CLI)."""
    try:
        data = json.loads(switch_file().read_text(encoding="utf-8")).get("sheet_match")
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError, AttributeError):
        return {}


def _sm(name: str, env: str, default: str = "") -> str:
    raw = (os.environ.get(env) or "").strip()
    if raw:
        return raw
    value = _sm_file().get(name)
    return str(value).strip() if value not in (None, "") else default


def sm_model() -> str:
    return _sm("model", "VC_SM_MODEL", SM_DEFAULT_MODEL)


def sm_effort() -> str:
    return _sm("effort", "VC_SM_EFFORT", SM_DEFAULT_EFFORT)


def sm_codex_bin() -> str:
    return _sm("codex_bin", "VC_SM_CODEX_BIN", "") or sc_codex_bin()


def sm_parallel() -> int:
    """Обращений к модели одновременно (свой предел, не общий с «Изменениями в листе»)."""
    try:
        return max(1, min(50, int(_sm("parallel", "VC_SM_PARALLEL", str(SM_DEFAULT_PARALLEL)))))
    except ValueError:
        return SM_DEFAULT_PARALLEL


# ------------------------------------------------------------------ полное сравнение спецификаций
# 19.10.2026: при 50 одновременных обращениях Codex отвечал «429 Too Many Requests» (ЭО2: 10 из 10, ЭО4: 4 из 4).
WS_DEFAULT_PARALLEL = 4
WS_DEFAULT_RETRY_DELAYS = (20, 60, 120)


def _ws(name: str, env: str, default: str = "") -> str:
    """Секция ``whole_specifications`` файла ``ai_settings.json`` или окружение."""
    raw = (os.environ.get(env) or "").strip()
    if raw:
        return raw
    try:
        data = json.loads(switch_file().read_text(encoding="utf-8")).get("whole_specifications")
        value = data.get(name) if isinstance(data, dict) else None
    except (OSError, ValueError, AttributeError):
        value = None
    if isinstance(value, list):
        return ",".join(str(x) for x in value)
    return str(value).strip() if value not in (None, "") else default


def ws_parallel() -> int:
    """Версий спецификации, отправляемых модели одновременно."""
    try:
        return max(1, min(50, int(_ws("parallel", "VC_WS_PARALLEL", str(WS_DEFAULT_PARALLEL)))))
    except ValueError:
        return WS_DEFAULT_PARALLEL


def ws_break_after() -> int:
    """Полные спецификации: сколько версий подряд без единой годной не прошли проверку формы СО, чтобы остальные
    версии модели не отправлять (их листы СО описывает листовой ИИ). 0 — без досрочной остановки."""
    try:
        return max(0, min(50, int(_ws("break_after", "VC_WS_BREAK_AFTER", "3"))))
    except ValueError:
        return 3


def ws_transient_rounds() -> int:
    """Повторов этапа после временного отказа провайдера (429), прежде чем этап считается ошибкой."""
    try:
        return max(0, min(5, int(_ws("transient_rounds", "VC_WS_TRANSIENT_ROUNDS", "3"))))
    except ValueError:
        return 3


def ws_transient_wait_s() -> int:
    try:
        return max(0, min(3600, int(_ws("transient_wait_s", "VC_WS_TRANSIENT_WAIT_S", "600"))))
    except ValueError:
        return 600


def ws_retry_delays() -> tuple[int, ...]:
    """Паузы (с) перед повтором обращения после временного отказа провайдера (TRANSIENT, в т. ч. 429)."""
    raw = _ws("retry_delays_s", "VC_WS_RETRY_DELAYS", ",".join(map(str, WS_DEFAULT_RETRY_DELAYS)))
    try:
        return tuple(max(0, min(600, int(x))) for x in raw.split(",") if x.strip())[:5]
    except ValueError:
        return WS_DEFAULT_RETRY_DELAYS


def sm_snapshot() -> dict:
    return {"prompt": SM_PROMPT_VERSION, "model": sm_model(), "effort": sm_effort(), "codex_bin": sm_codex_bin() or None,
            "parallel": sm_parallel(), "image_width": SM_IMAGE_WIDTH}


def digest(data: object) -> str:
    raw = json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def snapshot() -> dict:
    return {"enabled": enabled(), "enabled_source": enabled_source(), "executor": executor(), "model": model(), "effort": effort(),
            "concurrency": concurrency(), "call_timeout_s": call_timeout_s(), "json_retries": json_retries(),
            "packet": packet_settings(), "prompt_version": PROMPT_VERSION, "schema_version": SCHEMA_VERSION}
