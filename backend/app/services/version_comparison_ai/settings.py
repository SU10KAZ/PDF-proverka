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


def digest(data: object) -> str:
    raw = json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def snapshot() -> dict:
    return {"enabled": enabled(), "enabled_source": enabled_source(), "executor": executor(), "model": model(), "effort": effort(),
            "concurrency": concurrency(), "call_timeout_s": call_timeout_s(), "json_retries": json_retries(),
            "packet": packet_settings(), "prompt_version": PROMPT_VERSION, "schema_version": SCHEMA_VERSION}
