"""Централизованный реестр имён артефактов ранних этапов пайплайна.

История: анализ блоков исторически назывался `02_blocks_analysis.json`, а текст —
`01_text_analysis.json`. Но в проде блоки считаются ПЕРЕД текстом
(`PIPELINE_BLOCKS_BEFORE_TEXT_ENABLED`), поэтому номера свапнуты, чтобы отражать
порядок выполнения: блоки = `01`, текст = `02`.

Правила использования:
- **писатели** всегда пишут канонические (новые) имена из констант ниже;
- **читатели** резолвят путь через :func:`resolve_existing` — новое имя в приоритете,
  legacy читается как fallback (для ещё не мигрированных данных на диске);
- **проверка результата только что выполненного этапа** должна брать канонический путь
  НАПРЯМУЮ (без fallback), иначе устаревший legacy-файл замаскирует ошибку записи.

Ключи статуса `has_01_text_analysis`/`has_02_blocks_analysis` и рабочие каталоги
кропов (`blocks_stage02_100`) намеренно НЕ переименовываются — они самосогласованы
и не лежат на диске как имена этих JSON-артефактов.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ── Канонические (новые) имена ──────────────────────────────────────────────
TEXT_ANALYSIS_FILENAME = "02_text_analysis.json"
BLOCKS_ANALYSIS_FILENAME = "01_blocks_analysis.json"
BLOCKS_FOR_TEXT_FILENAME = "01_blocks_for_text.json"

# ── Канонические значения поля `stage` внутри JSON ──────────────────────────
TEXT_ANALYSIS_STAGE = "02_text_analysis"
BLOCKS_ANALYSIS_STAGE = "01_blocks_analysis"
BLOCKS_FOR_TEXT_STAGE = "01_blocks_for_text"

# ── Ключ метаданных внутри блокового JSON ───────────────────────────────────
BLOCKS_META_KEY = "stage01_meta"
BLOCKS_META_KEY_LEGACY = "stage02_meta"

# canonical filename -> legacy filename (только для чтения существующих данных)
LEGACY_ALIASES = {
    TEXT_ANALYSIS_FILENAME: "01_text_analysis.json",
    BLOCKS_ANALYSIS_FILENAME: "02_blocks_analysis.json",
    BLOCKS_FOR_TEXT_FILENAME: "02_blocks_for_text.json",
}

# legacy stage-value -> canonical (для нормализации при чтении поля `stage`)
STAGE_VALUE_ALIASES = {
    "01_text_analysis": TEXT_ANALYSIS_STAGE,
    "02_blocks_analysis": BLOCKS_ANALYSIS_STAGE,
    "02_blocks_for_text": BLOCKS_FOR_TEXT_STAGE,
}

# Оба имени (каноническое + legacy) — для списков очистки/бэкапа: чтобы re-audit
# удалял и старый теневой файл, не давая ему замаскировать новый прогон.
TEXT_ANALYSIS_ALL_NAMES = (TEXT_ANALYSIS_FILENAME, "01_text_analysis.json")
BLOCKS_ANALYSIS_ALL_NAMES = (BLOCKS_ANALYSIS_FILENAME, "02_blocks_analysis.json")
BLOCKS_FOR_TEXT_ALL_NAMES = (BLOCKS_FOR_TEXT_FILENAME, "02_blocks_for_text.json")


def resolve_existing(dir_path, name: str) -> Path:
    """Вернуть путь к артефакту `name` в `dir_path`, учитывая legacy-имя.

    Приоритет — каноническое (новое) имя; если его нет, но есть legacy-алиас —
    вернуть legacy. Если нет ни того, ни другого — вернуть канонический путь
    (несуществующий), чтобы вызывающий код штатно отработал `.exists() == False`.

    Если присутствуют ОБА файла и их размеры различаются — залогировать конфликт
    (каноническое всё равно побеждает); это сигнал незавершённой миграции.
    """
    d = Path(dir_path)
    canonical = d / name
    legacy_name = LEGACY_ALIASES.get(name)
    if canonical.exists():
        if legacy_name:
            legacy = d / legacy_name
            try:
                if legacy.exists() and legacy.stat().st_size != canonical.stat().st_size:
                    logger.warning(
                        "stage_artifacts: canonical %s и legacy %s оба присутствуют и "
                        "различаются по размеру в %s — используется каноническое",
                        name, legacy_name, d,
                    )
            except OSError:
                pass
        return canonical
    if legacy_name:
        legacy = d / legacy_name
        if legacy.exists():
            return legacy
    return canonical


def normalize_stage_value(value: Optional[str]) -> Optional[str]:
    """Привести legacy-значение поля `stage` к каноническому (или вернуть как есть)."""
    if not value:
        return value
    return STAGE_VALUE_ALIASES.get(value, value)
