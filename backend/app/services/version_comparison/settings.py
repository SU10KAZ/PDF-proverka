"""Параметры сравнения версий (читаются из окружения при каждом вызове)."""
from __future__ import annotations

import os

# Версии входят в ключи кэша. PROFILE_VERSION — профиль страницы и
# сопоставление листов (меняется при правке profile/stamp/lineage);
# ALGO_VERSION — результат пары листов (меняется при правке engine/separation/
# registration). Раздельно, чтобы правка наложения не пересчитывала профили.
PROFILE_VERSION = "vc1.1"
ALGO_VERSION = "vc1.11"


def _float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, "").strip() or default)
    except ValueError:
        return default


def _int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, "").strip() or default)
    except ValueError:
        return default


def enabled() -> bool:
    raw = (os.environ.get("VERSION_COMPARISON_ENABLED") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def dpi() -> int:
    return max(72, min(400, _int("VERSION_COMPARISON_DPI", 200)))


def tolerance_pt() -> float:
    return max(0.0, _float("VERSION_COMPARISON_TOL_PT", 0.5))


def workers() -> int:
    return max(1, _int("VERSION_COMPARISON_WORKERS", 4))


# Пороги маски «краски»: тёмные линии и светлые элементы (заливки, штриховки).
DARK_THRESHOLD = 200
LIGHT_THRESHOLD = 250
# Канал цвета: разница цветности a*b* (Lab) на общих линиях и яркости L во
# внутренних пикселях линий (без сглаженных краёв).
COLOR_DELTA = 25
LUMA_DELTA = 60
# Шум: компонента не больше стольких пикселей, примыкающая к общей линии.
NOISE_MAX_PX = 2
# Светлый элемент «только в A/B»: насколько он темнее самого тёмного пикселя
# другой версии в радиусе допуска (отсекает дрожание сглаживания у порога).
LIGHT_DELTA = 10
# Оформление линии: тёмная линия одной версии, на месте которой в другой есть
# только светлая или тонкая линия. Светлая область шире стольких пикселей —
# заливка, а не линия: тёмная линия поверх неё считается удалённой/добавленной.
STYLE_FILL_PX = 7
# Порядок наложения: цвет на пересечении сменился, а цвет почти каждой такой
# точки есть рядом в другой версии (расстояние в Lab 8-бит OpenCV; доля точек).
OVERDRAW_DELTA = 25
OVERDRAW_MIN_SHARE = 0.6
# Перемещённый блок: не меньше стольких слов с общим вектором (±допуск, pt),
# длина вектора не меньше BLOCK_MIN_SHIFT_PT; доля пикселей «только A/B» блока,
# объяснённых сдвигом, — не меньше BLOCK_MIN_IOU (остальное — изменения внутри).
BLOCK_MIN_WORDS = 5
BLOCK_TOL_PT = 1.0
BLOCK_MIN_SHIFT_PT = 3.0
BLOCK_MIN_IOU = 0.6
# «Надпись без текстового слоя»: знак не шире GLYPH_MAX_ASPECT своих высот
# (широкие курсивные «т», «ш» — до 1,6; дуга облака изменений — 3–4); залитое
# пятно (доля пикселей рамки больше GLYPH_MAX_FILL) — не знак; строка только
# из прямых штрихов (толщина не больше GLYPH_STROKE_RATIO длины) — штриховка
# или параллельные линии, а не надпись.
GLYPH_MAX_ASPECT = 2.0
GLYPH_MAX_FILL = 0.85
GLYPH_STROKE_RATIO = 0.25
# Регион — сдвиг, если такая доля его пикселей объясняется вектором сдвига соседних надписей.
SHIFT_EXPLAINED_SHARE = 0.9
# …и лежит не дальше стольких мм от сдвинутой надписи или блока.
SHIFT_NEAR_MM = 5.0
# Свой сдвиг знака ищется в пределах стольких pt от сдвига соседней надписи.
SHIFT_REFINE_PT = 6.0
# Группировка пикселей отличий в регион (закрытие), мм.
REGION_CLOSE_MM = 1.5
# Слово «на том же месте» — в пределах допуска сравнения; дальше, но не дальше
# WORD_MOVE_PT — «сдвиг надписи» (тот же текст), иначе «только в A / только в B».
WORD_MOVE_PT = 20.0
# Сопоставление листов: пороги оценки.
MATCH_HIGH = 0.55
MATCH_MEDIUM = 0.35
MATCH_CANDIDATE_FLOOR = 0.20
# Выравнивание по опорным словам.
ANCHOR_MIN_INLIERS = 8
ANCHOR_MIN_RATIO = 0.15
ANCHOR_MAX_MEDIAN_RESIDUAL_PT = 0.6
ANCHOR_RANSAC_PT = 2.0
# Контроль выравнивания: доля совпадения масок (Dice) при 36 dpi.
ALIGN_MIN_DICE = 0.5
# Совмещение «как есть» заменяет найденное по словам, если Dice выше хотя бы на столько.
ALIGN_SWITCH_MARGIN = 0.1
# Отделение предупреждений: доля площади листа у кандидата-картинки.
WARNING_MIN_AREA_SHARE = 0.20
