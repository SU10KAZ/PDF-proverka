"""Реэкспорт норм-ядра из канонической корневой копии (reserc.md #38-dedup).

Единый источник истины — `norms/_core.py` (корень репозитория): там лежат
ДАННЫЕ норм (norms_db.json / norms_paragraphs.json / tools / vault) и правки
#34-#37 (числовая чувствительность цитат, доверенный native-кеш, метрики).

Раньше здесь была вторая, разъехавшаяся копия — её правили #34-#37, но live-путь
(`stages.runner` → `from norms import …`) использует КОРЕНЬ, поэтому те правки не
действовали. Теперь это тонкий алиас на корневой модуль: импорт
`backend.app.pipeline.stages.norms._core` отдаёт ровно `norms._core`."""
import sys

import norms._core as _canonical

sys.modules[__name__] = _canonical
