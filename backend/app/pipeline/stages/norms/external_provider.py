"""Реэкспорт провайдера статусов норм из канонической корневой копии (#38-dedup).

Единый источник — `norms/external_provider.py` (резолвит status_index в
in-repo norms/tools/status_index.json, authoritative 565). Тонкий алиас для
обратной совместимости импортов
`backend.app.pipeline.stages.norms.external_provider`."""
import sys

import norms.external_provider as _canonical

sys.modules[__name__] = _canonical
