"""Реэкспорт native-верификации цитат из канонической корневой копии (#38-dedup).

Единый источник — `norms/_native_verify.py` (#34: NORMS_TOOLS_PATH → in-repo
norms/tools; #35: числовая чувствительность). Тонкий алиас для обратной
совместимости импортов `backend.app.pipeline.stages.norms._native_verify`."""
import sys

import norms._native_verify as _canonical

sys.modules[__name__] = _canonical
