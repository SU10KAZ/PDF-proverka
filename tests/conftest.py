"""Общая тест-конфигурация.

Тесты не должны зависеть от production `.env`. В частности, портальная
аутентификация (`PORTAL_AUTH_ENABLED=true` в prod) ломает TestClient-тесты,
которые ходят в API без логина. Жёстко выключаем её ДО импорта приложения.

`backend/app/main.py` грузит `.env` через `os.environ.setdefault(...)`, поэтому
переменная, выставленная здесь раньше импорта, не перезаписывается значением из
`.env`.
"""
import os

os.environ["PORTAL_AUTH_ENABLED"] = "false"

# Rollout-флаги stage_comparison (могут быть включены в prod `.env` для
# контролируемого прогона) нейтрализуем по умолчанию — тесты должны проверять
# поведение при ДЕФОЛТНЫХ значениях, а не подхватывать `.env`. Тест, которому
# нужен флаг ON, выставляет его сам через monkeypatch.setenv. Выставляем ДО
# импорта приложения, чтобы `os.environ.setdefault` в main.py не перезаписал.
for _rollout_flag in (
    "STAGE_COMPARISON_GRSH_FEEDER_EXTRACTION_ENABLED",
    "STAGE_COMPARISON_GRAPHIC_STRUCTURED_EXTRACTION_ENABLED",
    "STAGE_COMPARISON_BLOCK_PDF_SOURCE_ENABLED",
):
    os.environ[_rollout_flag] = "false"
