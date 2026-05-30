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
