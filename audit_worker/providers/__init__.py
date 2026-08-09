"""Слой provider identity воркера: установка, авторизация, наблюдение лимитов.

Что этот пакет делает и, главное, чего он НЕ делает.

Делает: спрашивает у локально установленных CLI ровно те сведения, которые
провайдер отдаёт ОФИЦИАЛЬНО и БЕЗ обращения к модели, — версию, состояние
авторизации и (там, где такой интерфейс существует) остаток лимита. Приводит
разнородные ответы к одному нормализованному виду и отдаёт его агенту, который
кладёт его в heartbeat.

Не делает:
  * не читает содержимое credential-файлов — только факт существования, режим
    и владельца;
  * не выполняет ни одного запроса к модели без отдельного явного флага
    (`AUDIT_WORKER_ALLOW_REAL_PROVIDER_PROBE`), и даже с ним — не автоматически;
  * не выдумывает проценты: отсутствие официального источника даёт
    `quota_state="unknown"` и `remaining_pct=None`, а не ноль и не сто;
  * не ходит в веб-кабинет, не разбирает cookie и не трогает недокументированные
    HTTP-эндпоинты провайдеров.

Разделение провайдеров — не удобство, а требование: у Claude и Codex РАЗНЫЕ
изолированные HOME, и подпроцесс одного провайдера не получает переменные
второго. Окружение строится с нуля белым списком (как в `audit_runner.build_env`),
поэтому ни worker-token, ни адрес центра, ни ключи платных API до CLI не
доезжают.
"""
from __future__ import annotations

from audit_worker.providers.auth_mode import (
    AUTH_MODE_AMBIENT_USER,
    AUTH_MODE_ISOLATED_PROVIDER_HOME,
    AUTH_MODE_UNAVAILABLE,
    AUTH_MODES,
    DEFAULT_AUTH_MODE,
    UnknownAuthMode,
    normalize_auth_mode,
    require_auth_mode,
)
from audit_worker.providers.errors import (
    PROVIDER_ERROR_CODES,
    classify_exception,
    classify_process_result,
)
from audit_worker.providers.identity import ProviderIdentity
from audit_worker.providers.paths import (
    PROVIDER_CODEX,
    PROVIDER_CLAUDE,
    SUPPORTED_PROVIDERS,
    ProviderHome,
    provider_home,
    providers_root,
)
from audit_worker.providers.quota import (
    CONFIDENCE_LEVELS,
    QUOTA_SOURCE_PRIORITY,
    QUOTA_STATES,
    ProviderQuotaSnapshot,
    QuotaWindow,
)

__all__ = [
    "AUTH_MODES",
    "AUTH_MODE_AMBIENT_USER",
    "AUTH_MODE_ISOLATED_PROVIDER_HOME",
    "AUTH_MODE_UNAVAILABLE",
    "CONFIDENCE_LEVELS",
    "DEFAULT_AUTH_MODE",
    "PROVIDER_CLAUDE",
    "PROVIDER_CODEX",
    "PROVIDER_ERROR_CODES",
    "QUOTA_SOURCE_PRIORITY",
    "QUOTA_STATES",
    "ProviderHome",
    "ProviderIdentity",
    "ProviderQuotaSnapshot",
    "QuotaWindow",
    "SUPPORTED_PROVIDERS",
    "UnknownAuthMode",
    "classify_exception",
    "classify_process_result",
    "normalize_auth_mode",
    "provider_home",
    "providers_root",
    "require_auth_mode",
]
