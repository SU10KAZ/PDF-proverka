"""Конфигурация агента: только env + файлы состояния, никаких зависимостей от backend."""
from __future__ import annotations

import os
import platform
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

from audit_worker import PROTOCOL_VERSION, __version__

#: Максимум вызовов модели, который задание вправе заказать на ОДНУ попытку.
#:
#: Живёт ЗДЕСЬ, а не в провайдерском слое, по границе, которую держит тест
#: `test_pipeline_does_not_import_the_provider_layer`: `audit_runner` проверяет
#: это поле на приёме задания и не имеет права знать о `audit_worker.providers`.
#: Дублировать литерал в двух валидаторах — верный способ развести их со
#: временем, поэтому источник один и он нейтрален к обеим сторонам.
#:
#: Было 8 — число из времён, когда попытка означала ОДИН этап. Полный
#: worker-участок это цепочка: вызов на каждый графический блок плюс текст,
#: свод, оптимизация и её проверка. Граница осталась рубежом: реальный потолок
#: задаёт разрешение оператора, которое списывается до запуска.
#:
#: Было 64 — число из времён, когда мост схлопывал ансамбль этапа 01 в одну
#: ногу. С планом маршрутизации (11I) ансамбль исполняется целиком: три
#: детектора плюс судья, то есть ЧЕТЫРЕ обращения на графический блок. При 64
#: документ из пятнадцати блоков обрывался бы на середине, уже оплатив
#: большую часть вызовов. Рубеж обязан быть выше любого честно посчитанного
#: бюджета, иначе он превращается в скрытый лимит на размер документа.
MAX_INFERENCES_CEILING = 1024

DEFAULT_ROOT = "/var/lib/audit-worker"


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError:
        return default


@dataclass
class WorkerConfig:
    dispatcher_url: str
    root: Path
    display_name: str
    # HTTPS data plane may be exposed independently from registration/control.
    # None preserves the production behavior: package traffic uses dispatcher_url.
    data_plane_base_url: str | None = None
    heartbeat_interval_sec: float = 30.0
    poll_wait_sec: int = 25
    event_flush_interval_sec: float = 1.0
    event_batch_max: int = 500
    max_slots: int = 1
    request_timeout_sec: float = 60.0
    # Потолки тестового задания. Воркер зажимает параметры ПОВТОРНО, даже если
    # центр прислал что-то большее — доверять входу нельзя (§4 задания).
    test_max_total_sec: float = 300.0
    test_max_steps: int = 100
    test_max_result_bytes: int = 8 * 1024 * 1024
    verify_tls: bool = True
    # Dev-режим: разрешает http:// ТОЛЬКО для localhost. Глобального
    # отключения проверки TLS не существует — см. validate_transport_security.
    allow_insecure_localhost: bool = False
    extra_capabilities: dict = field(default_factory=dict)
    # ─── Хранение локальных данных (этап 3.5) ───────────────────────────────
    # Сухой прогон по умолчанию: менеджер считает кандидатов и показывает
    # ожидаемый выигрыш, но НИЧЕГО не стирает, пока удаление не включено явно.
    retention_enabled: bool = True
    retention_delete_enabled: bool = False
    retention_days: int = 30
    retention_scan_interval_sec: int = 3600
    disk_warning_free_bytes: int = 5 * 1024 * 1024 * 1024
    disk_critical_free_bytes: int = 1 * 1024 * 1024 * 1024
    # Автозапуск локального исполнителя вместе с агентом. Только для dev:
    # в проде это два systemd-юнита, и рестарт агента не трогает исполнителя.
    dev_spawn_executor: bool = False
    # ─── Реальный аудит (этап ExecutionBackend) ─────────────────────────────
    # Отпечаток кода конвейера на этом VPS. Центр сверяет его со своим и не
    # показывает несовместимый воркер как доступный: разные ревизии дают
    # разные артефакты при одинаковых входных данных.
    pipeline_revision: str | None = None
    # Каталог с установленным кодом платформы (корень репозитория). Из него
    # запускается фиксированный internal runner. Задаётся АДМИНИСТРАТОРОМ VPS,
    # не центром: путь к исполняемому коду не может приходить из задания.
    pipeline_root: Path | None = None
    # Интерпретатор, которым запускается конвейер. Пусто = тот же, что у
    # исполнителя. Задаётся администратором VPS, не центром.
    pipeline_python: str | None = None
    # Приём заданий типа audit_pipeline_v1. Включение подсистемы воркеров этого
    # НЕ включает: реальный аудит требует отдельного осознанного решения.
    audit_pipeline_enabled: bool = False
    # Разрешение запускать НАСТОЯЩИЕ Claude/Codex. По умолчанию запрещено, и
    # это независимо от audit_pipeline_enabled: тестовый прогон реального
    # конвейера не должен тратить подписку.
    allow_real_llm: bool = False
    # Каталог поддельных CLI-провайдеров. Задаётся конфигурацией ВОРКЕРА и
    # никогда полем задания (§17 задания).
    fake_provider_dir: Path | None = None
    # Жёсткий предел одновременных audit_pipeline_v1. Доказанный максимум — 1.
    real_audit_max_slots: int = 1
    # ─── Provider auth & quota gate (этап 11) ───────────────────────────────
    # Наблюдение за установленными CLI провайдеров: версия, авторизация, лимит.
    # Ноль обращений к моделям — включено по умолчанию именно поэтому.
    provider_gate_enabled: bool = True
    # Ритм опроса. Три РАЗНЫЕ частоты: heartbeat не опрашивает провайдеров
    # вовсе, авторизация проверяется чаще лимита, лимит — реже (§17 задания).
    provider_auth_check_interval_sec: float = 300.0
    provider_quota_probe_interval_sec: float = 900.0
    # Через сколько снимок лимита перестаёт считаться действующим. Показать
    # вчерашние проценты как сегодняшние хуже, чем не показать ничего.
    provider_quota_stale_after_sec: float = 1800.0
    provider_timeout_sec: float = 60.0
    # Порог «мало осталось». По умолчанию None: `low` не вычисляется, пока
    # порог не задан явно (§12) — иначе он появился бы в двух местах сразу.
    provider_quota_low_threshold_pct: float | None = None
    # Ручная привязка провайдера воркера к общей учётной записи. Задаётся
    # ОПЕРАТОРОМ; автоматически аккаунт по секретным данным не определяется.
    provider_account_groups: dict = field(default_factory=dict)
    # Запрет провайдера политикой на этом воркере (комплаенс-стоп).
    provider_policy_blocked: dict = field(default_factory=dict)
    # Явные пути к CLI, если администратор VPS поставил их не туда, куда
    # кладёт официальный установщик. Поиска по PATH нет намеренно: в PATH
    # воркера первым идёт каталог ПОДДЕЛЬНЫХ провайдеров.
    provider_executables: dict = field(default_factory=dict)
    # ─── Режим авторизации провайдера (этап 11b) ────────────────────────────
    # ОТКУДА CLI берёт учётные данные: `isolated_provider_home` (умолчание,
    # поведение этапа 11), `ambient_user` (личная авторизация пользователя
    # VPS) или `unavailable`. Задаётся ПОПРОВАЙДЕРНО и только явно: общей
    # переменной нет намеренно — `ambient_user` открывает CLI личный каталог
    # человека, и такое включается на одной машине для одного провайдера, а
    # не «глобально по умолчанию».
    provider_auth_modes: dict = field(default_factory=dict)
    # РАЗРЕШЕНИЕ НА НАСТОЯЩИЙ ВЫЗОВ МОДЕЛИ. Отдельно от allow_real_llm:
    # контрольный запрос этапа 11 и боевой аудит — разные решения.
    allow_real_provider_probe: bool = False
    # ─── Мост «конвейер → ProviderAdapter» (этап 11C) ────────────────────────
    # РАЗРЕШЕНИЕ МАШИНЫ на то, чтобы конвейер вообще мог дотянуться до
    # авторизованного CLI. Третье, независимое от двух прежних решение, и
    # порядок здесь важен: администратор VPS разрешает КАНАЛ (эта настройка),
    # оператор разрешает КОНКРЕТНОЕ задание (файл `allow_synthetic_inference`),
    # центр формулирует ТРЕБОВАНИЕ (`provider_requirement` в задании). Ни одно
    # из трёх само по себе вызова модели не открывает.
    pipeline_provider_bridge_enabled: bool = False
    # Сколько попытка ЖДЁТ разрешения оператора, прежде чем быть отвергнутой.
    # Ноль (умолчание) = прежняя строгость: нет разрешения — сразу отказ.
    # Ненулевое значение делает «ждёт разрешения» отдельным состоянием, а не
    # провалом, — ровно так же, как «ждёт слот». Смысл в том, что разрешение
    # привязано к ЗАДАНИЮ (§9 этапа 11C), а значит выписать его можно только
    # ПОСЛЕ того, как задание создано: до этого момента task_id не существует.
    pipeline_provider_grant_wait_sec: float = 0.0
    # ─── Автоматическое разрешение по заданию центра (этап 11G) ─────────────
    # ЧТО ИМЕННО МЕНЯЕТСЯ. До 11G третье решение из тройки выше — «оператор
    # разрешает КОНКРЕТНОЕ задание» — существовало файлом, который человек
    # создавал руками ПОСЛЕ появления задания. Для диагностических этапов это
    # было правильно: центр не умел сказать, чего он хочет, и подписью под
    # расходом чужой подписки был именно файл. С 11G центр присылает
    # ограниченное требование (провайдер, способность, белый список этапов,
    # потолок обращений), и подпись переносится ТУДА, ГДЕ ЕЙ МЕСТО: на
    # регистрацию воркера у центра плюс эти две настройки машины.
    #
    # Разрешение при этом НЕ исчезает: оно по-прежнему выписывается, ложится на
    # диск ДО вызова модели, привязано к заданию, имеет срок и списывается
    # атомарно. Меняется только автор записи — штатный код исполнителя вместо
    # руки оператора.
    #
    # Обе настройки — рубеж МАШИНЫ, а не задания: ни центр, ни задание изменить
    # их не могут. Нулевой потолок (умолчание) означает «автоматических
    # разрешений нет», и тогда поведение дословно прежнее.
    pipeline_provider_auto_grant_enabled: bool = False
    # Сколько обращений к модели машина готова отдать ОДНОМУ заданию. Требование
    # центра зажимается этим числом сверху: заказать больше, чем разрешил
    # владелец VPS, задание не может.
    pipeline_provider_max_inferences: int = 0
    # Срок жизни автоматического разрешения. Забытое разрешение без срока —
    # открытая дверь, о которой никто не помнит.
    pipeline_provider_grant_ttl_sec: float = 6 * 3600.0
    # Файл со значениями, которых не должно быть в ответе модели (контрольная
    # строка канарейки и т.п.). Содержимое в репозитории не хранится намеренно:
    # хранить контрольную строку в Git значит обесценить её проверку.
    provider_forbidden_literals_file: Path | None = None
    # Подмена сетевого слоя httpx. Только для end-to-end тестов (ASGITransport):
    # настоящий агент против настоящего приложения без сокетов. В проде None.
    transport: object | None = None
    # Control plane. Polling remains the production-compatible default.  gRPC
    # is explicit and never falls back to polling after a stream failure.
    control_transport: Literal["polling", "grpc"] = "polling"
    grpc_target: str | None = None
    grpc_security_mode: Literal["test_insecure", "mtls"] = "test_insecure"
    grpc_ca_bundle_path: Path | None = None
    grpc_client_certificate_path: Path | None = None
    grpc_key_store_dir: Path | None = None
    grpc_key_store_backend: Literal["auto", "linux_permissions", "windows_dpapi"] = "auto"
    grpc_server_identity: str | None = None
    grpc_renew_before_sec: float = 7 * 86400.0
    grpc_renew_jitter: float = 0.2
    grpc_connect_timeout_sec: float = 15.0
    grpc_heartbeat_interval_override_sec: float | None = None
    grpc_reconnect_min_delay_sec: float = 1.0
    grpc_reconnect_max_delay_sec: float = 30.0
    grpc_reconnect_jitter: float = 0.2
    grpc_max_send_message_bytes: int = 1024 * 1024
    grpc_max_receive_message_bytes: int = 1024 * 1024
    grpc_outbound_queue_max: int = 128
    grpc_protocol_versions: tuple[int, ...] = (1,)

    @property
    def state_path(self) -> Path:
        return self.root / "worker_state.json"

    @property
    def token_path(self) -> Path:
        return self.root / "token"

    @property
    def jobs_dir(self) -> Path:
        return self.root / "jobs"

    @property
    def runtime_dir(self) -> Path:
        return self.root / "runtime"

    @property
    def local_db_path(self) -> Path:
        """worker.db — общий транзакционный стык агента и исполнителя."""
        return self.root / "worker.db"

    @property
    def trash_dir(self) -> Path:
        """Локальная корзина: сюда каталог переезжает ДО стирания содержимого."""
        return self.root / "trash"

    @property
    def providers_dir(self) -> Path:
        """Корень provider identity: `<root>/providers/<провайдер>/…`.

        Лежит в каталоге ДАННЫХ, а не кода: обновление и откат релиза воркера
        (`app/<релиз>/` + ссылка `current`) не должны стирать авторизацию.
        Менеджер удержания сюда не заглядывает — он сканирует только `jobs/`.
        """
        return self.root / "providers"

    @property
    def tombstones_dir(self) -> Path:
        """Следы удалённых попыток: hash и сроки остаются, данных нет."""
        return self.runtime_dir / "tombstones"

    def job_dir(self, job_id: str, attempt_id: str) -> Path:
        """Путь строится ТОЛЬКО из UUID (I-11): внешний код проекта сюда не попадает."""
        from audit_worker.paths import attempt_dir

        return attempt_dir(self.jobs_dir, job_id, attempt_id)

    def ensure_dirs(self) -> None:
        # `providers/` в этом списке намеренно НЕТ: его создаёт ProviderManager
        # с режимом 0700. Общий 0750 остальных каталогов слишком широк для
        # учётных данных провайдеров.
        for path in (
            self.root, self.jobs_dir, self.runtime_dir, self.trash_dir,
            self.tombstones_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(self.root, 0o750)
        except OSError:
            pass

    def declared_provider_capabilities(self) -> dict:
        """`{провайдер: [способности]}` из ЛОКАЛЬНОЙ политики моделей.

        Пустой словарь — законный и информативный ответ: он означает «политики
        на машине нет», и центр обязан прочитать это как «задание с вызовами
        сюда выдавать нельзя», а не как «поле не прислали, наверное можно».

        Ошибка чтения политики подавляется намеренно: heartbeat не имеет права
        падать из-за неверного JSON в файле администратора. Отсутствие
        способностей в объявлении и есть сообщение о неисправности, и оно
        приводит к отказу ДО создания задания.
        """
        if not (self.allow_real_llm and self.pipeline_provider_bridge_enabled):
            # В fake-режиме объявлять способности не о чем: моста нет, и
            # «умею strong_audit» было бы неправдой.
            return {}
        try:
            from audit_worker.providers import model_policy

            policy = model_policy.load_policy(self.root)
        except Exception:                            # noqa: BLE001 — heartbeat не падает
            return {}
        out: dict[str, list[str]] = {}
        for (provider, capability) in sorted(policy.capabilities):
            if not self._provider_usable(provider):
                # Способность, записанная в политике, но неисполнимая на этой
                # машине, — худший вид объявления: центр по нему НАЗНАЧИТ
                # задание, соберёт многогигабайтный пакет и узнает правду в
                # середине прогона. Для провайдера с ключом «политика говорит,
                # что умеем» и «умеем» — разные утверждения (§37-B задания).
                continue
            out.setdefault(provider, []).append(capability)
        return out

    def _provider_usable(self, provider: str) -> bool:
        """Может ли воркер ФАКТИЧЕСКИ исполнить вызов этого провайдера.

        Для CLI-провайдеров ответ даёт вход оператора, и проверить его без
        запуска процесса нельзя — поэтому здесь их не трогаем: объявление
        остаётся по политике, как было до 11J. Для провайдера с ключом
        проверка бесплатна и точна (`os.stat` файла), и не сделать её значило
        бы объявлять способность, которой нет.
        """
        try:
            from audit_worker.providers import openrouter_secret, paths

            if not paths.is_http_provider(provider):
                return True
            home = paths.provider_home(self.root, provider)
            return bool(openrouter_secret.probe(home.credential_path).configured)
        except Exception:                            # noqa: BLE001 — heartbeat не падает
            return False

    @staticmethod
    def _endpoints_stubbed() -> bool:
        try:
            from audit_worker.providers import openrouter_adapter

            return bool(openrouter_adapter.stubbed_endpoints_declared())
        except Exception:                            # noqa: BLE001 — heartbeat не падает
            return False

    def capabilities(self) -> dict:
        from audit_worker import slots as _slots

        job_types = ["test_pipeline_v1"]
        if self.audit_pipeline_enabled:
            job_types.append("audit_pipeline_v1")
        caps = {
            # Что воркер УМЕЕТ запускать. В тестовом режиме это поддельные CLI:
            # центр обязан видеть разницу, иначе «аудит прошёл» ничего не значит.
            "providers": (
                ["claude_cli", "codex_cli"] if self.allow_real_llm
                else ["fake_claude_cli", "fake_codex_cli"]
            ),
            "provider_mode": "real" if self.allow_real_llm else "fake",
            "real_llm_enabled": self.allow_real_llm,
            "audit_pipeline_enabled": self.audit_pipeline_enabled,
            "real_audit_max_slots": self.real_audit_max_slots,
            "pipeline_revision": self.pipeline_revision,
            "compressions": ["gzip", "none"],
            "job_types": job_types,
            # Сколько одновременных попыток ПРОВЕРЕНО этой сборкой воркера.
            # Не «сколько хочет оператор» — центр берёт минимум из обоих.
            "max_verified_slots": _slots.MAX_VERIFIED_SLOTS,
            "python": platform.python_version(),
            "os": f"{platform.system()} {platform.release()}",
            "cores": os.cpu_count() or 1,
            "max_package_bytes": 2 * 1024 * 1024 * 1024,
            "worker_package": __version__,
            # Наблюдение за провайдерами (этап 11). Отдельно от `providers`:
            # тот список говорит, ЧТО воркер умеет запускать, а этот — умеет ли
            # он вообще рассказать центру о состоянии установленных CLI.
            "provider_gate_enabled": self.provider_gate_enabled,
            "provider_probe_allowed": self.allow_real_provider_probe,
            # Может ли конвейер этого воркера вообще дойти до авторизованного
            # CLI. Отдельно от `provider_probe_allowed`: контрольный запрос и
            # рабочий вызов из конвейера — разные каналы и разные решения.
            "pipeline_provider_bridge_enabled": self.pipeline_provider_bridge_enabled,
            # Что воркер умеет ПО СМЫСЛУ, а не по имени модели (этап 11G).
            # Центр сверяет с этим полем требование ДО создания задания:
            # иначе отказ «локальная политика не покрывает способность»
            # приходил бы уже после сборки пакета и его выдачи.
            #
            # Точных идентификаторов моделей здесь нет намеренно: они —
            # собственность машины, и центру знать их незачем. Наружу уходит
            # только «claude умеет strong_audit».
            "provider_capabilities": self.declared_provider_capabilities(),
            # Понимает ли воркер ПЛАН МАРШРУТИЗАЦИИ (этап 11I).
            #
            # Объявление обязательно именно потому, что нагрузка задания
            # разбирается закрытым набором полей: воркер прошлой сборки
            # отвергнет незнакомое поле `routing_plan` целиком, ДО исполнения.
            # Центр обязан узнать об этом заранее и не выдавать такому воркеру
            # задание с планом — а не выяснять это по отказу на приёме.
            "routing_plan_v1": True,
            # Умеет ли воркер провайдера БЕЗ CLI (этап 11J). Отдельный флаг, а
            # не вывод из `provider_capabilities`: пустой список способностей
            # OpenRouter означает «ключа нет», а отсутствие флага — «сборка
            # старая и про такого провайдера не знает вовсе». Центру нужны
            # разные сообщения оператору: «выдайте ключ» и «обновите воркер».
            "http_providers_v1": True,
            # Объявлены ли внешние точки этой машины заглушками. Ставит
            # администратор стенда; на боевом воркере значение False.
            #
            # Поле обязано быть ВИДИМЫМ центру: без него прогон на стенде и
            # боевой прогон неотличимы в отчёте, а «аудит прошёл» перестаёт
            # что-либо значить. Оно же — единственное условие, при котором
            # адрес шлюза разрешено увести с официального хоста.
            "provider_endpoints_stubbed": self._endpoints_stubbed(),
            "provider_auto_grant_enabled": self.pipeline_provider_auto_grant_enabled,
            "provider_max_inferences_per_job": self.pipeline_provider_max_inferences,
        }
        caps.update(self.extra_capabilities)
        return caps


def _env_optional_float(name: str) -> float | None:
    """Число или None. Пустая строка = «не задано», а не ноль.

    Разница принципиальна для порога «мало осталось»: ноль означал бы, что
    состояние `low` не наступает никогда, и оператор об этом не узнал бы.
    """
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _env_provider_map(prefix: str, suffix: str) -> dict:
    """Карта провайдер → значение из переменных вида PREFIX_CLAUDE_SUFFIX."""
    from audit_worker.providers.paths import SUPPORTED_PROVIDERS

    out: dict = {}
    for provider in SUPPORTED_PROVIDERS:
        raw = os.environ.get(f"{prefix}_{provider.upper()}_{suffix}", "").strip()
        if raw:
            out[provider] = raw
    return out


def _env_provider_auth_modes() -> dict:
    """Режим авторизации на провайдера. Неизвестное значение — фатально.

    Единственное место во всём разборе конфигурации, где ошибка значения валит
    старт. Причина в цене тихой ошибки именно здесь: остальные настройки при
    опечатке дают заметный сбой (не тот путь, не тот интервал), а эта —
    правдоподобную ложь. Воркер с `AUDIT_WORKER_PROVIDER_CODEX_AUTH_MODE=ambient`
    (без `_user`) молча остался бы в изоляции и честно сообщил бы центру «вход
    не выполнен»; оператор искал бы проблему в подписке, а не в букве.
    """
    from audit_worker.providers.auth_mode import UnknownAuthMode, require_auth_mode

    out: dict = {}
    for provider, raw in _env_provider_map("AUDIT_WORKER_PROVIDER", "AUTH_MODE").items():
        try:
            out[provider] = require_auth_mode(raw)
        except UnknownAuthMode as exc:
            raise SystemExit(
                f"AUDIT_WORKER_PROVIDER_{provider.upper()}_AUTH_MODE: {exc}"
            ) from exc
    return out


def _max_slots_from_env() -> int:
    """Число слотов из окружения, зажатое доказанным максимумом этапа.

    Предупреждение печатается: молчаливое «5 → 2» оставило бы оператора в
    уверенности, что у него пять слотов, и он списал бы простой на что угодно,
    кроме настройки.
    """
    from audit_worker import slots as _slots

    limit = _slots.normalize_max_slots(os.environ.get("AUDIT_WORKER_MAX_SLOTS"))
    if limit.notice:
        print(f"[audit-worker] ВНИМАНИЕ: {limit.notice}", file=sys.stderr)
    return limit.value


def load_config(
    argv_root: str | None = None, *, require_dispatcher: bool = True
) -> WorkerConfig:
    """Собрать конфигурацию.

    `require_dispatcher=False` — для ИСПОЛНИТЕЛЯ: он к центру не ходит, и
    требовать от него адрес центра значило бы намекать, что когда-нибудь
    сходит. Проверка транспорта в этом случае тоже не выполняется.
    """
    root = Path(
        argv_root or os.environ.get("AUDIT_WORKER_ROOT") or DEFAULT_ROOT
    ).expanduser().resolve()
    url = os.environ.get("AUDIT_WORKER_DISPATCHER_URL", "").strip().rstrip("/")
    if not url and require_dispatcher:
        raise SystemExit(
            "AUDIT_WORKER_DISPATCHER_URL не задан. Пример:\n"
            "  export AUDIT_WORKER_DISPATCHER_URL=https://auditmanager.app"
        )
    config = WorkerConfig(
        dispatcher_url=url,
        root=root,
        display_name=os.environ.get("AUDIT_WORKER_NAME", "").strip()
        or f"{platform.node()}",
        data_plane_base_url=(
            os.environ.get("AUDIT_WORKER_DATA_PLANE_BASE_URL", "")
            .strip()
            .rstrip("/")
            or None
        ),
        heartbeat_interval_sec=_env_float("AUDIT_WORKER_HEARTBEAT_SEC", 30.0),
        poll_wait_sec=_env_int("AUDIT_WORKER_POLL_WAIT_SEC", 25),
        max_slots=_max_slots_from_env(),
        extra_capabilities={
            "bootstrap_version": os.environ.get(
                "AUDIT_WORKER_BOOTSTRAP_VERSION", "unknown"
            ).strip() or "unknown",
            "provider_policy_version": _env_int(
                "AUDIT_WORKER_PROVIDER_POLICY_VERSION", 0
            ),
            "provider_policy_sha256": os.environ.get(
                "AUDIT_WORKER_PROVIDER_POLICY_SHA256", ""
            ).strip(),
            "routing_compatibility": [
                item
                for item in os.environ.get(
                    "AUDIT_WORKER_ROUTING_COMPATIBILITY", ""
                ).split(",")
                if item in {"claude_gpt_codex", "codex_exec"}
            ],
        },
        request_timeout_sec=_env_float("AUDIT_WORKER_TIMEOUT_SEC", 60.0),
        test_max_total_sec=_env_float("AUDIT_WORKER_TEST_MAX_SEC", 300.0),
        retention_enabled=_env_bool("AUDIT_WORKER_RETENTION_ENABLED", True),
        # По умолчанию ВЫКЛЮЧЕНО. Включать отдельно и осознанно: неверное
        # правило удаления стоит дороже, чем занятое место.
        retention_delete_enabled=_env_bool(
            "AUDIT_WORKER_RETENTION_DELETE_ENABLED", False
        ),
        retention_days=max(1, _env_int("AUDIT_WORKER_RETENTION_DAYS", 30)),
        retention_scan_interval_sec=max(
            60, _env_int("AUDIT_WORKER_RETENTION_SCAN_INTERVAL_SEC", 3600)
        ),
        disk_warning_free_bytes=_env_int(
            "AUDIT_WORKER_DISK_WARNING_FREE_BYTES", 5 * 1024 * 1024 * 1024
        ),
        disk_critical_free_bytes=_env_int(
            "AUDIT_WORKER_DISK_CRITICAL_FREE_BYTES", 1 * 1024 * 1024 * 1024
        ),
        dev_spawn_executor=_env_bool("AUDIT_WORKER_DEV_SPAWN_EXECUTOR", False),
        pipeline_revision=(
            os.environ.get("AUDIT_WORKER_PIPELINE_REVISION", "").strip() or None
        ),
        pipeline_root=(
            Path(os.environ["AUDIT_WORKER_PIPELINE_ROOT"]).expanduser().resolve()
            if os.environ.get("AUDIT_WORKER_PIPELINE_ROOT", "").strip()
            else None
        ),
        pipeline_python=(
            os.environ.get("AUDIT_WORKER_PIPELINE_PYTHON", "").strip() or None
        ),
        audit_pipeline_enabled=_env_bool("AUDIT_WORKER_AUDIT_PIPELINE_ENABLED", False),
        allow_real_llm=_env_bool("AUDIT_WORKER_ALLOW_REAL_LLM", False),
        fake_provider_dir=(
            Path(os.environ["AUDIT_WORKER_FAKE_PROVIDER_DIR"]).expanduser().resolve()
            if os.environ.get("AUDIT_WORKER_FAKE_PROVIDER_DIR", "").strip()
            else None
        ),
        # Единица — не настройка, а доказанный предел этапа. Значение больше
        # зажимается: заявлять больше без прогона нельзя.
        real_audit_max_slots=min(
            1, max(0, _env_int("AUDIT_WORKER_REAL_AUDIT_MAX_SLOTS", 1))
        ),
        # ─── Provider gate (этап 11) ────────────────────────────────────────
        provider_gate_enabled=_env_bool("AUDIT_WORKER_PROVIDER_GATE_ENABLED", True),
        provider_auth_check_interval_sec=_env_float(
            "AUDIT_WORKER_PROVIDER_AUTH_CHECK_INTERVAL_SEC", 300.0
        ),
        provider_quota_probe_interval_sec=_env_float(
            "PROVIDER_QUOTA_PROBE_INTERVAL_SEC",
            _env_float("AUDIT_WORKER_PROVIDER_QUOTA_PROBE_INTERVAL_SEC", 900.0),
        ),
        provider_quota_stale_after_sec=_env_float(
            "AUDIT_WORKER_PROVIDER_QUOTA_STALE_AFTER_SEC", 1800.0
        ),
        provider_timeout_sec=_env_float("AUDIT_WORKER_PROVIDER_TIMEOUT_SEC", 60.0),
        provider_quota_low_threshold_pct=_env_optional_float(
            "DISTRIBUTED_WORKERS_QUOTA_LOW_THRESHOLD_PCT"
        ),
        provider_account_groups=_env_provider_map(
            "AUDIT_WORKER_PROVIDER", "ACCOUNT_GROUP_ID"
        ),
        provider_policy_blocked={
            name: value.lower() in {"1", "true", "yes", "on"}
            for name, value in _env_provider_map(
                "AUDIT_WORKER_PROVIDER", "POLICY_BLOCKED"
            ).items()
        },
        provider_executables={
            name: Path(value).expanduser()
            for name, value in _env_provider_map(
                "AUDIT_WORKER_PROVIDER", "EXECUTABLE"
            ).items()
        },
        # Опечатка в значении обязана валить старт, а не откатываться к
        # умолчанию: воркер, который «думает» про ambient, а работает в
        # изоляции, отрапортует центру «вход не выполнен» и будет выглядеть
        # сломанным провайдером вместо сломанной настройки.
        provider_auth_modes=_env_provider_auth_modes(),
        # Реальный вызов модели. Умолчание false — и оно НЕ должно зависеть от
        # allow_real_llm: контрольный запрос этапа 11 и боевой аудит решаются
        # отдельно, иначе включение одного тихо включало бы второе.
        allow_real_provider_probe=_env_bool(
            "AUDIT_WORKER_ALLOW_REAL_PROVIDER_PROBE", False
        ),
        pipeline_provider_bridge_enabled=_env_bool(
            "AUDIT_WORKER_PIPELINE_PROVIDER_ENABLED", False
        ),
        pipeline_provider_grant_wait_sec=max(
            0.0, _env_float("AUDIT_WORKER_PIPELINE_PROVIDER_GRANT_WAIT_SEC", 0.0)
        ),
        pipeline_provider_auto_grant_enabled=_env_bool(
            "AUDIT_WORKER_PIPELINE_PROVIDER_AUTO_GRANT_ENABLED", False
        ),
        pipeline_provider_max_inferences=max(
            0,
            min(
                MAX_INFERENCES_CEILING,
                _env_int("AUDIT_WORKER_PIPELINE_PROVIDER_MAX_INFERENCES", 0),
            ),
        ),
        pipeline_provider_grant_ttl_sec=max(
            60.0,
            _env_float("AUDIT_WORKER_PIPELINE_PROVIDER_GRANT_TTL_SEC", 6 * 3600.0),
        ),
        provider_forbidden_literals_file=(
            Path(os.environ["AUDIT_WORKER_PROVIDER_FORBIDDEN_LITERALS_FILE"])
            .expanduser().resolve()
            if os.environ.get(
                "AUDIT_WORKER_PROVIDER_FORBIDDEN_LITERALS_FILE", ""
            ).strip()
            else None
        ),
        # verify_tls намеренно НЕ управляется переменной окружения: глобальный
        # verify=false — это тихое отключение защиты канала. Единственная
        # послабляющая настройка — dev-флаг для localhost (проверяется отдельно).
        verify_tls=True,
        allow_insecure_localhost=os.environ.get(
            "AUDIT_WORKER_ALLOW_INSECURE_LOCALHOST", "false"
        ).lower() in {"1", "true", "yes", "on"},
        control_transport=(
            "grpc"
            if os.environ.get(
                "AUDIT_WORKER_TRANSPORT_MODE",
                os.environ.get("AUDIT_WORKER_CONTROL_TRANSPORT", "polling"),
            ).strip().lower() in {"grpc", "grpc_stream"}
            else os.environ.get(
                "AUDIT_WORKER_TRANSPORT_MODE",
                os.environ.get("AUDIT_WORKER_CONTROL_TRANSPORT", "polling"),
            ).strip().lower()
        ),
        grpc_target=(
            os.environ.get("AUDIT_WORKER_GRPC_TARGET", "").strip() or None
        ),
        grpc_security_mode=os.environ.get(
            "AUDIT_WORKER_GRPC_SECURITY_MODE", "test_insecure"
        ).strip().lower(),
        grpc_ca_bundle_path=(
            Path(os.environ["AUDIT_WORKER_GRPC_CA_BUNDLE"]).expanduser().resolve()
            if os.environ.get("AUDIT_WORKER_GRPC_CA_BUNDLE", "").strip() else None
        ),
        grpc_client_certificate_path=(
            Path(os.environ["AUDIT_WORKER_GRPC_CLIENT_CERT"]).expanduser().resolve()
            if os.environ.get("AUDIT_WORKER_GRPC_CLIENT_CERT", "").strip() else None
        ),
        grpc_key_store_dir=(
            Path(os.environ["AUDIT_WORKER_GRPC_KEY_STORE_DIR"]).expanduser().resolve()
            if os.environ.get("AUDIT_WORKER_GRPC_KEY_STORE_DIR", "").strip() else None
        ),
        grpc_key_store_backend=os.environ.get(
            "AUDIT_WORKER_GRPC_KEY_STORE_BACKEND", "auto"
        ).strip().lower(),
        grpc_server_identity=(
            os.environ.get("AUDIT_WORKER_GRPC_SERVER_IDENTITY", "").strip() or None
        ),
        grpc_renew_before_sec=max(
            300.0, _env_float("AUDIT_WORKER_GRPC_RENEW_BEFORE_SEC", 7 * 86400.0)
        ),
        grpc_renew_jitter=min(
            1.0, max(0.0, _env_float("AUDIT_WORKER_GRPC_RENEW_JITTER", 0.2))
        ),
        grpc_connect_timeout_sec=max(
            1.0, _env_float("AUDIT_WORKER_GRPC_CONNECT_TIMEOUT_SEC", 15.0)
        ),
        grpc_heartbeat_interval_override_sec=(
            max(
                5.0,
                _env_float("AUDIT_WORKER_GRPC_HEARTBEAT_OVERRIDE_SEC", 30.0),
            )
            if os.environ.get("AUDIT_WORKER_GRPC_HEARTBEAT_OVERRIDE_SEC", "").strip()
            else None
        ),
        grpc_reconnect_min_delay_sec=max(
            0.5, _env_float("AUDIT_WORKER_GRPC_RECONNECT_MIN_SEC", 1.0)
        ),
        grpc_reconnect_max_delay_sec=max(
            0.5, _env_float("AUDIT_WORKER_GRPC_RECONNECT_MAX_SEC", 30.0)
        ),
        grpc_reconnect_jitter=min(
            1.0, max(0.0, _env_float("AUDIT_WORKER_GRPC_RECONNECT_JITTER", 0.2))
        ),
        grpc_max_send_message_bytes=max(
            1024, _env_int("AUDIT_WORKER_GRPC_MAX_SEND_BYTES", 1024 * 1024)
        ),
        grpc_max_receive_message_bytes=max(
            1024, _env_int("AUDIT_WORKER_GRPC_MAX_RECEIVE_BYTES", 1024 * 1024)
        ),
        grpc_outbound_queue_max=max(
            8, _env_int("AUDIT_WORKER_GRPC_OUTBOUND_QUEUE_MAX", 128)
        ),
        grpc_protocol_versions=tuple(
            int(item.strip())
            for item in os.environ.get(
                "AUDIT_WORKER_GRPC_PROTOCOL_VERSIONS", "1"
            ).split(",")
            if item.strip().isdigit()
        ),
    )
    if require_dispatcher:
        validate_transport_security(config)
        validate_control_transport(config)
    return config


class InsecureTransportError(SystemExit):
    """Небезопасный транспорт: агент не стартует, а не работает молча по HTTP."""


LOCALHOST_HOSTS = {"localhost", "127.0.0.1", "::1", "[::1]"}


def _validate_https_base_url(
    value: str, *, setting_name: str, allow_insecure_localhost: bool
) -> None:
    parsed = urlparse(value)
    scheme = (parsed.scheme or "").lower()
    host = (parsed.hostname or "").lower()

    if scheme == "https":
        return
    if scheme != "http":
        raise InsecureTransportError(
            f"{setting_name}: ожидается https:// (получено {scheme or 'без схемы'}://)"
        )
    if host not in LOCALHOST_HOSTS:
        raise InsecureTransportError(
            f"{setting_name}={value}: HTTP запрещён для "
            f"внешнего хоста {host!r}. Используйте https://. "
            "HTTP допустим только к localhost и только с "
            "AUDIT_WORKER_ALLOW_INSECURE_LOCALHOST=true."
        )
    if not allow_insecure_localhost:
        raise InsecureTransportError(
            "HTTP к localhost требует явного AUDIT_WORKER_ALLOW_INSECURE_LOCALHOST=true "
            "(dev-режим). В проде используйте https://."
        )


def validate_transport_security(config: "WorkerConfig") -> None:
    """Проверить HTTPS отдельно для dispatcher и настроенного data plane."""
    _validate_https_base_url(
        config.dispatcher_url,
        setting_name="AUDIT_WORKER_DISPATCHER_URL",
        allow_insecure_localhost=config.allow_insecure_localhost,
    )
    if config.data_plane_base_url:
        _validate_https_base_url(
            config.data_plane_base_url,
            setting_name="AUDIT_WORKER_DATA_PLANE_BASE_URL",
            allow_insecure_localhost=config.allow_insecure_localhost,
        )


def validate_control_transport(config: "WorkerConfig") -> None:
    """Validate the explicit control-plane selection; there is no fallback."""
    if config.control_transport not in {"polling", "grpc"}:
        raise InsecureTransportError(
            "AUDIT_WORKER_CONTROL_TRANSPORT должен быть polling или grpc"
        )
    if config.control_transport == "polling":
        return
    if not config.grpc_target:
        raise InsecureTransportError(
            "AUDIT_WORKER_GRPC_TARGET обязателен для grpc control transport"
        )
    if config.grpc_security_mode not in {"test_insecure", "mtls"}:
        raise InsecureTransportError("неподдерживаемый gRPC security mode")
    if config.grpc_protocol_versions != (1,):
        raise InsecureTransportError(
            "12C поддерживает только Agent Stream protocol version 1"
        )
    if config.grpc_reconnect_max_delay_sec < config.grpc_reconnect_min_delay_sec:
        raise InsecureTransportError(
            "gRPC reconnect max delay не может быть меньше min delay"
        )
    if not 0.0 <= config.grpc_reconnect_jitter <= 1.0:
        raise InsecureTransportError("gRPC reconnect jitter должен быть в диапазоне 0..1")
    if config.grpc_renew_before_sec <= 0:
        raise InsecureTransportError("gRPC renew-before должен быть положительным")
    if not 0.0 <= config.grpc_renew_jitter <= 1.0:
        raise InsecureTransportError("gRPC renewal jitter должен быть в диапазоне 0..1")
    if min(
        config.grpc_max_send_message_bytes,
        config.grpc_max_receive_message_bytes,
        config.grpc_outbound_queue_max,
    ) <= 0:
        raise InsecureTransportError("gRPC client limits должны быть положительными")
    target_match = __import__("re").fullmatch(
        r"(?P<host>[A-Za-z0-9.-]+|\[[0-9A-Fa-f:]+\]):(?P<port>[0-9]{1,5})",
        config.grpc_target,
    )
    if target_match is None or not 1 <= int(target_match.group("port")) <= 65535:
        raise InsecureTransportError("gRPC target должен иметь вид host:port")
    host = target_match.group("host").strip("[]").lower()
    if config.grpc_security_mode == "test_insecure":
        if host not in LOCALHOST_HOSTS:
            raise InsecureTransportError(
                "test_insecure gRPC разрешён только к loopback; внешний endpoint запрещён"
            )
        if any((
            config.grpc_ca_bundle_path, config.grpc_client_certificate_path,
            config.grpc_key_store_dir, config.grpc_server_identity,
        )):
            raise InsecureTransportError("test_insecure не принимает mTLS identity settings")
        return
    required = {
        "AUDIT_WORKER_GRPC_CA_BUNDLE": config.grpc_ca_bundle_path,
        "AUDIT_WORKER_GRPC_CLIENT_CERT": config.grpc_client_certificate_path,
        "AUDIT_WORKER_GRPC_KEY_STORE_DIR": config.grpc_key_store_dir,
        "AUDIT_WORKER_GRPC_SERVER_IDENTITY": config.grpc_server_identity,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise InsecureTransportError("mTLS config missing: " + ", ".join(missing))
    if str(config.grpc_server_identity).lower() != host:
        raise InsecureTransportError(
            "mTLS server identity must equal gateway target host; overrides are forbidden"
        )
    for label, path in (
        ("CA bundle", config.grpc_ca_bundle_path),
        ("client certificate", config.grpc_client_certificate_path),
    ):
        if path is None or not path.is_file():
            raise InsecureTransportError(f"mTLS {label} file is unavailable")
    if config.grpc_key_store_backend not in {
        "auto", "linux_permissions", "windows_dpapi"
    }:
        raise InsecureTransportError("unknown Worker key-store backend")


def python_executable() -> str:
    return sys.executable or "python3"
