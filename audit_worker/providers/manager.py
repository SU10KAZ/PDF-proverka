"""ProviderManager — когда спрашивать провайдеров и что класть в heartbeat.

Ритм опроса (§17 задания) — три РАЗНЫХ частоты, и смешивать их нельзя:

  * heartbeat            — каждые ~30 с. Провайдеров НЕ опрашивает: отдаёт
                           последний известный снимок. Иначе каждые полминуты
                           на чужом VPS поднимались бы два процесса CLI.
  * проверка авторизации — реже heartbeat (по умолчанию 5 мин). Дешёвая,
                           локальная, к сети почти не ходит.
  * опрос лимита         — ещё реже (по умолчанию 15 мин) и ТОЛЬКО там, где
                           существует официальный способ без обращения к
                           модели. Для Claude такого способа нет — значит для
                           Claude автоматического опроса нет вовсе.
  * контрольный запрос к модели — НИКОГДА автоматически. Только по явной
                           команде с двумя независимыми разрешениями.

Провал опроса провайдера не имеет права ни уронить агента, ни изменить
состояние воркера, ни тронуть задание (§27). Поэтому каждый вызов адаптера
обёрнут, а результатом отказа становится снимок с честным `error`-состоянием,
а не исключение наружу.

Устаревание: снимок, у которого истёк `stale_after`, отдаётся НЕ как есть, а
переводится в `stale`. Показать позавчерашние 62 % как текущие — ровно та
ошибка, ради недопущения которой заведено поле.
"""
from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Any, Callable, Optional

from audit_worker import redaction
from audit_worker.providers import errors, probe_grant, quota
from audit_worker.providers.auth_mode import AUTH_MODE_UNAVAILABLE, DEFAULT_AUTH_MODE
from audit_worker.providers.base import ProbeResult, ProviderAdapter
from audit_worker.providers.claude_adapter import ClaudeProviderAdapter
from audit_worker.providers.codex_adapter import CodexProviderAdapter
from audit_worker.providers.identity import (
    AUTH_UNKNOWN,
    INSTALL_MISSING,
    POLICY_ALLOWED,
    ProviderIdentity,
)
from audit_worker.providers.paths import (
    PROVIDER_CLAUDE,
    PROVIDER_CODEX,
    SUPPORTED_PROVIDERS,
    provider_home,
)

_ADAPTERS: dict[str, type[ProviderAdapter]] = {
    PROVIDER_CLAUDE: ClaudeProviderAdapter,
    PROVIDER_CODEX: CodexProviderAdapter,
}


class ProviderManager:
    """Держит адаптеры, кеширует снимки и решает, когда опрашивать заново."""

    def __init__(
        self,
        *,
        worker_root: Path,
        auth_check_interval_sec: float = 300.0,
        quota_probe_interval_sec: float = 900.0,
        stale_after_sec: float = 1800.0,
        timeout_sec: float = 60.0,
        low_threshold_pct: Optional[float] = None,
        account_groups: Optional[dict[str, str]] = None,
        policy_blocked: Optional[dict[str, bool]] = None,
        auth_modes: Optional[dict[str, str]] = None,
        inference_allowed: bool = False,
        enabled: bool = True,
        executables: Optional[dict[str, Path]] = None,
        on_process: Optional[Callable[[int, str], None]] = None,
        log: Optional[Callable[[str], None]] = None,
    ) -> None:
        self.worker_root = Path(worker_root)
        self.enabled = bool(enabled)
        self.auth_check_interval_sec = max(30.0, float(auth_check_interval_sec))
        # Опрос лимита не может быть чаще проверки авторизации: он тяжелее и
        # ходит в сеть провайдера. Зажимаем явно, а не надеемся на настройку.
        self.quota_probe_interval_sec = max(
            self.auth_check_interval_sec, float(quota_probe_interval_sec)
        )
        self.stale_after_sec = max(60.0, float(stale_after_sec))
        self.low_threshold_pct = low_threshold_pct
        self._log = log or (lambda _m: None)
        self._lock = threading.Lock()
        self._identities: dict[str, ProviderIdentity] = {}
        self._quotas: dict[str, quota.ProviderQuotaSnapshot] = {}
        self._auth_checked_at: dict[str, float] = {}
        self._quota_checked_at: dict[str, float] = {}
        self._last_probe: dict[str, ProbeResult] = {}

        groups = dict(account_groups or {})
        blocked = dict(policy_blocked or {})
        overrides = dict(executables or {})
        # Режим берётся ПОПРОВАЙДЕРНО (`modes.get(name)`), как `account_groups`
        # и `policy_blocked`, а не одним значением на всех, как
        # `inference_allowed`. Второй образец здесь был бы ошибкой: он включил
        # бы личный каталог человека сразу обоим CLI (§13 задания).
        modes = dict(auth_modes or {})
        self.adapters: dict[str, ProviderAdapter] = {}
        for name in SUPPORTED_PROVIDERS:
            # При выключенном гейте режим не применяется вовсе. Иначе
            # `AUDIT_WORKER_PROVIDER_GATE_ENABLED=false` перестал бы быть
            # аварийным выключателем: разрешение домашнего каталога читает
            # базу учётных записей и на хосте без записи для UID (контейнер со
            # случайным пользователем) бросает исключение — воркер падал бы на
            # старте именно тогда, когда оператор пытается его этим флагом
            # спасти.
            mode = modes.get(name) if self.enabled else None
            home = provider_home(self.worker_root, name, auth_mode=mode)
            if self.enabled:
                # Каталоги создаём даже когда CLI ещё не установлен: пустая
                # раскладка с правами 0700 — это готовое место для будущего
                # входа оператора, а не мусор.
                try:
                    home.ensure_dirs()
                except OSError as exc:
                    self._log(f"provider {name}: не создать раскладку: {exc}")
            self.adapters[name] = _ADAPTERS[name](
                home,
                executable=overrides.get(name),
                timeout_sec=timeout_sec,
                account_group_id=groups.get(name),
                policy_blocked=bool(blocked.get(name)),
                inference_allowed=bool(inference_allowed),
                low_threshold_pct=low_threshold_pct,
                stale_after_sec=self.stale_after_sec,
                on_process=on_process,
            )

    # ─── Опрос ───────────────────────────────────────────────────────────────
    def refresh(self, *, force: bool = False, now: Optional[float] = None) -> None:
        """Опросить провайдеров, у которых подошёл срок. Никогда не бросает."""
        if not self.enabled:
            return
        moment = now if now is not None else time.time()
        for name, adapter in self.adapters.items():
            try:
                self._refresh_one(name, adapter, moment=moment, force=force)
            except Exception as exc:                   # noqa: BLE001 — см. §27
                # Отказ провайдера не имеет права остановить агента. Пишем в
                # кеш честное состояние ошибки и идём дальше.
                #
                # Текст исключения проходит редактор: он уезжает в `detail`
                # снимка, а оттуда в heartbeat и на экран оператора. Сегодня
                # туда попадают только имена переменных, но полагаться на
                # дисциплину авторов будущих адаптеров здесь нельзя — в
                # сообщении чужой библиотеки может оказаться и путь, и URL с
                # учётными данными.
                safe = redaction.redact(
                    str(exc), extra_literals=(str(self.worker_root),)
                )
                self._log(f"provider {name}: опрос не удался: {safe}")
                with self._lock:
                    self._quotas[name] = quota.unknown_snapshot(
                        name,
                        auth_state=AUTH_UNKNOWN,
                        quota_state=quota.QUOTA_ERROR,
                        reason=f"внутренняя ошибка опроса: {safe}",
                        observed_at=moment,
                        probe_error_code=errors.classify_exception(exc),
                    )

    def _refresh_one(
        self, name: str, adapter: ProviderAdapter, *, moment: float, force: bool
    ) -> None:
        need_auth = force or (
            moment - self._auth_checked_at.get(name, 0.0) >= self.auth_check_interval_sec
        )
        if need_auth:
            identity = adapter.identity()
            with self._lock:
                self._identities[name] = identity
                self._auth_checked_at[name] = moment

        identity = self._identities.get(name)
        if identity is None:
            return

        if adapter.home.auth_mode == AUTH_MODE_UNAVAILABLE:
            # Режим объявлен оператором: учётных данных нет. Опрос лимита
            # запустил бы CLI (у Claude — ради `auth status`, у Codex — ради
            # `app-server`) и оставил бы следы в чужом HOME ради заведомо
            # известного ответа. Снимок собирается без единого подпроцесса.
            with self._lock:
                self._quotas[name] = quota.unknown_snapshot(
                    name,
                    auth_state=identity.auth_state,
                    quota_state=quota.QUOTA_AUTH_REQUIRED,
                    reason=(
                        "режим авторизации unavailable: провайдер на этом "
                        "воркере не используется"
                    ),
                    observed_at=moment,
                    probe_error_code=errors.ERR_AUTH_REQUIRED,
                )
                self._quota_checked_at[name] = moment
            return

        if not adapter.supports_zero_inference_quota():
            # Официального способа нет — опрашивать нечего и незачем. Снимок
            # пересобирается вместе с авторизацией: он зависит только от неё.
            if need_auth:
                snapshot = adapter.quota_status()
                with self._lock:
                    self._quotas[name] = snapshot
                    self._quota_checked_at[name] = moment
            return

        need_quota = force or (
            moment - self._quota_checked_at.get(name, 0.0) >= self.quota_probe_interval_sec
        )
        if not need_quota:
            return
        if identity.installation_status == INSTALL_MISSING:
            with self._lock:
                self._quotas[name] = quota.unknown_snapshot(
                    name, auth_state=identity.auth_state,
                    reason="CLI провайдера не установлен", observed_at=moment,
                    probe_error_code=errors.ERR_CLI_MISSING,
                )
                self._quota_checked_at[name] = moment
            return
        snapshot = adapter.quota_status()
        with self._lock:
            self._quotas[name] = snapshot
            self._quota_checked_at[name] = moment

    # ─── Чтение кеша ─────────────────────────────────────────────────────────
    def identity(self, provider: str) -> Optional[ProviderIdentity]:
        with self._lock:
            return self._identities.get(provider)

    def quota(
        self, provider: str, *, now: Optional[float] = None
    ) -> Optional[quota.ProviderQuotaSnapshot]:
        with self._lock:
            snapshot = self._quotas.get(provider)
        if snapshot is None:
            return None
        return _staleness_applied(snapshot, now=now)

    # ─── Представление для heartbeat ─────────────────────────────────────────
    def heartbeat_payload(self, *, now: Optional[float] = None) -> list[dict[str, Any]]:
        """Безопасные сведения о провайдерах для центра (§16 задания).

        Отдаётся ПОСЛЕДНИЙ известный снимок, а не свежий опрос: heartbeat идёт
        каждые 30 секунд, и опрашивать провайдеров в его такте значило бы
        поднимать процессы CLI 2880 раз в сутки ради данных, которые меняются
        раз в час.
        """
        if not self.enabled:
            return []
        moment = now if now is not None else time.time()
        out: list[dict[str, Any]] = []
        for name in SUPPORTED_PROVIDERS:
            identity = self.identity(name)
            snapshot = self.quota(name, now=moment)
            if identity is None and snapshot is None:
                continue
            payload: dict[str, Any] = {
                "provider": name,
                "observed_at": moment,
            }
            if identity is not None:
                payload.update(identity.as_center_payload())
            else:
                adapter = self.adapters.get(name)
                payload.update({
                    "installation_status": INSTALL_MISSING,
                    "auth_state": AUTH_UNKNOWN,
                    "auth_method": "none",
                    # Режим известен из настройки, даже когда опроса ещё не
                    # было: он берётся из конфигурации, а не из ответа CLI.
                    "auth_mode": (
                        adapter.home.auth_mode if adapter is not None
                        else DEFAULT_AUTH_MODE
                    ),
                    "policy_state": POLICY_ALLOWED,
                    "inference_allowed": False,
                })
            payload["quota"] = snapshot.as_dict() if snapshot is not None else None
            payload["last_auth_check_at"] = self._auth_checked_at.get(name)
            payload["last_quota_check_at"] = self._quota_checked_at.get(name)
            # Остаток разрешений на реальный контрольный запрос. Кладётся в
            # `capability`, а не верхним ключом, по той же причине, что и
            # `auth_mode`: санитайзер центра собирает верхний уровень
            # перечислением полей и новый ключ молча отбросил бы, а
            # `capability_json` сохраняется целиком.
            #
            # Зачем это центру вообще. Без него «может ли ЭТОТ воркер потратить
            # настоящий запрос» — вопрос, на который отвечает только человек с
            # ssh на машину. Разрешение, которого не видно, невозможно ни
            # проверить, ни отозвать вовремя: оператор узнаёт о нём по счёту.
            capability = payload.get("capability")
            if isinstance(capability, dict):
                capability["inference_probe_grant_remaining"] = (
                    probe_grant.read_state(self.worker_root, name).remaining
                )
            out.append(payload)
        return out

    def warnings(self, *, now: Optional[float] = None) -> list[dict[str, Any]]:
        """Предупреждения о провайдерах — ОТДЕЛЬНОЙ строкой, severity=warn.

        Ни одно из них не является ошибкой воркера: карточка VPS обязана
        остаться online, а провайдер — показать своё состояние сам (§27).
        """
        if not self.enabled:
            return []
        moment = now if now is not None else time.time()
        out: list[dict[str, Any]] = []
        for name in SUPPORTED_PROVIDERS:
            identity = self.identity(name)
            if identity is None:
                continue
            if identity.auth_mode == AUTH_MODE_UNAVAILABLE:
                # Проверяется ПЕРВЫМ, до «не установлен», и это не порядок ради
                # порядка. Самая естественная конфигурация режима — «мы
                # сознательно не даём этому провайдеру учётных данных здесь,
                # поэтому его тут и нет». В обратном порядке ветка
                # INSTALL_MISSING перехватывала бы её вместе с `continue`, и
                # оператор в ответ на СВОЮ настройку получал бы тревожное «CLI
                # не установлен» — то самое сообщение, отличать от которого эта
                # ветка и написана.
                out.append({
                    "code": f"provider_{name}_auth_unavailable",
                    "severity": "warn",
                    "message": (
                        f"{name}: режим авторизации unavailable — провайдер "
                        "на этом воркере не используется по решению оператора"
                    ),
                })
                continue
            if identity.installation_status == INSTALL_MISSING:
                out.append({
                    "code": f"provider_{name}_missing",
                    "severity": "warn",
                    "message": f"CLI {name} не установлен на воркере",
                })
                continue
            if identity.auth_state != "logged_in":
                out.append({
                    "code": f"provider_{name}_auth",
                    "severity": "warn",
                    "message": (
                        f"{name}: авторизация не выполнена "
                        f"({identity.auth_state}) — реальный аудит на этом "
                        "провайдере невозможен"
                    ),
                })
            creds = identity.credential_facts or {}
            if creds.get("world_readable") or creds.get("group_readable"):
                out.append({
                    "code": f"provider_{name}_credential_permissions",
                    "severity": "error",
                    "message": (
                        f"{name}: файл учётных данных доступен на чтение не "
                        f"только владельцу (режим {creds.get('mode')})"
                    ),
                })
            snapshot = self.quota(name, now=moment)
            if snapshot is not None and snapshot.quota_state == quota.QUOTA_LIMITED:
                out.append({
                    "code": f"provider_{name}_limited",
                    "severity": "warn",
                    "message": f"{name}: лимит исчерпан",
                })
        return out

    # ─── Контрольный запрос ──────────────────────────────────────────────────
    def minimal_probe(self, provider: str, *, confirmed_by_operator: bool) -> ProbeResult:
        """Один запрос к модели. Оба разрешения проверяются в адаптере."""
        adapter = self.adapters.get(provider)
        if adapter is None:
            return ProbeResult(
                provider=provider, allowed=False, performed=False,
                error_code=errors.ERR_UNKNOWN, detail="неизвестный провайдер",
            )
        if adapter.home.auth_mode == AUTH_MODE_UNAVAILABLE:
            # Режим обещает, что CLI этого провайдера не запускается вовсе.
            # Обещание, действующее только на автоматических путях, — не
            # обещание: ручной контрольный запрос точно так же дошёл бы до
            # чужого HOME, а оператор, объявивший провайдера неиспользуемым,
            # меньше всего ожидает, что он всё-таки будет вызван.
            return ProbeResult(
                provider=provider, allowed=False, performed=False,
                error_code=errors.ERR_AUTH_REQUIRED,
                detail=(
                    "режим авторизации unavailable: провайдер на этом воркере "
                    "не используется"
                ),
            )
        before = self.quota(provider)
        result = adapter.minimal_probe(confirmed_by_operator=confirmed_by_operator)
        if result.performed:
            # После настоящего вызова снимок заведомо устарел: пересобираем.
            self.refresh(force=True)
        after = self.quota(provider)
        with self._lock:
            self._last_probe[provider] = result
        self._log(
            f"provider {provider}: контрольный запрос "
            f"allowed={result.allowed} performed={result.performed} "
            f"exit={result.exit_code} matched={result.matched_expected}; "
            f"квота до={before.quota_state if before else None} "
            f"после={after.quota_state if after else None}"
        )
        return result

    def last_probe(self, provider: str) -> Optional[ProbeResult]:
        with self._lock:
            return self._last_probe.get(provider)


def _staleness_applied(
    snapshot: quota.ProviderQuotaSnapshot, *, now: Optional[float] = None
) -> quota.ProviderQuotaSnapshot:
    """Просроченный снимок отдаётся как `stale`, а не как действующий."""
    if not snapshot.is_stale(now=now):
        return snapshot
    if snapshot.quota_state in (
        quota.QUOTA_STALE, quota.QUOTA_UNKNOWN, quota.QUOTA_AUTH_REQUIRED,
        quota.QUOTA_POLICY_BLOCKED, quota.QUOTA_ERROR,
    ):
        return snapshot
    age = int((now or time.time()) - snapshot.observed_at)
    return snapshot.with_state(
        quota.QUOTA_STALE,
        detail=(
            f"снимок устарел ({age} с назад, было {snapshot.quota_state}); "
            "показанные числа — последние известные, а не текущие"
        ),
    )
