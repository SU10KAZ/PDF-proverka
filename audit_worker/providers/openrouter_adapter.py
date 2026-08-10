"""OpenRouterProviderAdapter — платный шлюз как ПОЛНОЦЕННЫЙ провайдер воркера.

Зачем он появился (этап 11J).

До 11J слой провайдеров знал ровно два способа обратиться к модели, и оба —
через запуск CLI. Первая нога ансамбля этапа 01 идёт не в CLI, а в OpenRouter
по HTTPS, и адаптера для неё не было вовсе. Следствие 11I сформулировало это
как отдельное ограничение (KI-11I-5): НИ ОДИН существующий воркер не мог
исполнить точный пресет — GPT-нога обязательна в обоих, а канала для неё на
воркере нет. Пока это так, «удалённый аудит» означает аудит, у которого центр
втихую доигрывает одну ногу за воркера, — то есть не удалённый.

Три вещи, которые здесь НЕ изобретаются заново, а переиспользуются:

  * **форма запроса** взята у боевого центрального пути
    (`gemma_findings_only.call_gpt_for_block`) дословно: тот же URL, те же
    заголовки, тот же `messages` с `image_url`/data-URL, тот же
    `response_format=json_schema`, тот же разбор `usage`. Второй диалект
    запроса к тому же шлюзу означал бы, что нога на воркере и нога на центре
    дают несравнимые ответы — ровно то, ради чего затевался план маршрутизации;
  * **контракт результата** — общий `ProviderInferenceResult`. Ни одна строка
    сводящего кода этапа 01 (`combine_detector_results`, провенанс, счётчики)
    об OpenRouter не знает и знать не должна;
  * **гейты вызова** — общий `_inference_gate`: политика, разрешение на
    инференс, подтверждение вызывающего. Своей копии `if` здесь нет.

ИНВАРИАНТЫ HTTP-ПРОВАЙДЕРА. Инварианты I-P1…I-P8 описывают подпроцесс: чистое
окружение, свой HOME, пустой cwd, убийство группы, stdin вместо argv. У
провайдера без подпроцесса ни одного из этих объектов не существует, и делать
вид, что они соблюдаются, было бы хуже, чем честно назвать другие:

  I-H1. **Ключ живёт ровно один вызов.** Он читается из локального файла в
        момент запроса и не сохраняется ни на адаптере, ни в модуле. Поля, в
        котором он пережил бы вызов, нет по построению — значит его нет ни в
        `repr`, ни в дампе, ни в pickle попытки.
  I-H2. **Ключ не приходит извне.** Ни из задания, ни из привязки, ни из
        окружения процесса: `OPENROUTER_API_KEY` входит в `FORBIDDEN_ENV_NAMES`
        и отсутствует в белом списке окружения конвейера. Единственный источник
        — файл, который оператор VPS разложил заранее (`openrouter_secret`).
  I-H3. **Окружение процесса не влияет на маршрут запроса.** HTTP-клиент
        создаётся с `trust_env=False`: `HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY` и
        `SSL_CERT_FILE` из окружения игнорируются. Иначе переменная окружения
        уводила бы запрос ВМЕСТЕ С КЛЮЧОМ на чужой хост, и ни один рубеж этого
        бы не заметил: ответ пришёл бы нормальный.
  I-H4. **Адрес шлюза — константа модуля либо явная настройка администратора.**
        Из задания он не приходит никогда. Не-HTTPS адрес и адрес вне
        официального хоста принимаются ТОЛЬКО при объявленном режиме заглушек
        (`AUDIT_WORKER_PROVIDER_ENDPOINTS_STUBBED`), и режим этот виден центру
        в heartbeat: подменить прод на заглушку молча нельзя.
  I-H5. **Ни промпт, ни вложение не попадают на диск.** У CLI-провайдера
        картинка неизбежно пишется файлом (`codex exec -i`); здесь она уходит
        байтами в теле запроса, и временного файла не возникает вовсе.
  I-H6. **Ответ проходит редактор секретов до возврата.** Как и у CLI: дальше
        значение живёт в отчёте о прогоне и уезжает центру.

Чего адаптер НЕ делает:

  * не ходит в сеть ради heartbeat. `GET /key` вернул бы лимиты, но это
    сетевой запрос с ключом ради данных, которые меняются раз в сутки; §7
    задания прямо требует показывать `configured`, а не выдумывать `verified`;
  * не выбирает модель. Строку даёт ЛОКАЛЬНАЯ политика воркера
    (`model_policy`), как и для CLI-провайдеров;
  * не повторяет запрос сам. Повтор — это второй оплаченный вызов, и решение о
    нём принимает журнал попытки (`inference_ledger`), а не транспорт.
"""
from __future__ import annotations

import base64
import json
import time
from dataclasses import replace
from pathlib import Path
from typing import Any, Optional, Sequence
from urllib.parse import urlparse

from audit_worker import redaction
from audit_worker.providers import errors, openrouter_secret
from audit_worker.providers.base import AuthStatus, ProbeResult, ProviderAdapter
from audit_worker.providers.identity import (
    AUTH_LOGGED_IN,
    AUTH_LOGGED_OUT,
)
from audit_worker.providers.inference import (
    STATUS_ERROR,
    STATUS_SUCCESS,
    ProviderInferenceResult,
    sha256_text,
)
from audit_worker.providers.paths import PROVIDER_OPENROUTER
from audit_worker.providers.quota import (
    QUOTA_AUTH_REQUIRED,
    QUOTA_UNKNOWN,
    SOURCE_UNAVAILABLE,
    STABILITY_NOT_APPLICABLE,
    ProviderQuotaSnapshot,
    unknown_snapshot,
)

#: Официальный адрес шлюза. Совпадает с боевым центральным путём
#: (`gemma_findings_only.OPENROUTER_URL`) намеренно: две строки для одного
#: сервиса разъезжаются при первой же смене версии API.
OFFICIAL_BASE_URL = "https://openrouter.ai/api/v1"
OFFICIAL_HOST = "openrouter.ai"
CHAT_COMPLETIONS_PATH = "/chat/completions"

#: Переопределение адреса шлюза АДМИНИСТРАТОРОМ VPS. Нужно для двух законных
#: случаев: корпоративный обратный прокси и стенд с заглушкой. В обоих случаях
#: значение задаёт человек с ssh на машину, а не центр и не задание.
BASE_URL_ENV = "AUDIT_WORKER_PROVIDER_OPENROUTER_BASE_URL"

#: Объявление «внешние точки этого воркера — заглушки». Ставит администратор
#: стенда. Делает две вещи сразу: разрешает не-HTTPS адрес шлюза и ПОКАЗЫВАЕТ
#: это центру (`config.capabilities()`), чтобы «аудит прошёл» на стенде нельзя
#: было спутать с боевым прогоном.
STUBBED_ENDPOINTS_ENV = "AUDIT_WORKER_PROVIDER_ENDPOINTS_STUBBED"

#: Заголовки атрибуции OpenRouter. Значения фиксированы: они попадают в
#: биллинг-панель владельца ключа, и данным задания там не место.
ATTRIBUTION_REFERER = "https://localhost"
ATTRIBUTION_TITLE = "audit-worker"

#: Типы вложений, которые шлюз принимает как `image_url` с data-URL.
SUPPORTED_IMAGE_MEDIA_TYPES: tuple[str, ...] = (
    "image/png", "image/jpeg", "image/webp", "image/gif",
)

#: Потолок на суммарный размер вложений одного запроса. Не оптимизация, а
#: предохранитель: data-URL раздувает байты в 4/3, и запрос на десятки
#: мегабайт — это оплаченный таймаут, а не анализ чертежа.
MAX_ATTACHMENT_BYTES = 24 * 1024 * 1024

#: Срок по умолчанию, если вызывающий не назвал свой. Совпадает с боевым
#: `DEFAULT_TIMEOUT_S` центральной ноги.
DEFAULT_TIMEOUT_SEC = 200.0

#: Потолок ответа. Совпадает с боевым центральным путём
#: (`gemma_findings_only.DEFAULT_MAX_TOKENS`): длина ответа — свойство прогона,
#: а не маршрута, выбранного шлюзом.
MAX_OUTPUT_TOKENS = 16000


def stubbed_endpoints_declared(env: Optional[dict[str, str]] = None) -> bool:
    """Объявил ли администратор этой машины, что внешние точки — заглушки."""
    import os

    source = env if env is not None else os.environ
    raw = str(source.get(STUBBED_ENDPOINTS_ENV, "") or "").strip().lower()
    return raw in ("1", "true", "yes", "on")


class OpenRouterEndpointError(RuntimeError):
    """Адрес шлюза не проходит проверку. Отказ ДО обращения к сети."""


def resolve_base_url(env: Optional[dict[str, str]] = None) -> str:
    """Адрес шлюза. Константа модуля либо настройка администратора VPS.

    Проверка адреса — не формальность. Значение решает, КУДА уедет заголовок
    `Authorization` с ключом владельца, и ошибка здесь не выглядит ошибкой:
    чужой хост ответит нормальным JSON, а этап отчитается об успехе.
    """
    import os

    source = env if env is not None else os.environ
    raw = str(source.get(BASE_URL_ENV, "") or "").strip().rstrip("/")
    if not raw:
        return OFFICIAL_BASE_URL
    parsed = urlparse(raw)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise OpenRouterEndpointError(
            f"{BASE_URL_ENV}: ожидается абсолютный http(s)-адрес"
        )
    official = parsed.scheme == "https" and parsed.hostname == OFFICIAL_HOST
    if official:
        return raw
    if not stubbed_endpoints_declared(env=source):
        # Fail closed. Единственный способ увести ключ на неофициальный хост —
        # объявить машину стендом, а объявление это видно центру.
        #
        # В сообщение уходит СХЕМА И ХОСТ, а не значение переменной целиком, и
        # это не косметика. Сообщение попадает в `capability_snapshot`, оттуда
        # в heartbeat, в `capability_json` центральной БД и в её резервные
        # копии. Законное применение переменной — корпоративный обратный
        # прокси, а его адрес операторы пишут в формах `https://user:pass@host`
        # и `?api_key=…`. Пропустить такую строку через редактор
        # НЕДОСТАТОЧНО: редактор ловит формы, которые мы предвидели, и на
        # пароле с символом `@` оставляет хвост. Не положить значение в
        # сообщение надёжнее, чем вычищать его оттуда.
        #
        # Соседняя, успешная ветка уже поступает именно так: наружу уходит
        # `urlparse(base).hostname`, а не полный URL.
        raise OpenRouterEndpointError(
            f"{BASE_URL_ENV} указывает на {parsed.scheme}://{parsed.hostname} "
            f"вместо официального шлюза {OFFICIAL_HOST!r} по HTTPS. Такой "
            f"адрес принимается только при объявленном "
            f"{STUBBED_ENDPOINTS_ENV}=true, и объявление уезжает в heartbeat: "
            "подменить боевой шлюз молча нельзя"
        )
    return raw


class OpenRouterProviderAdapter(ProviderAdapter):
    """Провайдер без CLI: обращение к модели — HTTPS-запрос из этого процесса."""

    provider = PROVIDER_OPENROUTER
    parser_version = "1"

    # ─── Установка и авторизация ─────────────────────────────────────────────
    def executable_path(self) -> Optional[Path]:
        """У HTTP-провайдера исполняемого файла нет. Не «не нашли», а «нет»."""
        return None

    def installed(self) -> bool:
        """«Установлен» = есть чем сделать запрос.

        Для CLI-провайдера установка — это файл на диске; здесь ей соответствует
        наличие HTTP-клиента в окружении процесса конвейера. Разница по смыслу
        нулевая: в обоих случаях вопрос один — «канал физически существует?».
        """
        return self._httpx() is not None

    @staticmethod
    def _httpx():
        try:
            import httpx                                   # noqa: PLC0415
        except ModuleNotFoundError:
            return None
        return httpx

    def version(self) -> Optional[str]:
        """Версия HTTP-клиента вместо версии CLI.

        Поле называется `cli_version` и переименовывать его ради одного
        провайдера дороже, чем объяснить: оператору нужно понимать, каким кодом
        сделан вызов, а у HTTP-провайдера этот код — клиентская библиотека.
        """
        httpx = self._httpx()
        if httpx is None:
            return None
        return f"httpx/{getattr(httpx, '__version__', 'unknown')}"

    def secret_status(self) -> openrouter_secret.SecretStatus:
        """Настроен ли ключ. Без чтения содержимого и без сети."""
        return openrouter_secret.probe(self.home.credential_path)

    def credential_facts(self) -> dict[str, Any]:
        """Факты о ФАКТИЧЕСКИ читаемом файле ключа.

        Базовый `identity()` считает их по `home.credential_path`
        безусловно — для CLI-провайдеров это верно, потому что путь у них один.
        Здесь путь может быть переопределён администратором
        (`AUDIT_WORKER_PROVIDER_OPENROUTER_CREDENTIAL`), и тогда центральная
        телеметрия описывала бы НЕСУЩЕСТВУЮЩИЙ файл, одновременно сообщая
        `auth_state=logged_in`. Оператор видел бы «ключа нет, но провайдер
        авторизован» и не мог бы понять, какой из двух фактов ложный.
        """
        status = self.secret_status()
        return {
            "exists": bool(status.present),
            "mode": status.mode,
            "owner_is_current_user": status.owner_is_current_user,
            "group_readable": status.group_readable,
            "world_readable": status.world_readable,
            # Абсолютного пути здесь нет ни в каком виде: центру достаточно
            # знать, откуда он взят — из раскладки или из настройки машины.
            "path_source": status.source,
        }

    def identity(self):
        """`ProviderIdentity` с фактами о РЕАЛЬНОМ файле ключа."""
        base = super().identity()
        if not base.credential_facts and base.auth_mode == "unavailable":
            return base
        return replace(base, credential_facts=self.credential_facts())

    def auth_status(self) -> AuthStatus:
        """Состояние авторизации БЕЗ обращения к сети (§7 задания).

        `logged_in` здесь означает ровно «ключ на месте и права узкие». Что он
        действителен, воркер узнает только первым рабочим вызовом — и выдавать
        предположение за проверку нельзя: оператор прочитал бы `verified` как
        «можно ставить задание», а получил бы 401 в середине оплаченного
        прогона соседних ног.
        """
        status = self.secret_status()
        if status.configured:
            return AuthStatus(
                auth_state=AUTH_LOGGED_IN,
                auth_method="api_key",
                # Идентификатора учётной записи у нас нет и добывать его
                # сетевым запросом мы не будем. Отпечаток остаётся пустым —
                # честнее, чем хэшировать путь к файлу и звать это аккаунтом.
                stable_identifier=None,
                detail=(
                    "ключ настроен локально; действительность НЕ проверена — "
                    "проверка стоила бы запроса к платному шлюзу"
                ),
            )
        return AuthStatus(
            auth_state=AUTH_LOGGED_OUT,
            auth_method="none",
            error_code=errors.ERR_AUTH_REQUIRED,
            detail=status.reason or "ключ OpenRouter на этом воркере не настроен",
        )

    # ─── Квота ───────────────────────────────────────────────────────────────
    def supports_zero_inference_quota(self) -> bool:
        """Нет. У шлюза есть `GET /key`, но это СЕТЕВОЙ запрос с ключом.

        Формально он бесплатен. Практически это обращение к платному сервису
        каждые пятнадцать минут с чужого VPS ради числа, которое конвейеру не
        нужно: решение «звать или не звать» принимает разрешение оператора, а
        не остаток баланса. §7 задания решает развилку в пользу `configured`.
        """
        return False

    def quota_source_name(self) -> str:
        return SOURCE_UNAVAILABLE

    def quota_source_stability(self) -> str:
        return STABILITY_NOT_APPLICABLE

    def quota_status(self, *, auth: Optional[AuthStatus] = None) -> ProviderQuotaSnapshot:
        state = auth or self.auth_status()
        configured = state.auth_state == AUTH_LOGGED_IN
        return unknown_snapshot(
            self.provider,
            auth_state=state.auth_state,
            quota_state=QUOTA_UNKNOWN if configured else QUOTA_AUTH_REQUIRED,
            reason=(
                "ключ настроен; остаток баланса не опрашивается — это сетевой "
                "запрос к платному шлюзу ради числа, которым конвейер не "
                "пользуется"
                if configured
                else (state.detail or "ключ не настроен")
            ),
            cli_version=self.version(),
            probe_error_code=None if configured else errors.ERR_AUTH_REQUIRED,
        )

    def capability_snapshot(self) -> dict[str, Any]:
        snapshot = super().capability_snapshot()
        status = self.secret_status()
        # Значения ключа здесь нет и быть не может: `SecretStatus` его не несёт.
        snapshot["transport"] = "https_api"
        snapshot["credential"] = status.as_dict()
        snapshot["endpoints_stubbed"] = stubbed_endpoints_declared()
        try:
            base = resolve_base_url()
        except OpenRouterEndpointError as exc:
            # Через редактор — как ВСЁ, что уезжает центру. Сообщение содержит
            # значение переменной администратора, а в ней может оказаться и
            # адрес внутреннего прокси, и (при ошибке настройки) строка с
            # учётными данными в форме `https://user:pass@host`.
            snapshot["endpoint_error"] = redaction.redact(str(exc))
        else:
            # Хост, а не полный URL с возможными параметрами: центру нужно
            # знать «боевой шлюз или заглушка», а не раскладку прокси.
            snapshot["endpoint_host"] = urlparse(base).hostname or ""
            snapshot["endpoint_official"] = base.startswith(OFFICIAL_BASE_URL)
        return snapshot

    # ─── Контрольный запрос ──────────────────────────────────────────────────
    def minimal_probe(self, *, confirmed_by_operator: bool = False) -> ProbeResult:
        """Один платный запрос по явной команде оператора.

        Гейты те же, что у CLI-провайдеров: политика, разрешение на инференс и
        подтверждение человека. Своей копии проверок здесь нет — они берутся у
        рабочего вызова, потому что расходятся такие копии всегда.
        """
        started = time.time()
        if self.policy_blocked:
            return ProbeResult(
                provider=self.provider, allowed=False, performed=False,
                error_code=errors.ERR_POLICY_BLOCKED,
                detail="провайдер отключён политикой на этом воркере",
            )
        if not (self.inference_allowed and confirmed_by_operator):
            return ProbeResult(
                provider=self.provider, allowed=False, performed=False,
                error_code=errors.ERR_POLICY_BLOCKED,
                detail=(
                    "контрольный запрос к платному шлюзу требует и разрешения "
                    "на инференс, и подтверждения оператора"
                ),
            )
        status = self.secret_status()
        if not status.configured:
            return ProbeResult(
                provider=self.provider, allowed=True, performed=False,
                error_code=errors.ERR_AUTH_REQUIRED,
                detail=status.reason,
            )
        result = self._request(
            prompt="Ответь строго JSON-объектом {\"ok\": true} и ничем больше.",
            images=(),
            purpose="minimal_probe",
            timeout_sec=60.0,
            model=None,
            reasoning_effort=None,
        )
        payload = result.result if isinstance(result.result, dict) else {}
        return ProbeResult(
            provider=self.provider,
            allowed=True,
            performed=result.status == STATUS_SUCCESS or result.error_code is None,
            started_at=started,
            duration_sec=result.duration_ms / 1000.0,
            exit_code=result.exit_code,
            matched_expected=bool(payload.get("ok")),
            usage=dict(result.usage),
            error_code=result.error_code,
            detail=result.detail,
        )

    # ─── Рабочий вызов ───────────────────────────────────────────────────────
    def structured_inference(
        self,
        prompt: str,
        *,
        purpose: str,
        timeout_sec: Optional[float] = None,
        model: Optional[str] = None,
        accepted_reported_models: Sequence[str] = (),
        model_report: str = "required",
        reasoning_effort: Optional[str] = None,
    ) -> ProviderInferenceResult:
        return self._structured(
            prompt,
            images=(),
            purpose=purpose,
            timeout_sec=timeout_sec,
            model=model,
            accepted_reported_models=accepted_reported_models,
            model_report=model_report,
            reasoning_effort=reasoning_effort,
        )

    def structured_inference_multimodal(
        self,
        prompt: str,
        *,
        images: Sequence[tuple[str, bytes]],
        purpose: str,
        timeout_sec: Optional[float] = None,
        model: Optional[str] = None,
        accepted_reported_models: Sequence[str] = (),
        model_report: str = "required",
        reasoning_effort: Optional[str] = None,
    ) -> ProviderInferenceResult:
        """Тот же вызов с вложением. Байтами в теле запроса, без временных файлов.

        Здесь проходит §12 задания: детектор обязан увидеть РОВНО тот блок,
        который выбрал конвейер. Байты приходят от `provider_transport.read_crop`
        (тот же вызов, что у codex-ног), кодируются в data-URL и сверяются по
        sha256 ПОСЛЕ кодирования — то есть проверяется именно то, что уйдёт в
        сеть, а не то, что было в аргументе.
        """
        if not images:
            return self._error(
                errors.ERR_UNKNOWN,
                "мультимодальный вызов без изображений: молчаливый переход на "
                "текстовый путь запрещён — этап получил бы анализ чертежа без "
                "чертежа",
            )
        return self._structured(
            prompt,
            images=tuple(images),
            purpose=purpose,
            timeout_sec=timeout_sec,
            model=model,
            accepted_reported_models=accepted_reported_models,
            model_report=model_report,
            reasoning_effort=reasoning_effort,
        )

    # ─── Внутреннее ──────────────────────────────────────────────────────────
    def _error(
        self, code: str, detail: str, *, model: Optional[str] = None
    ) -> ProviderInferenceResult:
        return ProviderInferenceResult(
            provider=self.provider, model=model, status=STATUS_ERROR,
            auth_mode=self.home.auth_mode, error_code=code,
            detail=redaction.redact(detail),
        )

    def _structured(
        self,
        prompt: str,
        *,
        images: Sequence[tuple[str, bytes]],
        purpose: str,
        timeout_sec: Optional[float],
        model: Optional[str],
        accepted_reported_models: Sequence[str],
        model_report: str,
        reasoning_effort: Optional[str],
    ) -> ProviderInferenceResult:
        blocked = self._inference_gate(confirmed_by_caller=True, purpose=purpose)
        if blocked is not None:
            return blocked
        text = str(prompt or "")
        if not text.strip():
            return self._error(
                errors.ERR_UNKNOWN, "пустой промпт: рабочий вызов не выполняется"
            )
        requested_model = str(model).strip() if model else ""
        accepted = tuple(
            str(x).strip() for x in accepted_reported_models if str(x).strip()
        )
        if not requested_model:
            # Отличие от CLI-провайдеров, и оно принципиальное. У Claude без
            # `--model` ответит модель учётной записи по умолчанию — плохо, но
            # работает. Здесь `model` — ОБЯЗАТЕЛЬНОЕ поле тела запроса: без
            # него шлюз выбрал бы маршрут сам, то есть аудит уехал бы на
            # неизвестную модель по неизвестной цене.
            return self._error(
                errors.ERR_MODEL_MISMATCH,
                "локальная политика воркера не назначила модель для этого "
                "маршрута: платный шлюз требует явного идентификатора, и "
                "выбирать его за администратора машины нельзя",
            )
        if not accepted:
            return self._error(
                errors.ERR_MODEL_MISMATCH,
                f"модель {requested_model!r} назначена, но список допустимых "
                "фактических идентификаторов пуст: сверять ответ не с чем",
            )
        total_bytes = 0
        for media_type, blob in images:
            if not blob:
                return self._error(
                    errors.ERR_UNKNOWN,
                    "пустое изображение во вложении: вызов не выполняется",
                )
            if str(media_type) not in SUPPORTED_IMAGE_MEDIA_TYPES:
                return self._error(
                    errors.ERR_UNKNOWN,
                    f"неподдерживаемый тип вложения {media_type!r}; допустимы "
                    f"{list(SUPPORTED_IMAGE_MEDIA_TYPES)}",
                )
            total_bytes += len(blob)
        if total_bytes > MAX_ATTACHMENT_BYTES:
            return self._error(
                errors.ERR_UNKNOWN,
                f"суммарный размер вложений {total_bytes} байт больше потолка "
                f"{MAX_ATTACHMENT_BYTES}: запрос не отправляется",
            )
        return self._request(
            prompt=text,
            images=tuple(images),
            purpose=purpose,
            timeout_sec=timeout_sec,
            model=requested_model,
            reasoning_effort=reasoning_effort,
            accepted=accepted,
            model_report=model_report,
        )

    def _request(
        self,
        *,
        prompt: str,
        images: Sequence[tuple[str, bytes]],
        purpose: str,
        timeout_sec: Optional[float],
        model: Optional[str],
        reasoning_effort: Optional[str],
        accepted: Sequence[str] = (),
        model_report: str = "required",
    ) -> ProviderInferenceResult:
        httpx = self._httpx()
        if httpx is None:
            return self._error(
                errors.ERR_CLI_MISSING,
                "HTTP-клиент недоступен в этом окружении: канал к шлюзу "
                "физически отсутствует",
                model=model,
            )
        try:
            base_url = resolve_base_url()
        except OpenRouterEndpointError as exc:
            return self._error(errors.ERR_POLICY_BLOCKED, str(exc), model=model)

        # Ключ читается ПОСЛЕДНИМ — после всех проверок, которые могут отказать
        # без него. Чем короче отрезок, на котором значение живёт в памяти, тем
        # меньше поверхность (I-H1).
        try:
            api_key = openrouter_secret.read_secret(self.home.credential_path)
        except openrouter_secret.OpenRouterSecretError as exc:
            # §24 задания: пропавший между preflight и действием ключ проваливает
            # КОНКРЕТНОЕ действие. Ни подмены провайдера, ни пропуска ноги.
            return self._error(errors.ERR_AUTH_REQUIRED, str(exc), model=model)

        content: list[dict[str, Any]] = [{"type": "text", "text": prompt}]
        for media_type, blob in images:
            encoded = base64.b64encode(blob).decode("ascii")
            content.append({
                "type": "image_url",
                "image_url": {"url": f"data:{media_type};base64,{encoded}"},
            })
        payload: dict[str, Any] = {
            "model": model,
            "messages": [{"role": "user", "content": content}],
            "temperature": 0.2,
            # Потолок ответа. Боевой центральный путь его задаёт
            # (`gemma_findings_only.DEFAULT_MAX_TOKENS`), и без него шлюз берёт
            # умолчание маршрута — то есть длина ответа перестаёт быть
            # свойством прогона. Усечение при этом не проходит молча: ответ с
            # `finish_reason=length` отвергается ниже.
            "max_tokens": MAX_OUTPUT_TOKENS,
            # Ответ обязан быть JSON-ОБЪЕКТОМ. Центральный путь требует этого
            # строгой схемой; здесь схема ответа принадлежит этапу, а не
            # транспорту (мост проверяет поля сам), поэтому запрашивается
            # общий объектный режим. Без него модель вольна ответить прозой, и
            # `_first_json_object` вернёт None уже ПОСЛЕ оплаты.
            "response_format": {"type": "json_object"},
        }
        if reasoning_effort:
            payload["reasoning"] = {"effort": str(reasoning_effort)}
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": ATTRIBUTION_REFERER,
            "X-Title": ATTRIBUTION_TITLE,
        }
        timeout = float(timeout_sec) if timeout_sec else DEFAULT_TIMEOUT_SEC
        url = base_url.rstrip("/") + CHAT_COMPLETIONS_PATH
        started = time.monotonic()
        try:
            # `trust_env=False` — I-H3. Прокси и корневые сертификаты из
            # окружения игнорируются: переменная окружения не имеет права
            # переадресовать запрос вместе с заголовком авторизации.
            with httpx.Client(trust_env=False, timeout=timeout) as client:
                response = client.post(url, headers=headers, json=payload)
        except Exception as exc:                          # noqa: BLE001
            duration = int((time.monotonic() - started) * 1000)
            code = _classify_transport_exception(exc)
            return ProviderInferenceResult(
                provider=self.provider, model=model, status=STATUS_ERROR,
                auth_mode=self.home.auth_mode, duration_ms=duration,
                error_code=code,
                # Ключ передаётся редактору ЯВНЫМ литералом, а не оставляется
                # на правило формы `sk-…`. Правило формы описывает ключи,
                # которые мы видели; исключение клиента способно вернуть
                # заголовок в любом виде, включая усечённый и перекодированный,
                # и тогда правило не сработает, а литерал сработает.
                detail=redaction.redact(
                    f"{type(exc).__name__}: {exc}", extra_literals=(api_key,),
                ),
            )
        finally:
            # Значение перестаёт быть достижимым сразу после запроса. Питон не
            # даёт затереть строку в памяти, но убрать последнюю ссылку — даёт,
            # и это единственное, что здесь вообще можно сделать честно.
            del api_key
            headers.pop("Authorization", None)
        duration = int((time.monotonic() - started) * 1000)
        return self._finalize(
            response,
            duration_ms=duration,
            requested_model=str(model or ""),
            accepted=tuple(accepted),
            model_report=model_report,
        )

    def _finalize(
        self,
        response: Any,
        *,
        duration_ms: int,
        requested_model: str,
        accepted: Sequence[str],
        model_report: str,
    ) -> ProviderInferenceResult:
        """Разбор ответа шлюза. Ни одна ветка не возвращает сырьё без редактора."""
        status_code = int(getattr(response, "status_code", 0) or 0)
        body_text = ""
        try:
            body_text = response.text or ""
        except Exception:                                  # noqa: BLE001
            body_text = ""
        if status_code >= 400:
            return ProviderInferenceResult(
                provider=self.provider, model=requested_model or None,
                status=STATUS_ERROR, auth_mode=self.home.auth_mode,
                duration_ms=duration_ms, exit_code=status_code,
                error_code=classify_http_status(status_code, body_text),
                # Тело ответа шлюза может содержать эхо запроса. В `detail`
                # уходит КОД и короткая выжимка, прошедшая редактор.
                detail=redaction.redact(f"HTTP {status_code}: {body_text[:300]}"),
                raw_sha256=sha256_text(body_text),
                raw_bytes=len(body_text.encode("utf-8", errors="replace")),
            )
        try:
            data = response.json()
        except Exception:                                  # noqa: BLE001
            return ProviderInferenceResult(
                provider=self.provider, model=requested_model or None,
                status=STATUS_ERROR, auth_mode=self.home.auth_mode,
                duration_ms=duration_ms, exit_code=status_code,
                error_code=errors.ERR_MALFORMED_STATUS,
                detail="ответ шлюза не является JSON",
                raw_sha256=sha256_text(body_text),
                raw_bytes=len(body_text.encode("utf-8", errors="replace")),
            )
        if not isinstance(data, dict):
            return ProviderInferenceResult(
                provider=self.provider, model=requested_model or None,
                status=STATUS_ERROR, auth_mode=self.home.auth_mode,
                duration_ms=duration_ms, exit_code=status_code,
                error_code=errors.ERR_MALFORMED_STATUS,
                detail="ответ шлюза не является объектом JSON",
            )
        # Шлюз умеет возвращать 200 с полем `error` внутри — «ошибка апстрима,
        # но HTTP в порядке». Без этой ветки такой ответ прошёл бы как пустой
        # успех, и этап получил бы ноль замечаний вместо отказа.
        upstream_error = data.get("error")
        if isinstance(upstream_error, dict) or isinstance(upstream_error, str):
            summary = (
                upstream_error.get("message") if isinstance(upstream_error, dict)
                else upstream_error
            )
            code = (
                upstream_error.get("code") if isinstance(upstream_error, dict) else None
            )
            classified = (
                classify_http_status(int(code), str(summary or ""))
                if isinstance(code, int)
                else (errors.classify_text(str(summary or "")) or errors.ERR_PROVIDER_UNAVAILABLE)
            )
            return ProviderInferenceResult(
                provider=self.provider, model=requested_model or None,
                status=STATUS_ERROR, auth_mode=self.home.auth_mode,
                duration_ms=duration_ms, exit_code=status_code,
                error_code=classified,
                detail=redaction.redact(f"шлюз вернул ошибку: {str(summary)[:300]}"),
            )

        choices = data.get("choices")
        choice = choices[0] if isinstance(choices, list) and choices else {}
        message = choice.get("message") if isinstance(choice, dict) else {}
        raw = (message or {}).get("content") or ""
        if not isinstance(raw, str):
            raw = json.dumps(raw, ensure_ascii=False)
        finish_reason = (choice or {}).get("finish_reason")
        reported_model = str(data.get("model") or "") or None

        usage = _normalize_usage(data.get("usage"))
        common = {
            "provider": self.provider,
            "model": reported_model or requested_model or None,
            "auth_mode": self.home.auth_mode,
            "duration_ms": duration_ms,
            # `exit_code` — общее поле контракта, и `validate_inference`
            # требует от него РОВНО НУЛЯ на успехе (`inference.py`, проверка
            # "exit_code"). Ставить сюда 200 значило бы, что каждый успешный
            # HTTP-вызов проваливает проверку результата. Поэтому семантика
            # сохраняется дословно — «ноль = получилось», — а сам статус
            # ответа попадает в `detail` тех веток, где он что-то объясняет.
            "exit_code": 0 if 200 <= status_code < 300 else status_code,
            "usage": usage,
            "raw_sha256": sha256_text(raw),
            "raw_bytes": len(raw.encode("utf-8", errors="replace")),
        }

        # Сверка модели — тот же рубеж, что у CLI-провайдеров (11D). Шлюз
        # официально маршрутизирует запрос между апстримами и вправе ответить
        # другой строкой; молча принять её значило бы получить аудит, который
        # не с чем сравнивать.
        if reported_model and accepted and model_report != "optional":
            if not _model_matches(reported_model, accepted):
                return ProviderInferenceResult(
                    status=STATUS_ERROR,
                    error_code=errors.ERR_MODEL_MISMATCH,
                    detail=(
                        f"шлюз ответил моделью {reported_model!r}, а локальная "
                        f"политика назначила {requested_model!r}"
                    ),
                    **common,
                )

        if finish_reason == "length":
            # Усечённый ответ нельзя считать успехом, даже если обрезанный JSON
            # случайно разобрался: часть находок потеряна. Дословно то же
            # правило, что на центральном пути.
            return ProviderInferenceResult(
                status=STATUS_ERROR,
                error_code=errors.ERR_MALFORMED_STATUS,
                detail="ответ усечён (finish_reason=length): часть результата потеряна",
                **common,
            )
        parsed = self._first_json_object(raw)
        if parsed is None:
            return ProviderInferenceResult(
                status=STATUS_ERROR,
                error_code=errors.ERR_MALFORMED_STATUS,
                detail="в ответе модели нет JSON-объекта",
                **common,
            )
        return ProviderInferenceResult(status=STATUS_SUCCESS, result=parsed, **common)

    # ─── Окружение подпроцесса ───────────────────────────────────────────────
    def provider_env(self) -> dict[str, str]:
        """Подпроцесса нет — переменных для него тоже.

        Метод абстрактный в базовом классе, и реализовать его надо. Пустой
        словарь здесь — утверждение, а не заглушка: если однажды кто-то позовёт
        `run()` у этого адаптера, окружение окажется пустым и запуск провалится
        громко, а не унаследует чужие переменные.
        """
        return {}


def classify_http_status(status_code: int, body: str = "") -> str:
    """Код ответа шлюза → код ошибки провайдера (§23 задания).

    Классификация по СТАТУСУ, а не по тексту: текст ошибки платного шлюза
    меняется без предупреждения, а 429 остаётся 429. Текст используется только
    там, где статус неоднозначен.
    """
    if status_code in (401, 403):
        return errors.ERR_AUTH_REQUIRED
    if status_code == 402:
        # Отдельный случай, и он не `auth_required`: ключ действителен, но
        # денег на нём нет. Оператору нужны разные действия — пополнить счёт
        # либо перевыпустить ключ.
        return errors.ERR_RATE_LIMITED
    if status_code == 429:
        return errors.ERR_RATE_LIMITED
    if status_code == 404:
        # Шлюз отвечает 404 на неизвестную модель. Это не «сервис недоступен».
        return errors.ERR_INCOMPATIBLE_CLI
    if status_code == 408 or status_code == 504:
        return errors.ERR_TIMEOUT
    if 500 <= status_code < 600:
        return errors.ERR_PROVIDER_UNAVAILABLE
    from_text = errors.classify_text(body)
    if from_text:
        return from_text
    if 400 <= status_code < 500:
        return errors.ERR_MALFORMED_STATUS
    return errors.ERR_UNKNOWN


def _classify_transport_exception(exc: BaseException) -> str:
    """Исключение HTTP-клиента → код ошибки. Таймаут отличается от сети."""
    name = type(exc).__name__.lower()
    if "timeout" in name:
        return errors.ERR_TIMEOUT
    if "proxy" in name or "connect" in name or "network" in name or "transport" in name:
        return errors.ERR_NETWORK
    return errors.classify_exception(exc)


def _model_matches(reported: str, accepted: Sequence[str]) -> bool:
    """Совпал ли фактический идентификатор с одним из допустимых.

    Сравнение нечувствительно к регистру и к суффиксу после `:` — шлюз
    приписывает к строке модели вариант маршрутизации (`…:floor`, `…:nitro`),
    и считать это другой моделью было бы неверно.
    """
    def _norm(value: str) -> str:
        return str(value or "").strip().lower().split(":", 1)[0]

    target = _norm(reported)
    return any(_norm(item) == target for item in accepted)


def _normalize_usage(raw: Any) -> dict[str, Any]:
    """`usage` шлюза → общая форма (§22 задания).

    Имена полей приводятся к тем же, что отдают CLI-адаптеры
    (`input_tokens`/`output_tokens`/`total_cost_usd`), иначе счётчики этапа
    считали бы ноль на одной ноге ансамбля и настоящее число на другой.
    Дополнительного запроса ради расхода здесь нет: берётся то, что уже пришло
    в ответе.
    """
    if not isinstance(raw, dict):
        return {}
    details = raw.get("completion_tokens_details")
    out: dict[str, Any] = {}
    prompt_tokens = raw.get("prompt_tokens")
    completion_tokens = raw.get("completion_tokens")
    if isinstance(prompt_tokens, (int, float)):
        out["input_tokens"] = int(prompt_tokens)
    if isinstance(completion_tokens, (int, float)):
        out["output_tokens"] = int(completion_tokens)
    if isinstance(raw.get("total_tokens"), (int, float)):
        out["total_tokens"] = int(raw["total_tokens"])
    if isinstance(details, dict) and isinstance(
        details.get("reasoning_tokens"), (int, float)
    ):
        out["reasoning_tokens"] = int(details["reasoning_tokens"])
    # OpenRouter отдаёт стоимость в кредитах поля `cost` (равны долларам США).
    # Приводим к тому же имени, под которым цену отдают CLI-адаптеры.
    cost = raw.get("cost")
    if isinstance(cost, (int, float)):
        out["total_cost_usd"] = float(cost)
    return out


__all__ = [
    "OpenRouterProviderAdapter",
    "OpenRouterEndpointError",
    "OFFICIAL_BASE_URL",
    "BASE_URL_ENV",
    "STUBBED_ENDPOINTS_ENV",
    "SUPPORTED_IMAGE_MEDIA_TYPES",
    "classify_http_status",
    "resolve_base_url",
    "stubbed_endpoints_declared",
]
