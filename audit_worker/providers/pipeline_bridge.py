"""Мост «конвейер → ProviderAdapter»: ЕДИНСТВЕННЫЙ путь модели в audit_pipeline_v1.

До этапа 11C разрыв был объявленным: провайдерский слой жил в агенте и в
CLI-подкоманде, а конвейер о нём не знал вовсе (док 11b §13, §27 — нерешённая
развилка). Конвейер звал CLI сам, через `backend/app/services/llm/*`, с
`HOME` внутри каталога попытки — то есть находил НЕавторизованный бинарь.

Мост закрывает разрыв, не заводя второго провайдерского фреймворка:

  * авторизация, окружение с нуля, отключение инструментов, нейтрализация
    личного контекста, `stdin`, убийство группы по таймауту и редакция вывода
    остаются ровно там, где были, — в `ProviderAdapter`;
  * решение «каким провайдером» принимает `ProviderResolver` на стороне
    исполнителя, ДО запуска процесса конвейера, и записывает его файлом;
  * этот модуль только предъявляет решение адаптеру и следит, чтобы вызов
    случился не больше одного раза (I-P9).

Границы, которые мост НЕ переходит:

  * не читает учётных данных и не копирует их — адаптер находит их сам;
  * не получает worker-token, адрес центра и execution-token: их нет ни в
    привязке, ни в окружении процесса конвейера;
  * не выписывает разрешений. Разрешение (`inference_grant`) списывает
    исполнитель ДО запуска процесса, и мост лишь проверяет, что в привязке
    стоит его идентификатор;
  * не активируется сам. Нет переменной `AUDIT_WORKER_PROVIDER_BINDING` — нет
    моста, и код платформы ведёт себя ровно как до 11C. На центре переменной
    нет никогда.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Sequence

from audit_worker.providers import errors
from audit_worker.providers.auth_mode import (
    AUTH_MODE_AMBIENT_USER,
    resolve_ambient_home,
)
from audit_worker.providers.inference import (
    STATUS_ERROR,
    ProviderInferenceResult,
    ValidationReport,
    sha256_text,
    validate_inference,
)
from audit_worker.providers.inference_ledger import (
    STATE_ALLOWED,
    STATE_INDETERMINATE,
    STATE_REPLAY,
    InferenceLedger,
    LedgerEntry,
    call_key,
)
from audit_worker.providers.paths import (
    PROVIDER_CLAUDE,
    PROVIDER_CODEX,
    ProviderHome,
)
from audit_worker.providers.resolver import (
    BINDING_ENV,
    BINDING_FILENAME,
    ProviderBinding,
    ProviderResolutionError,
)


class ProviderBridgeError(RuntimeError):
    """Мост активен, но вызов невозможен. Тихого обхода не бывает."""


def attachments_digest(images: Sequence[tuple[str, bytes]]) -> str:
    """Отпечаток вложений вызова. Пустая последовательность — пустая строка.

    Считается по паре `(media_type, байты)` каждого вложения в ПОРЯДКЕ передачи:
    порядок содержателен, потому что модель видит блоки именно в нём.
    """
    if not images:
        return ""
    import hashlib

    digest = hashlib.sha256()
    for media_type, blob in images:
        digest.update(str(media_type).encode("utf-8", "replace"))
        digest.update(b"\x00")
        digest.update(hashlib.sha256(blob or b"").digest())
        digest.update(b"\x00")
    return digest.hexdigest()


def binding_path() -> Optional[Path]:
    raw = os.environ.get(BINDING_ENV, "").strip()
    return Path(raw) if raw else None


def active() -> bool:
    """Активен ли мост в ЭТОМ процессе.

    Три исхода, и третий появился на 11D:

      * переменной нет → `False`. Это центр, и поведение обязано остаться
        прежним;
      * переменная есть и файл на месте → `True`;
      * переменная есть, а файла нет → **исключение**.

    Третий случай раньше давал `False` — при том что докстринг обещал
    «заметность на первом же вызове». Обещание не выполнялось: вызывающий
    видел «мост неактивен» и уходил на прежний путь, то есть запускал
    `claude -p` по PATH из-под изолированного HOME (неавторизованным) либо
    платный HTTP. На синтетическом этапе 11C это было безобидно; на боевом
    этапе это тихий обход провайдерского слоя, замаскированный под обычную
    ошибку этапа. Ошибка развёртывания обязана падать, а не подменять
    транспорт.
    """
    raw = os.environ.get(BINDING_ENV, "").strip()
    if not raw:
        return False
    path = Path(raw)
    if path.is_file():
        return True
    raise ProviderBridgeError(
        f"{BINDING_ENV}={raw!r} задана, но файла привязки нет. Это ошибка "
        "развёртывания. Тихий возврат к прежнему транспорту запрещён: он "
        "означал бы вызов неавторизованного CLI из-под изолированного HOME."
    )


def load_binding() -> ProviderBinding:
    path = binding_path()
    if path is None:
        raise ProviderBridgeError(
            f"{BINDING_ENV} не задана: мост провайдеров в этом процессе не активен"
        )
    try:
        return ProviderBinding.read(path)
    except ProviderResolutionError as exc:
        raise ProviderBridgeError(str(exc)) from None


@dataclass(frozen=True)
class BridgeOutcome:
    """Что вернул мост: результат, состояние журнала и проверка."""

    provider_result: ProviderInferenceResult
    ledger: LedgerEntry
    validation: Optional[ValidationReport] = None
    #: True, если модель звали ИМЕННО СЕЙЧАС. False — результат взят из журнала.
    performed: bool = False

    @property
    def ok(self) -> bool:
        return self.provider_result.ok and (
            self.validation is None or self.validation.passed
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "performed": self.performed,
            "ledger": self.ledger.as_dict(),
            "provider_result": self.provider_result.as_dict(),
            "validation": self.validation.as_dict() if self.validation else None,
        }


def _build_home(binding: ProviderBinding, *, route=None) -> ProviderHome:
    """Раскладка провайдера по привязке.

    В ambient-режиме `ambient_home` вычисляется ЗДЕСЬ через базу учётных
    записей, а не берётся из привязки: путь в файле можно подменить, запись в
    `/etc/passwd` — нет. Это то же правило, что и в `auth_mode.resolve_ambient_home`,
    и ослаблять его на пути конвейера было бы бессмысленно.
    """
    provider_name = route.provider if route is not None else binding.provider
    auth_mode = route.auth_mode if route is not None and route.auth_mode else binding.auth_mode
    root = Path(
        route.provider_root if route is not None and route.provider_root
        else binding.provider_root
    )
    if auth_mode == AUTH_MODE_AMBIENT_USER:
        return ProviderHome(
            provider=provider_name,
            root=root,
            auth_mode=auth_mode,
            ambient_home=resolve_ambient_home(),
        )
    return ProviderHome(
        provider=provider_name,
        root=root,
        auth_mode=auth_mode,
    )


def _select_route(binding: ProviderBinding, *, provider: str, capability: str):
    """Маршрут под действие плана. Отсутствие описанной пары — отказ.

    Молчаливый откат на «основной» провайдер привязки здесь был бы худшим из
    возможных поведений: ансамбль выполнился бы, но не тем составом, и в
    результате об этом не осталось бы ни следа.
    """
    if not binding.routes:
        return None
    if not provider and not capability:
        return None
    route = binding.route_for(str(provider), str(capability))
    if route is None:
        raise ProviderBridgeError(
            f"привязка не содержит маршрута для провайдера {provider!r} и "
            f"способности {capability!r}: локальная политика воркера такой "
            f"пары не описывает. Подмена другим провайдером запрещена — "
            f"ансамбль, собранный не из тех ног, это не тот же аудит"
        )
    return route


def build_adapter(binding: ProviderBinding, *, on_process=None, route=None):
    """Собрать адаптер по привязке. Единственное место, где `inference_allowed=True`.

    Флаг ставится не «по конфигурации», а по факту существования привязки,
    которую исполнитель выписал, уже списав разрешение оператора. Разрешение,
    следующее из переменной окружения процесса конвейера, было бы разрешением,
    которое воркер выдал себе сам.
    """
    from audit_worker.providers.claude_adapter import ClaudeProviderAdapter
    from audit_worker.providers.codex_adapter import CodexProviderAdapter

    classes = {
        PROVIDER_CLAUDE: ClaudeProviderAdapter,
        PROVIDER_CODEX: CodexProviderAdapter,
    }
    provider_name = route.provider if route is not None else binding.provider
    factory = classes.get(provider_name)
    if factory is None:
        raise ProviderBridgeError(f"неизвестный провайдер привязки: {provider_name!r}")
    home = _build_home(binding, route=route)
    try:
        home.ensure_dirs()
    except OSError as exc:
        raise ProviderBridgeError(f"не создать раскладку провайдера: {exc}") from None
    executable = (
        route.executable if route is not None and route.executable else binding.executable
    )
    return factory(
        home,
        executable=Path(executable) if executable else None,
        timeout_sec=(route.timeout_sec if route is not None else binding.timeout_sec),
        inference_allowed=True,
        on_process=on_process,
    )


def run_stage_inference(
    *,
    job_dir: Path,
    stage: str,
    prompt: str,
    purpose: str = "",
    binding: Optional[ProviderBinding] = None,
    #: Действие ПЛАНА МАРШРУТИЗАЦИИ (этап 11I). Тройка «идентификатор действия +
    #: провайдер + способность» заменяет прежнее «одна модель на попытку».
    #:
    #: `action_id` обязателен не ради журнала, а ради exactly-once: ключ вызова
    #: складывался из (попытка, провайдер, purpose, промпт, вложения), и две
    #: ноги ансамбля с ОДИНАКОВЫМ промптом давали ОДИН ключ. Вторая нога молча
    #: получала replay ответа первой — то есть ансамбль из двух моделей
    #: вырождался в одну, не оставляя следа.
    action_id: str = "",
    provider: str = "",
    capability: str = "",
    reasoning_effort: str = "",
    required_result_fields: Sequence[str] = (),
    field_types: Optional[dict[str, Any]] = None,
    expected_semantics: Optional[dict[str, Any]] = None,
    claim_task_id: str = "",
    claim_attempt_id: str = "",
    timeout_sec: Optional[float] = None,
    on_process=None,
    images: Sequence[tuple[str, bytes]] = (),
) -> BridgeOutcome:
    """Выполнить ОДИН вызов модели для этапа конвейера.

    Порядок шагов — и есть содержание инварианта I-P9:

      1. привязка и белый список этапов (что нельзя — нельзя до всего);
      2. потолок вызовов попытки;
      3. журнал: `replay` → отдаём сохранённое, `indeterminate` → отказ;
      4. заявка в журнале СОЗДАЁТСЯ ДО обращения к модели;
      5. вызов;
      6. результат сохраняется в журнал СРАЗУ, до любой проверки и до любого
         события — иначе падение между «ответили» и «сохранили» стоило бы
         второго оплаченного вызова.
    """
    binding = binding or load_binding()
    stage_name = str(stage or "")
    purpose = str(purpose or stage_name or "inference")
    if binding.routes and not action_id:
        # Привязка с маршрутами означает, что задание пришло с планом. Вызов
        # без идентификатора действия в такой попытке — это вызов, которого нет
        # в плане, и разрешать его значило бы вернуть «маршрут решается на
        # месте» ровно там, где 11I его убирает.
        raise ProviderBridgeError(
            f"этап {stage_name!r} обращается к модели без action_id, а задание "
            "пришло с планом маршрутизации: каждое обращение обязано "
            "соответствовать действию плана"
        )
    if binding.allowed_stages and stage_name not in binding.allowed_stages:
        raise ProviderBridgeError(
            f"этап {stage_name!r} не входит в белый список привязки "
            f"{list(binding.allowed_stages)}: обращение к модели запрещено"
        )
    route = _select_route(binding, provider=provider, capability=capability)
    effective_provider = route.provider if route else binding.provider
    ledger = InferenceLedger(job_dir, attempt_id=binding.attempt_id, job_id=binding.job_id)
    images = tuple(images or ())
    attachments_sha256 = attachments_digest(images)
    key = call_key(
        attempt_id=binding.attempt_id, provider=effective_provider,
        purpose=purpose, prompt=prompt,
        attachments_sha256=attachments_sha256,
        action_id=action_id,
    )
    summary = ledger.summary()
    entry = ledger.inspect(key)
    if entry.state == STATE_REPLAY and entry.result is not None:
        return BridgeOutcome(
            provider_result=entry.result, ledger=entry, performed=False,
            validation=_validate(entry.result, binding=binding, route=route,
                                 required_result_fields=required_result_fields,
                                 field_types=field_types,
                                 expected_semantics=expected_semantics,
                                 claim_task_id=claim_task_id,
                                 claim_attempt_id=claim_attempt_id),
        )
    if entry.state == STATE_INDETERMINATE:
        raise ProviderBridgeError(
            f"вызов {key} уже начинался, но результат не сохранён. Повтор "
            "запрещён (I-P9): неизвестно, была ли попытка оплачена. Решение "
            "принимает оператор."
        )
    if int(summary.get("calls_started", 0)) >= int(binding.max_inferences):
        raise ProviderBridgeError(
            f"исчерпан потолок вызовов попытки: начато {summary.get('calls_started')} "
            f"из {binding.max_inferences}"
        )
    if not binding.grant_id:
        raise ProviderBridgeError(
            "в привязке нет идентификатора разрешения: рабочий вызов модели "
            "без списанного разрешения оператора не выполняется"
        )

    # Адаптер строится ДО заявки в журнале, и это порядок, а не стиль.
    #
    # Раньше он строился после `begin`, и всё, что могло сорваться при сборке
    # (`resolve_ambient_home`, `ensure_dirs`, неизвестный провайдер), оставляло
    # в журнале заявку-сироту: попытка навсегда «неизвестного исхода», хотя
    # адаптер даже не был создан и модель заведомо не звали.
    adapter = build_adapter(binding, on_process=on_process, route=route)
    # То же и с гейтами, знать которые можно ЗАРАНЕЕ: провайдер отключён
    # политикой, вызовы запрещены, CLI отсутствует, промпт пуст, модель
    # назначена без списка допустимых. Все они означают «до модели не дошли»,
    # а списанная заявка означала бы «дошли, но исход неизвестен» — то есть
    # съеденную попытку и вечный replay ошибки при любом повторе.
    unreachable = _preflight(adapter, binding, prompt, images=images, route=route)
    if unreachable:
        raise ProviderBridgeError(unreachable)

    claim = ledger.begin(
        key, provider=effective_provider, purpose=purpose,
        prompt_sha256=sha256_text(prompt),
    )
    if claim.state != STATE_ALLOWED:
        # Гонку выиграл другой процесс между `inspect` и `begin`.
        if claim.state == STATE_REPLAY and claim.result is not None:
            return BridgeOutcome(
                provider_result=claim.result, ledger=claim, performed=False,
                validation=_validate(claim.result, binding=binding, route=route,
                                     required_result_fields=required_result_fields,
                                     field_types=field_types,
                                     expected_semantics=expected_semantics,
                                     claim_task_id=claim_task_id,
                                     claim_attempt_id=claim_attempt_id),
            )
        raise ProviderBridgeError(
            f"вызов {key} захвачен другим процессом: повтор запрещён (I-P9)"
        )

    model_id = route.model if route else binding.model
    accepted_models = (
        route.accepted_reported_models if route else binding.accepted_reported_models
    )
    report_mode = route.model_report if route else binding.model_report

    # УРОВЕНЬ УСИЛИЯ (11I). До этой правки параметр принимался мостом и молча
    # выбрасывался: план обещал визуальной ноге оптимизации `xhigh`, а вызов
    # уходил на умолчание CLI — и хэш плана удостоверял свойство, которого в
    # прогоне не было. Передаётся только тем адаптерам, которые его понимают:
    # у Claude CLI такого параметра нет, и подсовывать его туда значило бы
    # менять `TypeError` на другую форму той же лжи.
    effort = str(reasoning_effort or "").strip().lower()
    effort_kwargs: dict[str, Any] = {}
    if effort:
        import inspect as _inspect

        target = (
            getattr(adapter, "structured_inference_multimodal", None) if images
            else getattr(adapter, "structured_inference", None)
        )
        try:
            accepts = "reasoning_effort" in _inspect.signature(target).parameters
        except (TypeError, ValueError):                 # noqa: BLE001
            accepts = False
        if accepts:
            effort_kwargs["reasoning_effort"] = effort
    try:
        # Модель и допустимые фактические её идентификаторы берутся ИЗ
        # ПРИВЯЗКИ (маршрута плана), а не из аргументов вызывающего: этап
        # конвейера называет СПОСОБНОСТЬ, а какой строке она соответствует на
        # этой машине, решила локальная политика до запуска процесса.
        if images:
            inference = getattr(adapter, "structured_inference_multimodal", None)
            if inference is None:
                raise ProviderBridgeError(
                    f"провайдер {binding.provider!r} не умеет передавать "
                    "изображения: молчаливый переход на текстовый вызов "
                    "запрещён — этап получил бы анализ чертежа без чертежа"
                )
            result = inference(
                prompt, images=images, purpose=purpose, timeout_sec=timeout_sec,
                model=model_id,
                accepted_reported_models=accepted_models,
                model_report=report_mode,
                **effort_kwargs,
            )
        else:
            result = adapter.structured_inference(
                prompt, purpose=purpose, timeout_sec=timeout_sec,
                model=model_id,
                accepted_reported_models=accepted_models,
                model_report=report_mode,
                **effort_kwargs,
            )
    except BaseException as exc:                    # noqa: BLE001 — см. ниже
        # Исключение ПОСЛЕ заявки означает неизвестный исход: запрос мог уйти.
        # Помечаем явно и не даём повторить автоматически.
        ledger.mark_indeterminate(key, reason=f"{type(exc).__name__}: {exc}")
        raise
    try:
        ledger.complete(key, result)
    except BaseException as exc:                    # noqa: BLE001
        # Ответ получен и ОПЛАЧЕН, а записать его не вышло (диск полон, ФС
        # только на чтение, каталог попытки снесён). Раньше эта строка стояла
        # вне защиты: заявка оставалась без пометки, и попытка выглядела как
        # «начата и брошена» — без единого слова о том, что результат вообще
        # был. Пометка не спасает сам результат, но делает исход честным.
        ledger.mark_indeterminate(
            key,
            reason=(
                f"результат получен, но не сохранён: {type(exc).__name__}: {exc}"
            ),
        )
        raise
    validation = _validate(
        result, binding=binding, route=route,
        required_result_fields=required_result_fields,
        field_types=field_types, expected_semantics=expected_semantics,
        claim_task_id=claim_task_id, claim_attempt_id=claim_attempt_id,
    )
    return BridgeOutcome(
        provider_result=result, ledger=ledger.inspect(key),
        validation=validation, performed=True,
    )


def _preflight(
    adapter,
    binding: ProviderBinding,
    prompt: str,
    *,
    images: Sequence[tuple[str, bytes]] = (),
    route=None,
) -> str:
    """Причина, по которой вызов НЕ СОСТОИТСЯ. Пустая строка — путь открыт.

    Проверяется ровно то, что известно ДО запуска процесса. Смысл — отделить
    «до модели не дошли» от «модель ответила ошибкой»: первое не должно стоить
    ни списанной заявки, ни съеденного потолка, ни вечного replay при повторе.

    Проверяется МАРШРУТ, которым вызов будет сделан, а не первичная привязка:
    у codex-ноги своя модель, свой список допустимых идентификаторов и свой
    адаптер, и «модель назначена» у первичного провайдера ничего о ней не
    говорит.
    """
    provider_name = route.provider if route is not None else binding.provider
    model_id = route.model if route is not None else binding.model
    accepted = (
        route.accepted_reported_models if route is not None
        else binding.accepted_reported_models
    )
    if not model_id:
        # Вызов без НАЗНАЧЕННОЙ модели — это ровно та слепота 11C, ради
        # устранения которой писалась локальная политика: argv уходит без
        # `--model`, отвечает модель учётной записи по умолчанию, а обе сверки
        # (в адаптере и в проверке результата) условны и молча пропускаются.
        # Рабочий этап на непроверяемых условиях не выполняется.
        return (
            "в привязке нет назначенной модели: рабочий вызов без явной модели "
            "не выполняется (иначе ответила бы модель учётной записи по "
            "умолчанию, и ни одна проверка этого не заметила бы)"
        )
    if getattr(adapter, "policy_blocked", False):
        return f"провайдер {provider_name!r} отключён политикой на этом воркере"
    if not getattr(adapter, "inference_allowed", False):
        return f"рабочий вызов модели не разрешён на адаптере {provider_name!r}"
    if not str(prompt or "").strip():
        return "пустой промпт: рабочий вызов не выполняется"
    if images and not hasattr(adapter, "structured_inference_multimodal"):
        # Проверяется ДО заявки: провайдер, не умеющий вложений, — это «до
        # модели не дошли», и стоить попытки это не должно.
        return (
            f"провайдер {provider_name!r} не умеет передавать изображения: "
            "молчаливый переход на текстовый вызов запрещён — этап получил бы "
            "анализ чертежа без чертежа"
        )
    if model_id and not accepted:
        return (
            f"назначенная модель есть, но список допустимых фактических "
            f"идентификаторов у маршрута {provider_name!r} пуст: сверять ответ "
            "не с чем"
        )
    try:
        installed = adapter.installed()
    except Exception:                                   # noqa: BLE001
        installed = False
    if not installed:
        return (
            f"CLI провайдера {provider_name!r} не найден по пути привязки: "
            "вызов невозможен"
        )
    return ""


def _validate(
    result: ProviderInferenceResult,
    *,
    binding: ProviderBinding,
    required_result_fields: Sequence[str],
    field_types: Optional[dict[str, Any]],
    expected_semantics: Optional[dict[str, Any]],
    claim_task_id: str = "",
    claim_attempt_id: str = "",
    route=None,
) -> ValidationReport:
    """Сверка идентичности здесь НЕ тавтологична — и это главное в функции.

    Слева стоит привязка (её написал ИСПОЛНИТЕЛЬ по строке очереди), справа —
    значения, которые вызывающий взял из `run_spec.json` (его написал
    `audit_runner` по той же строке очереди, но другим кодом и другим полем).
    Совпадение двух независимых записей и есть доказательство «результат
    относится к этой попытке». Сравнение привязки с самой собой доказывало бы
    только то, что файл не поменялся между двумя строками кода.

    С 11I у попытки НЕСКОЛЬКО маршрутов, и сверять ответ codex-ноги с
    ожиданиями первичного провайдера привязки нельзя: у них разный провайдер,
    разная модель и разный режим отчёта о модели. Такая сверка не «строже» —
    она просто ложна, и падала бы КАЖДАЯ нога, кроме первичной, уже ПОСЛЕ
    оплаченного вызова. Поэтому ожидания берутся из МАРШРУТА, которым вызов
    фактически сделан; поля попытки (`task_id`, `attempt_id`, запрещённые
    литералы) остаются от привязки — они общие для всех маршрутов.
    """
    return validate_inference(
        result,
        expected_provider=(route.provider if route is not None else binding.provider),
        expected_auth_mode=(
            route.auth_mode if route is not None and route.auth_mode
            else binding.auth_mode
        ),
        required_result_fields=required_result_fields,
        field_types=field_types,
        expected_semantics=expected_semantics,
        forbidden_literals=binding.forbidden_literals,
        task_id=binding.task_id,
        attempt_id=binding.attempt_id,
        claim_task_id=claim_task_id or binding.task_id,
        claim_attempt_id=claim_attempt_id or binding.attempt_id,
        expected_model=(
            (route.model if route is not None else binding.model) or ""
        ),
        accepted_reported_models=(
            route.accepted_reported_models if route is not None
            else binding.accepted_reported_models
        ),
        model_report=(
            route.model_report if route is not None else binding.model_report
        ),
    )


def attempt_dir() -> Path:
    """Каталог попытки, выведенный из пути привязки.

    Привязка лежит в `<job_dir>/metadata/provider_binding.json`, поэтому каталог
    попытки — родитель её родителя. Отдельного поля в привязке для этого нет
    намеренно: два источника одного пути расходятся ровно тогда, когда это
    дороже всего — при разборе чужого прогона.

    ФОРМА ПУТИ ПРОВЕРЯЕТСЯ. Без проверки привязка, положенная в `/tmp/b.json`,
    давала бы «каталог попытки» = `/`, и построенный на нём гейт «писать только
    внутрь попытки» вырождался бы в тождественную истину: под `/` лежит всё.
    Корень доверия обязан иметь ту же форму, что и раскладка, которая его
    порождает.
    """
    path = binding_path()
    if path is None:
        raise ProviderBridgeError(f"{BINDING_ENV} не задана")
    resolved = path.resolve()
    if resolved.name != BINDING_FILENAME or resolved.parent.name != "metadata":
        raise ProviderBridgeError(
            f"привязка лежит не по раскладке попытки: {resolved}. Ожидается "
            f"<каталог попытки>/metadata/{BINDING_FILENAME} — иначе каталог "
            "попытки определяется неверно и гейт записи теряет смысл"
        )
    return resolved.parent.parent


def stored_outcome(
    *,
    stage: str,
    prompt: str,
    purpose: str = "",
    binding: Optional[ProviderBinding] = None,
    required_result_fields: Sequence[str] = (),
    field_types: Optional[dict[str, Any]] = None,
    expected_semantics: Optional[dict[str, Any]] = None,
    claim_task_id: str = "",
    claim_attempt_id: str = "",
) -> Optional[BridgeOutcome]:
    """Достать УЖЕ сохранённый результат вызова, ничего не выполняя.

    Нужен там, где вызов сделан не самим вызывающим: этап конвейера зовёт
    модель через штатную точку `_run_cli`, которая возвращает legacy-кортеж
    `(код, текст, CLIResult)` и структурированному контракту места в себе не
    имеет. Вместо того чтобы протаскивать объект через слои, результат берётся
    из ЖУРНАЛА — то есть из того же места, которое переживает рестарт. Побочный
    выигрыш: путь чтения результата один и тот же и после вызова, и после
    перезапуска процесса.
    """
    binding = binding or load_binding()
    purpose = str(purpose or stage or "inference")
    ledger = InferenceLedger(
        attempt_dir(), attempt_id=binding.attempt_id, job_id=binding.job_id
    )
    key = call_key(
        attempt_id=binding.attempt_id, provider=binding.provider,
        purpose=purpose, prompt=prompt,
    )
    entry = ledger.inspect(key)
    if entry.state != STATE_REPLAY or entry.result is None:
        return None
    return BridgeOutcome(
        provider_result=entry.result,
        ledger=entry,
        # «Выполнено сейчас» = вызов сделал ЭТОТ процесс. Читатель журнала и
        # исполнитель вызова здесь один и тот же процесс (этап зовёт
        # `_run_cli`, тот — мост), поэтому сравнение pid отвечает ровно на
        # нужный вопрос и не требует передавать флаг через слои.
        performed=(entry.performed_by_pid == os.getpid()),
        validation=_validate(
            entry.result, binding=binding,
            required_result_fields=required_result_fields,
            field_types=field_types, expected_semantics=expected_semantics,
            claim_task_id=claim_task_id, claim_attempt_id=claim_attempt_id,
        ),
    )


def route_cli_call(
    *, stage: str, prompt: str, timeout_sec: Optional[float] = None
) -> tuple[int, str, dict[str, Any]]:
    """Штатная точка вызова CLI конвейера, переведённая на ProviderAdapter.

    Возвращает то, что ждёт вызывающий из `backend/app/services/llm`: код
    возврата, текст ответа и числовой расход. Структурированный результат при
    этом никуда не девается — он лежит в журнале попытки, и этап забирает его
    через `stored_outcome`.

    Отказ здесь ВСЕГДА исключение, а не «вернёмся к прежнему пути». Мост
    активен только тогда, когда исполнитель выписал привязку, уже списав
    разрешение оператора; молчаливый обход в этот момент означал бы вызов
    неавторизованного CLI из-под изолированного HOME — то есть тихий провал,
    который выглядит как обычная ошибка этапа.
    """
    outcome = run_stage_inference(
        job_dir=attempt_dir(), stage=stage, prompt=prompt, timeout_sec=timeout_sec,
    )
    result = outcome.provider_result
    # Текстом отдаётся КАНОНИЧЕСКИЙ разобранный объект, а не сырой ответ: он
    # уже прошёл разбор и не тащит за собой обрамление CLI.
    import json as _json

    text = _json.dumps(result.result, ensure_ascii=False) if result.result else (
        result.detail or ""
    )
    usage = dict(result.usage)
    usage.setdefault("duration_ms", result.duration_ms)
    return int(result.exit_code if result.exit_code is not None else 1), text, usage


def unavailable_result(provider: str, detail: str) -> ProviderInferenceResult:
    """Отказ в форме контракта — для путей, где исключение не годится."""
    return ProviderInferenceResult(
        provider=provider, model=None, status=STATUS_ERROR,
        error_code=errors.ERR_POLICY_BLOCKED, detail=detail,
    )
