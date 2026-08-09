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
    ProviderBinding,
    ProviderResolutionError,
)


class ProviderBridgeError(RuntimeError):
    """Мост активен, но вызов невозможен. Тихого обхода не бывает."""


def binding_path() -> Optional[Path]:
    raw = os.environ.get(BINDING_ENV, "").strip()
    return Path(raw) if raw else None


def active() -> bool:
    """Активен ли мост в ЭТОМ процессе.

    Проверяется наличие файла, а не только переменной: переменная, указывающая
    в никуда, — это ошибка развёртывания, и она обязана быть заметной на первом
    же вызове, а не превращаться в тихий возврат к прежнему пути.
    """
    path = binding_path()
    return bool(path and path.is_file())


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


def _build_home(binding: ProviderBinding) -> ProviderHome:
    """Раскладка провайдера по привязке.

    В ambient-режиме `ambient_home` вычисляется ЗДЕСЬ через базу учётных
    записей, а не берётся из привязки: путь в файле можно подменить, запись в
    `/etc/passwd` — нет. Это то же правило, что и в `auth_mode.resolve_ambient_home`,
    и ослаблять его на пути конвейера было бы бессмысленно.
    """
    if binding.auth_mode == AUTH_MODE_AMBIENT_USER:
        return ProviderHome(
            provider=binding.provider,
            root=Path(binding.provider_root),
            auth_mode=binding.auth_mode,
            ambient_home=resolve_ambient_home(),
        )
    return ProviderHome(
        provider=binding.provider,
        root=Path(binding.provider_root),
        auth_mode=binding.auth_mode,
    )


def build_adapter(binding: ProviderBinding, *, on_process=None):
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
    factory = classes.get(binding.provider)
    if factory is None:
        raise ProviderBridgeError(f"неизвестный провайдер привязки: {binding.provider!r}")
    home = _build_home(binding)
    try:
        home.ensure_dirs()
    except OSError as exc:
        raise ProviderBridgeError(f"не создать раскладку провайдера: {exc}") from None
    return factory(
        home,
        executable=Path(binding.executable) if binding.executable else None,
        timeout_sec=binding.timeout_sec,
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
    required_result_fields: Sequence[str] = (),
    field_types: Optional[dict[str, Any]] = None,
    expected_semantics: Optional[dict[str, Any]] = None,
    claim_task_id: str = "",
    claim_attempt_id: str = "",
    timeout_sec: Optional[float] = None,
    on_process=None,
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
    if binding.allowed_stages and stage_name not in binding.allowed_stages:
        raise ProviderBridgeError(
            f"этап {stage_name!r} не входит в белый список привязки "
            f"{list(binding.allowed_stages)}: обращение к модели запрещено"
        )
    ledger = InferenceLedger(job_dir, attempt_id=binding.attempt_id, job_id=binding.job_id)
    key = call_key(
        attempt_id=binding.attempt_id, provider=binding.provider,
        purpose=purpose, prompt=prompt,
    )
    summary = ledger.summary()
    entry = ledger.inspect(key)
    if entry.state == STATE_REPLAY and entry.result is not None:
        return BridgeOutcome(
            provider_result=entry.result, ledger=entry, performed=False,
            validation=_validate(entry.result, binding=binding,
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

    claim = ledger.begin(
        key, provider=binding.provider, purpose=purpose,
        prompt_sha256=sha256_text(prompt),
    )
    if claim.state != STATE_ALLOWED:
        # Гонку выиграл другой процесс между `inspect` и `begin`.
        if claim.state == STATE_REPLAY and claim.result is not None:
            return BridgeOutcome(
                provider_result=claim.result, ledger=claim, performed=False,
                validation=_validate(claim.result, binding=binding,
                                     required_result_fields=required_result_fields,
                                     field_types=field_types,
                                     expected_semantics=expected_semantics,
                                     claim_task_id=claim_task_id,
                                     claim_attempt_id=claim_attempt_id),
            )
        raise ProviderBridgeError(
            f"вызов {key} захвачен другим процессом: повтор запрещён (I-P9)"
        )

    adapter = build_adapter(binding, on_process=on_process)
    try:
        result = adapter.structured_inference(
            prompt, purpose=purpose, timeout_sec=timeout_sec,
        )
    except BaseException as exc:                    # noqa: BLE001 — см. ниже
        # Исключение ПОСЛЕ заявки означает неизвестный исход: запрос мог уйти.
        # Помечаем явно и не даём повторить автоматически.
        ledger.mark_indeterminate(key, reason=f"{type(exc).__name__}: {exc}")
        raise
    ledger.complete(key, result)
    validation = _validate(
        result, binding=binding,
        required_result_fields=required_result_fields,
        field_types=field_types, expected_semantics=expected_semantics,
        claim_task_id=claim_task_id, claim_attempt_id=claim_attempt_id,
    )
    return BridgeOutcome(
        provider_result=result, ledger=ledger.inspect(key),
        validation=validation, performed=True,
    )


def _validate(
    result: ProviderInferenceResult,
    *,
    binding: ProviderBinding,
    required_result_fields: Sequence[str],
    field_types: Optional[dict[str, Any]],
    expected_semantics: Optional[dict[str, Any]],
    claim_task_id: str = "",
    claim_attempt_id: str = "",
) -> ValidationReport:
    """Сверка идентичности здесь НЕ тавтологична — и это главное в функции.

    Слева стоит привязка (её написал ИСПОЛНИТЕЛЬ по строке очереди), справа —
    значения, которые вызывающий взял из `run_spec.json` (его написал
    `audit_runner` по той же строке очереди, но другим кодом и другим полем).
    Совпадение двух независимых записей и есть доказательство «результат
    относится к этой попытке». Сравнение привязки с самой собой доказывало бы
    только то, что файл не поменялся между двумя строками кода.
    """
    return validate_inference(
        result,
        expected_provider=binding.provider,
        expected_auth_mode=binding.auth_mode,
        required_result_fields=required_result_fields,
        field_types=field_types,
        expected_semantics=expected_semantics,
        forbidden_literals=binding.forbidden_literals,
        task_id=binding.task_id,
        attempt_id=binding.attempt_id,
        claim_task_id=claim_task_id or binding.task_id,
        claim_attempt_id=claim_attempt_id or binding.attempt_id,
    )


def attempt_dir() -> Path:
    """Каталог попытки, выведенный из пути привязки.

    Привязка лежит в `<job_dir>/metadata/provider_binding.json`, поэтому каталог
    попытки — родитель её родителя. Отдельного поля в привязке для этого нет
    намеренно: два источника одного пути расходятся ровно тогда, когда это
    дороже всего — при разборе чужого прогона.
    """
    path = binding_path()
    if path is None:
        raise ProviderBridgeError(f"{BINDING_ENV} не задана")
    return path.resolve().parent.parent


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
