"""Слой ИИ-разрешения: неоднозначное расхождение → нормальная находка.

Место в конвейере строго определено:

    детерминированный синтез
        ↓  review_items (то, что система честно не смогла разобрать)
    ИИ-разрешение
        ↓
    верификатор                      ← провал не публикуется никогда
        ↓
    материализация тем же кодом, что и типизированный ответ человека
        ↓
    Stage 7 — готовая находка

    остаток → Stage 5 — вопрос инженеру

Что этот слой НЕ делает. Он не трогает обычные детерминированные изменения:
им модель не нужна. Он не пишет в ответы человека — у него собственный
артефакт. Он не ставит «подтверждено инженером»: подтверждает только инженер.
И он не является источником истины — источником остаётся детерминированный
слой, а ИИ лишь интерпретирует уже собранные доказательства.

Бюджеты сделаны так, чтобы исчерпание любого из них не роняло прогон: остаток
честно уезжает человеку с причиной BUDGET_EXHAUSTED, а не исчезает.
"""
from __future__ import annotations

import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

from .. import facet_taxonomy, object_identity
from ..production_artifacts import content_signature, stable_id, utc_now
from ..unified_change_policy.contract import UNKNOWN_DIMENSION
from . import cache as cache_module
from . import evidence as evidence_module
from . import gateway, prompts, response_contract, schemas, settings, verifier
from . import vision as vision_module

KIND = "stage_comparison_ai_resolutions"
SCHEMA_VERSION = "ai-resolutions.v1"
LAYER_VERSION = "stage-comparison-ai-resolution.v1"

#: Итог по одному элементу.
RESOLVED = "AI_RESOLVED"
HUMAN_REQUIRED = "HUMAN_REQUIRED"

#: Почему элемент остался человеку. Коды технические, к инженеру они попадают
#: уже переведёнными — см. review_presentation/фронтенд.
REASON_VERIFIER_REJECTED = "VERIFIER_REJECTED"
REASON_CRITIC_REJECTED = "CRITIC_REJECTED"
REASON_MODEL_FAILED = "MODEL_FAILED"
REASON_MODEL_TIMEOUT = "MODEL_TIMEOUT"
REASON_CANCELLED = "CANCELLED"
REASON_BUDGET_EXHAUSTED = "BUDGET_EXHAUSTED"
REASON_MODEL_DECLINED = "MODEL_DECLINED"
REASON_VISION_CONTRADICTS = "VISION_CONTRADICTS_TEXT"
REASON_VISION_INSUFFICIENT = "VISION_INSUFFICIENT"
#: Критик был обязателен и не смог ответить. Публиковать разбор нельзя:
#: заявленная режимом проверка не выполнена, а «не выполнена» и «выполнена и
#: не нашла ошибок» — разные утверждения.
REASON_CRITIC_UNAVAILABLE = "CRITIC_UNAVAILABLE"
#: Критик ОТВЕТИЛ, но ответ не соответствует собственной схеме: нет
#: обязательного поля, значение вне перечисления, не тот тип. Такой ответ не
#: является ни принятием, ни отклонением — по нему неизвестно ничего, и
#: «ACCEPT» в нём весит ровно столько же, сколько случайная строка.
REASON_CRITIC_INVALID = "CRITIC_INVALID"
#: Среда слоя не готова: нет CLI, модель не поддержана, изоляция не доказана.
REASON_RUNTIME_UNAVAILABLE = "RUNTIME_UNAVAILABLE"

ProgressCallback = Callable[[dict[str, Any]], None]


@dataclass
class _Budget:
    """Живой счётчик пределов прогона. Исчерпание — не ошибка, а остановка.

    Партии идут параллельно, поэтому «проверить и занять» обязано быть одной
    операцией: иначе четыре потока одновременно увидят последний свободный
    слот и займут его вчетвером.
    """

    max_items: int
    max_batches: int
    max_critic_passes: int
    max_vision_items: int
    deadline: float
    batches_started: int = 0
    critic_passes: int = 0
    vision_items: int = 0
    exhausted_reasons: set[str] = field(default_factory=set)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def out_of_time(self) -> bool:
        if time.monotonic() > self.deadline:
            with self._lock:
                self.exhausted_reasons.add("max_session_seconds")
            return True
        return False

    def _take(self, field_name: str, limit: int, reason: str) -> bool:
        with self._lock:
            if getattr(self, field_name) >= limit:
                self.exhausted_reasons.add(reason)
                return False
            setattr(self, field_name, getattr(self, field_name) + 1)
            return True

    def take_batch(self) -> bool:
        return self._take("batches_started", self.max_batches, "max_batches")

    def take_critic(self) -> bool:
        return self._take(
            "critic_passes", self.max_critic_passes, "max_critic_passes"
        )

    def take_vision(self) -> bool:
        return self._take("vision_items", self.max_vision_items, "max_vision_items")


def _audit(
    *,
    provider_family: str,
    model: str,
    reasoning_level: str | None,
    role: str,
    evidence_digest: str,
    output: Mapping[str, Any] | None,
    call: gateway.CallResult | None,
    cache_hit: bool,
) -> dict[str, Any]:
    """След одного обращения к модели. Секретов здесь нет и быть не может."""
    return {
        "provider_family": provider_family,
        "model": model,
        "reasoning_level": reasoning_level,
        "role": role,
        "prompt_version": prompts.prompt_versions().get(role),
        "schema_version": schemas.SCHEMA_VERSION,
        "verifier_version": verifier.VERIFIER_VERSION,
        "evidence_digest": evidence_digest,
        "output_digest": content_signature(output) if output is not None else None,
        "duration_ms": call.duration_ms if call is not None else 0,
        "attempts": call.attempts if call is not None else 0,
        "session_id": call.session_id if call is not None else None,
        "cache_hit": bool(cache_hit),
        "timestamp": utc_now(),
    }


def _human_entry(
    item: evidence_module.EvidenceItem,
    *,
    reason: str,
    detail: str = "",
    question: str | None = None,
    audit: Mapping[str, Any] | None = None,
    verifier_result: Mapping[str, Any] | None = None,
    critic_result: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "review_evidence_id": item.item_id,
        "atom_id": item.atom_id,
        "status": HUMAN_REQUIRED,
        "reason_code": reason,
        "reason_detail": detail[:500],
        "human_question": question,
        "typed_resolution": None,
        "confidence": None,
        "engineering_summary": None,
        "evidence_quotes": [],
        "verifier": dict(verifier_result) if verifier_result else None,
        "critic": dict(critic_result) if critic_result else None,
        "vision": None,
        "audit": dict(audit) if audit else None,
    }


def _grounding_corpus(item: evidence_module.EvidenceItem) -> list[str]:
    """Всё, что модель видела по этому элементу, — как плоские строки."""
    view = item.model_view()
    lines: list[str] = []
    for key in ("left_context", "right_context"):
        for line in view.get(key) or ():
            text = line.get("text") if isinstance(line, Mapping) else line
            if str(text or "").strip():
                lines.append(str(text))
    for key in ("before_value", "after_value"):
        value = view.get(key)
        if str(value or "").strip():
            lines.append(str(value))
    return lines


def _typed_resolution_from(
    resolution: Mapping[str, Any],
    item: evidence_module.EvidenceItem | None = None,
) -> dict[str, Any]:
    """Ответ модели → тот же типизированный контракт, что заполняет человек.

    Внутренние ссылки здесь НЕ ставятся: их детерминированно чеканит
    review_queue.mint_project_entity_ref из object_label — тем же кодом и тем
    же семейством префиксов, что и для ответа инженера. Иначе ИИ породил бы
    объект-двойник рядом с настоящим.

    Свойство — тот же случай. Распознанное детерминированным слоем свойство
    берётся как есть; предложенное моделью принимается, только если справочник
    его узнал. Чеканить `facet_ai_<что угодно>` из свободной строки значит
    выдавать за ссылку то, за чем не стоит ничего, кроме формулировки модели.
    """
    typed: dict[str, Any] = {
        "dimension": resolution.get("dimension"),
        "direction": resolution.get("direction"),
        "outcome": resolution.get("outcome"),
        "object_label": resolution.get("object_label"),
    }
    for name in ("before_value", "after_value"):
        value = resolution.get(name)
        if value is not None:
            typed[name] = value
    known_facet = ""
    if item is not None:
        known_facet = str(item.deterministic_state.get("facet_ref") or "").strip()
    if known_facet:
        typed["facet_ref"] = known_facet
    else:
        proposed = facet_taxonomy.facet_from_label(resolution.get("facet_label"))
        if proposed:
            typed["facet_ref"] = stable_id("facet_ai_", proposed)
    return {key: value for key, value in typed.items() if value is not None}


#: Поводы позвать критика. Список закрыт: «на всякий случай» поводом не
#: является. Критик — вторая модель на один элемент, и запускать его на каждом
#: существенном изменении значит платить за проверку там, где проверять нечего:
#: разбор, прошедший верификатор без единого замечания, с высокой уверенностью,
#: без повтора и без опоры на картинку, уже доказан цитатами.
TRIGGER_MATERIAL_UNSURE = "MATERIAL_WITHOUT_FULL_CONFIDENCE"
TRIGGER_LOW_CONFIDENCE = "LOW_CONFIDENCE"
TRIGGER_VERIFIER_RETRY = "VERIFIER_RETRY"
TRIGGER_VERIFIER_WARNINGS = "VERIFIER_WARNINGS"
TRIGGER_CONTRADICTION = "CONTRADICTION"
TRIGGER_VISION_DEPENDENT = "VISION_DEPENDENT_MATERIAL"

CRITIC_TRIGGERS = (
    TRIGGER_MATERIAL_UNSURE,
    TRIGGER_LOW_CONFIDENCE,
    TRIGGER_VERIFIER_RETRY,
    TRIGGER_VERIFIER_WARNINGS,
    TRIGGER_CONTRADICTION,
    TRIGGER_VISION_DEPENDENT,
)

#: Слова, которыми детерминированный слой сообщает о противоречии.
_CONTRADICTION_MARKERS = ("contradict", "contested", "conflict")


def critic_triggers(
    item: evidence_module.EvidenceItem,
    resolution: Mapping[str, Any],
    check: verifier.VerifyResult,
    *,
    retried: bool,
    vision_used: bool,
) -> list[str]:
    """Почему именно этот разбор нуждается во второй модели."""
    triggers: list[str] = []
    outcome = resolution.get("outcome")
    confidence = str(resolution.get("confidence") or "")
    if retried:
        triggers.append(TRIGGER_VERIFIER_RETRY)
    if check.warnings:
        triggers.append(TRIGGER_VERIFIER_WARNINGS)
    if confidence in {"LOW", "UNKNOWN"}:
        triggers.append(TRIGGER_LOW_CONFIDENCE)
    if outcome == "MATERIAL_CHANGE" and confidence != "HIGH":
        triggers.append(TRIGGER_MATERIAL_UNSURE)
    if outcome == "MATERIAL_CHANGE" and vision_used:
        triggers.append(TRIGGER_VISION_DEPENDENT)
    reason_codes = " ".join(
        str(code) for code in (item.deterministic_state or {}).get("reason_codes") or ()
    ).lower()
    if any(marker in reason_codes for marker in _CONTRADICTION_MARKERS):
        triggers.append(TRIGGER_CONTRADICTION)
    return sorted(set(triggers))


class AiResolutionLayer:
    """Одна ограниченная задача на прогон. Долгоживущей памяти сессии нет.

    Корректность системы не зависит от того, удастся ли восстановить сессию
    модели: каждый вызов самодостаточен, а его результат либо проходит
    верификатор и становится находкой, либо не публикуется вовсе.
    """

    def __init__(
        self,
        *,
        cache_dir: Path | str | None = None,
        cancel: gateway.CancelToken | None = None,
        progress: ProgressCallback | None = None,
        call: Callable[..., gateway.CallResult] | None = None,
        pdf_paths: Mapping[str, str] | None = None,
        graphic_route: str | None = None,
        stamp_identity_by_side: Mapping[str, Mapping[str, Any]] | None = None,
        run_id: str = "",
        mode: str | None = None,
    ) -> None:
        # Режим фиксируется на прогон, а не читается из окружения на каждом
        # элементе: партии идут в пуле потоков, и переменная окружения,
        # изменённая посреди прогона, дала бы разную глубину у соседних партий.
        self.mode = settings.normalize_mode(mode) if mode else settings.mode()
        self.deep = self.mode == settings.MODE_DEEP
        self.cache = cache_module.ResponseCache(cache_dir)
        self.cancel = cancel or gateway.CancelToken()
        # Метка прогона на каждом дочернем процессе: отмена одной пары не
        # имеет права снести вызовы соседней, а параллельные пары в очереди —
        # обычный режим.
        self.run_id = run_id or uuid.uuid4().hex
        self.progress = progress
        self._call = call or gateway.call
        # Пути к PDF нужны только визуальному резерву; без них он выключен —
        # рисовать нечего, и это честнее, чем звать модель без картинки.
        self.pdf_paths = dict(pdf_paths or {})
        self.graphic_route = graphic_route
        # Штамп листа, прочитанный из вектор-слоя. Он первичен: увиденное на
        # картинке не имеет права молча его переопределить.
        # Запасной путь для вызовов без пакета: обычно идентичность приезжает
        # на самом элементе, из отношения листов.
        self.stamp_identity_by_side = {
            str(side).upper(): dict(value)
            for side, value in (stamp_identity_by_side or {}).items()
            if isinstance(value, Mapping)
        }
        self._counters = threading.Lock()
        self.model_calls = 0
        self.failures = 0
        self.timeouts = 0
        self.vision_calls = 0
        # Отказ верификатора, за которым последовал успешный повтор, не виден
        # в итоговых причинах — а именно он показывает, сколько стоит первый
        # проход на низком уровне рассуждения.
        self.verifier_failed_first_pass = 0
        self.retries_used = 0
        # Сколько раз критик был ОБЯЗАН отработать и сколько раз не смог.
        # Второе число — это ровно та часть глубокого режима, которая не
        # выполнена, и прятать его нельзя.
        self.critic_required = 0
        self.critic_unavailable = 0
        # Ответ пришёл, но контракта не выполнил. Считается отдельно от
        # «не ответил»: причины разные, лечатся по-разному, а смешанный
        # счётчик прячет как раз тот случай, где модель отвечает уверенно
        # и структурно неполно.
        self.critic_invalid = 0

    # ── Обращения к модели ────────────────────────────────────────────────

    def _cached_call(
        self,
        *,
        provider_family: str,
        model: str,
        reasoning_level: str | None,
        prompt: str,
        schema: Mapping[str, Any],
        digest: str,
        role: str,
        system_prompt: str | None = None,
        images: Sequence[str] = (),
    ) -> tuple[dict[str, Any] | None, gateway.CallResult | None, bool]:
        key = cache_module.cache_key(
            evidence_digest=digest,
            model=model,
            reasoning_level=reasoning_level,
            prompt_version=prompts.prompt_versions().get(role, ""),
            schema_version=schemas.SCHEMA_VERSION,
            role=role,
        )
        cached = self.cache.load(key)
        if cached is not None:
            return cached, None, True
        result = self._call(
            provider_family,
            prompt,
            model=model,
            schema=dict(schema),
            reasoning_level=reasoning_level,
            retries=settings.max_retries(),
            cancel=self.cancel,
            system_prompt=system_prompt,
            images=list(images),
            run_id=self.run_id,
        )
        with self._counters:
            self.model_calls += 1
            if not result.ok:
                if result.error_kind == "TIMEOUT":
                    self.timeouts += 1
                else:
                    self.failures += 1
        if not result.ok:
            return None, result, False
        self.cache.store(
            key,
            result.parsed or {},
            {
                "model": model,
                "reasoning_level": reasoning_level,
                "role": role,
                "provider_family": provider_family,
            },
        )
        return result.parsed, result, False

    # ── Разбор одной партии ───────────────────────────────────────────────

    def _resolve_package(
        self,
        package: evidence_module.EvidencePackage,
        budget: _Budget,
    ) -> list[dict[str, Any]]:
        items = {item.item_id: item for item in package.items}
        views = [item.model_view() for item in package.items]
        if not budget.take_batch():
            return [
                _human_entry(item, reason=REASON_BUDGET_EXHAUSTED,
                             detail="исчерпан предел числа партий")
                for item in package.items
            ]
        if budget.out_of_time():
            return [
                _human_entry(item, reason=REASON_BUDGET_EXHAUSTED,
                             detail="исчерпано время сеанса")
                for item in package.items
            ]
        if self.cancel.cancelled:
            return [
                _human_entry(item, reason=REASON_CANCELLED, detail="прогон отменён")
                for item in package.items
            ]

        digest = package.digest()
        model = settings.analyst_model()
        level = settings.analyst_effort()
        payload, call, cache_hit = self._cached_call(
            provider_family=settings.CODEX_SESSION,
            model=model,
            reasoning_level=level,
            prompt=prompts.analyst_prompt(package.model_view()),
            schema=schemas.ANALYST_SCHEMA,
            digest=digest,
            role="analyst",
        )
        if payload is None:
            reason = (
                REASON_MODEL_TIMEOUT
                if call is not None and call.error_kind == "TIMEOUT"
                else REASON_CANCELLED
                if call is not None and call.error_kind == "CANCELLED"
                else REASON_MODEL_FAILED
            )
            detail = call.error if call is not None else "модель не ответила"
            return [
                _human_entry(item, reason=reason, detail=detail)
                for item in package.items
            ]

        verified, batch_problems = verifier.verify_batch(views, payload)
        by_id = {
            str(value.get("item_id") or ""): value
            for value in (payload.get("resolutions") or [])
            if isinstance(value, Mapping)
        }
        audit = _audit(
            provider_family=settings.CODEX_SESSION, model=model,
            reasoning_level=level, role="analyst", evidence_digest=digest,
            output=payload, call=call, cache_hit=cache_hit,
        )

        output: list[dict[str, Any]] = []
        for item in package.items:
            resolution = by_id.get(item.item_id)
            check = verified.get(item.item_id)
            if resolution is None or check is None:
                output.append(_human_entry(
                    item, reason=REASON_MODEL_FAILED,
                    detail="; ".join(batch_problems)[:500] or "элемент без ответа",
                    audit=audit,
                ))
                continue
            output.append(self._finish_item(
                item, resolution, check, audit, budget, digest,
                retried=False,
            ))
        return output

    def _finish_item(
        self,
        item: evidence_module.EvidenceItem,
        resolution: Mapping[str, Any],
        check: verifier.VerifyResult,
        audit: Mapping[str, Any],
        budget: _Budget,
        digest: str,
        *,
        retried: bool,
        vision_used: bool = False,
    ) -> dict[str, Any]:
        if not check.ok:
            if not retried:
                with self._counters:
                    self.verifier_failed_first_pass += 1
            if not retried and settings.max_retries() > 0:
                retry = self._retry_item(item, budget)
                if retry is not None:
                    return retry
            return _human_entry(
                item, reason=REASON_VERIFIER_REJECTED,
                detail="; ".join(check.errors)[:500],
                question=resolution.get("human_question"),
                audit=audit, verifier_result=check.as_dict(),
            )
        if resolution.get("resolution_status") != "AI_RESOLVED":
            visual = self._try_vision(item, resolution, check, budget, retried=retried)
            if visual is not None:
                return visual
            return _human_entry(
                item,
                reason=REASON_MODEL_DECLINED,
                detail=str(resolution.get("human_reason") or ""),
                question=resolution.get("human_question"),
                audit=audit, verifier_result=check.as_dict(),
            )

        critic_result = None
        triggers = critic_triggers(
            item, resolution, check, retried=retried, vision_used=vision_used,
        )
        if self.deep and triggers:
            with self._counters:
                self.critic_required += 1
            critic_result, critic_failure, violations = self._run_critic(
                item, resolution, budget,
            )
            if critic_result is None:
                # Глубокий режим обещал дополнительную проверку и не смог её
                # провести. Принять разбор здесь значило бы выдать «не
                # проверено» за «проверено и возражений нет».
                with self._counters:
                    if critic_failure == REASON_CRITIC_INVALID:
                        self.critic_invalid += 1
                    else:
                        self.critic_unavailable += 1
                entry = _human_entry(
                    item, reason=critic_failure or REASON_CRITIC_UNAVAILABLE,
                    detail="; ".join(violations or triggers)[:500],
                    question=resolution.get("human_question"),
                    audit=audit, verifier_result=check.as_dict(),
                )
                entry["critic_triggers"] = triggers
                entry["critic_contract_violations"] = list(violations)
                return entry
            if critic_result.get("verdict") != "ACCEPT":
                return _human_entry(
                    item, reason=REASON_CRITIC_REJECTED,
                    detail=str(critic_result.get("explanation") or ""),
                    question=resolution.get("human_question"),
                    audit=audit, verifier_result=check.as_dict(),
                    critic_result=critic_result,
                )

        return {
            "review_evidence_id": item.item_id,
            "atom_id": item.atom_id,
            "status": RESOLVED,
            "reason_code": None,
            "reason_detail": "",
            "human_question": None,
            "typed_resolution": _typed_resolution_from(resolution, item),
            "confidence": resolution.get("confidence"),
            "engineering_summary": resolution.get("engineering_summary"),
            "evidence_quotes": [
                dict(value) for value in resolution.get("evidence_quotes") or []
                if isinstance(value, Mapping)
            ],
            # Доказательство названия объекта едет вместе с ответом: чеканка
            # внутренних ссылок обязана проверять то же самое, что проверил
            # верификатор. Без него второй рубеж видит только значения, в
            # которых вид объекта («помещение») не встречается никогда, и
            # проверять ему нечем.
            "object_evidence": object_identity.supporting_evidence(
                resolution.get("object_label"), _grounding_corpus(item),
            ),
            "verifier": check.as_dict(),
            "critic": critic_result,
            "critic_triggers": triggers,
            "vision": None,
            "audit": dict(audit),
        }

    def _retry_item(
        self,
        item: evidence_module.EvidenceItem,
        budget: _Budget,
        *,
        vision_used: bool = False,
    ) -> dict[str, Any] | None:
        """Одиночный повтор на высоком уровне рассуждения.

        Высокий уровень включается ТОЛЬКО после отказа верификатора: платить
        за него на каждом обычном элементе незачем — на подтверждённых листах
        низкий уровень и так проходил проверку.
        """
        if budget.out_of_time() or self.cancel.cancelled:
            return None
        with self._counters:
            self.retries_used += 1
        package = evidence_module.EvidencePackage(items=[item])
        digest = package.digest() + ":retry"
        model = settings.analyst_model()
        level = settings.retry_effort()
        payload, call, cache_hit = self._cached_call(
            provider_family=settings.CODEX_SESSION,
            model=model,
            reasoning_level=level,
            prompt=prompts.analyst_prompt(package.model_view()),
            schema=schemas.ANALYST_SCHEMA,
            digest=digest,
            role="analyst",
        )
        if payload is None:
            return None
        verified, _problems = verifier.verify_batch([item.model_view()], payload)
        resolution = next(
            (
                value for value in payload.get("resolutions") or []
                if isinstance(value, Mapping)
                and str(value.get("item_id") or "") == item.item_id
            ),
            None,
        )
        check = verified.get(item.item_id)
        if resolution is None or check is None:
            return None
        audit = _audit(
            provider_family=settings.CODEX_SESSION, model=model,
            reasoning_level=level, role="analyst", evidence_digest=digest,
            output=payload, call=call, cache_hit=cache_hit,
        )
        return self._finish_item(
            item, resolution, check, audit, budget, digest, retried=True,
            vision_used=vision_used,
        )

    def _try_vision(
        self,
        item: evidence_module.EvidenceItem,
        resolution: Mapping[str, Any],
        check: verifier.VerifyResult,
        budget: _Budget,
        *,
        retried: bool,
    ) -> dict[str, Any] | None:
        """Посмотреть на чертёж — и всё равно вернуть вывод текстовому слою.

        Возвращает готовую запись, если резерв реально что-то изменил, и None,
        если он неприменим: тогда элемент уходит человеку обычным путём.
        """
        if retried or not self.deep or not self.pdf_paths:
            # После пересмотра с картинкой второй заход к ней запрещён: иначе
            # отказ и резерв начнут вызывать друг друга по кругу.
            return None
        if not vision_module.needs_vision(
            resolution=resolution,
            graphic_route=self.graphic_route,
            source=item.source,
        ):
            return None
        if not budget.take_vision() or budget.out_of_time() or self.cancel.cancelled:
            return None

        workdir = vision_module.crop_workdir()
        try:
            try:
                crops = vision_module.render_crops(
                    pdf_paths=self.pdf_paths,
                    locations=item.locations,
                    out_dir=workdir,
                    sheet_pages=item.sheet_pages,
                )
            except Exception:  # noqa: BLE001 — отрисовка не должна ронять прогон
                return None
            if not crops:
                return None
            # Ключ кэша обязан зависеть от САМОГО изображения: страница,
            # координаты кропа, отпечаток картинки и отпечаток исходного PDF.
            # Иначе перерисованный кроп — другой отступ, исправленный документ,
            # другое разрешение — вернёт ответ, данный про другую картинку.
            digest = content_signature({
                "evidence_digest": item.evidence_digest,
                "role": "vision",
                **vision_module.cache_identity(crops),
            })
            payload, call, cache_hit = self._cached_call(
                provider_family=settings.CODEX_SESSION,
                model=settings.vision_model(),
                reasoning_level=settings.vision_effort(),
                prompt=prompts.vision_prompt(
                    item.model_view(), dict(resolution),
                    # Подпись каждой картинки начинается с её адреса: назвать
                    # изображение модель может только тем, что ей показали.
                    captions=[crop.prompt_line() for crop in crops],
                ),
                schema=schemas.VISION_SCHEMA,
                digest=digest,
                role="vision",
                images=[crop.path for crop in crops],
            )
        finally:
            import shutil

            shutil.rmtree(workdir, ignore_errors=True)
        if payload is None:
            return None
        with self._counters:
            self.vision_calls += 1
        verdict = str(payload.get("verdict") or "")
        observations, side_problems = vision_module.observations_by_side(
            payload, crops
        )
        # Штамп доказан для ПАРЫ листов: у STAMP_EXACT ключ обеих сторон
        # совпадает, иначе отношения бы не было.
        stamp = item.stamp_identity or self.stamp_identity_by_side.get("PAIR") or {}
        contradicts_stamp = [
            side for side, text in observations.items()
            if vision_module.contradicts_text_stamp(text, stamp)
        ]
        vision_record = {
            "source": "VISION",
            "verdict": verdict,
            "observed_left": observations.get("LEFT"),
            "observed_right": observations.get("RIGHT"),
            "observation_image_refs": vision_module.observation_image_refs(payload),
            "side_problems": side_problems,
            "contradicts_text_stamp": sorted(contradicts_stamp),
            "confidence": payload.get("confidence"),
            "explanation": str(payload.get("explanation") or ""),
            "crops": [
                {
                    "side": crop.side,
                    "page": crop.page,
                    "vision_image_ref": crop.vision_image_ref,
                    "crop_ref": crop.crop_ref,
                    "whole_sheet": crop.whole_sheet,
                    "bbox": list(crop.bbox) if crop.bbox else None,
                    "digest": crop.digest,
                    "document_digest": crop.document_digest,
                }
                for crop in crops
            ],
            "audit": _audit(
                provider_family=settings.CODEX_SESSION,
                model=settings.vision_model(),
                reasoning_level=settings.vision_effort(), role="vision",
                evidence_digest=digest, output=payload, call=call,
                cache_hit=cache_hit,
            ),
        }
        if contradicts_stamp:
            # Текстовый штамп доказан вектор-слоем и первичен. Расхождение с
            # ним — повод показать инженеру оба доказательства, а не молча
            # заменить прочитанное увиденным.
            entry = _human_entry(
                item, reason=REASON_VISION_CONTRADICTS,
                detail=(
                    "чертёж противоречит доказанному штампу листа: "
                    + ", ".join(sorted(contradicts_stamp))
                ),
                question=resolution.get("human_question"),
                verifier_result=check.as_dict(),
            )
            entry["vision"] = vision_record
            return entry
        if verdict == "CONTRADICTS_TEXT":
            # Чертёж спорит с текстом — это повод показать инженеру оба, а не
            # повод переписать текст картинкой.
            entry = _human_entry(
                item, reason=REASON_VISION_CONTRADICTS,
                detail=vision_record["explanation"],
                question=resolution.get("human_question"),
                verifier_result=check.as_dict(),
            )
            entry["vision"] = vision_record
            return entry
        if side_problems:
            # Модель описала сторону, изображения которой ей не показывали:
            # либо перепутаны стороны, либо описано не то. Ни то, ни другое не
            # является доказательством.
            entry = _human_entry(
                item, reason=REASON_VISION_INSUFFICIENT,
                detail="; ".join(side_problems),
                question=resolution.get("human_question"),
                verifier_result=check.as_dict(),
            )
            entry["vision"] = vision_record
            return entry
        if verdict != "CONFIRMS_TEXT":
            entry = _human_entry(
                item, reason=REASON_VISION_INSUFFICIENT,
                detail=vision_record["explanation"],
                question=resolution.get("human_question"),
                verifier_result=check.as_dict(),
            )
            entry["vision"] = vision_record
            return entry

        if not observations:
            return None
        lines = evidence_module.vision_lines(
            item,
            {
                **payload,
                "observed_left": observations.get("LEFT"),
                "observed_right": observations.get("RIGHT"),
                "model": settings.vision_model(),
            },
            crops=[
                {
                    "side": crop.side,
                    "page": crop.page,
                    "vision_image_ref": crop.vision_image_ref,
                    "crop_ref": crop.crop_ref,
                    "digest": crop.digest,
                    "whole_sheet": crop.whole_sheet,
                }
                for crop in crops
            ],
        )
        enriched = evidence_module.EvidenceItem(**{
            **item.as_dict(),
            "left_context": [*item.left_context, *lines["LEFT"]],
            "right_context": [*item.right_context, *lines["RIGHT"]],
        })
        enriched.evidence_digest = f"{item.evidence_digest}:vision-confirmed"
        final = self._retry_item(enriched, budget, vision_used=True)
        if final is None:
            return None
        final["vision"] = vision_record
        return final

    def _run_critic(
        self,
        item: evidence_module.EvidenceItem,
        resolution: Mapping[str, Any],
        budget: _Budget,
    ) -> tuple[dict[str, Any] | None, str | None, list[str]]:
        """Разбор критика, либо причина, по которой его нет.

        Возвращает (результат, код отказа, нарушения контракта). Ровно одно из
        первых двух заполнено: «критика нет» и «критик не возражает» обязаны
        быть различимы на выходе, иначе неудача превращается в согласие.
        """
        if not budget.take_critic() or budget.out_of_time() or self.cancel.cancelled:
            return None, REASON_CRITIC_UNAVAILABLE, []
        digest = f"{item.evidence_digest}:{content_signature(resolution)}"
        model = settings.critic_model()
        payload, call, cache_hit = self._cached_call(
            provider_family=settings.CLAUDE_SESSION,
            model=model,
            reasoning_level=None,
            prompt=prompts.critic_prompt(item.model_view(), resolution),
            schema=schemas.CRITIC_SCHEMA,
            digest=digest,
            role="critic",
            system_prompt=prompts.CRITIC_SYSTEM_PROMPT,
        )
        if payload is None:
            # Недоступный критик не имеет права ни принять, ни отклонить.
            return None, REASON_CRITIC_UNAVAILABLE, []
        # Контракт проверяется ЦЕЛИКОМ и ДО чтения вердикта. Прочитать
        # «ACCEPT» из ответа, в котором нет обязательных problems и
        # explanation, — значит принять решение по документу, которого нет:
        # непонятно даже, отвечала ли модель на этот вопрос. Полнота проверки
        # берётся из самой схемы, поэтому новое поле схемы становится
        # обязательным здесь автоматически, без правки этого метода.
        violations = response_contract.validate(payload, schemas.CRITIC_SCHEMA)
        if violations:
            return None, REASON_CRITIC_INVALID, violations
        return {
            "verdict": str(payload["verdict"]),
            "problems": [dict(value) for value in payload["problems"]],
            "explanation": str(payload["explanation"]),
            "audit": _audit(
                provider_family=settings.CLAUDE_SESSION, model=model,
                reasoning_level=None, role="critic", evidence_digest=digest,
                output=payload, call=call, cache_hit=cache_hit,
            ),
        }, None, []

    # ── Вход ──────────────────────────────────────────────────────────────

    def resolve(
        self,
        *,
        review_items: Sequence[Mapping[str, Any]],
        preparation: Mapping[str, Any],
        sheet_relations: Mapping[str, Any],
        comparison_groups: Iterable[Mapping[str, Any]],
        generated_at: str | None = None,
    ) -> dict[str, Any]:
        started = time.perf_counter()
        budget = _Budget(
            max_items=settings.max_items(),
            max_batches=settings.max_batches(),
            max_critic_passes=settings.max_critic_passes(),
            max_vision_items=settings.max_vision_items(),
            deadline=time.monotonic() + settings.max_session_seconds(),
        )
        ordered = sorted(
            (dict(item) for item in review_items),
            key=lambda value: str(value.get("review_evidence_id") or ""),
        )
        accepted = ordered[: budget.max_items]
        skipped = ordered[budget.max_items :]
        if skipped:
            budget.exhausted_reasons.add("max_items")

        packages = evidence_module.build_packages(
            review_items=accepted,
            preparation=preparation,
            sheet_relations=sheet_relations,
            comparison_groups=comparison_groups,
            batch_size=settings.batch_size(),
        )
        self._report(
            phase="started", processed=0, total=len(accepted),
            resolved=0, human=0,
        )

        results: list[dict[str, Any]] = []
        processed = 0
        resolved = 0
        with ThreadPoolExecutor(max_workers=settings.concurrency()) as pool:
            for batch in pool.map(
                lambda package: self._resolve_package(package, budget), packages
            ):
                results.extend(batch)
                processed += len(batch)
                resolved += sum(1 for value in batch if value["status"] == RESOLVED)
                self._report(
                    phase="running", processed=processed, total=len(accepted),
                    resolved=resolved, human=processed - resolved,
                )

        skipped_items = {
            str(value.get("review_evidence_id") or ""): value for value in skipped
        }
        for review_id, value in sorted(skipped_items.items()):
            results.append({
                "review_evidence_id": review_id,
                "atom_id": str(value.get("atom_id") or ""),
                "status": HUMAN_REQUIRED,
                "reason_code": REASON_BUDGET_EXHAUSTED,
                "reason_detail": "исчерпан предел числа элементов",
                "human_question": None,
                "typed_resolution": None,
                "confidence": None,
                "engineering_summary": None,
                "evidence_quotes": [],
                "verifier": None,
                "critic": None,
                "vision": None,
                "audit": None,
            })

        results.sort(key=lambda value: str(value["review_evidence_id"]))
        duration_ms = int((time.perf_counter() - started) * 1000)
        self._report(
            phase="completed", processed=len(results), total=len(ordered),
            resolved=resolved, human=len(results) - resolved,
        )
        return self._artifact(
            results, ordered, budget, duration_ms, generated_at=generated_at,
        )

    def _artifact(
        self,
        results: list[dict[str, Any]],
        ordered: list[dict[str, Any]],
        budget: _Budget,
        duration_ms: int,
        *,
        generated_at: str | None,
    ) -> dict[str, Any]:
        resolved = [value for value in results if value["status"] == RESOLVED]
        human = [value for value in results if value["status"] != RESOLVED]
        reasons: dict[str, int] = {}
        for value in human:
            code = str(value.get("reason_code") or "UNKNOWN")
            reasons[code] = reasons.get(code, 0) + 1
        return {
            "kind": KIND,
            "schema_version": SCHEMA_VERSION,
            "layer_version": LAYER_VERSION,
            "version": 1,
            "generated_at": generated_at or utc_now(),
            "mode": self.mode,
            "run_mode": settings.run_mode_label(self.mode),
            "settings": settings.snapshot(self.mode),
            "prompt_versions": prompts.prompt_versions(),
            "verifier_version": verifier.VERIFIER_VERSION,
            "input_signature": content_signature({
                "layer": LAYER_VERSION,
                "items": [
                    str(value.get("review_evidence_id") or "") for value in ordered
                ],
                "settings": settings.snapshot(self.mode),
            }),
            "resolutions": results,
            "diagnostics": {
                "input_items": len(ordered),
                "processed_items": len(results),
                "ai_resolved": len(resolved),
                "human_required": len(human),
                "human_reasons": dict(sorted(reasons.items())),
                "verifier_rejected": reasons.get(REASON_VERIFIER_REJECTED, 0),
                "critic_rejected": reasons.get(REASON_CRITIC_REJECTED, 0),
                "model_failed": reasons.get(REASON_MODEL_FAILED, 0),
                "model_timeout": reasons.get(REASON_MODEL_TIMEOUT, 0),
                "budget_exhausted": reasons.get(REASON_BUDGET_EXHAUSTED, 0),
                "budgets_hit": sorted(budget.exhausted_reasons),
                "batches": budget.batches_started,
                "critic_passes": budget.critic_passes,
                "vision_items": budget.vision_items,
                "vision_calls": self.vision_calls,
                "model_calls": self.model_calls,
                "verifier_failed_first_pass": self.verifier_failed_first_pass,
                "retries_used": self.retries_used,
                "critic_required": self.critic_required,
                "critic_unavailable": self.critic_unavailable,
                "critic_invalid": self.critic_invalid,
                "critic_unavailable_items": reasons.get(
                    REASON_CRITIC_UNAVAILABLE, 0
                ),
                "critic_invalid_items": reasons.get(REASON_CRITIC_INVALID, 0),
                # Глубокий режим считается выполненным частично, если хотя бы
                # одна обязательная проверка критика не состоялась. «Частично»
                # честнее, чем «завершён»: заявленной проверки не было.
                # Ответ, не выполнивший контракт, — тоже несостоявшаяся
                # проверка: разобрать его нечем.
                "mode_completeness": (
                    "PARTIAL"
                    if self.deep and (self.critic_unavailable or self.critic_invalid)
                    else "COMPLETE"
                ),
                "model_failures": self.failures,
                "model_timeouts": self.timeouts,
                "cache": self.cache.statistics(),
                "duration_ms": duration_ms,
                "cancelled": self.cancel.cancelled,
                "uses_model": True,
            },
        }

    def _report(self, **payload: Any) -> None:
        if self.progress is None:
            return
        try:
            self.progress(dict(payload))
        except Exception:
            # Прогресс — это индикация, а не результат: его сбой не должен
            # отменять уже полученные разрешения.
            return


def empty_artifact(
    *, generated_at: str | None = None, mode: str | None = None,
) -> dict[str, Any]:
    """Артефакт слоя без разрешений: он существует, но ничего не утверждает.

    Режим передаётся, когда слой обещал разбор и не смог его дать: артефакт
    упавшего «глубокого» прогона, записанный как «Быстро», — это неверный
    аудитный след, а разбирать по нему потом нечего.
    """
    effective = settings.normalize_mode(mode) if mode else settings.MODE_OFF
    return {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "layer_version": LAYER_VERSION,
        "version": 1,
        "generated_at": generated_at or utc_now(),
        "mode": effective,
        "run_mode": settings.run_mode_label(effective),
        "settings": settings.snapshot(effective),
        "prompt_versions": prompts.prompt_versions(),
        "verifier_version": verifier.VERIFIER_VERSION,
        # Режим входит в идентичность артефакта: пустой результат «глубокой
        # проверки» и пустой результат «Быстро» — это разные результаты, и
        # склеивать их по одной подписи нельзя.
        "input_signature": content_signature({
            "layer": LAYER_VERSION,
            "items": [],
            "settings": settings.snapshot(effective),
        }),
        "resolutions": [],
        "diagnostics": {
            "input_items": 0,
            "processed_items": 0,
            "ai_resolved": 0,
            "human_required": 0,
            "human_reasons": {},
            "critic_required": 0,
            "critic_unavailable": 0,
            "critic_invalid": 0,
            "mode_completeness": "COMPLETE",
            "uses_model": False,
        },
    }


def unavailable_artifact(
    review_items: Sequence[Mapping[str, Any]],
    *,
    runtime: Mapping[str, Any] | None = None,
    mode: str | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Слой не запущен: среда не готова. Все элементы честно уезжают человеку.

    Это не то же самое, что режим OFF. В OFF система ничего не обещала; здесь
    она обещала и не смогла, и каждый элемент обязан получить причину, а не
    молча остаться без разбора.
    """
    problems = [str(value) for value in (runtime or {}).get("problems") or ()]
    detail = "; ".join(problems)[:500] or "среда ИИ-слоя не готова"
    resolutions = [
        {
            "review_evidence_id": str(item.get("review_evidence_id") or ""),
            "atom_id": str(item.get("atom_id") or ""),
            "status": HUMAN_REQUIRED,
            "reason_code": REASON_RUNTIME_UNAVAILABLE,
            "reason_detail": detail,
            "human_question": None,
            "typed_resolution": None,
            "confidence": None,
            "engineering_summary": None,
            "evidence_quotes": [],
            "verifier": None,
            "critic": None,
            "vision": None,
            "audit": None,
        }
        for item in review_items or ()
    ]
    resolutions.sort(key=lambda value: str(value["review_evidence_id"]))
    return {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "layer_version": LAYER_VERSION,
        "version": 1,
        "generated_at": generated_at or utc_now(),
        "mode": settings.normalize_mode(mode) if mode else settings.mode(),
        "run_mode": settings.run_mode_label(mode or settings.mode()),
        "settings": settings.snapshot(mode or settings.mode()),
        "prompt_versions": prompts.prompt_versions(),
        "verifier_version": verifier.VERIFIER_VERSION,
        "runtime": dict(runtime or {}),
        "input_signature": content_signature({
            "layer": LAYER_VERSION,
            "items": [
                str(item.get("review_evidence_id") or "")
                for item in review_items or ()
            ],
            "settings": settings.snapshot(mode or settings.mode()),
            "runtime_ok": False,
        }),
        "resolutions": resolutions,
        "diagnostics": {
            "input_items": len(resolutions),
            "processed_items": len(resolutions),
            "ai_resolved": 0,
            "human_required": len(resolutions),
            "human_reasons": (
                {REASON_RUNTIME_UNAVAILABLE: len(resolutions)} if resolutions else {}
            ),
            "runtime_ready": False,
            "runtime_problems": problems,
            "critic_required": 0,
            "critic_unavailable": 0,
            "critic_invalid": 0,
            "mode_completeness": "PARTIAL",
            "uses_model": False,
        },
    }


def typed_resolutions_by_review_id(
    artifact: Mapping[str, Any] | None,
) -> dict[str, dict[str, Any]]:
    """Только разрешённые элементы: остальное — работа человека, не ИИ."""
    output: dict[str, dict[str, Any]] = {}
    if not isinstance(artifact, Mapping):
        return output
    for value in artifact.get("resolutions") or []:
        if not isinstance(value, Mapping) or value.get("status") != RESOLVED:
            continue
        typed = value.get("typed_resolution")
        if not isinstance(typed, Mapping) or not typed:
            continue
        if typed.get("dimension") in {None, "", UNKNOWN_DIMENSION}:
            continue
        review_id = str(value.get("review_evidence_id") or "")
        if review_id:
            output[review_id] = dict(value)
    return output


__all__ = [
    "AiResolutionLayer",
    "HUMAN_REQUIRED",
    "KIND",
    "LAYER_VERSION",
    "RESOLVED",
    "REASON_BUDGET_EXHAUSTED",
    "REASON_CANCELLED",
    "CRITIC_TRIGGERS",
    "REASON_CRITIC_INVALID",
    "REASON_CRITIC_REJECTED",
    "REASON_CRITIC_UNAVAILABLE",
    "REASON_RUNTIME_UNAVAILABLE",
    "REASON_MODEL_DECLINED",
    "REASON_MODEL_FAILED",
    "REASON_MODEL_TIMEOUT",
    "REASON_VERIFIER_REJECTED",
    "SCHEMA_VERSION",
    "critic_triggers",
    "empty_artifact",
    "unavailable_artifact",
    "typed_resolutions_by_review_id",
]
