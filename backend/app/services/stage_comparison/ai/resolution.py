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
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

from ..production_artifacts import content_signature, stable_id, utc_now
from ..unified_change_policy.contract import UNKNOWN_DIMENSION
from . import cache as cache_module
from . import evidence as evidence_module
from . import gateway, prompts, schemas, settings, verifier, vision as vision_module

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


def _typed_resolution_from(resolution: Mapping[str, Any]) -> dict[str, Any]:
    """Ответ модели → тот же типизированный контракт, что заполняет человек.

    Внутренние ссылки здесь НЕ ставятся: их детерминированно чеканит
    review_queue.mint_project_entity_ref из object_label — тем же кодом и тем
    же семейством префиксов, что и для ответа инженера. Иначе ИИ породил бы
    объект-двойник рядом с настоящим.
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
    facet = str(resolution.get("facet_label") or "").strip()
    if facet:
        typed["facet_ref"] = stable_id("facet_ai_", facet.casefold())
    return {key: value for key, value in typed.items() if value is not None}


def _needs_critic(resolution: Mapping[str, Any], *, retried: bool) -> bool:
    """Критик — редкий и дорогой. Он нужен там, где ошибка дорого стоит."""
    if retried:
        return True
    if resolution.get("outcome") == "MATERIAL_CHANGE":
        return True
    if resolution.get("confidence") in {"LOW", "MEDIUM"}:
        return True
    return False


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
    ) -> None:
        self.cache = cache_module.ResponseCache(cache_dir)
        self.cancel = cancel or gateway.CancelToken()
        self.progress = progress
        self._call = call or gateway.call
        # Пути к PDF нужны только визуальному резерву; без них он выключен —
        # рисовать нечего, и это честнее, чем звать модель без картинки.
        self.pdf_paths = dict(pdf_paths or {})
        self.graphic_route = graphic_route
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
        if settings.deep() and _needs_critic(resolution, retried=retried):
            critic_result = self._run_critic(item, resolution, budget)
            if critic_result is not None and critic_result.get("verdict") != "ACCEPT":
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
            "typed_resolution": _typed_resolution_from(resolution),
            "confidence": resolution.get("confidence"),
            "engineering_summary": resolution.get("engineering_summary"),
            "evidence_quotes": [
                dict(value) for value in resolution.get("evidence_quotes") or []
                if isinstance(value, Mapping)
            ],
            "verifier": check.as_dict(),
            "critic": critic_result,
            "vision": None,
            "audit": dict(audit),
        }

    def _retry_item(
        self,
        item: evidence_module.EvidenceItem,
        budget: _Budget,
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
        if retried or not settings.deep() or not self.pdf_paths:
            # После пересмотра с картинкой второй заход к ней запрещён: иначе
            # отказ и резерв начнут вызывать друг друга по кругу.
            return None
        if not vision_module.needs_vision(
            resolution=resolution, graphic_route=self.graphic_route
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
                )
            except Exception:  # noqa: BLE001 — отрисовка не должна ронять прогон
                return None
            if not crops:
                return None
            digest = f"{item.evidence_digest}:vision"
            payload, call, cache_hit = self._cached_call(
                provider_family=settings.CODEX_SESSION,
                model=settings.vision_model(),
                reasoning_level=settings.vision_effort(),
                prompt=prompts.vision_prompt(item.model_view(), dict(resolution)),
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
        vision_record = {
            "verdict": verdict,
            "observed_left": payload.get("observed_left"),
            "observed_right": payload.get("observed_right"),
            "confidence": payload.get("confidence"),
            "explanation": str(payload.get("explanation") or ""),
            "crops": [
                {"side": crop.side, "page": crop.page} for crop in crops
            ],
            "audit": _audit(
                provider_family=settings.CODEX_SESSION,
                model=settings.vision_model(),
                reasoning_level=settings.vision_effort(), role="vision",
                evidence_digest=digest, output=payload, call=call,
                cache_hit=cache_hit,
            ),
        }
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
        if verdict != "CONFIRMS_TEXT":
            entry = _human_entry(
                item, reason=REASON_VISION_INSUFFICIENT,
                detail=vision_record["explanation"],
                question=resolution.get("human_question"),
                verifier_result=check.as_dict(),
            )
            entry["vision"] = vision_record
            return entry

        observations = vision_module.observations_to_context(payload)
        if not any(observations.values()):
            return None
        enriched = evidence_module.EvidenceItem(**{
            **item.as_dict(),
            "left_context": [*item.left_context, *observations["LEFT"]],
            "right_context": [*item.right_context, *observations["RIGHT"]],
        })
        enriched.evidence_digest = f"{item.evidence_digest}:vision-confirmed"
        final = self._retry_item(enriched, budget)
        if final is None:
            return None
        final["vision"] = vision_record
        return final

    def _run_critic(
        self,
        item: evidence_module.EvidenceItem,
        resolution: Mapping[str, Any],
        budget: _Budget,
    ) -> dict[str, Any] | None:
        if not budget.take_critic() or budget.out_of_time() or self.cancel.cancelled:
            return None
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
            return None
        verdict = str(payload.get("verdict") or "")
        if verdict not in schemas.CRITIC_VERDICTS:
            return None
        return {
            "verdict": verdict,
            "problems": [
                dict(value) for value in payload.get("problems") or []
                if isinstance(value, Mapping)
            ],
            "explanation": str(payload.get("explanation") or ""),
            "audit": _audit(
                provider_family=settings.CLAUDE_SESSION, model=model,
                reasoning_level=None, role="critic", evidence_digest=digest,
                output=payload, call=call, cache_hit=cache_hit,
            ),
        }

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
            "mode": settings.mode(),
            "settings": settings.snapshot(),
            "prompt_versions": prompts.prompt_versions(),
            "verifier_version": verifier.VERIFIER_VERSION,
            "input_signature": content_signature({
                "layer": LAYER_VERSION,
                "items": [
                    str(value.get("review_evidence_id") or "") for value in ordered
                ],
                "settings": settings.snapshot(),
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


def empty_artifact(*, generated_at: str | None = None) -> dict[str, Any]:
    """Артефакт выключенного слоя: он существует, но ничего не утверждает."""
    return {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "layer_version": LAYER_VERSION,
        "version": 1,
        "generated_at": generated_at or utc_now(),
        "mode": settings.MODE_OFF,
        "settings": settings.snapshot(),
        "prompt_versions": prompts.prompt_versions(),
        "verifier_version": verifier.VERIFIER_VERSION,
        "input_signature": content_signature({"layer": LAYER_VERSION, "items": []}),
        "resolutions": [],
        "diagnostics": {
            "input_items": 0,
            "processed_items": 0,
            "ai_resolved": 0,
            "human_required": 0,
            "human_reasons": {},
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
    "REASON_CRITIC_REJECTED",
    "REASON_MODEL_DECLINED",
    "REASON_MODEL_FAILED",
    "REASON_MODEL_TIMEOUT",
    "REASON_VERIFIER_REJECTED",
    "SCHEMA_VERSION",
    "empty_artifact",
    "typed_resolutions_by_review_id",
]
