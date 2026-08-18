"""Вычисляемое представление провайдеров и учётных записей для экрана.

Здесь живут четыре расчёта, каждый из которых легко сделать неправильно.

СВЕДЕНИЕ ГРУППЫ (§15). Один аккаунт, два VPS — два снимка ОДНОГО лимита.
Складывать проценты нельзя ни при каких обстоятельствах: 40 % и 40 % — это не
80 %, это одни и те же 40 %, увиденные дважды. `reconcile_group` выбирает ОДИН
снимок по объявленной политике: сначала по надёжности источника, затем по
свежести. Политика названа в ответе (`reconciliation_policy`), чтобы решение
можно было оспорить, а не угадывать.

ДНИ ДО СБРОСА (§13, §25). Две даты — наблюдаемая и ручная — считаются
независимо и показываются обе. Автоматика не «уточняет» человека: расхождение
между ними само по себе новость.

ПРЕДУПРЕЖДЕНИЕ «ЛИМИТ ПРОПАДАЁТ» (§23). Зажигается только если сброс близко И
остаток ДЕЙСТВИТЕЛЬНО известен и выше порога — либо если оператор сам пометил
аккаунт как неиспользованный. По неизвестному остатку не предупреждаем: это
ложный вызов человека.

ПРЕДПРОСМОТР РАНЖИРОВАНИЯ (§26). Чистая функция без побочных эффектов. Она
ничего не назначает и не может назначить: у неё нет доступа к очереди. Её
задача — объяснить, почему воркер оказался бы выше или ниже, и честно сказать
«остаток неизвестен», когда он неизвестен.
"""
from __future__ import annotations

import time
from typing import Any, Optional

from backend.app.services.distributed_workers import provider_accounts, repositories
from backend.app.services.distributed_workers.settings import DistributedWorkersSettings

#: Надёжность источника: меньше — надёжнее. Совпадает с §11 задания.
_SOURCE_RANK = {
    "official_structured_api": 0,
    "official_app_server_rpc": 1,
    "official_machine_readable": 2,
    "official_documented_text": 3,
    "observed_rate_limit_response": 4,
    "local_usage_statistics": 5,
    "operator_manual": 6,
    "unavailable": 7,
}
_CONFIDENCE_RANK = {"high": 0, "medium": 1, "low": 2, "none": 3}

#: Состояния, в которых воркер точно НЕ годится под новую работу провайдера.
_BLOCKING_QUOTA_STATES = frozenset({"limited", "cooldown", "auth_required", "policy_blocked"})

RECONCILIATION_POLICY = "most_trustworthy_then_freshest_single_snapshot"


def _rank(state: dict[str, Any]) -> tuple[int, int, int, float]:
    """Ключ сортировки снимков одной группы.

    Порядок: привязка оператором → надёжность источника → достоверность →
    свежесть.

    Первый уровень появился по итогам адверсариальной проверки. Воркер вправе
    объявить группу сам (штатный механизм — переменная окружения на его
    машине), но это заявление полу-доверенное: машина, впервые вышедшая на
    связь с чужой группой и «более надёжным» источником, иначе становилась бы
    выбранным снимком и показывала оператору выдуманный остаток по чужому
    аккаунту. Привязка, сделанная человеком, старше любого заявления.
    """
    quota = state.get("quota") or {}
    return (
        0 if state.get("account_group_source") == "operator" else 1,
        _SOURCE_RANK.get(str(quota.get("source")), 99),
        _CONFIDENCE_RANK.get(str(quota.get("confidence")), 99),
        # Свежесть — по убыванию, поэтому со знаком минус.
        -float(state.get("observed_at") or 0.0),
    )


def reconcile_group(
    states: list[dict[str, Any]], *, settings: DistributedWorkersSettings,
    now: Optional[float] = None,
) -> dict[str, Any]:
    """Свести снимки одной учётной записи в ОДНО состояние.

    Возвращает выбранный снимок и перечень всех участников — чтобы на экране
    было видно, чей именно снимок принят и какие были отвергнуты.
    """
    moment = float(now if now is not None else time.time())
    usable = [s for s in states if isinstance(s, dict)]
    if not usable:
        return {
            "quota_state": "unknown",
            "remaining_pct": None,
            "quota_source": None,
            "quota_confidence": None,
            "observed_next_reset_at": None,
            "observed_at": None,
            "chosen_worker_id": None,
            "reconciliation_policy": RECONCILIATION_POLICY,
            "contributing_workers": [],
            "stale": True,
            # Поле обязано присутствовать во ВСЕХ ветвях: учётная запись без
            # единого снимка (её только что завёл оператор) — обычное
            # состояние, а не исключение, и читатель не должен об этом знать.
            "aggregated": False,
        }
    ordered = sorted(usable, key=_rank)
    chosen = ordered[0]
    quota = chosen.get("quota") or {}
    observed_at = float(chosen.get("observed_at") or 0.0)
    stale = (moment - observed_at) > max(60, int(settings.quota_stale_sec)) if observed_at else True

    state = str(quota.get("quota_state") or chosen.get("quota_state") or "unknown")
    remaining = quota.get("estimated_remaining_pct")
    if remaining is None:
        remaining = chosen.get("remaining_pct")

    # Порог «мало осталось» применяет ЦЕНТР. Раньше он применялся только на
    # воркере, а карточка при этом писала «Порог: 25 %», которого никто не
    # применил: воркер читает ту же переменную с дефолтом None, центр — с
    # дефолтом 25. Экран обещал одно, данные считались по другому.
    threshold = int(settings.quota_low_threshold_pct)
    if (
        threshold > 0
        and state == "ready"
        and isinstance(remaining, (int, float))
        and remaining <= threshold
    ):
        state = "low"

    if stale and state in ("ready", "low"):
        # Просроченный снимок не выдаётся за действующий. Число сохраняем —
        # оно последнее известное — но состояние честно говорит `stale`.
        state = "stale"
    return {
        "quota_state": state,
        "remaining_pct": remaining,
        "quota_source": quota.get("source"),
        "quota_confidence": quota.get("confidence"),
        "quota_source_stability": quota.get("source_stability"),
        "observed_next_reset_at": quota.get("next_reset_at") or chosen.get("observed_next_reset_at"),
        "observed_at": observed_at or None,
        "chosen_worker_id": chosen.get("worker_id"),
        "reconciliation_policy": RECONCILIATION_POLICY,
        "contributing_workers": [
            {
                "worker_id": s.get("worker_id"),
                "quota_state": (s.get("quota") or {}).get("quota_state") or s.get("quota_state"),
                "remaining_pct": (s.get("quota") or {}).get("estimated_remaining_pct"),
                "source": (s.get("quota") or {}).get("source"),
                "observed_at": s.get("observed_at"),
                "auth_state": s.get("auth_state"),
                "account_group_source": s.get("account_group_source"),
            }
            for s in ordered
        ],
        "stale": stale,
        # Явное отрицание: числа НЕ складывались. Поле нужно затем, чтобы
        # будущая правка, решившая «а давайте суммировать», сломала тест.
        "aggregated": False,
    }


def _days_until(target: Optional[float], *, now: float) -> Optional[float]:
    if target is None:
        return None
    return (float(target) - now) / 86400.0


def account_view(
    account: dict[str, Any],
    states: list[dict[str, Any]],
    *,
    settings: DistributedWorkersSettings,
    now: Optional[float] = None,
) -> dict[str, Any]:
    """Полное представление учётной записи для экрана «Аудит-воркеры»."""
    moment = float(now if now is not None else time.time())
    mine = [
        s for s in states
        if s.get("provider") == account.get("provider")
        and s.get("account_group_id") == account.get("account_group_id")
    ]
    reconciled = reconcile_group(mine, settings=settings, now=moment)

    manual_reset = account.get("manual_next_reset_at")
    observed_reset = reconciled.get("observed_next_reset_at")
    days_manual = _days_until(manual_reset, now=moment)
    days_observed = _days_until(observed_reset, now=moment)

    warning_days = account.get("warning_days") or list(provider_accounts.DEFAULT_WARNING_DAYS)
    triggered = _triggered_thresholds(days_manual, days_observed, warning_days)

    remaining = reconciled.get("remaining_pct")
    threshold = int(settings.quota_low_threshold_pct)
    low_enabled = threshold > 0

    unused = _reset_soon_unused(
        remaining_pct=remaining,
        low_threshold_pct=threshold if low_enabled else None,
        operator_marked_unused=bool(account.get("operator_marked_unused")),
        days_manual=days_manual,
        days_observed=days_observed,
        warning_days=warning_days,
    )

    return {
        "account_id": account.get("account_id"),
        "provider": account.get("provider"),
        "display_name": account.get("display_name"),
        "account_group_id": account.get("account_group_id"),
        "account_kind": account.get("account_kind"),
        "policy_state": account.get("policy_state"),
        "notes": account.get("notes"),
        "attached_worker_ids": [s.get("worker_id") for s in mine],
        "auth_summary": _auth_summary(mine),
        "quota_state": reconciled["quota_state"],
        "observed_remaining_pct": remaining,
        "quota_confidence": reconciled["quota_confidence"],
        "quota_source": reconciled["quota_source"],
        "quota_source_stability": reconciled.get("quota_source_stability"),
        "observed_next_reset_at": observed_reset,
        "manual_next_reset_at": manual_reset,
        "manual_reset_label": account.get("manual_reset_label"),
        "manual_reset_recurrence": account.get("manual_reset_recurrence"),
        "reset_timezone": account.get("reset_timezone"),
        "days_to_manual_reset": days_manual,
        "days_to_observed_reset": days_observed,
        # Расхождение дат — самостоятельная новость, а не повод «поправить»
        # человека автоматикой (§13).
        "reset_dates_disagree": _dates_disagree(manual_reset, observed_reset),
        "warning_days": warning_days,
        "warnings_triggered": triggered,
        "operator_marked_unused": bool(account.get("operator_marked_unused")),
        "reset_soon_unused": unused,
        "low_threshold_pct": threshold if low_enabled else None,
        "low_threshold_enabled": low_enabled,
        "reconciliation": {
            "policy": reconciled["reconciliation_policy"],
            "chosen_worker_id": reconciled["chosen_worker_id"],
            "contributing_workers": reconciled["contributing_workers"],
            "aggregated": reconciled["aggregated"],
            "stale": reconciled["stale"],
        },
        "last_checked_at": reconciled.get("observed_at"),
        "created_at": account.get("created_at"),
        "updated_at": account.get("updated_at"),
    }


def _auth_summary(states: list[dict[str, Any]]) -> dict[str, Any]:
    if not states:
        return {"logged_in": 0, "total": 0, "states": {}}
    counts: dict[str, int] = {}
    for state in states:
        key = str(state.get("auth_state") or "unknown")
        counts[key] = counts.get(key, 0) + 1
    return {
        "logged_in": counts.get("logged_in", 0),
        "total": len(states),
        "states": counts,
    }


def _dates_disagree(manual: Optional[float], observed: Optional[float]) -> bool:
    if manual is None or observed is None:
        return False
    # Час допуска: округления и часовые пояса не должны выглядеть конфликтом.
    return abs(float(manual) - float(observed)) > 3600.0


def _triggered_thresholds(
    days_manual: Optional[float],
    days_observed: Optional[float],
    warning_days: list[int],
) -> list[dict[str, Any]]:
    """Какие пороги предупреждения сработали и по КАКОЙ дате.

    Источник даты указывается явно: «за 3 дня» по ручной дате и по
    наблюдаемой — разные утверждения с разной надёжностью.
    """
    out: list[dict[str, Any]] = []
    for source, days in (("manual", days_manual), ("observed", days_observed)):
        if days is None or days < 0:
            continue
        for threshold in sorted(warning_days):
            if days <= threshold:
                out.append({
                    "source": source,
                    "threshold_days": threshold,
                    "days_left": round(days, 3),
                })
                break
    return out


def _reset_soon_unused(
    *,
    remaining_pct: Optional[float],
    low_threshold_pct: Optional[int],
    operator_marked_unused: bool,
    days_manual: Optional[float],
    days_observed: Optional[float],
    warning_days: list[int],
) -> dict[str, Any]:
    """`reset_soon_unused` — предупреждение «лимит сгорит неиспользованным».

    Условие сознательно узкое (§23). Ложная тревога тут дороже пропуска:
    оператор, которого позвали зря дважды, перестанет реагировать на третий раз.
    """
    horizon = max(warning_days) if warning_days else 0
    candidates = [d for d in (days_manual, days_observed) if d is not None and d >= 0]
    if not candidates:
        return {"active": False, "reason": "дата сброса неизвестна"}
    days_left = min(candidates)
    reset_source = "manual" if days_manual is not None and days_left == days_manual else "observed"
    if days_left > horizon:
        return {"active": False, "reason": "до сброса дальше порога предупреждения"}

    if operator_marked_unused:
        return {
            "active": True,
            "days_left": round(days_left, 3),
            "reset_source": reset_source,
            "remaining_source": "operator_manual",
            "remaining_pct": remaining_pct,
            "confidence": "operator_stated",
            "reason": "оператор отметил учётную запись как почти не использованную",
        }
    if remaining_pct is None:
        # Ровно тот случай, ради которого §23 запрещает угадывать: сброс
        # близко, но использован ли лимит — неизвестно. Молчим.
        return {
            "active": False,
            "reason": (
                "остаток неизвестен: предупреждать не по чему. Отметьте учётную "
                "запись вручную, если знаете, что лимит не израсходован"
            ),
        }
    if low_threshold_pct is None:
        return {
            "active": False,
            "reason": "порог DISTRIBUTED_WORKERS_QUOTA_LOW_THRESHOLD_PCT не задан",
        }
    if remaining_pct <= float(low_threshold_pct):
        return {"active": False, "reason": "остаток мал — сгорать нечему"}
    return {
        "active": True,
        "days_left": round(days_left, 3),
        "reset_source": reset_source,
        "remaining_source": "observed",
        "remaining_pct": remaining_pct,
        "confidence": "observed",
        "reason": (
            f"до сброса {days_left:.1f} дн., а остаток {remaining_pct:.0f}% "
            "выше порога — неиспользованный лимит сгорит"
        ),
    }


def accounts_overview(
    *, settings: DistributedWorkersSettings, now: Optional[float] = None
) -> list[dict[str, Any]]:
    accounts = provider_accounts.list_accounts(settings=settings)
    states = provider_accounts.list_worker_provider_states(settings=settings)
    return [
        account_view(account, states, settings=settings, now=now)
        for account in accounts
    ]


# ─── Предпросмотр ранжирования (§26) ─────────────────────────────────────────
def rank_workers_for_future_job(
    *,
    provider: str,
    settings: DistributedWorkersSettings,
    workers: Optional[list[dict[str, Any]]] = None,
    states: Optional[list[dict[str, Any]]] = None,
    accounts: Optional[list[dict[str, Any]]] = None,
    now: Optional[float] = None,
) -> dict[str, Any]:
    """ЧИСТЫЙ предпросмотр: кто подошёл бы под будущее задание и почему.

    Ничего не назначает. Функция даже не имеет доступа к очереди заданий — это
    структурная гарантия, а не обещание: автодиспетчеризация на этом этапе
    выключена (§26), и включать её здесь нечем.

    Главный будущий приоритет — «ближайший сброс при значимом остатке». Но
    когда остаток неизвестен, ручная дата даёт только ПРЕДУПРЕЖДЕНИЕ оператору
    и НЕ повышает воркера в списке: тратить лимит вслепую нельзя.
    """
    moment = float(now if now is not None else time.time())
    if provider not in provider_accounts.PROVIDERS:
        raise provider_accounts.ProviderAccountError(f"неизвестный провайдер {provider!r}")

    all_workers = workers if workers is not None else repositories.list_workers(settings=settings)
    all_states = (
        states if states is not None
        else provider_accounts.list_worker_provider_states(settings=settings)
    )
    all_accounts = (
        accounts if accounts is not None
        else provider_accounts.list_accounts(settings=settings)
    )
    account_by_group = {
        (a["provider"], a["account_group_id"]): a for a in all_accounts
    }
    state_by_worker = {
        s["worker_id"]: s for s in all_states if s.get("provider") == provider
    }

    rows: list[dict[str, Any]] = []
    for worker in all_workers:
        worker_id = worker.get("worker_id")
        state = state_by_worker.get(worker_id)
        reasons: list[str] = []
        compatible = True

        if worker.get("registration_status") != "approved":
            compatible = False
            reasons.append("воркер не одобрен")
        if worker.get("connection_status") == "offline":
            compatible = False
            reasons.append("нет связи")
        free_slots = int(worker.get("calculated_free_slots") or 0)
        if free_slots <= 0:
            compatible = False
            reasons.append("нет свободного слота")
        if state is None:
            compatible = False
            reasons.append("воркер не сообщал о провайдере")
        else:
            if state.get("installation_status") != "installed":
                compatible = False
                reasons.append("CLI провайдера не установлен")
            if state.get("auth_state") != "logged_in":
                compatible = False
                reasons.append("провайдер не авторизован")
            if state.get("policy_state") == "policy_blocked":
                compatible = False
                reasons.append("провайдер запрещён политикой")
            quota_state = str((state.get("quota") or {}).get("quota_state") or "unknown")
            if quota_state in _BLOCKING_QUOTA_STATES:
                compatible = False
                reasons.append(f"состояние лимита: {quota_state}")

        group = (state or {}).get("account_group_id")
        account = account_by_group.get((provider, group)) if group else None
        view = (
            account_view(account, all_states, settings=settings, now=moment)
            if account is not None else None
        )
        if account is not None and account.get("policy_state") == "policy_blocked":
            compatible = False
            reasons.append("учётная запись запрещена политикой")

        remaining = view.get("observed_remaining_pct") if view else None
        # Протухший остаток — НЕ измеренный. Иначе воркер с трёхдневным
        # снимком поднимался бы наверх как «остаток известен, сброс близко»,
        # то есть лимит расходовался бы по устаревшему числу.
        snapshot_stale = bool((view or {}).get("reconciliation", {}).get("stale"))
        if snapshot_stale:
            remaining = None
        days_to_reset = None
        if view:
            candidates = [
                d for d in (view.get("days_to_observed_reset"), view.get("days_to_manual_reset"))
                if d is not None and d >= 0
            ]
            days_to_reset = min(candidates) if candidates else None

        # Ключ сортировки. Первым — пригодность. Затем: если остаток ИЗВЕСТЕН
        # и значим, приоритет отдаётся ближайшему сбросу (лимит иначе сгорит).
        # Если остаток неизвестен — воркер идёт ПОСЛЕ тех, у кого он известен:
        # расходовать вслепую нельзя даже при близком сбросе.
        remaining_known = remaining is not None
        sort_key = (
            0 if compatible else 1,
            0 if remaining_known else 1,
            days_to_reset if (remaining_known and days_to_reset is not None) else 10_000.0,
            -(remaining or 0.0),
            -free_slots,
        )
        rows.append({
            "worker_id": worker_id,
            "display_name": worker.get("display_name"),
            "compatible": compatible,
            "reasons": reasons,
            "free_slots": free_slots,
            "auth_state": (state or {}).get("auth_state", "unknown"),
            # Состояние берётся из СВЕДЁННОГО представления, а не из сырого
            # снимка: иначе в одной строке уживались бы «ready» из сырья и
            # остаток, посчитанный с учётом устаревания.
            "quota_state": (
                view.get("quota_state") if view
                else ((state or {}).get("quota") or {}).get("quota_state", "unknown")
            ),
            "remaining_pct": remaining,
            "remaining_known": remaining_known,
            "snapshot_stale": snapshot_stale,
            "account_group_id": group,
            "account_id": (account or {}).get("account_id"),
            "days_to_reset": days_to_reset,
            "reset_source": (
                "observed" if (view and view.get("days_to_observed_reset") is not None)
                else ("manual" if (view and view.get("days_to_manual_reset") is not None)
                      else None)
            ),
            "_sort": sort_key,
        })

    rows.sort(key=lambda r: r["_sort"])
    for row in rows:
        row.pop("_sort", None)
    return {
        "provider": provider,
        "generated_at": moment,
        # Явное и проверяемое утверждение: предпросмотр ничего не назначает.
        "auto_dispatch_enabled": False,
        "explanation": (
            "Предпросмотр. Задания не назначаются: автоматическая выдача на "
            "этом этапе выключена. Порядок — сначала пригодные, затем те, у "
            "кого остаток ИЗВЕСТЕН, среди них — с ближайшим сбросом. Воркеры "
            "с неизвестным остатком не поднимаются вверх по ручной дате: "
            "расходовать лимит вслепую нельзя."
        ),
        "workers": rows,
    }
