"""Сводка по объектам для переключателя объектов в шапке портала.

Даёт по КАЖДОМУ объекту два числа — те же, что в строке «Итого» таблицы
«Разделы проекта» на Главной:

  * `not_started`  — «Не запускались на проверку»: у последней версии проекта
    нет ни замечаний, ни оптимизаций (зеркало `isProjectUnanalyzed` во фронте);
  * `no_decisions` — «Нет решений эксперта»: всё, что эксперт не закрыл
    полностью на последней версии, включая ни разу не проверенные проекты
    (`total − expert_checked`, зеркало `sectionStatsTotals`).

Считаются УНИКАЛЬНЫЕ проекты: карточки-версии одного проекта («X» и «X V2») —
один проект, берётся его последняя версия. Логика намеренно повторяет
`app.js` (`latestProjectCards` / `isProjectUnanalyzed` / `isProjectExpertResolved`),
чтобы цифра в переключателе совпадала с цифрой на Главной после переключения.

Стоимость: полный список проектов каждого объекта (~0,2–0,6 с на объект на
тёплой ФС), поэтому результат кешируется на `_TTL` секунд. Фронт дёргает
endpoint лениво — при открытии выпадашки, не при загрузке страницы.
"""
from __future__ import annotations

import re
import time
from typing import Optional

_TTL = 60.0
_cache: dict[str, object] = {"ts": 0.0, "data": None}

# Суффикс версии в имени карточки («X V2», «X_V2») — зеркало
# `_VERSION_SUFFIX_RE` во фронте и `base_project_key` на бэкенде.
_VERSION_SUFFIX_RE = re.compile(r"[\s_\-]+[Vv]\s*(\d+)\s*$")


def _card_version_rank(card: dict) -> int:
    m = _VERSION_SUFFIX_RE.search(str(card.get("project_id") or ""))
    return int(m.group(1)) if m else 0


def _base_key(card: dict) -> str:
    return str(card.get("base_project_key") or card.get("project_id") or "")


def _latest_cards(cards: list[dict]) -> list[dict]:
    """Последняя карточка каждого логического проекта."""
    by_key: dict[str, dict] = {}
    for card in cards:
        key = _base_key(card)
        prev = by_key.get(key)
        if prev is None:
            by_key[key] = card
            continue
        d = _card_version_rank(card) - _card_version_rank(prev)
        if d > 0 or (d == 0 and (card.get("version_no") or 1) > (prev.get("version_no") or 1)):
            by_key[key] = card
    return list(by_key.values())


def _is_unanalyzed(card: dict) -> bool:
    return not ((card.get("findings_count") or 0) > 0
                or (card.get("optimization_count") or 0) > 0)


def _is_expert_resolved(card: dict) -> bool:
    """Эксперт закрыл ПОСЛЕДНЮЮ версию (обе галочки карточки).

    Пустая категория галочку не блокирует: проект без оптимизаций иначе никогда
    не станет проверенным (их статус остаётся пустым)."""
    if _is_unanalyzed(card):
        return False   # проверять нечего
    findings_ok = (not (card.get("findings_count") or 0) > 0
                   or card.get("findings_review_status") == "complete")
    opt_ok = (not (card.get("optimization_count") or 0) > 0
              or card.get("optimization_review_status") == "complete")
    if findings_ok and opt_ok:
        return True
    return card.get("expert_review_status") == "complete"


def _object_cards(object_id: str) -> list[dict]:
    """Карточки проектов объекта в том же виде, что отдаёт GET /api/projects."""
    from backend.app.services.storage.storage_read_facade import production_uses_v2

    if production_uses_v2():
        from backend.app.services.storage import read_canary
        return read_canary.v2_projects_list(object_id=object_id).get("projects") or []
    from backend.app.services.common import project_service
    with project_service.pinned_object(object_id):
        return [p.model_dump() for p in project_service.list_projects()]


def compute_object_stats(object_id: str) -> dict:
    """Сводка одного объекта. Ошибка чтения → нули + `"error": True`."""
    try:
        cards = _object_cards(object_id)
    except Exception as exc:   # fail-soft: битый объект не роняет весь список
        print(f"[object_stats] {object_id}: {exc}")
        return {"total": 0, "not_started": 0, "no_decisions": 0,
                "expert_checked": 0, "error": True}
    latest = _latest_cards(cards)
    not_started = sum(1 for c in latest if _is_unanalyzed(c))
    expert_checked = sum(1 for c in latest if _is_expert_resolved(c))
    return {
        "total": len(latest),
        "not_started": not_started,
        "no_decisions": len(latest) - expert_checked,
        "expert_checked": expert_checked,
    }


def list_object_stats(force: bool = False) -> dict[str, dict]:
    """{object_id: сводка} по всем объектам. TTL-кеш на `_TTL` секунд."""
    now = time.time()
    cached = _cache.get("data")
    if not force and cached is not None and (now - float(_cache["ts"])) < _TTL:
        return cached   # type: ignore[return-value]
    from backend.app.services.common import object_service
    stats = {obj["id"]: compute_object_stats(obj["id"])
             for obj in object_service.list_objects()}
    _cache["ts"] = now
    _cache["data"] = stats
    return stats


def invalidate(_object_id: Optional[str] = None) -> None:
    """Сбросить кеш (например, после добавления/удаления объекта)."""
    _cache["ts"] = 0.0
    _cache["data"] = None
