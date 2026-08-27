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

Скорость (жалоба Андрея Ивановича 27.08.2026: цифры появлялись через ~5 с):

  * считаем ДЁШЕВО. Полный `v2_projects_list` строит на каждый документ весь
    `ProjectStatus` — сводку версий, список входных файлов, pipeline summary,
    индекс блоков; на 540 документах это ~1,5–2,7 с тёплой ФС и ~5 с холодной.
    Здесь читаются только три файла на документ (замечания, оптимизации,
    вердикты эксперта) — остальное для двух цифр не нужно;
  * никто не ждёт расчёта. Протухший кеш отдаётся СРАЗУ, а обновление идёт
    фоновым потоком (stale-while-revalidate); синхронно считаем только на
    холодном старте и по `?force=true`. Кеш прогревается на старте backend
    (`warm_async`), поэтому первое открытие списка уже застаёт готовые цифры.
"""
from __future__ import annotations

import re
import threading
import time
from pathlib import Path
from typing import Optional

_TTL = 120.0
_cache: dict[str, object] = {"ts": 0.0, "data": None}
_refresh_lock = threading.Lock()
_refreshing = False

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


def _v2_lean_cards(object_id: str) -> list[dict]:
    """Только поля, нужные для двух цифр, — без построения полного ProjectStatus.

    Берём те же источники и те же helper'ы, что `read_canary._v2_project_status`
    (замечания, `optimization.json → meta`, `04_review/expert_review.json`),
    чтобы цифры не разъехались с карточками на Главной, но не читаем всё
    остальное: сводку версий, входные файлы, pipeline summary, индекс блоков.
    """
    from backend.app.services.storage import read_canary as rc

    a = rc._adapter()
    if not a.is_available():
        raise FileNotFoundError(f"projects_v2 недоступен: {a.objects_root}")
    folder, _name = rc._current_object_folder(a, object_id=object_id)
    if not folder:
        return []          # объекта нет в v2 → пусто, без кросс-объектной свалки
    hidden = rc._v2_load_hidden_set()
    cards: list[dict] = []
    for doc in a.list_documents(object_folder=folder):
        if rc._v2_doc_hidden(doc, hidden):
            continue
        doc_dir = Path(doc["doc_dir"])
        vid = doc.get("current_version") or "v1"
        fdata = a.read_findings(doc_dir, vid) or {}
        fitems = (fdata.get("findings", fdata.get("items", []))
                  if isinstance(fdata, dict) else [])
        fcount = len(fitems) if isinstance(fitems, list) else 0
        ocount, _by_type, _savings = rc._v2_optimization(a, doc_dir, vid)
        expert, freview, oreview = rc._v2_review_statuses(
            a, doc_dir, vid, fcount, ocount)
        cards.append({
            "project_id": doc["document_code"],
            "base_project_key": rc._base_project_key(doc["document_code"]),
            "version_no": rc._vno(vid),
            "findings_count": fcount,
            "optimization_count": ocount,
            "findings_review_status": freview,
            "optimization_review_status": oreview,
            "expert_review_status": expert,
        })
    return cards


def _object_cards(object_id: str) -> list[dict]:
    """Карточки проектов объекта: поля те же, что у GET /api/projects."""
    from backend.app.services.storage.storage_read_facade import production_uses_v2

    if production_uses_v2():
        return _v2_lean_cards(object_id)
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


def _compute_all() -> dict[str, dict]:
    from backend.app.services.common import object_service
    return {obj["id"]: compute_object_stats(obj["id"])
            for obj in object_service.list_objects()}


def _refresh_now() -> dict[str, dict]:
    stats = _compute_all()
    _cache["ts"] = time.time()
    _cache["data"] = stats
    return stats


def _refresh_in_background() -> None:
    """Пересчёт в отдельном потоке. Одновременно — не больше одного."""
    global _refreshing
    with _refresh_lock:
        if _refreshing:
            return
        _refreshing = True

    def _run():
        global _refreshing
        try:
            _refresh_now()
        except Exception as exc:      # фоновое обновление не должно ронять сервис
            print(f"[object_stats] фоновое обновление: {exc}")
        finally:
            with _refresh_lock:
                _refreshing = False

    threading.Thread(target=_run, name="object-stats-refresh", daemon=True).start()


def list_object_stats(force: bool = False) -> dict[str, dict]:
    """{object_id: сводка} по всем объектам.

    Свежий кеш отдаётся как есть; ПРОТУХШИЙ — тоже сразу, а пересчёт уходит в
    фон (stale-while-revalidate): открытие списка объектов не должно ждать
    обхода файлов. Синхронно считаем только когда кеша ещё нет или запрошен
    `force`.
    """
    cached = _cache.get("data")
    if force or cached is None:
        return _refresh_now()
    if (time.time() - float(_cache["ts"])) >= _TTL:
        _refresh_in_background()
    return cached   # type: ignore[return-value]


def warm_async() -> None:
    """Прогреть кеш в фоне (зовётся на старте backend)."""
    _refresh_in_background()


def invalidate(_object_id: Optional[str] = None) -> None:
    """Сбросить кеш (например, после добавления/удаления объекта)."""
    _cache["ts"] = 0.0
    _cache["data"] = None
