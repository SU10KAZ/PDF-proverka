"""Разбор итоговых карточек по четырём корзинам моделью (Claude Opus 5.5 по подписке).

Кнопка «Сформировать отчёт»: если разбора нет — фоновый прогон. Карточки идут
пачками по ``BATCH_SIZE``; в каждом вызове — промпт ``prompts/project_change/it_baskets_ru.md``,
краткий список всех ИТ (для «Связано с» и антидвойного счёта), подробности
карточек пачки, том ПД по страницам и страницы РД, на которые ссылаются
фрагменты пачки. Ответ модели проверяется схемой (корзины и уверенность — только
из легенды) и составом id; сырой ответ пишется ДО проверки. Повторов нет:
непрошедшая пачка остаётся пустой и досчитывается следующим запуском.

Ручной разбор инженера главнее: прогон его не затирает, пока не попросили
пересчитать явно (``force``).
"""
from __future__ import annotations

import json
import os
import threading
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from . import it_analysis
from .report_xlsx import BASKETS, CARD_COLUMNS, CONFIDENCE, ID_RE, it_id

STAGE = "PROJECTCHANGE_IT_BASKETS"
MODEL, REASONING = "claude-opus-5-5", "xhigh"
BATCH_SIZE = 5
PD_CHARS_BUDGET = 400_000
RD_CHARS_BUDGET = 350_000
QUOTE_CHARS = 700
PROMPT_PATH = Path(__file__).resolve().parents[4] / "prompts" / "project_change" / "it_baskets_ru.md"
RUN_SCHEMA = "projectchange-it-model-run/1"
FIELDS = [key for key, _, _ in CARD_COLUMNS]

RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object", "additionalProperties": False, "required": ["cards"],
    "properties": {"cards": {"type": "array", "minItems": 1, "items": {
        "type": "object", "additionalProperties": False, "required": FIELDS,
        "properties": {**{k: {"type": "string"} for k in FIELDS},
                       "id": {"type": "string", "pattern": r"^ИТ-\d{3}(\.\d+)?$"},
                       "basket": {"type": "string", "enum": list(BASKETS)},
                       "confidence": {"type": "string", "enum": list(CONFIDENCE)}},
    }}},
}

_lock = threading.Lock()
_running: dict[str, dict[str, Any]] = {}  # ключ итоговой сводки → {"thread", "cancel"}


class RunRefused(RuntimeError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    os.replace(tmp, path)


def _cut(text: Any, limit: int) -> str:
    text = str(text or "")
    return text if len(text) <= limit else text[:limit] + " …"


# ---------------------------------------------------------------- данные вызова

def item_payload(index: int, c: dict[str, Any], pc_no: dict[str, str]) -> dict[str, Any]:
    """Карточка ИТ в том виде, в каком её видит модель (то же, что лист «Итоговые»)."""
    params = []
    for p in (c.get("parameters") or [])[:80]:
        row = {"name": p.get("name") or "", "old": p.get("old") or "", "new": p.get("new") or "",
               "unit": p.get("unit") or ""}
        if p.get("location"):
            row["location"] = p["location"]
        values = [{"location": v.get("location") or v.get("card_id") or "", "old": v.get("old_value") or "",
                   "new": v.get("new_value") or ""} for v in (p.get("values") or [])[:12]]
        if values:
            row["by_location"] = values
        params.append(row)
    out = {
        "id": it_id(index), "title": c.get("title") or "", "summary": c.get("summary") or "",
        "old_state": c.get("old_state") or "", "new_state": c.get("new_state") or "",
        "basis": c.get("why_one_event") or "", "parameters": params,
        "conflicts": [x.get("statement") or "" for x in c.get("conflicts") or []],
        "evidence": [{"side": e.get("side"), "page": e.get("page"), "source": e.get("source_type"),
                      "document": (e.get("document") or {}).get("label") or "",
                      "explanation": e.get("short_explanation_ru") or "", "quote": _cut(e.get("quote"), QUOTE_CHARS)}
                     for e in (c.get("evidence") or [])[:80]],
    }
    if c.get("kind") == "CONSOLIDATED":
        out["members"] = [{"pc": pc_no.get(str(m.get("id")), ""), "title": m.get("title") or "",
                           "summary": m.get("summary") or ""} for m in c["lineage"]["members"]]
        out["differences"] = c["lineage"].get("distinguishing_check") or ""
        out["manifestations"] = [{"locations": m.get("locations") or [], "note": m.get("note") or ""}
                                 for m in c.get("manifestations") or []]
    return out


def _pages(md_path: str | None) -> dict[int, str]:
    from backend.app.services.common.md_page_index import index_markdown_pages

    if not md_path or not Path(md_path).is_file():
        return {}
    return {p.number: p.body for p in index_markdown_pages(Path(md_path).read_text(encoding="utf-8"))}


def _select(pages: dict[int, str], wanted: Counter, budget: int) -> tuple[dict[str, str], list[int]]:
    """Страницы по убыванию числа ссылок, пока влезают в бюджет; пропущенные — списком."""
    chosen, used, skipped = {}, 0, []
    for page, _ in sorted(wanted.items(), key=lambda kv: (-kv[1], kv[0])):
        text = pages.get(page)
        if text is None:
            continue
        if used + len(text) > budget:
            skipped.append(page)
            continue
        chosen[page], used = text, used + len(text)
    return {str(p): chosen[p] for p in sorted(chosen)}, sorted(skipped)


def batch_payload(view: dict[str, Any], indices: list[int], *, system: str, pd: dict[int, str],
                  rd: dict[int, str]) -> tuple[dict[str, Any], dict[str, Any]]:
    """(данные для модели, справка о том, что ушло) для одной пачки."""
    items = view.get("consolidated") or []
    pc_no = {str(c.get("id")): f"PC-{i + 1:03d}" for i, c in enumerate(view.get("original") or [])}
    old_pages, new_pages = Counter(), Counter()
    for i in indices:
        for e in items[i].get("evidence") or []:
            (old_pages if e.get("side") == "OLD" else new_pages)[int(e.get("page") or 0)] += 1
    if sum(len(t) for t in pd.values()) <= PD_CHARS_BUDGET:
        pd_sel, pd_skipped = {str(p): t for p, t in sorted(pd.items())}, []
    else:  # том ПД не влезает: сперва страницы фрагментов, затем остальные по порядку
        ranked = Counter({p: 1 for p in pd})
        ranked.update({p: 1000 * n for p, n in old_pages.items()})
        pd_sel, pd_skipped = _select(pd, ranked, PD_CHARS_BUDGET)
    rd_sel, rd_skipped = _select(rd, new_pages, RD_CHARS_BUDGET)
    data = {
        "system": system,
        "all_items": [{"id": it_id(i), "title": _cut(c.get("title") or c.get("summary"), 200),
                       "old": _cut(c.get("old_state"), 300), "new": _cut(c.get("new_state"), 300)}
                      for i, c in enumerate(items)],
        "items": [item_payload(i, items[i], pc_no) for i in indices],
        "pd_pages": pd_sel, "rd_pages": rd_sel,
    }
    receipt = {"ids": [it_id(i) for i in indices], "pd_pages": len(pd_sel), "pd_pages_skipped": pd_skipped,
               "rd_pages": [int(p) for p in rd_sel], "rd_pages_skipped": rd_skipped,
               "chars": len(json.dumps(data, ensure_ascii=False))}
    return data, receipt


def batches(n_items: int, size: int | None = None) -> list[list[int]]:
    size = size or BATCH_SIZE
    return [list(range(s, min(s + size, n_items))) for s in range(0, n_items, size)]


def check_cards(response: Any, expected: list[str]) -> tuple[list[dict[str, str]], list[str]]:
    """Строки ответа или список нарушений: каждый id пачки ровно раз, хвосты — только своих родителей."""
    cards = response.get("cards") if isinstance(response, dict) else None
    if not isinstance(cards, list):
        return [], ["нет массива cards"]
    errors, seen, rows = [], set(), []
    for card in cards:
        if not isinstance(card, dict):
            errors.append("карточка должна быть объектом")
            continue
        rid = str(card.get("id") or "")
        m = ID_RE.match(rid)
        parent = f"ИТ-{m.group(1)}" if m else ""
        if not m or parent not in expected:
            errors.append(f"чужой или неверный id {rid!r}")
            continue
        if rid in seen:
            errors.append(f"{rid} повторяется")
            continue
        if card.get("basket") not in BASKETS or card.get("confidence") not in CONFIDENCE:
            errors.append(f"{rid}: корзина или уверенность не из легенды")
            continue
        seen.add(rid)
        rows.append({k: str(card.get(k) or "").strip() for k in FIELDS})
    missing = [e for e in expected if e not in seen]
    if missing:
        errors.append("нет карточек: " + ", ".join(missing))
    return rows, errors


def ordered(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """ИТ по номеру, хвосты сразу за родителем."""
    def key(r):
        m = ID_RE.match(r["id"])
        return int(m.group(1)), int(m.group(2) or 0)
    return sorted(rows, key=key)


# ---------------------------------------------------------------- прогон

def _key(view: dict[str, Any], session_id: str, pair_id: str) -> str:
    return f"{session_id}/{pair_id}/{view['source_run_id']}/{view['consolidator_run_id']}"


def _base(session_id: str, pair_id: str, view: dict[str, Any]) -> Path:
    return it_analysis._dir(session_id, pair_id, view["source_run_id"], view["consolidator_run_id"])


def run_state(session_id: str, pair_id: str, view: dict[str, Any]) -> dict[str, Any]:
    path = _base(session_id, pair_id, view) / "model_run.json"
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"schema": RUN_SCHEMA, "status": "NONE"}
    with _lock:
        alive = _key(view, session_id, pair_id) in _running
    if state.get("status") in ("QUEUED", "RUNNING") and not alive:
        state["status"] = "INTERRUPTED"  # процесс портала перезапускался посреди прогона
    return state


def _default_provider():
    from backend.app.services.project_change_v3.provider import ClaudeOpusProvider

    return ClaudeOpusProvider(model=MODEL, reasoning=REASONING,
                              binary=os.environ.get("PROJECT_COMPARISON_V3_CLAUDE_BIN", "").strip() or None)


def _readiness() -> None:
    from backend.app.services.project_change_v3 import provider_gate
    from backend.app.services.project_change_v3.contracts import MODEL_PROFILES, use_profile

    with use_profile(MODEL_PROFILES["opus55"]):
        gate = provider_gate.check_provider_readiness()
    if not gate.get("available"):
        raise RunRefused("provider_unavailable", "Модель недоступна: " + str(gate.get("reason") or gate))


def start(session_id: str, pair_id: str, view: dict[str, Any], *, document_code: str, object_label: str,
          author: str, force: bool = False, provider_factory: Callable[[], Any] | None = None,
          check_readiness: bool = True, background: bool = True) -> dict[str, Any]:
    """Запустить разбор моделью. Без ``force`` — только недостающие ИТ и никогда поверх ручного разбора."""
    current = it_analysis.load(session_id, pair_id, view)
    n = len(view.get("consolidated") or [])
    todo = list(range(n))
    if current and not force:
        if (current.get("source") or it_analysis.SOURCE_MANUAL) == it_analysis.SOURCE_MANUAL:
            raise RunRefused("manual_exists", "Загружен ручной разбор инженера — модель его не затирает. "
                                              "Пересчитать моделью можно явной кнопкой.")
        failed = set(current.get("failed_ids") or [])
        todo = [i for i in range(n) if it_id(i) in failed]
        if not todo:
            raise RunRefused("already_done", "Разбор моделью уже готов.")
    key = _key(view, session_id, pair_id)
    with _lock:
        if key in _running:
            raise RunRefused("running", "Разбор уже идёт.")
        if check_readiness:
            _readiness()
        cancel = threading.Event()
        _running[key] = {"cancel": cancel, "thread": None}
    run_id = uuid.uuid4().hex
    base = _base(session_id, pair_id, view)
    groups = batches(n) if len(todo) == n else [todo[s:s + BATCH_SIZE] for s in range(0, len(todo), BATCH_SIZE)]
    state = {"schema": RUN_SCHEMA, "run_id": run_id, "status": "QUEUED", "model": MODEL, "reasoning": REASONING,
             "started_at": _now(), "finished_at": None, "started_by": author, "force": force,
             "batches_total": len(groups), "batches_done": 0, "batches_failed": 0,
             "items_total": len(todo), "current": None, "errors": []}
    try:
        _write(base / "model_run.json", state)
    except Exception:
        with _lock:
            _running.pop(key, None)
        raise

    def work():
        try:
            _execute(session_id, pair_id, view, groups, state, base, cancel,
                     provider=(provider_factory or _default_provider)(),
                     system=f"{document_code} · {object_label}", author=author, previous=current, force=force)
        except Exception as exc:  # непредвиденный сбой: состояние не должно «висеть» в RUNNING
            state.update(status="FAILED", finished_at=_now(),
                         errors=state["errors"] + [f"{type(exc).__name__}: {exc}"])
            _write(base / "model_run.json", state)
        finally:
            with _lock:
                _running.pop(key, None)

    if background:
        thread = threading.Thread(target=work, name=f"it-baskets-{run_id[:8]}", daemon=True)
        with _lock:
            _running[key]["thread"] = thread
        try:
            thread.start()
        except Exception:
            with _lock:
                _running.pop(key, None)
            raise
    else:
        work()
    return run_state(session_id, pair_id, view) if not background else state


def cancel(session_id: str, pair_id: str, view: dict[str, Any]) -> bool:
    with _lock:
        entry = _running.get(_key(view, session_id, pair_id))
    if not entry:
        return False
    entry["cancel"].set()
    token = entry.get("token")
    if token is not None:
        token.cancel()
    return True


def _execute(session_id, pair_id, view, groups, state, base, cancel_event, *, provider, system, author,
             previous, force) -> None:
    from backend.app.services.stage_comparison import store

    pair = store._load_pair(session_id, pair_id) or {}
    pd = _pages((pair.get("left") or {}).get("md_path"))
    rd = _pages((pair.get("right") or {}).get("md_path"))
    if not pd or not rd:
        raise RunRefused("markdown_missing", "Нет Markdown ПД или РД для этой пары")
    prompt = PROMPT_PATH.read_text(encoding="utf-8")
    try:
        from backend.app.services.stage_comparison.ai.gateway import CancelToken

        provider.cancel_token = CancelToken()
        with _lock:
            entry = _running.get(_key(view, session_id, pair_id))
            if entry is not None:
                entry["token"] = provider.cancel_token
        if cancel_event.is_set():
            provider.cancel_token.cancel()
    except ImportError:  # тестовый провайдер без gateway
        pass
    run_dir = base / "model_runs" / state["run_id"]
    rows: list[dict[str, str]] = []
    failed: list[str] = []
    state.update(status="RUNNING")
    _write(base / "model_run.json", state)
    for n, indices in enumerate(groups, 1):
        ids = [it_id(i) for i in indices]
        if cancel_event.is_set():
            failed.extend(ids)
            continue
        state["current"] = {"batch": n, "ids": ids, "started_at": _now()}
        _write(base / "model_run.json", state)
        data, receipt = batch_payload(view, indices, system=system, pd=pd, rd=rd)
        call_dir = run_dir / f"{n:02d}"
        _write(call_dir / "REQUEST.json", {**receipt, "stage": STAGE, "model": MODEL, "reasoning": REASONING})
        response, error = None, None
        try:
            response = provider.complete(stage=STAGE, call_id=f"{state['run_id'][:12]}-{n:02d}", pair_id=pair_id,
                                         prompt=prompt, data=data, schema=RESPONSE_SCHEMA, images=[])
        except Exception as exc:  # ProviderError и любые сбои транспорта: пачка не принята
            error = f"{getattr(exc, 'code', type(exc).__name__)}: {exc}"
        # сырой ответ — до проверки (то, за что уже заплачено, не теряется)
        _write(call_dir / "RESPONSE.json", {"response": response if response is not None
                                            else getattr(provider, "last_response", None),
                                            "transport": getattr(provider, "last_transport", None),
                                            "error": error, "finished_at": _now()})
        batch_rows, problems = check_cards(response, ids) if error is None else ([], [error])
        _write(call_dir / "VALIDATION.json", {"ok": not problems, "problems": problems})
        if problems:
            failed.extend(ids)
            state["batches_failed"] += 1
            state["errors"].append(f"пачка {n} ({ids[0]}…{ids[-1]}): " + "; ".join(problems)[:500])
        else:
            rows.extend(batch_rows)
        state["batches_done"] += 1
        _write(base / "model_run.json", state)
    _finish(session_id, pair_id, view, rows, failed, state, base, author=author, previous=previous, force=force,
            cancelled=cancel_event.is_set())


def _finish(session_id, pair_id, view, rows, failed, state, base, *, author, previous, force, cancelled) -> None:
    n = len(view.get("consolidated") or [])
    if previous and not force and (previous.get("source") == it_analysis.SOURCE_MODEL):
        # досчёт: принятые прежде ИТ остаются, недостающие заменяются новыми
        redone = {ID_RE.match(r["id"]).group(1) for r in rows}
        keep = [r for r in previous["rows"] if ID_RE.match(r["id"]).group(1) not in redone
                and r["id"].split(".")[0] not in failed]
        rows = keep + rows
    accepted = {r["id"].split(".")[0] for r in rows if r.get("basket")}
    rows = [r for r in rows if r.get("basket")]
    failed_ids = [it_id(i) for i in range(n) if it_id(i) not in accepted]
    from .report_xlsx import template_rows
    template = {r["id"]: r for r in template_rows(view)}
    rows = ordered(rows + [template[i] for i in failed_ids])  # непосчитанные ИТ — строками шаблона
    warnings = [f"Модель не разобрала: {', '.join(failed_ids)} — повторите «Сформировать отчёт»"] if failed_ids else []
    if rows and len(failed_ids) < n:
        with it_analysis.WRITE_LOCK:
            if it_analysis.load(session_id, pair_id, view) != previous:
                state.update(status="FAILED", finished_at=_now(), current=None,
                             errors=state["errors"] + ["Разбор изменён во время работы модели. "
                                                       "Новый разбор сохранён; ответы модели остались в истории прогона."])
                _write(base / "model_run.json", state)
                return
            it_analysis.store(session_id, pair_id, view, rows, warnings=warnings, source=it_analysis.SOURCE_MODEL,
                              author=author, extra={"model": MODEL, "reasoning": REASONING,
                                                    "model_run_id": state["run_id"], "failed_ids": failed_ids})
    state.update(status="CANCELLED" if cancelled else ("COMPLETED" if not failed_ids else
                                                      ("PARTIAL" if len(failed_ids) < n else "FAILED")),
                 finished_at=_now(), current=None, failed_ids=failed_ids)
    _write(base / "model_run.json", state)
