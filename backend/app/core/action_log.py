"""
Журнал действий (action log) — сквозная запись всех действий в системе.

Зачем: постоянная летопись «кто, что, когда сделал и чем закончилось», по
которой потом можно разбирать ошибки (какие действия предшествовали сбою,
какие запросы падали, какие этапы конвейера рушились и почему).

Три источника событий:
  * kind="api"      — HTTP-запросы портала (ActionLogMiddleware): метод, путь,
                      логин инженера, статус, длительность. Шумовые поллинговые
                      GET отфильтрованы, но любой ответ >=400 и любое
                      исключение пишутся всегда.
  * kind="pipeline" — переходы этапов конвейера (хук в
                      audit_logger.update_pipeline_log — единая воронка всех
                      stage-статусов).
  * kind="app_log"  — WARNING/ERROR из стандартного logging всех модулей
                      backend (мост install_logging_bridge на root-логгере).
  * kind="system"   — старт/остановка сервера (lifespan).

Формат: суточные append-only JSONL-файлы ACTION_LOG_DIR/actions-YYYY-MM-DD.jsonl,
одна строка = одно событие {"ts", "kind", ...}. Файлы старше
ACTION_LOG_RETENTION_DAYS удаляются при первом событии нового дня.

Всё fail-soft: ни одна ошибка журнала не должна ломать основной поток.
Чтение — read_events()/stats() (используются /api/action-log и
scripts/analyze_action_log.py).
"""
from __future__ import annotations

import json
import logging
import re
import sys
import threading
import time
import traceback
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path

from backend.app.core import config

_LOCK = threading.Lock()
# (директория, день) последней записи — для запуска ретеншн-чистки на смене дня.
_LAST_WRITE_KEY: tuple[str, str] | None = None
# Однократное предупреждение о невозможности писать журнал (не спамить stderr).
_WRITE_FAILED_WARNED = False
# Байт записано в текущий (dir, day) — против заполнения диска штормом событий
# (инициализируется из stat() файла при смене ключа).
_DAY_BYTES = 0
_DAY_CAP_MARKED = False


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


def _today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _log_dir() -> Path:
    # Через атрибут модуля (не from-import), чтобы monkeypatch в тестах работал.
    return Path(config.ACTION_LOG_DIR)


def _cleanup_old_files(log_dir: Path) -> None:
    """Удалить суточные файлы старше ACTION_LOG_RETENTION_DAYS дней. Fail-soft."""
    try:
        retention = int(getattr(config, "ACTION_LOG_RETENTION_DAYS", 180))
        if retention <= 0:
            return
        cutoff = (datetime.now() - timedelta(days=retention)).strftime("%Y-%m-%d")
        for path in log_dir.glob("actions-*.jsonl"):
            # Дата из имени; лексикографическое сравнение == хронологическое.
            day = path.name[len("actions-"):-len(".jsonl")]
            if day < cutoff:
                try:
                    path.unlink()
                except OSError:
                    pass
    except Exception:
        pass


def log_event(kind: str, **fields) -> None:
    """Записать одно событие в журнал. Никогда не бросает исключений.

    None-значения в fields отбрасываются, чтобы записи оставались компактными.
    """
    global _LAST_WRITE_KEY, _WRITE_FAILED_WARNED, _DAY_BYTES, _DAY_CAP_MARKED
    try:
        if not getattr(config, "ACTION_LOG_ENABLED", True):
            return
        record = {"ts": _now_iso(), "kind": kind}
        for key, value in fields.items():
            if value is not None:
                record[key] = value
        data = json.dumps(record, ensure_ascii=False, default=str) + "\n"

        log_dir = _log_dir()
        day = _today_str()
        with _LOCK:
            write_key = (str(log_dir), day)
            file_path = log_dir / f"actions-{day}.jsonl"
            if _LAST_WRITE_KEY != write_key:
                log_dir.mkdir(parents=True, exist_ok=True)
                _cleanup_old_files(log_dir)
                _LAST_WRITE_KEY = write_key
                _DAY_CAP_MARKED = False
                try:
                    _DAY_BYTES = file_path.stat().st_size
                except OSError:
                    _DAY_BYTES = 0
            # Потолок суточного объёма: защита диска от штормов событий.
            max_bytes = int(getattr(config, "ACTION_LOG_MAX_DAY_BYTES", 0) or 0)
            encoded = data.encode("utf-8")
            if max_bytes > 0 and _DAY_BYTES + len(encoded) > max_bytes:
                if not _DAY_CAP_MARKED:
                    _DAY_CAP_MARKED = True
                    marker = json.dumps(
                        {"ts": _now_iso(), "kind": "system",
                         "event": "day_cap_reached", "max_bytes": max_bytes},
                        ensure_ascii=False,
                    ) + "\n"
                    with open(file_path, "a", encoding="utf-8") as f:
                        f.write(marker)
                    _DAY_BYTES += len(marker.encode("utf-8"))
                return
            with open(file_path, "a", encoding="utf-8") as f:
                f.write(data)
            _DAY_BYTES += len(encoded)
        # Успешная запись — новая полоса отказов снова даст предупреждение.
        _WRITE_FAILED_WARNED = False
    except Exception as e:
        # Самолечение: сбросить кэш-ключ, чтобы следующая запись заново сделала
        # mkdir (директорию могли удалить при чистке диска посреди дня).
        _LAST_WRITE_KEY = None
        if not _WRITE_FAILED_WARNED:
            _WRITE_FAILED_WARNED = True
            try:
                print(f"[action_log] запись журнала не удалась: {e}", file=sys.stderr)
            except Exception:
                pass


# ─── Шум-фильтр HTTP (поллинговые GET фронта) ────────────────────────────────
# Применяется ТОЛЬКО к успешным GET (<400): мутирующие запросы и любые ошибки
# пишутся всегда. Пути с regex-паттернами ниже фронт опрашивает раз в секунды —
# без фильтра журнал превращается в шум и распухает.
_NOISE_PATTERNS = [
    r"^/static/",
    r"^/favicon\.ico$",
    r"^/api/info$",
    r"^/api/auth/me$",
    r"/status$",             # /api/audit/batch/status, {pid}/status, pause/status и т.п.
    r"/live-status$",
    r"/resume-info$",
    r"/health$",
    r"^/api/usage/",         # виджеты расхода токенов опрашиваются постоянно
    r"^/api/audit/prepare-data/queue$",
    r"^/api/audit/.+/log$",  # live-лог аудита тянется в цикле
    r"^/api/document/.+/page/",              # рендер страниц при листании PDF
    r"^/api/tiles/.+/blocks/(image|region-image)/",
    r"-jobs/[^/]+$",         # поллинг статуса job'ов stage-comparison
    r"/pipeline-qwen-opus/[^/]+$",  # GET-статус qwen-opus job (POST не фильтруется)
    r"/auto-match/[^/]+$",
    r"/auto-match-last$",
    r"/run-active$",
    r"/run-status/",
    r"/comparison-statuses$",
    r"/pairs/[^/]+/(page-image|block-image)$",  # картинки пар stage-comparison
]

_NOISE_RE: re.Pattern | None = None
_NOISE_RE_KEY: tuple | None = None


def _get_noise_re() -> re.Pattern:
    """Скомпилированный шум-фильтр; пересобирается при смене NOISE_EXTRA (тесты)."""
    global _NOISE_RE, _NOISE_RE_KEY
    extra = tuple(getattr(config, "ACTION_LOG_NOISE_EXTRA", []) or [])
    if _NOISE_RE is None or _NOISE_RE_KEY != extra:
        patterns = list(_NOISE_PATTERNS)
        for pat in extra:
            try:
                re.compile(pat)
                patterns.append(pat)
            except re.error:
                pass
        _NOISE_RE = re.compile("|".join(f"(?:{p})" for p in patterns))
        _NOISE_RE_KEY = extra
    return _NOISE_RE


def is_noise_path(path: str) -> bool:
    return bool(_get_noise_re().search(path))


# ─── HTTP middleware ─────────────────────────────────────────────────────────
class ActionLogMiddleware:
    """Pure-ASGI middleware: пишет kind="api" событие на каждый HTTP-запрос.

    Pure ASGI (не BaseHTTPMiddleware) — не трогает тело запроса/ответа, поэтому
    безопасен для стриминга (chat/stream), загрузок файлов и не добавляет
    задержки. Событие пишется ПОСЛЕ полного ответа (известны статус и
    длительность). Исключение из downstream логируется и пробрасывается дальше
    (наружный ServerErrorMiddleware Starlette по-прежнему вернёт 500).
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or not (
            getattr(config, "ACTION_LOG_ENABLED", True)
            and getattr(config, "ACTION_LOG_HTTP_ENABLED", True)
        ):
            await self.app(scope, receive, send)
            return

        start = time.monotonic()
        holder = {"status": None}

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                holder["status"] = message.get("status")
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception as exc:
            self._log_request(scope, holder["status"] or 500, start, exc=exc)
            raise
        else:
            self._log_request(scope, holder["status"], start)

    def _log_request(self, scope, status, start, exc: Exception | None = None) -> None:
        try:
            path = scope.get("path", "")
            method = scope.get("method", "")
            if (
                exc is None
                and method in ("GET", "HEAD", "OPTIONS")
                and isinstance(status, int)
                and status < 400
                and is_noise_path(path)
            ):
                return

            actor = self._resolve_actor(scope)
            query = (scope.get("query_string") or b"").decode("utf-8", "replace")[:500]
            route_path = getattr(scope.get("route"), "path", None)
            path_params = scope.get("path_params") or {}
            project_id = path_params.get("project_id") or path_params.get("target_project_id")
            client = scope.get("client")

            fields = {
                "actor": actor,
                "method": method,
                "path": path[:500],
                "route": route_path if route_path and route_path != path else None,
                "query": query or None,
                "project_id": project_id,
                "status": status,
                "dur_ms": round((time.monotonic() - start) * 1000),
                "ip": client[0] if client else None,
            }
            if exc is not None:
                fields["error"] = f"{type(exc).__name__}: {exc}"[:500]
                fields["traceback"] = traceback.format_exc()[:4000]
            log_event("api", **fields)
        except Exception:
            pass

    @staticmethod
    def _resolve_actor(scope) -> str | None:
        """Логин инженера из session-cookie (портал-auth). Fail-soft."""
        try:
            from starlette.requests import Request

            from backend.app.core import portal_auth

            request = Request(scope)
            return portal_auth.request_username(request, portal_auth.get_settings())
        except Exception:
            return None


# ─── Хук конвейера ───────────────────────────────────────────────────────────
def log_pipeline_event(
    project_id: str,
    stage: str,
    status: str,
    message: str = "",
    error: str = "",
    duration_sec: int | None = None,
) -> None:
    """Событие перехода этапа конвейера. Вызывается из audit_logger. Fail-soft."""
    try:
        if not getattr(config, "ACTION_LOG_PIPELINE_ENABLED", True):
            return
        log_event(
            "pipeline",
            project_id=project_id,
            stage=stage,
            status=status,
            message=(message or None) and str(message)[:1000],
            error=(error or None) and str(error)[:2000],
            duration_sec=duration_sec,
        )
    except Exception:
        pass


# ─── Мост стандартного logging ───────────────────────────────────────────────
# Скользящее окно rate-limit для app_log: логгер, заголосивший WARNING в цикле,
# не должен раздувать журнал. Отдельный лок — log_event берёт _LOCK,
# вложенный захват не-реентерабельного _LOCK из emit дал бы deadlock.
_APPLOG_LOCK = threading.Lock()
_APPLOG_WINDOW = {"minute": None, "count": 0, "suppressed": 0}


def _applog_gate() -> tuple[bool, int]:
    """(писать ли текущее событие, сколько подавлено в закрывшемся окне)."""
    limit = int(getattr(config, "ACTION_LOG_APPLOG_MAX_PER_MIN", 0) or 0)
    if limit <= 0:
        return True, 0
    minute = int(time.time() // 60)
    with _APPLOG_LOCK:
        closed_suppressed = 0
        if _APPLOG_WINDOW["minute"] != minute:
            closed_suppressed = _APPLOG_WINDOW["suppressed"]
            _APPLOG_WINDOW.update(minute=minute, count=0, suppressed=0)
        _APPLOG_WINDOW["count"] += 1
        if _APPLOG_WINDOW["count"] > limit:
            _APPLOG_WINDOW["suppressed"] += 1
            return False, closed_suppressed
        return True, closed_suppressed


class _ActionLogHandler(logging.Handler):
    """WARNING+ из любого модуля backend → событие kind="app_log"."""

    _reentry = threading.local()

    def emit(self, record: logging.LogRecord) -> None:
        if getattr(self._reentry, "active", False):
            return
        self._reentry.active = True
        try:
            allowed, closed_suppressed = _applog_gate()
            if closed_suppressed:
                log_event(
                    "app_log",
                    level="WARNING",
                    logger="action_log",
                    message=f"подавлено {closed_suppressed} событий app_log "
                            f"(лимит ACTION_LOG_APPLOG_MAX_PER_MIN)",
                )
            if not allowed:
                return
            exc_text = None
            if record.exc_info:
                exc_text = "".join(traceback.format_exception(*record.exc_info))[:4000]
            log_event(
                "app_log",
                level=record.levelname,
                logger=record.name,
                message=record.getMessage()[:2000],
                exc=exc_text,
            )
        except Exception:
            pass
        finally:
            self._reentry.active = False


# Хендлеры, добавленные мостом, — чтобы uninstall снимал ровно их.
_BRIDGE_HANDLERS: list[logging.Handler] = []


def install_logging_bridge() -> None:
    """Подключить мост root-логгера. Идемпотентно; вызывается на старте app.

    Добавление хендлера к root отключает logging.lastResort (WARNING+ → stderr),
    поэтому рядом ставится явный StreamHandler(stderr, WARNING) — прежнее
    поведение server.err.log сохраняется.
    """
    try:
        if not (
            getattr(config, "ACTION_LOG_ENABLED", True)
            and getattr(config, "ACTION_LOG_APPLOG_ENABLED", True)
        ):
            return
        root = logging.getLogger()
        if any(isinstance(h, _ActionLogHandler) for h in root.handlers):
            return
        handler = _ActionLogHandler(level=logging.WARNING)
        root.addHandler(handler)
        _BRIDGE_HANDLERS.append(handler)
        has_stream = any(
            isinstance(h, logging.StreamHandler) and not isinstance(h, _ActionLogHandler)
            for h in root.handlers
        )
        if not has_stream:
            stderr_handler = logging.StreamHandler(sys.stderr)
            stderr_handler.setLevel(logging.WARNING)
            root.addHandler(stderr_handler)
            _BRIDGE_HANDLERS.append(stderr_handler)
    except Exception:
        pass


def uninstall_logging_bridge() -> None:
    """Снять с root ровно те хендлеры, что добавил мост. Идемпотентно, fail-soft.

    Вызывается на shutdown (lifespan): в проде безвредно, а в тестах
    `with TestClient(app)` мост не переживает выход из контекста.
    """
    try:
        root = logging.getLogger()
        for handler in list(_BRIDGE_HANDLERS):
            root.removeHandler(handler)
            _BRIDGE_HANDLERS.remove(handler)
        # Страховка от хендлеров-сирот из чужих install (напр., другой процесс
        # импорта): _ActionLogHandler на root не должен оставаться никогда.
        for handler in list(root.handlers):
            if isinstance(handler, _ActionLogHandler):
                root.removeHandler(handler)
    except Exception:
        pass


# ─── Чтение журнала ──────────────────────────────────────────────────────────
def _is_error_event(event: dict) -> bool:
    if event.get("error") or event.get("exc") or event.get("traceback"):
        return True
    kind = event.get("kind")
    if kind == "api":
        status = event.get("status")
        return isinstance(status, int) and status >= 400
    if kind == "pipeline":
        return event.get("status") in ("error", "interrupted")
    if kind == "app_log":
        return True  # в журнал попадают только WARNING+
    return False


def read_events(
    date_from: str | None = None,
    date_to: str | None = None,
    kind: str | None = None,
    actor: str | None = None,
    q: str | None = None,
    errors_only: bool = False,
    limit: int = 200,
    offset: int = 0,
) -> dict:
    """Прочитать события журнала, новые → старые.

    date_from/date_to — 'YYYY-MM-DD' включительно (по имени суточного файла).
    q — подстрока (без учёта регистра) по сырой JSONL-строке события.
    """
    limit = max(1, min(int(limit), 2000))
    offset = max(0, min(int(offset), 50_000))
    q_lower = q.lower() if q else None

    log_dir = _log_dir()
    try:
        files = sorted(log_dir.glob("actions-*.jsonl"), reverse=True)
    except OSError:
        files = []

    items: list[dict] = []
    skipped = 0
    days_scanned = 0
    truncated = False

    for file_path in files:
        day = file_path.name[len("actions-"):-len(".jsonl")]
        if date_to and day > date_to:
            continue
        if date_from and day < date_from:
            break  # файлы отсортированы по убыванию — дальше только старее
        days_scanned += 1
        # Потоковый проход + deque: память O(offset+limit), а не O(размер файла).
        # Файл хронологический → deque(maxlen) держит НОВЕЙШИЕ совпадения; всё,
        # что он вытеснил, в ответ попасть не может.
        matched: deque = deque(maxlen=offset + limit)
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    if q_lower and q_lower not in line.lower():
                        continue
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if kind and event.get("kind") != kind:
                        continue
                    if actor and event.get("actor") != actor:
                        continue
                    if errors_only and not _is_error_event(event):
                        continue
                    matched.append(event)
        except OSError:
            continue
        for event in reversed(matched):
            if skipped < offset:
                skipped += 1
                continue
            items.append(event)
            if len(items) >= limit:
                truncated = True
                break
        if truncated:
            break

    return {"items": items, "days_scanned": days_scanned, "truncated": truncated}


def stats(days: int = 7) -> dict:
    """Сводка по последним N КАЛЕНДАРНЫМ дням: объёмы, ошибки, активность."""
    days = max(1, min(int(days), 366))
    cutoff = (datetime.now() - timedelta(days=days - 1)).strftime("%Y-%m-%d")
    log_dir = _log_dir()
    try:
        files = [
            p for p in sorted(log_dir.glob("actions-*.jsonl"), reverse=True)
            if p.name[len("actions-"):-len(".jsonl")] >= cutoff
        ][:days]
    except OSError:
        files = []

    day_rows = []
    totals = {"events": 0, "errors": 0, "by_kind": {}}
    for file_path in files:
        day = file_path.name[len("actions-"):-len(".jsonl")]
        by_kind: dict[str, int] = {}
        actors: dict[str, int] = {}
        paths: dict[str, int] = {}
        pipeline_errors: dict[str, int] = {}
        errors = 0
        total = 0
        try:
            f = open(file_path, "r", encoding="utf-8", errors="replace")
        except OSError:
            continue
        # Построчная итерация: O(1) памяти вместо read_text() всего файла.
        with f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                total += 1
                k = event.get("kind", "?")
                by_kind[k] = by_kind.get(k, 0) + 1
                if _is_error_event(event):
                    errors += 1
                    if k == "pipeline":
                        key = f"{event.get('project_id')}:{event.get('stage')}"
                        pipeline_errors[key] = pipeline_errors.get(key, 0) + 1
                a = event.get("actor")
                if a:
                    actors[a] = actors.get(a, 0) + 1
                if k == "api":
                    p = f"{event.get('method', '')} {event.get('route') or event.get('path', '')}"
                    paths[p] = paths.get(p, 0) + 1
        top = lambda d, n=10: sorted(d.items(), key=lambda kv: -kv[1])[:n]  # noqa: E731
        day_rows.append({
            "day": day,
            "total": total,
            "by_kind": by_kind,
            "errors": errors,
            "actors": dict(top(actors)),
            "top_paths": top(paths),
            "pipeline_errors": dict(top(pipeline_errors)),
        })
        totals["events"] += total
        totals["errors"] += errors
        for k, v in by_kind.items():
            totals["by_kind"][k] = totals["by_kind"].get(k, 0) + v

    return {"days": day_rows, "totals": totals}
