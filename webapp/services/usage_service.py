"""
Сервис трекинга потребления токенов.

Два режима:
1. UsageTracker — трекинг вызовов через webapp (записи в usage_data.json)
2. GlobalUsageScanner — парсинг ВСЕХ JSONL сессий Claude Code (~/.claude/projects/)
   Даёт полную картину как на дашборде Anthropic.
"""

import json
import os
import time
import threading
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional
from collections import defaultdict

from webapp.models.usage import UsageRecord, UsageCounters, GlobalUsageCounters
from webapp.services.project_service import resolve_project_dir

# Путь к файлу данных
_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
USAGE_DATA_FILE = _DATA_DIR / "usage_data.json"

# Путь к JSONL-файлам сессий Claude Code
CLAUDE_SESSIONS_DIR = Path.home() / ".claude" / "projects"

# Лимиты для Max 20x ($200/мес) — input+output без cache
# Калибруются через POST /api/usage/global/limits
WINDOW_5H_TOKEN_LIMIT = 12_000_000
WEEKLY_TOKEN_LIMIT = 17_000_000

# Автоочистка записей старше N дней
MAX_RECORD_AGE_DAYS = 30

_lock = threading.Lock()


class UsageTracker:
    """Singleton-трекер потребления токенов."""

    def __init__(self):
        self._records: list[dict] = []
        self._session_reset_at: Optional[str] = None
        self._load()

    # ── Persistence ──────────────────────────────────────────

    def _load(self):
        """Загрузить данные из файла."""
        if not USAGE_DATA_FILE.exists():
            self._records = []
            self._session_reset_at = datetime.now().isoformat()
            return
        try:
            with open(USAGE_DATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            self._records = data.get("records", [])
            self._session_reset_at = data.get(
                "session_reset_at", datetime.now().isoformat()
            )
        except (json.JSONDecodeError, OSError):
            self._records = []
            self._session_reset_at = datetime.now().isoformat()

    def _save(self):
        """Сохранить данные в файл с автоочисткой старых записей."""
        # Очистка старых записей
        cutoff = (datetime.now() - timedelta(days=MAX_RECORD_AGE_DAYS)).isoformat()
        self._records = [r for r in self._records if r.get("timestamp", "") >= cutoff]

        _DATA_DIR.mkdir(parents=True, exist_ok=True)
        data = {
            "version": 1,
            "session_reset_at": self._session_reset_at,
            "records": self._records,
        }
        tmp = USAGE_DATA_FILE.with_suffix(".tmp")
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            tmp.replace(USAGE_DATA_FILE)
        except OSError:
            if tmp.exists():
                tmp.unlink(missing_ok=True)

    # ── Record ───────────────────────────────────────────────

    def clear_project_usage(self, project_id: str):
        """Удалить все записи проекта (при старте нового аудита)."""
        with _lock:
            self._records = [
                r for r in self._records
                if r.get("project_id") != project_id
            ]
            self._save()

    def record_usage(self, record: UsageRecord):
        """Добавить запись о потреблении после Claude CLI вызова."""
        with _lock:
            self._records.append(record.model_dump())
            self._save()

    def enrich_from_jsonl(self, session_id: str, record_timestamp: str):
        """Legacy no-op. Токены теперь приходят напрямую из OpenRouter API.

        Метод сохранён для обратной совместимости (может вызываться из старого кода).
        """
        return

    def _find_jsonl(self, session_id: str) -> Optional[Path]:
        """Найти JSONL-файл сессии Claude по session_id."""
        if not CLAUDE_SESSIONS_DIR.exists():
            return None
        # Claude Code хранит JSONL в подпапках проекта
        for project_dir in CLAUDE_SESSIONS_DIR.iterdir():
            if not project_dir.is_dir():
                continue
            # Ищем файл с session_id в имени
            for f in project_dir.glob("*.jsonl"):
                if session_id in f.name:
                    return f
        return None

    # ── Counters ─────────────────────────────────────────────

    def get_counters(self) -> UsageCounters:
        """Вычислить текущие значения трёх счётчиков."""
        with _lock:
            records = list(self._records)
            session_reset = self._session_reset_at or datetime.now().isoformat()

        now = datetime.now()

        # 1. Сессионный (от последнего ручного сброса)
        session_recs = [r for r in records if r.get("timestamp", "") >= session_reset]
        s_input, s_output, s_cost, s_calls = self._sum_records(session_recs)

        # 2. 5-часовое скользящее окно
        cutoff_5h = (now - timedelta(hours=5)).isoformat()
        window_recs = [r for r in records if r.get("timestamp", "") >= cutoff_5h]
        w_input, w_output, w_cost, w_calls = self._sum_records(window_recs)
        w_total = w_input + w_output
        w_pct = min(100.0, round(w_total / WINDOW_5H_TOKEN_LIMIT * 100, 1)) if WINDOW_5H_TOKEN_LIMIT > 0 else 0
        w_remaining = max(0, WINDOW_5H_TOKEN_LIMIT - w_total)
        # Когда сбросится: timestamp самой старой записи в окне + 5ч
        w_resets = None
        if window_recs:
            oldest = min(r.get("timestamp", "") for r in window_recs)
            try:
                oldest_dt = datetime.fromisoformat(oldest)
                w_resets = (oldest_dt + timedelta(hours=5)).isoformat()
            except ValueError:
                pass

        # 3. Недельный (с понедельника 00:00)
        days_since_monday = now.weekday()
        monday = (now - timedelta(days=days_since_monday)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        weekly_recs = [r for r in records if r.get("timestamp", "") >= monday.isoformat()]
        wk_input, wk_output, wk_cost, wk_calls = self._sum_records(weekly_recs)
        wk_total = wk_input + wk_output
        wk_pct = min(100.0, round(wk_total / WEEKLY_TOKEN_LIMIT * 100, 1)) if WEEKLY_TOKEN_LIMIT > 0 else 0
        wk_remaining = max(0, WEEKLY_TOKEN_LIMIT - wk_total)

        return UsageCounters(
            # Сессия
            session_cost_usd=s_cost,
            session_input_tokens=s_input,
            session_output_tokens=s_output,
            session_total_tokens=s_input + s_output,
            session_calls=s_calls,
            session_started_at=session_reset,
            # 5ч окно
            window_5h_cost_usd=w_cost,
            window_5h_input_tokens=w_input,
            window_5h_output_tokens=w_output,
            window_5h_total_tokens=w_total,
            window_5h_calls=w_calls,
            window_5h_limit_tokens=WINDOW_5H_TOKEN_LIMIT,
            window_5h_remaining_tokens=w_remaining,
            window_5h_percent_used=w_pct,
            window_5h_resets_at=w_resets,
            # Неделя
            weekly_cost_usd=wk_cost,
            weekly_input_tokens=wk_input,
            weekly_output_tokens=wk_output,
            weekly_total_tokens=wk_total,
            weekly_calls=wk_calls,
            weekly_limit_tokens=WEEKLY_TOKEN_LIMIT,
            weekly_remaining_tokens=wk_remaining,
            weekly_percent_used=wk_pct,
            weekly_started_at=monday.isoformat(),
        )

    @staticmethod
    def _sum_records(records: list[dict]) -> tuple[int, int, float, int]:
        """Суммирование input/output/cost/calls из списка записей."""
        total_input = sum(r.get("input_tokens", 0) for r in records)
        total_output = sum(r.get("output_tokens", 0) for r in records)
        total_cost = sum(r.get("cost_usd", 0.0) for r in records)
        return total_input, total_output, total_cost, len(records)

    @staticmethod
    def _sum_notional(records: list[dict]) -> float:
        """Суммирование 'теоретической' стоимости CLI (сэкономлено по подписке)."""
        return sum(r.get("cost_usd_notional", 0.0) for r in records)

    @staticmethod
    def _dedup_for_duration(records: list[dict]) -> list[dict]:
        """
        Дедупликация записей по original stage для подсчёта duration.
        При retry одного batch (block_batch_019) записывается несколько records.
        Для duration оставляем только последнюю запись per original stage.
        """
        by_stage: dict[str, dict] = {}
        for r in records:
            orig_stage = r.get("stage", "?")
            existing = by_stage.get(orig_stage)
            if existing is None or r.get("timestamp", "") > existing.get("timestamp", ""):
                by_stage[orig_stage] = r
        return list(by_stage.values())

    @staticmethod
    def _get_pipeline_durations(project_id: str) -> dict[str, int]:
        """
        Прочитать pipeline_log.json и вернуть wall-clock duration (ms) по этапам.
        Ключи приведены к stage-именам usage (block_analysis, findings_merge и т.д.).
        """
        log_path = resolve_project_dir(project_id) / "_output" / "pipeline_log.json"
        if not log_path.exists():
            return {}

        try:
            with open(log_path, encoding="utf-8") as f:
                log = json.load(f)
        except Exception:
            return {}

        # Маппинг ключей pipeline_log → ключи usage stages_summary
        _stage_map = {
            "crop_blocks": "crop_blocks",
            "text_analysis": "text_analysis",
            "block_analysis": "block_analysis",
            "findings_merge": "findings_merge",
            "norm_verify": "norm_verify",
            "norm_fix": "norm_fix",
            "optimization": "optimization",
            "excel": "excel",
        }

        result = {}
        stages = log.get("stages", {})
        for log_key, usage_key in _stage_map.items():
            stage_data = stages.get(log_key, {})
            started = stage_data.get("started_at")
            completed = stage_data.get("completed_at")
            if started and completed:
                try:
                    s = datetime.fromisoformat(started)
                    e = datetime.fromisoformat(completed)
                    dur_ms = int((e - s).total_seconds() * 1000)
                    if dur_ms > 0:
                        result[usage_key] = dur_ms
                except (ValueError, TypeError):
                    pass
        return result

    @staticmethod
    def _get_audit_started_at(project_id: str) -> str | None:
        """
        Вернуть timestamp начала текущего аудита из pipeline_log.json.
        Берём started_at первого этапа (crop_blocks или text_analysis).
        """
        log_path = resolve_project_dir(project_id) / "_output" / "pipeline_log.json"
        if not log_path.exists():
            return None
        try:
            with open(log_path, encoding="utf-8") as f:
                log = json.load(f)
        except Exception:
            return None
        stages = log.get("stages", {})
        # Ищем started_at первого этапа по порядку
        for key in ("crop_blocks", "text_analysis", "block_analysis", "findings_merge"):
            started = (stages.get(key) or {}).get("started_at")
            if started:
                return started
        return None

    # ── Session Reset ────────────────────────────────────────

    def reset_session(self):
        """Сброс сессионного счётчика."""
        with _lock:
            self._session_reset_at = datetime.now().isoformat()
            self._save()

    def clear_all(self):
        """Полная очистка всех записей usage."""
        with _lock:
            self._records = []
            self._session_reset_at = datetime.now().isoformat()
            self._save()

    # ── History ──────────────────────────────────────────────

    def get_recent(self, limit: int = 50) -> list[dict]:
        """Последние N записей."""
        with _lock:
            return list(reversed(self._records[-limit:]))

    # ── Per-project aggregation ─────────────────────────────

    def get_project_usage(self, project_id: str) -> dict:
        """Агрегация usage по проекту: total + по этапам (stages_summary).
        Учитывает только записи текущего прогона (по started_at из pipeline_log)."""
        import re
        audit_started = self._get_audit_started_at(project_id)
        with _lock:
            project_recs = [r for r in self._records if r.get("project_id") == project_id]
        # Фильтр: только записи текущего прогона аудита
        if audit_started and project_recs:
            project_recs = [r for r in project_recs if r.get("timestamp", "") >= audit_started]

        if not project_recs:
            return {
                "project_id": project_id,
                "total_input_tokens": 0,
                "total_output_tokens": 0,
                "total_tokens": 0,
                "total_cost_usd": 0.0,
                "total_calls": 0,
                "stages_summary": {},
            }

        t_in, t_out, t_cost, t_calls = self._sum_records(project_recs)

        # Группировка по этапам: block_batch_*/tile_batch_* → block_analysis
        stages: dict[str, list[dict]] = defaultdict(list)
        _batch_re = re.compile(r"(block_batch|tile_batch)_\d+")
        _norm_re = re.compile(r"norm_verify(_chunk_\d+|_retry_\d+)")
        _findings_critic_re = re.compile(r"findings_critic(_chunk\d+)?")
        _opt_critic_re = re.compile(r"optimization_critic(_retry_\d+)?")
        _opt_corrector_re = re.compile(r"optimization_corrector(_retry_\d+)?")
        _legacy_map = {
            "main_audit": "findings_merge",
            "main_audit_retry": "findings_merge",
            "tile_audit": "block_analysis",
            "triage": "text_analysis",
            "triage_retry": "text_analysis",
            "smart_merge": "findings_merge",
            "smart_merge_retry": "findings_merge",
        }
        # Generic retry suffix: *_retry, *_retry_N → base stage
        _retry_re = re.compile(r"^(.+?)_retry(_\d+)?$")
        for r in project_recs:
            stage = r.get("stage", "unknown")
            if _batch_re.match(stage):
                stage = "block_analysis"
            elif _norm_re.match(stage):
                stage = "norm_verify"
            elif _findings_critic_re.match(stage):
                stage = "findings_critic"
            elif _opt_critic_re.match(stage):
                stage = "optimization_critic"
            elif _opt_corrector_re.match(stage):
                stage = "optimization_corrector"
            elif stage in _legacy_map:
                stage = _legacy_map[stage]
            else:
                # Fallback: strip _retry/_retry_N suffix
                m = _retry_re.match(stage)
                if m:
                    stage = _legacy_map.get(m.group(1), m.group(1))
            stages[stage].append(r)

        # Чистое время из pipeline_log (wall-clock: completed_at - started_at)
        pipeline_durations = self._get_pipeline_durations(project_id)

        stages_summary = {}
        for stage, recs in stages.items():
            s_in, s_out, s_cost, s_calls = self._sum_records(recs)
            # Приоритет: pipeline_log (wall-clock) > дедуплицированный duration_ms
            if stage in pipeline_durations:
                s_duration = pipeline_durations[stage]
            else:
                deduped = self._dedup_for_duration(recs)
                non_retry = [r for r in deduped if not r.get("is_retry", False)]
                dur_recs = non_retry if non_retry else deduped
                s_duration = sum(r.get("duration_ms", 0) for r in dur_recs)
            # Определить модель этапа: последняя (не retry) запись
            stage_model = ""
            non_retry_recs = [r for r in recs if not r.get("is_retry", False)]
            model_recs = non_retry_recs if non_retry_recs else recs
            if model_recs:
                stage_model = model_recs[-1].get("model", "")
            stages_summary[stage] = {
                "input_tokens": s_in,
                "output_tokens": s_out,
                "total_tokens": s_in + s_out,
                "cost_usd": round(s_cost, 4),
                "calls": s_calls,
                "duration_ms": s_duration,
                "model": stage_model,
            }

        # "Сэкономлено" — теоретическая стоимость CLI-вызовов (по подписке = $0)
        notional = self._sum_notional(project_recs)

        return {
            "project_id": project_id,
            "total_input_tokens": t_in,
            "total_output_tokens": t_out,
            "total_tokens": t_in + t_out,
            "total_cost_usd": round(t_cost, 4),
            "notional_cost_usd": round(notional, 4),
            "total_calls": t_calls,
            "stages_summary": stages_summary,
        }

    def get_all_projects_usage(self) -> dict:
        """Краткая сводка usage по всем проектам (с duration по этапам)."""
        import re
        with _lock:
            records = list(self._records)

        _batch_re = re.compile(r"(block_batch|tile_batch)_\d+")
        _norm_re = re.compile(r"norm_verify(_chunk_\d+|_retry_\d+)")
        _findings_critic_re = re.compile(r"findings_critic(_chunk\d+)?")
        _opt_critic_re = re.compile(r"optimization_critic(_retry_\d+)?")
        _opt_corrector_re = re.compile(r"optimization_corrector(_retry_\d+)?")
        _legacy_map = {
            "main_audit": "findings_merge",
            "main_audit_retry": "findings_merge",
            "tile_audit": "block_analysis",
            "triage": "text_analysis",
            "triage_retry": "text_analysis",
            "smart_merge": "findings_merge",
            "smart_merge_retry": "findings_merge",
        }
        _retry_re = re.compile(r"^(.+?)_retry(_\d+)?$")

        projects: dict[str, list[dict]] = defaultdict(list)
        for r in records:
            pid = r.get("project_id")
            if pid:
                projects[pid].append(r)

        result = {}
        for pid, recs in projects.items():
            # Фильтр: только записи текущего прогона аудита
            audit_started = self._get_audit_started_at(pid)
            if audit_started:
                recs = [r for r in recs if r.get("timestamp", "") >= audit_started]
            if not recs:
                continue
            t_in, t_out, t_cost, t_calls = self._sum_records(recs)

            # stages_summary с duration
            stages: dict[str, list[dict]] = defaultdict(list)
            for r in recs:
                stage = r.get("stage", "unknown")
                if _batch_re.match(stage):
                    stage = "block_analysis"
                elif _norm_re.match(stage):
                    stage = "norm_verify"
                elif _findings_critic_re.match(stage):
                    stage = "findings_critic"
                elif _opt_critic_re.match(stage):
                    stage = "optimization_critic"
                elif _opt_corrector_re.match(stage):
                    stage = "optimization_corrector"
                elif stage in _legacy_map:
                    stage = _legacy_map[stage]
                else:
                    m = _retry_re.match(stage)
                    if m:
                        stage = _legacy_map.get(m.group(1), m.group(1))
                stages[stage].append(r)

            # Чистое время из pipeline_log
            pipeline_durations = self._get_pipeline_durations(pid)

            stages_summary = {}
            for stage, srecs in stages.items():
                if stage in pipeline_durations:
                    s_dur = pipeline_durations[stage]
                else:
                    deduped = self._dedup_for_duration(srecs)
                    non_retry = [r for r in deduped if not r.get("is_retry", False)]
                    dur_recs = non_retry if non_retry else deduped
                    s_dur = sum(r.get("duration_ms", 0) for r in dur_recs)
                s_in, s_out, s_cost, s_calls = self._sum_records(srecs)
                stages_summary[stage] = {
                    "total_tokens": s_in + s_out,
                    "duration_ms": s_dur,
                }

            result[pid] = {
                "total_tokens": t_in + t_out,
                "total_cost_usd": round(t_cost, 4),
                "total_calls": t_calls,
                "stages_summary": stages_summary,
            }
        return result


# ══════════════════════════════════════════════════════════════
# GlobalUsageScanner — парсинг ВСЕХ JSONL из ~/.claude/projects/
# ══════════════════════════════════════════════════════════════

def _classify_model(model_id: str) -> str:
    """Определить семейство модели по ID."""
    m = model_id.lower()
    if "sonnet" in m:
        return "sonnet"
    if "opus" in m:
        return "opus"
    if "haiku" in m:
        return "haiku"
    return "other"


def _next_weekly_reset(now: datetime, reset_weekday: int, reset_hour: int) -> datetime:
    """Следующий еженедельный сброс.

    reset_weekday: 0=пн, 1=вт, ..., 3=чт, 6=вс
    reset_hour: час UTC (для MSK 20:00 = UTC 17:00)
    """
    days_ahead = (reset_weekday - now.weekday()) % 7
    reset = now.replace(hour=reset_hour, minute=0, second=0, microsecond=0)
    reset += timedelta(days=days_ahead)
    if reset <= now:
        reset += timedelta(weeks=1)
    return reset


def _prev_weekly_reset(now: datetime, reset_weekday: int, reset_hour: int) -> datetime:
    """Предыдущий еженедельный сброс (начало текущего недельного окна)."""
    nxt = _next_weekly_reset(now, reset_weekday, reset_hour)
    return nxt - timedelta(weeks=1)


def _format_duration(seconds: int) -> str:
    """Форматировать секунды в читаемую строку: '4 ч 31 мин'."""
    if seconds <= 0:
        return "сейчас"
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    parts = []
    if hours > 0:
        parts.append(f"{hours} ч")
    if minutes > 0:
        parts.append(f"{minutes} мин")
    return " ".join(parts) if parts else "< 1 мин"


class GlobalUsageScanner:
    """Парсит JSONL файлы Claude Code для глобальной статистики.

    Кэширует результат на CACHE_TTL секунд для производительности.
    Фильтрует файлы по mtime для минимизации I/O.
    """

    CACHE_TTL = 30  # секунд между полными сканированиями

    def __init__(self):
        self._cache: Optional[GlobalUsageCounters] = None
        self._cache_at: float = 0
        self._lock = threading.Lock()
        # Настройки сброса (по умолчанию пятница 9:00 MSK = 06:00 UTC)
        # Можно менять через set_weekly_reset()
        self.weekly_reset_weekday = 4  # пятница
        self.weekly_reset_hour_utc = 6  # 06:00 UTC = 09:00 MSK
        # Лимиты (output_tokens как основная метрика)
        self.session_5h_limit = WINDOW_5H_TOKEN_LIMIT
        self.weekly_all_limit = WEEKLY_TOKEN_LIMIT

    def get_counters(self) -> GlobalUsageCounters:
        """Получить глобальные счётчики (с кэшированием)."""
        now = time.time()
        if self._cache and (now - self._cache_at) < self.CACHE_TTL:
            return self._cache

        with self._lock:
            # Double-check после получения блокировки
            if self._cache and (time.time() - self._cache_at) < self.CACHE_TTL:
                return self._cache
            result = self._scan()
            self._cache = result
            self._cache_at = time.time()
            return result

    def invalidate_cache(self):
        """Сбросить кэш (для принудительного пересканирования)."""
        self._cache = None
        self._cache_at = 0

    def set_weekly_reset(self, weekday: int, hour_utc: int):
        """Изменить день/время еженедельного сброса."""
        self.weekly_reset_weekday = weekday
        self.weekly_reset_hour_utc = hour_utc
        self.invalidate_cache()

    def set_limits(self, session_5h: int = 0, weekly_all: int = 0):
        """Обновить лимиты."""
        if session_5h > 0:
            self.session_5h_limit = session_5h
        if weekly_all > 0:
            self.weekly_all_limit = weekly_all
        self.invalidate_cache()

    def _scan(self) -> GlobalUsageCounters:
        """Полное сканирование JSONL файлов."""
        t0 = time.time()
        now_utc = datetime.now(timezone.utc)
        now_local = datetime.now()

        # Временные границы
        cutoff_5h = now_utc - timedelta(hours=5)
        weekly_start = _prev_weekly_reset(
            now_utc, self.weekly_reset_weekday, self.weekly_reset_hour_utc
        )
        weekly_next = _next_weekly_reset(
            now_utc, self.weekly_reset_weekday, self.weekly_reset_hour_utc
        )

        # Для фильтрации файлов по mtime — берём самую раннюю границу (weekly)
        oldest_needed = weekly_start.timestamp()

        # Собираем JSONL файлы
        jsonl_files = self._find_jsonl_files(oldest_needed)

        # Аккумуляторы
        # 5h window
        s5h_out = s5h_in = s5h_cache_r = s5h_cache_c = s5h_msgs = 0
        s5h_oldest_ts = None

        # Weekly — all models
        wk_out = wk_in = wk_msgs = 0

        # Weekly — per model family
        wk_model: dict[str, dict] = defaultdict(
            lambda: {"output_tokens": 0, "input_tokens": 0, "cache_read_tokens": 0,
                     "cache_create_tokens": 0, "total_tokens": 0, "messages": 0}
        )

        total_files = len(jsonl_files)
        total_messages = 0

        for fpath in jsonl_files:
            try:
                with open(fpath, "r", encoding="utf-8", errors="replace") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            obj = json.loads(line)
                        except json.JSONDecodeError:
                            continue

                        # Только assistant-сообщения с usage
                        if obj.get("type") != "assistant":
                            continue
                        msg = obj.get("message", {})
                        if not isinstance(msg, dict):
                            continue
                        usage = msg.get("usage")
                        if not usage:
                            continue

                        # Таймстемп
                        ts_str = obj.get("timestamp", "")
                        if not ts_str:
                            continue
                        try:
                            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                            if ts.tzinfo is None:
                                ts = ts.replace(tzinfo=timezone.utc)
                        except (ValueError, TypeError):
                            continue

                        # Извлекаем токены
                        out_tok = usage.get("output_tokens", 0) or 0
                        in_tok = usage.get("input_tokens", 0) or 0
                        cache_r = usage.get("cache_read_input_tokens", 0) or 0
                        cache_c = usage.get("cache_creation_input_tokens", 0) or 0
                        model_id = msg.get("model", "unknown")
                        family = _classify_model(model_id)

                        total_messages += 1

                        # Weekly (все что после weekly_start)
                        if ts >= weekly_start:
                            wk_out += out_tok
                            wk_in += in_tok
                            wk_msgs += 1
                            md = wk_model[family]
                            md["output_tokens"] += out_tok
                            md["input_tokens"] += in_tok
                            md["cache_read_tokens"] += cache_r
                            md["cache_create_tokens"] += cache_c
                            md["total_tokens"] += out_tok + in_tok
                            md["messages"] += 1

                        # 5h window
                        if ts >= cutoff_5h:
                            s5h_out += out_tok
                            s5h_in += in_tok
                            s5h_cache_r += cache_r
                            s5h_cache_c += cache_c
                            s5h_msgs += 1
                            if s5h_oldest_ts is None or ts < s5h_oldest_ts:
                                s5h_oldest_ts = ts

            except OSError:
                continue

        # Расчёт процентов
        s5h_total = s5h_out + s5h_in
        s5h_pct = min(100.0, round(s5h_total / self.session_5h_limit * 100, 1)) if self.session_5h_limit > 0 else 0

        wk_all_total = wk_out + wk_in
        wk_all_pct = min(100.0, round(wk_all_total / self.weekly_all_limit * 100, 1)) if self.weekly_all_limit > 0 else 0

        # Таймер сброса 5h окна
        if s5h_oldest_ts:
            resets_at = s5h_oldest_ts + timedelta(hours=5)
            resets_in = max(0, int((resets_at - now_utc).total_seconds()))
        else:
            resets_in = 0
        resets_in_text = _format_duration(resets_in)

        # Таймер сброса недельного
        weekly_resets_in = max(0, int((weekly_next - now_utc).total_seconds()))
        # Форматирование: "четверг в 20:00"
        days_ru = ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"]
        # Конвертируем UTC час в локальное время для отображения
        weekly_next_local = weekly_next.astimezone(tz=None)
        weekly_resets_text = f"{days_ru[weekly_next_local.weekday()]} в {weekly_next_local.strftime('%H:%M')}"

        # Per-model weekly dict
        wk_model_out = {}
        for fam, data in wk_model.items():
            pct = min(100.0, round(data["total_tokens"] / self.weekly_all_limit * 100, 1)) if self.weekly_all_limit > 0 else 0
            wk_model_out[fam] = {**data, "percent": pct}

        elapsed_ms = int((time.time() - t0) * 1000)

        return GlobalUsageCounters(
            session_5h_output_tokens=s5h_out,
            session_5h_input_tokens=s5h_in,
            session_5h_cache_read_tokens=s5h_cache_r,
            session_5h_cache_create_tokens=s5h_cache_c,
            session_5h_total_tokens=s5h_total,
            session_5h_messages=s5h_msgs,
            session_5h_percent=s5h_pct,
            session_5h_limit=self.session_5h_limit,
            session_5h_resets_in_sec=resets_in,
            session_5h_resets_in_text=resets_in_text,
            weekly_all_output_tokens=wk_out,
            weekly_all_input_tokens=wk_in,
            weekly_all_total_tokens=wk_all_total,
            weekly_all_messages=wk_msgs,
            weekly_all_percent=wk_all_pct,
            weekly_all_limit=self.weekly_all_limit,
            weekly_resets_at=weekly_resets_text,
            weekly_resets_in_sec=weekly_resets_in,
            weekly_by_model=wk_model_out,
            scanned_files=total_files,
            scanned_messages=total_messages,
            scan_duration_ms=elapsed_ms,
            last_scan_at=now_local.isoformat(),
            covers_all_usage=False,
        )

    def _find_jsonl_files(self, oldest_mtime: float) -> list[Path]:
        """Найти все JSONL файлы новее указанного mtime."""
        result = []
        if not CLAUDE_SESSIONS_DIR.exists():
            return result

        for project_dir in CLAUDE_SESSIONS_DIR.iterdir():
            if not project_dir.is_dir():
                continue
            # Основные JSONL (сессии)
            for f in project_dir.glob("*.jsonl"):
                try:
                    if os.path.getmtime(f) >= oldest_mtime:
                        result.append(f)
                except OSError:
                    continue
            # Subagent JSONL
            subagents = project_dir / "subagents" if False else None
            # Пока пропускаем subagents — они дублируют данные основной сессии
        return result


    # ── Rate Limit Check ──────────────────────────────────────

    def check_rate_limit(self, threshold_pct: float = 90.0) -> dict:
        """
        Проверить, можно ли запускать новую Claude CLI сессию.

        Args:
            threshold_pct: порог в процентах (по умолчанию 90%)

        Returns:
            {
                "can_proceed": bool,
                "reason": str,
                "wait_seconds": int,     # сколько ждать до сброса
                "usage_pct": float,      # текущий % использования
                "resets_in_text": str,   # "2 ч 15 мин"
            }
        """
        counters = self.get_counters()

        pct = counters.session_5h_percent
        wait_sec = counters.session_5h_resets_in_sec

        if pct >= 100.0:
            return {
                "can_proceed": False,
                "reason": f"5ч лимит исчерпан ({pct:.0f}%)",
                "wait_seconds": wait_sec,
                "usage_pct": pct,
                "resets_in_text": counters.session_5h_resets_in_text,
            }

        if pct >= threshold_pct:
            return {
                "can_proceed": False,
                "reason": f"5ч лимит близок к исчерпанию ({pct:.0f}%)",
                "wait_seconds": wait_sec,
                "usage_pct": pct,
                "resets_in_text": counters.session_5h_resets_in_text,
            }

        return {
            "can_proceed": True,
            "reason": f"OK ({pct:.0f}% использовано)",
            "wait_seconds": 0,
            "usage_pct": pct,
            "resets_in_text": "",
        }


# Глобальные экземпляры
usage_tracker = UsageTracker()
global_scanner = GlobalUsageScanner()
