"""Локальный кеш использования Claude Code как источник остатка лимита.

Зачем модуль вообще существует
──────────────────────────────
У Claude Code нет команды `claude usage`/`limits`/`quota`, и официальный
машиночитаемый остаток публикуется только скрипту статусной строки — то есть
ценой обращения к модели (см. шапку `claude_adapter`). Опрашивать лимит
запросом запрещено: телеметрия не имеет права тратить подписку.

Но сам CLI по ходу ОБЫЧНОЙ работы складывает последнюю известную утилизацию в
свой конфигурационный файл, в ключ `cachedUsageUtilization`. Прочитать его —
это открыть локальный файл: ноль обращений к провайдеру, ноль токенов, ноль
сетевых запросов. Именно этот и только этот путь реализован здесь.

Чем этот источник НЕ является
─────────────────────────────
Он недокументирован. Ключ может исчезнуть, переехать или сменить смысл в любом
обновлении CLI, и никакого обещания совместимости нам никто не давал. Поэтому:

  * `source_stability = undocumented`, а достоверность — `medium`, а не `high`
    (у Codex `high` заслужено первой стороной и структурным ответом RPC);
  * любое отклонение формы читается как «источника нет», а не как «наверное,
    имелось в виду вот это»;
  * решения планировщика на этих числах не принимаются (см. §6 задания 12J).

Граница безопасности
────────────────────
Конфигурационный файл Claude Code соседствует с учётными данными: в нём живут
идентификаторы аккаунта и кеши, которых центру видеть незачем. Поэтому разбор
устроен как ЯВНЫЙ СПИСОК РАЗРЕШЁННОГО, а не как «уберём лишнее»:

  * наружу выходят только `utilization` и `resets_at` двух известных окон
    плюс `fetchedAtMs` — больше ничего из файла не покидает этот модуль;
  * ни один неизвестный ключ не логируется, не пересылается и не сохраняется;
  * при неожиданной форме возвращается `local_cache_schema_unsupported`, а не
    попытка догадаться;
  * ошибка чтения НИКОГДА не делает провайдера недоступным — она закрывает
    только квоту (§1 и §8 задания).

Смысл чисел
───────────
`utilization` — это ИСПОЛЬЗОВАНО. Оператору нужен остаток, поэтому окно несёт
оба значения: `used_pct` как пришло и `remaining_pct = 100 - used`. Перепутать
их — значит показать «осталось 16 %» там, где осталось 84 %.
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

#: Версия разборщика. Обязательна для нестабильных источников: без неё нельзя
#: понять, каким кодом получено значение, лежащее в истории квот.
PARSER_VERSION = "claude-local-usage-1"

#: Ключ верхнего уровня в конфигурационном файле Claude Code.
_CACHE_KEY = "cachedUsageUtilization"
_UTILIZATION_KEY = "utilization"
_FETCHED_AT_KEY = "fetchedAtMs"

#: Окна, которые мы понимаем. Закрытый список — это и есть allowlist: окно с
#: незнакомым именем не разбирается вовсе, потому что мы не знаем ни его
#: длительности, ни того, к чему оно относится.
WINDOW_FIVE_HOUR = "five_hour"
WINDOW_SEVEN_DAY = "seven_day"
KNOWN_WINDOWS: tuple[str, ...] = (WINDOW_FIVE_HOUR, WINDOW_SEVEN_DAY)

#: Номинальная длительность окна в секундах. Не влияет на числа, но позволяет
#: интерфейсу подписать «5 часов» / «7 дней», не догадываясь по имени.
WINDOW_DURATION_SEC: dict[str, int] = {
    WINDOW_FIVE_HOUR: 5 * 3600,
    WINDOW_SEVEN_DAY: 7 * 24 * 3600,
}

#: Разрешённые поля ВНУТРИ окна. Всё прочее (`limit_dollars`, `used_dollars`,
#: `remaining_dollars`, `scope`, …) не читается: денег на экране лимитов нет,
#: а любое лишнее поле — это лишний путь для утечки.
_WINDOW_ALLOWLIST: tuple[str, ...] = ("utilization", "resets_at")

# ─── Коды причины (§10 задания). Именно коды, а не свободный текст ───────────
REASON_AVAILABLE = "local_cache_available"
REASON_STALE = "local_cache_stale"
REASON_MISSING = "local_cache_missing"
REASON_SCHEMA_UNSUPPORTED = "local_cache_schema_unsupported"
REASON_NO_SOURCE = "no_safe_supported_source"

REASON_CODES: tuple[str, ...] = (
    REASON_AVAILABLE,
    REASON_STALE,
    REASON_MISSING,
    REASON_SCHEMA_UNSUPPORTED,
    REASON_NO_SOURCE,
)

#: Потолок размера конфигурационного файла. У живых установок это десятки
#: килобайт; мегабайты означают, что читать это в память незачем. Отказ здесь
#: безопаснее попытки: квота — не та ценность, ради которой стоит рисковать.
MAX_CONFIG_BYTES = 8 * 1024 * 1024

#: Насколько метка снимка может опережать наши часы и всё ещё считаться меткой,
#: а не мусором. Расхождение часов VPS в пределах пяти минут — норма; сутки
#: вперёд — это либо сбитые часы, либо подстановка, и в обоих случаях снимок
#: нельзя считать свежим.
MAX_CLOCK_SKEW_SEC = 300.0

#: Границы правдоподобия для даты сброса. Дата вне их не отвергает окно
#: целиком — просто у окна не будет `reset_at`: остаток сам по себе полезен.
_RESET_PAST_LIMIT_SEC = 90 * 24 * 3600
_RESET_FUTURE_LIMIT_SEC = 400 * 24 * 3600


@dataclass(frozen=True)
class LocalUsageWindow:
    """Одно окно лимита, уже переведённое из «использовано» в «осталось»."""

    window_id: str
    used_pct: float
    remaining_pct: float
    reset_at: Optional[float] = None
    duration_sec: Optional[int] = None


@dataclass(frozen=True)
class LocalUsageReading:
    """Результат чтения кеша.

    `reason` объясняет исход всегда, даже при успехе: интерфейс показывает не
    только число, но и то, откуда оно взялось. `windows` пуст при любом
    неуспешном исходе — «частично разобрали» здесь не бывает.
    """

    reason: str
    windows: tuple[LocalUsageWindow, ...] = ()
    #: Время, когда CLI получил эти числа (не время нашего чтения!).
    fetched_at: Optional[float] = None
    #: Время нашего чтения. Нужно ровно для одного: показать, что мы не
    #: выдаём момент открытия файла за момент наблюдения квоты.
    read_at: Optional[float] = None
    #: Короткое пояснение для оператора. Никогда не содержит данных из файла.
    detail: str = ""

    @property
    def ok(self) -> bool:
        return bool(self.windows) and self.fetched_at is not None

    @property
    def age_sec(self) -> Optional[float]:
        if self.fetched_at is None or self.read_at is None:
            return None
        return max(0.0, float(self.read_at) - float(self.fetched_at))

    def window(self, window_id: str) -> Optional[LocalUsageWindow]:
        for item in self.windows:
            if item.window_id == window_id:
                return item
        return None

    @property
    def most_constrained(self) -> Optional[LocalUsageWindow]:
        """Окно с наименьшим остатком.

        Пятичасовое окно бывает свободно, когда недельное почти выбрано, и
        «свободно» в этом случае — неправда. Ошибиться в сторону осторожности
        дешевле.
        """
        if not self.windows:
            return None
        return min(self.windows, key=lambda w: w.remaining_pct)


def candidate_paths(
    *, config_dir: Optional[Path] = None, home_dir: Optional[Path] = None
) -> tuple[Path, ...]:
    """Где может лежать конфигурационный файл Claude Code, в порядке проверки.

    Порядок не произволен. Когда задан `CLAUDE_CONFIG_DIR` (а воркер задаёт его
    всегда — см. `ClaudeProviderAdapter.provider_env`), CLI держит конфигурацию
    внутри этого каталога, и именно она относится к тому процессу, чью квоту мы
    показываем. Файл в HOME проверяется вторым: он существует у обычной
    установки без переопределения каталога.
    """
    out: list[Path] = []
    if config_dir is not None:
        out.append(Path(config_dir) / ".claude.json")
    if home_dir is not None:
        out.append(Path(home_dir) / ".claude.json")
    unique: list[Path] = []
    for path in out:
        if path not in unique:
            unique.append(path)
    return tuple(unique)


def read_local_usage(
    *,
    config_dir: Optional[Path] = None,
    home_dir: Optional[Path] = None,
    now: Optional[float] = None,
) -> LocalUsageReading:
    """Прочитать кеш утилизации. Никогда не бросает и никогда не ходит в сеть.

    Единственные операции — `os.stat` и чтение локального файла. Ни одного
    подпроцесса, ни одного запроса к модели: это требование §9 задания, а не
    оптимизация.
    """
    moment = float(now) if now is not None else time.time()
    paths = candidate_paths(config_dir=config_dir, home_dir=home_dir)
    if not paths:
        return LocalUsageReading(
            reason=REASON_MISSING,
            read_at=moment,
            detail="каталог Claude Code не определён",
        )

    seen_file = False
    last_schema_detail = ""
    for path in paths:
        raw = _load_json(path)
        if raw is _MISSING:
            continue
        seen_file = True
        if raw is _UNREADABLE:
            last_schema_detail = "конфигурационный файл Claude Code не читается"
            continue
        if raw is _MALFORMED:
            last_schema_detail = "конфигурационный файл Claude Code не разбирается как JSON"
            continue
        if not isinstance(raw, dict):
            last_schema_detail = "конфигурационный файл Claude Code не является объектом"
            continue

        cache = raw.get(_CACHE_KEY)
        if not isinstance(cache, dict):
            # Нормальное состояние, а не ошибка: CLI ещё не работал под этим
            # пользователем либо ещё не получал ответ с данными о лимите.
            continue

        fetched_at = _fetched_at(cache.get(_FETCHED_AT_KEY), now=moment)
        if fetched_at is None:
            last_schema_detail = (
                "в кеше Claude Code нет пригодной метки времени снимка"
            )
            continue

        windows = tuple(_windows(cache.get(_UTILIZATION_KEY), now=moment))
        if not windows:
            last_schema_detail = "в кеше Claude Code нет разобранных окон лимита"
            continue

        return LocalUsageReading(
            reason=REASON_AVAILABLE,
            windows=windows,
            fetched_at=fetched_at,
            read_at=moment,
            detail="локальный кеш Claude Code",
        )

    if last_schema_detail:
        return LocalUsageReading(
            reason=REASON_SCHEMA_UNSUPPORTED,
            read_at=moment,
            detail=last_schema_detail,
        )
    if seen_file:
        return LocalUsageReading(
            reason=REASON_MISSING,
            read_at=moment,
            detail=(
                "Claude Code ещё не сохранил данные об использовании: они "
                "появляются после обращений к модели этим пользователем"
            ),
        )
    return LocalUsageReading(
        reason=REASON_MISSING,
        read_at=moment,
        detail="конфигурационный файл Claude Code не найден",
    )


# ─── Внутреннее. Ниже ничего из файла наружу не просачивается ───────────────

class _Sentinel:
    __slots__ = ("name",)

    def __init__(self, name: str) -> None:
        self.name = name


_MISSING = _Sentinel("missing")
_UNREADABLE = _Sentinel("unreadable")
_MALFORMED = _Sentinel("malformed")


def _load_json(path: Path) -> Any:
    try:
        stat = os.stat(path)
    except (OSError, ValueError):
        return _MISSING
    if stat.st_size > MAX_CONFIG_BYTES:
        return _UNREADABLE
    try:
        with open(path, "r", encoding="utf-8") as handle:
            text = handle.read(MAX_CONFIG_BYTES + 1)
    except (OSError, ValueError, UnicodeDecodeError):
        return _UNREADABLE
    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError, RecursionError):
        return _MALFORMED


def _fetched_at(raw: Any, *, now: float) -> Optional[float]:
    """Метка снимка в секундах. Не подменяется временем чтения (§7 задания)."""
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        return None
    value = float(raw)
    if value != value or value <= 0:                        # NaN и мусор
        return None
    seconds = value / 1000.0
    if seconds > now + MAX_CLOCK_SKEW_SEC:
        # Метка из будущего сделала бы снимок вечно свежим.
        return None
    if seconds < now - 365 * 24 * 3600:
        return None
    return seconds


def _windows(raw: Any, *, now: float) -> Iterable[LocalUsageWindow]:
    if not isinstance(raw, dict):
        return []
    out: list[LocalUsageWindow] = []
    for window_id in KNOWN_WINDOWS:
        item = raw.get(window_id)
        if not isinstance(item, dict):
            continue
        used = _percent(item.get(_WINDOW_ALLOWLIST[0]))
        if used is None:
            continue
        out.append(
            LocalUsageWindow(
                window_id=window_id,
                used_pct=used,
                # ЕДИНСТВЕННОЕ место, где «использовано» превращается в
                # «осталось». Зажим не косметика: 100.4 % от округления на
                # стороне провайдера не должен стать остатком −0.4 %.
                remaining_pct=max(0.0, min(100.0, round(100.0 - used, 4))),
                reset_at=_reset_at(item.get(_WINDOW_ALLOWLIST[1]), now=now),
                duration_sec=WINDOW_DURATION_SEC.get(window_id),
            )
        )
    return out


def _percent(raw: Any) -> Optional[float]:
    # `bool` — подкласс `int`: без этой ветки `utilization: true` стало бы
    # «использован 1 %», то есть остатком 99 % из ничего.
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        return None
    value = float(raw)
    if value != value:                                      # NaN
        return None
    if value < -1.0 or value > 101.0:
        return None
    return max(0.0, min(100.0, value))


def _reset_at(raw: Any, *, now: float) -> Optional[float]:
    if not isinstance(raw, str) or not raw.strip():
        return None
    text = raw.strip()
    if text.endswith("Z") or text.endswith("z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except (ValueError, TypeError):
        return None
    if parsed.tzinfo is None:
        # Без зоны считаем UTC: провайдер отдаёт зону всегда, и «наивное»
        # значение здесь — уже отклонение от формы. Локальная зона воркера
        # сдвинула бы дату сброса на часы.
        parsed = parsed.replace(tzinfo=timezone.utc)
    try:
        epoch = parsed.timestamp()
    except (OverflowError, OSError, ValueError):
        return None
    if epoch < now - _RESET_PAST_LIMIT_SEC or epoch > now + _RESET_FUTURE_LIMIT_SEC:
        return None
    return epoch
