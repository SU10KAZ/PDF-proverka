"""Снимок ресурсов VPS и расчёт свободных слотов.

Формула §17.2 техпроекта. Этап 0 работает БЕЗ нормативной базы, поэтому
профиль RAM на задание — лёгкий (RAM_PER_JOB_LIGHT). Тяжёлый профиль
объявлен рядом, чтобы при подключении норм-этапа не изобретать его заново.

`binding_constraint` — не украшение: без него «свободно 1 из 3» невозможно
интерпретировать, и оператор не поймёт, что чинить.
"""
from __future__ import annotations

import os
import shutil
import threading
import time
from dataclasses import dataclass
from typing import Any, Optional

try:
    import psutil
except ImportError:  # psutil опционален: без него отдаём то, что даёт stdlib
    psutil = None  # type: ignore[assignment]

#: Потолок расчёта слотов. Совпадает с ДОКАЗАННЫМ максимумом этапа: считать
#: пять там, где проверены два, значило бы обещать неизвестное (см. slots.py).
HARD_CAP = 2

RAM_RESERVE_GB = 2.0            # ОС + сам агент; без норм-базы резерв скромный
RAM_PER_JOB_LIGHT_GB = 1.0      # профиль этапа 0: тестовый процесс
RAM_PER_JOB_NORMS_GB = 6.5      # профиль с норм-этапом (пока не используется)
SWAP_HARD_GB = 1.0

DISK_RESERVE_GB = 5.0
DISK_PER_JOB_GB = 0.5
DISK_HARD_MIN_GB = 2.0

CPU_RESERVE = 1
CPU_PER_JOB = 1.0

LA_SOFT = 1.0
LA_HARD = 1.5

SLOT_GROW_STABLE_SEC = 120.0


def _gb(value: float) -> float:
    return round(value / (1024 ** 3), 2)


def loadavg() -> tuple[float, float, float]:
    try:
        return os.getloadavg()
    except OSError:
        return (0.0, 0.0, 0.0)


@dataclass
class SlotCalculation:
    configured_max: int
    calculated_free: int
    components: dict[str, int]
    binding_constraint: str
    explanation: str


class ResourceMonitor:
    def __init__(self, data_root, *, configured_max_slots: int = 1):
        self.data_root = data_root
        self.configured_max_slots = max(0, min(HARD_CAP, configured_max_slots))
        self._last_value: Optional[int] = None
        self._pending_value: Optional[int] = None
        self._pending_since: float = 0.0
        # Монитор один на агента, а зовут его из ДВУХ потоков: сердцебиение и
        # главный цикл. Состояние гистерезиса — три обычных поля, и без замка
        # два потока переписывали их вперемешку: «рост только после периода
        # стабильности» превращался в лотерею, а показанное оператору число
        # слотов начинало мигать ровно там, где гистерезис и должен был это
        # мигание убрать.
        self._hysteresis_lock = threading.Lock()

    # ─── Снимок ──────────────────────────────────────────────────────────────
    def snapshot(self, *, active_jobs: int = 0, live_processes: int = 0) -> dict[str, Any]:
        ram_total = ram_available = swap_used = 0.0
        if psutil is not None:
            vm = psutil.virtual_memory()
            sw = psutil.swap_memory()
            ram_total, ram_available = _gb(vm.total), _gb(vm.available)
            swap_used = _gb(sw.used)

        try:
            usage = shutil.disk_usage(str(self.data_root))
            disk_total, disk_free = _gb(usage.total), _gb(usage.free)
        except OSError:
            disk_total = disk_free = 0.0

        cores = os.cpu_count() or 1
        la1, la5, la15 = loadavg()

        slots = self.calculate_slots(
            ram_available_gb=ram_available,
            swap_used_gb=swap_used,
            disk_free_gb=disk_free,
            cores=cores,
            la5=la5,
            active_jobs=active_jobs,
        )
        return {
            "at": time.time(),
            "ram": {
                "total_gb": ram_total,
                "available_gb": ram_available,
                "swap_used_gb": swap_used,
                "source": "psutil" if psutil else "unavailable",
            },
            "cpu": {"cores": cores, "la1": la1, "la5": la5, "la15": la15},
            "disk": {"path": str(self.data_root), "total_gb": disk_total, "free_gb": disk_free},
            "processes": {"live_children": live_processes, "active_jobs": active_jobs},
            "slots": {
                "configured_max": slots.configured_max,
                "calculated_free": slots.calculated_free,
                "components": slots.components,
                "binding_constraint": slots.binding_constraint,
                "explanation": slots.explanation,
            },
        }

    # ─── Слоты ───────────────────────────────────────────────────────────────
    def calculate_slots(
        self,
        *,
        ram_available_gb: float,
        swap_used_gb: float,
        disk_free_gb: float,
        cores: int,
        la5: float,
        active_jobs: int,
        ram_per_job_gb: float = RAM_PER_JOB_LIGHT_GB,
        now: Optional[float] = None,
    ) -> SlotCalculation:
        components: dict[str, int] = {}

        # S_ram: жёсткий ноль при свопе — это профиль обоих OOM-инцидентов.
        if swap_used_gb > SWAP_HARD_GB:
            components["s_ram"] = 0
        elif ram_available_gb <= 0:
            components["s_ram"] = HARD_CAP     # нет данных psutil — не ограничиваем
        else:
            components["s_ram"] = max(
                0, int((ram_available_gb - RAM_RESERVE_GB) // ram_per_job_gb)
            )

        if disk_free_gb and disk_free_gb < DISK_HARD_MIN_GB:
            components["s_disk"] = 0
        elif disk_free_gb <= 0:
            components["s_disk"] = HARD_CAP
        else:
            components["s_disk"] = max(
                0, int((disk_free_gb - DISK_RESERVE_GB) // DISK_PER_JOB_GB)
            )

        components["s_cpu"] = max(0, int((cores - CPU_RESERVE) // CPU_PER_JOB))

        per_core = (la5 / cores) if cores else 0.0
        if per_core >= LA_HARD:
            components["s_la"] = 0
        elif per_core >= LA_SOFT:
            components["s_la"] = max(0, self.configured_max_slots - 1)
        else:
            components["s_la"] = HARD_CAP

        components["s_cfg"] = self.configured_max_slots

        # При равных значениях «связывающим» называем настройку оператора: она
        # осмысленный ответ на вопрос «почему столько», а s_la = HARD_CAP — это
        # «ограничения нет». Раньше ничья решалась порядком ключей в словаре.
        _PRIORITY = ("s_cfg", "s_ram", "s_disk", "s_cpu", "s_la")
        binding = min(
            components, key=lambda k: (components[k], _PRIORITY.index(k))
        )
        raw = min(HARD_CAP, min(components.values()))
        # Гистерезис применяется к ЁМКОСТИ, а не к числу свободных слотов.
        # Иначе освобождение слота (задание закончилось) считалось бы «ростом»
        # и ждало две минуты стабильности: третье задание не стартовало бы
        # сразу после второго, хотя ресурсы для него уже есть. Сглаживать надо
        # мигание ресурсов, а не факт занятости.
        capacity = self._apply_hysteresis(raw, now=now)
        free = max(0, capacity - active_jobs)

        explanation = {
            "s_ram": f"RAM: ({ram_available_gb:.1f} − {RAM_RESERVE_GB}) / "
                     f"{ram_per_job_gb} = {components['s_ram']}"
                     + (" · своп превышен → 0" if swap_used_gb > SWAP_HARD_GB else ""),
            "s_disk": f"Диск: ({disk_free_gb:.1f} − {DISK_RESERVE_GB}) / "
                      f"{DISK_PER_JOB_GB} = {components['s_disk']}"
                      + (" · ниже жёсткого минимума → 0"
                         if disk_free_gb and disk_free_gb < DISK_HARD_MIN_GB else ""),
            "s_cpu": f"CPU: ({cores} − {CPU_RESERVE}) / {CPU_PER_JOB} = {components['s_cpu']}",
            "s_la": f"LA5/ядро = {per_core:.2f}",
            "s_cfg": f"Настройка оператора: {self.configured_max_slots}",
        }[binding]

        return SlotCalculation(
            configured_max=self.configured_max_slots,
            calculated_free=free,
            components=components,
            binding_constraint=binding,
            explanation=explanation,
        )

    def _apply_hysteresis(self, value: int, *, now: Optional[float] = None) -> int:
        """Сокращение — сразу, рост — только после периода стабильности.

        Без этого одно освободившееся ядро вызывает мигание «1 / 0» и дёргает
        планировщик.
        """
        with self._hysteresis_lock:
            return self._apply_hysteresis_locked(value, now=now)

    def _apply_hysteresis_locked(self, value: int, *, now: Optional[float] = None) -> int:
        stamp = now or time.time()
        if self._last_value is None:
            self._last_value = value
            return value
        if value <= self._last_value:
            self._last_value = value
            self._pending_value = None
            return value
        if self._pending_value != value:
            self._pending_value = value
            self._pending_since = stamp
            return self._last_value
        if stamp - self._pending_since >= SLOT_GROW_STABLE_SEC:
            self._last_value = value
            self._pending_value = None
            return value
        return self._last_value
