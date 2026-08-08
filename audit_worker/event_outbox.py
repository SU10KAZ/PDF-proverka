"""Дисковый EventOutbox: события сначала на диск, потом в сеть.

Это ядро инварианта I-01 «потеря связи не останавливает работу»: конвейер
пишет в файл и не имеет ни одного вызова к центру на критическом пути. Сеть —
забота отдельного отправителя, который при обрыве просто копит.

Свойства (§14.4 техпроекта):
  * монотонный `seq`, персистится в cursor.json атомарно; НИКОГДА не
    сбрасывается — в том числе при рестарте воркера;
  * события пишутся сегментами JSONL с fsync-порогом;
  * центр подтверждает last_seen_seq → двигаем last_acked_seq;
  * повторная передача безопасна (дедуп на центре по (job, attempt, seq));
  * подтверждённые сегменты уплотняются;
  * при потере сети журнал растёт до потолка, дальше — прореживание
    log_line с явным событием events_truncated (потеря видима, а не молчалива).

Секреты чистятся ПРИ ЗАПИСИ (I-12), а не перед отправкой: outbox — файл на
диске стороннего VPS.
"""
from __future__ import annotations

import json
import os
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Optional

try:                                     # pragma: no cover — только POSIX
    import fcntl
except ImportError:                      # pragma: no cover
    fcntl = None                         # type: ignore[assignment]

from audit_worker import redaction
from audit_worker.local_store import atomic_write_json, read_json

SEGMENT_MAX_BYTES = 64 * 1024 * 1024
SEGMENT_MAX_LINES = 50_000
OUTBOX_MAX_BYTES = 1024 * 1024 * 1024      # потолок на задание
FSYNC_EVERY = 64


class EventOutbox:
    def __init__(self, events_dir: Path, *, secret_literals: Iterable[str] = ()):
        self.dir = events_dir
        self.dir.mkdir(parents=True, exist_ok=True)
        self.cursor_path = self.dir / "cursor.json"
        # Отметка подтверждения живёт ОТДЕЛЬНЫМ файлом. С этапа 3.5 в журнал
        # пишет исполнитель, а подтверждает отправку агент — это разные
        # процессы. Держи они один файл, каждый затирал бы чужое поле: агент
        # откатывал бы last_written_seq, а исполнитель — last_acked_seq.
        self.ack_path = self.dir / "ack.json"
        self._secrets = tuple(s for s in secret_literals if s)
        self._pending_since_sync = 0
        cursor = read_json(self.cursor_path, None) or {}
        ack = read_json(self.ack_path, None) or {}
        self.last_written_seq: int = int(cursor.get("last_written_seq", 0))
        self.last_acked_seq: int = int(
            ack.get("last_acked_seq", cursor.get("last_acked_seq", 0))
        )
        self.active_segment: int = int(cursor.get("active_segment", 1))
        self.truncating: bool = bool(cursor.get("truncating", False))
        self._info_counter = 0
        # append() зовут из НЕСКОЛЬКИХ потоков: чтения stdout и stderr,
        # отправитель событий (worker_reconnected) и основной поток задания.
        # Без лока read-modify-write над last_written_seq выдавал двум событиям
        # один seq, и pending_batch навсегда обрывался на дубле — событие
        # (в том числе job_completed_locally) не уходило никогда.
        self._lock = threading.Lock()
        # Межпроцессный замок. Потокового лока мало: с этапа 3.5 в один и тот
        # же каталог пишет исполнитель (события конвейера) и агент
        # (`worker_reconnected`, `job_failed` при обрыве загрузки). Это разные
        # ПРОЦЕССЫ, у каждого свой `last_written_seq` в памяти — без замка оба
        # выдавали одному номеру два события, и второе терялось молча.
        self._lock_path = self.dir / ".seq.lock"
        self._repair_cursor_against_segments()

    @contextmanager
    def _interprocess_lock(self):
        """flock на весь цикл «прочитать курсор → записать → сохранить курсор».

        Если fcntl недоступен (не-Linux), деградируем до потокового лока:
        хуже, чем ничего, но одна из двух причин дублей всё равно закрыта.
        """
        if fcntl is None:
            yield
            return
        with self._lock_path.open("a+") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _repair_cursor_against_segments(self) -> None:
        """Согласовать курсор с тем, что реально лежит в сегментах.

        Курсор впереди файлов (жёсткий отказ машины, потеря страничного кэша)
        останавливал поток событий насовсем: pending_batch требует ровно
        `last_acked+1`, не находит его и возвращает пустой список, а 409
        от центра, который чинит рассинхрон, не приходит — отправлять нечего.
        """
        highest = 0
        for path in sorted(self.dir.glob("outbox-*.jsonl")):
            try:
                with path.open("r", encoding="utf-8") as fh:
                    for line in fh:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            seq = int(json.loads(line).get("seq", 0))
                        except (ValueError, TypeError):
                            continue
                        highest = max(highest, seq)
            except OSError:
                continue
        # Файлы — источник истины в ОБЕ стороны. Курсор впереди файлов ⇒
        # ужимаем; курсор позади файлов (потерян cursor.json, а сегменты целы)
        # ⇒ поднимаем, иначе следующая запись переиспользует занятые номера и
        # центр молча отбросит их как дубли.
        if highest and highest != self.last_written_seq:
            self.last_written_seq = highest
            self._save_cursor()
        if self.last_acked_seq > self.last_written_seq:
            self.last_acked_seq = self.last_written_seq
            self._save_ack()

    # ─── Сегменты ────────────────────────────────────────────────────────────
    def _segment_path(self, index: int) -> Path:
        return self.dir / f"outbox-{index:04d}.jsonl"

    def _rotate_if_needed(self) -> None:
        path = self._segment_path(self.active_segment)
        if not path.exists():
            return
        stat = path.stat()
        if stat.st_size >= SEGMENT_MAX_BYTES:
            self.active_segment += 1
            return
        if stat.st_size and stat.st_size > SEGMENT_MAX_BYTES // 2:
            return
        # Быстрая оценка по строкам только когда файл небольшой.
        if stat.st_size < 4 * 1024 * 1024:
            with path.open("r", encoding="utf-8") as fh:
                if sum(1 for _ in fh) >= SEGMENT_MAX_LINES:
                    self.active_segment += 1

    def total_bytes(self) -> int:
        return sum(p.stat().st_size for p in self.dir.glob("outbox-*.jsonl") if p.is_file())

    # ─── Запись ──────────────────────────────────────────────────────────────
    def append(
        self,
        event_type: str,
        payload: Optional[dict[str, Any]] = None,
        *,
        occurred_at: Optional[float] = None,
    ) -> Optional[int]:
        """Записать событие. Возвращает seq или None, если событие прорежено."""
        with self._lock, self._interprocess_lock():
            # Курсор мог продвинуть ДРУГОЙ процесс, пока мы ждали замок.
            self._refresh_writer_state()
            return self._append_locked(event_type, payload, occurred_at=occurred_at)

    def _refresh_writer_state(self) -> None:
        """Подтянуть состояние писателя с диска (под уже взятым замком)."""
        cursor = read_json(self.cursor_path, None) or {}
        disk_seq = int(cursor.get("last_written_seq", 0))
        if disk_seq > self.last_written_seq:
            self.last_written_seq = disk_seq
            self.active_segment = max(
                self.active_segment, int(cursor.get("active_segment", 1))
            )

    def _append_locked(
        self,
        event_type: str,
        payload: Optional[dict[str, Any]] = None,
        *,
        occurred_at: Optional[float] = None,
    ) -> Optional[int]:
        if self._should_drop(event_type):
            return None
        self._rotate_if_needed()
        seq = self.last_written_seq + 1
        record = {
            "seq": seq,
            "event_id": f"ev_{uuid.uuid4().hex[:16]}",
            "event_type": event_type,
            "occurred_at": occurred_at if occurred_at is not None else time.time(),
            "schema_version": 1,
            "payload": redaction.redact_mapping(payload or {}, extra_literals=self._secrets),
        }
        path = self._segment_path(self.active_segment)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
            self._pending_since_sync += 1
            if self._pending_since_sync >= FSYNC_EVERY:
                fh.flush()
                os.fsync(fh.fileno())
                self._pending_since_sync = 0
        self.last_written_seq = seq
        self._save_cursor()
        return seq

    def _should_drop(self, event_type: str) -> bool:
        """Прореживание при переполнении: структурные события не теряем никогда."""
        if event_type != "log_line":
            return False
        if self.total_bytes() < OUTBOX_MAX_BYTES:
            self.truncating = False
            return False
        if not self.truncating:
            self.truncating = True
            self._save_cursor()
            # Именно _append_locked: лок уже удерживается вызывающим append,
            # а он нереентерабельный — self.append() дал бы самоблокировку.
            self._append_locked(
                "events_truncated",
                {
                    "reason": "outbox_size_limit",
                    "limit_bytes": OUTBOX_MAX_BYTES,
                    "from_seq": self.last_written_seq + 1,
                    "policy": "log_line прореживается 1 из 20, структурные события сохраняются",
                },
            )
            return True
        self._info_counter += 1
        return self._info_counter % 20 != 0

    def _save_cursor(self) -> None:
        """Состояние ПИСАТЕЛЯ. `last_acked_seq` дублируется только для чтения
        глазами: источник истины для него — ack.json."""
        atomic_write_json(
            self.cursor_path,
            {
                "last_written_seq": self.last_written_seq,
                "last_acked_seq": self.last_acked_seq,
                "active_segment": self.active_segment,
                "truncating": self.truncating,
            },
        )

    def _save_ack(self) -> None:
        """Состояние ЧИТАТЕЛЯ (агента). Отдельный файл — отдельный писатель."""
        atomic_write_json(self.ack_path, {"last_acked_seq": self.last_acked_seq})

    def reload(self) -> None:
        """Перечитать позиции с диска.

        Обязательно для агента: журнал наполняет ДРУГОЙ процесс, и объект в
        памяти о новых событиях сам по себе не узнает — `has_pending` вечно
        отвечал бы «нечего отправлять».
        """
        cursor = read_json(self.cursor_path, None) or {}
        ack = read_json(self.ack_path, None) or {}
        with self._lock:
            self.last_written_seq = max(
                self.last_written_seq, int(cursor.get("last_written_seq", 0))
            )
            self.active_segment = max(
                self.active_segment, int(cursor.get("active_segment", 1))
            )
            self.last_acked_seq = max(
                self.last_acked_seq,
                int(ack.get("last_acked_seq", cursor.get("last_acked_seq", 0))),
            )

    # ─── Чтение и подтверждение ──────────────────────────────────────────────
    def pending_batch(self, limit: int = 500) -> list[dict[str, Any]]:
        """Непрерывный диапазон, начиная с last_acked_seq + 1.

        Непрерывность — контракт центра (§11.6): пакет с дырой отбивается 409.
        """
        want_from = self.last_acked_seq + 1
        collected: list[dict[str, Any]] = []
        # Уплотнённые сегменты тоже просматриваются. Центр после 409
        # sequence_gap может попросить повторить с номера, который уже уехал в
        # acked/: без этого запрошенного события не нашлось бы нигде, и поток
        # останавливался бы навсегда на «начало не с ожидаемого».
        segments = sorted(self.dir.glob("outbox-*.jsonl"))
        acked_dir = self.dir / "acked"
        if acked_dir.is_dir():
            segments = sorted(acked_dir.glob("outbox-*.jsonl")) + segments
        for segment in segments:
            with segment.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        item = json.loads(line)
                    except ValueError:
                        continue
                    seq = int(item.get("seq", 0))
                    if seq < want_from:
                        continue
                    if collected and seq != collected[-1]["seq"] + 1:
                        return collected           # дыра → отдаём префикс
                    if not collected and seq != want_from:
                        return collected           # начало не с ожидаемого
                    collected.append(item)
                    if len(collected) >= limit:
                        return collected
        return collected

    def ack(self, last_seen_seq: int) -> None:
        if last_seen_seq <= self.last_acked_seq:
            return
        self.last_acked_seq = min(last_seen_seq, self.last_written_seq)
        self._save_ack()
        self._compact()

    def rewind_to(self, expected_seq: int) -> None:
        """Центр сообщил, с какого номера повторять (ответ 409 sequence_gap)."""
        self.last_acked_seq = max(0, expected_seq - 1)
        self._save_ack()

    def _compact(self) -> None:
        """Убрать сегменты, целиком лежащие ниже last_acked_seq."""
        acked_dir = self.dir / "acked"
        for segment in sorted(self.dir.glob("outbox-*.jsonl")):
            index = int(segment.stem.split("-")[1])
            if index >= self.active_segment:
                continue
            max_seq = 0
            with segment.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        try:
                            max_seq = max(max_seq, int(json.loads(line).get("seq", 0)))
                        except ValueError:
                            continue
            if max_seq and max_seq <= self.last_acked_seq:
                acked_dir.mkdir(parents=True, exist_ok=True)
                segment.replace(acked_dir / segment.name)

    @property
    def has_pending(self) -> bool:
        return self.last_written_seq > self.last_acked_seq
