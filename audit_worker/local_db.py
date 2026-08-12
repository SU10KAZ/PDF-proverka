"""Локальная база воркера: точка стыка сетевого агента и исполнителя.

Зачем появилась. На этапе 0 агент и работа жили в одном процессе: сеть, опрос
центра и запуск тестового процесса делал один и тот же питон. Из этого
следовало ровно то, что и должно было: перезапуск СЕТЕВОГО агента убивал
РАБОТУ. Разделить их можно, только если у них есть общий транзакционный стык —
файлы через `os.replace` для «атомарно захватить задание» не годятся: два
исполнителя одновременно прочитают одно и то же и запустят два процесса.

Отсюда SQLite WAL и та же дисциплина, что на центре (ADR-007):
  * один писатель под threading.Lock, читатели — свои соединения;
  * захват задания — `BEGIN IMMEDIATE` + условный UPDATE, а не «прочитал и
    записал»;
  * миграции нумерованные, вперёд-только, каждая одной транзакцией.

Что здесь НЕ хранится: worker-токен, bootstrap-секрет, любые учётные данные.
База доступна исполнителю, а исполнителю сетевые секреты не положены (§8.2).
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Optional

SCHEMA_VERSION = 2

PRAGMAS = (
    "PRAGMA journal_mode = WAL",
    "PRAGMA synchronous = NORMAL",
    "PRAGMA busy_timeout = 10000",
    "PRAGMA foreign_keys = ON",
)

# Состояния локальной очереди. `interrupted` — диагностическое: процесс исчез
# без маркера завершения, и автоматически перезапускать его НЕЛЬЗЯ (§8.6).
QUEUE_QUEUED = "queued"
QUEUE_CLAIMED = "claimed"
QUEUE_RUNNING = "running"
QUEUE_FINISHED = "finished"
QUEUE_FAILED = "failed"
QUEUE_CANCELLED = "cancelled"
QUEUE_INTERRUPTED = "executor_interrupted"

TERMINAL_QUEUE_STATES = frozenset(
    {QUEUE_FINISHED, QUEUE_FAILED, QUEUE_CANCELLED, QUEUE_INTERRUPTED}
)

LEASE_SEC = 120.0

_MIGRATION_1 = """
CREATE TABLE IF NOT EXISTS executor_instances (
    executor_instance_id   TEXT PRIMARY KEY,
    process_pid            INTEGER NOT NULL,
    -- Тик старта из /proc: голый pid переиспользуется системой, и без метки
    -- времени чужой процесс легко принять за свой (I-17).
    process_start_identity REAL,
    version                TEXT,
    started_at             REAL NOT NULL,
    last_heartbeat_at      REAL NOT NULL,
    status                 TEXT NOT NULL DEFAULT 'online'
);

CREATE TABLE IF NOT EXISTS execution_queue (
    attempt_id          TEXT PRIMARY KEY,
    job_id              TEXT NOT NULL,
    job_type            TEXT NOT NULL,
    state               TEXT NOT NULL DEFAULT 'queued',
    claimed_by_executor TEXT,
    claim_generation    INTEGER NOT NULL DEFAULT 0,
    claimed_at          REAL,
    lease_expires_at    REAL,
    created_at          REAL NOT NULL,
    updated_at          REAL NOT NULL,
    params_json         TEXT NOT NULL DEFAULT '{}',
    result_json         TEXT
);
CREATE INDEX IF NOT EXISTS ix_queue_state ON execution_queue(state);

CREATE TABLE IF NOT EXISTS local_commands (
    local_command_id   TEXT PRIMARY KEY,
    command_type       TEXT NOT NULL,
    job_id             TEXT,
    attempt_id         TEXT,
    payload_json       TEXT NOT NULL DEFAULT '{}',
    status             TEXT NOT NULL DEFAULT 'pending',
    created_at         REAL NOT NULL,
    claimed_at         REAL,
    completed_at       REAL,
    result_json        TEXT,
    -- Идемпотентность сквозная: одна команда центра → одна локальная.
    central_command_id TEXT UNIQUE
);
CREATE INDEX IF NOT EXISTS ix_local_cmd_status ON local_commands(status);

CREATE TABLE IF NOT EXISTS process_registry (
    attempt_id             TEXT PRIMARY KEY,
    job_id                 TEXT NOT NULL,
    executor_instance_id   TEXT,
    pid                    INTEGER,
    process_start_identity REAL,
    command_fingerprint    TEXT,
    process_group_id       INTEGER,
    started_at             REAL,
    last_observed_at       REAL,
    status                 TEXT NOT NULL DEFAULT 'running',
    exit_code              INTEGER
);
"""

# ─────────────────────────────────────────────────────────────────────────────
# Миграция 2 (пред-пайплайновый гейт): номер события выдаёт БАЗА, а не файл.
#
# Что было. Счётчик `seq` жил в `cursor.json` рядом с сегментами журнала, а от
# гонки двух ПРОЦЕССОВ (агент пишет `worker_reconnected`, исполнитель — события
# конвейера) защищал `flock`. Это работает на Linux и деградирует до потокового
# лока везде, где `fcntl` недоступен или ФС его не поддерживает: два процесса
# снова выдают двум событиям один номер, а центр дедуплицирует по
# (job, attempt, seq) — второе теряется МОЛЧА. На одном слоте это было редкой
# гонкой, на двух становится штатным режимом.
#
# Что стало. Номер выдаётся транзакцией `BEGIN IMMEDIATE` в SQLite:
# инкремент счётчика и вставка строки журнала идут ОДНОЙ транзакцией, а
# PRIMARY KEY(job_id, attempt_id, sequence) делает дубль физически невозможным.
# Пары (job, attempt) независимы: своя строка счётчика у каждой.
#
# `written` отличает «номер выдан» от «строка легла в сегмент». Разрыв между
# ними — окно в доли секунды, но если процесс умрёт именно в нём, в файлах
# останется дыра. Она НЕ прячется: `EventOutbox.heal_allocation_gaps` дописывает
# на это место явное событие `events_truncated` с объяснением. Потеря видима, а
# поток событий не встаёт навсегда на ожидании пропавшего номера.
_MIGRATION_2 = """
CREATE TABLE IF NOT EXISTS event_sequences (
    job_id     TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    next_seq   INTEGER NOT NULL,
    updated_at REAL NOT NULL,
    PRIMARY KEY (job_id, attempt_id)
);

CREATE TABLE IF NOT EXISTS event_journal (
    job_id       TEXT NOT NULL,
    attempt_id   TEXT NOT NULL,
    sequence     INTEGER NOT NULL,
    event_id     TEXT NOT NULL,
    event_type   TEXT NOT NULL,
    allocated_at REAL NOT NULL,
    written      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (job_id, attempt_id, sequence)
);
CREATE INDEX IF NOT EXISTS ix_event_journal_unwritten
    ON event_journal(job_id, attempt_id) WHERE written = 0;
"""

MIGRATIONS: dict[int, str] = {1: _MIGRATION_1, 2: _MIGRATION_2}


class LocalDB:
    """Соединение к worker.db. Один экземпляр на процесс (agent или executor)."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._write_lock = threading.Lock()
        self._local = threading.local()
        self._migrate()

    # ─── Соединения ──────────────────────────────────────────────────────────
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.path), timeout=15.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        for pragma in PRAGMAS:
            conn.execute(pragma)
        return conn

    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._connect()
            self._local.conn = conn
        return conn

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    @contextmanager
    def write(self) -> Iterator[sqlite3.Connection]:
        with self._write_lock:
            conn = self._conn()
            conn.execute("BEGIN IMMEDIATE")
            # COMMIT внутри try. Снаружи он оставлял транзакцию открытой на
            # потоко-локальном соединении при любой своей ошибке (кончившийся
            # диск, SQLITE_BUSY на коммите), а соединение кэшируется навсегда:
            # поток после этого падал на КАЖДОЙ записи с «cannot start a
            # transaction within a transaction». Лечение — закрыть соединение.
            try:
                try:
                    yield conn
                except Exception:
                    conn.execute("ROLLBACK")
                    raise
                conn.execute("COMMIT")
            except Exception:
                if conn.in_transaction:
                    self.close()
                raise

    def read(self) -> sqlite3.Connection:
        return self._conn()

    def _migrate(self) -> None:
        conn = self._connect()
        try:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations "
                "(version INTEGER PRIMARY KEY, applied_at REAL NOT NULL)"
            )
            row = conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()
            version = int(row[0]) if row and row[0] is not None else 0
            for target in sorted(MIGRATIONS):
                if target <= version:
                    continue
                conn.execute("BEGIN IMMEDIATE")
                try:
                    for statement in MIGRATIONS[target].split(";"):
                        if statement.strip():
                            conn.execute(statement)
                    conn.execute(
                        "INSERT INTO schema_migrations (version, applied_at) VALUES (?,?)",
                        (target, time.time()),
                    )
                except Exception:
                    conn.execute("ROLLBACK")
                    raise
                conn.execute("COMMIT")
        finally:
            conn.close()

    # ─── Исполнители ─────────────────────────────────────────────────────────
    #: Идентификатор ЭТОГО воплощения исполнителя (ставит register_executor).
    instance_id_hint: Optional[str] = None

    def register_executor(self, *, version: str) -> str:
        from audit_worker import process_registry as procinfo

        instance_id = f"exe_{uuid.uuid4().hex[:12]}"
        now = time.time()
        pid = os.getpid()
        with self.write() as conn:
            conn.execute(
                "INSERT INTO executor_instances (executor_instance_id, process_pid, "
                "process_start_identity, version, started_at, last_heartbeat_at, status) "
                "VALUES (?,?,?,?,?,?,?)",
                (instance_id, pid, procinfo.process_start_time(pid), version,
                 now, now, "online"),
            )
        self.instance_id_hint = instance_id
        return instance_id

    def executor_heartbeat(self, instance_id: str, *, status: str = "online") -> None:
        with self.write() as conn:
            conn.execute(
                "UPDATE executor_instances SET last_heartbeat_at = ?, status = ? "
                "WHERE executor_instance_id = ?",
                (time.time(), status, instance_id),
            )

    def executor_stopped(self, instance_id: str) -> None:
        with self.write() as conn:
            conn.execute(
                "UPDATE executor_instances SET status = 'offline', last_heartbeat_at = ? "
                "WHERE executor_instance_id = ?",
                (time.time(), instance_id),
            )

    def executor_alive(self, instance_id: Optional[str]) -> bool:
        """Жив ли ИМЕННО этот исполнитель (pid + тик старта из /proc).

        Нужно, чтобы второй запущенный исполнитель не забрал попытки первого:
        он стал бы вторым наблюдателем за тем же процессом и вторым сборщиком
        того же архива. «Сам себя» живым здесь не считаем — восстановление
        своей же прерванной работы законно.
        """
        if not instance_id or instance_id == self.instance_id_hint:
            return False
        from audit_worker import process_registry as procinfo

        row = self.read().execute(
            "SELECT * FROM executor_instances WHERE executor_instance_id = ?",
            (instance_id,),
        ).fetchone()
        if row is None:
            return False
        return procinfo.is_alive(
            int(row["process_pid"]), row["process_start_identity"]
        )

    def latest_executor(self) -> Optional[dict[str, Any]]:
        """Самый свежий исполнитель. Живость решает вызывающий, а не флаг в БД."""
        row = self.read().execute(
            "SELECT * FROM executor_instances ORDER BY last_heartbeat_at DESC LIMIT 1"
        ).fetchone()
        return dict(row) if row else None

    def executor_snapshot(self, *, stale_sec: float = 90.0) -> dict[str, Any]:
        """Состояние исполнителя для heartbeat агента.

        Статус вычисляется по СВЕЖЕСТИ отметки и живости pid, а не по колонке
        `status`: упавший процесс свою строку в 'offline' не переведёт.
        """
        from audit_worker import process_registry as procinfo

        row = self.latest_executor()
        if row is None:
            return {"status": "offline", "running_processes": 0, "ambiguous_processes": 0}
        alive = procinfo.is_alive(
            int(row["process_pid"]), row["process_start_identity"]
        )
        age = time.time() - float(row["last_heartbeat_at"])
        if not alive:
            status = "offline"
        elif age <= stale_sec:
            status = "online"
        else:
            status = "stale"
        if row["status"] == "offline" and not alive:
            status = "offline"
        procs = self.list_processes()
        return {
            "executor_instance_id": row["executor_instance_id"],
            "status": status,
            "last_heartbeat_at": row["last_heartbeat_at"],
            "version": row["version"],
            "running_processes": sum(1 for p in procs if p["status"] == "running"),
            "ambiguous_processes": sum(1 for p in procs if p["status"] == "ambiguous"),
        }

    # ─── Очередь исполнения ──────────────────────────────────────────────────
    def enqueue(
        self, *, job_id: str, attempt_id: str, job_type: str, params: dict[str, Any]
    ) -> bool:
        """Поставить попытку в локальную очередь. Повтор безопасен."""
        now = time.time()
        with self.write() as conn:
            existing = conn.execute(
                "SELECT state FROM execution_queue WHERE attempt_id = ?", (attempt_id,)
            ).fetchone()
            if existing is not None:
                return False
            conn.execute(
                "INSERT INTO execution_queue (attempt_id, job_id, job_type, state, "
                "created_at, updated_at, params_json) VALUES (?,?,?,?,?,?,?)",
                (attempt_id, job_id, job_type, QUEUE_QUEUED, now, now,
                 json.dumps(params, ensure_ascii=False)),
            )
        return True

    def claim_next(
        self,
        executor_instance_id: str,
        *,
        capacity_limit: Optional[int] = None,
    ) -> Optional[dict[str, Any]]:
        """Атомарно захватить одну попытку.

        Захват — условный UPDATE под BEGIN IMMEDIATE. Два одновременно
        запущенных исполнителя не могут получить одну строку: проигравший
        увидит rowcount == 0 (§8.4).

        `capacity_limit` — АБСОЛЮТНЫЙ потолок одновременных попыток. Подсчёт
        занятых идёт ВНУТРИ той же транзакции: снаружи он давал бы окно, в
        котором два захвата успевают проскочить мимо лимита. Отказ по ёмкости —
        не ошибка попытки: строка остаётся `queued` и ждёт слот (§18 задания).
        """
        now = time.time()
        with self.write() as conn:
            if capacity_limit is not None:
                busy = conn.execute(
                    "SELECT COUNT(*) AS n FROM execution_queue WHERE state IN (?, ?)",
                    (QUEUE_CLAIMED, QUEUE_RUNNING),
                ).fetchone()
                if int(busy["n"]) >= int(capacity_limit):
                    return None
            row = conn.execute(
                "SELECT * FROM execution_queue WHERE state = ? "
                "ORDER BY created_at ASC LIMIT 1",
                (QUEUE_QUEUED,),
            ).fetchone()
            if row is None:
                return None
            cur = conn.execute(
                "UPDATE execution_queue SET state = ?, claimed_by_executor = ?, "
                "claim_generation = claim_generation + 1, claimed_at = ?, "
                "lease_expires_at = ?, updated_at = ? "
                "WHERE attempt_id = ? AND state = ?",
                (QUEUE_CLAIMED, executor_instance_id, now, now + LEASE_SEC, now,
                 row["attempt_id"], QUEUE_QUEUED),
            )
            if cur.rowcount != 1:
                return None
            claimed = dict(row)
            claimed.update(
                {
                    "state": QUEUE_CLAIMED,
                    "claimed_by_executor": executor_instance_id,
                    "claim_generation": int(row["claim_generation"]) + 1,
                }
            )
        return claimed

    def adopt_claim(self, attempt_id: str, executor_instance_id: str) -> bool:
        """Переписать владельца строки очереди на себя при подхвате после рестарта.

        Без этого новый исполнитель наблюдал за выжившим процессом, но строка
        очереди оставалась записана на МЁРТВОЕ воплощение: `renew_lease` с
        условием `claimed_by_executor = ?` не обновлял ничего, аренда тихо
        протухала, а `set_queue_state(..., executor_instance_id=...)` на путях,
        где владелец проверяется, промахивался бы мимо строки.

        Терминальные состояния не трогаются: попытку, у которой исход уже
        записан, подхватывать нечем и незачем.
        """
        terminal = sorted(TERMINAL_QUEUE_STATES)
        with self.write() as conn:
            cur = conn.execute(
                "UPDATE execution_queue SET claimed_by_executor = ?, "
                "lease_expires_at = ?, updated_at = ? WHERE attempt_id = ? "
                f"AND state NOT IN ({','.join('?' for _ in terminal)})",
                [executor_instance_id, time.time() + LEASE_SEC, time.time(), attempt_id]
                + terminal,
            )
            return cur.rowcount == 1

    def renew_lease(self, attempt_id: str, executor_instance_id: str) -> None:
        with self.write() as conn:
            conn.execute(
                "UPDATE execution_queue SET lease_expires_at = ?, updated_at = ? "
                "WHERE attempt_id = ? AND claimed_by_executor = ?",
                (time.time() + LEASE_SEC, time.time(), attempt_id, executor_instance_id),
            )

    def set_queue_state(
        self,
        attempt_id: str,
        state: str,
        *,
        executor_instance_id: Optional[str] = None,
        result: Optional[dict[str, Any]] = None,
        expect_states: Optional[tuple[str, ...]] = None,
    ) -> bool:
        """Перевести попытку в новое состояние. Возвращает, случилась ли запись.

        Два условия ставятся на уровне SQL, а не на уровне дисциплины вызовов:

        1. Из ТЕРМИНАЛЬНОГО состояния выйти нельзя. Без этого работала гонка:
           отмена видела попытку в `claimed`, ставила `cancelled` и отвечала
           центру «локально не выполняется» — центр объявлял попытку отменённой
           и освобождал слот, — а поток задания в этот момент дописывал
           `running` поверх и запускал настоящий процесс. Работа шла в контур
           уже отменённой попытки и сгорала молча.
        2. `expect_states` — условие «состояние не менялось с тех пор, как я его
           прочитал». Нужно там, где между чтением и записью принимается
           решение: сравнение внутри той же транзакции, что и запись, закрывает
           окно целиком.

        Возвращаемое значение обязано проверяться на путях, где отказ означает
        «кто-то меня опередил», — прежде всего в `run_attempt` перед стартом
        процесса.
        """
        fields = ["state = ?", "updated_at = ?"]
        args: list[Any] = [state, time.time()]
        if result is not None:
            fields.append("result_json = ?")
            args.append(json.dumps(result, ensure_ascii=False))
        sql = f"UPDATE execution_queue SET {', '.join(fields)} WHERE attempt_id = ?"
        args.append(attempt_id)
        # Гейт стоит ВСЕГДА, в том числе на переходе «терминальное →
        # терминальное»: второй исход поверх первого — это тоже потеря.
        terminal = sorted(TERMINAL_QUEUE_STATES)
        sql += f" AND state NOT IN ({','.join('?' for _ in terminal)})"
        args.extend(terminal)
        if expect_states:
            sql += f" AND state IN ({','.join('?' for _ in expect_states)})"
            args.extend(expect_states)
        if executor_instance_id:
            sql += " AND claimed_by_executor = ?"
            args.append(executor_instance_id)
        with self.write() as conn:
            cur = conn.execute(sql, args)
            return cur.rowcount == 1

    def queue_item(self, attempt_id: str) -> Optional[dict[str, Any]]:
        row = self.read().execute(
            "SELECT * FROM execution_queue WHERE attempt_id = ?", (attempt_id,)
        ).fetchone()
        return dict(row) if row else None

    def list_queue(self, *, states: Optional[tuple[str, ...]] = None) -> list[dict[str, Any]]:
        if states:
            placeholders = ",".join("?" for _ in states)
            rows = self.read().execute(
                f"SELECT * FROM execution_queue WHERE state IN ({placeholders}) "
                "ORDER BY created_at ASC",
                states,
            ).fetchall()
        else:
            rows = self.read().execute(
                "SELECT * FROM execution_queue ORDER BY created_at ASC"
            ).fetchall()
        return [dict(r) for r in rows]

    # ─── Локальные команды ───────────────────────────────────────────────────
    def enqueue_local_command(
        self,
        *,
        command_type: str,
        job_id: Optional[str],
        attempt_id: Optional[str],
        payload: dict[str, Any],
        central_command_id: Optional[str] = None,
    ) -> Optional[dict[str, Any]]:
        """Положить команду для исполнителя. Повтор по central_command_id — no-op."""
        local_id = f"lcmd_{uuid.uuid4().hex[:12]}"
        now = time.time()
        try:
            with self.write() as conn:
                conn.execute(
                    "INSERT INTO local_commands (local_command_id, command_type, "
                    "job_id, attempt_id, payload_json, status, created_at, "
                    "central_command_id) VALUES (?,?,?,?,?,?,?,?)",
                    (local_id, command_type, job_id, attempt_id,
                     json.dumps(payload, ensure_ascii=False), "pending", now,
                     central_command_id),
                )
        except sqlite3.IntegrityError:
            return self.local_command_by_central(central_command_id or "")
        return self.local_command(local_id)

    def local_command(self, local_command_id: str) -> Optional[dict[str, Any]]:
        row = self.read().execute(
            "SELECT * FROM local_commands WHERE local_command_id = ?", (local_command_id,)
        ).fetchone()
        return dict(row) if row else None

    def local_command_by_central(self, central_command_id: str) -> Optional[dict[str, Any]]:
        row = self.read().execute(
            "SELECT * FROM local_commands WHERE central_command_id = ?",
            (central_command_id,),
        ).fetchone()
        return dict(row) if row else None

    def claim_local_command(self) -> Optional[dict[str, Any]]:
        """Взять одну команду в работу. Второй исполнитель её уже не получит."""
        with self.write() as conn:
            row = conn.execute(
                "SELECT * FROM local_commands WHERE status = 'pending' "
                "ORDER BY created_at ASC LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            cur = conn.execute(
                "UPDATE local_commands SET status = 'processing', claimed_at = ? "
                "WHERE local_command_id = ? AND status = 'pending'",
                (time.time(), row["local_command_id"]),
            )
            if cur.rowcount != 1:
                return None
        item = dict(row)
        item["status"] = "processing"
        return item

    def requeue_orphan_commands(self) -> int:
        """Вернуть в очередь команды, застрявшие в `processing`.

        Исполнитель, умерший между захватом команды и её завершением, оставлял
        строку в `processing` навсегда: она больше не выдавалась
        `claim_local_command` и никогда не подтверждалась центру. Все локальные
        команды идемпотентны по построению, повторное исполнение безопасно.

        Если жив ДРУГОЙ исполнитель, не делаем ничего: строка в `processing`
        тогда, скорее всего, не сирота, а команда, которую он прямо сейчас
        выполняет. Вернуть её в очередь — значит начать вторую отмену того же
        процесса параллельно с первой, пока та ждёт свой гарантийный срок.
        Сироты подождут: их разберёт тот, кто останется последним.
        """
        if self.other_executor_alive():
            return 0
        with self.write() as conn:
            cur = conn.execute(
                "UPDATE local_commands SET status = 'pending', claimed_at = NULL "
                "WHERE status = 'processing'"
            )
        return int(cur.rowcount or 0)

    def other_executor_alive(self) -> bool:
        """Есть ли ЖИВОЙ исполнитель, кроме нас."""
        rows = self.read().execute(
            "SELECT executor_instance_id FROM executor_instances WHERE status = 'online'"
        ).fetchall()
        return any(self.executor_alive(row["executor_instance_id"]) for row in rows)

    def release_claim(self, attempt_id: str) -> None:
        """Вернуть захваченную, но НЕ начатую попытку в очередь."""
        with self.write() as conn:
            conn.execute(
                "UPDATE execution_queue SET state = ?, claimed_by_executor = NULL, "
                "lease_expires_at = NULL, updated_at = ? WHERE attempt_id = ? AND state = ?",
                (QUEUE_QUEUED, time.time(), attempt_id, QUEUE_CLAIMED),
            )

    def complete_local_command(
        self, local_command_id: str, result: dict[str, Any], *, status: str = "done"
    ) -> None:
        with self.write() as conn:
            conn.execute(
                "UPDATE local_commands SET status = ?, completed_at = ?, result_json = ? "
                "WHERE local_command_id = ?",
                (status, time.time(), json.dumps(result, ensure_ascii=False),
                 local_command_id),
            )

    def finished_local_commands(self) -> list[dict[str, Any]]:
        """Исполненные команды, чей результат ещё не подтверждён центру."""
        rows = self.read().execute(
            "SELECT * FROM local_commands WHERE status = 'done' "
            "AND central_command_id IS NOT NULL ORDER BY completed_at ASC"
        ).fetchall()
        return [dict(r) for r in rows]

    def mark_command_reported(self, local_command_id: str) -> None:
        with self.write() as conn:
            conn.execute(
                "UPDATE local_commands SET status = 'reported' WHERE local_command_id = ?",
                (local_command_id,),
            )

    def retry_reported_command(self, local_command_id: str) -> None:
        """A repeated central command proves the prior ACK was not committed."""
        with self.write() as conn:
            conn.execute(
                "UPDATE local_commands SET status = 'done' "
                "WHERE local_command_id = ? AND status = 'reported'",
                (local_command_id,),
            )

    def pending_local_command_count(self) -> int:
        row = self.read().execute(
            "SELECT COUNT(*) AS n FROM local_commands WHERE status IN "
            "('pending','processing')"
        ).fetchone()
        return int(row["n"]) if row else 0

    # ─── Реестр процессов ────────────────────────────────────────────────────
    def register_process(
        self,
        *,
        job_id: str,
        attempt_id: str,
        executor_instance_id: str,
        pid: int,
        process_start_identity: Optional[float],
        command_fingerprint: str,
        process_group_id: int,
    ) -> None:
        now = time.time()
        with self.write() as conn:
            conn.execute(
                "INSERT INTO process_registry (attempt_id, job_id, executor_instance_id, "
                "pid, process_start_identity, command_fingerprint, process_group_id, "
                "started_at, last_observed_at, status) VALUES (?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(attempt_id) DO UPDATE SET job_id = excluded.job_id, "
                "executor_instance_id = excluded.executor_instance_id, "
                "pid = excluded.pid, "
                "process_start_identity = excluded.process_start_identity, "
                "command_fingerprint = excluded.command_fingerprint, "
                "process_group_id = excluded.process_group_id, "
                "started_at = excluded.started_at, "
                "last_observed_at = excluded.last_observed_at, "
                "status = excluded.status, exit_code = NULL",
                (attempt_id, job_id, executor_instance_id, pid, process_start_identity,
                 command_fingerprint, process_group_id, now, now, "running"),
            )

    def update_process(self, attempt_id: str, **fields: Any) -> None:
        if not fields:
            return
        fields["last_observed_at"] = time.time()
        assignments = ", ".join(f"{k} = ?" for k in fields)
        with self.write() as conn:
            conn.execute(
                f"UPDATE process_registry SET {assignments} WHERE attempt_id = ?",
                (*fields.values(), attempt_id),
            )

    def process_row(self, attempt_id: str) -> Optional[dict[str, Any]]:
        row = self.read().execute(
            "SELECT * FROM process_registry WHERE attempt_id = ?", (attempt_id,)
        ).fetchone()
        return dict(row) if row else None

    def list_processes(self) -> list[dict[str, Any]]:
        rows = self.read().execute("SELECT * FROM process_registry").fetchall()
        return [dict(r) for r in rows]

    def process_row_any(self) -> Optional[dict[str, Any]]:
        """Любая запись реестра. Нужна наблюдателю, не знающему attempt_id."""
        row = self.read().execute(
            "SELECT * FROM process_registry ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        return dict(row) if row else None

    def live_process_count(self) -> int:
        """Сколько процессов реестра ДОКАЗАННО живы (pid + тик старта)."""
        from audit_worker import process_registry as procinfo

        return sum(
            1
            for row in self.list_processes()
            if row.get("status") == "running"
            and procinfo.is_alive(
                int(row.get("pid") or 0), row.get("process_start_identity")
            )
        )

    def claimed_attempt_count(self) -> int:
        """Сколько попыток локально захвачено (claimed или running)."""
        row = self.read().execute(
            "SELECT COUNT(*) AS n FROM execution_queue WHERE state IN (?, ?)",
            (QUEUE_CLAIMED, QUEUE_RUNNING),
        ).fetchone()
        return int(row["n"]) if row else 0

    # ─── Счётчик событий (межпроцессный, транзакционный) ─────────────────────
    def allocate_event_sequence(
        self,
        *,
        job_id: str,
        attempt_id: str,
        event_id: str,
        event_type: str,
        floor: int = 0,
    ) -> int:
        """Выдать следующий `seq` попытки. Атомарно и межпроцессно.

        Инкремент счётчика и вставка строки журнала — одна транзакция под
        `BEGIN IMMEDIATE`. PRIMARY KEY(job_id, attempt_id, sequence) страхует
        схемой: два процесса физически не могут получить один номер.

        `floor` — уже занятые номера, найденные в файлах журнала. Нужен при
        первом обращении к попытке, созданной прошлой версией воркера (её
        события есть в сегментах, а строки счётчика ещё нет).
        """
        now = time.time()
        with self.write() as conn:
            row = conn.execute(
                "SELECT next_seq FROM event_sequences WHERE job_id = ? AND attempt_id = ?",
                (job_id, attempt_id),
            ).fetchone()
            current = int(row["next_seq"]) if row is not None else 1
            seq = max(current, int(floor or 0) + 1)
            conn.execute(
                "INSERT INTO event_sequences (job_id, attempt_id, next_seq, updated_at) "
                "VALUES (?,?,?,?) ON CONFLICT(job_id, attempt_id) DO UPDATE SET "
                "next_seq = excluded.next_seq, updated_at = excluded.updated_at",
                (job_id, attempt_id, seq + 1, now),
            )
            conn.execute(
                "INSERT INTO event_journal (job_id, attempt_id, sequence, event_id, "
                "event_type, allocated_at, written) VALUES (?,?,?,?,?,?,0)",
                (job_id, attempt_id, seq, event_id, event_type, now),
            )
        return seq

    def mark_event_written(self, *, job_id: str, attempt_id: str, sequence: int) -> None:
        with self.write() as conn:
            conn.execute(
                "UPDATE event_journal SET written = 1 "
                "WHERE job_id = ? AND attempt_id = ? AND sequence = ?",
                (job_id, attempt_id, sequence),
            )

    def raise_event_sequence_floor(
        self, *, job_id: str, attempt_id: str, floor: int
    ) -> int:
        """Поднять счётчик до `floor + 1`, если файлы ушли вперёд базы.

        Опустить счётчик эта функция не может ни при каких данных: номер,
        однажды выданный, переиспользовать нельзя — центр отбросит повтор как
        дубль, и событие исчезнет молча.
        """
        with self.write() as conn:
            row = conn.execute(
                "SELECT next_seq FROM event_sequences WHERE job_id = ? AND attempt_id = ?",
                (job_id, attempt_id),
            ).fetchone()
            current = int(row["next_seq"]) if row is not None else 1
            target = max(current, int(floor or 0) + 1)
            if target != current or row is None:
                conn.execute(
                    "INSERT INTO event_sequences (job_id, attempt_id, next_seq, updated_at) "
                    "VALUES (?,?,?,?) ON CONFLICT(job_id, attempt_id) DO UPDATE SET "
                    "next_seq = excluded.next_seq, updated_at = excluded.updated_at",
                    (job_id, attempt_id, target, time.time()),
                )
        return target - 1

    def allocated_event_high(self, *, job_id: str, attempt_id: str) -> int:
        """Наибольший ВЫДАННЫЙ номер попытки (0, если не выдавали)."""
        row = self.read().execute(
            "SELECT next_seq FROM event_sequences WHERE job_id = ? AND attempt_id = ?",
            (job_id, attempt_id),
        ).fetchone()
        return int(row["next_seq"]) - 1 if row is not None else 0

    def unwritten_events(
        self, *, job_id: str, attempt_id: str, older_than: float = 0.0
    ) -> list[dict[str, Any]]:
        """Номера, которые выданы, но так и не легли в сегмент журнала."""
        rows = self.read().execute(
            "SELECT * FROM event_journal WHERE job_id = ? AND attempt_id = ? "
            "AND written = 0 AND allocated_at <= ? ORDER BY sequence ASC",
            (job_id, attempt_id, time.time() - max(0.0, older_than)),
        ).fetchall()
        return [dict(r) for r in rows]
