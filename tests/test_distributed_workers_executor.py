"""
test_distributed_workers_executor.py
------------------------------------
Инварианты разделения agent/executor — НАСТОЯЩИМИ процессами, а не моками.

Проверяется буквально то, ради чего разделение и делалось:

  I-02  перезапуск сетевого агента НЕ завершает локальный процесс;
  I-03  перезапуск сетевого агента НЕ создаёт второй процесс;
  §8.4  два одновременно запущенных исполнителя не порождают два процесса;
  §8.5  агент не является родителем процесса аудита;
  §8.6  рестарт исполнителя: жив → не трогаем, маркер → доупаковываем,
        ничего → executor_interrupted БЕЗ автоматического повтора;
  §10   отмена уходит только процессу с доказанной принадлежностью.

Тесты помечены `slow`: они поднимают uvicorn и несколько процессов.

Run: python -m pytest tests/test_distributed_workers_executor.py -v
"""
from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

httpx = pytest.importorskip("httpx")

BOOTSTRAP = "test-bootstrap-secret-0123456789abcdef"
PY = sys.executable or "python3"


# ─── Вспомогательное ─────────────────────────────────────────────────────────
def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait(predicate, *, timeout=25.0, interval=0.15, message="условие не наступило"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    raise AssertionError(message)


def _worker_env(root: Path, *, url: str | None = None) -> dict[str, str]:
    env = dict(os.environ)
    env["AUDIT_WORKER_ROOT"] = str(root)
    env["PYTHONPATH"] = str(_ROOT)
    env["AUDIT_WORKER_ALLOW_INSECURE_LOCALHOST"] = "true"
    env["AUDIT_WORKER_HEARTBEAT_SEC"] = "2"
    env["AUDIT_WORKER_POLL_WAIT_SEC"] = "1"
    env["AUDIT_WORKER_TEST_MAX_SEC"] = "120"
    if url:
        env["AUDIT_WORKER_DISPATCHER_URL"] = url
    else:
        env.pop("AUDIT_WORKER_DISPATCHER_URL", None)
    return env


def _spawn_executor(root: Path, **extra_env) -> subprocess.Popen:
    env = _worker_env(root)
    env.update({k: str(v) for k, v in extra_env.items()})
    return subprocess.Popen(  # noqa: S603 — фиксированный argv
        [PY, "-m", "audit_worker", "executor", "--root", str(root)],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )


def _stop(process: subprocess.Popen | None, *, sig=signal.SIGTERM) -> None:
    if process is None or process.poll() is not None:
        return
    try:
        process.send_signal(sig)
        process.wait(timeout=10)
    except Exception:  # noqa: BLE001 — в тесте важно только не оставить хвост
        process.kill()


def _seed_attempt(root: Path, *, steps=40, step_seconds=0.15):
    """Положить попытку в локальную очередь так же, как это делает агент."""
    from audit_worker.config import WorkerConfig
    from audit_worker.local_db import LocalDB
    from audit_worker.local_store import LocalJobStore

    config = WorkerConfig(dispatcher_url="http://center", root=root, display_name="t")
    config.ensure_dirs()
    job_id, attempt_id = str(uuid.uuid4()), str(uuid.uuid4())
    store = LocalJobStore(config.jobs_dir)
    store.create({
        "job_id": job_id, "attempt_id": attempt_id, "job_type": "test_pipeline_v1",
        "project_id": "ЖК «Событие 6.2» / корпус 3", "version_id": None,
        "execution_token": "etk_test", "params": {}, "package": {"manifest_version": 1},
    })
    db = LocalDB(config.local_db_path)
    db.enqueue(
        job_id=job_id, attempt_id=attempt_id, job_type="test_pipeline_v1",
        params={"label": "slow", "steps": steps, "step_seconds": step_seconds,
                "result_bytes": 1024},
    )
    return config, db, job_id, attempt_id


def _alive(pid: int) -> bool:
    from audit_worker.process_registry import process_start_time

    return process_start_time(pid) is not None


# ─── §8.4 Два исполнителя — один процесс ─────────────────────────────────────
@pytest.mark.slow
def test_two_executors_never_start_two_processes(tmp_path):
    """Два НАСТОЯЩИХ исполнителя на одной worker.db дают ровно один процесс."""
    root = tmp_path / "worker"
    config, db, job_id, attempt_id = _seed_attempt(root, steps=12, step_seconds=0.2)

    first = _spawn_executor(root)
    second = _spawn_executor(root)
    try:
        row = _wait(lambda: db.process_row(attempt_id),
                    message="процесс так и не зарегистрировался")
        # Захват достался ровно одному исполнителю.
        item = db.queue_item(attempt_id)
        assert item["claim_generation"] == 1, "попытку захватили дважды"
        owner = item["claimed_by_executor"]
        assert owner and row["executor_instance_id"] == owner

        # Второй исполнитель ЖИВ, но работу не подхватил.
        assert second.poll() is None or first.poll() is None
        instances = db.read().execute(
            "SELECT COUNT(*) AS n FROM executor_instances"
        ).fetchone()["n"]
        assert instances == 2, "оба исполнителя должны были зарегистрироваться"
        assert len(db.list_processes()) == 1, "запущено больше одного процесса"

        _wait(lambda: db.queue_item(attempt_id)["state"] == "finished",
              timeout=40, message="работа не завершилась")
        assert db.queue_item(attempt_id)["claim_generation"] == 1
    finally:
        _stop(first)
        _stop(second)


# ─── §8.6 Рестарт исполнителя ────────────────────────────────────────────────
@pytest.mark.slow
def test_executor_restart_does_not_duplicate_running_process(tmp_path):
    """Живой процесс переживает рестарт исполнителя, второй не появляется."""
    root = tmp_path / "worker"
    config, db, job_id, attempt_id = _seed_attempt(root, steps=40, step_seconds=0.25)

    first = _spawn_executor(root)
    try:
        row = _wait(lambda: db.process_row(attempt_id), message="процесс не стартовал")
        pid = int(row["pid"])
        assert _alive(pid)

        # Исполнитель уходит, процесс аудита остаётся.
        _stop(first, sig=signal.SIGKILL)
        time.sleep(0.5)
        assert _alive(pid), "процесс аудита не пережил смерть исполнителя"

        second = _spawn_executor(root)
        try:
            time.sleep(2.0)
            # Второго процесса нет: реестр по-прежнему знает один pid.
            processes = db.list_processes()
            assert len(processes) == 1
            assert int(processes[0]["pid"]) == pid
            _wait(lambda: db.queue_item(attempt_id)["state"] == "finished",
                  timeout=40, message="работа не была доведена до конца")
        finally:
            _stop(second)
    finally:
        _stop(first, sig=signal.SIGKILL)


@pytest.mark.slow
def test_executor_marks_interrupted_without_marker_and_never_retries(tmp_path):
    """Процесс исчез без маркера → executor_interrupted, автоповтора нет."""
    root = tmp_path / "worker"
    config, db, job_id, attempt_id = _seed_attempt(root, steps=40, step_seconds=0.25)

    first = _spawn_executor(root)
    try:
        row = _wait(lambda: db.process_row(attempt_id), message="процесс не стартовал")
        pid = int(row["pid"])
        _stop(first, sig=signal.SIGKILL)
        # Убиваем процесс аудита ЖЁСТКО: маркера завершения не будет.
        os.kill(pid, signal.SIGKILL)
        _wait(lambda: not _alive(pid), message="процесс не умер")
    finally:
        _stop(first, sig=signal.SIGKILL)

    second = _spawn_executor(root)
    try:
        _wait(
            lambda: db.queue_item(attempt_id)["state"] == "executor_interrupted",
            message="прерывание не распознано",
        )
        time.sleep(2.0)
        # Автоматического повтора НЕТ: новый процесс не запускался.
        assert db.queue_item(attempt_id)["state"] == "executor_interrupted"
        row = db.process_row(attempt_id)
        assert int(row["pid"]) == pid and row["status"] == "interrupted"
    finally:
        _stop(second)


@pytest.mark.slow
def test_executor_packages_result_finished_before_restart(tmp_path):
    """Процесс отработал до рестарта — архив собирается, работа не теряется."""
    root = tmp_path / "worker"
    config, db, job_id, attempt_id = _seed_attempt(root, steps=2, step_seconds=0.05)

    # Имитируем окно «процесс отработал, архив не собран»: маркер есть,
    # очередь в running, реестр указывает на мёртвый pid.
    from audit_worker.executor import write_completed_marker
    from audit_worker.test_runner import RunOutcome

    job_dir = config.job_dir(job_id, attempt_id)
    (job_dir / "result").mkdir(parents=True, exist_ok=True)
    (job_dir / "result" / "summary.json").write_text("{}", encoding="utf-8")
    (job_dir / "result" / "run_log.txt").write_text("ok\n", encoding="utf-8")
    write_completed_marker(job_dir, RunOutcome(
        exit_code=0, duration_sec=0.1, steps_done=2, steps_total=2))
    db.set_queue_state(attempt_id, "running")
    db.register_process(
        job_id=job_id, attempt_id=attempt_id, executor_instance_id="exe_dead",
        pid=999_999, process_start_identity=1.0,
        command_fingerprint="deadbeef", process_group_id=0,
    )

    executor = _spawn_executor(root)
    try:
        _wait(lambda: db.queue_item(attempt_id)["state"] == "finished",
              message="результат не собран после рестарта")
        from audit_worker.local_store import LocalJobStore

        meta = LocalJobStore(config.jobs_dir).load(job_id, attempt_id)
        assert meta["local_state"] == "completed_locally"
        assert meta["result_hash"]
        assert (job_dir / "result" / f"{attempt_id}.tar.gz").is_file()
    finally:
        _stop(executor)


# ─── §10 Безопасная отмена ───────────────────────────────────────────────────
@pytest.mark.slow
def test_cancel_terminates_only_verified_process(tmp_path):
    """Отмена бьёт по проверенной группе и только по ней."""
    root = tmp_path / "worker"
    config, db, job_id, attempt_id = _seed_attempt(root, steps=60, step_seconds=0.3)

    executor = _spawn_executor(root)
    try:
        row = _wait(lambda: db.process_row(attempt_id), message="процесс не стартовал")
        pid = int(row["pid"])
        db.enqueue_local_command(
            command_type="cancel_attempt", job_id=job_id, attempt_id=attempt_id,
            payload={"job_id": job_id, "attempt_id": attempt_id,
                     "grace_period_sec": 3},
            central_command_id="cmd-1",
        )
        _wait(lambda: db.local_command_by_central("cmd-1")["status"] == "done",
              message="команда отмены не исполнена")
        result = json.loads(db.local_command_by_central("cmd-1")["result_json"])
        assert result["status"] == "ok"
        assert result["detail"]["outcome"] == "cancelled"
        assert not _alive(pid), "процесс аудита не остановлен"
        assert db.queue_item(attempt_id)["state"] == "cancelled"
    finally:
        _stop(executor)


def test_cancel_refuses_when_fingerprint_does_not_match(tmp_path):
    """Отпечаток из второго источника не сошёлся → ЧУЖОЙ процесс не трогаем (I-17)."""
    from audit_worker.executor import Executor
    from audit_worker.local_store import LocalJobStore
    from audit_worker.process_control import OUTCOME_OWNERSHIP_MISMATCH
    from audit_worker.process_registry import process_start_time

    root = tmp_path / "worker"
    config, db, job_id, attempt_id = _seed_attempt(root, steps=1, step_seconds=0.0)
    # Процесс ЖИВОЙ и время старта настоящее (берём собственный pid), но
    # отпечаток команды в metadata.json другой: значит это не наш аудит.
    db.set_queue_state(attempt_id, "running")
    db.register_process(
        job_id=job_id, attempt_id=attempt_id, executor_instance_id="exe_x",
        pid=os.getpid(), process_start_identity=process_start_time(os.getpid()),
        command_fingerprint="ffff", process_group_id=os.getpgrp(),
    )
    LocalJobStore(config.jobs_dir).update(
        job_id, attempt_id, command_fingerprint="0000")

    executor = Executor(config, db=db)
    try:
        outcome = executor.cancel_attempt(job_id=job_id, attempt_id=attempt_id)
    finally:
        executor.shutdown()
    assert outcome["status"] == "error"
    assert outcome["detail"]["outcome"] == OUTCOME_OWNERSHIP_MISMATCH
    # Мы всё ещё живы — сигнал не ушёл.
    assert _alive(os.getpid())


def test_cancel_of_dead_pid_reports_not_running(tmp_path):
    """Записанный процесс мёртв (или это тёзка по pid) — сигналов не шлём."""
    from audit_worker.executor import Executor
    from audit_worker.process_control import OUTCOME_NOT_RUNNING

    root = tmp_path / "worker"
    config, db, job_id, attempt_id = _seed_attempt(root, steps=1, step_seconds=0.0)
    db.set_queue_state(attempt_id, "running")
    db.register_process(
        job_id=job_id, attempt_id=attempt_id, executor_instance_id="exe_x",
        pid=os.getpid(), process_start_identity=1.0,   # тик старта не совпадёт
        command_fingerprint="ffff", process_group_id=os.getpgrp(),
    )
    executor = Executor(config, db=db)
    try:
        outcome = executor.cancel_attempt(job_id=job_id, attempt_id=attempt_id)
    finally:
        executor.shutdown()
    assert outcome["detail"]["outcome"] == OUTCOME_NOT_RUNNING
    assert _alive(os.getpid())


def test_cancel_of_already_finished_attempt_keeps_result(tmp_path):
    from audit_worker.executor import Executor
    from audit_worker.process_control import OUTCOME_ALREADY_COMPLETED

    root = tmp_path / "worker"
    config, db, job_id, attempt_id = _seed_attempt(root, steps=1, step_seconds=0.0)
    db.set_queue_state(attempt_id, "finished", result={"outcome": "finished"})
    executor = Executor(config, db=db)
    try:
        outcome = executor.cancel_attempt(job_id=job_id, attempt_id=attempt_id)
    finally:
        executor.shutdown()
    assert outcome["detail"]["outcome"] == OUTCOME_ALREADY_COMPLETED
    assert db.queue_item(attempt_id)["state"] == "finished"


def test_cancel_of_unknown_attempt_is_safe(tmp_path):
    from audit_worker.executor import Executor
    from audit_worker.process_control import OUTCOME_NOT_RUNNING

    root = tmp_path / "worker"
    config, db, job_id, attempt_id = _seed_attempt(root, steps=1, step_seconds=0.0)
    executor = Executor(config, db=db)
    try:
        outcome = executor.cancel_attempt(
            job_id=str(uuid.uuid4()), attempt_id=str(uuid.uuid4()))
    finally:
        executor.shutdown()
    assert outcome["detail"]["outcome"] == OUTCOME_NOT_RUNNING


def _code_only(path: Path) -> str:
    """Исходник без комментариев и строковых литералов.

    Проверять надо КОД, а не пояснения: в комментарии слово «pkill» стоит ровно
    затем, чтобы объяснить, почему его здесь нет.
    """
    import io
    import tokenize

    pieces = []
    with path.open("rb") as fh:
        for token in tokenize.tokenize(fh.readline):
            if token.type in (tokenize.COMMENT, tokenize.STRING):
                continue
            pieces.append(token.string)
    return " ".join(pieces)


def test_no_pkill_or_killall_anywhere():
    """Поиск процесса по имени команды запрещён: попадём в чужой (§10)."""
    # `platform.system()` — про имя ОС, а не про запуск процесса, поэтому
    # ищем именно опасные вызовы, а не подстроку «system».
    banned = ("pkill", "killall", "pgrep", "os . system", "os . popen",
              "subprocess . getoutput", "check_output")
    offenders = []
    for path in sorted((_ROOT / "audit_worker").glob("*.py")):
        code = _code_only(path)
        for marker in banned:
            if marker in code:
                offenders.append(f"{path.name}: {marker}")
    assert not offenders, offenders


def test_agent_module_has_no_process_signalling():
    """У сетевого агента нет кода, шлющего сигналы процессам."""
    code = _code_only(_ROOT / "audit_worker" / "agent.py")
    for marker in ("kill", "killpg", "SIGTERM", "SIGKILL", "terminate_job",
                   "process_control", "test_runner"):
        assert marker not in code, f"агент не должен уметь {marker}"


def test_executor_never_reads_worker_token():
    """Исполнителю сетевые секреты не положены (§8.2)."""
    code = _code_only(_ROOT / "audit_worker" / "executor.py")
    for marker in ("read_token", "WorkerStateStore", "CenterClient", "httpx",
                   "requests", "urllib"):
        assert marker not in code, f"исполнитель не должен знать {marker}"


# ─── I-02 / I-03: рестарт СЕТЕВОГО агента ────────────────────────────────────
@pytest.fixture()
def live_center(tmp_path):
    """Настоящий uvicorn с центром на свободном порту."""
    port = _free_port()
    env = dict(os.environ)
    env.update({
        "PYTHONPATH": str(_ROOT),
        "DISTRIBUTED_WORKERS_ENABLED": "true",
        "DISTRIBUTED_WORKERS_DATA_DIR": str(tmp_path / "center"),
        "DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET": BOOTSTRAP,
        "DISTRIBUTED_WORKERS_LONG_POLL_SEC": "2",
        "DISTRIBUTED_WORKERS_UPLOAD_CHUNK_BYTES": "65536",
        "PORTAL_AUTH_ENABLED": "false",
        "DISTRIBUTED_WORKERS_ALLOW_INSECURE_ADMIN": "true",
    })
    process = subprocess.Popen(  # noqa: S603
        [PY, "-m", "uvicorn", "tests.worker_center_app:app",
         "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
        cwd=str(_ROOT), env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True,
    )
    url = f"http://127.0.0.1:{port}"
    try:
        _wait(
            lambda: _ping(url),
            timeout=30,
            message=f"центр не поднялся на {url}",
        )
        yield url
    finally:
        _stop(process)


def _ping(url: str) -> bool:
    try:
        return httpx.get(f"{url}/api/workers/status", timeout=1.0).status_code == 200
    except Exception:  # noqa: BLE001 — ещё не поднялся
        return False


@pytest.mark.slow
def test_killing_agent_does_not_stop_the_audit(tmp_path, live_center):
    """I-02/I-03 на настоящих процессах: центр + агент + исполнитель."""
    from audit_worker.local_db import LocalDB

    root = tmp_path / "worker"
    root.mkdir(parents=True, exist_ok=True)
    env = _worker_env(root, url=live_center)
    admin = httpx.Client(
        base_url=live_center, timeout=10.0,
        headers={"X-Requested-With": "audit-workers"},
    )

    # 1. Регистрация настоящим агентом, 2. одобрение, 3. получение токена.
    register = subprocess.run(  # noqa: S603
        [PY, "-m", "audit_worker", "register", "--root", str(root),
         "--bootstrap-secret", BOOTSTRAP],
        env=env, capture_output=True, text=True, cwd=str(_ROOT),
    )
    assert register.returncode == 0, register.stderr
    worker_id = json.loads(register.stdout)["worker_id"]
    assert admin.post(f"/api/workers/{worker_id}/approve",
                      json={"configured_max_slots": 1}).status_code == 200
    claim = subprocess.run(  # noqa: S603
        [PY, "-m", "audit_worker", "register", "--root", str(root)],
        env=env, capture_output=True, text=True, cwd=str(_ROOT),
    )
    assert claim.returncode == 0, claim.stderr

    executor = _spawn_executor(root)
    agent = subprocess.Popen(  # noqa: S603
        [PY, "-m", "audit_worker", "agent", "--root", str(root), "--max-jobs", "1"],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        cwd=str(_ROOT),
    )
    db = LocalDB(root / "worker.db")
    try:
        # 4. Оператор выдаёт длинное задание.
        created = admin.post("/api/workers/jobs", json={
            "worker_id": worker_id,
            "project_id": "13АВ/РД-АР3-К7",
            "params": {"label": "long", "steps": 40, "step_seconds": 0.3,
                       "result_bytes": 1024},
        })
        assert created.status_code == 200, created.text
        job_id = created.json()["job"]["job_id"]

        # 5. Исполнитель запустил процесс; агент ему НЕ родитель.
        row = _wait(lambda: db.process_row_any(), timeout=40,
                    message="исполнитель не запустил процесс")
        pid = int(row["pid"])
        assert os.getpgid(pid) != os.getpgid(agent.pid), (
            "процесс аудита оказался в группе агента"
        )
        assert _ppid(pid) != agent.pid, "агент оказался родителем процесса аудита"

        events_dir = (
            root / "jobs" / row["job_id"] / row["attempt_id"] / "events"
        )
        before = _outbox_size(events_dir)

        # 6. Убиваем АГЕНТА жёстко.
        agent.send_signal(signal.SIGKILL)
        agent.wait(timeout=10)
        time.sleep(1.5)

        # 7. Исполнитель и процесс аудита живы, журнал растёт.
        assert executor.poll() is None, "исполнитель умер вместе с агентом"
        assert _alive(pid), "процесс аудита умер вместе с агентом"
        _wait(lambda: _outbox_size(events_dir) > before, timeout=20,
              message="прогресс перестал писаться")

        # 8. Агент поднимается заново — второго процесса не появляется.
        agent2 = subprocess.Popen(  # noqa: S603
            [PY, "-m", "audit_worker", "agent", "--root", str(root),
             "--max-jobs", "1"],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            cwd=str(_ROOT),
        )
        try:
            time.sleep(3.0)
            assert len(db.list_processes()) == 1, "появился второй процесс аудита"
            assert int(db.list_processes()[0]["pid"]) == pid

            # 9. Работа доводится до конца и результат принимается центром.
            _wait(
                lambda: admin.get(f"/api/workers/jobs/{job_id}").json()["job"]["state"]
                == "completed",
                timeout=90,
                message="центр так и не принял результат",
            )
        finally:
            _stop(agent2)
    finally:
        _stop(agent, sig=signal.SIGKILL)
        _stop(executor)
        admin.close()


def _ppid(pid: int) -> int:
    try:
        with open(f"/proc/{pid}/stat", "rb") as fh:
            data = fh.read().decode("utf-8", "replace")
        return int(data[data.rindex(")") + 2:].split()[1])
    except (OSError, ValueError, IndexError):
        return -1


def _outbox_size(events_dir: Path) -> int:
    if not events_dir.is_dir():
        return 0
    return sum(p.stat().st_size for p in events_dir.glob("outbox-*.jsonl"))
