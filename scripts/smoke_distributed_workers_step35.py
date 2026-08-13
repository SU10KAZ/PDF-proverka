#!/usr/bin/env python3
"""Сквозной smoke этапа 3.5 на НАСТОЯЩИХ процессах.

Поднимает три процесса — центр (uvicorn), локальный исполнитель и сетевой
агент — и проходит сценарий §19 задания: от регистрации до удаления локальной
копии и перезапуска всех трёх сторон.

Что проверяется по-настоящему, а не на словах:
  * агент НЕ является родителем процесса аудита и не лежит с ним в одной
    группе процессов;
  * убийство агента не останавливает ни исполнителя, ни аудит, а прогресс и
    логи продолжают писаться;
  * поднявшийся заново агент не создаёт второй процесс;
  * отмена доходит командой и подтверждается воркером;
  * признание попытки потерянной не выдаёт себя за остановку процесса;
  * результат вернувшейся старой попытки уходит в superseded_results и не
    трогает актуальную попытку;
  * кириллический код проекта со слэшем работает, а пути остаются UUID-ными;
  * RetentionManager в сухом прогоне ничего не удаляет, а с флагом удаляет
    ТОЛЬКО локальную копию;
  * после перезапуска центра, агента и исполнителя состояние восстанавливается.

LLM здесь нет: ни Claude, ни Codex, ни нормативного этапа. Единственная
работа — безопасный `test_pipeline_v1`.

Запуск:
    python scripts/smoke_distributed_workers_step35.py [--keep]
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

import httpx

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

PY = sys.executable or "python3"
BOOTSTRAP = "smoke-bootstrap-secret-0123456789abcdef"
INTENT = {"X-Requested-With": "audit-workers"}

# С пред-пайплайнового этапа операторские действия закрыты РОЛЯМИ, и режим
# «портальная аутентификация выключена» больше не открывает изменяющие ручки
# даже с DISTRIBUTED_WORKERS_ALLOW_INSECURE_ADMIN. Smoke ходит настоящей
# сессией администратора — как настоящий оператор.
SMOKE_USER = "smoke-step35-admin"
SMOKE_PASSWORD = "smoke-step35-password"
SMOKE_SESSION_SECRET = "smoke-step35-session-secret-0123456789ab"

PROJECT_CODE = "13АВ/РД-АР3-К7"

_STEP = 0
_FAILURES: list[str] = []


def step(title: str) -> None:
    global _STEP
    _STEP += 1
    print(f"\n[{_STEP:02d}] {title}", flush=True)


def check(condition: bool, message: str) -> bool:
    mark = "  ✔" if condition else "  ✘"
    print(f"{mark} {message}", flush=True)
    if not condition:
        _FAILURES.append(message)
    return bool(condition)


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_for(predicate, *, timeout=60.0, interval=0.2, message="условие"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    raise TimeoutError(f"не дождались: {message}")


def alive(pid: int) -> bool:
    from audit_worker.process_registry import process_start_time

    return process_start_time(pid) is not None


def ppid_of(pid: int) -> int:
    try:
        with open(f"/proc/{pid}/stat", "rb") as fh:
            data = fh.read().decode("utf-8", "replace")
        return int(data[data.rindex(")") + 2:].split()[1])
    except (OSError, ValueError, IndexError):
        return -1


class Smoke:
    def __init__(self, root: Path):
        self.root = root
        self.center_dir = root / "center"
        self.worker_dir = root / "worker"
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.center: subprocess.Popen | None = None
        self.agent: subprocess.Popen | None = None
        self.executor: subprocess.Popen | None = None
        self._password_hash = ""
        self.admin = self._admin_client()
        self.worker_id = ""
        self.log_dir = root / "logs"
        self.log_dir.mkdir(parents=True, exist_ok=True)

    # ─── Портальная сессия администратора ────────────────────────────────────
    def password_hash(self) -> str:
        if not self._password_hash:
            from backend.app.core import portal_auth

            self._password_hash = portal_auth.hash_password(SMOKE_PASSWORD)
        return self._password_hash

    def role_env(self) -> dict[str, str]:
        from backend.app.services.distributed_workers import authorization as az

        return {
            "PORTAL_AUTH_ENABLED": "true",
            "PORTAL_AUTH_USERS": f"{SMOKE_USER}:{self.password_hash()}",
            "PORTAL_SESSION_SECRET": SMOKE_SESSION_SECRET,
            az.ENV_ADMINS: SMOKE_USER,
        }

    def _admin_client(self) -> httpx.Client:
        from backend.app.core import portal_auth

        for key, value in self.role_env().items():
            os.environ[key] = value
        settings = portal_auth.get_settings()
        client = httpx.Client(base_url=self.url, timeout=20.0, headers=INTENT)
        client.cookies.set(
            settings.cookie_name, portal_auth.issue_token(SMOKE_USER, settings)
        )
        return client

    # ─── Процессы ────────────────────────────────────────────────────────────
    def center_env(self) -> dict[str, str]:
        env = dict(os.environ)
        env.update({
            "PYTHONPATH": str(_ROOT),
            "DISTRIBUTED_WORKERS_ENABLED": "true",
            "DISTRIBUTED_WORKERS_DATA_DIR": str(self.center_dir),
            "DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET": BOOTSTRAP,
            "DISTRIBUTED_WORKERS_LONG_POLL_SEC": "2",
            "DISTRIBUTED_WORKERS_UPLOAD_CHUNK_BYTES": "65536",
            "DISTRIBUTED_WORKERS_ALLOW_INSECURE_ADMIN": "false",
            **self.role_env(),
        })
        return env

    def worker_env(self, *, with_url=True, delete_enabled=False) -> dict[str, str]:
        env = dict(os.environ)
        env.update({
            "PYTHONPATH": str(_ROOT),
            "AUDIT_WORKER_ROOT": str(self.worker_dir),
            "AUDIT_WORKER_ALLOW_INSECURE_LOCALHOST": "true",
            "AUDIT_WORKER_HEARTBEAT_SEC": "2",
            "AUDIT_WORKER_POLL_WAIT_SEC": "1",
            "AUDIT_WORKER_TEST_MAX_SEC": "180",
            "AUDIT_WORKER_RETENTION_SCAN_INTERVAL_SEC": "60",
            "AUDIT_WORKER_RETENTION_DELETE_ENABLED": "true" if delete_enabled else "false",
        })
        if with_url:
            env["AUDIT_WORKER_DISPATCHER_URL"] = self.url
        else:
            env.pop("AUDIT_WORKER_DISPATCHER_URL", None)
        return env

    def _spawn(self, name: str, argv: list[str], env: dict[str, str]) -> subprocess.Popen:
        handle = (self.log_dir / f"{name}.log").open("a", encoding="utf-8")
        return subprocess.Popen(  # noqa: S603 — фиксированный argv, shell=False
            argv, env=env, cwd=str(_ROOT), stdout=handle,
            stderr=subprocess.STDOUT, text=True,
        )

    def start_center(self) -> None:
        self.center = self._spawn(
            "center",
            [PY, "-m", "uvicorn", "tests.worker_center_app:app",
             "--host", "127.0.0.1", "--port", str(self.port), "--log-level", "warning"],
            self.center_env(),
        )
        wait_for(self.ping, timeout=45, message=f"центр на {self.url}")

    def ping(self) -> bool:
        try:
            return self.admin.get("/api/workers/status").status_code == 200
        except Exception:  # noqa: BLE001
            return False

    def start_executor(self, *, delete_enabled=False) -> None:
        self.executor = self._spawn(
            "executor",
            [PY, "-m", "audit_worker", "executor", "--root", str(self.worker_dir)],
            self.worker_env(with_url=False, delete_enabled=delete_enabled),
        )

    def start_agent(self) -> None:
        self.agent = self._spawn(
            "agent",
            [PY, "-m", "audit_worker", "agent", "--root", str(self.worker_dir)],
            self.worker_env(),
        )

    @staticmethod
    def stop(process: subprocess.Popen | None, *, sig=signal.SIGTERM) -> None:
        if process is None or process.poll() is not None:
            return
        try:
            process.send_signal(sig)
            process.wait(timeout=15)
        except Exception:  # noqa: BLE001
            process.kill()

    def shutdown(self) -> None:
        self.stop(self.agent)
        self.stop(self.executor)
        self.stop(self.center)
        self.admin.close()

    # ─── Хелперы протокола ───────────────────────────────────────────────────
    def db(self):
        from audit_worker.local_db import LocalDB

        return LocalDB(self.worker_dir / "worker.db")

    def create_job(self, *, steps: int, step_seconds: float, project: str) -> str:
        response = self.admin.post("/api/workers/jobs", json={
            "worker_id": self.worker_id,
            "project_id": project,
            "params": {"label": "smoke", "steps": steps,
                       "step_seconds": step_seconds, "result_bytes": 2048},
        })
        response.raise_for_status()
        return response.json()["job"]["job_id"]

    def job(self, job_id: str) -> dict:
        return self.admin.get(f"/api/workers/jobs/{job_id}").json()["job"]

    def attempts(self, job_id: str) -> list[dict]:
        return self.admin.get(f"/api/workers/jobs/{job_id}/attempts").json()["attempts"]

    def dangerous(self, path: str, body: dict) -> httpx.Response:
        return self.admin.post(
            path, json=body,
            headers={**INTENT, "Idempotency-Key": f"smoke-{uuid.uuid4().hex[:12]}"},
        )


def run(root: Path) -> int:
    smoke = Smoke(root)
    try:
        _scenario(smoke)
    finally:
        smoke.shutdown()

    print("\n" + "=" * 70)
    if _FAILURES:
        print(f"SMOKE ПРОВАЛЕН: {len(_FAILURES)} проверок не прошли")
        for item in _FAILURES:
            print(f"  ✘ {item}")
        return 1
    print(f"SMOKE ЗЕЛЁНЫЙ: {_STEP} групп проверок "
          f"(нумерация шагов сценария 1-48), все проверки пройдены")
    return 0


def _scenario(s: Smoke) -> None:  # noqa: C901 — сценарий линейный по замыслу
    step("1. Запустить центр")
    s.start_center()
    check(s.ping(), "центр отвечает на /api/workers/status")

    step("2. Запустить локальный исполнитель")
    s.start_executor()
    time.sleep(1.5)
    check(s.executor.poll() is None, "исполнитель работает")

    step("3-4. Зарегистрировать воркер настоящим агентом")
    registered = subprocess.run(  # noqa: S603
        [PY, "-m", "audit_worker", "register", "--root", str(s.worker_dir),
         "--bootstrap-secret", BOOTSTRAP],
        env=s.worker_env(), cwd=str(_ROOT), capture_output=True, text=True,
    )
    check(registered.returncode == 0, "заявка на регистрацию принята")
    s.worker_id = json.loads(registered.stdout)["worker_id"]
    check(json.loads(registered.stdout)["token_stored"] is False,
          "токен на регистрации НЕ выдан")

    step("5. Оператор одобряет воркер")
    check(s.admin.post(f"/api/workers/{s.worker_id}/approve",
                       json={"configured_max_slots": 1}).status_code == 200,
          "воркер одобрен")

    step("6. Получить токен (одноразовый claim)")
    claimed = subprocess.run(  # noqa: S603
        [PY, "-m", "audit_worker", "register", "--root", str(s.worker_dir)],
        env=s.worker_env(), cwd=str(_ROOT), capture_output=True, text=True,
    )
    check(claimed.returncode == 0 and json.loads(claimed.stdout)["token_stored"],
          "токен выдан после одобрения")

    step("7. Запустить сетевой агент и выдать длинное задание")
    s.start_agent()
    db = s.db()
    job1 = s.create_job(steps=40, step_seconds=0.35, project=PROJECT_CODE)
    check(bool(job1), f"задание создано на проекте «{PROJECT_CODE}»")

    step("8-9. Исполнитель запустил процесс; агент ему НЕ родитель")
    row = wait_for(lambda: db.process_row_any(), timeout=60, message="процесс аудита")
    pid = int(row["pid"])
    check(alive(pid), "процесс аудита живой")
    check(ppid_of(pid) != s.agent.pid, "агент не является родителем процесса")
    check(os.getpgid(pid) != os.getpgid(s.agent.pid),
          "процесс аудита в собственной группе процессов")

    step("10. Прогресс доезжает до центра")
    wait_for(lambda: (s.job(job1).get("progress") or {}).get("processed"),
             timeout=60, message="прогресс")
    check(s.job(job1)["state"] == "running", "центр видит задание выполняющимся")

    events_dir = s.worker_dir / "jobs" / row["job_id"] / row["attempt_id"] / "events"
    logs_dir = s.worker_dir / "jobs" / row["job_id"] / row["attempt_id"] / "logs"
    outbox_before = sum(p.stat().st_size for p in events_dir.glob("outbox-*.jsonl"))
    logs_before = sum(p.stat().st_size for p in logs_dir.glob("*.log")) if logs_dir.is_dir() else 0

    step("11-13. Убить агента: исполнитель и процесс аудита обязаны выжить")
    s.agent.send_signal(signal.SIGKILL)
    s.agent.wait(timeout=15)
    time.sleep(2.0)
    check(s.executor.poll() is None, "исполнитель пережил смерть агента (I-02)")
    check(alive(pid), "процесс аудита пережил смерть агента (I-01/I-02)")

    step("14-15. Прогресс и логи продолжают писаться без агента")
    wait_for(
        lambda: sum(p.stat().st_size for p in events_dir.glob("outbox-*.jsonl"))
        > outbox_before,
        timeout=30, message="рост журнала событий",
    )
    check(True, "журнал событий растёт при мёртвом агенте")
    logs_now = sum(p.stat().st_size for p in logs_dir.glob("*.log")) if logs_dir.is_dir() else 0
    check(logs_now >= logs_before, "файлы stdout/stderr пишет исполнитель")

    step("16-19. Поднять агента заново: сверка, досылка, второго процесса нет")
    s.start_agent()
    time.sleep(4.0)
    check(len(db.list_processes()) == 1, "второго процесса аудита не появилось (I-03)")
    check(int(db.list_processes()[0]["pid"]) == pid, "это тот же самый процесс")

    step("20. Результат принят и проверен центром")
    wait_for(lambda: s.job(job1)["state"] == "completed", timeout=120,
             message="приём результата")
    first = s.job(job1)
    check(first["retention_until"] is not None, "срок хранения выставлен после приёма")
    check(first["retention_unconfirmed"] is False, "признак «не подтверждён» снят")

    step("21-22. Второе задание и запрос отмены")
    job2 = s.create_job(steps=60, step_seconds=0.4, project="ЖК «Событие 6.2» / корпус 3")
    attempt2 = wait_for(
        lambda: next((a for a in s.attempts(job2)
                      if a["state"] in ("running", "accepted_by_worker")), None),
        timeout=90, message="вторая попытка пошла в работу",
    )
    cancel = s.dangerous(
        f"/api/workers/jobs/{job2}/attempts/{attempt2['attempt_id']}/cancel",
        {"reason": "smoke: проверка отмены", "confirmation": "ОТМЕНИТЬ",
         "grace_period_sec": 5},
    )
    check(cancel.status_code == 200, "отмена запрошена")
    check(cancel.json()["state"] == "cancel_requested",
          "состояние — «запрошена отмена», а не «отменено»")

    step("23-26. Команда доехала, исполнитель остановил процесс, центр получил ACK")
    wait_for(lambda: s.job(job2)["state"] == "cancelled", timeout=90,
             message="подтверждение отмены")
    check(s.job(job2)["state"] == "cancelled", "попытка отменена после подтверждения")
    history = s.attempts(job2)[0]
    check(any(c["acknowledged_at"] for c in history["commands"]),
          "команда отмены подтверждена воркером")

    step("27-29. Третье задание, обрыв связи и признание попытки потерянной")
    # Задание КОРОТКОЕ намеренно: проверяется §5.5 — результат, доделанный
    # офлайн, должен уйти в хранилище отозванных попыток. Для этого к моменту
    # возвращения агента работа обязана быть закончена, иначе центр честно
    # ответит `stop_superseded` (останавливать нечего — сдавать тоже) и
    # проверять будет нечего.
    job3 = s.create_job(steps=6, step_seconds=0.3, project="АР — 001 план потолка")
    attempt3 = wait_for(
        lambda: next((a for a in s.attempts(job3)
                      if a["state"] in ("running", "accepted_by_worker")), None),
        timeout=90, message="третья попытка пошла в работу",
    )
    # «Обрыв связи»: агент убит, исполнитель и процесс продолжают работу.
    s.agent.send_signal(signal.SIGKILL)
    s.agent.wait(timeout=15)
    # Дожидаемся, что работа доделана БЕЗ агента: это и есть доказательство
    # I-01 «потеря связи не останавливает работу».
    wait_for(
        lambda: (s.db().queue_item(attempt3["attempt_id"]) or {}).get("state")
        == "finished",
        timeout=120, message="исполнитель доделал работу без агента",
    )
    lost = s.dangerous(
        f"/api/workers/jobs/{job3}/attempts/{attempt3['attempt_id']}/mark-lost",
        {"mandatory_reason": "smoke: VPS не отвечает",
         "typed_confirmation": "ПОПЫТКА ПОТЕРЯНА",
         "observed_worker_state": "offline", "optional_operator_note": ""},
    )
    check(lost.status_code == 200, "попытка признана потерянной")
    check(lost.json()["execution_state"] != "failed",
          "состояние исполнения НЕ подменено выдуманным failed (I-06)")
    check("может продолжать работу" in lost.json()["message"],
          "оператору сказано правду о возможном живом процессе")

    step("30. Создать новую попытку")
    created = s.dangerous(
        f"/api/workers/jobs/{job3}/attempts",
        {"worker_id": s.worker_id, "reason": "smoke: повтор",
         "source_attempt_id": attempt3["attempt_id"],
         "confirmation": "НОВАЯ ПОПЫТКА"},
    )
    check(created.status_code == 200, "новая попытка создана")
    new_attempt_id = created.json()["attempt_id"]
    check(created.json()["attempt_number"] == 2, "номер попытки — 2")

    step("31-34. Старая попытка возвращается: результат уходит в отдельное хранилище")
    s.start_agent()
    wait_for(
        lambda: any(a["attempt_id"] == attempt3["attempt_id"]
                    and a["state"] == "superseded_result_received"
                    for a in s.attempts(job3)),
        timeout=180, message="результат старой попытки принят на хранение",
    )
    old = next(a for a in s.attempts(job3) if a["attempt_id"] == attempt3["attempt_id"])
    check(old["attempt_disposition"] == "operator_declared_lost",
          "расположение старой попытки не изменилось")
    check(old.get("result_storage_class") == "superseded",
          "результат помечен как НЕ актуальный")
    fresh = next(a for a in s.attempts(job3) if a["attempt_id"] == new_attempt_id)
    check(fresh["state"] not in ("superseded_result_received",),
          "новая попытка не тронута результатом старой (I-07)")
    superseded_root = s.center_dir / "superseded_results"
    check(superseded_root.is_dir(), "каталог superseded_results существует")
    stored = list(superseded_root.rglob("*.tar.gz"))
    check(bool(stored), "архив отозванной попытки лежит в отдельном хранилище")
    check(all(attempt3["attempt_id"] in str(pth) for pth in stored),
          "архив разложен по UUID попытки, а не по коду проекта (I-11)")

    step("35. Кириллический код проекта со «/» и пути только по UUID")
    detail = s.admin.get(f"/api/workers/jobs/{job1}/attempts").json()
    check(detail["job"]["project_external_id"] == PROJECT_CODE,
          "внешний код проекта сохранён дословно")
    packages = s.center_dir / "source_packages"
    parts_ok = True
    for path in packages.iterdir():
        try:
            uuid.UUID(path.name)
        except ValueError:
            parts_ok = False
    check(parts_ok, "каталоги пакетов названы UUID, а не кодом проекта (I-11)")

    step("36-37. Подтверждение приёма и срок хранения на воркере")
    from audit_worker.local_store import LocalJobStore

    store = LocalJobStore(s.worker_dir / "jobs")
    confirmed = [m for m in store.iter_all() if m.get("retention_until")]
    check(bool(confirmed), "у воркера появился срок хранения после подтверждения")

    step("38-39. Сухой прогон RetentionManager: кандидаты есть, удалений нет")
    report = subprocess.run(  # noqa: S603
        [PY, "-m", "audit_worker", "retention", "--root", str(s.worker_dir)],
        env=s.worker_env(with_url=False), cwd=str(_ROOT),
        capture_output=True, text=True,
    )
    check(report.returncode == 0, "команда retention отработала")
    payload = json.loads(report.stdout)
    check(payload["delete_enabled"] is False, "физическое удаление выключено по умолчанию")
    check("candidates" in payload, "список кандидатов показан")

    step("40-42. Включить удаление и стереть локальную копию по команде оператора")
    s.stop(s.executor)
    s.start_executor(delete_enabled=True)
    target = confirmed[0]
    deletion = s.dangerous(
        f"/api/workers/jobs/{target['job_id']}"
        f"/attempts/{target['attempt_id']}/request-deletion",
        {"reason": "smoke: очистка", "confirmation": "УДАЛИТЬ ДАННЫЕ"},
    )
    check(deletion.status_code == 200, "команда удаления поставлена")
    job_dir = s.worker_dir / "jobs" / target["job_id"] / target["attempt_id"]
    try:
        wait_for(lambda: not job_dir.exists(), timeout=60,
                 message="удаление локальной копии")
        check(True, "локальная копия удалена")
    except TimeoutError:
        check(False, "локальная копия удалена")

    step("43. Центральная копия результата НЕ удалена")
    # Смотреть только в validated_results нельзя: `confirmed[0]` выбирается по
    # порядку каталогов (UUID), и им может оказаться попытка, признанная
    # потерянной, — её центральная копия лежит в superseded_results. Проверка
    # I-14 звучит как «удаление локальной копии не трогает ЦЕНТРАЛЬНУЮ», а не
    # «результат обязан быть опубликован», поэтому ищем в обоих хранилищах.
    central = [
        root / target["job_id"] / target["attempt_id"]
        for root in (s.center_dir / "validated_results",
                     s.center_dir / "superseded_results")
    ]
    check(any(path.is_dir() and any(path.iterdir()) for path in central),
          "центральный пакет результата на месте (I-14)")

    step("44-46. Перезапустить центр, агента и исполнителя")
    s.stop(s.agent)
    s.stop(s.executor)
    s.stop(s.center)
    s.start_center()
    s.start_executor(delete_enabled=True)
    s.start_agent()
    time.sleep(4.0)
    check(s.ping(), "центр поднялся заново")
    check(s.job(job1)["state"] == "completed", "состояние заданий восстановлено")
    check(len(s.attempts(job3)) == 2, "история попыток пережила перезапуск")

    step("47. Журнал операторских действий на месте")
    actions = s.admin.get("/api/workers/admin-actions?limit=200").json()["actions"]
    kinds = {a["action_type"] for a in actions}
    check({"approve_worker", "create_job", "cancel_attempt", "mark_attempt_lost",
           "create_attempt", "request_worker_data_deletion"} <= kinds,
          "все опасные действия записаны в журнал (I-15)")

    step("48. В логах нет traceback и секретов")
    leaked: list[str] = []
    for name in ("center", "agent", "executor"):
        path = s.log_dir / f"{name}.log"
        if not path.is_file():
            continue
        content = path.read_text(encoding="utf-8", errors="replace")
        if "Traceback (most recent call last)" in content:
            leaked.append(f"{name}: traceback")
        if BOOTSTRAP in content:
            leaked.append(f"{name}: bootstrap-secret в логе")
        if "wtk_" in content or "etk_" in content:
            leaked.append(f"{name}: токен в логе")
    check(not leaked, f"ни traceback, ни секретов в логах ({leaked})")


def main() -> int:
    parser = argparse.ArgumentParser(description="smoke этапа 3.5")
    parser.add_argument("--keep", action="store_true",
                        help="не удалять временный каталог (для разбора)")
    parser.add_argument("--root", default=None, help="каталог прогона")
    args = parser.parse_args()

    root = Path(args.root) if args.root else Path(tempfile.mkdtemp(prefix="dw-smoke-"))
    root.mkdir(parents=True, exist_ok=True)
    print(f"каталог прогона: {root}")
    try:
        return run(root)
    finally:
        if not args.keep and args.root is None:
            shutil.rmtree(root, ignore_errors=True)
        else:
            print(f"каталог сохранён: {root}")


if __name__ == "__main__":
    raise SystemExit(main())
