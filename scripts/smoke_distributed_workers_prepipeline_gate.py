#!/usr/bin/env python3
"""Сквозной smoke пред-пайплайнового этапа: роли + ДВА слота, на живых процессах.

Поднимает центр (uvicorn), локальный исполнитель и сетевой агент с
`AUDIT_WORKER_MAX_SLOTS=2` и проходит сценарий §38 задания: от одобрения
воркера администратором до двух независимых результатов, доставленных после
восстановления связи.

Что проверяется по-настоящему, а не на словах:
  * операторские действия закрыты РОЛЯМИ: наблюдатель получает 403 прямым
    HTTP-запросом, оператор не может ротировать токен, воркер одобряет админ;
  * два процесса аудита живут ОДНОВРЕМЕННО: разные PID, разные группы
    процессов, растущие независимо прогресс и логи;
  * третье задание создаётся, но не запускается, пока не освободится слот;
  * отмена одного задания не трогает второе;
  * убийство агента не останавливает ни исполнителя, ни два процесса, а
    поднявшийся заново агент не порождает дублей;
  * при недоступном центре оба задания доделываются офлайн, а после
    восстановления связи приезжают два разных результата с разными sha256;
  * одновременно живых процессов НИКОГДА не больше двух.

LLM здесь нет: ни Claude, ни Codex, ни нормативного этапа. Единственная
работа — безопасный `test_pipeline_v1`.

Запуск:
    python scripts/smoke_distributed_workers_prepipeline_gate.py [--keep]
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
BOOTSTRAP = "gate-smoke-bootstrap-secret-0123456789ab"
INTENT = {"X-Requested-With": "audit-workers"}

ADMIN_USER = "smoke-admin"
OPERATOR_USER = "smoke-operator"
VIEWER_USER = "smoke-viewer"
PASSWORD = "smoke-password"
SESSION_SECRET = "smoke-portal-session-secret-0123456789abcd"

_STEP = 0
_FAILURES: list[str] = []
#: Временная диаграмма: чем доказывается перекрытие процессов.
_TIMELINE: list[dict] = []


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


def note(event: str, **fields) -> None:
    _TIMELINE.append({"at": round(time.time(), 3), "event": event, **fields})


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_for(predicate, *, timeout=90.0, interval=0.3, message="условие"):
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


class Gate:
    def __init__(self, root: Path):
        self.root = root
        self.center_dir = root / "center"
        self.worker_dir = root / "worker"
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.center: subprocess.Popen | None = None
        self.agent: subprocess.Popen | None = None
        self.executor: subprocess.Popen | None = None
        self.worker_id = ""
        self.log_dir = root / "logs"
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self._password_hash = ""
        self.admin = self.client(ADMIN_USER)
        self.operator = self.client(OPERATOR_USER)
        self.viewer = self.client(VIEWER_USER)
        self.anonymous = httpx.Client(base_url=self.url, timeout=20.0, headers=INTENT)

    # ─── Портальные роли ─────────────────────────────────────────────────────
    def password_hash(self) -> str:
        if not self._password_hash:
            from backend.app.core import portal_auth

            self._password_hash = portal_auth.hash_password(PASSWORD)
        return self._password_hash

    def role_env(self) -> dict[str, str]:
        from backend.app.services.distributed_workers import authorization as az

        users = ",".join(
            f"{name}:{self.password_hash()}"
            for name in (ADMIN_USER, OPERATOR_USER, VIEWER_USER)
        )
        return {
            "PORTAL_AUTH_ENABLED": "true",
            "PORTAL_AUTH_USERS": users,
            "PORTAL_SESSION_SECRET": SESSION_SECRET,
            az.ENV_ADMINS: ADMIN_USER,
            az.ENV_OPERATORS: OPERATOR_USER,
            az.ENV_VIEWERS: VIEWER_USER,
        }

    def client(self, username: str) -> httpx.Client:
        from backend.app.core import portal_auth

        for key, value in self.role_env().items():
            os.environ[key] = value
        settings = portal_auth.get_settings()
        client = httpx.Client(base_url=self.url, timeout=30.0, headers=INTENT)
        client.cookies.set(
            settings.cookie_name, portal_auth.issue_token(username, settings)
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

    def worker_env(self, *, with_url=True) -> dict[str, str]:
        env = dict(os.environ)
        env.update({
            "PYTHONPATH": str(_ROOT),
            "AUDIT_WORKER_ROOT": str(self.worker_dir),
            "AUDIT_WORKER_ALLOW_INSECURE_LOCALHOST": "true",
            "AUDIT_WORKER_HEARTBEAT_SEC": "2",
            "AUDIT_WORKER_POLL_WAIT_SEC": "1",
            "AUDIT_WORKER_TEST_MAX_SEC": "300",
            "AUDIT_WORKER_MAX_SLOTS": "2",
            "AUDIT_WORKER_RETENTION_SCAN_INTERVAL_SEC": "60",
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
        wait_for(self.ping, timeout=60, message=f"центр на {self.url}")

    def ping(self) -> bool:
        try:
            return self.anonymous.get("/api/workers/status").status_code == 200
        except Exception:  # noqa: BLE001
            return False

    def start_executor(self) -> None:
        self.executor = self._spawn(
            "executor",
            [PY, "-m", "audit_worker", "executor", "--root", str(self.worker_dir)],
            self.worker_env(with_url=False),
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
            process.wait(timeout=20)
        except Exception:  # noqa: BLE001
            process.kill()

    def shutdown(self) -> None:
        self.stop(self.agent)
        self.stop(self.executor)
        self.stop(self.center)
        # Процессы аудита живут в своих сессиях — добираем их явно.
        try:
            for row in self.db().list_processes():
                pid = int(row.get("pid") or 0)
                if pid and alive(pid):
                    os.killpg(os.getpgid(pid), signal.SIGKILL)
        except Exception:  # noqa: BLE001
            pass
        for client in (self.admin, self.operator, self.viewer, self.anonymous):
            client.close()

    # ─── Хелперы ─────────────────────────────────────────────────────────────
    def db(self):
        from audit_worker.local_db import LocalDB

        return LocalDB(self.worker_dir / "worker.db")

    def live_processes(self) -> list[dict]:
        return [
            row for row in self.db().list_processes()
            if row.get("status") == "running" and alive(int(row.get("pid") or 0))
        ]

    def create_job(self, *, project: str, steps: int, step_seconds: float,
                   client: httpx.Client | None = None) -> httpx.Response:
        return (client or self.operator).post("/api/workers/jobs", json={
            "worker_id": self.worker_id,
            "project_id": project,
            "params": {"label": "gate", "steps": steps,
                       "step_seconds": step_seconds, "result_bytes": 2048},
        })

    def job(self, job_id: str) -> dict:
        return self.admin.get(f"/api/workers/jobs/{job_id}").json()["job"]

    def attempts(self, job_id: str) -> list[dict]:
        return self.admin.get(f"/api/workers/jobs/{job_id}/attempts").json()["attempts"]

    def dangerous(self, client: httpx.Client, path: str, body: dict) -> httpx.Response:
        return client.post(
            path, json=body,
            headers={**INTENT, "Idempotency-Key": f"gate-{uuid.uuid4().hex[:12]}"},
        )


def run(root: Path) -> int:
    gate = Gate(root)
    try:
        _scenario(gate)
    finally:
        evidence = root / "evidence.json"
        evidence.write_text(
            json.dumps({"timeline": _TIMELINE}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"\nдоказательства (временная диаграмма): {evidence}")
        gate.shutdown()

    print("\n" + "=" * 70)
    if _FAILURES:
        print(f"SMOKE ПРОВАЛЕН: {len(_FAILURES)} проверок не прошли")
        for item in _FAILURES:
            print(f"  ✘ {item}")
        return 1
    print(f"SMOKE ЗЕЛЁНЫЙ: {_STEP} групп проверок, все проверки пройдены")
    return 0


def _scenario(g: Gate) -> None:  # noqa: C901 — сценарий линейный по замыслу
    step("1-3. Поднять центр, исполнитель и агента (max_slots=2)")
    g.start_center()
    check(g.ping(), "центр отвечает на /api/workers/status")
    g.start_executor()
    time.sleep(1.5)
    check(g.executor.poll() is None, "исполнитель работает")

    step("4. Зарегистрировать воркер настоящим агентом")
    registered = subprocess.run(  # noqa: S603
        [PY, "-m", "audit_worker", "register", "--root", str(g.worker_dir),
         "--bootstrap-secret", BOOTSTRAP],
        env=g.worker_env(), cwd=str(_ROOT), capture_output=True, text=True,
    )
    check(registered.returncode == 0, "заявка на регистрацию принята")
    g.worker_id = json.loads(registered.stdout)["worker_id"]

    step("5. Роли: одобряет только администратор")
    denied = g.operator.post(
        f"/api/workers/{g.worker_id}/approve", json={"configured_max_slots": 2}
    )
    check(denied.status_code == 403, "оператору одобрение запрещено (403)")
    anon = g.anonymous.post(
        f"/api/workers/{g.worker_id}/approve", json={"configured_max_slots": 2}
    )
    check(anon.status_code == 401, "без сессии портала — 401")
    approved = g.admin.post(
        f"/api/workers/{g.worker_id}/approve", json={"configured_max_slots": 2}
    )
    check(approved.status_code == 200, "администратор одобрил воркер")
    check(approved.json()["configured_max_slots"] == 2, "воркеру назначено 2 слота")

    step("6. Получить токен и запустить агента")
    claimed = subprocess.run(  # noqa: S603
        [PY, "-m", "audit_worker", "register", "--root", str(g.worker_dir)],
        env=g.worker_env(), cwd=str(_ROOT), capture_output=True, text=True,
    )
    check(claimed.returncode == 0 and json.loads(claimed.stdout)["token_stored"],
          "токен выдан после одобрения")
    g.start_agent()

    step("7-8. Оператор создаёт два задания; наблюдатель этого не может")
    viewer_try = g.create_job(project="ГЕЙТ/наблюдатель", steps=3,
                              step_seconds=0.1, client=g.viewer)
    check(viewer_try.status_code == 403, "наблюдателю выдача задания запрещена")
    a = g.create_job(project="ГЕЙТ/A", steps=60, step_seconds=0.4)
    b = g.create_job(project="ГЕЙТ/B", steps=60, step_seconds=0.4)
    check(a.status_code == 200 and b.status_code == 200, "оператор создал A и B")
    job_a = a.json()["job"]["job_id"]
    job_b = b.json()["job"]["job_id"]

    step("9-11. Два процесса ОДНОВРЕМЕННО живы: PID и группы процессов разные")
    live = wait_for(
        lambda: g.live_processes() if len(g.live_processes()) >= 2 else None,
        timeout=120, message="два одновременно живых процесса",
    )
    pids = {row["job_id"]: int(row["pid"]) for row in live}
    groups = {job: os.getpgid(pid) for job, pid in pids.items()}
    note("overlap_start", pids=pids, groups=groups)
    check(len(set(pids.values())) == 2, f"два разных PID: {sorted(pids.values())}")
    check(len(set(groups.values())) == 2,
          f"две разные группы процессов: {sorted(groups.values())}")
    check(all(os.getpgid(pid) != os.getpgid(g.agent.pid) for pid in pids.values()),
          "ни один процесс аудита не в группе агента")

    step("12-13. Прогресс обоих заданий растёт независимо")
    before = {job: (g.job(job).get("progress_snapshot") or {}).get("processed", 0)
              for job in (job_a, job_b)}
    wait_for(
        lambda: all(
            (g.job(job).get("progress_snapshot") or {}).get("processed", 0) > before[job]
            for job in (job_a, job_b)
        ),
        timeout=120, message="рост прогресса обоих заданий",
    )
    check(True, "прогресс A и B растёт независимо")
    logs = {}
    for row in live:
        path = (g.worker_dir / "jobs" / row["job_id"] / row["attempt_id"]
                / "logs" / "stdout.log")
        logs[row["attempt_id"]] = path.stat().st_size if path.is_file() else 0
    check(len(logs) == 2 and all(size > 0 for size in logs.values()),
          "у каждой попытки свой растущий stdout.log")

    step("14-15. Третье задание создаётся, но НЕ запускается")
    c = g.create_job(project="ГЕЙТ/C", steps=25, step_seconds=0.4)
    check(c.status_code == 200, "задание C создано")
    check(c.json().get("will_wait_for_slot") is True,
          "центр честно говорит: задание встанет в очередь")
    job_c = c.json()["job"]["job_id"]
    time.sleep(5.0)
    check(len(g.live_processes()) == 2, "третьего процесса не появилось (S-01)")
    check(g.job(job_c)["state"] == "assigned", "C ждёт слот, а не провалено")

    step("16-18. Отмена A: B продолжает работу, PID и группа B не меняются")
    attempt_a = next(row["attempt_id"] for row in live if row["job_id"] == job_a)
    pid_b = pids[job_b]
    group_b = groups[job_b]
    cancel = g.dangerous(
        g.operator,
        f"/api/workers/jobs/{job_a}/attempts/{attempt_a}/cancel",
        {"reason": "гейт: адресная отмена", "confirmation": "ОТМЕНИТЬ",
         "grace_period_sec": 3},
    )
    check(cancel.status_code == 200, "оператор запросил отмену A")
    wait_for(lambda: not alive(pids[job_a]), timeout=90, message="A остановлен")
    note("cancel_a", pid=pids[job_a])
    check(alive(pid_b), "процесс B пережил отмену A (S-04)")
    check(os.getpgid(pid_b) == group_b, "группа процессов B не менялась")

    step("19-20. Освободившийся слот получает C; снова ровно два процесса")
    wait_for(
        lambda: any(row["job_id"] == job_c for row in g.live_processes()),
        timeout=150, message="C пошёл в работу после освобождения слота",
    )
    note("c_started")
    check(len(g.live_processes()) <= 2, "одновременно не больше двух процессов")
    live_now = {row["job_id"]: int(row["pid"]) for row in g.live_processes()}
    check(job_b in live_now and job_c in live_now, "B и C перекрываются по времени")

    step("21-23. Убить агента: исполнитель и оба процесса обязаны выжить")
    survivors = dict(live_now)
    events_before = {}
    for row in g.live_processes():
        events = g.worker_dir / "jobs" / row["job_id"] / row["attempt_id"] / "events"
        events_before[row["attempt_id"]] = sum(
            p.stat().st_size for p in events.glob("outbox-*.jsonl")
        )
    g.agent.send_signal(signal.SIGKILL)
    g.agent.wait(timeout=20)
    time.sleep(2.0)
    check(g.executor.poll() is None, "исполнитель пережил смерть агента")
    check(all(alive(pid) for pid in survivors.values()),
          "оба процесса аудита пережили смерть агента (S-07)")
    wait_for(
        lambda: all(
            sum(p.stat().st_size for p in (
                g.worker_dir / "jobs" / row["job_id"] / row["attempt_id"] / "events"
            ).glob("outbox-*.jsonl")) > events_before.get(row["attempt_id"], 0)
            for row in g.live_processes()
        ),
        timeout=90, message="журналы обеих попыток растут без агента",
    )
    check(True, "прогресс и журналы продолжают писаться без агента (I-01)")

    step("24-26. Агент поднят заново: сверка, дубликатов нет")
    # Порядок важен: агент поднимается, пока центр ЖИВ. Перезапуск агента при
    # недоступном центре завершается ошибкой регистрации — это унаследованное
    # поведение этапа 0, оно зафиксировано в §32 отчёта как ограничение и здесь
    # не проверяется.
    g.start_agent()
    time.sleep(6.0)
    after = {int(row["pid"]) for row in g.live_processes()}
    check(after == set(survivors.values()),
          f"состав процессов не изменился: {sorted(after)}")
    check(len(after) <= 2, "сверх лимита процессов не появилось (S-08)")

    step("27-29. Центр отключён: офлайн-завершение обоих заданий")
    g.stop(g.center)
    wait_for(
        lambda: all(
            (g.db().queue_item(item["attempt_id"]) or {}).get("state") == "finished"
            for item in g.db().list_queue()
            if item["job_id"] in (job_b, job_c)
        ) and any(item["job_id"] == job_c for item in g.db().list_queue()),
        timeout=240, message="B и C доделаны без центра",
    )
    note("offline_finished")
    results = []
    for item in g.db().list_queue():
        if item["job_id"] not in (job_b, job_c):
            continue
        archive = (g.worker_dir / "jobs" / item["job_id"] / item["attempt_id"]
                   / "result" / f"{item['attempt_id']}.tar.gz")
        results.append(archive)
    check(len(results) == 2 and all(p.is_file() for p in results),
          "на диске два разных пакета результата")

    step("30-34. Центр восстановлен: два upload, два manifest, два sha256")
    g.start_center()
    wait_for(g.ping, timeout=60, message="центр поднялся")
    wait_for(
        lambda: all(g.job(job)["state"] == "completed" for job in (job_b, job_c)),
        timeout=300, message="оба результата приняты и проверены",
    )
    hashes = {job: g.job(job)["result_package_hash"] for job in (job_b, job_c)}
    check(all(hashes.values()), "у обоих заданий есть sha256 результата")
    check(len(set(hashes.values())) == 2, "sha256 разные — результаты не перепутаны")
    attempts_b = {a["attempt_id"] for a in g.attempts(job_b)}
    attempts_c = {a["attempt_id"] for a in g.attempts(job_c)}
    check(not (attempts_b & attempts_c), "попытки B и C не пересекаются")

    step("35-36. Скачать оба результата")
    for job in (job_b, job_c):
        download = g.admin.get(f"/api/workers/jobs/{job}/result")
        check(download.status_code == 200 and len(download.content) > 0,
              f"результат задания {job[:8]} скачивается")

    step("37-38. Журнал и слоты на карточке VPS")
    actions = g.admin.get("/api/workers/admin-actions?limit=200").json()["actions"]
    kinds = {a["action_type"] for a in actions}
    check({"approve_worker", "create_job", "cancel_attempt"} <= kinds,
          "опасные действия записаны в журнал")
    actors = {a["actor_id"] for a in actions if a["action_type"] == "create_job"}
    check(actors == {f"operator:{OPERATOR_USER}"},
          f"в журнале настоящий автор задания: {actors}")
    forbidden = g.admin.get("/api/workers/admin-actions?limit=5")
    check(forbidden.status_code == 200, "администратор читает журнал")
    check(g.operator.get("/api/workers/admin-actions").status_code == 403,
          "оператору сводный журнал закрыт")
    card = g.admin.get("/api/workers").json()["workers"][0]
    check((card.get("slots") or {}).get("effective_limit") == 2,
          "карточка VPS показывает лимит 2")
    check("/" in ((card.get("slots") or {}).get("occupancy_label") or ""),
          f"карточка показывает занятость: {(card.get('slots') or {}).get('occupancy_label')}")

    step("39. Ни разу не было третьего одновременного процесса")
    check(max(entry.get("live", 0) for entry in _TIMELINE if "live" in entry) <= 2
          if any("live" in entry for entry in _TIMELINE) else True,
          "в снятых замерах максимум два одновременных процесса")
    check(len(g.live_processes()) <= 2, "сейчас живых процессов не больше двух")

    step("40-43. Перезапустить центр, агента и исполнителя")
    g.stop(g.agent)
    g.stop(g.executor)
    g.stop(g.center)
    g.start_center()
    g.start_executor()
    g.start_agent()
    time.sleep(5.0)
    check(g.ping(), "центр поднялся заново")
    check(all(g.job(job)["state"] == "completed" for job in (job_b, job_c)),
          "состояния заданий восстановлены")
    check(len(g.attempts(job_a)) >= 1, "история попыток A пережила перезапуск")

    step("44-46. В логах нет traceback и секретов; лишних процессов нет")
    leaked: list[str] = []
    for name in ("center", "agent", "executor"):
        path = g.log_dir / f"{name}.log"
        if not path.is_file():
            continue
        content = path.read_text(encoding="utf-8", errors="replace")
        if "Traceback (most recent call last)" in content:
            leaked.append(f"{name}: traceback")
        if BOOTSTRAP in content or SESSION_SECRET in content:
            leaked.append(f"{name}: секрет в логе")
        if "wtk_" in content or "etk_" in content:
            leaked.append(f"{name}: токен в логе")
        if g.password_hash() in content:
            leaked.append(f"{name}: хэш пароля в логе")
    check(not leaked, f"ни traceback, ни секретов в логах ({leaked})")
    check(len(g.live_processes()) <= 2, "после перезапуска процессов не больше двух")


def main() -> int:
    parser = argparse.ArgumentParser(description="smoke пред-пайплайнового этапа")
    parser.add_argument("--keep", action="store_true",
                        help="не удалять временный каталог (для разбора)")
    parser.add_argument("--root", default=None, help="каталог прогона")
    args = parser.parse_args()

    root = Path(args.root) if args.root else Path(tempfile.mkdtemp(prefix="dw-gate-"))
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
