"""Точка входа агента: `python -m audit_worker`.

Подкоманды:
    register   — заявка на регистрацию (нужен bootstrap-секрет), а после
                 одобрения оператором — повторный вызов БЕЗ секрета, чтобы
                 обменять одноразовый claim-secret на токен
    run        — основной цикл: heartbeat + приём заданий
    status     — что агент знает о себе и своих заданиях (офлайн, без сети)
    selftest   — прогнать тестовый процесс локально, без центра

Переменные окружения:
    AUDIT_WORKER_DISPATCHER_URL   обязательна — адрес центра (https://…)
    AUDIT_WORKER_ROOT             каталог состояния (по умолчанию /var/lib/audit-worker)
    AUDIT_WORKER_NAME             отображаемое имя VPS
    AUDIT_WORKER_MAX_SLOTS        1..5
    AUDIT_WORKER_HEARTBEAT_SEC    период heartbeat, по умолчанию 30
    AUDIT_WORKER_ALLOW_INSECURE_LOCALHOST  разрешить http:// к localhost (dev)

Проверка TLS-сертификата не отключается ничем: переменной для этого нет
намеренно — иначе она рано или поздно окажется включённой в проде.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from audit_worker import __version__
from audit_worker.client import CenterError
from audit_worker.config import load_config
from audit_worker.local_store import LocalJobStore, WorkerStateStore
from audit_worker.registration import RegistrationRequired, ensure_registered


def _cmd_register(args: argparse.Namespace) -> int:
    config = load_config(args.root)
    config.ensure_dirs()
    store = WorkerStateStore(config.state_path, config.token_path)
    # Второй этап (обмен claim-secret на токен) секрета регистрации не требует:
    # он уже лежит на диске. Требовать его снова — заставлять оператора
    # держать bootstrap-secret под рукой дольше, чем нужно.
    if not args.bootstrap_secret and not (
        store.read_claim_secret() or store.read_token()
    ):
        print(
            "Нужен --bootstrap-secret: на диске нет ни claim-secret, ни токена.",
            file=sys.stderr,
        )
        return 2
    try:
        identity = ensure_registered(config, bootstrap_secret=args.bootstrap_secret)
    except CenterError as exc:
        print(f"Центр отклонил регистрацию: {exc}", file=sys.stderr)
        return 2
    except RegistrationRequired as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(json.dumps(
        {
            "worker_id": identity.get("worker_id"),
            "registration_status": identity.get("registration_status"),
            "instance_id": identity.get("instance_id"),
            "token_stored": bool(identity.get("token")),
            "root": str(config.root),
        },
        ensure_ascii=False, indent=2,
    ))
    if identity.get("token"):
        print("\nТокен получен. Запускайте `python -m audit_worker run`.")
    else:
        print(
            "\nДальше: оператор одобряет воркер на экране «Аудит-воркеры» "
            "(POST /api/workers/<worker_id>/approve), после чего повторите "
            "`python -m audit_worker register` уже БЕЗ --bootstrap-secret."
        )
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    from audit_worker.agent import WorkerAgent

    config = load_config(args.root)
    config.ensure_dirs()
    try:
        identity = ensure_registered(config, bootstrap_secret=args.bootstrap_secret)
    except RegistrationRequired as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except CenterError as exc:
        print(f"Центр отклонил регистрацию: {exc}", file=sys.stderr)
        return 2
    if not identity.get("token"):
        print("Токен воркера не найден. Выполните `register`.", file=sys.stderr)
        return 2

    agent = WorkerAgent(config, identity)
    try:
        agent.run_forever(max_jobs=args.max_jobs)
    except KeyboardInterrupt:
        print("\nостановка по Ctrl+C", file=sys.stderr)
        agent.shutdown()
    return 0


def _cmd_status(args: argparse.Namespace) -> int:
    config = load_config(args.root)
    store = WorkerStateStore(config.state_path, config.token_path)
    jobs = LocalJobStore(config.jobs_dir)
    state = store.load()
    unconfirmed = jobs.retention_unconfirmed()
    report = {
        "version": __version__,
        "root": str(config.root),
        "dispatcher_url": config.dispatcher_url,
        "worker_id": state.get("worker_id"),
        "last_instance_id": state.get("last_instance_id"),
        "token_present": bool(store.read_token()),
        "jobs": [
            {
                "job_id": m["job_id"],
                "attempt_id": m["attempt_id"],
                "local_state": m.get("local_state"),
                "result_hash": (m.get("result_hash") or "")[:16] or None,
                "retention_until": m.get("retention_until"),
            }
            for m in jobs.iter_all()
        ],
        "retention_unconfirmed": len(unconfirmed),
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if unconfirmed:
        print(
            f"\n⚠ {len(unconfirmed)} результат(ов) без подтверждения приёма центром: "
            "автоматическое удаление запрещено."
        )
    return 0


def _cmd_selftest(args: argparse.Namespace) -> int:
    """Прогнать тестовый процесс локально — проверка окружения без центра."""
    from audit_worker import test_runner

    root = Path(args.root or ".").resolve() / "_selftest"
    root.mkdir(parents=True, exist_ok=True)
    params = test_runner.validate_params(
        {"label": "selftest", "steps": args.steps, "step_seconds": 0.05,
         "result_bytes": 2048},
        max_total_sec=60.0,
    )
    print("argv:", " ".join(test_runner.build_argv(root / "work" / "test_params.json")))
    outcome = test_runner.run_test_job(
        params=params,
        job_dir=root,
        on_progress=lambda s, t, e, m: print(f"  прогресс {s}/{t}: {m}"),
        on_log=lambda stream, level, line: print(f"  [{stream}/{level}] {line}"),
    )
    print(f"код возврата: {outcome.exit_code}, длительность {outcome.duration_sec:.2f} с")
    summary = root / "result" / "summary.json"
    print("summary.json:", "есть" if summary.is_file() else "НЕТ")
    return 0 if outcome.exit_code == 0 else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="audit_worker", description="audit-worker — агент распределённого аудита"
    )
    parser.add_argument("--root", default=None, help="каталог состояния воркера")
    parser.add_argument("--version", action="version", version=__version__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_reg = sub.add_parser(
        "register",
        help="заявка на регистрацию, а после одобрения — получение токена",
    )
    p_reg.add_argument(
        "--bootstrap-secret",
        default=None,
        help="секрет регистрации; не нужен при повторном вызове после одобрения",
    )
    p_reg.set_defaults(func=_cmd_register)

    p_run = sub.add_parser("run", help="основной цикл агента")
    p_run.add_argument("--bootstrap-secret", default=None)
    p_run.add_argument("--max-jobs", type=int, default=None,
                       help="остановиться после N заданий (для smoke-прогона)")
    p_run.set_defaults(func=_cmd_run)

    p_status = sub.add_parser("status", help="локальное состояние (без сети)")
    p_status.set_defaults(func=_cmd_status)

    p_self = sub.add_parser("selftest", help="прогнать тестовый процесс локально")
    p_self.add_argument("--steps", type=int, default=3)
    p_self.set_defaults(func=_cmd_selftest)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
