"""Точка входа агента: `python -m audit_worker`.

Подкоманды:
    register   — заявка на регистрацию (нужен bootstrap-секрет), а после
                 одобрения оператором — повторный вызов БЕЗ секрета, чтобы
                 обменять одноразовый claim-secret на токен
    agent      — СЕТЕВОЙ агент: heartbeat, приём заданий, передача результатов.
                 Процессы аудита не запускает
    executor   — ЛОКАЛЬНЫЙ исполнитель: запускает работу, ведёт реестр
                 процессов, исполняет отмену и удаление данных. Сети не знает
    run        — DEV-ONLY: агент, который сам поднимает исполнителя рядом.
                 В проде это ДВА systemd-юнита (см. документацию этапа 3.5):
                 рестарт агента не должен трогать исполнителя
    retention  — показать кандидатов на удаление (сухой прогон)
    status     — что воркер знает о себе и своих заданиях (офлайн, без сети)
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
    # Подсказки человеку идут в stderr: stdout остаётся машиночитаемым JSON,
    # который парсят скрипты развёртывания и smoke-прогон.
    if identity.get("token"):
        print(
            "\nТокен получен. Запускайте `python -m audit_worker agent` "
            "(и отдельно `python -m audit_worker executor`).",
            file=sys.stderr,
        )
    else:
        print(
            "\nДальше: оператор одобряет воркер на экране «Аудит-воркеры» "
            "(POST /api/workers/<worker_id>/approve), после чего повторите "
            "`python -m audit_worker register` уже БЕЗ --bootstrap-secret.",
            file=sys.stderr,
        )
    return 0


def _cmd_agent(args: argparse.Namespace) -> int:
    """Сетевой агент. Процессы аудита запускает ИСПОЛНИТЕЛЬ, не он."""
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
    except Exception as exc:  # noqa: BLE001 — недоступный центр не повод падать
        # Сюда попадает только случай «токена на диске нет»: с токеном
        # ensure_registered уже не бросает сетевых ошибок. Без токена работать
        # действительно нечем, но сообщение обязано отличать «сеть» от «отказ».
        from audit_worker.registration import classify_center_failure

        try:
            reason = classify_center_failure(exc)
        except BaseException:                       # noqa: BLE001 — чужая ошибка
            raise
        print(
            f"Центр недоступен ({reason}: {exc}), а токена на диске нет — "
            "работать нечем. Выполните `register` после восстановления связи.",
            file=sys.stderr,
        )
        return 2
    if not identity.get("token"):
        print("Токен воркера не найден. Выполните `register`.", file=sys.stderr)
        return 2
    center_state = identity.get("center_state")
    if center_state and center_state != "online":
        # Агент СТАРТУЕТ. Локальная база откроется, текущие задания найдутся,
        # исполнитель продолжит работу; связь восстанавливается фоновым
        # backoff'ом главного цикла. Крэш-лупа под systemd больше нет.
        print(
            f"[audit-worker] центр недоступен ({center_state}) — стартую в "
            "деградированном режиме: локальные задания продолжаются, события "
            "копятся, связь восстановлю сам.",
            file=sys.stderr,
        )

    child = None
    if getattr(args, "with_executor", False):
        print(
            "ВНИМАНИЕ: --with-executor — режим разработки. Под systemd так "
            "запускать НЕЛЬЗЯ: рестарт агента унесёт с собой работу. "
            "В проде — два независимых юнита.",
            file=sys.stderr,
        )
        child = _spawn_executor(config)
    agent = WorkerAgent(config, identity)
    try:
        agent.run_forever(max_jobs=args.max_jobs)
    except KeyboardInterrupt:
        print("\nостановка по Ctrl+C", file=sys.stderr)
        agent.shutdown()
    finally:
        if child is not None:
            child.terminate()
    return 0


def _cmd_executor(args: argparse.Namespace) -> int:
    """Локальный исполнитель. Ни одного сетевого вызова к центру."""
    from audit_worker.executor import Executor

    config = load_config(args.root, require_dispatcher=False)
    config.ensure_dirs()
    executor = Executor(config)
    try:
        executor.run_forever(max_jobs=args.max_jobs)
    except KeyboardInterrupt:
        print("\nостановка по Ctrl+C", file=sys.stderr)
        executor.shutdown()
    return 0


def _spawn_executor(config):
    """DEV-ONLY: поднять исполнителя рядом с агентом.

    В проде так делать нельзя: тогда рестарт агента унесёт с собой и работу,
    ради разделения с которой всё и затевалось. Для прода — два systemd-юнита.

    Если живой исполнитель уже есть, второй НЕ поднимается: рестарт агента под
    systemd иначе плодил бы исполнителей, каждый со своими наблюдателями.
    """
    import os
    import subprocess

    from audit_worker import local_db

    db = local_db.LocalDB(config.local_db_path)
    existing = db.latest_executor()
    if existing and db.executor_alive(existing["executor_instance_id"]):
        print(
            "[dev] исполнитель уже запущен "
            f"(pid={existing['process_pid']}) — второй не поднимаю",
            file=sys.stderr,
        )
        return None

    env = dict(os.environ)
    env["AUDIT_WORKER_ROOT"] = str(config.root)
    print(
        "[dev] запускаю локальный исполнитель отдельным процессом; "
        "в проде используйте audit-worker-executor.service",
        file=sys.stderr,
    )
    return subprocess.Popen(  # noqa: S603 — фиксированный argv, shell=False
        [sys.executable, "-m", "audit_worker", "executor", "--root", str(config.root)],
        env=env,
        start_new_session=True,
    )


def _cmd_run(args: argparse.Namespace) -> int:
    """DEV-ONLY совместимость: агент + исполнитель одной командой."""
    print(
        "ВНИМАНИЕ: `run` — режим разработки. Он поднимает исполнителя рядом с "
        "агентом; в проде это ДВА отдельных systemd-юнита, иначе рестарт "
        "агента остановит и работу.",
        file=sys.stderr,
    )
    args.with_executor = True
    return _cmd_agent(args)


def _cmd_retention(args: argparse.Namespace) -> int:
    """Сухой прогон менеджера хранения: что он СЧИТАЕТ кандидатами."""
    from audit_worker.local_db import LocalDB
    from audit_worker.retention import RetentionManager

    config = load_config(args.root, require_dispatcher=False)
    config.ensure_dirs()
    manager = RetentionManager(config, LocalDB(config.local_db_path))
    report = {
        "delete_enabled": config.retention_delete_enabled,
        "retention_days": config.retention_days,
        "disk": manager.disk_snapshot(),
        "candidates": manager.candidates(),
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if not config.retention_delete_enabled:
        print(
            "\nФизическое удаление ВЫКЛЮЧЕНО "
            "(AUDIT_WORKER_RETENTION_DELETE_ENABLED=false) — это сухой прогон.",
            file=sys.stderr,
        )
    return 0


def _cmd_status(args: argparse.Namespace) -> int:
    config = load_config(args.root, require_dispatcher=False)
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
            "автоматическое удаление запрещено.",
            file=sys.stderr,
        )
    return 0


def _cmd_providers(args: argparse.Namespace) -> int:
    """Состояние провайдеров ЛОКАЛЬНО, без сети к центру.

    Команда только читает: версия CLI, состояние авторизации, права файла
    учётных данных (по `stat`, без открытия) и — там, где официальный способ
    существует — остаток лимита. Контрольный запрос к модели отсюда НЕ
    выполняется ни при каких флагах: для него есть отдельная подкоманда с
    двумя независимыми разрешениями.
    """
    from audit_worker.providers.manager import ProviderManager

    config = load_config(args.root, require_dispatcher=False)
    manager = ProviderManager(
        worker_root=config.root,
        enabled=True,
        auth_check_interval_sec=config.provider_auth_check_interval_sec,
        quota_probe_interval_sec=config.provider_quota_probe_interval_sec,
        stale_after_sec=config.provider_quota_stale_after_sec,
        timeout_sec=config.provider_timeout_sec,
        low_threshold_pct=config.provider_quota_low_threshold_pct,
        account_groups=dict(config.provider_account_groups or {}),
        policy_blocked=dict(config.provider_policy_blocked or {}),
        auth_modes=dict(config.provider_auth_modes or {}),
        executables=dict(config.provider_executables or {}),
        # Сознательно False: команда наблюдения не имеет права разрешить
        # вызов модели, каким бы ни было окружение.
        inference_allowed=False,
        log=lambda message: print(f"[providers] {message}", file=sys.stderr),
    )
    from audit_worker.providers import probe_grant

    manager.refresh(force=True)
    report = {
        "worker_root": str(config.root),
        "provider_gate_enabled": config.provider_gate_enabled,
        # Наблюдаемо, но на решение больше не влияет — см. `_cmd_provider_probe`.
        "inference_probe_env_flag_deprecated": config.allow_real_provider_probe,
        # Разрешение воркера: единственное, что теперь открывает контрольный
        # запрос со стороны машины.
        "inference_probe_grant": {
            name: probe_grant.read_state(config.root, name).as_dict()
            for name in manager.adapters
        },
        "auth_check_interval_sec": config.provider_auth_check_interval_sec,
        "quota_probe_interval_sec": config.provider_quota_probe_interval_sec,
        "providers": manager.heartbeat_payload(),
        "warnings": manager.warnings(),
    }
    if args.local:
        # Локальный разрез включает абсолютные пути — он для администратора
        # ЭТОГО VPS и в центр не уходит.
        report["local_detail"] = {
            name: (identity.as_dict() if (identity := manager.identity(name)) else None)
            for name in manager.adapters
        }
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0


def _cmd_provider_probe(args: argparse.Namespace) -> int:
    """ОДИН минимальный контрольный запрос к модели (§18 задания).

    Два независимых разрешения, и оба обязательны:
      1. файл `<worker_root>/config/allow_real_provider_probe` с ненулевым
         остатком — разрешение СО СТОРОНЫ ВОРКЕРА. Его нельзя приписать к
         SSH-команде: он либо лежит на машине, либо нет;
      2. `--i-confirm-single-real-request` — решение оператора здесь и сейчас.

    Переменной `AUDIT_WORKER_ALLOW_REAL_PROVIDER_PROBE` первое разрешение БОЛЬШЕ
    НЕ ЯВЛЯЕТСЯ (находка 9 дока 11b): её подставлял в ту же SSH-команду тот же
    вызывающий, который писал и флаг, — два «независимых» голоса принадлежали
    одному. Значение переменной осталось наблюдаемым в `providers`, но на
    решение не влияет.

    Промпт фиксирован в коде и не содержит ни документов проекта, ни путей, ни
    репозитория. Инструменты запрещены, запись файлов запрещена.
    """
    from audit_worker.providers import probe_grant
    from audit_worker.providers.manager import ProviderManager

    config = load_config(args.root, require_dispatcher=False)
    if not args.i_confirm_single_real_request:
        print(
            "Нужен явный флаг --i-confirm-single-real-request.\n"
            "Файл разрешения даёт возможность, флаг — конкретный запуск.",
            file=sys.stderr,
        )
        return 2
    # Списание — ДО построения менеджера и до вызова модели. Порядок важен:
    # «сначала спросили, потом списали» дарит бесплатную попытку при любом
    # падении в середине, а падение в середине как раз и означает, что запрос
    # мог уйти.
    try:
        remaining = probe_grant.consume(config.root, args.provider)
    except probe_grant.ProbeGrantError as exc:
        print(f"Контрольный запрос запрещён.\n{exc}", file=sys.stderr)
        return 2
    print(
        f"[probe] разрешение воркера списано: остаток по {args.provider} = {remaining}",
        file=sys.stderr,
    )
    manager = ProviderManager(
        worker_root=config.root,
        enabled=True,
        timeout_sec=max(120.0, config.provider_timeout_sec),
        low_threshold_pct=config.provider_quota_low_threshold_pct,
        account_groups=dict(config.provider_account_groups or {}),
        policy_blocked=dict(config.provider_policy_blocked or {}),
        auth_modes=dict(config.provider_auth_modes or {}),
        executables=dict(config.provider_executables or {}),
        inference_allowed=True,
        log=lambda message: print(f"[probe] {message}", file=sys.stderr),
    )
    manager.refresh(force=True)
    before = manager.quota(args.provider)
    result = manager.minimal_probe(args.provider, confirmed_by_operator=True)
    after = manager.quota(args.provider)
    print(json.dumps(
        {
            "probe": result.as_dict(),
            "quota_before": before.as_dict() if before else None,
            "quota_after": after.as_dict() if after else None,
        },
        ensure_ascii=False, indent=2, default=str,
    ))
    return 0 if (result.performed and result.error_code is None) else 1


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

    def add_root(sub_parser: argparse.ArgumentParser) -> None:
        """Разрешить `--root` и ПОСЛЕ подкоманды.

        systemd-юниты и скрипты пишут его именно так, а argparse по
        умолчанию принимает верхнеуровневый флаг только перед подкомандой.
        SUPPRESS нужен, чтобы отсутствующий флаг не затирал значение,
        указанное до подкоманды.
        """
        sub_parser.add_argument(
            "--root", default=argparse.SUPPRESS,
            help="каталог состояния воркера",
        )

    p_reg = sub.add_parser(
        "register",
        help="заявка на регистрацию, а после одобрения — получение токена",
    )
    add_root(p_reg)
    p_reg.add_argument(
        "--bootstrap-secret",
        default=None,
        help="секрет регистрации; не нужен при повторном вызове после одобрения",
    )
    p_reg.set_defaults(func=_cmd_register)

    p_agent = sub.add_parser(
        "agent", help="сетевой агент (процессы аудита не запускает)"
    )
    add_root(p_agent)
    p_agent.add_argument("--bootstrap-secret", default=None)
    p_agent.add_argument("--max-jobs", type=int, default=None,
                         help="остановиться после N заданий (для smoke-прогона)")
    p_agent.add_argument(
        "--with-executor", action="store_true",
        help=argparse.SUPPRESS,      # оставлено для совместимости, см. ниже
    )
    p_agent.set_defaults(func=_cmd_agent)

    p_exec = sub.add_parser(
        "executor", help="локальный исполнитель (сети не знает)"
    )
    add_root(p_exec)
    p_exec.add_argument("--max-jobs", type=int, default=None,
                        help="остановиться после N попыток (для smoke-прогона)")
    p_exec.set_defaults(func=_cmd_executor)

    p_run = sub.add_parser(
        "run", help="DEV-ONLY: агент + исполнитель одной командой"
    )
    add_root(p_run)
    p_run.add_argument("--bootstrap-secret", default=None)
    p_run.add_argument("--max-jobs", type=int, default=None,
                       help="остановиться после N заданий (для smoke-прогона)")
    p_run.set_defaults(func=_cmd_run)

    p_ret = sub.add_parser(
        "retention", help="кандидаты на удаление (сухой прогон, ничего не стирает)"
    )
    add_root(p_ret)
    p_ret.set_defaults(func=_cmd_retention)

    p_status = sub.add_parser("status", help="локальное состояние (без сети)")
    add_root(p_status)
    p_status.set_defaults(func=_cmd_status)

    p_prov = sub.add_parser(
        "providers",
        help="состояние Claude/Codex на этом воркере (без сети к центру, без вызова модели)",
    )
    add_root(p_prov)
    p_prov.add_argument(
        "--local", action="store_true",
        help="добавить локальный разрез с абсолютными путями (в центр не уходит)",
    )
    p_prov.set_defaults(func=_cmd_providers)

    p_probe = sub.add_parser(
        "provider-probe",
        help="ОДИН минимальный контрольный запрос к модели (нужны два разрешения)",
    )
    add_root(p_probe)
    # Список берётся из провайдерского пакета, а не пишется здесь литералами:
    # имена настоящих CLI не должны встречаться в модулях воркера, через
    # которые идёт КОНВЕЙЕР (см. test_no_llm_invocation_in_worker_package).
    from audit_worker.providers.paths import SUPPORTED_PROVIDERS

    p_probe.add_argument("provider", choices=SUPPORTED_PROVIDERS)
    p_probe.add_argument(
        "--i-confirm-single-real-request", action="store_true",
        help="подтверждение оператора на ЭТОТ запуск (env-флага недостаточно)",
    )
    p_probe.set_defaults(func=_cmd_provider_probe)

    p_self = sub.add_parser("selftest", help="прогнать тестовый процесс локально")
    add_root(p_self)
    p_self.add_argument("--steps", type=int, default=3)
    p_self.set_defaults(func=_cmd_selftest)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
