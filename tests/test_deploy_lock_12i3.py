"""12I.3 — замок на выкатку: две параллельные выкатки одного компонента.

Дефект не гипотетический. На 12I.2 во время боевой работы параллельная сессия
собрала свой релиз центра и перезапустила backend; обошлось только потому, что
вторая сессия заметила чужой симлинк. Здесь проверяется, что теперь второй
выкатке физически не дадут дойти до изменения боевого состояния.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scripts.deploy_lock import (  # noqa: E402
    COMPONENT_CENTER,
    COMPONENT_GATEWAY,
    COMPONENT_WORKER,
    DeployLockHeld,
    deploy_lock,
    lock_path,
    read_holder,
)


def test_second_deploy_of_same_component_fails_immediately(tmp_path):
    with deploy_lock(COMPONENT_CENTER, operation="deploy", release="a", lock_dir=tmp_path):
        started = time.monotonic()
        with pytest.raises(DeployLockHeld) as caught:
            with deploy_lock(COMPONENT_CENTER, operation="deploy", release="b",
                             lock_dir=tmp_path):
                pytest.fail("вторая выкатка не должна была получить замок")
        waited = time.monotonic() - started
    assert "DEPLOY_LOCK_HELD" in str(caught.value)
    assert waited < 2.0, "замок обязан отказывать сразу, а не ждать в очереди"


def test_independent_components_do_not_block_each_other(tmp_path):
    with deploy_lock(COMPONENT_CENTER, operation="deploy", lock_dir=tmp_path):
        with deploy_lock(COMPONENT_GATEWAY, operation="install", lock_dir=tmp_path):
            with deploy_lock(COMPONENT_WORKER, operation="deploy", instance="11l",
                             lock_dir=tmp_path):
                pass


def test_worker_instances_are_locked_separately(tmp_path):
    """11l и 11g живут на одной машине: общий замок запрещал бы обслуживать их разом."""
    with deploy_lock(COMPONENT_WORKER, operation="deploy", instance="11l", lock_dir=tmp_path):
        with deploy_lock(COMPONENT_WORKER, operation="deploy", instance="11g",
                         lock_dir=tmp_path):
            pass
        with pytest.raises(DeployLockHeld):
            with deploy_lock(COMPONENT_WORKER, operation="deploy", instance="11l",
                             lock_dir=tmp_path):
                pass


def test_lock_is_released_on_clean_exit(tmp_path):
    with deploy_lock(COMPONENT_CENTER, operation="deploy", lock_dir=tmp_path):
        pass
    with deploy_lock(COMPONENT_CENTER, operation="deploy", lock_dir=tmp_path):
        pass


def test_lock_is_released_when_precheck_raises(tmp_path):
    with pytest.raises(RuntimeError):
        with deploy_lock(COMPONENT_CENTER, operation="deploy", lock_dir=tmp_path):
            raise RuntimeError("предпроверка не прошла")
    with deploy_lock(COMPONENT_CENTER, operation="deploy", lock_dir=tmp_path):
        pass


def test_kernel_releases_lock_when_holder_dies(tmp_path):
    """Главное свойство flock: убитый процесс не оставляет замок навсегда.

    Файл-сигнал этого не умеет — его пришлось бы удалять руками, а руками
    удаляемый сигнал рано или поздно снесут не глядя.
    """
    script = textwrap.dedent(f"""
        import sys, time
        sys.path.insert(0, {str(_ROOT)!r})
        from scripts.deploy_lock import deploy_lock, COMPONENT_CENTER
        from pathlib import Path
        with deploy_lock(COMPONENT_CENTER, operation="deploy",
                         lock_dir=Path({str(tmp_path)!r})):
            print("HELD", flush=True)
            time.sleep(60)
    """)
    child = subprocess.Popen([sys.executable, "-c", script], stdout=subprocess.PIPE, text=True)
    try:
        assert child.stdout.readline().strip() == "HELD"
        with pytest.raises(DeployLockHeld):
            with deploy_lock(COMPONENT_CENTER, operation="deploy", lock_dir=tmp_path):
                pass
        child.kill()
        child.wait(timeout=10)
        for _ in range(50):
            try:
                with deploy_lock(COMPONENT_CENTER, operation="deploy", lock_dir=tmp_path):
                    break
            except DeployLockHeld:
                time.sleep(0.1)
        else:
            pytest.fail("ядро не освободило замок после смерти держателя")
    finally:
        if child.poll() is None:
            child.kill()


def test_holder_metadata_is_useful_and_free_of_secrets(tmp_path, monkeypatch):
    monkeypatch.setenv("SECRET_TOKEN", "s3cr3t-value")
    with deploy_lock(COMPONENT_CENTER, operation="deploy", release="ui-real-a5c47dc6",
                     milestone="12i3", lock_dir=tmp_path) as path:
        holder = read_holder(path)
    assert holder["component"] == "center"
    assert holder["operation"] == "deploy"
    assert holder["release"] == "ui-real-a5c47dc6"
    assert holder["milestone"] == "12i3"
    assert holder["pid"] == os.getpid()
    assert holder["user"] and holder["started_at_iso"]
    blob = json.dumps(holder, ensure_ascii=False).lower()
    for forbidden in ("s3cr3t", "token", "secret", "password", "authorization", "cookie"):
        assert forbidden not in blob


def test_unknown_component_is_refused(tmp_path):
    with pytest.raises(ValueError):
        lock_path("database", lock_dir=tmp_path)


def test_lock_file_survives_release_so_metadata_stays_readable(tmp_path):
    with deploy_lock(COMPONENT_CENTER, operation="deploy", lock_dir=tmp_path) as path:
        pass
    assert path.is_file(), "файл замка удалять нельзя: это открыло бы гонку за inode"


# ═════ Обёртка для не-питоновских установщиков ══════════════════════════════
def _wrapper(args, lock_dir: Path, *, extra_env=None):
    env = dict(os.environ, AUDITMANAGER_DEPLOY_LOCK_DIR=str(lock_dir),
               PYTHONPATH=str(_ROOT))
    env.update(extra_env or {})
    return subprocess.Popen(
        [sys.executable, str(_ROOT / "scripts/deploy_lock.py"), *args],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env,
    )


def test_wrapper_keeps_the_lock_through_exec(tmp_path):
    """Замок обязан ПЕРЕЖИТЬ запуск установщика.

    Дефект, который здесь сторожится: Python открывает файлы с O_CLOEXEC, и
    после `exec` дескриптор закрылся бы, а вместе с ним снялся бы и замок —
    установщик продолжал бы менять прод, а второй деплой спокойно взял бы
    замок и пошёл параллельно. Защита существовала бы только на бумаге.
    """
    marker = tmp_path / "installer-alive"
    installer = textwrap.dedent(f"""
        import sys, time
        from pathlib import Path
        Path({str(marker)!r}).write_text("running")
        print("INSTALLING", flush=True)
        time.sleep(30)
    """)
    child = _wrapper(
        ["--component", "gateway", "--operation", "install", "--",
         sys.executable, "-c", installer],
        tmp_path,
    )
    try:
        # Дожидаемся не таймером, а СОБЫТИЕМ: установщик сам сообщает о старте.
        line = ""
        while "INSTALLING" not in line:
            line = child.stdout.readline()
            assert line, "установщик не запустился"
        assert marker.is_file()
        with pytest.raises(DeployLockHeld):
            with deploy_lock(COMPONENT_GATEWAY, operation="install", lock_dir=tmp_path):
                pass
    finally:
        child.kill()
        child.wait(timeout=10)


def test_wrapper_process_is_the_installer_itself(tmp_path):
    """После exec держатель замка и установщик — ОДИН процесс.

    Иначе убийство обёртки освобождало бы замок, оставляя живого потомка,
    который продолжает менять production.
    """
    reporter = textwrap.dedent("""
        import os
        print("PID", os.getpid(), flush=True)
    """)
    child = _wrapper(
        ["--component", "worker", "--operation", "deploy", "--",
         sys.executable, "-c", reporter],
        tmp_path,
    )
    out, _ = child.communicate(timeout=30)
    reported = [line for line in out.splitlines() if line.startswith("PID ")]
    assert reported, out
    assert int(reported[0].split()[1]) == child.pid, (
        "команда должна исполняться в процессе обёртки, а не в потомке"
    )


def test_wrapper_returns_ex_tempfail_when_lock_is_held(tmp_path):
    with deploy_lock(COMPONENT_CENTER, operation="deploy", lock_dir=tmp_path):
        child = _wrapper(
            ["--component", "center", "--operation", "deploy", "--",
             sys.executable, "-c", "print('SHOULD NOT RUN')"],
            tmp_path,
        )
        out, err = child.communicate(timeout=30)
    assert child.returncode == 75, "EX_TEMPFAIL: занято, повторить позже"
    assert "DEPLOY_LOCK_HELD" in err
    assert "SHOULD NOT RUN" not in out


# ═════ Проводка замка в выкатку воркера ═════════════════════════════════════
def test_worker_deploy_script_imports_when_run_directly():
    """Документированный прямой запуск не имеет права падать на импорте.

    `python scripts/deploy_audit_worker.py …` кладёт в `sys.path[0]` каталог
    `scripts/`, и `from scripts.deploy_lock import …` не находился — выкатка
    отказывала ДО взятия замка, то есть не стартовала вовсе.
    """
    probe = subprocess.run(
        [sys.executable, str(_ROOT / "scripts/deploy_audit_worker.py"), "--help"],
        capture_output=True, text=True,
        env={k: v for k, v in os.environ.items() if k != "PYTHONPATH"},
    )
    assert probe.returncode == 0, probe.stderr
    assert "ModuleNotFoundError" not in probe.stderr


def test_worker_lock_instance_is_one_installation_not_one_spelling():
    from scripts.deploy_audit_worker import worker_lock_instance

    by_ip = worker_lock_instance(host="127.0.0.1", user="auditworker_11l",
                                 remote_root="/home/auditworker_11l/audit-worker")
    by_name = worker_lock_instance(host="localhost", user="auditworker_11l",
                                   remote_root="/home/auditworker_11l/audit-worker")
    assert by_ip == by_name, (
        "два написания одного хоста давали бы разные замки — и две выкатки "
        "в один корень пошли бы параллельно"
    )
    other_user = worker_lock_instance(host="127.0.0.1", user="auditworker_11g",
                                      remote_root="/home/auditworker_11g/audit-worker")
    assert by_ip != other_user, "разные установки на одной машине не слепляются"
    same_basename = worker_lock_instance(host="127.0.0.1", user="auditworker_11l",
                                         remote_root="/opt/other/audit-worker")
    assert by_ip != same_basename, "совпадение последнего сегмента пути — не совпадение"


@pytest.mark.parametrize("command", ["build", "verify"])
def test_non_mutating_worker_commands_do_not_take_the_lock(command):
    """Замок запрещает вторую ВЫКАТКУ, а не безобидную параллельную сборку."""
    from scripts.deploy_audit_worker import _MUTATING_COMMANDS

    assert command not in _MUTATING_COMMANDS


@pytest.mark.parametrize("command", ["deploy", "rollback"])
def test_mutating_worker_commands_take_the_lock(command):
    from scripts.deploy_audit_worker import _MUTATING_COMMANDS

    assert command in _MUTATING_COMMANDS, "откат меняет прод так же, как выкатка"


def test_failed_nested_acquisition_does_not_disown_the_real_holder(tmp_path):
    """Отказавшая попытка не имеет права снимать учёт чужого дескриптора.

    Иначе внешний держатель терял бы возможность передать замок установщику
    через `exec` — из-за чужой неудачи, к которой он не имеет отношения.
    """
    from scripts import deploy_lock as module

    with deploy_lock(COMPONENT_CENTER, operation="deploy", lock_dir=tmp_path) as path:
        assert str(path) in module._HELD_FDS
        with pytest.raises(DeployLockHeld):
            with deploy_lock(COMPONENT_CENTER, operation="deploy", lock_dir=tmp_path):
                pass
        assert str(path) in module._HELD_FDS, (
            "чужая неудача стёрла учёт замка, который мы держим"
        )
        assert module._lock_fd_for_exec(path) >= 0


def test_mutating_primitive_is_locked_even_when_called_directly(tmp_path, monkeypatch):
    """Прямой вызов примитива мимо `main()` обязан упереться в чужой замок.

    Так его зовёт smoke-сценарий: `deploy.remote_switch_current(...)`. Замок
    только у разбора команд оставлял бы smoke и штатную выкатку одного воркера
    конкурировать за один симлинк и один рестарт.

    Держатель — ОТДЕЛЬНЫЙ процесс: повторный вход разрешён в пределах своего
    процесса, и держать замок здесь же означало бы проверять не то.
    """
    from scripts import deploy_audit_worker as deploy
    from scripts import deploy_lock as lock_module

    # Каталог замков читается как глобаль модуля в момент вызова: примитив
    # берёт замок без явного `lock_dir`, и подменять надо именно её.
    monkeypatch.setattr(lock_module, "DEFAULT_LOCK_DIR", tmp_path)
    remote = deploy.Remote(host="127.0.0.1", user="auditworker_11l",
                           root="/home/auditworker_11l/audit-worker-11l", dry_run=True)
    instance = remote.lock_instance
    script = textwrap.dedent(f"""
        import sys, time
        sys.path.insert(0, {str(_ROOT)!r})
        from pathlib import Path
        from scripts.deploy_lock import deploy_lock, COMPONENT_WORKER
        with deploy_lock(COMPONENT_WORKER, operation="deploy",
                         instance={instance!r}, lock_dir=Path({str(tmp_path)!r})):
            print("HELD", flush=True)
            time.sleep(60)
    """)
    child = subprocess.Popen([sys.executable, "-c", script], stdout=subprocess.PIPE,
                             text=True)
    try:
        assert child.stdout.readline().strip() == "HELD"
        with pytest.raises(DeployLockHeld):
            deploy.remote_switch_current(remote, "release-x")
    finally:
        child.kill()
        child.wait(timeout=10)


def test_deploy_flow_does_not_block_itself(tmp_path, monkeypatch):
    """Штатная выкатка держит замок снаружи и обязана пройти шаг внутри."""
    from scripts import deploy_audit_worker as deploy

    from scripts import deploy_lock as lock_module

    monkeypatch.setattr(lock_module, "DEFAULT_LOCK_DIR", tmp_path)
    remote = deploy.Remote(host="127.0.0.1", user="auditworker_11l",
                           root="/home/auditworker_11l/audit-worker-11l", dry_run=True)
    with deploy_lock(COMPONENT_WORKER, operation="deploy",
                     instance=remote.lock_instance, lock_dir=tmp_path):
        deploy.remote_switch_current(remote, "release-x")   # не должно упасть


# ═════ Имена юнитов: догадка из пути — не факт ══════════════════════════════
def test_units_are_discovered_from_systemd_not_guessed_from_the_path():
    """Боевые имена содержат хэш установки, которого в пути нет.

    Промах молча давал `UNIT_ABSENT`: рестарта нет, выкатка «успешна», а
    воркер работает на старом коде при уже переключённом `current`.
    """
    from scripts.deploy_audit_worker import discover_units, units_for_root

    root = "/home/auditworker_11l/audit-worker-11l"
    guessed = units_for_root(root)
    real = ["audit-worker-audit-worker-11l-30b5e4d544-agent.service",
            "audit-worker-audit-worker-11l-30b5e4d544-executor.service"]
    assert set(guessed) != set(real), "иначе этот тест ничего не сторожит"

    class _Remote:
        root = "/home/auditworker_11l/audit-worker-11l"

        def run(self, script, **kwargs):
            listing = "\n".join(f"{name} loaded active running описание" for name in real)
            return type("R", (), {"stdout": "\n".join(real), "stderr": "",
                                  "returncode": 0})()

    assert discover_units(_Remote(), root) == real


def test_unit_discovery_falls_back_quietly_when_it_learns_nothing():
    """Разведка не вправе ронять выкатку: молчание = остаёмся на умолчании."""
    from scripts.deploy_audit_worker import discover_units

    class _Silent:
        def run(self, script, **kwargs):
            return type("R", (), {"stdout": "", "stderr": "", "returncode": 1})()

    assert discover_units(_Silent(), "/home/auditworker_11l/audit-worker-11l") == []
