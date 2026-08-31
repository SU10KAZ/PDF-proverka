#!/usr/bin/env python3
"""Канонический переключатель боевого релиза центра.

Порядок намеренно один и тот же и НЕ переставляется: все проверки выполняются
ДО остановки. Отказ на проверке не должен стоить простоя — а именно так и
выходило, когда прекчек стоял после `systemctl stop`.

Замок берётся первым: на 12I.2 параллельная сессия выкатила свой релиз и
перезапустила backend посреди чужой работы (см. `scripts/deploy_lock.py`).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.build_center_release import (  # noqa: E402
    REQUIRED_PATHS,
    fileset_digest,
)
from scripts.deploy_lock import COMPONENT_CENTER, deploy_lock  # noqa: E402
from scripts.production_source_guard import (  # noqa: E402
    ProductionSourceNotCanonical,
    verify_production_source,
)
from norms.runtime import runtime_problems  # noqa: E402

ROOT = Path("/home/coder/auditmanager")
SERVICE = "auditmanager-backend.service"
HEALTH_URL = "http://127.0.0.1:8081/api/info"
DEFAULT_NORMS_RUNTIME_TOOLS = ROOT / "shared" / "norms" / "tools"

#: Файлы, определяющие ПРОВОД потока. Совпадение с релизом шлюза — условие
#: допустимости разъезда версий (см. диагностику совместимости).
WIRE_FILES = (
    "app/contracts/agent_stream/v1/agent_stream_v1.desc",
    "app/contracts/agent_stream/v1/agent_stream.proto",
    "app/contracts/agent_stream/v1/common.proto",
)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _systemctl_user(*args: str) -> str:
    return subprocess.check_output(["systemctl", "--user", *args], text=True).strip()


def _switch(target: Path) -> None:
    """Атомарная замена симлинка `current`."""
    tmp_link = ROOT / "current.tmp"
    if tmp_link.exists() or tmp_link.is_symlink():
        tmp_link.unlink()
    tmp_link.symlink_to(target)
    os.replace(tmp_link, ROOT / "current")


def _health(timeout_sec: int = 30) -> int:
    for _ in range(timeout_sec):
        time.sleep(1)
        try:
            with urllib.request.urlopen(HEALTH_URL, timeout=3) as response:
                if response.status == 200:
                    return 200
        except Exception:  # noqa: BLE001 — сервис ещё поднимается
            continue
    return 0


def prechecks(new: Path, gateway_release: Optional[Path]) -> list[str]:
    problems: list[str] = []
    if not (new / "app").is_dir():
        problems.append("нет каталога app")
    manifest_path = new / "release-manifest.json"
    if not manifest_path.is_file():
        problems.append("нет release-manifest.json")
        return problems
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    venv_python = new / "venv/bin/python"
    if not venv_python.is_symlink():
        problems.append("venv/bin/python не симлинк")
    if not os.access(venv_python, os.X_OK):
        problems.append("venv/bin/python не исполняем")

    schema = (manifest.get("database_schema") or {}).get("target")
    if schema != 13:
        problems.append(f"целевая схема базы {schema!r} != 13: миграция запрещена")

    # Отпечаток пересчитывается ЗДЕСЬ, а не берётся из манифеста на веру.
    # Между сборкой и выкаткой каталог мог измениться: правка «на живую» в
    # уже собранном релизе, переброшенный симлинк, снятый бит чтения для
    # прочих. Импорт такое пропустит, здоровье — тем более: 200 отдаёт любой
    # процесс на порту. Тогда рецепт объявил бы успешной выкатку не того,
    # что собрано.
    recorded = manifest.get("fileset_sha256")
    if recorded:
        try:
            actual = fileset_digest(new / "app")
        except (OSError, ValueError) as exc:
            problems.append(f"дерево релиза непригодно для отпечатка: {exc}")
        else:
            if actual != recorded:
                problems.append(
                    "дерево релиза не совпадает с манифестом: "
                    f"{actual[:12]}… вместо {str(recorded)[:12]}…"
                )
    else:
        # Релизы, собранные сборщиком из /tmp, отпечатка не несут. Отказывать
        # из-за этого нельзя — именно на такой релиз откатываются, — но и
        # молчать о непроверяемости не следует.
        print("ВНИМАНИЕ: в манифесте нет fileset_sha256, подмена дерева "
              "не проверяется (релиз старого сборщика)", file=sys.stderr)

    # Обязательный набор проверяется ВСЕГДА, независимо от отпечатка. Иначе
    # релиз без `frontend/static/js/distributed-feature.js` проходит и импорт,
    # и здоровье (200 отдаёт любой процесс на порту), а распределённый экран
    # ломается — то есть выкатывается то, что прежний сборщик отвергал.
    for rel in REQUIRED_PATHS:
        if not (new / "app" / rel).exists():
            problems.append(f"нет обязательного пути: {rel}")
    for probe_dir in (new, new / "app", new / "venv", new / "venv/bin"):
        # Нужны ОБА бита: без `x` шлюз не войдёт в каталог, даже видя его.
        if probe_dir.exists() and (os.stat(probe_dir).st_mode & 0o005) != 0o005:
            problems.append(f"каталог закрыт для прочих: {probe_dir}")

    runtime_tools = Path(
        os.environ.get("NORMS_TOOLS_PATH", str(DEFAULT_NORMS_RUNTIME_TOOLS))
    ).expanduser()
    runtime_python = Path(
        os.environ.get(
            "NORMS_MCP_PYTHON",
            str(runtime_tools / "venv" / "bin" / "python"),
        )
    ).expanduser()
    for item in runtime_problems(
        code_tools_path=new / "app" / "norms" / "tools",
        runtime_tools_path=runtime_tools,
        python_path=runtime_python,
    ):
        problems.append(f"norms runtime: {item}")

    if gateway_release is not None:
        for rel in WIRE_FILES:
            ours, theirs = new / rel, gateway_release / rel
            if not theirs.is_file():
                problems.append(f"у шлюза нет {rel}")
            elif _sha(ours) != _sha(theirs):
                problems.append(f"провод разошёлся со шлюзом: {rel}")

    probe = subprocess.run(
        [str(venv_python), "-c", "import backend.app.main; import backend.app.agent_gateway"],
        cwd=str(new / "app"),
        env=dict(os.environ, PYTHONPATH=str(new / "app"), PYTHONDONTWRITEBYTECODE="1"),
        capture_output=True, text=True,
    )
    if probe.returncode != 0:
        tail = (probe.stdout + probe.stderr).strip().splitlines()[-1:] or [""]
        problems.append(f"релиз не импортируется из своего каталога: {tail[0]}")
    return problems


def running_gateway_release_dir() -> Path:
    """Каталог релиза, из которого РЕАЛЬНО работает шлюз сейчас.

    Спрашиваем systemd, а не оператора: параметр командной строки описывает
    намерение, а сверять провод надо с тем, что исполняется. Ошибиться здесь —
    значит переключить центр на несовместимый со шлюзом контракт потока.
    """
    value = subprocess.check_output(
        ["systemctl", "show", "auditmanager-agent-gateway.service",
         "-p", "WorkingDirectory", "--value"], text=True).strip()
    if not value:
        raise SystemExit("не удалось определить рабочий каталог шлюза")
    directory = Path(value)
    # WorkingDirectory указывает на <release>/app.
    return directory.parent if directory.name == "app" else directory


def running_release_dir() -> Optional[Path]:
    """Каталог релиза, из которого РЕАЛЬНО исполняется backend сейчас.

    Спрашиваем systemd про MainPID и ядро про рабочий каталог этого процесса.
    Возвращает None, если узнать не удалось: диагностика не вправе валить
    выкатку, которая в остальном прошла.
    """
    try:
        pid = int(subprocess.check_output(
            ["systemctl", "--user", "show", SERVICE, "-p", "MainPID", "--value"],
            text=True).strip() or 0)
        if pid <= 0:
            return None
        cwd = Path(f"/proc/{pid}/cwd").resolve()
    except (OSError, ValueError, subprocess.CalledProcessError):
        return None
    # WorkingDirectory указывает на <release>/app.
    return cwd.parent if cwd.name == "app" else cwd


def _verify_release_source(release_dir: Path) -> dict[str, object]:
    """Коммит релиза обязан быть опубликован в канонической ветке.

    Читается манифест собираемого релиза, а не рабочее дерево: между сборкой и
    переключением может пройти час и смениться ветка, а переключаем мы именно
    эти байты.
    """
    manifest_path = release_dir / "release-manifest.json"
    if not manifest_path.is_file():
        raise SystemExit(f"нет манифеста релиза: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    commit = str(manifest.get("commit") or "")
    if not commit:
        raise SystemExit(
            f"в манифесте {manifest_path} нет поля commit: происхождение "
            "релиза недоказуемо, переключение запрещено"
        )
    return verify_production_source(REPO_ROOT, commit=commit)


def deploy(release_id: str, *, gateway_release_dir: str = "", milestone: str = "",
           dry_run: bool = False) -> dict[str, object]:
    new = ROOT / "releases" / release_id
    # Сверка провода со шлюзом НЕ отключаема. Каталог берётся у systemd —
    # у того, что исполняется, а не у оператора. Явный параметр остаётся, но
    # он только СВЕРЯЕТСЯ с фактом: ошибиться здесь значит переключить центр
    # на контракт потока, несовместимый с работающим шлюзом.
    gateway = running_gateway_release_dir()
    if gateway_release_dir and Path(gateway_release_dir).resolve() != gateway.resolve():
        raise SystemExit(
            f"--gateway-release-dir={gateway_release_dir} не совпадает с "
            f"работающим шлюзом ({gateway})"
        )

    # Страж происхождения ДО замка и до любой мутации указателя. Коммит берётся
    # из манифеста САМОГО релиза, а не из текущего HEAD: переключать `current`
    # на релиз, собранный из неопубликованного коммита, запрещено даже если
    # дерево-источник сейчас в порядке. Так закрывается и обратный случай —
    # релиз, собранный до появления стража.
    source_receipt = _verify_release_source(new)

    with deploy_lock(COMPONENT_CENTER, operation="deploy", release=release_id,
                     milestone=milestone):
        # Текущий указатель читается ВНУТРИ замка: снятый заранее, он мог бы
        # описывать состояние до чужой успешной выкатки, и наш откат вернул бы
        # прод на позапрошлый релиз.
        previous = (
            Path(os.readlink(ROOT / "current")) if (ROOT / "current").is_symlink() else None
        )
        problems = prechecks(new, gateway)
        if problems:
            for item in problems:
                print(f"ПРЕДПРОВЕРКА НЕ ПРОШЛА: {item}", file=sys.stderr)
            raise SystemExit("боевое состояние не тронуто")
        print("предпроверки: OK (боевое состояние ещё не тронуто)")
        if dry_run:
            return {"dry_run": True, "release": release_id, "prechecks": "PASS"}

        _switch(new)
        print(f"current -> {os.readlink(ROOT / 'current')}")
        try:
            _systemctl_user("restart", SERVICE)
            code = _health()
            if code != 200:
                raise RuntimeError(f"здоровье не подтверждено: HTTP {code}")
            # HTTP 200 доказывает, что НА ПОРТУ кто-то жив, а не что жив
            # именно новый релиз: старый процесс мог пережить рестарт и
            # продолжать отвечать. Спрашиваем ядро, из какого каталога работает
            # служба, и НЕ считаем незнание успехом: недоказанная выкатка — это
            # выкатка, о которой рецепт напишет неправду.
            running = running_release_dir()
            if running is None:
                raise RuntimeError(
                    "не удалось доказать, из какого релиза работает служба "
                    "(MainPID или /proc недоступны)"
                )
            if running != new.resolve():
                raise RuntimeError(f"служба работает из {running}, а не из {new}")
        except Exception as exc:  # noqa: BLE001 — сюда же попадает отказ restart
            # Отказ САМОГО restart раньше проскакивал мимо отката: исключение
            # улетало наружу, новый указатель оставался активным, а backend мог
            # быть уже остановлен. Любой исход после переключения обязан вести
            # к возврату в предыдущее состояние — и всё это ДО снятия замка.
            print(f"ВЫКАТКА НЕ УДАЛАСЬ ({exc}) — откат", file=sys.stderr)
            if previous is not None:
                _switch(previous)
                try:
                    _systemctl_user("restart", SERVICE)
                except Exception as restart_exc:  # noqa: BLE001
                    print(f"рестарт при откате не удался: {restart_exc}", file=sys.stderr)
                rolled = _health()
                back = running_release_dir()
                print(f"откат выполнен, здоровье старого релиза: HTTP {rolled}, "
                      f"работает из {back}", file=sys.stderr)
                if rolled != 200:
                    print("ВНИМАНИЕ: старый релиз тоже не отвечает", file=sys.stderr)
                elif back is None or back != previous.resolve():
                    # Замок ещё наш, и это последняя точка, где о неполном
                    # откате можно сказать вслух.
                    print(f"ВНИМАНИЕ: после отката служба работает из {back}, "
                          f"а ожидался {previous}", file=sys.stderr)
            # Замок держится до конца отката: снять его раньше значило бы
            # пустить чужую выкатку в незастабилизированное состояние.
            raise SystemExit("выкатка отменена")

        receipt = {
            "schema": "auditmanager.deploy_receipt.v1",
            "release_id": release_id,
            "before": str(previous) if previous else None,
            "after": str(new),
            "completed_at": time.time(),
            "backend_restarted": True,
            "gateway_restarted": False,
            "deploy_tool": "scripts/deploy_center_release.py",
            "deploy_lock": "scripts/deploy_lock.py",
            "production_source_guard": "scripts/production_source_guard.py",
            "production_source_canonical": True,
            "production_source_commit": source_receipt["source_commit"],
            "canonical_remote_branch": (
                f"{source_receipt['canonical_remote']}/"
                f"{source_receipt['canonical_branch']}"
            ),
            "canonical_remote_head": source_receipt["canonical_remote_head"],
            "milestone": milestone,
            "push_merge": "YES/CANONICAL",
        }
        stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        path = ROOT / "deploy-receipts" / f"{milestone or 'center'}-deploy-{stamp}.json"
        path.write_text(json.dumps(receipt, ensure_ascii=False, indent=2) + "\n",
                        encoding="utf-8")
        print(f"рецепт: {path}")
        return {"release": release_id, "health": code, "receipt": str(path)}


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release", required=True)
    parser.add_argument("--gateway-release-dir", default="",
                        help="необязательная СВЕРКА: каталог всё равно берётся "
                             "из работающего юнита шлюза")
    parser.add_argument("--milestone", default="")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    print(json.dumps(deploy(args.release, gateway_release_dir=args.gateway_release_dir,
                            milestone=args.milestone, dry_run=args.dry_run),
                     ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
