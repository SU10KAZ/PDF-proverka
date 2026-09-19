#!/usr/bin/env python3
"""Доустановить runtime-зависимости по lock-файлу в venv СОБИРАЕМОГО релиза.

Сборщик релиза клонирует venv донора и никогда не делает `pip install`,
поэтому строка в requirements.txt сама до боевого venv не доезжает. Этот
скрипт — единственный документированный путь добавить пакет: только из
локального каталога колёс, только по хешам из lock-файла, без сети, без
зависимостей сверх перечисленных, без байткода (файлы пакетов побайтно равны
колёсам). После установки — `pip check`, проверка импорта и повторная печать
venv в вид готового релиза (`seal_venv`).

Живой релиз не трогается никогда: venv внутри `releases/` или за симлинком
`current` отвергается до любой записи. Уже существующие файлы venv меняться не
вправе — дельта обязана состоять только из новых путей, иначе отказ.

    python3 scripts/install_release_runtime_deps.py \
        --venv <staging-release>/venv \
        --lock requirements-projectchange-v3-runtime.lock.txt \
        --wheelhouse <каталог колёс> --receipt <квитанция.json>
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
from scripts.build_center_release import seal_venv  # noqa: E402

AUDITMANAGER_ROOT = Path(os.environ.get("AUDITMANAGER_ROOT", "/home/coder/auditmanager"))
_REQ = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)==([^\s\\]+)")
_HASH = re.compile(r"--hash=sha256:([0-9a-f]{64})")


class InstallRefused(RuntimeError):
    """Установка отвергнута до любой записи в venv."""


def canonical(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def parse_lock(text: str) -> dict[str, dict[str, object]]:
    """{каноническое имя: {version, hashes}}; каждая строка обязана иметь хеш."""
    logical: list[str] = []
    buffer = ""
    for raw in text.splitlines():
        line = raw.rstrip()
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if line.endswith("\\"):
            buffer += line[:-1] + " "
            continue
        logical.append(buffer + line)
        buffer = ""
    if buffer.strip():
        logical.append(buffer)
    pins: dict[str, dict[str, object]] = {}
    for line in logical:
        match = _REQ.match(line.strip())
        if not match:
            raise InstallRefused(f"строка lock-файла без точной версии: {line.strip()[:80]}")
        hashes = _HASH.findall(line)
        if not hashes:
            raise InstallRefused(f"строка lock-файла без sha256: {match.group(1)}")
        name = canonical(match.group(1))
        if name in pins:
            raise InstallRefused(f"пакет указан дважды: {name}")
        pins[name] = {"version": match.group(2), "hashes": hashes}
    if not pins:
        raise InstallRefused("lock-файл пуст")
    return pins


def refuse_live_target(venv: Path, root: Path = AUDITMANAGER_ROOT) -> None:
    """Писать можно только в venv релиза, который ещё собирается."""
    release = venv.resolve().parent
    releases = (root / "releases").resolve()
    if release == releases or releases in release.parents:
        raise InstallRefused(f"venv принадлежит готовому релизу ({release}); готовые релизы неизменяемы")
    current = root / "current"
    if current.exists() and current.resolve() == release:
        raise InstallRefused("venv принадлежит боевому релизу (current)")
    if not (venv / "bin" / "python").exists():
        raise InstallRefused(f"не venv: {venv}")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def snapshot(venv: Path) -> dict[str, str]:
    """Путь → содержимое (sha256 / цель симлинка / 'D'); права не входят (их ставит seal_venv)."""
    out: dict[str, str] = {}
    for path in sorted(venv.rglob("*")):
        rel = str(path.relative_to(venv))
        if path.is_symlink():
            out[rel] = "L:" + os.readlink(path)
        elif path.is_dir():
            out[rel] = "D"
        else:
            out[rel] = "F:" + sha256_file(path)
    return out


def wheel_hashes(wheelhouse: Path) -> dict[str, str]:
    return {p.name: sha256_file(p) for p in sorted(wheelhouse.glob("*.whl"))}


def _run(cmd: list[str]) -> subprocess.CompletedProcess:
    env = dict(os.environ, PIP_DISABLE_PIP_VERSION_CHECK="1", PYTHONDONTWRITEBYTECODE="1",
               PIP_NO_INPUT="1", PIP_CONFIG_FILE=os.devnull)
    env.pop("PIP_INDEX_URL", None)
    env.pop("PIP_EXTRA_INDEX_URL", None)
    return subprocess.run(cmd, capture_output=True, text=True, env=env, cwd="/")


def install(venv: Path, lock: Path, wheelhouse: Path, receipt: dict[str, object] | None = None) -> dict[str, object]:
    """Установить lock в venv; квитанция заполняется и при отказе (передаётся снаружи)."""
    venv, lock, wheelhouse = venv.absolute(), lock.resolve(), wheelhouse.resolve()  # pip идёт из cwd=/
    refuse_live_target(venv)
    pins = parse_lock(lock.read_text(encoding="utf-8"))
    wheels = wheel_hashes(wheelhouse)
    allowed = {h for pin in pins.values() for h in pin["hashes"]}  # type: ignore[union-attr]
    stray = sorted(name for name, digest in wheels.items() if digest not in allowed)
    if stray:
        raise InstallRefused(f"в каталоге колёс есть колёса вне lock-файла: {stray}")
    python = venv / "bin" / "python"
    before = snapshot(venv)
    receipt = {} if receipt is None else receipt
    try:
        # Права вернёт seal_venv; на время установки каталоги venv открыты на запись владельцу.
        for path in [venv, *venv.rglob("*")]:
            if path.is_dir() and not path.is_symlink():
                path.chmod(stat.S_IMODE(path.stat().st_mode) | 0o200)
        proc = _run([str(python), "-m", "pip", "install", "--no-index", "--no-deps", "--require-hashes",
                     "--no-compile", "--no-cache-dir", "--find-links", str(wheelhouse), "-r", str(lock)])
        receipt.update(pip_install_returncode=proc.returncode,
                       pip_install_output_tail=(proc.stdout + proc.stderr)[-3000:])
        if proc.returncode != 0:
            raise InstallRefused("pip install завершился ошибкой")
        after = snapshot(venv)
        changed = sorted(p for p in before if p in after and after[p] != before[p])
        removed = sorted(p for p in before if p not in after)
        added = sorted(p for p in after if p not in before)
        receipt.update(venv_paths_added=len(added), venv_paths_changed=changed, venv_paths_removed=removed)
        if changed or removed:
            raise InstallRefused("установка изменила существующие файлы venv")
        check = _run([str(python), "-m", "pip", "check"])
        freeze = _run([str(python), "-m", "pip", "freeze", "--all"])
        versions = _run([str(python), "-c", "import importlib.metadata as m, json, sys; "
                         "print(json.dumps({n: m.version(n) for n in sys.argv[1:]}))", *pins])
        probe = _run([str(python), "-c", "import jsonschema, importlib.metadata as m; "
                      "jsonschema.validate({'a': 1}, {'type': 'object', 'required': ['a']}); "
                      "print(m.version('jsonschema'))"])
        installed = json.loads(versions.stdout) if versions.returncode == 0 else {}
        receipt.update(
            pip_check_returncode=check.returncode,
            pip_check_output=(check.stdout + check.stderr).strip(),
            pip_freeze=freeze.stdout.splitlines(),
            installed_versions=installed,
            import_probe={"returncode": probe.returncode, "output": (probe.stdout + probe.stderr).strip()},
            added_files_sha256={p: after[p][2:] for p in added if after[p].startswith("F:")},
        )
        mismatched = {n: (installed.get(n), pin["version"]) for n, pin in pins.items()
                      if installed.get(n) != pin["version"]}
        if check.returncode != 0 or probe.returncode != 0 or mismatched:
            raise InstallRefused(f"проверка после установки не прошла: check={check.returncode} "
                                 f"import={probe.returncode} versions={mismatched}")
    finally:
        seal_venv(venv)
    return receipt


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--venv", type=Path, required=True)
    parser.add_argument("--lock", type=Path, required=True)
    parser.add_argument("--wheelhouse", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args(argv)
    receipt: dict[str, object] = {
        "schema": "release-runtime-deps-install/1",
        "at": datetime.now(timezone.utc).isoformat(),
        "venv": str(args.venv),
        "lock": str(args.lock),
        "lock_sha256": sha256_file(args.lock),
        "wheelhouse": str(args.wheelhouse),
        "wheels_sha256": wheel_hashes(args.wheelhouse),
        "method": "pip install --no-index --no-deps --require-hashes --no-compile --find-links <wheelhouse> -r <lock>",
        "network": False,
    }
    try:
        install(args.venv, args.lock, args.wheelhouse, receipt)
        receipt["status"] = "PASS"
    except InstallRefused as exc:
        receipt.update(status="REFUSED", error=str(exc))
    args.receipt.write_text(json.dumps(receipt, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: receipt.get(k) for k in ("status", "error", "installed_versions", "pip_check_returncode")},
                     ensure_ascii=False))
    return 0 if receipt["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
