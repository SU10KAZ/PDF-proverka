#!/usr/bin/env python3
"""Атомарно установить общий нормативный runtime для immutable releases.

По умолчанию создаёт ``/home/coder/auditmanager/shared/norms``. Существующий
target никогда не изменяется и не удаляется: обновление готовится отдельным
каталогом и переключается только явной операцией оператора.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from norms.runtime import runtime_problems  # noqa: E402

DEFAULT_TARGET = Path("/home/coder/auditmanager/shared/norms")
DEFAULT_PYTHON = Path("/opt/cpython-3.12/bin/python3.12")
TORCH_CPU_INDEX = "https://download.pytorch.org/whl/cpu"
TORCH_CPU_VERSION = "2.11.0+cpu"
REQUIRED_DATA_FILES = ("status_index.json", "paragraphs_embeddings.npz")
# Кеш цитат пунктов пополняется прогонами, поэтому ставится рядом с vault,
# а не в каталоге кода релиза (тот только на чтение).
SEEDED_STATE_FILES = ("norms_paragraphs.json",)
OPTIONAL_DATA_FILES = (
    "paragraphs.jsonl",
    "embeddings.npz",
    "active_norms.json",
    "refs_graph.json",
    "semantic_neighbors.json",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _copy(source: Path, target: Path) -> None:
    """Использовать reflink, когда его поддерживает файловая система."""
    completed = subprocess.run(
        ["cp", "--reflink=auto", "-a", str(source), str(target)],
        capture_output=True,
        text=True,
    )
    if completed.returncode == 0:
        return
    if source.is_dir():
        shutil.copytree(source, target, symlinks=True)
    else:
        shutil.copy2(source, target)


def _check_source(source_norms: Path, python: Path) -> None:
    tools = source_norms / "tools"
    missing = [
        path
        for path in (
            source_norms / "vault",
            tools / "runtime-requirements.txt",
            *(tools / name for name in REQUIRED_DATA_FILES),
        )
        if not path.exists()
    ]
    if missing:
        raise SystemExit("неполный source norms: " + ", ".join(map(str, missing)))
    if not python.is_file() or not os.access(python, os.X_OK):
        raise SystemExit(f"Python 3.12 не исполняем: {python}")
    version = subprocess.check_output(
        [
            str(python),
            "-c",
            "import sys; print(f'{sys.version_info[0]}.{sys.version_info[1]}')",
        ],
        text=True,
    ).strip()
    if version != "3.12":
        raise SystemExit(f"нужен Python 3.12, получен {version}: {python}")


def install(*, source_norms: Path, target: Path, python: Path) -> dict[str, object]:
    source_norms = source_norms.resolve()
    target = target.absolute()
    python = python.resolve()
    if target.is_symlink():
        raise SystemExit(f"target не может быть символической ссылкой: {target}")
    if target.exists():
        raise SystemExit(
            f"target уже существует и не будет изменён: {target}. "
            "Укажите новый каталог и переключите его отдельно."
        )
    _check_source(source_norms, python)

    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".norms-runtime-", dir=target.parent))
    try:
        runtime_tools = staging / "tools"
        runtime_tools.mkdir()
        _copy(source_norms / "vault", staging / "vault")
        source_tools = source_norms / "tools"
        copied: list[str] = []
        for name in (*REQUIRED_DATA_FILES, *OPTIONAL_DATA_FILES):
            source = source_tools / name
            if not source.is_file():
                continue
            _copy(source, runtime_tools / name)
            copied.append(name)

        # Состояние, которое прогоны ДОПИСЫВАЮТ: ставим рядом с vault, чтобы
        # запись не упиралась в read-only каталог кода релиза.
        for name in SEEDED_STATE_FILES:
            source = source_norms / name
            if source.is_file():
                _copy(source, staging / name)
                copied.append(name)

        subprocess.run(
            [str(python), "-m", "venv", str(runtime_tools / "venv")],
            check=True,
        )
        runtime_python = runtime_tools / "venv" / "bin" / "python"
        subprocess.run(
            [
                str(runtime_python), "-m", "pip", "install", "--no-cache-dir",
                "--index-url", TORCH_CPU_INDEX, f"torch=={TORCH_CPU_VERSION}",
            ],
            check=True,
        )
        subprocess.run(
            [
                str(runtime_python), "-m", "pip", "install", "--no-cache-dir",
                "-r", str(source_tools / "runtime-requirements.txt"),
            ],
            check=True,
        )

        problems = runtime_problems(
            code_tools_path=source_tools,
            runtime_tools_path=runtime_tools,
            python_path=runtime_python,
            timeout_sec=60,
        )
        if problems:
            raise RuntimeError("runtime smoke-test: " + "; ".join(problems))

        package_probe = (
            "import importlib.metadata,json;"
            "names=('mcp','numpy','PyYAML','pydantic','pydantic-settings',"
            "'sentence-transformers','torch','transformers');"
            "print(json.dumps({n:importlib.metadata.version(n) for n in names}))"
        )
        packages = json.loads(
            subprocess.check_output(
                [str(runtime_python), "-c", package_probe], text=True
            )
        )
        manifest = {
            "schema": "auditmanager.norms-runtime.v1",
            "created_at": dt.datetime.now().astimezone().isoformat(),
            "source_norms": str(source_norms),
            "python": str(python),
            "runtime_python": "tools/venv/bin/python",
            "packages": packages,
            "requirements_sha256": _sha256(
                source_tools / "runtime-requirements.txt"
            ),
            "data_files": copied,
            "status_index_sha256": _sha256(runtime_tools / "status_index.json"),
            "paragraphs_embeddings_sha256": _sha256(
                runtime_tools / "paragraphs_embeddings.npz"
            ),
        }
        (staging / "runtime-manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        os.rename(staging, target)
        return {**manifest, "target": str(target)}
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-norms", type=Path, default=REPO_ROOT / "norms")
    parser.add_argument("--target", type=Path, default=DEFAULT_TARGET)
    parser.add_argument("--python", type=Path, default=DEFAULT_PYTHON)
    args = parser.parse_args()
    result = install(
        source_norms=args.source_norms,
        target=args.target,
        python=args.python,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    print(f"NORMS_TOOLS_PATH={args.target.absolute() / 'tools'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
