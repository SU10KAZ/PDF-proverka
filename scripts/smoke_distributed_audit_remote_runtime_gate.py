#!/usr/bin/env python3
"""Живой smoke удалённой ноги: Executor → дочерний процесс → настоящие этапы.

**Что этот прогон доказывает, чего не доказывают тесты.** Все дефекты
предыдущего этапа лежали на стыке процессов: пакет, распакованный на воркере,
резолвился не тем резолвером; конфигурация бралась с чужой машины; запись
уходила в каталог установленного кода. Ни один юнит-тест этого не видел —
477 зелёных тестов сосуществовали с удалённой ногой, которая не могла
стартовать вовсе. Поэтому здесь всё настоящее:

  * настоящий сборщик пакета и настоящий манифест;
  * настоящая безопасная распаковка воркера;
  * настоящая `worker.db` и настоящая локальная очередь;
  * настоящий `python -m audit_worker executor` ОТДЕЛЬНЫМ процессом;
  * настоящий дочерний процесс конвейера, который порождает ИСПОЛНИТЕЛЬ;
  * настоящий `PipelineManager._dispatch_action` и настоящие stage-runner'ы;
  * поддельные провайдеры — ТОЛЬКО на внешней границе, отдельными процессами.

Заменить исполнителя прямым вызовом runner-функции нельзя: именно граница
«исполнитель → дочерний процесс» и есть то место, где ломалось всё
предыдущее.

**Три рубежа, и каждый умеет провалить прогон:**

  1. сетевой guard — любое соединение вне loopback убивает процесс (код 97);
  2. сторож записи — любая запись вне каталога попытки убивает процесс (96);
  3. корень установленного кода делается read-only на время прогона.

У первых двух есть ОБЯЗАТЕЛЬНАЯ самопроверка: «в логе пусто» без неё означает
что угодно, в том числе «сторож не подхватился».

Запуск:  python scripts/smoke_distributed_audit_remote_runtime_gate.py [--keep]

Ненулевой код возврата = нарушение. Реальные Claude/Codex/OpenRouter не
вызываются; VPS не подключается.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

PY = sys.executable or "python3"

# Точки проверки. Печатаются в порядке выполнения; первая же провалившаяся
# останавливает прогон — продолжать после нарушения изоляции бессмысленно.
_CHECKS: list[tuple[str, bool, str]] = []
_FAILED = False


def check(ok: bool, title: str, detail: str = "") -> bool:
    global _FAILED
    _CHECKS.append((title, bool(ok), detail))
    mark = "OK  " if ok else "СБОЙ"
    line = f"[{mark}] {title}"
    if detail:
        line += f" — {detail}"
    print(line, flush=True)
    if not ok:
        _FAILED = True
    return bool(ok)


def fatal(title: str, detail: str = "") -> None:
    check(False, title, detail)
    raise SystemExit(_finish())


def _finish() -> int:
    passed = sum(1 for _, ok, _ in _CHECKS if ok)
    print("\n" + "=" * 78)
    print(f"ИТОГ: {passed}/{len(_CHECKS)} проверок пройдено")
    if _FAILED:
        print("НАРУШЕНИЯ:")
        for title, ok, detail in _CHECKS:
            if not ok:
                print(f"  • {title}" + (f" — {detail}" if detail else ""))
    print("=" * 78)
    return 1 if _FAILED else 0


# ─── Стенд ───────────────────────────────────────────────────────────────────
class Stand:
    """Каталоги, окружение и процессы одного прогона."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.central_v2 = root / "central" / "projects_v2"
        self.local_case = root / "local_case"
        self.worker_root = root / "worker"
        self.packages = root / "packages"
        self.guard_dir = root / "guard"
        self.home = root / "home"
        self.tmp = root / "tmp"
        self.providers = root / "fake_providers"
        self.evidence = root / "evidence"
        self.netguard_log = self.evidence / "netguard.log"
        self.writeguard_log = self.evidence / "writeguard.log"
        self.executor: Optional[subprocess.Popen] = None
        self._code_root_ro = False
        for path in (
            self.central_v2, self.local_case, self.worker_root, self.packages,
            self.guard_dir, self.home, self.tmp, self.providers, self.evidence,
        ):
            path.mkdir(parents=True, exist_ok=True)

    # ── окружение ───────────────────────────────────────────────────────────
    def base_env(self, *, writeguard_allow: list[Path]) -> dict[str, str]:
        from tests.distributed_audit_e2e import isolation

        env = isolation.build_process_env(
            repo_root=REPO_ROOT,
            home=self.home,
            tmp_dir=self.tmp,
            netguard_dir=self.guard_dir,
            netguard_log=self.netguard_log,
        )
        env["E2E_WRITEGUARD"] = "1"
        env["E2E_WRITEGUARD_LOG"] = str(self.writeguard_log)
        env["E2E_WRITEGUARD_ALLOW"] = os.pathsep.join(
            str(Path(p).resolve()) for p in writeguard_allow
        )
        env["AUDIT_WORKER_ALLOW_REAL_LLM"] = "false"
        return env

    def stop(self) -> None:
        proc = self.executor
        if proc is None or proc.poll() is not None:
            return
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except (OSError, ProcessLookupError):
            try:
                proc.terminate()
            except OSError:
                pass
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except (OSError, ProcessLookupError):
                pass

    # ── read-only корень кода ───────────────────────────────────────────────
    def freeze_code_root(self) -> bool:
        """Сделать корень установленного кода нечувствительным к записи.

        Полный `chmod -R` по репозиторию был бы и медленным, и опасным
        (worktree чужой работы рядом). Достаточно ВЕРХНЕГО уровня: именно туда
        целится `comparison/` — каталог создаётся в корне, и без права записи
        на сам корень его создать нельзя.
        """
        try:
            self._code_root_mode = REPO_ROOT.stat().st_mode
            os.chmod(REPO_ROOT, 0o555)
            self._code_root_ro = True
        except OSError:
            return False
        return True

    def unfreeze_code_root(self) -> None:
        if self._code_root_ro:
            try:
                os.chmod(REPO_ROOT, self._code_root_mode)
            except OSError:
                pass
            self._code_root_ro = False


def _wait_for(predicate, *, timeout: float, interval: float = 0.5) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def _sha256(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with Path(path).open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


# ─── Семантическая проекция pre-norm ─────────────────────────────────────────
#: Что сравнивается между локальным и удалённым прогоном. Список УЗКИЙ
#: намеренно: полное семантическое сравнение — следующий этап, здесь
#: проверяется только то, что удалённая нога дошла до той же границы с тем же
#: содержательным результатом.
PRE_NORM_ARTIFACTS = ("03_findings.json", "01_blocks_analysis.json",
                      "02_text_analysis.json")

#: Ключи, различие которых допустимо: они меняются от прогона к прогону по
#: построению и содержательного смысла не несут.
_VOLATILE_KEYS = frozenset({
    "generated_at", "created_at", "finished_at", "started_at", "completed_at",
    "timestamp", "duration_sec", "duration_ms", "elapsed", "job_id",
    "attempt_id", "run_id", "pid", "project_dir", "output_dir", "artifacts_dir",
    "runtime_plan_path", "path", "file_path", "_meta", "meta", "usage",
    "cost_usd", "tokens", "input_tokens", "output_tokens", "model_calls",
})


#: Хвосты имён, означающие «это измерение времени». Перечислять такие ключи
#: поимённо бессмысленно: их добавляют этапы по мере появления, и список
#: отставал бы навсегда. Первым же прогоном так и вышло — расхождение
#: `stage01_meta.wall_clock_s` 0.6 против 0.5.
_VOLATILE_SUFFIXES = ("_s", "_ms", "_sec", "_secs", "_seconds", "_at", "_time")
_VOLATILE_SUBSTRINGS = ("wall_clock", "duration", "elapsed", "latency", "timing")


def _is_volatile(key: str) -> bool:
    name = str(key)
    if name in _VOLATILE_KEYS:
        return True
    lowered = name.lower()
    if any(part in lowered for part in _VOLATILE_SUBSTRINGS):
        return True
    return lowered.endswith(_VOLATILE_SUFFIXES)


def semantic_projection(value: Any) -> Any:
    """Убрать из артефакта всё, что обязано различаться, и ничего больше.

    Абсолютные пути вычищаются по значению, а не по имени ключа: путь к
    каталогу попытки попадает в артефакты в разных полях, и перечислять их
    поимённо значило бы догонять список вечно.
    """
    if isinstance(value, dict):
        return {
            key: semantic_projection(item)
            for key, item in sorted(value.items())
            if not _is_volatile(key)
        }
    if isinstance(value, list):
        return [semantic_projection(item) for item in value]
    if isinstance(value, str) and value.startswith("/") and len(value) > 1:
        return "<path>"
    if isinstance(value, float):
        return round(value, 6)
    return value


def load_projection(directory: Path) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for name in PRE_NORM_ARTIFACTS:
        path = Path(directory) / name
        if not path.is_file():
            out[name] = None
            continue
        try:
            out[name] = semantic_projection(json.loads(path.read_text(encoding="utf-8")))
        except (OSError, ValueError) as exc:
            out[name] = {"_unreadable": str(exc)}
    return out


def findings_count(directory: Path) -> int:
    path = Path(directory) / "03_findings.json"
    if not path.is_file():
        return -1
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return -1
    if isinstance(data, dict):
        for key in ("findings", "items", "results"):
            if isinstance(data.get(key), list):
                return len(data[key])
    return len(data) if isinstance(data, list) else 0


# ─── Шаги прогона ────────────────────────────────────────────────────────────
def build_fixtures(stand: Stand):
    from tests.distributed_audit_e2e import fixture as fx

    central = fx.build_project_fixture(stand.central_v2)
    local = fx.clone_fixture(central, stand.local_case / "projects_v2")
    check(central.version_dir.is_dir(), "фикстура projects_v2 создана",
          str(central.version_dir.relative_to(stand.root)))
    # Вариант БЕЗ 99_service: раскладка версий в корпусе неоднородна, и пакет
    # обязан собираться на обоих.
    no_service = fx.build_project_fixture(
        stand.root / "central_no_service" / "projects_v2",
        document_code="ТЕСТ-РД-АР2-К2",
    )
    shutil.rmtree(no_service.version_dir / "99_service", ignore_errors=True)
    check(not (no_service.version_dir / "99_service").exists(),
          "вторая фикстура без 99_service подготовлена")
    return central, local, no_service


#: Профиль флагов попытки. Минимальный и явный: снимок обязан быть
#: воспроизводимым, а `collect_feature_flags_snapshot()` берёт окружение
#: ТЕКУЩЕГО процесса и от прогона к прогону меняется.
SMOKE_FEATURE_FLAGS = {"AUDIT_ROLE": "worker"}


def build_config_snapshot(stand: Stand) -> dict[str, Any]:
    """Снимок промптов и моделей — тем же кодом, которым его делает центр.

    Хэши обязаны совпасть с тем, что воркер пересчитает после распаковки
    (`verify_snapshot`). Синтетические значения здесь означали бы прогон,
    который падает на сверке снимка и до конвейера не доходит вовсе.
    """
    from backend.app.services.distributed_workers import project_package
    from tests.distributed_audit_e2e import fixture as fx

    staging = stand.root / "snapshot_source"
    prompts_dir = fx.prompts_snapshot_dir(REPO_ROOT, staging / "prompts")
    models_file = fx.stage_models_snapshot(staging / "stage_models.json")

    prompts = project_package.collect_prompt_snapshot(prompts_dir)
    models = project_package.collect_model_config_snapshot(models_file)
    return {
        "files": {**prompts, **models},
        "feature_flags": dict(SMOKE_FEATURE_FLAGS),
        "prompt_bundle_hash": project_package.hash_files(prompts),
        "model_config_hash": project_package.hash_files(models),
        "feature_flags_hash": project_package.hash_json(SMOKE_FEATURE_FLAGS),
        "stage_models": json.loads(models_file.read_text(encoding="utf-8")),
    }


def build_runtime_snapshot(revision: str, config_snapshot: dict[str, Any]):
    from backend.app.services.distributed_workers import project_package, runtime_config

    return runtime_config.build_snapshot(
        pipeline_revision=revision,
        protocol_version=1,
        package_manifest_version=1,
        execution_profile="remote_audit_pilot_v1",
        project_layout_version=project_package.PROJECT_LAYOUT_VERSION,
        # Ключевая точка: центр объявляет V2, а окружение ХОСТА воркера ниже
        # будет выставлено в legacy. Победить обязан пакет.
        projects_v2_write_mode="projects_v2_primary",
        provider_mode="fake",
        stage_model_mapping=config_snapshot["stage_models"],
        prompt_bundle_hash=config_snapshot["prompt_bundle_hash"],
        model_config_hash=config_snapshot["model_config_hash"],
        feature_flags=config_snapshot["feature_flags"],
        feature_flags_hash=config_snapshot["feature_flags_hash"],
        created_at=1.0,
    )


def build_source_package(stand: Stand, fixture, snapshot, config_snapshot,
                         *, job_id, attempt_id) -> dict:
    from backend.app.services.distributed_workers import project_package

    dest = stand.packages / f"src_{attempt_id}.tar.gz"
    return project_package.build_project_source_package(
        dest_path=dest,
        version_dir=fixture.version_dir,
        manifest_base={
            "manifest_version": 1,
            "package_id": f"pkg_{attempt_id}",
            "job_id": job_id,
            "attempt_id": attempt_id,
            "project_id": fixture.project_id,
            "project_external_id": fixture.external_id,
            "version_id": fixture.version_id,
            "job_type": "audit_pipeline_v1",
            "execution_profile": "remote_audit_pilot_v1",
            "pipeline_revision": snapshot.pipeline_revision,
            "runtime_snapshot_hash": snapshot.snapshot_hash(),
        },
        snapshot_files=dict(config_snapshot["files"]),
        feature_flags=dict(config_snapshot["feature_flags"]),
        runtime_config=snapshot.to_package_bytes(),
    )


def unpack_like_agent(stand: Stand, manifest: dict, archive: Path, job_dir: Path) -> None:
    """Пройти распаковку ТЕМ ЖЕ кодом, которым её делает агент."""
    from audit_worker import package_io

    staging = job_dir / "unpack_staging"
    staging.mkdir(parents=True, exist_ok=True)
    package_io.verify_and_unpack(
        archive=archive,
        expected_sha256=manifest["archive"]["sha256"],
        work_dir=staging,
        compression=manifest.get("compression"),
    )
    package_io.require_portable_layout(manifest, staging)
    for source_name, dest_name in package_io.AUDIT_PACKAGE_SECTIONS:
        source = staging / source_name
        if not source.is_dir():
            continue
        destination = job_dir / dest_name
        shutil.rmtree(destination, ignore_errors=True)
        destination.parent.mkdir(parents=True, exist_ok=True)
        os.replace(source, destination)


def seed_snapshot_dir(job_dir: Path, prompts_source: Path) -> None:
    """Промпты и модели попытки. Берутся НАСТОЯЩИЕ промпты репозитория."""
    from tests.distributed_audit_e2e import fixture as fx

    snapshot_dir = job_dir / "snapshot"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    fx.prompts_snapshot_dir(prompts_source, snapshot_dir / "prompts")
    fx.stage_models_snapshot(snapshot_dir / "stage_models.json")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep", action="store_true",
                        help="не удалять каталог прогона (для разбора)")
    parser.add_argument("--root", default=None)
    parser.add_argument("--timeout", type=float, default=1800.0)
    args = parser.parse_args()

    import tempfile

    root = Path(args.root) if args.root else Path(
        tempfile.mkdtemp(prefix="rrg_smoke_")
    )
    print(f"Каталог прогона: {root}\n")
    stand = Stand(root)
    try:
        return run(stand, timeout=args.timeout)
    finally:
        stand.unfreeze_code_root()
        stand.stop()
        if not args.keep:
            shutil.rmtree(root, ignore_errors=True)
        else:
            print(f"\nКаталог прогона сохранён: {root}")


def run(stand: Stand, *, timeout: float) -> int:      # noqa: C901 — сценарий линейный
    from tests.distributed_audit_e2e import fixture as fx, isolation
    from backend.app.pipeline.execution import fake_providers
    from audit_worker import local_db, package_io

    # ── 1. Guard'ы и их самопроверка ────────────────────────────────────────
    isolation.install_netguard(stand.guard_dir)
    probe_env = stand.base_env(writeguard_allow=[stand.root])
    check(isolation.selfcheck_netguard(PY, probe_env),
          "сетевой guard взведён (самопроверка убила процесс кодом 97)")
    forbidden = REPO_ROOT / "_writeguard_probe.tmp"
    check(isolation.selfcheck_writeguard(PY, probe_env, forbidden=forbidden),
          "сторож записи взведён (самопроверка убила процесс кодом 96)")
    check(isolation.selfcheck_writeguard_allows(
              PY, probe_env, allowed=stand.evidence / "allowed_probe.tmp"),
          "сторож записи НЕ ломает разрешённую запись")
    check(not forbidden.exists(), "запрещённая проба ничего не создала")

    # ── 2. Фикстуры и снимок ────────────────────────────────────────────────
    central, local, no_service = build_fixtures(stand)
    revision = "git:" + "0" * 40
    config_snapshot = build_config_snapshot(stand)
    snapshot = build_runtime_snapshot(revision, config_snapshot)
    check(snapshot.snapshot_hash().startswith("sha256:"),
          "снимок runtime-конфигурации собран", snapshot.snapshot_hash()[:23] + "…")

    # ── 3. Пакет и его раскладка ────────────────────────────────────────────
    job_id, attempt_id = str(uuid.uuid4()), str(uuid.uuid4())
    manifest = build_source_package(stand, central, snapshot, config_snapshot,
                                    job_id=job_id, attempt_id=attempt_id)
    archive = stand.packages / f"src_{attempt_id}.tar.gz"
    check(manifest["project_layout_version"] == 2,
          "манифест объявляет переносимую раскладку 2")
    check(manifest["portable_projects_root"] == "payload/projects_v2/",
          "манифест содержит portable_projects_root",
          manifest["portable_projects_root"])
    check("/" not in manifest["document_id"],
          "внешний код проекта путём не стал",
          f"external={manifest['project_external_id']!r} → dir={manifest['document_id']!r}")
    check(all(not e["path"].startswith("/") and ".." not in e["path"].split("/")
              for e in manifest["files"]),
          "в пакете нет абсолютных путей и обхода каталога")
    check(not any("backend/" in e["path"] or e["path"].endswith(".py")
                  for e in manifest["files"]),
          "в пакете нет исходного кода приложения")

    # Вторая раскладка версии — без 99_service.
    m2 = build_source_package(stand, no_service, snapshot, config_snapshot,
                              job_id=str(uuid.uuid4()), attempt_id=str(uuid.uuid4()))
    check(m2["project_layout_version"] == 2,
          "версия без 99_service тоже упаковалась", f"файлов {len(m2['files'])}")

    # ── 4. Настоящая worker.db и очередь ────────────────────────────────────
    jobs_root = stand.worker_root / "jobs"
    job_dir = jobs_root / job_id / attempt_id
    job_dir.mkdir(parents=True, exist_ok=True)
    unpack_like_agent(stand, manifest, archive, job_dir)
    check((job_dir / "project" / "objects").is_dir(),
          "распакован переносимый корень projects_v2")
    check((job_dir / "runtime" / "runtime_config.json").is_file(),
          "снимок runtime-конфигурации распакован")
    check((job_dir / "snapshot" / "stage_models.json").is_file(),
          "снимок stage_models приехал в пакете")

    version_dir = package_io.portable_version_dir(job_dir / "project")
    check(version_dir.is_dir(), "каталог версии найден в переносимом корне",
          str(version_dir.relative_to(job_dir)))
    source_hash_before = fx.source_tree_hash(version_dir)
    pdf_path = version_dir / "01_input" / f"{central.document_code}.pdf"
    pdf_hash_before = _sha256(pdf_path)

    # Резолвер обязан находить проект ВНУТРИ каталога попытки и нигде больше.
    resolver_env = dict(os.environ)
    resolver_env.update({
        "AUDIT_PROJECTS_DIR": str(job_dir / "project"),
        "AUDIT_PROJECTS_V2_DIR": str(job_dir / "project"),
        "AUDIT_PROJECTS_V2_WRITE_MODE": "projects_v2_primary",
        "AUDIT_DISABLE_DOTENV": "1",
    })
    resolved = subprocess.run(                              # noqa: S603
        [PY, "-c",
         "import json,sys;"
         "from backend.app.services.storage.v2_primary_wiring import resolve_v2_job_paths;"
         "r=resolve_v2_job_paths(sys.argv[1], sys.argv[2], run_id='probe');"
         "print(json.dumps([str(x) for x in r] if r else None))",
         central.project_id, central.version_id],
        env={**resolver_env, "PYTHONPATH": str(REPO_ROOT)},
        capture_output=True, text=True, timeout=180, cwd=str(REPO_ROOT),
    )
    try:
        resolved_paths = json.loads((resolved.stdout or "null").strip().splitlines()[-1])
    except (ValueError, IndexError):
        resolved_paths = None
    check(bool(resolved_paths),
          "resolve_v2_job_paths находит проект после распаковки",
          (resolved.stderr or "")[-200:] if not resolved_paths else "")
    check(bool(resolved_paths) and all(str(job_dir) in p for p in resolved_paths),
          "все разрешённые пути лежат ВНУТРИ каталога попытки")

    # ── 5. Поддельные провайдеры ────────────────────────────────────────────
    fake_providers.materialize(stand.providers)
    check(fake_providers.looks_like_fake_dir(stand.providers),
          "каталог поддельных провайдеров помечен маркером")

    # ── 6. Настоящий исполнитель отдельным процессом ────────────────────────
    from audit_worker import audit_runner

    params = {
        "execution_profile": "remote_audit_pilot_v1",
        "action": "full",
        "retry_stage": None,
        "include_optimization": True,
        "include_norms": False,
        "project_layout_version": 2,
        "pipeline_revision": revision,
        "expected_source_tree_hash": manifest["source_tree_hash"],
        "prompt_bundle_hash": config_snapshot["prompt_bundle_hash"],
        "model_config_hash": config_snapshot["model_config_hash"],
        "feature_flags_hash": config_snapshot["feature_flags_hash"],
        "runtime_snapshot_hash": snapshot.snapshot_hash(),
        "required_result_artifacts": list(audit_runner.REQUIRED_RESULT_ARTIFACTS),
    }
    from audit_worker.local_store import LocalJobStore

    store = LocalJobStore(jobs_root)
    store.create({
        "job_id": job_id,
        "attempt_id": attempt_id,
        "project_id": central.project_id,
        "version_id": central.version_id,
        "job_type": "audit_pipeline_v1",
        "params": params,
        "package": {"sha256": manifest["archive"]["sha256"], "manifest_version": 1,
                    "version_relative_path": manifest["version_relative_path"]},
    })
    store.update(job_id, attempt_id, worker_id="wrk-smoke")
    db = local_db.LocalDB(stand.worker_root / "worker.db")
    db.enqueue(job_id=job_id, attempt_id=attempt_id,
               job_type="audit_pipeline_v1", params=params)

    # Окружение ХОСТА исполнителя намеренно противоречит пакету: если победит
    # оно, конвейер запишет результат в legacy-раскладке и парити развалится.
    executor_env = stand.base_env(writeguard_allow=[stand.worker_root, stand.tmp,
                                                    stand.evidence, stand.providers])
    executor_env.update({
        "AUDIT_WORKER_ROOT": str(stand.worker_root),
        "AUDIT_WORKER_PIPELINE_ROOT": str(REPO_ROOT),
        "AUDIT_WORKER_PIPELINE_REVISION": revision,
        "AUDIT_WORKER_AUDIT_PIPELINE_ENABLED": "true",
        "AUDIT_WORKER_ALLOW_REAL_LLM": "false",
        "AUDIT_WORKER_FAKE_PROVIDER_DIR": str(stand.providers),
        "AUDIT_WORKER_PROVIDER_DIR": str(stand.providers),
        "AUDIT_WORKER_REAL_AUDIT_MAX_SLOTS": "1",
        "AUDIT_PROJECTS_V2_WRITE_MODE": "legacy",     # ← ловушка
        "DISTRIBUTED_WORKERS_ENABLED": "true",
    })
    (stand.evidence / "executor_env.json").write_text(
        json.dumps(executor_env, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    check("OPENROUTER_API_KEY" not in executor_env
          and "ANTHROPIC_API_KEY" not in executor_env
          and "CLAUDE_CODE_OAUTH_TOKEN" not in executor_env,
          "в окружении исполнителя нет ключей провайдеров")

    # `.env`-ловушка в родительском каталоге установленного кода.
    trap = REPO_ROOT.parent / ".env"
    trap_created = False
    if not trap.exists():
        try:
            trap.write_text(
                "PAID_API_ENABLED=true\nOPENROUTER_API_KEY=sk-or-v1-TRAP\n"
                "AUDIT_PROJECTS_V2_WRITE_MODE=legacy\n",
                encoding="utf-8",
            )
            trap_created = True
        except OSError:
            pass

    log_path = stand.evidence / "executor.log"
    with log_path.open("wb") as log_fh:
        stand.executor = subprocess.Popen(          # noqa: S603 — argv фиксирован
            [PY, "-m", "audit_worker", "executor",
             "--root", str(stand.worker_root), "--max-jobs", "1"],
            cwd=str(REPO_ROOT), env=executor_env,
            stdout=log_fh, stderr=subprocess.STDOUT,
            shell=False, start_new_session=True,
        )
    check(stand.executor.poll() is None, "настоящий Executor запущен отдельным процессом",
          f"pid={stand.executor.pid}")

    # Дочерний процесс конвейера обязан появиться и быть порождён ИСПОЛНИТЕЛЕМ.
    #
    # Ловить его снимком `ps` ненадёжно: короткий прогон успевает закончиться
    # между двумя опросами, и «не поймали» стало бы неотличимо от «не было».
    # Поэтому первичное доказательство — запись, которую делает САМ
    # исполнитель в реестре процессов (`register_process` из `on_start`):
    # pid, отпечаток argv и группа процессов. Снимок дерева процессов идёт
    # дополнением, когда процесс ещё жив.
    child_evidence: dict[str, Any] = {}
    live_tree: dict[str, Any] = {}

    def _child_recorded() -> bool:
        row = db.process_row(attempt_id)
        if not row:
            meta_now = store.load(job_id, attempt_id) or {}
            row = {"pid": meta_now.get("pid"),
                   "command_fingerprint": meta_now.get("command_fingerprint"),
                   "process_group_id": meta_now.get("process_group_id")}
        if row and row.get("pid"):
            child_evidence.update(row)
            if not live_tree:
                snap = isolation.process_tree_report([stand.executor.pid])
                for entry in snap.get("processes", []):
                    if "remote_audit_runner" in str(entry.get("args", "")):
                        live_tree.update(entry)
            return True
        return False

    found_child = _wait_for(_child_recorded, timeout=300, interval=0.2)
    check(found_child, "дочерний процесс конвейера порождён исполнителем",
          f"pid={child_evidence.get('pid')} "
          f"pgid={child_evidence.get('process_group_id')} "
          f"fingerprint={str(child_evidence.get('command_fingerprint'))[:16]}")
    expected_fp = audit_runner.command_fingerprint(
        audit_runner.build_argv(
            job_dir / "metadata" / "run_spec.json",
            config=type("C", (), {"pipeline_python": PY})(),
        )
    )
    check(str(child_evidence.get("command_fingerprint") or "") == expected_fp,
          "argv дочернего процесса — фиксированный audit_pipeline_v1 entrypoint",
          f"ожидался отпечаток {expected_fp[:16]}, получен "
          f"{str(child_evidence.get('command_fingerprint'))[:16]}")
    (stand.evidence / "process_evidence.json").write_text(
        json.dumps({"executor_pid": stand.executor.pid,
                    "child": child_evidence, "live_tree": live_tree,
                    "expected_fingerprint": expected_fp},
                   ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # Корень кода замораживается ПОСЛЕ старта (импорт модулей уже произошёл).
    check(stand.freeze_code_root(), "корень установленного кода сделан read-only")

    # ── 7. Ожидание завершения ──────────────────────────────────────────────
    finished = _wait_for(lambda: stand.executor.poll() is not None, timeout=timeout,
                         interval=1.0)
    stand.unfreeze_code_root()
    tail = ""
    if log_path.is_file():
        tail = log_path.read_text(encoding="utf-8", errors="replace")[-2500:]
    check(finished, "исполнитель завершил работу", tail[-400:] if not finished else "")
    if trap_created:
        trap.unlink(missing_ok=True)

    # ── 8. Изоляция: сеть, запись, провайдеры ───────────────────────────────
    net_hits = [h for h in isolation.netguard_hits(stand.netguard_log)
                if "example.invalid" not in h]
    check(not net_hits, "внешних сетевых соединений не было",
          "; ".join(net_hits[:3]))
    write_hits = [h for h in isolation.writeguard_hits(stand.writeguard_log)
                  if "_writeguard_probe" not in h]
    check(not write_hits, "записей вне каталога попытки не было",
          "; ".join(write_hits[:3]))
    check(not (REPO_ROOT / "comparison" / "classic_codex_ab" / "backups").exists()
          or not any((REPO_ROOT / "comparison" / "classic_codex_ab" / "backups").iterdir()),
          "в корне установленного кода не появился comparison/…/backups")
    check(not (Path(stand.home) / ".claude").exists()
          and not (Path(stand.home) / ".codex").exists(),
          "в изолированном HOME не появилась авторизация CLI")

    calls = fake_providers.read_call_log(
        job_dir / "logs" / "fake_provider_calls.jsonl"
    )
    check(bool(calls), "поддельные провайдеры вызывались отдельными процессами",
          f"вызовов: {len(calls)}")
    tree = isolation.process_tree_report([stand.executor.pid])
    check(not tree.get("suspicious"), "настоящие claude/codex не запускались",
          "; ".join(str(x) for x in (tree.get("suspicious") or [])[:2]))

    # ── 9. Применённая конфигурация ─────────────────────────────────────────
    applied_path = job_dir / "metadata" / "applied_runtime_config.json"
    applied = {}
    if applied_path.is_file():
        applied = json.loads(applied_path.read_text(encoding="utf-8"))
    check(applied.get("runtime_snapshot_hash") == snapshot.snapshot_hash(),
          "применён ИМЕННО тот снимок, который отправил центр")
    check(applied.get("applied_write_mode") == "projects_v2_primary",
          "write mode взят из пакета, а не с хоста",
          f"хост объявлял legacy, применено {applied.get('applied_write_mode')!r}")

    # ── 10. Артефакты, целостность исходников, пакет результата ─────────────
    run_dirs = sorted((version_dir / "03_analysis" / "runs").glob("*"))
    worker_out = run_dirs[-1] if run_dirs else (version_dir / "03_analysis" / "latest")
    for name in PRE_NORM_ARTIFACTS:
        check((worker_out / name).is_file() or
              (version_dir / "03_analysis" / "latest" / name).is_file(),
              f"pre-norm артефакт присутствует: {name}")

    check(fx.source_tree_hash(version_dir) == source_hash_before,
          "исходное дерево версии не изменилось")
    check(_sha256(pdf_path) == pdf_hash_before, "исходный PDF байтово не изменился")

    marker = job_dir / "work" / "completed.marker"
    check(marker.is_file() or (job_dir / "work" / "process_exit.json").is_file(),
          "маркер завершения записан")

    result_archive = job_dir / "result" / f"{attempt_id}.tar.gz"
    check(result_archive.is_file(), "пакет результата собран",
          f"{result_archive.stat().st_size} байт" if result_archive.is_file() else tail[-400:])
    if result_archive.is_file():
        import tarfile

        with tarfile.open(result_archive, "r:gz") as tar:
            names = tar.getnames()
            rm = json.loads(
                tar.extractfile("package_manifest.json").read().decode("utf-8")
            )
        check(rm.get("package_type") == "result", "манифест результата: package_type")
        check(rm.get("applied_write_mode") == "projects_v2_primary",
              "манифест результата фиксирует применённый write mode")
        check(rm.get("runtime_snapshot_hash") == snapshot.snapshot_hash(),
              "манифест результата фиксирует хэш снимка")
        check(rm.get("provider_mode") == "fake",
              "манифест результата фиксирует provider_mode=fake")
        check(not rm.get("forbidden_stages_not_run") or
              set(rm["forbidden_stages_not_run"]) == set(
                  ("norm_verify", "decision_carryover", "debt_control", "excel")),
              "манифест результата: центральные этапы не выполнялись")
        declared = {e["path"] for e in rm.get("files", [])}
        check(declared == {n for n in names if n != "package_manifest.json"},
              "манифест результата соответствует фактическим файлам",
              f"в манифесте {len(declared)}, в архиве {len(names) - 1}")
        check(not any(n.endswith(".py") or "/.env" in n or n.endswith(".env")
                      for n in names),
              "в пакете результата нет кода и .env")
        check(not any("01_input/" in n or "02_work/" in n for n in names),
              "в пакете результата нет исходников заказчика")

    # ── 11. Локальный baseline и узкая parity ───────────────────────────────
    local_out = run_local_baseline(stand, local, snapshot, config_snapshot, revision)
    if local_out is not None:
        remote_projection = load_projection(worker_out)
        if all(v is None for v in remote_projection.values()):
            remote_projection = load_projection(version_dir / "03_analysis" / "latest")
        local_projection = load_projection(local_out)
        diff = [
            name for name in PRE_NORM_ARTIFACTS
            if local_projection.get(name) != remote_projection.get(name)
        ]
        (stand.evidence / "parity.json").write_text(
            json.dumps({"local": local_projection, "worker": remote_projection,
                        "diff": diff}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        check(not diff, "семантическая проекция pre-norm совпала (local ↔ worker)",
              "различия: " + ", ".join(diff) if diff else "")
        local_n = findings_count(local_out)
        worker_n = max(findings_count(worker_out),
                       findings_count(version_dir / "03_analysis" / "latest"))
        check(local_n == worker_n, "число замечаний совпало",
              f"local={local_n}, worker={worker_n}")
        # Совпадение НУЛЯ с НУЛЁМ ничего не доказывает: при пустом наборе
        # дедуп, критик, корректор и обогащение листом/страницей не
        # выполняются ни на одной стороне. Непустой набор — условие
        # содержательности всей проверки выше.
        check(local_n > 0, "сравнивался НЕПУСТОЙ набор замечаний",
              f"замечаний: {local_n}")

    # ── 12. Отсутствие мусора ───────────────────────────────────────────────
    dirty = subprocess.run(                                 # noqa: S603
        ["git", "status", "--short"], cwd=str(REPO_ROOT),
        capture_output=True, text=True, timeout=120,
    ).stdout
    runtime_junk = [
        line for line in dirty.splitlines()
        if any(mark in line for mark in
               ("worker.db", ".tar.gz", "comparison/", "evidence", "sonnet_clean"))
    ]
    check(not runtime_junk, "runtime-мусор в рабочем дереве не появился",
          "; ".join(runtime_junk[:3]))

    return _finish()


def run_local_baseline(stand: Stand, fixture, snapshot, config_snapshot,
                       revision: str) -> Optional[Path]:
    """Локальный pre-norm прогон в тех же условиях — для узкой parity.

    Тот же runner, тот же снимок, те же поддельные провайдеры и тот же
    процессный гейт центральных этапов. Различаются только каталоги: иначе
    сравнивались бы не два прогона, а один и его копия.
    """
    from tests.distributed_audit_e2e import isolation

    job_dir = stand.local_case / "attempt"
    for name in ("project", "snapshot", "runtime", "work", "result", "logs",
                 "metadata", "usage", "comparison"):
        (job_dir / name).mkdir(parents=True, exist_ok=True)
    # `project/` локального случая — тот же переносимый корень.
    shutil.rmtree(job_dir / "project", ignore_errors=True)
    shutil.copytree(fixture.v2_root, job_dir / "project")
    seed_snapshot_dir(job_dir, stand.root / "snapshot_source")
    (job_dir / "snapshot" / "feature_flags.json").write_text(
        json.dumps(config_snapshot["feature_flags"], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (job_dir / "runtime" / "runtime_config.json").write_bytes(
        snapshot.to_package_bytes()
    )

    spec = {
        "job_id": "local-baseline", "attempt_id": "local-attempt",
        "project_id": fixture.project_id, "version_id": fixture.version_id,
        "profile": "remote_audit_pilot_v1", "action": "full", "retry_stage": None,
        "include_optimization": True, "include_norms": False,
        "pipeline_revision": revision,
        "expected_source_tree_hash": "",
        "prompt_bundle_hash": config_snapshot["prompt_bundle_hash"],
        "model_config_hash": config_snapshot["model_config_hash"],
        "feature_flags_hash": config_snapshot["feature_flags_hash"],
        "runtime_snapshot_hash": snapshot.snapshot_hash(),
        "provider_mode": "fake",
        "paths": {
            "project": str(job_dir / "project"),
            "snapshot": str(job_dir / "snapshot"),
            "runtime": str(job_dir / "runtime"),
            "work": str(job_dir / "work"),
            "result": str(job_dir / "result"),
            "logs": str(job_dir / "logs"),
            "metadata": str(job_dir / "metadata"),
            "usage": str(job_dir / "usage"),
        },
    }
    spec_path = job_dir / "metadata" / "run_spec.json"
    spec_path.write_text(json.dumps(spec, ensure_ascii=False, indent=2), encoding="utf-8")

    from audit_worker import audit_runner

    env = stand.base_env(writeguard_allow=[stand.local_case, stand.tmp,
                                           stand.evidence, stand.providers])
    env.update(audit_runner.isolated_roots(job_dir))
    env.update({
        "PYTHONPATH": os.pathsep.join([str(stand.guard_dir), str(REPO_ROOT)]),
        "AUDIT_DISABLE_DOTENV": "1",
        "AUDIT_WORKER_FAKE_PROVIDER_DIR": str(stand.providers),
        "AUDIT_WORKER_PROVIDER_MODE": "fake",
        "AUDIT_PROJECTS_V2_WRITE_MODE": "legacy",     # та же ловушка
    })
    env["PATH"] = os.pathsep.join([str(stand.providers), env.get("PATH", "")])
    for root in audit_runner.isolated_roots(job_dir).values():
        Path(root).mkdir(parents=True, exist_ok=True)

    log_path = stand.evidence / "local_baseline.log"
    with log_path.open("wb") as fh:
        proc = subprocess.run(                              # noqa: S603
            [PY, "-u", "-m", "backend.app.pipeline.remote_audit_runner", str(spec_path)],
            cwd=str(REPO_ROOT), env=env, stdout=fh, stderr=subprocess.STDOUT,
            timeout=1800, shell=False,
        )
    check(proc.returncode == 0, "локальный pre-norm baseline прошёл",
          log_path.read_text(encoding="utf-8", errors="replace")[-400:]
          if proc.returncode else "")
    if proc.returncode != 0:
        return None
    from audit_worker import package_io

    version_dir = package_io.portable_version_dir(job_dir / "project")
    runs = sorted((version_dir / "03_analysis" / "runs").glob("*"))
    return runs[-1] if runs else version_dir / "03_analysis" / "latest"


if __name__ == "__main__":
    raise SystemExit(main())
