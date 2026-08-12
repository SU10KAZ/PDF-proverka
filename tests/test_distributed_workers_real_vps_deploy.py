"""Тесты механики доставки audit-worker на сторонний VPS.

Что здесь проверяется и почему именно так
─────────────────────────────────────────
Установка на чужую машину — это ровно то место, где ошибка не видна до тех
пор, пока не станет дорогой: увезли `.env` — утекли ключи; забыли файл —
конвейер упал через час работы; переключили симлинк неатомарно — поймали
полурелиз. Проверять это «на живом VPS» нельзя каждый раз, поэтому вся
детерминированная часть — сборка артефакта, манифест, denylist, имена
релизов, генерация юнитов — покрыта здесь и гоняется без сети.

Живой межсерверный прогон (`scripts/smoke_distributed_audit_real_vps.py`)
эти тесты НЕ дублирует: он доказывает транспорт, они — что артефакт,
который туда поедет, собран правильно.
"""

from __future__ import annotations

import json
import sys
import tarfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import deploy_audit_worker as deploy                            # noqa: E402


REVISION = "git:" + "d" * 40


# ─── allowlist и denylist ────────────────────────────────────────────────────


def test_bundle_includes_everything_the_pipeline_reads_as_a_file():
    """Пути, на которые `config.py` ссылается как на ФАЙЛЫ-скрипты, обязаны ехать.

    Их отсутствие не ловится импортом: конвейер спотыкается о них уже в
    работе, на конкретном этапе, и выглядит это как «этап упал», а не как
    «пакет неполон».
    """
    files = {str(p) for p in deploy.collect_bundle_files(REPO_ROOT)}
    for required in (
        "blocks.py",
        "process_project.py",
        "generate_excel_report.py",
        "norms/_core.py",
        "backend/__init__.py",
        "audit_worker/__main__.py",
        "backend/app/pipeline/remote_audit_runner.py",
        "backend/app/data/model_prices.json",
        "prompts/disciplines/_registry.json",
        "requirements-worker.txt",
        "requirements-worker-pipeline.txt",
    ):
        assert required in files, f"в бандле нет {required}"


def test_bundle_carries_registry_but_not_discipline_profiles():
    """Реестр дисциплин — да, профили — нет.

    Профиль (`role.md`/`checklist.md`) приезжает в пакете ЗАДАНИЯ и там же
    сверяется по хэшу. Если те же файлы лежали бы ещё и в дереве кода,
    подстановка чужой дисциплины прошла бы молча — вместо громкого падения.
    """
    files = {str(p) for p in deploy.collect_bundle_files(REPO_ROOT)}
    assert "prompts/disciplines/_registry.json" in files
    leaked = sorted(
        p for p in files
        if p.startswith("prompts/disciplines/")
        and p != "prompts/disciplines/_registry.json"
    )
    assert not leaked, f"профили дисциплин просочились в бандл: {leaked[:5]}"


def test_bundle_has_no_secrets_or_data_dirs():
    files = deploy.collect_bundle_files(REPO_ROOT)
    assert deploy.audit_bundle_files(files) == []


@pytest.mark.parametrize(
    "path",
    [
        ".env",
        "backend/.env",
        "audit_worker/token",
        "data/claim_secret",
        "backend/app/workers.db",
        "some/dir/worker.db",
        "home/.claude/settings.json",
        "home/.codex/auth.json",
        "certs/server.pem",
        "certs/server.key",
        "node_modules/pkg/index.js",
        ".git/config",
        "deploy/id_rsa",
        "x/my_credentials/file.txt",
    ],
)
def test_denylist_rejects_secret_paths(path):
    assert deploy._denied_reason(Path(path)) is not None, f"{path} должен быть запрещён"


@pytest.mark.parametrize(
    "path",
    [
        "projects/x/y.json",
        "projects_v2/objects/a/b.json",
        "knowledge_base/decisions_log.json",
        "comparison/state.json",
        "frontend/index.html",
        "logs/actions/a.jsonl",
    ],
)
def test_denylist_rejects_root_data_dirs(path):
    assert deploy._denied_reason(Path(path)) is not None


@pytest.mark.parametrize(
    "path",
    [
        # Те же слова, но как ПАКЕТЫ КОДА внутри backend — законны.
        "backend/app/services/knowledge_base/__init__.py",
        "backend/app/services/knowledge_base/missing_norms_service.py",
        "backend/app/pipeline/stages/comparison/state.py",
        "audit_worker/local_db.py",
    ],
)
def test_denylist_does_not_reject_legitimate_code_packages(path):
    """Ровно тот случай, который однажды уже завалил сборку.

    `knowledge_base` как каталог ДАННЫХ в корне запрещён, а
    `backend/app/services/knowledge_base/` — обычный сервис. Проверка «имя
    где угодно в пути» вырезала бы половину backend.
    """
    assert deploy._denied_reason(Path(path)) is None, f"{path} запрещён по ошибке"


def test_prune_removes_bytecode_and_caches():
    assert deploy._should_prune(Path("backend/app/__pycache__/main.cpython-312.pyc"))
    assert deploy._should_prune(Path("audit_worker/x.pyc"))
    assert deploy._should_prune(Path("logs/app.log"))
    assert not deploy._should_prune(Path("audit_worker/agent.py"))


# ─── хэши и манифест ─────────────────────────────────────────────────────────


def test_tree_hash_is_order_independent_and_content_sensitive(tmp_path):
    (tmp_path / "a.txt").write_text("alpha", encoding="utf-8")
    (tmp_path / "b.txt").write_text("beta", encoding="utf-8")
    files = [Path("a.txt"), Path("b.txt")]

    assert deploy.tree_hash(tmp_path, files) == deploy.tree_hash(tmp_path, list(reversed(files)))

    before = deploy.tree_hash(tmp_path, files)
    (tmp_path / "b.txt").write_text("beta!", encoding="utf-8")
    assert deploy.tree_hash(tmp_path, files) != before


def test_manifest_has_every_required_field(tmp_path):
    files = deploy.collect_bundle_files(REPO_ROOT)
    manifest = deploy.build_manifest(REPO_ROOT, files, pipeline_revision=REVISION)
    for field in (
        "package_format_version", "worker_version", "protocol_version",
        "pipeline_revision", "source_commit", "created_at", "tree_hash",
        "requirements_hash", "compatible_execution_profiles",
        "file_count", "total_bytes", "files",
    ):
        assert field in manifest, f"в манифесте нет поля {field}"
    assert manifest["pipeline_revision"] == REVISION
    assert manifest["protocol_version"] == 1
    assert manifest["tree_hash"].startswith("sha256:")
    assert manifest["requirements_hash"].startswith("sha256:")
    assert "remote_audit_pilot_v1" in manifest["compatible_execution_profiles"]


def test_manifest_worker_version_matches_package():
    from audit_worker import PROTOCOL_VERSION, __version__

    assert deploy.worker_version(REPO_ROOT) == __version__
    assert deploy.protocol_version(REPO_ROOT) == PROTOCOL_VERSION


def test_release_name_encodes_tree_hash(tmp_path):
    files = deploy.collect_bundle_files(REPO_ROOT)
    manifest = deploy.build_manifest(
        REPO_ROOT, files, pipeline_revision=REVISION, created_at="2026-08-09T10:00:00Z"
    )
    name = deploy.release_name(manifest)
    assert name.startswith("20260809T100000")
    assert manifest["tree_hash"].split(":", 1)[1][:12] in name


# ─── артефакт целиком ────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    out = tmp_path_factory.mktemp("artifact")
    return deploy.build_artifact(REPO_ROOT, out, pipeline_revision=REVISION)


def test_artifact_matches_its_manifest(built):
    assert deploy.verify_artifact(built.archive, built.manifest_path) == []


def test_artifact_contains_manifest_inside(built):
    with tarfile.open(built.archive, "r:gz") as tar:
        names = tar.getnames()
        assert "MANIFEST.json" in names
        inside = json.loads(tar.extractfile("MANIFEST.json").read().decode("utf-8"))
    assert inside["pipeline_revision"] == REVISION
    assert inside["tree_hash"] == built.manifest["tree_hash"]


def test_artifact_has_no_forbidden_entries(built):
    with tarfile.open(built.archive, "r:gz") as tar:
        names = [m.name for m in tar.getmembers() if m.isfile()]
    for name in names:
        if name == "MANIFEST.json":
            continue
        assert deploy._denied_reason(Path(name)) is None, f"в архиве запрещённый путь {name}"


def test_verify_detects_tampered_archive(built, tmp_path):
    """Подмена архива обязана ловиться по sha256, а не «выглядеть нормально»."""
    fake = tmp_path / built.archive.name
    fake.write_bytes(built.archive.read_bytes() + b"\0")
    problems = deploy.verify_artifact(fake, built.manifest_path)
    assert problems and any("sha256" in p for p in problems)


def test_verify_detects_manifest_file_list_drift(built, tmp_path):
    manifest = json.loads(built.manifest_path.read_text(encoding="utf-8"))
    manifest["files"] = manifest["files"][:-1]
    drifted = tmp_path / "drifted.manifest.json"
    drifted.write_text(json.dumps(manifest), encoding="utf-8")
    problems = deploy.verify_artifact(built.archive, drifted)
    assert problems and any("состав расходится" in p for p in problems)


def test_build_refuses_when_allowlist_path_is_missing(tmp_path):
    with pytest.raises(SystemExit):
        deploy.collect_bundle_files(tmp_path, include=("audit_worker/",))


# ─── удалённая сторона: аргументы, без сети ──────────────────────────────────


def test_remote_target_is_built_from_arguments_not_hardcoded():
    remote = deploy.Remote(host="198.51.100.7", user="someone", root="/srv/aw")
    assert remote.target == "someone@198.51.100.7"
    source = (REPO_ROOT / "scripts" / "deploy_audit_worker.py").read_text(encoding="utf-8")
    assert "176.12.77" not in source, "адрес пилотного VPS не должен быть зашит в код"
    assert "password" not in source.lower().replace("passwordless", "")


def test_remote_commands_quote_the_root(monkeypatch):
    """Корень с пробелом не должен разваливать удалённую команду."""
    captured: list[str] = []

    class _Fake(deploy.Remote):
        def run(self, script, *, timeout=600, check=True):
            captured.append(script)
            import subprocess

            return subprocess.CompletedProcess([], 0, "", "")

    remote = _Fake(host="h", user="u", root="/srv/audit worker")
    deploy.remote_bootstrap_layout(remote)
    assert captured and "'/srv/audit worker'" in captured[0]


def test_switch_current_is_atomic():
    """Симлинк переключается через временный + `mv -T`, а не rm+ln."""
    captured: list[str] = []

    class _Fake(deploy.Remote):
        def run(self, script, *, timeout=600, check=True):
            captured.append(script)
            import subprocess

            return subprocess.CompletedProcess([], 0, "SWITCH_OK prev=a now=b", "")

    remote = _Fake(host="h", user="u", root="/srv/aw")
    deploy.remote_switch_current(remote, "rel-1")
    script = captured[0]
    assert "mv -T" in script, "переключение должно быть атомарным"
    assert "rm -f" not in script.split("ln -sfn")[0], "старый симлинк нельзя удалять заранее"


def test_install_release_verifies_sha_before_unpacking():
    captured: list[str] = []

    class _Fake(deploy.Remote):
        def run(self, script, *, timeout=600, check=True):
            captured.append(script)
            import subprocess

            return subprocess.CompletedProcess([], 0, "SHA_OK x", "")

    remote = _Fake(host="h", user="u", root="/srv/aw")
    deploy.remote_install_release(remote, "a.tar.gz", "a.manifest.json", "rel", "abc")
    script = captured[0]
    assert script.index("sha256sum") < script.index("tar -xzf"), (
        "хэш обязан проверяться ДО распаковки, иначе проверять уже нечего"
    )


def test_venv_lives_outside_release():
    """venv не внутри релиза: иначе откат означал бы переустановку зависимостей."""
    captured: list[str] = []

    class _Fake(deploy.Remote):
        def run(self, script, *, timeout=600, check=True):
            captured.append(script)
            import subprocess

            return subprocess.CompletedProcess([], 0, "VENV_OK", "")

    remote = _Fake(host="h", user="u", root="/srv/aw")
    deploy.remote_sync_venv(remote, "rel-1")
    script = captured[0]
    assert '"$root/venv"' in script
    assert '"$root/app/$rel/venv"' not in script


def test_layout_keeps_code_and_data_apart():
    captured: list[str] = []

    class _Fake(deploy.Remote):
        def run(self, script, *, timeout=600, check=True):
            captured.append(script)
            import subprocess

            return subprocess.CompletedProcess([], 0, "LAYOUT_OK", "")

    remote = _Fake(host="h", user="u", root="/srv/aw")
    deploy.remote_bootstrap_layout(remote)
    script = captured[0]
    for part in ("app", "data", "config", "logs"):
        assert part in script
    assert "chmod 750" in script, "каталоги данных и конфигурации не должны быть общедоступны"


# ─── конфигурация воркера и systemd ──────────────────────────────────────────


@pytest.fixture(scope="module")
def smoke():
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    import smoke_distributed_audit_real_vps as module

    return module


def test_worker_env_has_no_secrets(smoke):
    body = smoke.worker_env_file(
        root="/srv/aw", central_url="https://example.test",
        revision=REVISION, display_name="pilot",
    )
    lowered = body.lower()
    for forbidden in ("bootstrap", "secret", "token", "api_key", "password"):
        assert forbidden not in lowered, f"в worker.env просочилось «{forbidden}»"


def test_worker_env_forbids_real_llm_and_points_at_fakes(smoke):
    body = smoke.worker_env_file(
        root="/srv/aw", central_url="https://example.test",
        revision=REVISION, display_name="pilot",
    )
    assert "AUDIT_WORKER_ALLOW_REAL_LLM=false" in body
    assert "AUDIT_WORKER_FAKE_PROVIDER_DIR=/srv/aw/fake_providers" in body
    assert "AUDIT_WORKER_REAL_AUDIT_MAX_SLOTS=1" in body


def test_worker_env_sets_locale_and_pythonpath(smoke):
    """Две поправки, каждая из которых один раз уже стоила прогона."""
    body = smoke.worker_env_file(
        root="/srv/aw", central_url="https://example.test",
        revision=REVISION, display_name="pilot",
    )
    # Белый список окружения дочернего процесса — (PATH, LANG, LC_ALL, TZ),
    # а пути проектов кириллические.
    assert "LANG=C.UTF-8" in body and "LC_ALL=C.UTF-8" in body
    # Пакет живёт внутри релиза, значит `-m audit_worker` ищет его по PYTHONPATH.
    assert "PYTHONPATH=/srv/aw/current" in body


def test_worker_env_carries_revision_verbatim(smoke):
    body = smoke.worker_env_file(
        root="/srv/aw", central_url="https://example.test",
        revision=REVISION, display_name="pilot",
    )
    assert f"AUDIT_WORKER_PIPELINE_REVISION={REVISION}" in body


def _directives(unit: str) -> list[str]:
    """Только действующие строки юнита.

    Комментарии отбрасываются намеренно: в них перечислены как раз ЗАПРЕЩЁННЫЕ
    директивы — с объяснением, почему их нет. Проверка «подстроки в тексте»
    ловила бы объяснение вместо кода.
    """
    return [
        line.strip() for line in unit.splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def test_units_are_independent_of_each_other(smoke):
    """Ни одной директивы, связывающей агента и исполнителя (инвариант I-02)."""
    for kind in ("agent", "executor"):
        directives = _directives(smoke.systemd_unit(kind=kind, root="/srv/aw"))
        for forbidden in ("Requires=", "PartOf=", "BindsTo=", "Requisite=", "BoundBy="):
            offenders = [d for d in directives if d.startswith(forbidden)]
            assert not offenders, f"{kind}: связь {forbidden} запрещена ({offenders})"


def test_units_use_kill_mode_process(smoke):
    """KillMode=process: рестарт юнита не вправе убивать идущий аудит."""
    for kind in ("agent", "executor"):
        unit = smoke.systemd_unit(kind=kind, root="/srv/aw")
        assert "KillMode=process" in unit


def test_units_read_environment_from_file_not_inline(smoke):
    """Секретов в юните нет; всё окружение — из файла 0600."""
    for kind in ("agent", "executor"):
        unit = smoke.systemd_unit(kind=kind, root="/srv/aw")
        assert "EnvironmentFile=/srv/aw/config/worker.env" in unit
        assert "AUDIT_WORKER_DISPATCHER_URL=" not in unit
        assert "bootstrap" not in unit.lower()


def test_agent_unit_waits_for_network_executor_does_not(smoke):
    agent = smoke.systemd_unit(kind="agent", root="/srv/aw")
    executor = smoke.systemd_unit(kind="executor", root="/srv/aw")
    assert "network-online.target" in agent
    assert "network-online.target" not in executor, (
        "исполнитель к центру не ходит — сети ему ждать незачем"
    )


def test_units_point_at_venv_python_and_current_release(smoke):
    for kind in ("agent", "executor"):
        unit = smoke.systemd_unit(kind=kind, root="/srv/aw")
        assert f"ExecStart=/srv/aw/venv/bin/python -m audit_worker {kind}" in unit


# ─── smoke-скрипт: безопасность по умолчанию ─────────────────────────────────


def test_smoke_never_offers_a_real_llm_switch(smoke):
    """Ключа включения настоящих моделей нет в РАЗБОРЕ АРГУМЕНТОВ.

    Проверяется парсер, а не текст файла: в docstring фраза «параметра
    `--real-llm` здесь нет» присутствует законно, и поиск подстроки объявлял
    бы нарушением ровно то предложение, которое обещает обратное.
    """
    options: set[str] = set()
    for action in smoke.build_parser()._actions:                # noqa: SLF001
        options.update(action.option_strings)
    for forbidden in ("--real-llm", "--allow-real-llm", "--live-llm"):
        assert forbidden not in options, f"smoke предлагает {forbidden}"
    assert not [o for o in options if "real" in o and "llm" in o]


def test_smoke_refuses_remote_actions_without_the_flag(smoke):
    worker = smoke.Worker(host="h", user="u", root="/srv/aw", allow_actions=False)
    with pytest.raises(SystemExit):
        worker.act("echo nope")


def test_smoke_host_is_a_required_argument(smoke):
    parser = smoke.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([])
    args = parser.parse_args(["--worker-host", "h", "--worker-user", "u"])
    assert args.allow_remote_actions is False, "по умолчанию — только preflight"
    assert args.mode == "test"


def test_smoke_uses_non_eom_discipline(smoke):
    assert smoke.DISCIPLINE_SECTION != "EOM"
    assert smoke.DISCIPLINE_SECTION == "VK"
    assert smoke.DISCIPLINE_FOLDER == "ВК"


def test_first_json_object_survives_trailing_human_text(smoke):
    """Подкоманды воркера печатают JSON, а следом — подсказку оператору."""
    text = '{\n "worker_id": "wrk_1",\n "token_stored": false\n}\nДальше: одобрите воркер.\n'
    assert smoke._first_json_object(text)["worker_id"] == "wrk_1"
    assert smoke._first_json_object("нет json") == {}
