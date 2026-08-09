"""Запуск реального аудита на воркере — изолированно и фиксированным argv.

Тот же принцип, что и у `test_runner`: **argv строит воркер**. Из задания
приходят только скаляры, и они проверяются здесь повторно. Что физически
невозможно из-за конструкции:

  * подставить исполняемый файл — берётся интерпретатор процесса воркера;
  * подставить модуль — имя точки входа константа этого файла;
  * подставить аргумент — их ровно четыре и они фиксированы;
  * подставить путь — все пути вычисляются от каталога попытки;
  * подставить переменную окружения — env собирается из белого списка.

Отличие от `test_runner` одно: запускается не игрушечный процесс, а
установленный на этом VPS код платформы. Где он лежит, знает АДМИНИСТРАТОР
воркера (`AUDIT_WORKER_PIPELINE_ROOT`), а не центр: путь к исполняемому коду
не может приходить из задания.

Бизнес-логики этапов здесь нет и быть не должно — она в самом конвейере.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

#: Точка входа установленного конвейера. КОНСТАНТА: центр её не задаёт.
PIPELINE_ENTRYPOINT_MODULE = "backend.app.pipeline.remote_audit_runner"

#: Единственный профиль, который воркер соглашается исполнять.
SUPPORTED_PROFILE = "remote_audit_pilot_v1"

#: Действия, которые профиль допускает.
#:
#: `provider_selfcheck` (этап 11C) — синтетическая проверка сквозного пути
#: «задание → конвейер → ProviderAdapter → CLI». Это ПОЛНОЦЕННОЕ действие
#: профиля, а не отладочный ключ: оно идёт тем же типом задания, тем же
#: исполнителем, той же точкой входа конвейера, с теми же снимками центра и
#: той же сборкой пакета. Отличается ровно тем, что делает: один вызов модели
#: вместо восьми этапов аудита — и поэтому у него свой список обязательных
#: артефактов.
SUPPORTED_ACTIONS = frozenset({"full", "audit", "resume", "provider_selfcheck"})

#: Действие синтетической проверки провайдера.
ACTION_PROVIDER_SELFCHECK = "provider_selfcheck"

#: Имя переменной, которой воркер сообщает процессу конвейера путь к привязке
#: провайдера. ЛИТЕРАЛ, а не импорт из `audit_worker.providers`, и это не
#: небрежность: модуль, который строит argv и окружение процесса конвейера,
#: по-прежнему НИЧЕГО не знает о провайдерском слое — ему нужно только имя
#: переменной. Совпадение литерала с `resolver.BINDING_ENV` проверяется тестом
#: `test_binding_env_name_matches_provider_layer`.
PROVIDER_BINDING_ENV = "AUDIT_WORKER_PROVIDER_BINDING"

#: Обязательные артефакты результата. Дублируют центральный список намеренно:
#: каждый рубеж держит оборону сам.
#: Текст ловит ~40 % замечаний, визуальный анализ — остальные 60 %. Прогон, в
#: котором `03_findings.json` есть, а `01_blocks_analysis.json` нет, — это ровно
#: форма известного инцидента «нога провайдера молча отвалилась»: пакет прошёл
#: бы транспорт как успешный и уехал на центр аудитом наполовину.
REQUIRED_RESULT_ARTIFACTS: tuple[str, ...] = (
    "work/pipeline_log.json",
    "result/03_findings.json",
    "result/01_blocks_analysis.json",
    "result/02_text_analysis.json",
    "result/audit_manifest.json",
    "usage/usage_report.json",
)

#: Обязательные артефакты СИНТЕТИЧЕСКОЙ проверки провайдера. Свой список, а не
#: урезанный общий: требовать `03_findings.json` от прогона, который аудита не
#: выполнял, значило бы либо завалить его всегда, либо подделать артефакт.
SYNTHETIC_REQUIRED_RESULT_ARTIFACTS: tuple[str, ...] = (
    "work/pipeline_log.json",
    "result/provider_selfcheck.json",
    "result/audit_manifest.json",
    "usage/usage_report.json",
)


def required_artifacts_for(action: str) -> tuple[str, ...]:
    """Какие артефакты обязательны для ЭТОГО действия."""
    if str(action) == ACTION_PROVIDER_SELFCHECK:
        return SYNTHETIC_REQUIRED_RESULT_ARTIFACTS
    return REQUIRED_RESULT_ARTIFACTS

#: Переменные окружения, которые НАСЛЕДУЮТСЯ у процесса воркера. Всё, чего
#: здесь нет, до конвейера не доходит — включая токены, адрес центра, секреты
#: воркера и `AUDIT_PROJECTS_V2_WRITE_MODE` хоста.
#:
#: `HOME` и `TMPDIR` из списка УБРАНЫ намеренно: их значения вычисляются от
#: каталога попытки. Наследованный `HOME` означал бы `~/.claude`, `~/.codex` и
#: `~/.claude/projects` чужой машины, то есть и запись вне изоляции, и
#: ambient-авторизацию настоящих CLI.
_ENV_WHITELIST = ("PATH", "LANG", "LC_ALL", "TZ")

#: Дополнительные системные переменные, без которых интерпретатор на некоторых
#: VPS не стартует вовсе. Секретов среди них нет, значения не путь к данным.
_ENV_SYSTEM_OPTIONAL = ("LD_LIBRARY_PATH", "SSL_CERT_FILE", "SSL_CERT_DIR")

#: Корни данных, каждый ВНУТРИ каталога попытки. Порядок и состав дублируются
#: проверкой `remote_audit_runner.apply_runtime_paths`: воркер их выставляет,
#: код платформы — проверяет.
def isolated_roots(job_dir: Path) -> dict[str, str]:
    """Все корни данных и записи процесса конвейера, от каталога попытки.

    Единственное место, где эта карта задаётся. Раньше часть путей не имела
    override вовсе (`comparison/` под корнем установленного кода), а часть
    наследовалась у хоста (`HOME`, `TMPDIR`) — и то и другое означало запись
    мимо каталога попытки.
    """
    job_dir = Path(job_dir)
    return {
        "AUDIT_DATA_DIR": str(job_dir / "work" / "data"),
        "AUDIT_APP_DATA_DIR": str(job_dir / "work" / "app_data"),
        # `project/` — переносимый корень `projects_v2` целиком:
        # objects/<obj>/disciplines/<Д>/documents/<код>/versions/<vid>/…
        "AUDIT_PROJECTS_DIR": str(job_dir / "project"),
        "AUDIT_PROJECTS_V2_DIR": str(job_dir / "project"),
        "AUDIT_PROMPTS_DIR": str(job_dir / "snapshot" / "prompts"),
        "AUDIT_ACTION_LOG_DIR": str(job_dir / "logs" / "actions"),
        "COMPARISON_ROOT": str(job_dir / "comparison"),
        "AUDIT_CLEAN_CWD_ROOT": str(job_dir / "work" / "tmp" / "clean_cwd"),
        "AUDIT_CODEX_WORKDIR": str(job_dir / "work" / "agent_workdir"),
        "AUDIT_BLOCK_CROP_CACHE_DIR": str(job_dir / "work" / "cache" / "block_crops"),
        "HOME": str(job_dir / "work" / "home"),
        "TMPDIR": str(job_dir / "work" / "tmp"),
    }

TERMINATE_GRACE_SEC = 30.0


class AuditJobRejected(ValueError):
    """Параметры реального аудита не прошли проверку воркера."""


@dataclass(frozen=True)
class SafeAuditParams:
    execution_profile: str
    action: str
    retry_stage: Optional[str]
    include_optimization: bool
    include_norms: bool
    pipeline_revision: str
    expected_source_tree_hash: str
    prompt_bundle_hash: str
    model_config_hash: str
    feature_flags_hash: str
    runtime_snapshot_hash: str
    discipline_id: str
    discipline_profile_hash: str
    required_result_artifacts: tuple[str, ...]
    #: Требование к провайдеру в виде обычного словаря скаляров. Разбирает его
    #: резолвер исполнителя; здесь оно только переносится.
    provider_requirement: Optional[dict[str, Any]] = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "execution_profile": self.execution_profile,
            "action": self.action,
            "retry_stage": self.retry_stage,
            "include_optimization": self.include_optimization,
            "include_norms": self.include_norms,
            "pipeline_revision": self.pipeline_revision,
            "expected_source_tree_hash": self.expected_source_tree_hash,
            "prompt_bundle_hash": self.prompt_bundle_hash,
            "model_config_hash": self.model_config_hash,
            "feature_flags_hash": self.feature_flags_hash,
            "runtime_snapshot_hash": self.runtime_snapshot_hash,
            "discipline_id": self.discipline_id,
            "discipline_profile_hash": self.discipline_profile_hash,
            "required_result_artifacts": list(self.required_result_artifacts),
            "provider_requirement": self.provider_requirement,
        }


_ALLOWED_FIELDS = {
    "execution_profile", "action", "retry_stage", "include_optimization",
    "include_norms", "project_layout_version", "pipeline_revision",
    "expected_source_tree_hash", "prompt_bundle_hash", "model_config_hash",
    "feature_flags_hash", "runtime_snapshot_hash", "discipline_id",
    "discipline_profile_hash", "required_result_artifacts",
    # ЛОГИЧЕСКОЕ требование центра к провайдеру (этап 11C). Ни путей, ни
    # учётных данных, ни токенов: только имя провайдера, ожидаемая модель,
    # белый список этапов и потолок вызовов. Разбирает его резолвер на стороне
    # исполнителя; здесь проверяется лишь форма — чтобы негодная нагрузка
    # отвергалась там же, где и все остальные (§4 задания).
    "provider_requirement",
}

#: Поля требования к провайдеру. Проверяются формально, без импорта
#: провайдерского слоя (см. комментарий к PROVIDER_BINDING_ENV).
_PROVIDER_REQUIREMENT_FIELDS = {
    "provider", "model", "allowed_stages", "max_inferences",
    # Этап 11D: ЛОГИЧЕСКАЯ способность вместо точного идентификатора модели.
    # Здесь проверяется только форма; какой строке она соответствует НА ЭТОЙ
    # машине — знает локальная политика воркера, и импортировать её сюда
    # незачем (см. комментарий к PROVIDER_BINDING_ENV о независимости рубежа
    # формы от провайдерского слоя).
    "capability",
}


def _validate_provider_requirement(raw: Any) -> Optional[dict[str, Any]]:
    """Форма требования. Смысл — забота резолвера, форма — забота этого рубежа."""
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise AuditJobRejected("provider_requirement: ожидается объект")
    unknown = set(raw) - _PROVIDER_REQUIREMENT_FIELDS
    if unknown:
        raise AuditJobRejected(
            f"provider_requirement: недопустимые поля {sorted(unknown)}"
        )
    provider = str(raw.get("provider") or "").strip()
    if not provider or len(provider) > 32 or not provider.isalpha():
        raise AuditJobRejected(
            f"provider_requirement.provider={provider!r} имеет недопустимую форму"
        )
    model = raw.get("model")
    if model is not None and (not isinstance(model, str) or len(model) > 128):
        raise AuditJobRejected("provider_requirement.model: строка не длиннее 128")
    stages = raw.get("allowed_stages") or []
    if not isinstance(stages, list) or not all(isinstance(x, str) for x in stages):
        raise AuditJobRejected(
            "provider_requirement.allowed_stages: ожидается список строк"
        )
    for stage in stages:
        if not stage.replace("_", "").isalnum() or len(stage) > 64:
            raise AuditJobRejected(
                f"provider_requirement.allowed_stages: недопустимое имя {stage!r}"
            )
    try:
        max_inferences = int(raw.get("max_inferences") or 0)
    except (TypeError, ValueError):
        raise AuditJobRejected(
            "provider_requirement.max_inferences: ожидается целое число"
        ) from None
    if not 0 <= max_inferences <= 8:
        raise AuditJobRejected(
            f"provider_requirement.max_inferences={max_inferences} вне [0, 8]"
        )
    capability = raw.get("capability")
    if capability is not None:
        if not isinstance(capability, str) or not capability.strip():
            raise AuditJobRejected("provider_requirement.capability: непустая строка")
        capability = capability.strip()
        if not capability.replace("_", "").isalnum() or len(capability) > 64:
            raise AuditJobRejected(
                f"provider_requirement.capability={capability!r} имеет недопустимую форму"
            )
        if model:
            raise AuditJobRejected(
                "provider_requirement: capability и model взаимоисключимы — "
                "точную модель для способности выбирает воркер"
            )
    return {
        "provider": provider.lower(),
        "model": model or None,
        "allowed_stages": list(stages),
        "max_inferences": max_inferences,
        "capability": capability or None,
    }


def validate_params(raw: dict[str, Any], *, config: Any) -> SafeAuditParams:
    """Проверить нагрузку задания. Неизвестное поле — отказ, а не игнор."""
    data = raw or {}
    unknown = set(data) - _ALLOWED_FIELDS
    if unknown:
        raise AuditJobRejected(f"Недопустимые поля в задании: {sorted(unknown)}")

    profile = str(data.get("execution_profile") or "")
    if profile != SUPPORTED_PROFILE:
        raise AuditJobRejected(
            f"Профиль {profile!r} не поддерживается: воркер знает только "
            f"{SUPPORTED_PROFILE!r}"
        )
    action = str(data.get("action") or "full")
    if action not in SUPPORTED_ACTIONS:
        raise AuditJobRejected(f"Действие {action!r} не входит в профиль")
    if data.get("include_norms"):
        # Не «не рекомендуется», а невозможно: нормативной базы на воркере нет,
        # и запись в общий norms_paragraphs.json запрещена архитектурно.
        raise AuditJobRejected(
            "Нормативный этап на воркере не выполняется: include_norms=true отвергнут"
        )
    revision = str(data.get("pipeline_revision") or "").strip()
    local_revision = str(getattr(config, "pipeline_revision", "") or "").strip()
    if not local_revision:
        raise AuditJobRejected(
            "AUDIT_WORKER_PIPELINE_REVISION не задана на воркере — сверять "
            "ревизию кода не с чем"
        )
    if revision != local_revision:
        raise AuditJobRejected(
            f"Ревизия конвейера не совпадает: задание {revision!r}, "
            f"воркер {local_revision!r}"
        )
    if not getattr(config, "audit_pipeline_enabled", False):
        raise AuditJobRejected(
            "Приём реального аудита выключен (AUDIT_WORKER_AUDIT_PIPELINE_ENABLED=false)"
        )
    root = getattr(config, "pipeline_root", None)
    if not root or not Path(root).is_dir():
        raise AuditJobRejected(
            "AUDIT_WORKER_PIPELINE_ROOT не указывает на установленный код платформы"
        )

    retry_stage = data.get("retry_stage")
    if retry_stage is not None:
        retry_stage = str(retry_stage)
        if not retry_stage.replace("_", "").isalnum() or len(retry_stage) > 64:
            raise AuditJobRejected(f"Недопустимое имя этапа: {retry_stage!r}")

    # Хэш снимка runtime-конфигурации обязателен. Пустое значение означало бы
    # «примени что найдёшь», а найдёт процесс окружение ХОСТА — то есть режим
    # записи хранилища определяла бы машина, а не центр.
    runtime_hash = str(data.get("runtime_snapshot_hash") or "").strip()
    if len(runtime_hash) < 8:
        raise AuditJobRejected(
            "runtime_snapshot_hash отсутствует или слишком короток — задание "
            "без снимка runtime-конфигурации не исполняется"
        )

    # Дисциплина и хэш её профиля обязательны и проверяются как СКАЛЯРЫ: имя
    # каталога из них не строится нигде на воркере — раскладку профиля делает
    # код платформы по своему проверенному манифесту.
    discipline = str(data.get("discipline_id") or "").strip()
    if not discipline or len(discipline) > 32 or any(
        ch in discipline for ch in "/\\ \t\r\n"
    ):
        raise AuditJobRejected(
            f"discipline_id={discipline!r} отсутствует или имеет недопустимую "
            "форму — задание без дисциплины не исполняется"
        )
    profile_hash = str(data.get("discipline_profile_hash") or "").strip()
    if len(profile_hash) < 8:
        raise AuditJobRejected(
            "discipline_profile_hash отсутствует — проверить, каким профилем "
            "пойдёт прогон, было бы нечем"
        )

    # Свой список зависит от ДЕЙСТВИЯ: синтетическая проверка провайдера не
    # выполняет аудита и артефактов аудита не производит.
    own = required_artifacts_for(action)
    required = data.get("required_result_artifacts") or list(own)
    if not isinstance(required, list) or not all(isinstance(x, str) for x in required):
        raise AuditJobRejected("required_result_artifacts: ожидается список строк")
    # Расширить список заданием нельзя: берём пересечение со СВОИМ.
    merged = tuple(sorted(set(own) | (set(required) & set(own))))

    requirement = _validate_provider_requirement(data.get("provider_requirement"))
    if action == ACTION_PROVIDER_SELFCHECK and requirement is None:
        raise AuditJobRejected(
            "действие provider_selfcheck без provider_requirement бессмысленно: "
            "проверять нечего"
        )

    return SafeAuditParams(
        execution_profile=profile,
        action=action,
        retry_stage=retry_stage,
        include_optimization=bool(data.get("include_optimization", True)),
        include_norms=False,
        pipeline_revision=revision,
        expected_source_tree_hash=str(data.get("expected_source_tree_hash") or ""),
        prompt_bundle_hash=str(data.get("prompt_bundle_hash") or ""),
        model_config_hash=str(data.get("model_config_hash") or ""),
        feature_flags_hash=str(data.get("feature_flags_hash") or ""),
        runtime_snapshot_hash=runtime_hash,
        discipline_id=discipline,
        discipline_profile_hash=profile_hash,
        required_result_artifacts=merged,
        provider_requirement=requirement,
    )


#: Имя файла-маркера, которым каталог провайдеров объявляет себя поддельным.
#: Сами имена исполняемых файлов берутся ИЗ маркера — в пакете воркера их нет.
PROVIDER_MARKER_FILE = "PROVIDERS.json"


def provider_dir_is_fake(path: Path) -> bool:
    """Подтвердить, что каталог содержит подделки, а не настоящие CLI.

    Существования каталога недостаточно: пустой каталог (или указанный на
    `~/.local/bin`) префиксует PATH, ничего не перекрывая, и настоящий бинарь
    находится обычным резолвом — при том, что центру уже отрапортовано
    `provider_mode="fake"`. Маркер превращает заявление в проверяемый факт.
    """
    marker = Path(path) / PROVIDER_MARKER_FILE
    if not marker.is_file():
        return False
    try:
        data = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    if not isinstance(data, dict) or data.get("mode") != "fake":
        return False
    names = data.get("binaries")
    if not isinstance(names, list) or not names:
        return False
    return all(
        isinstance(name, str) and name and (Path(path) / name).is_file()
        for name in names
    )


def build_argv(spec_path: Path, *, config: Any) -> list[str]:
    """Фиксированный argv. Переменная часть одна — путь к спецификации."""
    python = str(getattr(config, "pipeline_python", "") or "") or (
        sys.executable or "python3"
    )
    return [python, "-u", "-m", PIPELINE_ENTRYPOINT_MODULE, str(spec_path)]


def build_env(
    *,
    config: Any,
    job_dir: Path,
    provider_dir: Optional[Path],
    provider_binding: Optional[Path] = None,
) -> dict[str, str]:
    """Окружение из белого списка + корни данных, вычисленные от каталога попытки.

    Ни одна переменная не приходит из задания. Секретов воркера здесь нет:
    исполнитель их и не знает — токен читает только агент.
    """
    # Окружение строится С НУЛЯ, а не копированием `os.environ` с последующей
    # чисткой: чистка знает только то, что в неё внесли, и любой новый секрет в
    # окружении воркера доехал бы до конвейера по умолчанию.
    env = {k: os.environ[k] for k in _ENV_WHITELIST if k in os.environ}
    for name in _ENV_SYSTEM_OPTIONAL:
        if name in os.environ:
            env[name] = os.environ[name]
    root = Path(getattr(config, "pipeline_root"))
    env["PYTHONPATH"] = str(root)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["AUDIT_ROLE"] = "worker"
    # Все корни данных и записи уводятся ВНУТРЬ каталога попытки. Обращение к
    # путям центра невозможно не потому, что «мы так не делаем», а потому что
    # их значения указывают в другое место.
    env.update(isolated_roots(job_dir))
    # Белый список бесполезен, если процесс восстановит окружение сам:
    # конфигурация платформы на импорте вызывает `load_dotenv()`, а тот ищет
    # `.env` вверх от файла и находит его в корне УСТАНОВЛЕННОГО кода. Оттуда
    # вернулись бы и ключи платных API, и `PAID_API_ENABLED=true`.
    env["AUDIT_DISABLE_DOTENV"] = "1"
    if provider_dir is not None:
        # Поддельные провайдеры: путь и явные переменные, по которым конвейер
        # резолвит бинари. PATH тоже правится — часть путей резолва идёт через
        # него, и оставить там настоящий CLI значило бы оставить дыру.
        env["PATH"] = f"{provider_dir}:{env.get('PATH', '')}"
        env["AUDIT_WORKER_PROVIDER_MODE"] = "fake"
        env["AUDIT_WORKER_FAKE_PROVIDER_DIR"] = str(provider_dir)
        # Журнал вызовов подделок. Он и есть доказательство «модель звали, но
        # звали подделку»: без него «внешних соединений не было» неотличимо от
        # «этап до модели не дошёл вовсе». Файл лежит ВНУТРИ каталога попытки и
        # уезжает в пакет результата вместе с остальными логами.
        env["AUDIT_WORKER_FAKE_CALL_LOG"] = str(
            job_dir / "logs" / "fake_provider_calls.jsonl"
        )
        # Точки резолва, которые обходят PATH и берут путь из своей переменной,
        # а также платный HTTP-путь (подделкой CLI он не закрывается вовсе)
        # перекрываются НА СТОРОНЕ КОДА ПЛАТФОРМЫ, в
        # backend/app/pipeline/remote_audit_runner.enforce_fake_providers: имена
        # исполняемых файлов моделей в пакете воркера не упоминаются намеренно
        # (проверяется тестом test_no_llm_invocation_in_worker_package).
    else:
        env["AUDIT_WORKER_PROVIDER_MODE"] = "real"
    if provider_binding is not None:
        # Единственный канал, которым процесс конвейера узнаёт о провайдерском
        # слое. Значение — путь к файлу ВНУТРИ каталога попытки; учётных данных
        # и токенов в нём нет (см. `resolver.ProviderBinding`). Нет переменной —
        # нет моста, и конвейер работает ровно как до этапа 11C.
        env[PROVIDER_BINDING_ENV] = str(provider_binding)
    return env


def command_fingerprint(argv: list[str]) -> str:
    return hashlib.sha256("\x00".join(argv).encode("utf-8")).hexdigest()[:32]


@dataclass
class AuditRunOutcome:
    exit_code: int
    duration_sec: float
    stages_done: int = 0
    stages_total: int = 0
    failed_message: Optional[str] = None
    stdout_lines: int = 0
    stderr_lines: int = 0


def prepare_job_dir(job_dir: Path) -> dict[str, Path]:
    """Разложить каталог попытки. Ничего вне него не создаётся."""
    layout = {
        "source_package": job_dir / "source_package",
        "unpack_staging": job_dir / "unpack_staging",
        "project": job_dir / "project",
        "snapshot": job_dir / "snapshot",
        "work": job_dir / "work",
        "result": job_dir / "result",
        "logs": job_dir / "logs",
        "metadata": job_dir / "metadata",
        "package_output": job_dir / "package_output",
        "usage": job_dir / "usage",
        # Снимок runtime-конфигурации из пакета: он определяет режим записи
        # хранилища, и без него запуск запрещён.
        "runtime": job_dir / "runtime",
        # Снимок профиля дисциплины из пакета. Раскладывает его в рабочие
        # каталоги код платформы, а не агент.
        "discipline_profile": job_dir / "discipline_profile",
        # `comparison/` раньше не имела каталога вовсе и уезжала в корень
        # установленного кода (Б-4).
        "comparison": job_dir / "comparison",
    }
    for path in layout.values():
        path.mkdir(parents=True, exist_ok=True)
    # Каталоги, вычисляемые `isolated_roots`, но не входящие в раскладку
    # разделов: без них процесс создаёт их сам — а `HOME`, созданный процессом
    # позже, успевает побыть несуществующим и часть библиотек падает.
    for extra in isolated_roots(job_dir).values():
        Path(extra).mkdir(parents=True, exist_ok=True)
    return layout


def run_audit_job(
    *,
    params: SafeAuditParams,
    job_dir: Path,
    job_id: str,
    attempt_id: str,
    project_id: str,
    version_id: Optional[str],
    config: Any,
    provider_dir: Optional[Path],
    provider_binding: Optional[Path] = None,
    on_progress: Callable[[dict[str, Any]], None],
    on_log: Callable[[str, str, str], None],
    on_start: Optional[Callable[[int, str], None]] = None,
) -> AuditRunOutcome:
    """Запустить установленный конвейер в изолированном каталоге попытки."""
    layout = prepare_job_dir(job_dir)
    spec_path = layout["metadata"] / "run_spec.json"
    spec = {
        "job_id": job_id,
        "attempt_id": attempt_id,
        "project_id": project_id,
        "version_id": version_id,
        "profile": params.execution_profile,
        "action": params.action,
        "retry_stage": params.retry_stage,
        "include_optimization": params.include_optimization,
        "include_norms": False,
        "pipeline_revision": params.pipeline_revision,
        "expected_source_tree_hash": params.expected_source_tree_hash,
        "prompt_bundle_hash": params.prompt_bundle_hash,
        "model_config_hash": params.model_config_hash,
        "feature_flags_hash": params.feature_flags_hash,
        "runtime_snapshot_hash": params.runtime_snapshot_hash,
        "discipline_id": params.discipline_id,
        "discipline_profile_hash": params.discipline_profile_hash,
        "required_result_artifacts": list(params.required_result_artifacts),
        "provider_mode": "fake" if provider_dir is not None else "real",
        # Разрешение воркера на настоящие модели. Раньше поле не писалось
        # вовсе, поэтому `assert_compatible` всегда получал False и снимок с
        # `provider_mode="real"` отвергался безусловно — то есть настройка
        # `AUDIT_WORKER_ALLOW_REAL_LLM` не работала ни в одну сторону.
        "allow_real_llm": bool(getattr(config, "allow_real_llm", False)),
        # Требование центра переносится в спеку КАК ЕСТЬ: код платформы обязан
        # видеть, что именно было заказано, и сверять это с привязкой, которую
        # исполнитель выписал по своему решению.
        "provider_requirement": params.provider_requirement,
        "provider_binding": str(provider_binding) if provider_binding else None,
        "paths": {key: str(value) for key, value in layout.items()},
    }
    spec_path.write_text(json.dumps(spec, ensure_ascii=False, indent=2), encoding="utf-8")

    argv = build_argv(spec_path, config=config)
    env = build_env(
        config=config, job_dir=job_dir, provider_dir=provider_dir,
        provider_binding=provider_binding,
    )
    fingerprint = command_fingerprint(argv)
    started = time.time()

    stdout_path = layout["logs"] / "stdout.log"
    stderr_path = layout["logs"] / "stderr.log"
    stdout_from = stdout_path.stat().st_size if stdout_path.exists() else 0
    stderr_from = stderr_path.stat().st_size if stderr_path.exists() else 0

    # Дескрипторы принадлежат САМОМУ процессу: уход наблюдателя не должен
    # ронять аудит SIGPIPE'ом на первой строке вывода (тот же урок, что и в
    # test_runner).
    with stdout_path.open("ab") as out_fh, stderr_path.open("ab") as err_fh:
        process = subprocess.Popen(  # noqa: S603 — argv фиксирован, shell=False
            argv,
            cwd=str(Path(getattr(config, "pipeline_root"))),
            env=env,
            stdout=out_fh,
            stderr=err_fh,
            shell=False,
            start_new_session=True,
        )
    if on_start:
        on_start(process.pid, fingerprint)

    state = {"stages_done": 0, "stages_total": 0, "failed": None,
             "stdout_lines": 0, "stderr_lines": 0}
    lock = threading.Lock()

    def handle(name: str, line: str) -> None:
        with lock:
            state[f"{name}_lines"] += 1
        if name == "stdout" and line.startswith("{"):
            try:
                event = json.loads(line)
            except ValueError:
                on_log(name, "info", line)
                return
            kind = str(event.get("type") or "")
            if kind in ("stage_started", "stage_progress", "stage_completed",
                        "artifact_created", "usage"):
                with lock:
                    if kind == "stage_completed":
                        state["stages_done"] += 1
                    total = event.get("stage_total")
                    if total:
                        state["stages_total"] = int(total)
                on_progress(event)
                return
            if kind == "failed":
                with lock:
                    state["failed"] = str(event.get("message") or "конвейер сообщил сбой")
                on_log(name, "error", str(event.get("message") or ""))
                return
            on_log(name, "info", line)
            return
        on_log(name, "error" if name == "stderr" else "info", line)

    finished = threading.Event()

    def follow(path: Path, name: str, offset: int) -> None:
        pending = ""
        with path.open("r", encoding="utf-8", errors="replace") as fh:
            fh.seek(offset)
            while True:
                chunk = fh.read(65536)
                if chunk:
                    pending += chunk
                    *lines, pending = pending.split("\n")
                    for line in lines:
                        if line.strip():
                            handle(name, line.rstrip("\r"))
                    continue
                if finished.is_set():
                    if pending.strip():
                        handle(name, pending.strip())
                    return
                time.sleep(0.05)

    threads = [
        threading.Thread(target=follow, args=(stdout_path, "stdout", stdout_from),
                         name="audit-stdout", daemon=True),
        threading.Thread(target=follow, args=(stderr_path, "stderr", stderr_from),
                         name="audit-stderr", daemon=True),
    ]
    for thread in threads:
        thread.start()
    process.wait()
    time.sleep(0.15)
    finished.set()
    for thread in threads:
        thread.join(timeout=15)

    return AuditRunOutcome(
        exit_code=process.returncode,
        duration_sec=time.time() - started,
        stages_done=int(state["stages_done"]),
        stages_total=int(state["stages_total"]),
        failed_message=state["failed"],
        stdout_lines=int(state["stdout_lines"]),
        stderr_lines=int(state["stderr_lines"]),
    )


def missing_required_artifacts(job_dir: Path, required: tuple[str, ...]) -> list[str]:
    """Каких обязательных артефактов нет. Пустой список = пакет полон."""
    missing: list[str] = []
    for rel in required:
        path = Path(job_dir) / rel
        if not path.is_file() or path.stat().st_size == 0:
            missing.append(rel)
    return missing
