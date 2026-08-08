"""Снимок runtime-конфигурации попытки удалённого аудита.

**Зачем отдельный снимок, когда уже есть манифест пакета и нагрузка задания.**
Манифест описывает АРХИВ, нагрузка описывает ЗАДАНИЕ, а прогон определяется
третьим — тем, как процесс конвейера настроен изнутри: в каком режиме он пишет
хранилище, какие модели у этапов, какие флаги включены, какая политика путей.
Ровно на этом стыке и лежал закрытый здесь дефект: `AUDIT_PROJECTS_V2_WRITE_MODE`
на воркер не передавался вовсе, `storage_write_facade.get_write_mode()` читал
`os.environ` воркера и молча дефолтил в `legacy`, а центр в это время работал в
`projects_v2_primary`. Результат прогона зависел от машины, на которой он шёл, —
то есть воспроизводимости не было.

**Почему `extra="forbid"` и обязательные поля без значений по умолчанию.**
Умолчание в снимке означает «воркер додумал за центр». Именно так и появляется
класс дефектов «на центре одно, на воркере другое, и никто не заметил»:
отсутствующее поле берётся из окружения ЧУЖОЙ машины. Поэтому неизвестное поле —
отказ, отсутствующее обязательное — отказ, и оба до запуска конвейера.

**Чего в снимке нет и быть не может:** ключей, токенов, паролей, `HOME`,
абсолютных путей центра, произвольного окружения, имени исполняемого файла,
argv, shell-команды, имени Python-модуля. Проверяется машинно
(`assert_no_secrets`), а не обещанием.
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

#: Версия схемы снимка. Растёт при несовместимом изменении состава полей.
RUNTIME_SNAPSHOT_VERSION = 1

#: Версии схемы, которые воркер соглашается исполнять.
SUPPORTED_SNAPSHOT_VERSIONS: frozenset[int] = frozenset({1})

#: Режимы записи хранилища. Закрытый набор — «неизвестное значение» на этом
#: рубеже недопустимо: `get_write_mode()` fail-safe дефолтит в `legacy`, и
#: опечатка в конфигурации молча меняла бы раскладку результата.
WRITE_MODES = ("legacy", "dual_write_shadow", "projects_v2_primary")

#: Версия политики путей: какие корни обязаны лежать внутри каталога попытки.
PATH_POLICY_VERSION = 2

#: Версия политики окружения: состав白 списка переменных дочернего процесса.
ENVIRONMENT_POLICY_VERSION = 2

_SECRETISH = re.compile(
    r"(secret|token|password|passwd|api[_-]?key|credential|cookie|bootstrap|"
    r"authorization|bearer|oauth)",
    re.IGNORECASE,
)


class RuntimeConfigError(ValueError):
    """Снимок runtime-конфигурации не принят."""


class AuditRuntimeConfigSnapshot(BaseModel):
    """Immutable-снимок конфигурации прогона на КОНКРЕТНУЮ попытку.

    `frozen=True` — не украшение: снимок хэшируется и уезжает в пакет, и
    изменение его в памяти после сборки означало бы, что хэш описывает не то,
    что применилось.
    """

    model_config = ConfigDict(
        extra="forbid", frozen=True, protected_namespaces=(),
    )

    snapshot_version: int
    pipeline_revision: str
    protocol_version: int
    package_manifest_version: int
    execution_profile: Literal["remote_audit_pilot_v1"]
    project_layout_version: int
    projects_v2_write_mode: Literal["legacy", "dual_write_shadow", "projects_v2_primary"]
    include_norms: Literal[False]
    provider_mode: Literal["fake", "real"]
    stage_model_mapping: dict[str, str] = Field(default_factory=dict)
    prompt_bundle_hash: str
    model_config_hash: str
    feature_flags: dict[str, str] = Field(default_factory=dict)
    feature_flags_hash: str
    output_schema_versions: dict[str, int] = Field(default_factory=dict)
    path_policy_version: int
    environment_policy_version: int
    created_at: float

    # ── сериализация ────────────────────────────────────────────────────────
    def canonical_json(self) -> bytes:
        """Каноническое представление: сортировка ключей, без пробельного шума.

        Хэш обязан зависеть только от ЗНАЧЕНИЙ. Иначе переупаковка того же
        снимка меняла бы хэш, и сверка на воркере ловила бы форматирование.
        """
        return json.dumps(
            self.model_dump(mode="json"),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

    def snapshot_hash(self) -> str:
        return "sha256:" + hashlib.sha256(self.canonical_json()).hexdigest()

    def to_package_bytes(self) -> bytes:
        """Читаемое представление для файла в пакете.

        Форматированный JSON, но хэш считается по КАНОНИЧЕСКОМУ виду: человек
        должен иметь возможность открыть файл, а машина — сверить значение.
        """
        payload = self.model_dump(mode="json")
        payload["_snapshot_hash"] = self.snapshot_hash()
        return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True).encode(
            "utf-8"
        )


def assert_no_secrets(snapshot: "AuditRuntimeConfigSnapshot") -> None:
    """Рубеж: снимок уезжает на ЧУЖОЙ VPS.

    Проверяются и имена, и значения: `feature_flags` собирается по префиксам, и
    достаточно однажды добавить префикс, под который попадёт ключ, чтобы он
    уехал наружу.
    """
    problems: list[str] = []
    for key, value in sorted(snapshot.feature_flags.items()):
        if _SECRETISH.search(key):
            problems.append(f"feature_flags.{key}: имя похоже на секрет")
        text = str(value)
        if text.startswith(("sk-", "ghp_", "gho_", "xoxb-", "eyJ")):
            problems.append(f"feature_flags.{key}: значение похоже на ключ")
        if text.startswith("/") and len(text) > 1:
            problems.append(f"feature_flags.{key}: абсолютный путь центра")
    for key in sorted(snapshot.stage_model_mapping):
        if _SECRETISH.search(key):
            problems.append(f"stage_model_mapping.{key}: имя похоже на секрет")
    if problems:
        raise RuntimeConfigError(
            "Снимок runtime-конфигурации содержит недопустимое: "
            + "; ".join(problems[:5])
        )


def build_snapshot(
    *,
    pipeline_revision: str,
    protocol_version: int,
    package_manifest_version: int,
    execution_profile: str,
    project_layout_version: int,
    projects_v2_write_mode: str,
    provider_mode: str,
    stage_model_mapping: dict[str, str],
    prompt_bundle_hash: str,
    model_config_hash: str,
    feature_flags: dict[str, str],
    feature_flags_hash: str,
    output_schema_versions: Optional[dict[str, int]] = None,
    created_at: float,
) -> AuditRuntimeConfigSnapshot:
    """Собрать снимок с проверкой обязательного ДО сборки пакета.

    `projects_v2_write_mode` обязан прийти явным значением: попытка «взять из
    окружения, если не передали» и есть источник закрываемого дефекта.
    """
    mode = str(projects_v2_write_mode or "").strip().lower()
    if mode not in WRITE_MODES:
        raise RuntimeConfigError(
            f"projects_v2_write_mode={projects_v2_write_mode!r} вне закрытого "
            f"набора {WRITE_MODES}"
        )
    snapshot = AuditRuntimeConfigSnapshot(
        snapshot_version=RUNTIME_SNAPSHOT_VERSION,
        pipeline_revision=str(pipeline_revision or ""),
        protocol_version=int(protocol_version),
        package_manifest_version=int(package_manifest_version),
        execution_profile=execution_profile,          # type: ignore[arg-type]
        project_layout_version=int(project_layout_version),
        projects_v2_write_mode=mode,                  # type: ignore[arg-type]
        include_norms=False,
        provider_mode=provider_mode,                  # type: ignore[arg-type]
        stage_model_mapping={str(k): str(v) for k, v in (stage_model_mapping or {}).items()},
        prompt_bundle_hash=str(prompt_bundle_hash or ""),
        model_config_hash=str(model_config_hash or ""),
        feature_flags={str(k): str(v) for k, v in (feature_flags or {}).items()},
        feature_flags_hash=str(feature_flags_hash or ""),
        output_schema_versions=dict(output_schema_versions or {}),
        path_policy_version=PATH_POLICY_VERSION,
        environment_policy_version=ENVIRONMENT_POLICY_VERSION,
        created_at=float(created_at),
    )
    if not snapshot.pipeline_revision:
        raise RuntimeConfigError("pipeline_revision пуст — сверять ревизии нечем")
    assert_no_secrets(snapshot)
    return snapshot


def load_snapshot(raw: bytes | str, *, expected_hash: Optional[str] = None) -> AuditRuntimeConfigSnapshot:
    """Прочитать снимок из пакета и сверить его хэш.

    Порядок значим: сначала СТРУКТУРА (неизвестное поле — отказ), потом хэш.
    Обратный порядок дал бы «хэш сошёлся» для документа, который мы не умеем
    интерпретировать.
    """
    blob = raw.encode("utf-8") if isinstance(raw, str) else bytes(raw)
    try:
        data = json.loads(blob.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise RuntimeConfigError(f"runtime_config.json нечитаем: {exc}") from exc
    if not isinstance(data, dict):
        raise RuntimeConfigError("runtime_config.json не является объектом JSON")

    embedded = data.pop("_snapshot_hash", None)
    try:
        snapshot = AuditRuntimeConfigSnapshot(**data)
    except Exception as exc:                          # noqa: BLE001 — детали ниже
        raise RuntimeConfigError(f"Снимок не прошёл валидацию: {exc}") from exc

    actual = snapshot.snapshot_hash()
    if embedded and str(embedded) != actual:
        raise RuntimeConfigError(
            f"Хэш внутри снимка не совпал: заявлен {str(embedded)[:23]}…, "
            f"вычислен {actual[:23]}…"
        )
    if expected_hash and str(expected_hash) != actual:
        raise RuntimeConfigError(
            f"Хэш снимка не совпал с заявленным в задании: ожидался "
            f"{str(expected_hash)[:23]}…, вычислен {actual[:23]}…"
        )
    if snapshot.snapshot_version not in SUPPORTED_SNAPSHOT_VERSIONS:
        raise RuntimeConfigError(
            f"Версия снимка {snapshot.snapshot_version} не поддерживается "
            f"(поддерживаются {sorted(SUPPORTED_SNAPSHOT_VERSIONS)})"
        )
    return snapshot


def assert_compatible(
    snapshot: AuditRuntimeConfigSnapshot,
    *,
    supported_profiles: tuple[str, ...],
    supported_layout_versions: frozenset[int],
    allow_real_llm: bool,
) -> None:
    """Семантическая совместимость: структура сошлась, но исполнимо ли это.

    Разделено с валидацией намеренно: «снимок корректен» и «этот воркер имеет
    право так работать» — разные вопросы, и второй решается КОНФИГУРАЦИЕЙ
    воркера, а не содержимым пакета.
    """
    if snapshot.execution_profile not in supported_profiles:
        raise RuntimeConfigError(
            f"Профиль {snapshot.execution_profile!r} воркером не поддерживается"
        )
    if snapshot.project_layout_version not in supported_layout_versions:
        raise RuntimeConfigError(
            f"Раскладка проекта {snapshot.project_layout_version} не поддерживается "
            f"(поддерживаются {sorted(supported_layout_versions)})"
        )
    if snapshot.include_norms is not False:
        raise RuntimeConfigError("include_norms=true недопустим на воркере")
    # Переносимая раскладка 2 резолвится ТОЛЬКО через `resolve_v2_job_paths`,
    # то есть только в режиме `projects_v2_primary`. В двух других режимах
    # `_resolve_job_paths` уходит в legacy-ветку, а `resolve_project_dir` без
    # `must_exist` возвращает ФАНТОМНЫЙ путь вместо ошибки — и прогон падает
    # часами позже как «нет PDF», а не как «проекта нет в пакете».
    # Класс дефекта в этом репозитории известен и повторялся трижды.
    if snapshot.project_layout_version >= 2 and (
        snapshot.projects_v2_write_mode != "projects_v2_primary"
    ):
        raise RuntimeConfigError(
            f"Раскладка {snapshot.project_layout_version} требует режима записи "
            f"'projects_v2_primary', а снимок объявляет "
            f"{snapshot.projects_v2_write_mode!r}: переносимое дерево в этом "
            "режиме не резолвится вовсе"
        )
    if snapshot.provider_mode == "real" and not allow_real_llm:
        raise RuntimeConfigError(
            "Снимок требует настоящих провайдеров, а воркер их не разрешает "
            "(AUDIT_WORKER_ALLOW_REAL_LLM=false)"
        )


def describe_applied(
    snapshot: AuditRuntimeConfigSnapshot, *, applied_write_mode: str
) -> dict[str, Any]:
    """Что ФАКТИЧЕСКИ применилось. Уезжает в evidence и в пакет результата."""
    return {
        "runtime_snapshot_hash": snapshot.snapshot_hash(),
        "snapshot_version": snapshot.snapshot_version,
        "execution_profile": snapshot.execution_profile,
        "pipeline_revision": snapshot.pipeline_revision,
        "declared_write_mode": snapshot.projects_v2_write_mode,
        "applied_write_mode": applied_write_mode,
        "provider_mode": snapshot.provider_mode,
        "include_norms": snapshot.include_norms,
        "project_layout_version": snapshot.project_layout_version,
        "path_policy_version": snapshot.path_policy_version,
        "environment_policy_version": snapshot.environment_policy_version,
        "prompt_bundle_hash": snapshot.prompt_bundle_hash,
        "model_config_hash": snapshot.model_config_hash,
        "feature_flags_hash": snapshot.feature_flags_hash,
        "stage_model_mapping": dict(snapshot.stage_model_mapping),
    }
