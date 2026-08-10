"""Фиксированная точка входа конвейера для удалённого исполнения.

Это НЕ второй конвейер. Здесь нет ни одной стадии, ни одного правила
оркестрации и ни одной строки бизнес-логики аудита: модуль читает
спецификацию, выставляет корни данных, зовёт существующий
`PipelineManager._dispatch_action` и переводит его прогресс в NDJSON на stdout.
Всё остальное делает тот же код, что и на центре, — иначе «удалённый аудит»
означал бы «другой аудит».

Почему отдельная точка входа, а не «CLI с аргументами»: воркеру нельзя дать
канал «выполни произвольную команду». Имя этого модуля — константа в
`audit_worker/audit_runner.py`, единственный аргумент — путь к спецификации,
которую написал САМ воркер. Центр в этой цепочке не участвует.

Запуск:  python -m backend.app.pipeline.remote_audit_runner <run_spec.json>
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Optional

#: Этапы, которые удалённому профилю запрещены. Проверка машинная: спека
#: приходит от воркера, но правило живёт здесь, в коде платформы.
FORBIDDEN_STAGES = ("norm_verify", "decision_carryover", "debt_control", "excel")

#: Переменные окружения, которые открывают доступ к платным HTTP-провайдерам.
#: Поддельные CLI закрывают только «последний метр» двух бинарей; ноги, которые
#: ходят по HTTPS (OpenRouter, OpenAI, Gemini, Anthropic API), подделкой не
#: закрываются вовсе — их нужно гасить именно так, снятием ключа.
_LLM_SECRET_ENV = (
    "OPENROUTER_API_KEY", "OPENAI_API_KEY", "ANTHROPIC_API_KEY",
    "GOOGLE_API_KEY", "GEMINI_API_KEY", "DEEPSEEK_API_KEY",
    "QWEN_API_KEY", "LLM_API_KEY", "AUDIT_LLM_API_KEY",
)

#: Переменные, которыми конвейер резолвит CLI МИМО PATH. Оставить их без
#: перекрытия значит оставить прямой путь к настоящему Claude/Codex.
_CLI_PATH_ENV = {
    "CLAUDE_CLI_BIN": "claude",
    "AUDIT_CODEX_CLI_PATH": "codex",
    "CODEX_CLI_PATH": "codex",
}

#: Корни данных и записи, которые ОБЯЗАНЫ указывать внутрь каталога попытки.
#:
#: Список рос дважды и оба раза по факту пропущенной записи: сперва
#: `AUDIT_PROJECTS_DIR` (рабочий корень legacy-раскладки), затем `COMPARISON_ROOT`,
#: `HOME`, `TMPDIR`, каталог «чистой» cwd для `claude -p` и рабочий каталог
#: `codex exec`. Последние пять — это ровно те записи, которых не было ни в
#: одном `AUDIT_*`-корне: снимок уезжал в каталог установленного кода, а
#: `/tmp/sonnet_clean` и `~/.claude` были общими для всех заданий машины.
_ISOLATED_ROOT_ENV = (
    "AUDIT_PROJECTS_DIR", "AUDIT_PROJECTS_V2_DIR", "AUDIT_DATA_DIR",
    "AUDIT_APP_DATA_DIR", "AUDIT_PROMPTS_DIR", "AUDIT_ACTION_LOG_DIR",
    "COMPARISON_ROOT", "AUDIT_CLEAN_CWD_ROOT", "AUDIT_CODEX_WORKDIR",
    "AUDIT_BLOCK_CROP_CACHE_DIR", "HOME", "TMPDIR",
)

#: Профили, которые ЭТА точка входа умеет исполнять.
SUPPORTED_PROFILES = ("remote_audit_pilot_v1",)

#: Имя переменной строгого режима профиля дисциплины. ДУБЛИКАТ
#: `discipline_identity.STRICT_PROFILE_ENV` — намеренный: `harden_process_env`
#: обязан отработать ДО первого импорта `backend.app.core.config`.
DISCIPLINE_STRICT_ENV = "AUDIT_DISCIPLINE_PROFILE_STRICT"


def harden_process_env() -> None:
    """Закрыть каналы, которые возвращают процессу окружение центра.

    Вызывается ПЕРВЫМ действием, до любого импорта из `backend.app`: и
    `AUDIT_DISABLE_DOTENV`, и запрет центральных этапов читаются на импорте
    конфигурации, то есть позже уже поздно.
    """
    os.environ["AUDIT_DISABLE_DOTENV"] = "1"
    from backend.app.pipeline.execution.registry import (      # локальный импорт: только константа
        CENTRAL_STAGES_DISABLED_ENV,
    )

    os.environ[CENTRAL_STAGES_DISABLED_ENV] = "1"
    # Строгий режим профиля дисциплины: подстановка EOM вместо отсутствующего
    # профиля здесь недопустима. На центре такой аудит виден в логе оператора,
    # на воркере лог остаётся на чужой машине — и «раздел ВК аудирован
    # профилем ЭОМ» узнать неоткуда.
    #
    # Имя переменной ЛИТЕРАЛ, а не импорт: `discipline_identity` тянет
    # `backend.app.core.config`, а конфигурация читает и `AUDIT_DISABLE_DOTENV`,
    # и запрет центральных этапов НА ИМПОРТЕ. Импортировать что-либо из
    # `backend.app` внутри этой функции значит зафиксировать порядок, который
    # следующая правка сломает молча. Совпадение константы проверяется тестом.
    os.environ[DISCIPLINE_STRICT_ENV] = "1"


def emit(event: dict[str, Any]) -> None:
    """Одна строка NDJSON на stdout. Наблюдатель воркера читает именно их."""
    sys.stdout.write(json.dumps(event, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def validate_project_id(raw: Any) -> str:
    """Проверить `project_id` как ЧАСТЬ ПУТИ, а не как ярлык.

    `project_id` в этом проекте — путь относительно корня проектов (включая
    подпапку дисциплины, «АР/133-23-ГК-АР5»), и `resolve_project_dir` делает
    `projects_dir / project_id`. Значит непроверенное значение выводит запись и
    ЧТЕНИЕ за каталог попытки: `..` поднимается вверх, а абсолютный путь при
    join просто отбрасывает левую часть. Прочитанное при этом уезжает в
    `03_findings.json`, то есть в пакет результата и на центр.
    """
    value = str(raw or "").strip()
    if not value:
        raise SystemExit("project_id пуст")
    if len(value) > 300:
        raise SystemExit("project_id длиннее 300 символов")
    if value.startswith(("/", "\\")) or (len(value) > 1 and value[1] == ":"):
        raise SystemExit(f"project_id не может быть абсолютным путём: {value!r}")
    normalized = value.replace("\\", "/")
    parts = [part for part in normalized.split("/") if part]
    if not parts:
        raise SystemExit(f"project_id не содержит имени: {value!r}")
    for part in parts:
        if part in (".", "..") or part.startswith("~"):
            raise SystemExit(f"project_id содержит недопустимый сегмент: {value!r}")
    if any(ord(ch) < 32 for ch in value):
        raise SystemExit("project_id содержит управляющие символы")
    return value


def load_spec(path: Path) -> dict[str, Any]:
    spec = json.loads(Path(path).read_text(encoding="utf-8"))
    if spec.get("include_norms"):
        raise SystemExit("include_norms=true недопустим для удалённого профиля")
    if spec.get("profile") != "remote_audit_pilot_v1":
        raise SystemExit(f"Неизвестный профиль: {spec.get('profile')!r}")
    stage = spec.get("retry_stage")
    if stage and stage in FORBIDDEN_STAGES:
        raise SystemExit(f"Этап {stage!r} выполняется только на центре")
    spec["project_id"] = validate_project_id(spec.get("project_id"))
    return spec


def apply_runtime_paths(spec: dict[str, Any]) -> None:
    """Закрепить корни данных внутри каталога попытки.

    Переменные уже выставлены воркером; здесь они ПРОВЕРЯЮТСЯ. Смысл проверки
    не в недоверии к воркеру, а в том, что процесс, запущенный руками с
    неполным окружением, не должен писать в чужие каталоги.
    """
    paths = spec.get("paths") or {}
    project_root = Path(paths.get("project") or "")
    if not project_root.is_dir():
        raise SystemExit(f"Каталог проекта не найден: {project_root}")
    # Переносимый корень обязан быть корнем `projects_v2`, а не «каталогом с
    # файлами версии». Проверка здесь, а не только в распаковщике: процесс,
    # запущенный руками по чужой спеке, не должен доходить до первого этапа с
    # деревом, которое не резолвится.
    if not (project_root / "objects").is_dir():
        raise SystemExit(
            f"{project_root} не является переносимым корнем projects_v2: нет "
            "каталога objects/ (плоская раскладка пакета версии 1 не "
            "поддерживается — на ней resolve_v2_job_paths возвращает None, "
            "а resolve_project_dir отдаёт файл вместо каталога)"
        )
    job_dir = project_root.parent.resolve()
    for name in _ISOLATED_ROOT_ENV:
        value = os.environ.get(name, "")
        if not value:
            raise SystemExit(f"{name} не задана — запуск вне изоляции запрещён")
        resolved = Path(value).resolve()
        if job_dir not in resolved.parents and resolved != job_dir:
            raise SystemExit(
                f"{name}={value} указывает вне каталога попытки {job_dir}"
            )
    # Остальные пути спеки тоже пишутся конвейером, и проверялись только
    # `project`. Смысл функции — «процесс, запущенный руками с неполным
    # окружением, не должен писать в чужие каталоги»; для env-корней это
    # выполнялось, для путей спеки — нет.
    for name in ("result", "work", "usage", "metadata", "snapshot", "runtime",
                 "logs", "comparison", "discipline_profile"):
        raw = paths.get(name)
        if not raw:
            continue
        resolved = Path(raw).resolve()
        if job_dir not in resolved.parents and resolved != job_dir:
            raise SystemExit(
                f"paths.{name}={raw} указывает вне каталога попытки {job_dir}"
            )

    # ROOT_DIR/BASE_DIR воркер не выставляет, а `.env` мог бы — после
    # AUDIT_DISABLE_DOTENV не может, но проверка дешёвая и явная.
    for name in ("AUDIT_ROOT_DIR", "AUDIT_BASE_DIR"):
        if os.environ.get(name):
            raise SystemExit(f"{name} не должна быть задана при удалённом исполнении")


def apply_runtime_snapshot(spec: dict[str, Any]) -> dict[str, Any]:
    """Прочитать снимок runtime-конфигурации из пакета и ПРИМЕНИТЬ его.

    Это и есть закрытие ограничения 4 отчёта 06: до сих пор
    `AUDIT_PROJECTS_V2_WRITE_MODE` на воркер не передавался вовсе, а
    `storage_write_facade.get_write_mode()` читал `os.environ` ВОРКЕРА и
    fail-safe дефолтил в `legacy` — тогда как центр работает в
    `projects_v2_primary`. Результат прогона зависел от машины.

    Порядок обязателен и не переставляется:

      1. снимок обязан существовать — иначе отказ ДО запуска конвейера;
      2. структура (неизвестное поле — отказ, отсутствующее обязательное — отказ);
      3. хэш сверяется с заявленным в задании;
      4. семантическая совместимость с ЭТИМ воркером;
      5. и только потом значение попадает в окружение процесса.

    Значение из пакета побеждает значение хоста ПО ПОСТРОЕНИЮ: `build_env`
    воркера переменную не наследует вовсе (её нет в белом списке), поэтому в
    момент вызова её в окружении просто нет — а если процесс запущен руками и
    она там оказалась, она перезаписывается здесь и факт перезаписи попадает в
    evidence.
    """
    from backend.app.services.distributed_workers import project_package, runtime_config

    paths = spec.get("paths") or {}
    runtime_dir = Path(paths.get("runtime") or "")
    source = runtime_dir / "runtime_config.json"
    if not source.is_file():
        raise SystemExit(
            f"Снимок runtime-конфигурации не найден: {source}. Запуск без него "
            "запрещён: режим записи хранилища взялся бы с ХОСТА воркера."
        )
    try:
        snapshot = runtime_config.load_snapshot(
            source.read_bytes(),
            expected_hash=spec.get("runtime_snapshot_hash") or None,
        )
        runtime_config.assert_compatible(
            snapshot,
            supported_profiles=SUPPORTED_PROFILES,
            supported_layout_versions=project_package.SUPPORTED_PROJECT_LAYOUT_VERSIONS,
            allow_real_llm=bool(spec.get("allow_real_llm")),
        )
    except runtime_config.RuntimeConfigError as exc:
        raise SystemExit(f"Снимок runtime-конфигурации отвергнут: {exc}") from exc

    host_value = os.environ.get("AUDIT_PROJECTS_V2_WRITE_MODE")
    os.environ["AUDIT_PROJECTS_V2_WRITE_MODE"] = snapshot.projects_v2_write_mode

    # Фактически применённое значение читается ОБРАТНО у фасада, а не
    # переписывается из снимка: «мы выставили переменную» и «фасад считает так
    # же» — разные утверждения, и evidence обязан содержать второе.
    from backend.app.services.storage import storage_write_facade

    applied = storage_write_facade.get_write_mode()
    if applied != snapshot.projects_v2_write_mode:
        raise SystemExit(
            f"Режим записи не применился: снимок требует "
            f"{snapshot.projects_v2_write_mode!r}, фасад видит {applied!r}"
        )

    # Режим провайдеров — тоже часть снимка, и он ОБЯЗЫВАЕТ. Раньше подделки
    # включались только по спеке, которую пишет сам воркер: воркер с
    # `AUDIT_WORKER_ALLOW_REAL_LLM=true` мог выполнить задание, заказанное как
    # `fake`, настоящими моделями — а в манифест результата уехало бы
    # `provider_mode` из снимка, то есть «fake». Ужесточение одностороннее:
    # снимок может потребовать подделок, но не может потребовать настоящих.
    if snapshot.provider_mode == "fake" and str(spec.get("provider_mode")) != "fake":
        spec["provider_mode"] = "fake"

    evidence = runtime_config.describe_applied(snapshot, applied_write_mode=applied)
    evidence["host_write_mode_overridden"] = host_value
    evidence["provider_mode_forced_by_snapshot"] = (
        snapshot.provider_mode == "fake"
    )
    metadata_dir = Path(paths.get("metadata") or runtime_dir)
    metadata_dir.mkdir(parents=True, exist_ok=True)
    (metadata_dir / "applied_runtime_config.json").write_text(
        json.dumps(evidence, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return evidence


def apply_discipline_profile(spec: dict[str, Any]) -> dict[str, Any]:
    """Разложить снимок профиля дисциплины и ПОДТВЕРДИТЬ, что он применился.

    Три отдельных утверждения, и каждое проверяется своим шагом:

      1. пакет содержит снимок профиля, его состав и хэши сходятся, а его
         `discipline_id`/`tree_hash` совпадают с заявленными в задании;
      2. файлы разложены в те каталоги, откуда конвейер читает профиль
         (`AUDIT_PROMPTS_DIR/disciplines/<dir>` и
         `AUDIT_APP_DATA_DIR/discipline_checklists*`);
      3. `discipline_service.load_discipline(discipline_id)` возвращает ИМЕННО
         эту дисциплину — то есть профиль не просто лежит на диске, а
         действительно выбирается.

    Третий шаг существен: первые два выполнялись бы и в мире, где
    `load_discipline` продолжал бы молча подставлять EOM. «Файл на месте» и
    «профиль применён» — разные утверждения.
    """
    from backend.app.services.common import discipline_identity, discipline_service
    from backend.app.services.distributed_workers import discipline_profile

    paths = spec.get("paths") or {}
    profile_root = Path(paths.get("discipline_profile") or "")
    expected_id = str(spec.get("discipline_id") or "").strip()
    expected_hash = str(spec.get("discipline_profile_hash") or "").strip()
    if not profile_root.is_dir():
        raise SystemExit(
            f"Снимок профиля дисциплины не найден: {profile_root}. Запуск без "
            "него запрещён: профиль взялся бы из дерева установленного кода."
        )
    prompts_dir = Path(os.environ.get("AUDIT_PROMPTS_DIR") or "")
    app_data_dir = Path(os.environ.get("AUDIT_APP_DATA_DIR") or "")
    if not prompts_dir or not app_data_dir:
        raise SystemExit(
            "AUDIT_PROMPTS_DIR/AUDIT_APP_DATA_DIR не заданы — раскладывать "
            "профиль некуда"
        )
    try:
        applied = discipline_profile.materialize_profile(
            profile_root,
            prompts_dir=prompts_dir,
            app_data_dir=app_data_dir,
            expected_discipline=expected_id or None,
            expected_tree_hash=expected_hash or None,
        )
    except discipline_profile.DisciplineProfileSnapshotError as exc:
        raise SystemExit(f"Снимок профиля дисциплины отвергнут: {exc}") from exc

    # Кэш профилей мог быть прогрет импортом до раскладки файлов.
    discipline_service.invalidate_cache()
    try:
        profile = discipline_service.load_discipline(expected_id)
    except (
        discipline_identity.DisciplineError,
        discipline_service.DisciplineProfileMissing,
    ) as exc:
        raise SystemExit(
            f"Профиль дисциплины {expected_id!r} не загружается после "
            f"раскладки: {exc}"
        ) from exc
    if profile.code != expected_id:
        raise SystemExit(
            f"Конвейер выбрал профиль {profile.code!r} вместо {expected_id!r} — "
            "подстановка чужого профиля запрещена"
        )
    if not profile.role.strip() or not profile.checklist.strip():
        raise SystemExit(
            f"Профиль {expected_id!r} загружен пустым (role/checklist) — "
            "аудит без ролевого профиля не запускается"
        )
    applied["role_chars"] = len(profile.role)
    applied["checklist_chars"] = len(profile.checklist)
    applied["loaded_code"] = profile.code
    metadata_dir = Path(paths.get("metadata") or profile_root)
    metadata_dir.mkdir(parents=True, exist_ok=True)
    (metadata_dir / "applied_discipline_profile.json").write_text(
        json.dumps(applied, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return applied


def apply_model_snapshot(spec: dict[str, Any]) -> Optional[Path]:
    """Положить снимок `stage_models.json` туда, где конвейер его читает.

    Без этого шага центр хэшировал конфигурацию моделей, воркер сверял хэш — и
    запускал аудит на СВОИХ дефолтах из кода (`ensemble/gpt-codex`, то есть
    платный HTTP). Проверка хэша при этом давала ложную уверенность «тот же код
    и та же конфигурация».
    """
    import shutil

    paths = spec.get("paths") or {}
    source = Path(paths.get("snapshot") or ".") / "stage_models.json"
    app_data = os.environ.get("AUDIT_APP_DATA_DIR", "")
    if not source.is_file() or not app_data:
        return None
    target = Path(app_data) / "stage_models.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)
    # Копирования НЕДОСТАТОЧНО: `STAGE_MODEL_CONFIG` читается один раз на
    # импорте `backend.app.core.config`, и если конфигурация уже импортирована
    # (а её тянет почти любой шаг подготовки), снимок не применится — прогон
    # молча уедет на дефолты кода, включая `ensemble/gpt-codex` с ногой в
    # OpenRouter по HTTPS. Раньше это «работало» только потому, что импорт
    # случался позже; порядок импортов гарантией быть не может.
    from backend.app.core import config as _config

    applied = _config.reload_stage_model_config()
    stage01 = applied.get("block_batch")
    if not stage01:
        raise SystemExit(
            "Снимок stage_models.json применён, но модель этапа block_batch не "
            "определена — прогон пошёл бы на дефолте кода"
        )
    return target


def enforce_fake_providers(spec: dict[str, Any]) -> dict[str, Any]:
    """Гарантировать, что настоящая модель НЕ будет вызвана.

    Подделка двух CLI закрывает только вызовы через `subprocess`. Реальные
    дефолты этапов ходят в OpenRouter по HTTPS, а `CLAUDE_CLI_BIN` и
    `AUDIT_CODEX_CLI_PATH` резолвят бинарь мимо PATH. Поэтому в fake-режиме:

    * платный API выключается явно;
    * ключи провайдеров удаляются из окружения — нога без ключа падает, а не
      уходит в сеть;
    * переменные резолва CLI указываются на подделки;
    * отсутствие подделок = отказ запуска, а не тихий переход к настоящему CLI.
    """
    if str(spec.get("provider_mode") or "") != "fake":
        return {"mode": "real"}

    os.environ["PAID_API_ENABLED"] = "false"
    removed = [name for name in _LLM_SECRET_ENV if os.environ.pop(name, None)]

    fake_dir = Path(os.environ.get("AUDIT_WORKER_FAKE_PROVIDER_DIR") or "")
    if not fake_dir.is_dir():
        raise SystemExit(
            "provider_mode=fake, но каталог поддельных провайдеров не найден: "
            f"{fake_dir}"
        )
    bound: dict[str, str] = {}
    for env_name, binary in _CLI_PATH_ENV.items():
        candidate = fake_dir / binary
        if not candidate.is_file():
            raise SystemExit(
                f"В каталоге подделок нет {binary!r}: настоящий CLI остался бы "
                "достижимым через " + env_name
            )
        os.environ[env_name] = str(candidate)
        bound[env_name] = str(candidate)
    return {"mode": "fake", "secrets_removed": len(removed), "cli_bound": bound}


def bind_providers(spec: dict[str, Any]) -> dict[str, Any]:
    """Сверить привязку провайдера со спекой и объявить мост активным.

    Переменную `AUDIT_WORKER_PROVIDER_BINDING` ставит ИСПОЛНИТЕЛЬ, а не этот
    модуль: разрешение, которое процесс конвейера выписывает себе сам, не
    является разрешением. Здесь только ПРОВЕРКА — и она нужна ровно по той же
    причине, что и `apply_runtime_paths`: процесс, запущенный руками с чужой
    спекой, не должен добраться до авторизованного CLI.

    Три утверждения, каждое своим шагом:

      1. fake-режим и привязка несовместимы. В fake-режиме конвейер обязан
         ходить к подделкам, и «мост к настоящему CLI» рядом с ними — это либо
         ошибка развёртывания, либо обход запрета. Отказ, а не выбор одного из
         двух;
      2. задание и попытка в привязке совпадают с заданием и попыткой в спеке.
         Спеку и привязку пишет один исполнитель, но РАЗНЫМ кодом и из разных
         полей строки очереди: совпадение двух записей — доказательство, что
         привязка относится к этой попытке;
      3. привязанный провайдер — тот, которого потребовал центр.
    """
    binding_env = os.environ.get("AUDIT_WORKER_PROVIDER_BINDING", "").strip()
    requirement = spec.get("provider_requirement") or None
    if not binding_env:
        if requirement and int((requirement or {}).get("max_inferences") or 0) > 0:
            raise SystemExit(
                "задание требует вызова модели, но привязка провайдера не "
                "передана процессу конвейера: исполнитель её не выписал"
            )
        return {"bridge": "inactive"}

    if str(spec.get("provider_mode") or "") == "fake":
        raise SystemExit(
            "provider_mode=fake и привязка провайдера одновременно: в режиме "
            "подделок мост к настоящему CLI недопустим"
        )

    from audit_worker.providers.resolver import (            # noqa: PLC0415
        ProviderBinding,
        ProviderResolutionError,
    )

    try:
        binding = ProviderBinding.read(Path(binding_env))
    except ProviderResolutionError as exc:
        raise SystemExit(f"привязка провайдера отвергнута: {exc}") from None

    if binding.job_id != str(spec.get("job_id") or ""):
        raise SystemExit(
            f"привязка провайдера относится к заданию {binding.job_id!r}, "
            f"а спека — к {spec.get('job_id')!r}"
        )
    if binding.attempt_id != str(spec.get("attempt_id") or ""):
        raise SystemExit(
            f"привязка провайдера относится к попытке {binding.attempt_id!r}, "
            f"а спека — к {spec.get('attempt_id')!r}"
        )
    if requirement and binding.provider != str(requirement.get("provider") or ""):
        raise SystemExit(
            f"привязан провайдер {binding.provider!r}, а задание требует "
            f"{requirement.get('provider')!r}"
        )
    if not binding.grant_id:
        raise SystemExit(
            "в привязке нет идентификатора разрешения: вызов модели без "
            "списанного разрешения оператора не выполняется"
        )
    spec["_provider_binding"] = binding
    routing = activate_routing_plan(spec, binding=binding)
    # В evidence уезжает ПУБЛИЧНЫЙ вид: без абсолютных путей и без контрольных
    # литералов оператора.
    return {"bridge": "active", **binding.as_public_dict(), "routing_plan": routing}


def activate_routing_plan(
    spec: dict[str, Any], *, binding: Any = None
) -> dict[str, Any]:
    """Разобрать план задания, сверить хэш и сделать его планом ПРОГОНА.

    Три утверждения, и каждое стоит отдельного отказа.

    **План обязан разбираться и проходить доменную проверку.** Отвергнутый план
    — это не «поедем как раньше»: «как раньше» означает читать глобальную
    конфигурацию машины, а она на воркере вообще не та, что была у оператора в
    момент запуска.

    **Хэш плана обязан совпасть с хэшем в привязке.** Привязку писал исполнитель
    из нагрузки задания, план приехал той же нагрузкой, но разными полями и
    разным кодом. Совпадение двух независимо посчитанных значений — это и есть
    доказательство, что центр и воркер держат ОДИН маршрут.

    **Задание с вызовами модели без плана не исполняется.** Fail closed: иначе
    первое же задание от старого центра тихо вернуло бы прежнее поведение.
    """
    from backend.app.services.audit_routing import active_plan as _active_plan
    from backend.app.services.audit_routing import validator as _routing_validator
    from backend.app.services.audit_routing.plan import RoutingPlan, RoutingPlanError

    raw = spec.get("routing_plan") or None
    requirement = spec.get("provider_requirement") or None
    wants_model = bool(requirement) and int((requirement or {}).get("max_inferences") or 0) > 0
    if not raw:
        if wants_model:
            raise SystemExit(
                "задание требует обращений к модели, но не несёт плана "
                "маршрутизации. Возврат к глобальной конфигурации запрещён: "
                "состав моделей обязан приходить замороженным вместе с заданием"
            )
        _active_plan.clear()
        return {"active": False, "reason": "план не передан, вызовы модели не требуются"}

    try:
        plan = RoutingPlan.from_dict(raw)
        _routing_validator.validate(plan)
    except RoutingPlanError as exc:
        raise SystemExit(f"план маршрутизации отвергнут: {exc}") from None

    declared = str(getattr(binding, "routing_plan_hash", "") or "")
    if declared:
        try:
            plan.assert_hash(declared)
        except RoutingPlanError as exc:
            raise SystemExit(str(exc)) from None

    _active_plan.set_plan(plan)
    applied = apply_routing_flags(plan)
    described = _active_plan.describe()
    described["applied_feature_flags"] = applied
    return described


#: Флаги, которые конвейер читает КАК АТРИБУТЫ модуля конфигурации, а не из
#: окружения на каждом вызове. Их мало и они перечислены поимённо: угадывать по
#: совпадению имён нельзя — в `config` есть одноимённые значения другого типа.
_CONFIG_BOOL_FLAGS: tuple[str, ...] = (
    "STAGE01_DUAL_REVIEW_ENABLED",
    "STAGE01_DUAL_GAP_SEARCH_ENABLED",
    "OPTIMIZATION_CRITIC_DETERMINISTIC",
    "PIPELINE_VERIFIER_ENABLED",
    "PIPELINE_NORMS_AFTER_MERGE_ENABLED",
    "PIPELINE_BLOCKS_BEFORE_TEXT_ENABLED",
    "FINDING_EVIDENCE_OCR_OBSERVER_ENABLED",
)

#: То же для модуля этапа 01: там часть флагов читается на импорте модуля.
_STAGE01_MODULE_FLAGS: tuple[str, ...] = (
    "STAGE01_THIRD_LEG_ENABLED",
    "STAGE01_PROTECTION_TABLE_CHECK_ENABLED",
)


def _flag_is_true(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def apply_routing_flags(plan: Any) -> dict[str, Any]:
    """Применить ЗАМОРОЖЕННЫЕ флаги плана к процессу конвейера.

    Без этого шага заморозка была бы наполовину фикцией, и это не гипотеза.

    Процесс конвейера на воркере стартует с окружением, собранным С НУЛЯ по
    белому списку (`PATH/LANG/LC_ALL/TZ`) плюс `AUDIT_DISABLE_DOTENV=1`. Снимок
    флагов приезжает в пакете, но только СВЕРЯЕТСЯ по хэшу и никуда не
    применяется — то есть на воркере действуют КОДОВЫЕ умолчания. Для
    `OPTIMIZATION_CRITIC_DETERMINISTIC` это `False` при плане, объявляющем
    `true`: этап F OPT Fix уходил бы в агентную ветку, которой в плане нет, и
    получал бы отказ моста на каждом прогоне, где критик нашёл проблемы. Для
    `STAGE01_DUAL_GAP_SEARCH_ENABLED` — судья работал бы без кропа, то есть
    без gap-search, который план ему предписывает.

    Порядок применения: окружение (для тех, кто читает его на каждом вызове),
    затем атрибуты уже импортированных модулей (для тех, кто прочитал его
    однажды). Второе без первого не работает, первое без второго — тоже.
    """
    flags = plan.flags if hasattr(plan, "flags") else {}
    if not flags:
        return {"applied": 0}

    # ЗАКРЫТЫЙ список имён, а не «всё, что заглавными» (этап 11J).
    #
    # Прежний фильтр `name.isupper()` означал, что ЛЮБОЕ имя из нагрузки
    # задания попадает в окружение процесса конвейера. Снимок флагов на
    # СТОРОНЕ ЦЕНТРА собирается по закрытому списку (`collect_feature_flags`),
    # но воркер разбирает план заново и такой проверки не делал: он сверял
    # набор верхних ключей и запрет точных моделей, а состав `feature_flags`
    # оставался свободным словарём.
    #
    # Цена дыры стала другой ровно на 11J. До него в окружении конвейера не
    # было ни одной переменной, меняющей МАРШРУТ СЕТЕВОГО ЗАПРОСА; теперь
    # такие есть: адрес шлюза, объявление заглушек и путь к файлу ключа.
    # Задание, положившее `AUDIT_WORKER_PROVIDER_OPENROUTER_BASE_URL` в
    # `feature_flags`, увело бы ключ владельца VPS на произвольный хост — и
    # выглядело бы это как обычный успешный прогон.
    #
    # Список берётся из реестра ЦЕНТРА, потому что именно он определяет, что
    # такое «флаг маршрутизации». Всё прочее отбрасывается и попадает в отчёт
    # отдельным полем: молча выброшенный флаг — это план, который исполнен не
    # так, как записан.
    from backend.app.services.audit_routing import registry as _routing_registry

    allowed = set(_routing_registry.ROUTING_FEATURE_FLAGS)
    applied: dict[str, Any] = {}
    rejected: list[str] = []
    for name, value in flags.items():
        if not isinstance(name, str) or not name.isupper():
            # Снимок несёт и типизированные значения вроде класса модели
            # Claude — они не переменные окружения и в него не пишутся.
            continue
        if name.startswith("CLAUDE_DEFAULT_"):
            continue
        if name not in allowed:
            rejected.append(name)
            continue
        os.environ[name] = str(value)
        applied[name] = str(value)
    if rejected:
        # Не исключение: план мог приехать с новой сборки центра, знающей флаг,
        # которого эта сборка воркера ещё не знает. Но и не молчание — отказ
        # обязан быть виден в отчёте о прогоне.
        logging.getLogger(__name__).warning(
            "routing_plan.feature_flags: имена вне реестра маршрутизации "
            "отброшены: %s", sorted(rejected),
        )

    # Модули, прочитавшие флаг однажды. Импортируются мягко: конвейер мог их
    # ещё не тронуть, и это нормально — тогда они прочитают уже правленое
    # окружение сами.
    try:
        from backend.app.core import config as _cfg

        for name in _CONFIG_BOOL_FLAGS:
            if name in flags and hasattr(_cfg, name):
                setattr(_cfg, name, _flag_is_true(flags[name]))
    except Exception:                                  # noqa: BLE001 — fail-soft
        pass

    stage01 = sys.modules.get(
        "backend.app.pipeline.stages.block_analysis.gemma_findings_only"
    )
    if stage01 is not None:
        for name in _STAGE01_MODULE_FLAGS:
            if name in flags and hasattr(stage01, name):
                setattr(stage01, name, _flag_is_true(flags[name]))
    return {"applied": len(applied), "flags": applied,
            "rejected": sorted(rejected)}


def verify_snapshot(spec: dict[str, Any]) -> dict[str, Any]:
    """Сверить распакованные снимки с заявленными хэшами."""
    from backend.app.services.distributed_workers import project_package

    paths = spec.get("paths") or {}
    snapshot_dir = Path(paths.get("snapshot") or "")
    result: dict[str, Any] = {"prompts": None, "models": None, "flags": None}
    if not snapshot_dir.is_dir():
        return result

    prompts = project_package.collect_prompt_snapshot(snapshot_dir / "prompts")
    result["prompts"] = project_package.hash_files(prompts)
    models = project_package.collect_model_config_snapshot(
        snapshot_dir / "stage_models.json"
    )
    result["models"] = project_package.hash_files(models)
    flags_path = snapshot_dir / "feature_flags.json"
    if flags_path.is_file():
        flags = json.loads(flags_path.read_text(encoding="utf-8"))
        result["flags"] = project_package.hash_json(flags)

    mismatches = []
    if spec.get("prompt_bundle_hash") and result["prompts"] != spec["prompt_bundle_hash"]:
        mismatches.append("prompts")
    if spec.get("model_config_hash") and result["models"] != spec["model_config_hash"]:
        mismatches.append("stage_models")
    if spec.get("feature_flags_hash") and result["flags"] != spec["feature_flags_hash"]:
        mismatches.append("feature_flags")
    if mismatches:
        raise SystemExit(
            "Снимок конфигурации не совпадает с заявленным: " + ", ".join(mismatches)
        )
    return result


def write_result_manifest(spec: dict[str, Any], payload: dict[str, Any]) -> Path:
    paths = spec.get("paths") or {}
    target = Path(paths.get("result") or ".") / "audit_manifest.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return target


def write_usage_report(spec: dict[str, Any], entries: list[dict[str, Any]]) -> Path:
    paths = spec.get("paths") or {}
    target = Path(paths.get("usage") or ".") / "usage_report.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(
            {
                "job_id": spec.get("job_id"),
                "attempt_id": spec.get("attempt_id"),
                "provider_mode": spec.get("provider_mode"),
                "generated_at": time.time(),
                "entries": entries,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return target


def collect_usage(project_id: str) -> list[dict[str, Any]]:
    """Собрать расход ЛОКАЛЬНОГО прогона. В центральные файлы воркер не пишет."""
    try:
        from backend.app.services.common.usage_service import usage_tracker

        data = usage_tracker.get_project_usage(project_id)      # type: ignore[attr-defined]
    except Exception:                              # noqa: BLE001 — учёт fail-soft
        return []
    if not isinstance(data, dict):
        return []
    entries: list[dict[str, Any]] = []
    # `get_project_usage` отдаёт разбивку по этапам под ключом `stages_summary`.
    # Чтение `stages` давало ПУСТОЙ отчёт всегда — и при этом файл существовал,
    # то есть проверка «обязательные артефакты на месте» его пропускала.
    per_stage = data.get("stages_summary")
    if not isinstance(per_stage, dict):
        per_stage = data.get("stages") if isinstance(data.get("stages"), dict) else {}
    for stage, payload in (per_stage or {}).items():
        if not isinstance(payload, dict):
            continue
        entries.append(
            {
                "stage": stage,
                "model": payload.get("model") or "",
                "input_tokens": int(payload.get("input_tokens") or 0),
                "output_tokens": int(payload.get("output_tokens") or 0),
                "cache_creation_tokens": int(payload.get("cache_creation_tokens") or 0),
                "cache_read_tokens": int(payload.get("cache_read_tokens") or 0),
                "cost_usd": float(payload.get("paid_cost_usd") or 0.0),
                "cost_usd_notional": float(payload.get("notional_cost_usd") or 0.0),
                "calls": int(payload.get("calls") or 0),
                "duration_ms": int(payload.get("duration_ms") or 0),
                "source": "worker",
            }
        )
    return entries


def write_pipeline_log(spec: dict[str, Any], stages: dict[str, Any]) -> Path:
    """Журнал этапов для действий, которые не идут через `_dispatch_action`.

    На основном пути `pipeline_log.json` пишет `audit_logger`, и оттуда его
    копирует `publish_deliverables`. Синтетическая проверка провайдера
    менеджера не запускает вовсе, поэтому журнал пишется здесь — но пишется
    НАСТОЯЩИЙ, в той же схеме: по нему работает проверка §14 (`audit_stage_history`),
    и подделывать её вход было бы бессмысленно.
    """
    paths = spec.get("paths") or {}
    target = Path(paths.get("work") or ".") / "pipeline_log.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(
            {
                "job_id": spec.get("job_id"),
                "attempt_id": spec.get("attempt_id"),
                "project_id": spec.get("project_id"),
                "action": spec.get("action"),
                "updated_at": time.time(),
                "stages": stages,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return target


def run_provider_selfcheck(spec: dict[str, Any]) -> int:
    """Синтетическая проверка сквозного пути к модели (этап 11C).

    Действие профиля, а не отладочный режим: те же снимки центра, та же
    изоляция, та же сборка пакета. Разница только в объёме работы — один вызов
    модели вместо восьми этапов аудита.
    """
    import asyncio

    from backend.app.models.audit import AuditJob
    from backend.app.pipeline.manager import pipeline_manager
    from backend.app.pipeline.stages import provider_selfcheck as stage_mod

    paths = spec.get("paths") or {}
    project_id = str(spec.get("project_id") or "")
    job = AuditJob(
        job_id=str(spec.get("job_id") or "remote"),
        project_id=project_id,
        version_id=spec.get("version_id"),
    )
    emit({"type": "stage_started", "stage": stage_mod.STAGE_NAME,
          "stage_index": 1, "stage_total": 1})
    started = time.time()
    try:
        _root, version_dir, _output = pipeline_manager._resolve_job_paths(job)  # noqa: SLF001
    except Exception as exc:                            # noqa: BLE001
        message = f"каталог версии не резолвится: {type(exc).__name__}: {exc}"
        emit({"type": "failed", "message": message})
        write_pipeline_log(spec, {stage_mod.STAGE_NAME: {"status": "error",
                                                         "error": message}})
        write_process_exit(spec, 1, error=message)
        return 1
    if version_dir is None:
        message = "каталог версии не определён — синтетическая проверка невозможна"
        emit({"type": "failed", "message": message})
        write_pipeline_log(spec, {stage_mod.STAGE_NAME: {"status": "error",
                                                         "error": message}})
        write_process_exit(spec, 1, error=message)
        return 1

    job_dir = Path(paths.get("project") or ".").parent.resolve()
    try:
        artifact = asyncio.run(
            stage_mod.run_stage(
                job_dir=job_dir,
                version_dir=Path(version_dir),
                result_dir=Path(paths.get("result") or "."),
                project_id=project_id,
                document_code=Path(project_id).name or None,
                job_id=str(spec.get("job_id") or ""),
                attempt_id=str(spec.get("attempt_id") or ""),
            )
        )
    except Exception as exc:                            # noqa: BLE001
        message = f"{type(exc).__name__}: {exc}"
        emit({"type": "failed", "message": message})
        traceback.print_exc(file=sys.stderr)
        write_pipeline_log(spec, {stage_mod.STAGE_NAME: {"status": "error",
                                                         "error": message}})
        write_process_exit(spec, 1, error=message)
        return 1

    ok, problems = stage_mod.artifact_is_successful(artifact)
    duration = round(time.time() - started, 2)
    emit({
        "type": "stage_completed",
        "stage": stage_mod.STAGE_NAME,
        "status": "done" if ok else "error",
        "duration_sec": duration,
        # В событие уходят только признаки и числа: ни промпта, ни ответа.
        "validation_passed": bool((artifact.get("validation") or {}).get("passed")),
        "inference_performed": bool(artifact.get("performed")),
    })
    write_pipeline_log(spec, {
        stage_mod.STAGE_NAME: {
            "status": "done" if ok else "error",
            "duration_sec": duration,
            "error": problems or None,
        }
    })
    usage_entries = []
    provider_result = artifact.get("provider_result") or {}
    usage = provider_result.get("usage") or {}
    if usage or provider_result.get("model"):
        usage_entries.append({
            "stage": stage_mod.STAGE_NAME,
            "model": provider_result.get("model") or "",
            "input_tokens": int(usage.get("input_tokens", 0) or 0),
            "output_tokens": int(usage.get("output_tokens", 0) or 0),
            "cache_creation_tokens": int(usage.get("cache_creation_input_tokens", 0) or 0),
            "cache_read_tokens": int(usage.get("cache_read_input_tokens", 0) or 0),
            "cost_usd": 0.0,
            "cost_usd_notional": float(usage.get("total_cost_usd", 0.0) or 0.0),
            "calls": 1 if artifact.get("performed") else 0,
            "duration_ms": int(provider_result.get("duration_ms", 0) or 0),
            "source": "worker_provider_bridge",
        })
    write_usage_report(spec, usage_entries)
    write_result_manifest(spec, {
        "worker_stage_plan": [stage_mod.STAGE_NAME],
        "completed_stages": [stage_mod.STAGE_NAME] if ok else [],
        "forbidden_stages_not_run": list(FORBIDDEN_STAGES),
        "applied_runtime_config": spec.get("_applied_runtime_config") or {},
        "applied_discipline_profile": spec.get("_applied_discipline_profile") or {},
        "discipline_id": spec.get("discipline_id"),
        "discipline_profile_hash": spec.get("discipline_profile_hash"),
        "job_id": spec.get("job_id"),
        "attempt_id": spec.get("attempt_id"),
        "project_id": project_id,
        "version_id": spec.get("version_id"),
        "profile": spec.get("profile"),
        "action": spec.get("action"),
        "pipeline_revision": spec.get("pipeline_revision"),
        "provider_mode": spec.get("provider_mode"),
        "provider_bridge": (
            spec["_provider_binding"].as_public_dict()
            if spec.get("_provider_binding") is not None else None
        ),
        "status": "completed" if ok else "failed",
        "error": problems or None,
        "stage_completion": {stage_mod.STAGE_NAME: "done" if ok else "error"},
        "resume_hint": None,
        "central_only_stages": list(FORBIDDEN_STAGES),
        "finished_at": time.time(),
    })
    write_process_exit(spec, 0 if ok else 1, error=problems or None)
    return 0 if ok else 1


def run(spec: dict[str, Any]) -> int:
    """Выполнить конвейер существующим кодом платформы."""
    import asyncio

    from backend.app.models.audit import AuditJob, BatchQueueItem, JobStatus
    from backend.app.pipeline.manager import pipeline_manager

    if str(spec.get("action") or "") == "provider_selfcheck":
        return run_provider_selfcheck(spec)

    project_id = str(spec.get("project_id") or "")
    version_id = spec.get("version_id")
    job = AuditJob(
        job_id=str(spec.get("job_id") or "remote"),
        project_id=project_id,
        version_id=version_id,
    )
    item = BatchQueueItem(
        project_id=project_id,
        version_id=version_id,
        action=str(spec.get("action") or "full"),
        retry_stage=spec.get("retry_stage"),
        job_id=job.job_id,
    )

    # Снимок статусов ДО прогона: журнал накопительный и приезжает в пакете.
    stages_before = snapshot_stage_statuses(spec)
    emit({"type": "stage_started", "stage": "pipeline", "stage_total": 1})
    started = time.time()
    try:
        asyncio.run(
            pipeline_manager._dispatch_action(         # noqa: SLF001 — тот же конвейер
                item, job, default_action=item.action,
            )
        )
    except Exception as exc:                            # noqa: BLE001
        emit({"type": "failed", "message": f"{type(exc).__name__}: {exc}"})
        traceback.print_exc(file=sys.stderr)
        # Маркер пишется и на провале: «процесс дошёл до конца сам и упал» —
        # это не то же самое, что «процесс убит рестартом», и воркер должен
        # видеть разницу.
        write_process_exit(spec, 1, error=f"{type(exc).__name__}: {exc}")
        return 1

    ok = job.status == JobStatus.COMPLETED
    emit(
        {
            "type": "stage_completed",
            "stage": "pipeline",
            "status": "done" if ok else "error",
            "duration_sec": round(time.time() - started, 2),
        }
    )
    stages, resume_hint = publish_deliverables(spec, job)
    history = audit_stage_history(spec, before=stages_before)
    if history["violations"]:
        # Центральный этап ВЫПОЛНИЛСЯ на воркере. Пакет собирать нельзя: он
        # прошёл бы транспорт как успешный, а на центре его отверг бы импортёр
        # по артефакту — то есть многочасовой прогон выбрасывался бы целиком,
        # и причина была бы видна только там.
        message = "На воркере выполнились центральные этапы: " + ", ".join(
            history["violations"]
        )
        emit({"type": "failed", "message": message})
        sys.stderr.write(message + "\n")
        write_process_exit(spec, 1, error=message)
        return 1
    write_result_manifest(
        spec,
        {
            "worker_stage_plan": history["worker_stage_plan"],
            "completed_stages": history["completed_stages"],
            "forbidden_stages_not_run": history["forbidden_stages_not_run"],
            "applied_runtime_config": spec.get("_applied_runtime_config") or {},
            "applied_discipline_profile": spec.get("_applied_discipline_profile") or {},
            "discipline_id": spec.get("discipline_id"),
            "discipline_profile_hash": spec.get("discipline_profile_hash"),
            "job_id": spec.get("job_id"),
            "attempt_id": spec.get("attempt_id"),
            "project_id": project_id,
            "version_id": version_id,
            "profile": spec.get("profile"),
            "action": spec.get("action"),
            "pipeline_revision": spec.get("pipeline_revision"),
            "provider_mode": spec.get("provider_mode"),
            "provider_bridge": (
                spec["_provider_binding"].as_public_dict()
                if spec.get("_provider_binding") is not None else None
            ),
            "status": getattr(job.status, "value", str(job.status)),
            "error": job.error_message,
            "stage_completion": stages or {"pipeline": "done" if ok else "error"},
            "resume_hint": resume_hint,
            "central_only_stages": list(FORBIDDEN_STAGES),
            "finished_at": time.time(),
        },
    )
    write_usage_report(spec, collect_usage(project_id))
    write_process_exit(spec, 0 if ok else 1, error=job.error_message)
    return 0 if ok else 1


#: Этапы, которые удалённый профиль ОБЯЗАН уметь выполнять. Список — контракт
#: границы, а не пожелание: он же уезжает в манифест результата, и центр по
#: нему видит, докуда дошла удалённая нога.
WORKER_STAGE_PLAN: tuple[str, ...] = (
    "crop_blocks", "block_context", "block_analysis", "text_analysis",
    "findings_merge", "findings_review", "optimization", "optimization_review",
)

#: Статусы, при которых этап считается НЕ выполнявшимся. `deferred` — штатный
#: маркер «отложено на центр», который ставит процессный гейт.
_NOT_RUN_STATUSES = frozenset({"", "deferred", "skipped", "pending", "blocked"})


def _stage_statuses(log_path: Path) -> dict[str, str]:
    """Статусы этапов из журнала. Отсутствие журнала — пустая карта."""
    if not log_path.is_file():
        return {}
    try:
        data = json.loads(log_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    stages = data.get("stages")
    if not isinstance(stages, dict):
        return {}
    return {
        str(name): str((entry or {}).get("status") or "").strip().lower()
        for name, entry in stages.items()
        if isinstance(entry, dict)
    }


def snapshot_stage_statuses(spec: dict[str, Any]) -> dict[str, str]:
    """Снять статусы этапов ДО прогона.

    `pipeline_log.json` НАКОПИТЕЛЬНЫЙ и приезжает на воркер внутри пакета: у
    версии, которую центр уже аудировал, там лежит `norm_verify: done` с
    прошлого раза. Без этого снимка проверка §14 обвиняла бы безупречный
    многочасовой прогон в том, что сделал центр месяцем раньше, — и делала бы
    это на последнем шаге, уже после всей работы.

    Действие `resume` входит в профиль, то есть случай не гипотетический.
    """
    paths = spec.get("paths") or {}
    project_root = Path(paths.get("project") or "")
    before: dict[str, str] = {}
    try:
        from audit_worker import package_io                # noqa: PLC0415

        version_dir = package_io.portable_version_dir(project_root)
    except Exception:                                      # noqa: BLE001
        return before
    for candidate in (
        version_dir / "03_analysis" / "latest" / "pipeline_log.json",
        version_dir / "99_service" / "pipeline_log.json",
    ):
        before.update(_stage_statuses(candidate))
    return before


def audit_stage_history(
    spec: dict[str, Any], *, before: Optional[dict[str, str]] = None
) -> dict[str, Any]:
    """Проверить ФАКТИЧЕСКУЮ историю этапов после прогона.

    Третий рубеж границы, и единственный, который смотрит на РЕЗУЛЬТАТ, а не на
    намерение. Первые два (валидатор `retry_stage` и процессный гейт) проверяют,
    что этап не будет запущен; этот проверяет, что он не был запущен. Разница
    существенна: гейт стоит в четырёх местах менеджера, и пятое место, которое
    однажды появится, этой проверкой будет поймано, а теми двумя — нет.

    Сравнивается ДЕЛЬТА к состоянию до прогона: унаследованный из пакета
    `norm_verify: done` центрального происхождения нарушением не является.
    """
    paths = spec.get("paths") or {}
    after = _stage_statuses(Path(paths.get("work") or ".") / "pipeline_log.json")
    prior = dict(before or {})

    violated: set[str] = set()
    violations: list[str] = []
    for name in FORBIDDEN_STAGES:
        status = after.get(name, "")
        if not status or status in _NOT_RUN_STATUSES:
            continue
        if prior.get(name, "") == status:
            # Тот же статус, что был до прогона: этап выполнил ЦЕНТР, и запись
            # просто приехала в пакете вместе с деревом версии.
            continue
        violated.add(name)
        violations.append(f"{name}={status}")

    completed = sorted(
        name for name, status in after.items()
        if status and status not in _NOT_RUN_STATUSES and prior.get(name, "") != status
    )
    return {
        "completed_stages": completed,
        # Членство по МНОЖЕСТВУ, а не поиск подстроки в склеенной строке:
        # строка давала верный ответ лишь по случайности текущих четырёх имён.
        "forbidden_stages_not_run": [
            name for name in FORBIDDEN_STAGES if name not in violated
        ],
        "violations": violations,
        "worker_stage_plan": list(WORKER_STAGE_PLAN),
        "inherited_stage_statuses": prior,
    }


def write_process_exit(spec: dict[str, Any], code: int, *, error: Any = None) -> Path:
    """Последнее действие процесса: маркер «я дошёл до конца сам».

    Второй источник для `read_completed_marker` на воркере. Без него
    перезапущенный исполнитель объявлял ЗАВЕРШЁННЫЙ многочасовой аудит
    прерванным: `completed.marker` пишет наблюдатель, а он рестартом и умер.
    Файл кладётся в `work/`, потому что именно там его ищет
    `process_control.classify_after_restart`.
    """
    paths = spec.get("paths") or {}
    target = Path(paths.get("work") or ".") / "process_exit.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "exit_code": int(code),
        "job_id": spec.get("job_id"),
        "attempt_id": spec.get("attempt_id"),
        "finished_at": time.time(),
    }
    if error:
        payload["error"] = str(error)[:500]
    tmp = target.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, target)
    return target


def publish_deliverables(spec: dict[str, Any], job: Any) -> tuple[dict[str, Any], Optional[str]]:
    """Скопировать обязательные артефакты в `result/` и `work/`.

    Пакет результата собирает воркер, и он обязан находить артефакты по
    фиксированным путям — а конвейер пишет их туда, куда велит раскладка
    версии (она неоднородна). Здесь и происходит перевод одного в другое.
    Копия, а не перенос: исходное дерево проекта остаётся целым.
    """
    import shutil

    from backend.app.pipeline.manager import pipeline_manager

    paths = spec.get("paths") or {}
    result_dir = Path(paths.get("result") or ".")
    work_dir = Path(paths.get("work") or ".")
    result_dir.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)

    try:
        _root, version_dir, output_dir = pipeline_manager._resolve_job_paths(job)  # noqa: SLF001
    except Exception:                              # noqa: BLE001 — диагностика ниже
        return {}, None
    output_dir = Path(output_dir)

    # Раскладка неоднородна, и это не теория: на projects_v2 артефакты этапов
    # ложатся в `03_analysis/runs/<run_id>`, а `pipeline_log.json` пишет
    # `audit_logger` — и он кладёт его в `03_analysis/latest`. Поиск только по
    # per-run каталогу давал УСПЕШНЫЙ прогон, который исполнитель объявлял
    # провалившимся по «нет обязательных артефактов: work/pipeline_log.json».
    # Порядок кандидатов значим: per-run каталог свежее и приоритетнее.
    candidates = [output_dir]
    if version_dir is not None:
        latest = Path(version_dir) / "03_analysis" / "latest"
        if latest.resolve() != output_dir.resolve():
            candidates.append(latest)

    def _find(name: str) -> Optional[Path]:
        for directory in candidates:
            candidate = directory / name
            if candidate.is_file():
                return candidate
        return None

    for name in (
        "03_findings.json", "03_findings_review.json",
        "optimization.json", "optimization_review.json",
        "01_blocks_analysis.json", "02_text_analysis.json",
    ):
        source = _find(name)
        if source is not None:
            shutil.copy2(source, result_dir / name)

    log_path = _find("pipeline_log.json")
    stages: dict[str, Any] = {}
    if log_path is not None:
        shutil.copy2(log_path, work_dir / "pipeline_log.json")
        try:
            data = json.loads(log_path.read_text(encoding="utf-8"))
            stages = {
                key: (value or {}).get("status")
                for key, value in (data.get("stages") or {}).items()
            }
        except (OSError, ValueError):
            stages = {}

    resume_hint = None
    try:
        from backend.app.pipeline.resume_detector import detect_resume_stage

        # Сигнатура — (project_id, *, version_id): путь сюда передавать нельзя,
        # детектор сам резолвит каталог версии. Раньше здесь уходил путь, из-за
        # чего подсказка ВСЕГДА была None (исключение глушилось ниже).
        info = detect_resume_stage(job.project_id, version_id=job.version_id)
        resume_hint = info.get("stage") if isinstance(info, dict) else None
    except Exception:                              # noqa: BLE001 — подсказка не блокер
        resume_hint = None
    return stages, resume_hint


def main(argv: Optional[list[str]] = None) -> int:
    args = list(argv if argv is not None else sys.argv[1:])
    if len(args) != 1:
        sys.stderr.write(
            "Использование: python -m backend.app.pipeline.remote_audit_runner "
            "<run_spec.json>\n"
        )
        return 2
    # Порядок обязателен: и запрет `.env`, и запрет центральных этапов читаются
    # на импорте конфигурации, поэтому выставляются до первого обращения к
    # `backend.app.core.config` (его тянет verify_snapshot).
    harden_process_env()
    spec = load_spec(Path(args[0]))
    apply_runtime_paths(spec)
    # Снимок применяется ДО провайдеров и моделей: он задаёт режим записи
    # хранилища, а значит и то, куда лягут артефакты всех последующих шагов.
    applied_runtime = apply_runtime_snapshot(spec)
    spec["_applied_runtime_config"] = applied_runtime
    # Дисциплина берётся из СНИМКА, а не из спеки: спеку пишет воркер, снимок
    # подписан хэшем центра. Расхождение — отказ, а не «доверимся воркеру».
    snapshot_discipline = str(applied_runtime.get("discipline_id") or "")
    if snapshot_discipline and str(spec.get("discipline_id") or "") != snapshot_discipline:
        raise SystemExit(
            f"discipline_id задания {spec.get('discipline_id')!r} не совпадает "
            f"со снимком центра {snapshot_discipline!r}"
        )
    snapshot_profile_hash = str(applied_runtime.get("discipline_profile_hash") or "")
    if snapshot_profile_hash and str(
        spec.get("discipline_profile_hash") or ""
    ) != snapshot_profile_hash:
        raise SystemExit(
            "discipline_profile_hash задания не совпадает со снимком центра"
        )
    applied_profile = apply_discipline_profile(spec)
    spec["_applied_discipline_profile"] = applied_profile
    providers = enforce_fake_providers(spec)
    provider_bridge = bind_providers(spec)
    models_path = apply_model_snapshot(spec)
    snapshot = verify_snapshot(spec)
    emit(
        {
            "type": "stage_started",
            "stage": "verify_snapshot",
            "snapshot": snapshot,
            "providers": providers,
            "provider_bridge": provider_bridge,
            "model_config_applied": bool(models_path),
            "runtime_config": applied_runtime,
            "discipline_profile": applied_profile,
        }
    )
    return run(spec)


if __name__ == "__main__":
    raise SystemExit(main())
