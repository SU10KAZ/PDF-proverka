"""Переносимость путей ВНУТРИ артефактов, возвращаемых воркером.

Отчёт 08 (§24 п. 2) закрыл имена записей в архиве, но не их СОДЕРЖИМОЕ:
`pipeline_log.artifacts_dir`, `block_context_summary.project_dir`,
`stage01_meta.runtime_plan_path` несут абсолютные пути каталога попытки на
чужой машине. Импорт таких артефактов в проект центра означает, что в дереве
заказчика навсегда остаётся `/var/lib/audit-worker/jobs/<uuid>/<uuid>/…` —
путь, которого на центре нет и который следующий читатель примет за настоящий.

**Почему не глобальная замена подстроки.** Артефакт аудита — это текст
замечаний, цитаты норм и числа. Слепой `str.replace` по JSON правит и их: одна
подстрока в описании замечания меняет смысл вывода эксперта, и заметить это
нельзя ничем. Поэтому нормализация здесь **schema-aware**: каждое поле,
несущее путь, названо поимённо и имеет ЯВНУЮ стратегию, а любое НЕ названное
поле с абсолютным путём отвергает пакет целиком (CH-21).

Три стратегии, и выбор между ними — содержательное решение, а не вкус:

  `RELATIVIZE`  путь указывает внутрь каталога версии и на центре имеет смысл
                → остаётся относительным путём от корня версии;
  `DROP`        путь описывает РАНТАЙМ воркера (план запуска, временный
                каталог, HOME) → на центре смысла не имеет вовсе, поле
                обнуляется;
  `REDACT`      путь нужен для разбора инцидента, но исполняемой ссылкой
                становиться не должен → заменяется меткой.

Отвергать неизвестное — сторона ошибки, выбранная сознательно: пропущенное
поле с чужим путём тихо живёт в проекте годами, а отвергнутый пакет виден в
первую же минуту и добавляется в реестр одной строкой.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Optional


class PathStrategy(str, Enum):
    RELATIVIZE = "relativize"
    DROP = "drop"
    REDACT = "redact"


#: Метка вместо пути. Строка не является путём ни на одной системе — то есть
#: её нельзя «случайно открыть».
REDACTED = "<worker-path>"

#: Поля, чьё значение — путь ВНУТРИ каталога версии. На центре они осмысленны,
#: но только относительными: абсолютный путь воркера здесь не резолвится.
RELATIVIZE_KEYS: frozenset[str] = frozenset(
    {
        "artifacts_dir",
        "output_dir",
        "project_dir",
        "version_dir",
        "run_dir",
        "blocks_dir",
        "crop_path",
        "crop_file",
        "image_path",
        "md_path",
        "pdf_path",
        "source_pdf",
        "source_md",
        "input_path",
        "result_json_path",
        "document_graph_path",
        "path",
        "file_path",
        "filepath",
        "file",
        "src",
        "dst",
        "output_path",
        "target_path",
    }
)

#: Поля, описывающие РАНТАЙМ воркера. На центре не значат ничего.
DROP_KEYS: frozenset[str] = frozenset(
    {
        "runtime_plan_path",
        "worker_root",
        "job_dir",
        "attempt_dir",
        "tmp_dir",
        "tmpdir",
        "temp_dir",
        "home",
        "home_dir",
        "cwd",
        "workdir",
        "work_dir",
        "prompts_dir",
        "data_dir",
        "app_data_dir",
        "cache_dir",
        "comparison_root",
        "comparison_path",
        "provider_dir",
        "fake_provider_dir",
        "binary",
        "executable",
        "cli_path",
        "spec_path",
        "root_dir",
        "base_dir",
        "pipeline_root",
    }
)

#: Поля диагностики: путь остаётся ВИДИМЫМ, но не остаётся ссылкой.
REDACT_KEYS: frozenset[str] = frozenset(
    {
        "log_path",
        "stdout_path",
        "stderr_path",
        "log_file",
        "trace_path",
        "evidence_path",
    }
)

#: Единая карта. Пересечения запрещены: одно поле — одна стратегия.
PATH_FIELD_RULES: dict[str, PathStrategy] = {
    **{key: PathStrategy.RELATIVIZE for key in RELATIVIZE_KEYS},
    **{key: PathStrategy.DROP for key in DROP_KEYS},
    **{key: PathStrategy.REDACT for key in REDACT_KEYS},
}

#: Расширения, содержимое которых нормализуется. Всё остальное (PNG, PDF)
#: путей в себе не несёт и трогать его нельзя.
NORMALIZED_SUFFIXES: tuple[str, ...] = (".json", ".jsonl")

#: Что считается ЗНАЧЕНИЕМ-ПУТЁМ. Не «строка со слэшем»: описание замечания
#: тоже содержит слэши. Требуется, чтобы строка ЦЕЛИКОМ была абсолютным путём —
#: без переводов строки, без табуляций и без кавычек.
#: Пробел и двоеточие исключены намеренно: без них под шаблон попадала
#: диагностика вида «/bin/bash: line 1: rg: command not found», а такая строка
#: в поле неизвестной схемы означала отказ ВСЕГО пакета результата.
_ABSOLUTE_PATH = re.compile(r"^/(?:[^/\0\s:\"']+/?)*$")


def looks_like_absolute_path(value: Any) -> bool:
    """Является ли значение абсолютным путём ЦЕЛИКОМ."""
    if not isinstance(value, str):
        return False
    text = value.strip()
    if len(text) < 2 or not text.startswith("/"):
        return False
    if any(ch in text for ch in "\n\r\t"):
        return False
    return bool(_ABSOLUTE_PATH.match(text))


class PortablePathError(RuntimeError):
    """Артефакт содержит путь, который нельзя ни переписать, ни объяснить."""


@dataclass
class NormalizationReport:
    """Что сделано и что не удалось. Оба списка одинаково важны."""

    rewritten: list[dict[str, Any]] = field(default_factory=list)
    dropped: list[dict[str, Any]] = field(default_factory=list)
    redacted: list[dict[str, Any]] = field(default_factory=list)
    violations: list[dict[str, Any]] = field(default_factory=list)
    files_touched: list[str] = field(default_factory=list)
    files_scanned: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "files_scanned": self.files_scanned,
            "files_touched": sorted(self.files_touched),
            "rewritten": self.rewritten[:500],
            "dropped": self.dropped[:500],
            "redacted": self.redacted[:500],
            "violations": self.violations[:200],
            "rewritten_count": len(self.rewritten),
            "dropped_count": len(self.dropped),
            "redacted_count": len(self.redacted),
            "violation_count": len(self.violations),
        }


def _relativize(value: str, *, version_rel_marker: str) -> Optional[str]:
    """Сделать путь относительным каталога версии, если он в неё указывает.

    Ориентир — сегмент `versions/<vid>` в самом пути: каталог версии на воркере
    лежит в переносимом дереве, то есть его ХВОСТ совпадает с хвостом на центре
    побайтово. Это надёжнее любой попытки угадать корень каталога попытки,
    которого центр не знает вовсе.

    Путь, указывающий НА САМ каталог версии, даёт `"."`: `block_context_summary`
    пишет туда именно его, и «не нашли разделитель» было бы неверным ответом.
    """
    text = value.strip().rstrip("/")
    parts = version_rel_marker.strip("/").split("/")
    if len(parts) == 2:
        pattern = re.compile(re.escape(f"/{parts[0]}/{parts[1]}") + r"(?=/|$)")
    else:
        pattern = re.compile(r"/" + re.escape(parts[0]) + r"/[^/]+(?=/|$)")
    match = None
    for match in pattern.finditer(text):
        pass                                   # нужен ПОСЛЕДНИЙ: вложенность возможна
    if match is None:
        return None
    tail = text[match.end():].strip("/")
    if not tail:
        return "."
    if ".." in tail.split("/"):
        return None
    return tail


def _walk(
    node: Any,
    *,
    version_rel_marker: str,
    where: str,
    report: NormalizationReport,
    rel_file: str,
    key_name: str = "",
) -> tuple[Any, bool]:
    """Обойти структуру, применяя стратегию по ИМЕНИ поля.

    Возвращает `(значение, изменилось ли)`. Имя поля берётся у ближайшего
    словаря: путь внутри списка наследует имя списка (`"sources": [...]`),
    иначе элементы списков были бы «полем без имени» и отвергали бы пакет.
    """
    if isinstance(node, dict):
        out: dict[str, Any] = {}
        changed = False
        for key, value in node.items():
            child, child_changed = _walk(
                value,
                version_rel_marker=version_rel_marker,
                where=f"{where}.{key}" if where else str(key),
                report=report,
                rel_file=rel_file,
                key_name=str(key),
            )
            out[key] = child
            changed = changed or child_changed
        return out, changed
    if isinstance(node, list):
        out_list: list[Any] = []
        changed = False
        for index, item in enumerate(node):
            child, child_changed = _walk(
                item,
                version_rel_marker=version_rel_marker,
                where=f"{where}[{index}]",
                report=report,
                rel_file=rel_file,
                key_name=key_name,
            )
            out_list.append(child)
            changed = changed or child_changed
        return out_list, changed
    if not looks_like_absolute_path(node):
        return node, False

    strategy = PATH_FIELD_RULES.get(key_name)
    entry = {"file": rel_file, "field": where, "key": key_name, "value": node}
    if strategy is None:
        report.violations.append(
            {**entry, "reason": "поле с абсолютным путём не описано контрактом"}
        )
        return node, False
    if strategy is PathStrategy.DROP:
        report.dropped.append(entry)
        return None, True
    if strategy is PathStrategy.REDACT:
        report.redacted.append(entry)
        return REDACTED, True
    relative = _relativize(str(node), version_rel_marker=version_rel_marker)
    if relative is None:
        # Путь помечен как «внутри версии», но в версию не указывает. Молча
        # обнулить нельзя: это либо новое поле, либо чужой каталог.
        report.violations.append(
            {**entry, "reason": "путь не указывает внутрь каталога версии"}
        )
        return node, False
    report.rewritten.append({**entry, "relative": relative})
    return relative, True


def normalize_staged_tree(
    staged_project: Path,
    *,
    version_rel_marker: str = "versions",
    version_id: Optional[str] = None,
) -> NormalizationReport:
    """Нормализовать пути внутри артефактов, лежащих в staging импорта.

    Работает ТОЛЬКО в staging — по дереву проекта заказчика ничего не пишется
    до тех пор, пока весь план не признан допустимым.
    """
    staged_project = Path(staged_project)
    marker = f"{version_rel_marker}/{version_id}" if version_id else version_rel_marker
    report = NormalizationReport()
    if not staged_project.is_dir():
        return report
    for path in sorted(staged_project.rglob("*")):
        if not path.is_file() or path.is_symlink():
            continue
        if path.suffix.lower() not in NORMALIZED_SUFFIXES:
            continue
        rel_file = path.relative_to(staged_project).as_posix()
        report.files_scanned += 1
        if path.suffix.lower() == ".jsonl":
            _normalize_jsonl(path, rel_file, marker, report)
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            # Нечитаемый артефакт — не повод падать здесь: его отвергнет
            # проверка обязательных артефактов, и с внятным сообщением.
            continue
        result, changed = _walk(
            data,
            version_rel_marker=marker,
            where="",
            report=report,
            rel_file=rel_file,
        )
        if changed:
            path.write_text(
                json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            report.files_touched.append(rel_file)
    return report


def _normalize_jsonl(
    path: Path, rel_file: str, marker: str, report: NormalizationReport
) -> None:
    lines_out: list[str] = []
    changed_any = False
    try:
        raw_lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return
    for index, line in enumerate(raw_lines):
        stripped = line.strip()
        if not stripped:
            lines_out.append(line)
            continue
        try:
            payload = json.loads(stripped)
        except ValueError:
            lines_out.append(line)
            continue
        result, changed = _walk(
            payload,
            version_rel_marker=marker,
            where=f"[{index}]",
            report=report,
            rel_file=rel_file,
        )
        lines_out.append(json.dumps(result, ensure_ascii=False))
        changed_any = changed_any or changed
    if changed_any:
        path.write_text("\n".join(lines_out) + "\n", encoding="utf-8")
        report.files_touched.append(rel_file)


def residual_absolute_paths(staged_project: Path) -> list[dict[str, Any]]:
    """Повторный проход ПОСЛЕ нормализации: не осталось ли абсолютных путей.

    Первый проход доказывает «мы обработали то, что нашли», второй — «не
    осталось ничего». Разные утверждения: первое верно и тогда, когда обход
    пропустил ветку структуры.
    """
    found: list[dict[str, Any]] = []
    staged_project = Path(staged_project)
    for path in sorted(staged_project.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in NORMALIZED_SUFFIXES:
            continue
        rel_file = path.relative_to(staged_project).as_posix()
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        for payload in _iter_json_documents(text, path.suffix.lower()):
            for where, value in _iter_strings(payload, ""):
                if looks_like_absolute_path(value):
                    found.append({"file": rel_file, "field": where, "value": value})
    return found


def _iter_json_documents(text: str, suffix: str):
    if suffix == ".jsonl":
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except ValueError:
                continue
        return
    try:
        yield json.loads(text)
    except ValueError:
        return


def _iter_strings(node: Any, where: str):
    if isinstance(node, dict):
        for key, value in node.items():
            yield from _iter_strings(value, f"{where}.{key}" if where else str(key))
        return
    if isinstance(node, list):
        for index, item in enumerate(node):
            yield from _iter_strings(item, f"{where}[{index}]")
        return
    if isinstance(node, str):
        yield where, node


def relative_paths_are_safe(staged_project: Path) -> list[str]:
    """Проверить, что переписанные значения не содержат обхода каталога."""
    bad: list[str] = []
    for path in sorted(Path(staged_project).rglob("*")):
        if not path.is_file() or path.suffix.lower() not in NORMALIZED_SUFFIXES:
            continue
        rel_file = path.relative_to(staged_project).as_posix()
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        for payload in _iter_json_documents(text, path.suffix.lower()):
            for where, value in _iter_strings(payload, ""):
                if _is_relative_path_value(value) and ".." in value.split("/"):
                    bad.append(f"{rel_file}:{where}={value}")
    return bad


def _is_relative_path_value(value: str) -> bool:
    """Похоже ли значение на ОТНОСИТЕЛЬНЫЙ путь, а не на прозу.

    Текст замечания тоже содержит `/` и точки («см. п. 1.2/3»), поэтому одного
    слэша мало: требуется отсутствие пробелов и хотя бы два сегмента. Проверка
    узкая намеренно — она ловит результат нашей же нормализации, а не
    произвольную строку.
    """
    text = str(value or "")
    if not text or text.startswith("/") or " " in text or "\n" in text:
        return False
    parts = text.split("/")
    return len(parts) >= 2 and all(parts)
