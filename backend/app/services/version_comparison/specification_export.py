"""Bind whole-specification results to a run and select the sole MD source.

Reads cached metadata only. Does not profile/re-extract a PDF or call a model.
The legacy run spec.json remains untouched.
"""
from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path

from backend.app.services.common.atomic_json import atomic_write_json
from . import paths, profile, spec
from .specification_compare import change_entries

SCHEMA = "version_comparison.whole_specification/v1"
FILENAME = "whole_specification.json"
SERIES_SCHEMA = "whole-specification-series/1"


def _read(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Ожидается JSON-объект: {path.name}")
    return value


def _identity(run: dict) -> dict:
    return {key: run.get(key) for key in ("object_id", "run_id", "project_id")} | {
        side: {key: run[side].get(key) for key in ("version_id", "sha256", "file_name")}
        for side in ("a", "b")}


def _pages(side: dict) -> set[int]:
    return {p for p in (side.get("selection") or {}).get("pages", []) if type(p) is int and p > 0}


@lru_cache(maxsize=4096)
def _drawing_stamp(root: str, object_id: str, sha: str, page: int, title: str) -> bool:
    """Страница «spec» по геометрии — чертёж по полному штампу (слова версии неизменны для sha)."""
    rec = next((p for p in (profile.load(object_id, sha) or {}).get("pages", []) if p.get("page") == page), {})
    try:
        words = profile.page_words(object_id, sha, page)
    except (OSError, ValueError):
        words = None
    return spec.drawing_stamp(rec, words)


def _drawing_page(object_id: str, sha: str, page: int, title: str) -> bool:
    return _drawing_stamp(str(paths.root()), object_id, sha, page, title)


@lru_cache(maxsize=2048)
def _scope(root: str, object_id: str, sha: str) -> dict | None:
    from . import specification_prepare
    return specification_prepare.version_scope(paths.object_dir(object_id), sha)


def _excluded_path(object_id: str) -> Path:
    return paths.object_dir(object_id) / "whole_specifications" / "excluded_versions.json"


def excluded(object_id: str) -> dict:
    """Версии (sha PDF), чьё извлечение СО не прошло строгую проверку полного сравнения (например,
    двухуровневая СО «Щит … в составе:» → подгруппа → позиции): их переходы описывает листовой ИИ."""
    path = _excluded_path(object_id)
    try:
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    except (OSError, ValueError):
        return {}


def exclude_versions(object_id: str, versions: dict[str, list[str]], when: str) -> None:
    from backend.app.services.common.atomic_json import load_modify_save
    load_modify_save(_excluded_path(object_id), lambda d: d | {
        sha: {"reasons": reasons, "at": when} for sha, reasons in versions.items()}, default={})


def text_spec(object_id: str, sha: str | None) -> bool:
    """Версия годится для полного сравнения СО (specification_prepare.version_scope): текстовая СО формы
    с девятью графами, вся область разобрана. Не годится (кривые, скан, другая форма, таблица на чертеже) —
    листы СО переходов с ней описывает листовой ИИ. PDF версии нет — неизвестно, прежнее поведение."""
    if not sha:
        return True
    if sha in excluded(object_id):
        return False
    scope = _scope(str(paths.root()), object_id, sha)
    return scope is None or scope["ok"]


def context(object_id: str, run_id: str) -> dict:
    """Cached page classification, independent of the sheet AI job's coverage."""
    if not run_id:
        return {"run": None, "kinds": {"a": {}, "b": {}}, "spec_pages": {"a": set(), "b": set()},
                "reports": [], "covered": {"a": set(), "b": set()}, "issue": None, "whole_spec": False}
    folder = paths.run_dir(object_id, run_id)
    run_path = folder / "run.json"
    run = _read(run_path) if run_path.exists() else None
    ctx = {"run": run, "kinds": {"a": {}, "b": {}}, "spec_pages": {"a": set(), "b": set()},
           "reports": [], "covered": {"a": set(), "b": set()}, "issue": None, "whole_spec": False}
    if run:
        # Полное сравнение СО — только если у обеих версий перехода СО с текстовым слоем;
        # иначе листы СО перехода — обычные листы для ИИ (ничего не остаётся неописанным).
        ctx["whole_spec"] = all(text_spec(object_id, (run.get(s) or {}).get("sha256")) for s in ("a", "b"))
        for side in ("a", "b"):
            sha = (run.get(side) or {}).get("sha256")
            cached = profile.load(object_id, sha) if sha else None
            for page in (cached or {}).get("pages", []):
                p = page.get("page")
                if type(p) is int and p > 0:
                    kind = page.get("kind")
                    # The geometry classifier also marks drawings with small
                    # embedded tables as spec. Their drawing stamp wins here.
                    if kind == "spec" and _drawing_page(object_id, sha, p, (page.get("stamp") or {}).get("title") or ""):
                        kind = "drawing"
                    ctx["kinds"][side][p] = kind
                    if kind == "spec" and ctx["whole_spec"]:
                        ctx["spec_pages"][side].add(p)
        for item in run.get("items", []) if ctx["whole_spec"] else []:
            if "spec" in item:
                for side in ("a", "b"):
                    p = item.get(f"{side}_page")
                    if p and ctx["kinds"][side].get(p) in (None, "spec"):
                        ctx["spec_pages"][side].add(p)
    artifact = folder / FILENAME
    if artifact.exists():
        try:
            bundle = _read(artifact)
            if not run or bundle.get("schema") != SCHEMA or bundle.get("binding") != _identity(run):
                raise ValueError("результат полной спецификации относится к другим версиям")
            reports = _validate_reports(bundle.get("reports"), run)
            ctx["reports"] = reports
            ctx["model"] = bundle.get("model")
            for item in reports:
                for side, name in (("a", "old"), ("b", "new")):
                    pages = _pages(item["comparison"][name])
                    ctx["covered"][side].update(pages)
                    # Explicit whole-spec scope is also a classifier when there
                    # is no cached page kind. A known drawing is never suppressed.
                    if ctx["whole_spec"]:
                        ctx["spec_pages"][side].update(p for p in pages if ctx["kinds"][side].get(p) in (None, "spec"))
        except (ValueError, KeyError, TypeError, OSError) as exc:
            ctx["issue"] = str(exc)
    for item in (run or {}).get("items", []):
        is_specification(ctx, item)
    return ctx


def _spec_title(title: str) -> bool:
    text = re.sub(r"\s+", " ", str(title or "")).strip().casefold()
    text = re.sub(r"^(?:со\s+)?л\.\s*\S+\s*[·:—-]\s*", "", text)
    # Do not treat 'План ... Спецификация ...' or an arbitrary summary mention
    # as a standalone specification sheet.
    return bool(re.match(r"^спецификаци[яи]\b", text)) and not re.search(r"\b(план|схема|разрез|узел)\b", text)


def is_specification(ctx: dict, sheet: dict, result: dict | None = None) -> bool:
    if not ctx.get("whole_spec", True):
        return False
    pages =[(side, sheet.get(f"{side}_page") or ((result or {}).get(side) or {}).get("page")) for side in ("a", "b")]
    present = [(side, p) for side, p in pages if p]
    kinds = [ctx["kinds"][side].get(p) for side, p in present]
    if any(kind and kind != "spec" for kind in kinds):
        return False
    known = present and all(p in ctx["spec_pages"][side] for side, p in present)
    single_kind = ((result or {}).get("single") or {}).get("sheet_kind")
    identified = bool(known or _spec_title(sheet.get("title")) or _spec_title(single_kind))
    if identified:
        for side, p in present:
            ctx["spec_pages"][side].add(p)
    return identified


def contains_specification(ctx: dict, sheet: dict) -> bool:
    """Input routing: even an incorrectly mixed drawing/spec pair must not send a spec page."""
    return is_specification(ctx, sheet) or any(
        sheet.get(f"{side}_page") in ctx["spec_pages"][side] and
        ctx["kinds"][side].get(sheet.get(f"{side}_page")) in (None, "spec")
        for side in ("a", "b"))


def _validate_reports(reports, run: dict) -> list[dict]:
    if not isinstance(reports, list) or not reports:
        raise ValueError("нет результатов полной спецификации")
    unique = []
    seen = set()
    claimed = {"old": set(), "new": set()}
    source_hashes = {"old": set(), "new": set()}
    for report in reports:
        if not isinstance(report, dict) or not isinstance(report.get("comparison"), dict):
            raise ValueError("некорректный результат спецификации")
        cmp = report["comparison"]
        if not cmp.get("complete") or not all((cmp.get(name) or {}).get("complete") for name in ("old", "new")):
            raise ValueError("полная спецификация ещё не разобрана с обеих сторон")
        for name, side in (("old", "a"), ("new", "b")):
            src = cmp[name]
            expected = run[side]
            if not src.get("pdf_sha256") or src["pdf_sha256"] != expected["sha256"]:
                raise ValueError(f"{name}: pdf_sha256 не совпадает с версией прогона")
            if src.get("document") != expected.get("file_name"):
                raise ValueError(f"{name}: имя исходного документа не совпадает с версией прогона")
            if not src.get("source_sha256") or not _pages(src):
                raise ValueError(f"{name}: нет отпечатка MD или полной области спецификации")
            if (src.get("selection") or {}).get("mode") not in {"pages", "all_blocks"}:
                raise ValueError("для общей сводки нужны целые страницы спецификации, а не выбор отдельных блоков")
            rows = [pair[name] for pair in cmp.get("pairs", [])]
            rows += cmp.get("removed" if name == "old" else "added", [])
            rows += [row for group in cmp.get("unresolved", []) for row in group.get(f"{name}_rows", [])]
            if any(ref.get("page") not in _pages(src) for row in rows for ref in row.get("sources", [])):
                raise ValueError(f"{name}: позиция ссылается на страницу вне области спецификации")
            source_hashes[name].add(src["source_sha256"])
            if len(source_hashes[name]) > 1:
                raise ValueError("для одной версии переданы разные исходные MD спецификации")
        key = json.dumps(cmp, ensure_ascii=False, sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        for name in ("old", "new"):
            # Overlapping excerpts of one side must not supply separate deltas.
            # Sharing a physical page is fine if two specification blocks differ.
            src = cmp[name]
            blocks = (src.get("selection") or {}).get("block_ids") or []
            if not blocks:
                raise ValueError(f"{name}: не указаны блоки спецификации")
            keys = {(src["source_sha256"], block) for block in blocks}
            if claimed[name] & keys:
                raise ValueError("области спецификаций пересекаются; возможен повтор изменений")
            claimed[name].update(keys)
        # Validate the renderer's required contract before publishing anything.
        change_entries(cmp)
        unique.append(report)
    return unique


def publish(object_id: str, run_id: str, report: dict) -> Path:
    """Attach a prepared result explicitly; no model call and no run mutation."""
    run = _read(paths.run_dir(object_id, run_id) / "run.json")
    if run.get("object_id") != object_id or run.get("run_id") != run_id:
        raise ValueError("идентификатор прогона не совпадает")
    bundle = _prepare_bundle(object_id, run_id, run, report)
    path = paths.run_dir(object_id, run_id) / FILENAME
    atomic_write_json(path, bundle)
    return path


def _prepare_bundle(object_id: str, run_id: str, run: dict, report: dict) -> dict:
    if run.get("object_id") != object_id or run.get("run_id") != run_id:
        raise ValueError("идентификатор прогона не совпадает")
    if report.get("schema") != "whole-specification/1":
        raise ValueError("ожидается отчёт whole-specification/1")
    reports = _validate_reports(report.get("reports"), run)
    ctx = context(object_id, run_id)
    for item in reports:
        for side, name in (("a", "old"), ("b", "new")):
            if any(ctx["kinds"][side].get(page) not in (None, "spec") for page in _pages(item["comparison"][name])):
                raise ValueError("выбранная область включает чертёж; отдельная спецификация должна состоять из листов СО")
    bundle = {"schema": SCHEMA, "binding": _identity(run), "reports": reports,
              "model": (report.get("settings") or {}).get("model")}
    return bundle


def publish_series(object_id: str, series_id: str, manifest: dict) -> list[Path]:
    """Bind every transition report to its exact run; validate all before writing any."""
    record = _read(paths.series_path(object_id, series_id))
    if record.get("object_id") != object_id or record.get("series_id") != series_id:
        raise ValueError("идентификатор серии не совпадает")
    if manifest.get("schema") != SERIES_SCHEMA:
        raise ValueError(f"ожидается манифест {SERIES_SCHEMA}")
    transitions = manifest.get("transitions")
    pairs = record.get("pairs") or []
    if not isinstance(transitions, list) or len(transitions) != len(pairs):
        raise ValueError("для публикации нужны результаты всех переходов серии")

    prepared: list[tuple[Path, dict]] = []
    seen_runs: set[str] = set()
    previous_b: dict | None = None
    previous_new_md: set[str] | None = None
    for index, (pair, entry) in enumerate(zip(pairs, transitions), 1):
        skipped = isinstance(entry, dict) and entry.get("skipped") == "no_text_spec"
        if skipped:
            # Переход без СО с текстовым слоем у одной из версий: его листы СО описывает ИИ по листам.
            if entry.get("run_id") != pair.get("run_id") or context(object_id, pair.get("run_id"))["whole_spec"]:
                raise ValueError(f"переход {index}: пропуск допустим только без текстовой СО")
            previous_b = (_read(paths.run_dir(object_id, pair["run_id"]) / "run.json").get("b") or {})
            previous_new_md = None
            continue
        if not isinstance(entry, dict) or not isinstance(entry.get("report"), dict):
            raise ValueError(f"переход {index}: отсутствует отчёт")
        run_id = pair.get("run_id")
        if not run_id or entry.get("run_id") != run_id or run_id in seen_runs:
            raise ValueError(f"переход {index}: run_id не совпадает с прогоном серии")
        seen_runs.add(run_id)
        run = _read(paths.run_dir(object_id, run_id) / "run.json")
        if run.get("project_id") != record.get("project_id"):
            raise ValueError(f"переход {index}: прогон относится к другому проекту")
        if run.get("state") != "done":
            raise ValueError(f"переход {index}: сравнение не завершено")
        for side, pair_key in (("a", "a"), ("b", "b")):
            if (run.get(side) or {}).get("version_id") != pair.get(pair_key):
                raise ValueError(f"переход {index}: версия {side.upper()} не совпадает с серией")
        current_a = run.get("a") or {}
        if previous_b and any(previous_b.get(key) != current_a.get(key) for key in ("version_id", "sha256")):
            raise ValueError(f"переход {index}: версия A разрывает цепочку серии")
        bundle = _prepare_bundle(object_id, run_id, run, entry["report"])
        new_md = {item["comparison"]["new"]["source_sha256"] for item in bundle["reports"]}
        old_md = {item["comparison"]["old"]["source_sha256"] for item in bundle["reports"]}
        if previous_new_md is not None and previous_new_md != old_md:
            raise ValueError(f"переход {index}: отпечаток MD общей версии не совпадает с предыдущим переходом")
        previous_b = run.get("b") or {}
        previous_new_md = new_md
        prepared.append((paths.run_dir(object_id, run_id) / FILENAME, bundle))
    # Input and run bindings have all passed before the first filesystem update.
    for path, bundle in prepared:
        atomic_write_json(path, bundle)
    return [path for path, _ in prepared]


def summary_status(object_id: str, series_id: str) -> dict[str, int | bool]:
    """Read-only readiness of whole-specification results for the stored series."""
    path = paths.series_path(object_id, series_id)
    try:
        record = _read(path)
        if record.get("object_id") != object_id or record.get("series_id") != series_id:
            return {"ready": False, "transitions": 0, "complete": 0}
    except (OSError, ValueError, TypeError):
        return {"ready": False, "transitions": 0, "complete": 0}

    pairs = record.get("pairs") or []
    complete = 0
    for pair in pairs:
        run_id = pair.get("run_id")
        if not run_id:
            continue
        try:
            run = _read(paths.run_dir(object_id, run_id) / "run.json")
            if (run.get("object_id") != object_id or run.get("run_id") != run_id
                    or run.get("project_id") != record.get("project_id") or run.get("state") != "done"
                    or (run.get("a") or {}).get("version_id") != pair.get("a")
                    or (run.get("b") or {}).get("version_id") != pair.get("b")):
                continue
            ctx = context(object_id, run_id)
            if ctx["reports"] and not coverage_issue(ctx):
                complete += 1
        except (OSError, ValueError, KeyError, TypeError):
            continue
    transitions = len(pairs)
    return {"ready": transitions > 0 and complete == transitions,
            "transitions": transitions, "complete": complete}


def coverage_issue(ctx: dict) -> str | None:
    if ctx["issue"]:
        return "Спецификация не описана: результат полного сравнения не подходит к выбранным версиям."
    if any(ctx["spec_pages"][side] - ctx["covered"][side] for side in ("a", "b")):
        return "Спецификация не описана полностью: отсутствует результат сравнения всех её листов."
    if any(item["comparison"].get("unresolved") for item in ctx["reports"]):
        return "Спецификация не описана полностью: остаются неоднозначные соответствия позиций."
    return None
