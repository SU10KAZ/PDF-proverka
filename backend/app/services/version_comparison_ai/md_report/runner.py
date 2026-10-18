"""Задание «Аналитический отчёт (PDF)»: приём MD → разбор → аналитик → проверяющий → составитель → PDF.

Хранилище — ``objects/<object_id>/md_reports/<report_id>/`` каталога «Сравнения версий»:
``source/`` (точная копия входа, только чтение), ``manifest.json`` и ``plan.json`` (структура и блоки),
``prompts/`` (действительные тексты правил и шаблонов), ``calls/<этап>/<единица>/attempt-N/``
(отправленное задание, сырой ответ, события CLI, метаданные), ``results/`` (проверенные результаты
этапов с ключом совместимости), ``report.json`` (report_v1), ``checks.json``, ``report.pdf``.

Исполнитель — тот же ``codex exec``, что у ИИ-описаний (``executor.CodexExecDescriber``): каждое
обращение — отдельная эфемерная сессия без общего состояния, поэтому независимые группы идут
параллельно (``settings.parallel()``). Контекст сессии не хранит промежуточных результатов: всё
нужное для продолжения лежит на диске. Повторный запуск берёт только результаты с тем же ключом
(хеш источника и блоков, версия промптов и схем, модель, усилие).
"""
from __future__ import annotations

import hashlib
import json
import os
import secrets
import shutil
import threading
import traceback
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from backend.app.services.common.atomic_json import atomic_write_json, load_modify_save
from backend.app.services.stage_comparison.ai import response_contract
from backend.app.services.version_comparison import paths

from .. import executor as ex
from .. import settings as ai_settings
from . import parse, pdf, plan as plan_mod, schemas, settings, stages

SCHEMA = "version_comparison_ai.md_report/v1"
BOOT_ID = secrets.token_hex(6)
ACTIVE = ("queued", "running", "cancelling")
RESUMABLE = ("interrupted", "cancelled", "paused_quota", "paused_auth", "incomplete", "failed")
STATE_LABELS = {
    "queued": "в очереди", "running": "идёт", "cancelling": "отменяется…", "done": "готово",
    "incomplete": "неполный результат", "failed": "ошибка", "cancelled": "отменено",
    "interrupted": "прервано — можно продолжить", "paused_quota": "пауза: исчерпана квота подписки Codex",
    "paused_auth": "пауза: Codex не авторизован",
}
STAGE_LABELS = {"prepare": "файл принят, подготовка", "analysis": "анализ переходов", "verify": "проверка выводов",
                "compose": "составление отчёта", "pdf": "формирование PDF"}
RETRY_KINDS = ("INVALID_JSON", "TRANSIENT", "UNKNOWN", "TIMEOUT", "CHECK")
_lock = threading.Lock()
_tokens: dict[str, ex.CancelToken] = {}
_threads = ThreadPoolExecutor(max_workers=4, thread_name_prefix="vc-mdr-job")


class ReportError(ValueError):
    pass


class _Stop(Exception):
    """Остановка задания: отмена или ограничение аккаунта (квота, вход)."""

    def __init__(self, kind: str):
        super().__init__(kind)
        self.kind = kind


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ------------------------------------------------------------------ пути

def reports_dir(object_id: str) -> Path:
    return paths.object_dir(object_id) / "md_reports"


def report_dir(object_id: str, report_id: str) -> Path:
    return reports_dir(object_id) / paths.safe_id(report_id)


def _job_path(object_id: str, report_id: str) -> Path:
    return report_dir(object_id, report_id) / "job.json"


def _update(object_id: str, report_id: str, fn: Callable[[dict], dict]) -> dict:
    return load_modify_save(_job_path(object_id, report_id), fn, default={})


def _describer() -> ex.ChangeDescriber:
    if ex._override is not None:
        return ex._override
    return ex.CodexExecDescriber(model=settings.model(), effort=settings.effort(), binary=settings.codex_bin() or None)


def runtime() -> dict[str, Any]:
    """Что увидит пользователь до запуска: модель, CLI, усилие, параллельность; без обращения к модели."""
    d = _describer()
    rt = d.describe_runtime()
    lim = settings.model_limits(rt.get("model"))
    return {"enabled": ai_settings.enabled(), "executor": rt, "model_limits": lim, "settings": settings.snapshot(),
            "transfer": "Полный текст загруженной MD-сводки будет передан исполнителю Codex на подписке сервера "
                        "частями; секреты в задания не входят."}


# ------------------------------------------------------------------ создание

def _safe_name(name: str) -> str:
    base = Path(name or "summary.md").name
    base = "".join(ch if ch.isalnum() or ch in "._- ()" else "_" for ch in base).strip(" .") or "summary.md"
    return base[:120] if base.lower().endswith(".md") else (base[:117] + ".md")


def create(object_id: str, raw: bytes, filename: str, *, project_id: str | None = None, consent: bool = False,
           user: str | None = None, origin: dict | None = None) -> dict[str, Any]:
    if not ai_settings.enabled():
        raise ReportError("ИИ-обработка в разделе «Сравнение версий» выключена")
    if not consent:
        raise ReportError("нужно подтвердить передачу текста сводки исполнителю")
    if not paths.object_dir(object_id).exists():
        raise ReportError("объект не найден")
    if project_id:
        from backend.app.services.version_comparison import store
        store.project(store.load_catalog(object_id), project_id)     # CatalogError, если проекта нет
    if not raw:
        raise ReportError("файл пустой")
    if len(raw) > settings.MAX_UPLOAD_BYTES:
        raise ReportError(f"файл больше {settings.MAX_UPLOAD_BYTES // (1024 * 1024)} МБ")
    manifest = parse.parse(raw, filename)            # отказ при не-UTF-8; структуру проверяем сразу
    if not manifest["transitions"]:
        raise ReportError("в файле не найдено ни одного перехода «## N. … A → B» — это не сводка изменений")
    sha = hashlib.sha256(raw).hexdigest()
    # Повторное нажатие: идущее задание с тем же файлом не дублируется.
    for j in list_reports(object_id):
        if j.get("state") in ACTIVE and (j.get("source") or {}).get("sha256") == sha:
            return get(object_id, j["report_id"])
    d = _describer()
    check = d.validate_runtime()
    if not check.get("ok"):
        raise ReportError("среда исполнителя не готова: " + "; ".join(check.get("problems") or []))
    report_id = datetime.now().strftime("mr%Y%m%d%H%M%S") + secrets.token_hex(2)
    rdir = report_dir(object_id, report_id)
    (rdir / "source").mkdir(parents=True, exist_ok=False)
    name = _safe_name(filename)
    sfile = rdir / "source" / name
    sfile.write_bytes(raw)
    os.chmod(sfile, 0o444)
    pdir = rdir / "prompts"
    pdir.mkdir()
    for n in stages.PROMPT_NAMES:
        (pdir / f"md_report_{n}.md").write_text(stages.prompt_text(n), encoding="utf-8")
    atomic_write_json(pdir / "schemas.json", {"analyst": schemas.ANALYST_SCHEMA, "verifier": schemas.VERIFIER_SCHEMA,
                                              "composer": schemas.COMPOSER_SCHEMA})
    job = {
        "schema": SCHEMA, "report_id": report_id, "object_id": object_id, "project_id": project_id,
        "origin": origin or {"kind": "upload"}, "user": user, "created_at": _now(),
        "source": {"filename": filename, "stored_name": name, "bytes": len(raw), "sha256": sha,
                   "lines": manifest["lines_total"], "project_code": manifest.get("project_code"),
                   "transitions": len(manifest["transitions"])},
        "consent": {"at": _now(), "user": user, "text": "Передача текста сводки исполнителю Codex подтверждена."},
        "executor": d.describe_runtime(), "runtime_check": {k: check.get(k) for k in ("ok", "problems", "checks")},
        "model_limits": settings.model_limits(d.describe_runtime().get("model")),
        "settings": settings.snapshot(),
        "versions": {"prompt": settings.PROMPT_VERSION, "prompt_digest": stages.prompt_digest(),
                     "schema": settings.SCHEMA_VERSION, "schema_digest": schemas.digest()},
        "state": "queued", "stage": "prepare", "reason": None, "owner": BOOT_ID,
        "progress": {}, "units": {}, "usage": {"calls": 0, "input_tokens": 0, "cached_input_tokens": 0,
                                               "output_tokens": 0, "reasoning_output_tokens": 0},
        "history": [{"at": _now(), "event": "created"}],
    }
    atomic_write_json(rdir / "job.json", job)
    _start(object_id, report_id)
    return get(object_id, report_id)


def _start(object_id: str, report_id: str) -> None:
    tok = ex.CancelToken()
    with _lock:
        _tokens[report_id] = tok
    _threads.submit(_run, object_id, report_id, tok)


# ------------------------------------------------------------------ чтение и управление

def get(object_id: str, report_id: str) -> dict[str, Any]:
    path = _job_path(object_id, report_id)
    if not path.exists():
        raise ReportError("отчёт не найден")
    job = json.loads(path.read_text(encoding="utf-8"))
    with _lock:
        alive = report_id in _tokens
    if job.get("state") in ACTIVE and (job.get("owner") != BOOT_ID or not alive):
        def mark(j):
            if j.get("state") in ACTIVE:
                j.update(state="interrupted", reason="сервер перезапущен во время обработки — можно продолжить")
            return j
        job = _update(object_id, report_id, mark)
    job["state_label"] = STATE_LABELS.get(job["state"], job["state"])
    job["stage_label"] = STAGE_LABELS.get(job.get("stage"), job.get("stage"))
    job["pdf_ready"] = job["state"] == "done" and (report_dir(object_id, report_id) / "report.pdf").exists()
    return job


def list_reports(object_id: str, project_id: str | None = None) -> list[dict[str, Any]]:
    root = reports_dir(object_id)
    if not root.exists():
        return []
    out = []
    for d in sorted(root.iterdir(), reverse=True):
        if not (d / "job.json").exists():
            continue
        j = get(object_id, d.name)
        if project_id and j.get("project_id") != project_id:
            continue
        units = j.get("units") or {}
        reused = [u for u in units.values() if u.get("reused")]
        out.append({k: j.get(k) for k in ("report_id", "project_id", "state", "state_label", "stage", "stage_label",
                                           "reason", "created_at", "finished_at", "source", "progress", "pdf_ready",
                                           "origin", "executor", "checks_summary", "usage")}
                   | {"reuse": {"units": len(units), "reused": len(reused),
                                "from": sorted({u["reused_from"] for u in reused if u.get("reused_from")})}})
    return out


def cancel(object_id: str, report_id: str) -> dict[str, Any]:
    with _lock:
        tok = _tokens.get(report_id)
    if tok:
        tok.cancel()
        _update(object_id, report_id, lambda j: j | {"state": "cancelling"} if j.get("state") in ACTIVE else j)
    return get(object_id, report_id)


def resume(object_id: str, report_id: str) -> dict[str, Any]:
    """Продолжить: готовые совместимые результаты этапов берутся с диска, повторяются только недостающие."""
    job = get(object_id, report_id)
    if job["state"] in ACTIVE:
        raise ReportError("отчёт ещё формируется")
    if job["state"] == "done":
        raise ReportError("отчёт уже готов")
    if not ai_settings.enabled():
        raise ReportError("ИИ-обработка в разделе «Сравнение версий» выключена")
    _update(object_id, report_id, lambda j: j | {"state": "queued", "reason": None, "owner": BOOT_ID,
                                                 "history": j.get("history", []) + [{"at": _now(), "event": "resume"}]})
    _start(object_id, report_id)
    return get(object_id, report_id)


def rebuild_pdf(object_id: str, report_id: str) -> dict[str, Any]:
    """Пересобрать PDF из сохранённого проверенного report.json без обращения к модели."""
    job = get(object_id, report_id)
    if job["state"] in ACTIVE:
        raise ReportError("отчёт ещё формируется")
    rdir = report_dir(object_id, report_id)
    if not (rdir / "report.json").exists() or not (rdir / "checks.json").exists():
        raise ReportError("проверенного отчёта ещё нет — PDF собирать не из чего")
    checks = json.loads((rdir / "checks.json").read_text(encoding="utf-8"))
    if checks.get("report_errors") or checks.get("consistency"):
        raise ReportError("отчёт не прошёл проверку — PDF из него не собирается")
    _pdf_stage(object_id, report_id, rdir, json.loads((rdir / "report.json").read_text(encoding="utf-8")))
    return get(object_id, report_id)


def file_path(object_id: str, report_id: str, name: str) -> Path:
    if name not in ("report.pdf", "report.json"):
        raise ReportError("неизвестный файл")
    job = get(object_id, report_id)
    if name == "report.pdf" and not job["pdf_ready"]:
        raise ReportError("PDF готов только у проверенного полного отчёта")
    f = report_dir(object_id, report_id) / name
    if not f.exists():
        raise ReportError("файл ещё не создан")
    return f


def download_name(object_id: str, report_id: str) -> str:
    job = get(object_id, report_id)
    code = (job["source"].get("project_code") or Path(job["source"]["stored_name"]).stem)
    safe = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in code).strip("_") or "отчёт"
    return f"Оценка_изменений_{safe}_{report_id}.pdf"


# ------------------------------------------------------------------ обращение к исполнителю

def _thread_id(stdout_file: Path) -> str | None:
    try:
        for line in stdout_file.read_text(encoding="utf-8").splitlines():
            if '"thread.started"' in line:
                return json.loads(line).get("thread_id")
    except (OSError, ValueError):
        return None
    return None


class _Ctx:
    def __init__(self, object_id: str, report_id: str, rdir: Path, tok: ex.CancelToken):
        self.object_id, self.report_id, self.rdir, self.tok = object_id, report_id, rdir, tok
        self.describer = _describer()

    def set_unit(self, key: str, **fields) -> None:
        def apply(j):
            u = j.setdefault("units", {}).setdefault(key, {})
            u.update(fields)
            return j
        _update(self.object_id, self.report_id, apply)

    def add_usage(self, u: dict) -> None:
        def apply(j):
            tot = j.setdefault("usage", {})
            tot["calls"] = tot.get("calls", 0) + 1
            for k in ("input_tokens", "cached_input_tokens", "output_tokens", "reasoning_output_tokens"):
                tot[k] = tot.get(k, 0) + int(u.get(k) or 0)
            return j
        _update(self.object_id, self.report_id, apply)

    def call(self, stage: str, unit: str, prompt: str, schema: dict,
             check: Callable[[dict], dict]) -> tuple[dict | None, dict]:
        """Обращение с повтором при ответе не по схеме, непрошедшей проверке или временном отказе.

        Возвращает (ответ, сведения). Ответ с одними «исправимыми» замечаниями после последней попытки
        принимается с предупреждением; с «жёсткими» — нет. Квота, вход и отмена — исключение ``_Stop``.
        """
        key = f"{stage}:{unit}"
        attempts = settings.retries() + 1
        info: dict[str, Any] = {"attempts": [], "warnings": []}
        last_ok: tuple[dict, dict] | None = None
        feedback = ""
        for n in range(1, attempts + 1):
            if self.tok.cancelled:
                raise _Stop("CANCELLED")
            adir = self.rdir / "calls" / stage / unit / f"attempt-{n}"
            adir.mkdir(parents=True, exist_ok=True)
            text = prompt + feedback
            (adir / "prompt.md").write_text(text, encoding="utf-8")
            self.set_unit(key, state="running", attempt=n, started_at=_now())
            task = ex.DescribeTask(prompt=text, schema=schema, image_paths=[], timeout_s=settings.call_timeout_s(),
                                   marker=f"vcmr-{self.report_id}", packet_id=self.report_id, chunk_id=key,
                                   raw_path=str(adir / "answer.raw.txt"))
            res = self.describer.run(task, self.tok)
            self.add_usage(res.usage or {})
            rec = {"attempt": n, "ok": res.ok, "error_kind": res.error_kind, "error": res.error,
                   "seconds": round(res.duration_ms / 1000, 1), "usage": res.usage,
                   "thread_id": _thread_id(adir / "answer.raw.stdout.jsonl"), "prompt_chars": len(text),
                   "executor": self.describer.describe_runtime(), "at": _now()}
            if res.ok:
                errs = response_contract.validate(res.parsed, schema)
                if errs:
                    rec.update(ok=False, error_kind="INVALID_JSON", error="ответ не по схеме: " + "; ".join(errs[:5]))
                else:
                    chk = check(res.parsed)
                    rec["check"] = chk
                    atomic_write_json(adir / "answer.json", res.parsed)
                    if chk["ok"]:
                        info["attempts"].append(rec)
                        atomic_write_json(adir / "call.json", rec)
                        return res.parsed, info | {"check": chk}
                    if not chk.get("hard"):
                        last_ok = (res.parsed, chk)
                    rec.update(ok=False, error_kind="CHECK",
                               error="проверка программы: " + "; ".join((chk.get("hard") or []) + (chk.get("fixable") or []))[:600])
                    feedback = ("\n\nПРЕДЫДУЩИЙ ОТВЕТ ОТКЛОНЁН ПРОВЕРКОЙ ПРОГРАММЫ:\n- "
                                + "\n- ".join((chk.get("hard") or []) + (chk.get("fixable") or []))
                                + "\nВерни полный исправленный ответ по той же схеме.\n")
            info["attempts"].append(rec)
            atomic_write_json(adir / "call.json", rec)
            if rec["error_kind"] in ("QUOTA", "AUTH", "CANCELLED"):
                raise _Stop(rec["error_kind"])
            if rec["error_kind"] not in RETRY_KINDS:
                break
        if last_ok:
            info["warnings"] = last_ok[1].get("fixable") or []
            return last_ok[0], info | {"check": last_ok[1]}
        last = info["attempts"][-1] if info["attempts"] else {}
        info["error_kind"], info["error"] = last.get("error_kind"), last.get("error")
        return None, info


# ------------------------------------------------------------------ выполнение

STAGE_PROMPTS = {"analyst": "analyst", "verify": "verifier", "compose": "composer"}


def _stage_digest(rdir: Path, job: dict, stage: str) -> str:
    """Отпечаток этапа: промпты и схема, ЗАПИСАННЫЕ в отчёт при создании, и исполнитель задания."""
    name = STAGE_PROMPTS[stage]
    pdir = rdir / "prompts"
    texts = {n: (pdir / f"md_report_{n}.md").read_text(encoding="utf-8") for n in ("rules", "common", name)}
    schema = json.loads((pdir / "schemas.json").read_text(encoding="utf-8"))[name]
    exr = job.get("executor") or {}
    extra = {"verify_full_chars": (job.get("settings") or {}).get("verify_full_chars")} if stage == "verify" else {}
    return ai_settings.digest({"version": job["versions"]["prompt"], "texts": texts, "schema": schema,
                               "executor": exr.get("executor"), "model": exr.get("model"),
                               "effort": exr.get("effort")} | extra)


def _read(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _result_file(rdir: Path, stage: str, unit: str) -> Path:
    return rdir / "results" / ("compose.json" if stage == "compose" else f"{stage}/{paths.safe_id(unit)}.json")


def _inputs(rdir: Path, stage: str, unit: str) -> Any:
    """Что, кроме промпта, определяет ответ единицы: блоки источника или результаты предыдущего этапа."""
    plan = _read(rdir / "plan.json")
    if stage == "analyst":
        by = {b["block_id"]: b for b in plan["blocks"]}
        g = next((x for x in plan["groups"] if x["group_id"] == unit), None)
        return g and {"blocks": [by[b]["sha256"] for b in g["block_ids"]], "transitions": g["transition_ids"],
                      "header": [b["sha256"] for b in plan["blocks"] if b["kind"] == "header"]}
    if stage == "verify":
        u = next((x for x in plan["verify_units"] if x["unit_id"] == unit), None)
        files = [_result_file(rdir, "analyst", gid) for gid in (u or {}).get("group_ids", [])]
        if not u or not all(f.exists() for f in files):
            return None
        return {"unit": u, "analyst": ai_settings.digest({f.stem: _read(f)["answer"] for f in files})}
    files = [_result_file(rdir, "verify", u["unit_id"]) for u in plan["verify_units"]]
    if not all(f.exists() for f in files):
        return None
    return {"verified": ai_settings.digest({f.stem: _read(f)["verified"] for f in files})}


def unit_key(rdir: Path, job: dict, stage: str, unit: str) -> str | None:
    """Ключ совместимости результата единицы: SHA-256 входа, отпечаток этапа, единица и её входы."""
    try:
        inputs = _inputs(rdir, stage, unit)
        if inputs is None:
            return None
        return ai_settings.digest({"source": job["source"]["sha256"], "stage": _stage_digest(rdir, job, stage),
                                   "unit": unit, "inputs": inputs})[:32]
    except (OSError, ValueError, KeyError):
        return None


def _find_result(ctx: "_Ctx", job: dict, stage: str, unit: str) -> tuple[dict, str | None, str | None]:
    """(результат, откуда взят, ключ). Свой файл с тем же ключом или проверенный результат другого
    отчёта того же файла с тем же ключом (те же промпты этапа, исполнитель и входы) — без обращения."""
    key = unit_key(ctx.rdir, job, stage, unit)
    own = _result_file(ctx.rdir, stage, unit)
    if key is None:
        return {}, None, None
    if own.exists() and _read(own).get("key") == key:
        return _read(own), None, key
    root = reports_dir(ctx.object_id)
    for d in sorted(root.iterdir(), reverse=True) if root.exists() else []:
        if d.name == ctx.report_id or not (d / "job.json").exists():
            continue
        try:
            sj = _read(d / "job.json")
            f = _result_file(d, stage, unit)
            if sj.get("state") in ACTIVE or (sj.get("source") or {}).get("sha256") != job["source"]["sha256"] \
                    or not f.exists() or unit_key(d, sj, stage, unit) != key:
                continue
            data = _read(f) | {"key": key, "reused_from": {"report_id": d.name, "at": _read(f).get("at")}}
        except (OSError, ValueError, KeyError):
            continue
        atomic_write_json(own, data)
        return data, d.name, key
    return {}, None, key


def _run(object_id: str, report_id: str, tok: ex.CancelToken) -> None:
    rdir = report_dir(object_id, report_id)
    final: dict[str, Any] = {}
    try:
        job = _update(object_id, report_id, lambda j: j | {"state": "running", "owner": BOOT_ID, "stage": "prepare",
                                                           "started_at": j.get("started_at") or _now()})
        # Новый запуск (или продолжение) идёт с моделью и версиями из задания: смена настроек не смешивает ответы.
        ctx = _Ctx(object_id, report_id, rdir, tok)
        rt = ctx.describer.describe_runtime()
        if (rt.get("model"), rt.get("effort")) != (job["executor"].get("model"), job["executor"].get("effort")) \
                and ex._override is None:
            ctx.describer = ex.CodexExecDescriber(model=job["executor"]["model"], effort=job["executor"]["effort"],
                                                  binary=job["executor"].get("cli") or settings.codex_bin() or None)
        if job["versions"]["prompt_digest"] != stages.prompt_digest() or job["versions"]["schema_digest"] != schemas.digest():
            raise ReportError("промпты или схемы изменились после создания отчёта — создайте новый отчёт")
        raw = (rdir / "source" / job["source"]["stored_name"]).read_bytes()
        if hashlib.sha256(raw).hexdigest() != job["source"]["sha256"]:
            raise ReportError("исходный файл изменён после приёма")
        manifest = parse.parse(raw, job["source"]["filename"])
        lines = parse.split_lines(parse.decode(raw))
        try:
            plan = plan_mod.build(manifest, lines, settings.budget())
        except plan_mod.PlanError as exc:
            raise ReportError(f"структура не делится на задания без потерь: {exc}") from exc
        atomic_write_json(rdir / "manifest.json", manifest)
        atomic_write_json(rdir / "plan.json", plan)
        src = job["source"]
        _update(object_id, report_id, lambda j: j | {"stage": "analysis", "progress": {
            "transitions": len(manifest["transitions"]), "groups_total": len(plan["groups"]),
            "units_total": len(plan["verify_units"]), "groups_done": 0, "units_done": 0,
            "structure_issues": manifest["issues"], "blocks": len(plan["blocks"])}})
        analyst, verified, failures = _analyse_and_verify(ctx, job, manifest, plan, lines, src)
        missing = [t["label"] for t in manifest["transitions"]
                   if not any(t["transition_id"] in v["unit_transition_ids"] for v in verified.values())]
        cov = {"blocks_total": len(plan["blocks"]), "groups_ok": sorted(analyst), "units_ok": sorted(verified),
               "missing_transitions": missing, "failures": failures,
               "complete": not failures and not missing and len(analyst) == len(plan["groups"])}
        atomic_write_json(rdir / "results" / "coverage.json", cov)
        if not cov["complete"]:
            final = {"state": "incomplete", "reason": "не обработаны переходы: " + (", ".join(missing) or "—")
                     + ". " + "; ".join(f"{f['unit']}: {f['error']}" for f in failures)[:600]}
            return
        _update(object_id, report_id, lambda j: j | {"stage": "compose"})
        report, checks = _compose(ctx, job, manifest, verified, lines, src)
        atomic_write_json(rdir / "report.json", report)
        atomic_write_json(rdir / "checks.json", checks)
        _update(object_id, report_id, lambda j: j | {"checks_summary": {
            "warnings": checks["warnings"], "report_valid": not checks["report_errors"]}})
        if checks["report_errors"] or checks["consistency"]:
            final = {"state": "failed", "reason": "отчёт не прошёл проверку: "
                     + "; ".join(checks["report_errors"] + checks["consistency"])[:600]}
            return
        _pdf_stage(object_id, report_id, rdir, report)
    except _Stop as stop:
        final = {"state": "cancelled", "reason": "отменено пользователем"} if stop.kind == "CANCELLED" else \
            {"state": "paused_quota" if stop.kind == "QUOTA" else "paused_auth", "reason": ex.ERROR_TEXT[stop.kind]}
    except ReportError as exc:
        final = {"state": "failed", "reason": str(exc)}
    except Exception as exc:  # pragma: no cover - защитный путь
        final = {"state": "failed", "reason": f"{type(exc).__name__}: {exc}", "trace": traceback.format_exc()[-3000:]}
    finally:
        def finish(j):
            if final:
                j.update(final)
            elif tok.cancelled:
                j.update(state="cancelled", reason="отменено пользователем")
            elif j.get("state") in ACTIVE:
                j.update(state="failed", reason=j.get("reason") or "этап PDF не завершён")
            j["finished_at"] = _now()
            j["history"] = j.get("history", []) + [{"at": _now(), "event": j["state"]}]
            return j
        _update(object_id, report_id, finish)
        with _lock:
            _tokens.pop(report_id, None)


def _analyse_and_verify(ctx: _Ctx, job: dict, manifest: dict, plan: dict, lines: list[str], src: dict):
    """Аналитики групп параллельно; проверка единицы — как только готовы все её группы."""
    rid, rdir = ctx.report_id, ctx.rdir
    groups = {g["group_id"]: g for g in plan["groups"]}
    analyst: dict[str, dict] = {}
    verified: dict[str, dict] = {}
    failures: list[dict] = []
    stop: list[_Stop] = []

    def run_group(g: dict) -> None:
        old, src_report, key = _find_result(ctx, job, "analyst", g["group_id"])
        f = _result_file(rdir, "analyst", g["group_id"])
        if old:
            analyst[g["group_id"]] = old["answer"]
            ctx.set_unit(f"analyst:{g['group_id']}", state="done", reused=True, reused_from=src_report)
            return
        prompt = stages.analyst_prompt(rid, src, manifest, plan, g, lines)
        ans, info = ctx.call("analyst", g["group_id"], prompt, schemas.ANALYST_SCHEMA,
                             lambda a: stages.check_analyst(a, rid, g, plan, manifest))
        if ans is None:
            failures.append({"unit": g["group_id"], "stage": "analyst", "error_kind": info.get("error_kind"),
                             "error": info.get("error") or "ответ не получен"})
            ctx.set_unit(f"analyst:{g['group_id']}", state="failed", error=info.get("error"),
                         error_kind=info.get("error_kind"))
            return
        atomic_write_json(f, {"key": key, "group": g, "answer": ans, "check": info.get("check"), "at": _now()})
        analyst[g["group_id"]] = ans
        ctx.set_unit(f"analyst:{g['group_id']}", state="done", finished_at=_now())

    def run_unit(u: dict) -> None:
        results = {gid: analyst[gid] for gid in u["group_ids"]}
        old, src_report, key = _find_result(ctx, job, "verify", u["unit_id"])
        f = _result_file(rdir, "verify", u["unit_id"])
        if old:
            verified[u["unit_id"]] = old["verified"]
            ctx.set_unit(f"verify:{u['unit_id']}", state="done", reused=True, reused_from=src_report)
            return
        claims = stages.claims_for_unit(results)
        prompt, shown = stages.verifier_prompt(rid, src, manifest, plan, u, results, claims, lines)
        ans, info = ctx.call("verify", u["unit_id"], prompt, schemas.VERIFIER_SCHEMA,
                             lambda a: stages.check_verifier(a, rid, u, claims, shown))
        if ans is None:
            failures.append({"unit": u["unit_id"], "stage": "verify", "error_kind": info.get("error_kind"),
                             "error": info.get("error") or "ответ не получен"})
            ctx.set_unit(f"verify:{u['unit_id']}", state="failed", error=info.get("error"),
                         error_kind=info.get("error_kind"))
            return
        clean = stages.clean_verifier(ans, claims, shown) | {"unit_transition_ids": u["transition_ids"]}
        atomic_write_json(f, {"key": key, "unit": u, "claims": claims, "shown_ranges": shown, "answer": ans,
                              "verified": clean, "check": info.get("check"), "at": _now()})
        verified[u["unit_id"]] = clean
        ctx.set_unit(f"verify:{u['unit_id']}", state="done", finished_at=_now())

    def progress():
        _update(ctx.object_id, ctx.report_id, lambda j: j | {
            "stage": "verify" if len(analyst) == len(groups) else "analysis",
            "progress": j.get("progress", {}) | {"groups_done": len(analyst), "units_done": len(verified),
                                                 "failed": len(failures)}})

    def guarded(fn, arg):
        if stop or ctx.tok.cancelled:
            return
        try:
            fn(arg)
        except _Stop as s:
            stop.append(s)
        except Exception as exc:  # одна единица не роняет остальные
            failures.append({"unit": arg.get("group_id") or arg.get("unit_id"), "stage": fn.__name__,
                             "error_kind": "UNKNOWN", "error": f"{type(exc).__name__}: {exc}"})

    units_left = list(plan["verify_units"])
    with ThreadPoolExecutor(max_workers=settings.parallel(), thread_name_prefix="vc-mdr-call") as pool:
        pending = {pool.submit(guarded, run_group, g) for g in plan["groups"]}
        while pending:
            done, pending = wait(pending, return_when=FIRST_COMPLETED)
            progress()
            if stop or ctx.tok.cancelled:
                continue
            for u in list(units_left):
                if all(gid in analyst for gid in u["group_ids"]):
                    units_left.remove(u)
                    pending.add(pool.submit(guarded, run_unit, u))
    progress()
    if stop:
        raise stop[0]
    if ctx.tok.cancelled:
        raise _Stop("CANCELLED")
    return analyst, verified, failures


def _compose(ctx: _Ctx, job: dict, manifest: dict, verified: dict, lines: list[str], src: dict):
    rid, rdir = ctx.report_id, ctx.rdir
    old, src_report, key = _find_result(ctx, job, "compose", "C01")
    f = _result_file(rdir, "compose", "C01")
    if old:
        ans, chk, warnings = old["answer"], old["check"], old.get("warnings") or []
        ctx.set_unit("compose:C01", state="done", reused=True, reused_from=src_report)
    else:
        prompt = stages.composer_prompt(rid, src, manifest, verified)
        ans, info = ctx.call("compose", "C01", prompt, schemas.COMPOSER_SCHEMA,
                             lambda a: stages.check_composer(a, rid, manifest, verified, lines))
        if ans is None:
            ctx.set_unit("compose:C01", state="failed", error=info.get("error"), error_kind=info.get("error_kind"))
            raise ReportError(f"составитель не дал годного ответа: {info.get('error')}")
        ctx.set_unit("compose:C01", state="done", finished_at=_now(), warnings=info.get("warnings") or [])
        chk, warnings = info["check"], info.get("warnings") or []
        atomic_write_json(f, {"key": key, "answer": ans, "check": chk, "warnings": warnings, "at": _now()})
    report = stages.assemble(ans, manifest, verified, src, lines)
    errors = schemas.validate_report(report, manifest["lines_total"])
    cons = stages.consistency(report, manifest)
    claims = [c for v in verified.values() for c in v["claims"]]
    verdicts: dict[str, int] = {}
    for c in claims:
        verdicts[c["verdict"]] = verdicts.get(c["verdict"], 0) + 1
    checks = {"report_errors": errors, "consistency": cons, "warnings": warnings + chk.get("soft", []),
              "unconfirmed_numbers": chk.get("unconfirmed_numbers"), "words": chk.get("words"),
              "distribution": stages.distribution(report), "claims_total": len(claims), "verdicts": verdicts,
              "structure_issues": manifest["issues"], "inventory": manifest["inventory"]}
    return report, checks


def _pdf_stage(object_id: str, report_id: str, rdir: Path, report: dict) -> None:
    _update(object_id, report_id, lambda j: j | {"stage": "pdf", "state": "running" if j.get("state") in ACTIVE
                                                 else j.get("state")})
    job = json.loads(_job_path(object_id, report_id).read_text(encoding="utf-8"))
    at = datetime.fromisoformat(job.get("started_at") or job["created_at"]).astimezone().strftime("%d.%m.%Y %H:%M")
    out = rdir / "report.pdf"
    tmp = rdir / "report.new.pdf"
    try:
        pdf.render(report, tmp, analysed_at=at, sha256=job["source"]["sha256"])
        preview = rdir / "pdf_preview"
        shutil.rmtree(preview, ignore_errors=True)
        chk = pdf.check(tmp, report, preview, forbidden=(report_id,))
    except Exception as exc:  # noqa: BLE001 — ошибка рендера отдельная от анализа
        chk = {"ok": False, "problems": [f"ошибка формирования PDF: {type(exc).__name__}: {exc}"]}
    atomic_write_json(rdir / "pdf_check.json", chk)
    if chk["ok"]:
        tmp.replace(out)
        _update(object_id, report_id, lambda j: j | {"state": "done", "stage": "pdf", "reason": None,
                                                     "pdf": {"pages": chk.get("pages"), "words": chk.get("words"),
                                                             "warnings": chk.get("warnings"), "at": _now()}})
    else:
        tmp.unlink(missing_ok=True)
        _update(object_id, report_id, lambda j: j | {"state": "failed", "stage": "pdf",
                                                     "reason": "PDF не прошёл проверку: " + "; ".join(chk["problems"])[:500]})
