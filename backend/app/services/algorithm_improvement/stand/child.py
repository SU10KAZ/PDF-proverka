"""Один повтор (документ × плечо × номер) в отдельном процессе.

Запуск (делает родитель — `backend/scripts/algorithm_improvement_docnorm_stand.py`):
    cd <frozen/code>; python -m backend.app.services.algorithm_improvement.stand.child <spec.json>

Окружение (AUDIT_DATA_DIR, AUDIT_APP_DATA_DIR, NORMS_*, флаги) выставляет
родитель до старта интерпретатора. Здесь: сторож → проверка путей → подмена
вызовов моделей → боевые этапы: свод (доводка), верификатор, нормы.

Подменяется только самый нижний уровень вызовов моделей:
- `codex_runner.run_codex_json_messages` по `stage`:
    findings_merge        → архивная база свода (вход до docnorm);
    alia_ar_masonry_audit → архивный выход прохода по кладке (от docnorm не зависит);
    alia_docnorm_audit    → исполнитель плеча (сухой — поддельный, боевой — Codex);
    любой другой          → ошибка «неожиданный вызов модели»;
- `absence_guard.run_claude_verification` (страж отсутствия);
- `claude_runner.run_norm_fix / run_norm_verify / run_norm_requote / run_optimization_norm_fix`.
Сборка промпта docnorm, объединение, атомарность, доводка свода, верификатор и
нормы выполняются боевым кодом без изменений.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sys
import time
from pathlib import Path


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class Recorder:
    def __init__(self, path: Path):
        self.path = path
        self.counts: dict[str, int] = {}

    def add(self, kind: str, **fields) -> None:
        self.counts[kind] = self.counts.get(kind, 0) + 1
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps({"kind": kind, "n": self.counts[kind], **fields}, ensure_ascii=False) + "\n")


def main(spec_path: str) -> int:
    spec = json.loads(Path(spec_path).read_text(encoding="utf-8"))
    run_dir = Path(spec["run_dir"])
    receipts = run_dir / "receipts"
    mode = spec["mode"]
    assert mode in ("dry", "real"), mode
    if mode == "real" and os.environ.get("AIM_ALLOW_MODEL_CALLS") != spec.get("permission_token"):
        print("real mode без разрешения", file=sys.stderr)
        return 3

    # Сторож грузится по пути файла: импорт пакета `backend` до установки хука недопустим.
    import importlib.util
    backend_imported_before_guard = any(m == "backend" or m.startswith("backend.") for m in sys.modules)
    guard_spec = importlib.util.spec_from_file_location("aim_stand_guard", Path(__file__).with_name("guard.py"))
    guard = importlib.util.module_from_spec(guard_spec)
    guard_spec.loader.exec_module(guard)
    guard.install(
        write_roots=[run_dir],
        forbidden_read_roots=spec["forbidden_read_roots"],
        violations_path=receipts / "violations.jsonl",
        allow_spawn=(mode == "real"),
        allowed_exec_names=spec.get("allowed_exec_names", []),
    )

    from backend.app.core import config
    expected = {"DATA_DIR": spec["data_dir"], "APP_DATA_DIR": spec["app_data_dir"],
                "PROMPTS_DIR": str(Path(spec["data_dir"]) / "prompts")}
    actual = {k: str(getattr(config, k)) for k in expected}
    if any(Path(actual[k]).resolve() != Path(v).resolve() for k, v in expected.items()):
        raise SystemExit(f"пути конфигурации не указывают в песочницу: {actual}")
    if str(Path(config.ROOT_DIR).resolve()) != str(Path(spec["code_root"]).resolve()):
        raise SystemExit(f"ROOT_DIR={config.ROOT_DIR} — ожидался замороженный код {spec['code_root']}")

    rec = Recorder(receipts / "calls.jsonl")
    from backend.app.models.usage import LLMResult
    from backend.app.services.llm import claude_runner, codex_runner
    from backend.app.pipeline.stages.text_analysis import absence_guard
    from backend.app.pipeline.stages.norms import runner as norms_runner
    from backend.app.services.algorithm_improvement import docnorm_variant
    from backend.app.services.algorithm_improvement.stand import fakes

    inputs_latest = Path(spec["output_dir"])
    # Дисциплинарный целевой проход (кладка АР / защита ЭОМ / слаботочка СС / сталь КМ) от docnorm
    # не зависит — берётся из архива проверенного прогона, как и база свода.
    archived = {"findings_merge": inputs_latest / "03_findings_codex_base.json"}
    for stage in ("alia_ar_masonry_audit", "alia_eom_protection_audit", "alia_ss_lowcurrent_audit",
                  "alia_km_steel_audit"):
        archived[stage] = inputs_latest / f"03_findings_targeted_{stage}.json"
    archived_payloads = {k: (json.loads(p.read_text(encoding="utf-8")) if p.is_file() else None)
                         for k, p in archived.items()}
    seeds = json.loads(Path(spec["fake_seed"]).read_text(encoding="utf-8")) if mode == "dry" else []
    original_json_call = codex_runner.run_codex_json_messages
    docnorm_messages_seen: list[list[dict]] = []
    production_system_seen: list[str] = []

    async def dispatch_json(messages, *, timeout, on_output=None, stage="", project_id="", model=None, **kw):
        t0 = time.time()
        if stage in archived_payloads:
            payload = archived_payloads[stage]
            rec.add("archived_input", stage=stage, present=payload is not None, ts=t0)
            if payload is None:
                return LLMResult(is_error=True, error_message=f"stand: архивный выход {stage} отсутствует",
                                 model="archived")
            text = json.dumps(payload, ensure_ascii=False)
            return LLMResult(text=text, json_data=payload, model=f"archived:{stage}")
        if stage != "alia_docnorm_audit":
            rec.add("unexpected_model_call", stage=stage, ts=t0)
            raise RuntimeError(f"stand: неожиданный вызов модели на этапе {stage!r}")
        arm_messages = docnorm_variant.messages_for_arm(spec["arm"], messages)
        docnorm_messages_seen.append(arm_messages)
        production_system_seen.append(_sha(str(messages[0].get("content") or "")))
        msg_sha = _sha(json.dumps(arm_messages, ensure_ascii=False))
        if mode == "dry":
            payload = fakes.fake_docnorm_payload(seeds, arm=spec["arm"], repeat=spec["repeat"])
            text = json.dumps(payload, ensure_ascii=False)
            result = LLMResult(text=text, json_data=payload, model="FAKE")
        else:
            result = await original_json_call(arm_messages, timeout=timeout, on_output=on_output, stage=stage,
                                              project_id=project_id, model=model, **kw)
        rec.add("docnorm", arm=spec["arm"], repeat=spec["repeat"], fake=(mode == "dry"), ts_start=t0,
                ts_end=time.time(), model=result.model, input_tokens=result.input_tokens,
                output_tokens=result.output_tokens, is_error=result.is_error,
                messages_sha256=msg_sha, response_sha256=_sha(result.text or ""))
        return result

    codex_runner.run_codex_json_messages = dispatch_json

    original_absence = absence_guard.run_claude_verification

    def absence(md_text, candidates, *, timeout_sec=180):
        t0 = time.time()
        if mode == "dry":
            out = fakes.fake_absence_verdicts(candidates)
        else:
            out = original_absence(md_text, candidates, timeout_sec=timeout_sec)
        rec.add("absence_verifier", candidates=len(candidates), fake=(mode == "dry"), ts_start=t0,
                ts_end=time.time(), response_sha256=_sha(json.dumps(out, sort_keys=True, default=str)))
        return out

    absence_guard.run_claude_verification = absence

    def wrap_claude(name, fake_impl):
        original = getattr(claude_runner, name)

        async def wrapped(*args, **kwargs):
            t0 = time.time()
            if mode == "dry":
                res = await fake_impl(*args, **kwargs)
            else:
                res = await original(*args, **kwargs)
            cli = res[2] if isinstance(res, tuple) and len(res) > 2 else None
            rec.add(name, fake=(mode == "dry"), ts_start=t0, ts_end=time.time(),
                    exit_code=res[0] if isinstance(res, tuple) else None,
                    model=getattr(cli, "model", None),
                    input_tokens=getattr(cli, "input_tokens", None),
                    output_tokens=getattr(cli, "output_tokens", None))
            return res
        setattr(claude_runner, name, wrapped)

    async def fake_norm_fix(findings_to_fix_text, project_id, on_output=None, project_info=None, *,
                            output_dir=None, version_dir=None, version_id=None):
        changed = fakes.fake_norm_fix_apply(Path(output_dir or spec["output_dir"]) / "03_findings.json",
                                            findings_to_fix_text)
        return 0, f"FAKE norm_fix: {changed}", LLMResult(text="", model="FAKE")

    async def fake_noop(*args, **kwargs):
        return 0, "FAKE", LLMResult(text="", model="FAKE")

    wrap_claude("run_norm_fix", fake_norm_fix)
    for name in ("run_norm_verify", "run_norm_requote", "run_optimization_norm_fix"):
        wrap_claude(name, fake_noop)
    norms_runner.other_projects_on_norm_stage = lambda *_a, **_k: 0

    from backend.app.pipeline.context import PipelineStageContext
    from backend.app.pipeline.stages.findings_merge.runner import run_findings_merge
    from backend.app.pipeline.stages.findings_verify.runner import run_findings_verify
    from backend.app.services.common import version_service
    from backend.app.services.common.project_service import bind_object
    from backend.app.services.storage.projects_v2_source_resolver import load_version_project_info

    version_dir = Path(spec["version_dir"])
    output_dir = Path(spec["output_dir"])
    project_info = load_version_project_info(version_dir) or {}
    stage_log = receipts / "stage_log.jsonl"
    pipeline_log: dict = {"stages": {}}

    async def log(message, level="info", **_kw):
        with stage_log.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps({"ts": time.time(), "level": level, "message": str(message)}, ensure_ascii=False) + "\n")

    async def yes(*_a, **_k):
        return True

    async def no(*_a, **_k):
        return False

    def update_log(stage_key, status, **kw):
        pipeline_log["stages"][stage_key] = {"status": status, **{k: str(v)[:500] for k, v in kw.items()}}

    def record_usage(cli_result, stage_name, is_retry=False):
        rec.add("usage", stage=stage_name, model=getattr(cli_result, "model", None),
                input_tokens=getattr(cli_result, "input_tokens", None),
                output_tokens=getattr(cli_result, "output_tokens", None))

    async def no_subprocess(*_a, **_k):
        raise RuntimeError("stand: run_subprocess не ожидается")

    def make_ctx():
        return PipelineStageContext(
            project_dir=version_dir, project_id=spec["project_id"], output_dir=output_dir, log=log,
            check_before_launch=yes, check_pause=yes, wait_for_rate_limit=no, record_cli_usage=record_usage,
            update_pipeline_log=update_log, run_subprocess=no_subprocess, project_info=project_info,
            object_id=spec["object_id"], version_id=spec["version_id"], is_cancelled=lambda: False,
        )

    result_dir = run_dir / "result"
    result_dir.mkdir(exist_ok=True)

    def snapshot(name: str) -> list[str]:
        path = output_dir / "03_findings.json"
        data = json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {"findings": []}
        (result_dir / f"{name}.json").write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")
        return [str(f.get("id")) for f in data.get("findings", [])]

    async def pipeline() -> dict:
        tv = version_service.bind_version(spec["version_id"])
        to = bind_object(spec["object_id"])
        out: dict = {}
        try:
            r = await run_findings_merge(make_ctx())
            out["findings_merge"] = {"success": r.success, "error": getattr(r, "error", None)}
            out["ids_after_merge"] = snapshot("after_merge")
            r = await run_findings_verify(make_ctx())
            out["findings_verify"] = {"success": getattr(r, "success", None), "error": getattr(r, "error", None)}
            out["ids_after_verify"] = snapshot("after_verify")
            r = await norms_runner.run_norm_verification(make_ctx(), wait_before_fix=None)
            out["norm_verify"] = {"success": getattr(r, "success", None), "error": getattr(r, "error", None)}
            out["ids_final"] = snapshot("final")
        finally:
            version_service.unbind_version(tv)
            from backend.app.services.common.project_service import unbind_object
            unbind_object(to)
        return out

    t0 = time.time()
    status = "ok"
    error = None
    try:
        stages = asyncio.run(pipeline())
    except Exception as exc:  # noqa: BLE001 — результат повтора фиксируется как есть
        stages, status, error = {}, "error", f"{type(exc).__name__}: {exc}"
    user_msg = docnorm_messages_seen[0][1]["content"] if docnorm_messages_seen else ""
    base_path = archived["findings_merge"]
    base_prefix = base_path.read_text(encoding="utf-8")[:2000] if base_path.is_file() else ""
    checks = {
        "docnorm_called": bool(docnorm_messages_seen),
        "docnorm_sees_archived_base": bool(base_prefix) and base_prefix[:2000] in user_msg,
        "arm_system_prompt_sha256": _sha(docnorm_messages_seen[0][0]["content"]) if docnorm_messages_seen else None,
        "production_system_prompt_sha256": production_system_seen[0] if production_system_seen else None,
        "arm_a_equals_production": (spec["arm"] != "A") or (bool(production_system_seen) and
                                    production_system_seen[0] == _sha(docnorm_messages_seen[0][0]["content"])),
        "guard_before_backend_import": not backend_imported_before_guard,
        "variant_b_sha256": docnorm_variant.variant_sha256(),
        "norms_contract_ids_unchanged": (stages.get("ids_after_verify") == stages.get("ids_final"))
        if stages.get("ids_final") is not None else None,
        "guard_violations": guard.installed().count,
    }
    (receipts / "run.json").write_text(json.dumps({
        "status": status, "error": error, "duration_s": round(time.time() - t0, 1), "stages": {
            k: v for k, v in stages.items() if not k.startswith("ids_")},
        "counts": rec.counts, "checks": checks, "pipeline_log": pipeline_log,
        "ids": {k: v for k, v in stages.items() if k.startswith("ids_")},
    }, ensure_ascii=False, indent=1), encoding="utf-8")
    return 0 if status == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1]))
