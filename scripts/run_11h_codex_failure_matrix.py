#!/usr/bin/env python3
"""11H §17 — матрица отказов Codex-пути. Обращений к модели ноль.

Каждый сценарий отвечает на один вопрос: «что произойдёт, если…». Проверяется
не намерение, а ФАКТ — код ошибки, который вернёт боевой слой, и состояние
журнала вызовов после него.

Подделан ровно последний метр: бинарь CLI (`provider_bridge_stub`, ветка
codex) либо, где сценарий этого требует, его подмена на скрипт, который ведёт
себя нужным образом (таймаут, ненулевой выход, мусор вместо JSON).

Запуск:  python scripts/run_11h_codex_failure_matrix.py [--out ФАЙЛ]
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import stat
import sys
import tempfile
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from audit_worker.providers import errors                       # noqa: E402
from audit_worker.providers.auth_mode import AUTH_MODE_AMBIENT_USER  # noqa: E402
from audit_worker.providers.codex_adapter import CodexProviderAdapter  # noqa: E402
from audit_worker.providers.inference_ledger import InferenceLedger  # noqa: E402
from audit_worker.providers.paths import ProviderHome            # noqa: E402
from audit_worker.providers import pipeline_bridge               # noqa: E402
from audit_worker.providers.resolver import ProviderBinding       # noqa: E402
from backend.app.pipeline.execution import provider_bridge_stub   # noqa: E402

PNG_1X1 = bytes.fromhex(
    "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c4"
    "890000000a49444154789c63000100000500010d0a2db40000000049454e44ae426082"
)

RESULTS: list[dict[str, Any]] = []


def record(name: str, question: str, expected: str, actual: str, ok: bool,
           detail: str = "") -> None:
    RESULTS.append({
        "scenario": name, "question": question, "expected": expected,
        "actual": actual, "ok": bool(ok), "detail": detail[:300],
    })
    mark = "✓" if ok else "✗"
    print(f"  {mark} {name}: ожидалось {expected}, получено {actual}")
    if detail:
        print(f"      {detail[:200]}")


def adapter(root: Path, *, executable: Path | None, ambient: Path | None = None,
            timeout: float = 60.0) -> CodexProviderAdapter:
    home = ambient or (root / "ambient")
    (home / ".codex").mkdir(parents=True, exist_ok=True)
    return CodexProviderAdapter(
        ProviderHome(provider="codex", root=root / "prov",
                     auth_mode=AUTH_MODE_AMBIENT_USER, ambient_home=home),
        executable=executable, timeout_sec=timeout, inference_allowed=True,
    )


def stub(root: Path, *, model: str = "gpt-5.6-sol", call_log: Path | None = None) -> Path:
    binary = provider_bridge_stub.materialize(root / "stub", provider="codex")
    wrapper = binary.parent / "codex-with-env"
    lines = ["#!/bin/sh", f'AUDIT_PROVIDER_STUB_MODEL="{model}"',
             "export AUDIT_PROVIDER_STUB_MODEL"]
    if call_log is not None:
        lines += [f'AUDIT_PROVIDER_STUB_CALL_LOG="{call_log}"',
                  "export AUDIT_PROVIDER_STUB_CALL_LOG"]
    lines.append(f'exec "{binary}" "$@"')
    wrapper.write_text("\n".join(lines) + "\n", encoding="utf-8")
    wrapper.chmod(0o700)
    return wrapper


def script(path: Path, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


def binding(job_dir: Path, *, executable: Path, model: str = "gpt-5.6-sol",
            accepted: tuple[str, ...] = ("gpt-5.6-sol",),
            stages: tuple[str, ...] = ("block_analysis", "text_analysis"),
            max_inferences: int = 5, grant: str = "grant-11h") -> ProviderBinding:
    payload = {
        "schema_version": 1, "provider": "codex", "auth_mode": AUTH_MODE_AMBIENT_USER,
        "provider_root": str(job_dir / "providers" / "codex"),
        "executable": str(executable), "timeout_sec": 60.0,
        "job_id": "job-11h", "task_id": "job-11h", "attempt_id": "att-11h",
        "allowed_stages": list(stages), "max_inferences": max_inferences,
        "grant_id": grant, "model": model,
        "accepted_reported_models": list(accepted),
        "capability": "strong_audit", "forbidden_literals": [],
    }
    path = job_dir / "metadata" / "provider_binding.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    os.environ["AUDIT_WORKER_PROVIDER_BINDING"] = str(path)
    return ProviderBinding.read(path)


# ═════════════ Сценарии ══════════════════════════════════════════════════════

def s_not_logged_in(root: Path) -> None:
    exe = script(root / "bin" / "codex", '#!/bin/sh\n'
                 'case "$1 $2" in "login status") echo "Not logged in"; exit 1;; esac\n'
                 'echo "auth required" >&2; exit 1\n')
    result = adapter(root, executable=exe).structured_inference(
        "задача", purpose="text_analysis", model="gpt-5.6-sol",
        accepted_reported_models=("gpt-5.6-sol",))
    record("codex_not_logged_in", "вход не выполнен",
           "отказ вызова", result.error_code or "ok", not result.ok, result.detail or "")


def s_cli_missing(root: Path) -> None:
    result = adapter(root, executable=root / "нет-такого-файла").structured_inference(
        "задача", purpose="text_analysis", model="gpt-5.6-sol",
        accepted_reported_models=("gpt-5.6-sol",))
    record("cli_missing", "бинаря нет по пути привязки",
           errors.ERR_CLI_MISSING, result.error_code or "ok",
           result.error_code == errors.ERR_CLI_MISSING, result.detail or "")


def s_unsupported_capability(root: Path) -> None:
    from audit_worker.providers import model_policy

    policy = model_policy.parse_policy({
        "policy_version": 1,
        "codex": {"auth_mode": "ambient_user",
                  "capabilities": {"strong_audit": {"model": "gpt-5.6-sol"}}},
    })
    try:
        policy.resolve("codex", "fast_triage")
        record("unsupported_capability", "центр просит неизвестную способность",
               "отказ политики", "прошло", False)
    except model_policy.ProviderPolicyError as exc:
        record("unsupported_capability", "центр просит неизвестную способность",
               "отказ политики", "отказ политики", True, str(exc))


def s_unsupported_stage(root: Path) -> None:
    job = root / "job"
    exe = stub(root)
    bnd = binding(job, executable=exe, stages=("text_analysis",))
    try:
        pipeline_bridge.run_stage_inference(
            job_dir=job, stage="optimization", prompt="задача", binding=bnd)
        record("unsupported_stage", "этап вне белого списка привязки",
               "исключение моста", "прошло", False)
    except pipeline_bridge.ProviderBridgeError as exc:
        record("unsupported_stage", "этап вне белого списка привязки",
               "исключение моста", "исключение моста", True, str(exc))


def s_malformed_output(root: Path) -> None:
    exe = script(root / "bin" / "codex", '#!/bin/sh\n'
                 'case "$1 $2" in "login status") echo ok; exit 0;; esac\n'
                 'cat >/dev/null\n'
                 'echo \'{"type":"codex.thread.started","thread":{"model":"gpt-5.6-sol"}}\'\n'
                 'echo \'{"type":"item.completed","item":{"type":"agent_message",'
                 '"text":"это не JSON, а рассуждение"}}\'\n'
                 'exit 0\n')
    result = adapter(root, executable=exe).structured_inference(
        "задача", purpose="text_analysis", model="gpt-5.6-sol",
        accepted_reported_models=("gpt-5.6-sol",))
    record("malformed_structured_output", "модель ответила не JSON-объектом",
           errors.ERR_MALFORMED_STATUS, result.error_code or "ok",
           result.error_code == errors.ERR_MALFORMED_STATUS, result.detail or "")


def s_timeout(root: Path) -> None:
    exe = script(root / "bin" / "codex", '#!/bin/sh\ncat >/dev/null\nsleep 30\n')
    result = adapter(root, executable=exe, timeout=2.0).structured_inference(
        "задача", purpose="text_analysis", model="gpt-5.6-sol",
        accepted_reported_models=("gpt-5.6-sol",), timeout_sec=2.0)
    record("timeout", "CLI не ответил в срок", errors.ERR_TIMEOUT,
           result.error_code or "ok", result.error_code == errors.ERR_TIMEOUT,
           result.detail or "")


def s_nonzero_exit(root: Path) -> None:
    exe = script(root / "bin" / "codex", '#!/bin/sh\ncat >/dev/null\n'
                 'echo "stream error: rate limit exceeded" >&2\nexit 7\n')
    result = adapter(root, executable=exe).structured_inference(
        "задача", purpose="text_analysis", model="gpt-5.6-sol",
        accepted_reported_models=("gpt-5.6-sol",))
    ok = (not result.ok) and "rate limit exceeded" in (result.detail or "")
    record("nonzero_exit_keeps_error_text", "CLI вышел с ненулевым кодом",
           "отказ + СОХРАНЁННЫЙ текст ошибки",
           f"{result.error_code}, текст {'сохранён' if 'rate limit' in (result.detail or '') else 'потерян'}",
           ok, result.detail or "")


def s_model_mismatch(root: Path) -> None:
    exe = stub(root, model="gpt-4.1-mini")
    result = adapter(root, executable=exe).structured_inference(
        "задача", purpose="text_analysis", model="gpt-5.6-sol",
        accepted_reported_models=("gpt-5.6-sol",))
    record("model_mismatch", "ответила другая модель", errors.ERR_MODEL_MISMATCH,
           result.error_code or "ok", result.error_code == errors.ERR_MODEL_MISMATCH,
           result.detail or "")


def s_model_not_reported(root: Path) -> None:
    exe = script(root / "bin" / "codex", '#!/bin/sh\ncat >/dev/null\n'
                 'echo \'{"type":"item.completed","item":{"type":"agent_message",'
                 '"text":"{\\"findings\\": []}"}}\'\nexit 0\n')
    result = adapter(root, executable=exe).structured_inference(
        "задача", purpose="text_analysis", model="gpt-5.6-sol",
        accepted_reported_models=("gpt-5.6-sol",))
    record("model_not_reported", "CLI не назвал модель вовсе",
           errors.ERR_MODEL_MISMATCH, result.error_code or "ok",
           result.error_code == errors.ERR_MODEL_MISMATCH, result.detail or "")


def s_missing_attachment(root: Path) -> None:
    exe = stub(root)
    result = adapter(root, executable=exe).structured_inference_multimodal(
        "опиши", images=[], purpose="block_analysis", model="gpt-5.6-sol",
        accepted_reported_models=("gpt-5.6-sol",))
    record("missing_image_attachment", "мультимодальный вызов без изображения",
           "отказ без обращения к модели",
           "отказ" if not result.ok else "прошло", not result.ok, result.detail or "")


def s_attachment_unreadable(root: Path) -> None:
    """Вложение не открылось процессом CLI — этап обязан упасть, а не «ответить»."""
    binary = provider_bridge_stub.materialize(root / "stub2", provider="codex")
    wrapper = script(root / "bin" / "codex-unreadable", "#!/bin/sh\n"
                     'case "$1 $2" in "login status") echo ok; exit 0;; esac\n'
                     "# вырезаем вложение из argv: CLI получает флаг на файл,\n"
                     "# которого нет — ровно то, что случится при гонке очистки\n"
                     "args=\"\"\n"
                     'for a in "$@"; do case "$a" in --image=*) a="--image=/nonexistent/x.png";; esac\n'
                     ' args="$args \\"$a\\""; done\n'
                     f'eval exec "{binary}" $args\n')
    result = adapter(root, executable=wrapper).structured_inference_multimodal(
        "опиши", images=[("image/png", PNG_1X1)], purpose="block_analysis",
        model="gpt-5.6-sol", accepted_reported_models=("gpt-5.6-sol",))
    record("attachment_unreadable", "CLI не смог открыть вложение",
           "отказ вызова", "отказ" if not result.ok else "прошло",
           not result.ok, result.detail or "")


def s_attachment_hash_mismatch(root: Path) -> None:
    """Сверка sha256 записанного вложения стоит ДО запуска CLI."""
    import hashlib

    import audit_worker.providers.codex_adapter as mod

    original = hashlib.sha256
    calls = {"n": 0}

    def fake_sha256(data=b""):
        calls["n"] += 1
        # Второй вызов — это хэш ПРОЧИТАННОГО файла: подменяем его, имитируя
        # расхождение байтов на диске с переданными.
        if calls["n"] == 2:
            return original("чужие байты".encode("utf-8"))
        return original(data)

    exe = stub(root)
    mod.hashlib = type("m", (), {"sha256": staticmethod(fake_sha256)})()
    try:
        result = adapter(root, executable=exe).structured_inference_multimodal(
            "опиши", images=[("image/png", PNG_1X1)], purpose="block_analysis",
            model="gpt-5.6-sol", accepted_reported_models=("gpt-5.6-sol",))
    finally:
        mod.hashlib = hashlib
    ok = (not result.ok) and "хэш" in (result.detail or "")
    record("attachment_hash_mismatch", "записанное вложение не совпало с переданным",
           "отказ до обращения к модели",
           "отказ" if ok else (result.error_code or "прошло"), ok, result.detail or "")


def s_replay(root: Path) -> None:
    """Повтор того же вызова отдаёт СОХРАНЁННЫЙ результат, а не второй платный."""
    job = root / "job_replay"
    log = root / "replay_calls.jsonl"
    exe = stub(root, call_log=log)
    bnd = binding(job, executable=exe)
    first = pipeline_bridge.run_stage_inference(
        job_dir=job, stage="text_analysis", prompt="одна и та же задача", binding=bnd)
    second = pipeline_bridge.run_stage_inference(
        job_dir=job, stage="text_analysis", prompt="одна и та же задача", binding=bnd)
    calls = [c for c in provider_bridge_stub.read_call_log(log) if c.get("kind") == "inference"]
    ok = first.performed and not second.performed and len(calls) == 1
    record("attempt_replay", "тот же вызов повторён",
           "второй раз модель НЕ зовётся", f"обращений к CLI: {len(calls)}",
           ok, f"performed: {first.performed} → {second.performed}")


def s_duplicate_claim(root: Path) -> None:
    """Заявка есть, результата нет — автоматический повтор запрещён (I-P9)."""
    job = root / "job_dup"
    exe = stub(root)
    bnd = binding(job, executable=exe)
    ledger = InferenceLedger(job, attempt_id=bnd.attempt_id, job_id=bnd.job_id)
    from audit_worker.providers.inference_ledger import call_key

    key = call_key(attempt_id=bnd.attempt_id, provider="codex",
                   purpose="text_analysis", prompt="брошенная задача")
    ledger.begin(key, provider="codex", purpose="text_analysis", prompt_sha256="x")
    ledger.mark_indeterminate(key, reason="процесс убит между «ответили» и «сохранили»")
    try:
        pipeline_bridge.run_stage_inference(
            job_dir=job, stage="text_analysis", prompt="брошенная задача", binding=bnd)
        record("duplicate_claim_indeterminate", "исход прошлой попытки неизвестен",
               "отказ без повтора", "прошло", False)
    except pipeline_bridge.ProviderBridgeError as exc:
        record("duplicate_claim_indeterminate", "исход прошлой попытки неизвестен",
               "отказ без повтора", "отказ без повтора", True, str(exc))


def s_budget_exhausted(root: Path) -> None:
    job = root / "job_budget"
    exe = stub(root)
    bnd = binding(job, executable=exe, max_inferences=1)
    pipeline_bridge.run_stage_inference(
        job_dir=job, stage="text_analysis", prompt="первая", binding=bnd)
    try:
        pipeline_bridge.run_stage_inference(
            job_dir=job, stage="text_analysis", prompt="вторая", binding=bnd)
        record("budget_exhausted", "потолок вызовов попытки исчерпан",
               "отказ моста", "прошло", False)
    except pipeline_bridge.ProviderBridgeError as exc:
        record("budget_exhausted", "потолок вызовов попытки исчерпан",
               "отказ моста", "отказ моста", True, str(exc))


def s_no_grant(root: Path) -> None:
    job = root / "job_nogrant"
    exe = stub(root)
    bnd = binding(job, executable=exe, grant="")
    try:
        pipeline_bridge.run_stage_inference(
            job_dir=job, stage="text_analysis", prompt="задача", binding=bnd)
        record("no_grant", "в привязке нет разрешения оператора",
               "отказ моста", "прошло", False)
    except pipeline_bridge.ProviderBridgeError as exc:
        record("no_grant", "в привязке нет разрешения оператора",
               "отказ моста", "отказ моста", True, str(exc))


def s_binding_missing_file(root: Path) -> None:
    """Переменная есть, файла нет — это ошибка развёртывания, а не «моста нет»."""
    os.environ["AUDIT_WORKER_PROVIDER_BINDING"] = str(root / "нет" / "binding.json")
    try:
        pipeline_bridge.active()
        record("binding_env_without_file", "путь привязки задан, файла нет",
               "исключение", "вернул False", False)
    except pipeline_bridge.ProviderBridgeError as exc:
        record("binding_env_without_file", "путь привязки задан, файла нет",
               "исключение", "исключение", True, str(exc))
    finally:
        os.environ.pop("AUDIT_WORKER_PROVIDER_BINDING", None)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="")
    args = parser.parse_args()

    scenarios = [
        s_not_logged_in, s_cli_missing, s_unsupported_capability, s_unsupported_stage,
        s_malformed_output, s_timeout, s_nonzero_exit, s_model_mismatch,
        s_model_not_reported, s_missing_attachment, s_attachment_unreadable,
        s_attachment_hash_mismatch, s_replay, s_duplicate_claim, s_budget_exhausted,
        s_no_grant, s_binding_missing_file,
    ]
    print("═" * 72)
    print("11H — матрица отказов Codex (обращений к настоящей модели: 0)")
    print("═" * 72)
    for scenario in scenarios:
        root = Path(tempfile.mkdtemp(prefix="11h_fail_"))
        try:
            scenario(root)
        except Exception as exc:                              # noqa: BLE001
            record(scenario.__name__, "сценарий выполнен", "результат",
                   f"исключение {type(exc).__name__}", False, str(exc))
        finally:
            os.environ.pop("AUDIT_WORKER_PROVIDER_BINDING", None)
            shutil.rmtree(root, ignore_errors=True)

    passed = sum(1 for r in RESULTS if r["ok"])
    print("─" * 72)
    print(f"ИТОГ: {passed}/{len(RESULTS)}")
    payload = {
        "stage": "11H",
        "question": "как Codex-путь ведёт себя в отказных сценариях",
        "real_model_calls": 0,
        "passed": passed, "total": len(RESULTS),
        "scenarios": RESULTS,
    }
    if args.out:
        Path(args.out).write_text(
            json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"отчёт: {args.out}")
    return 0 if passed == len(RESULTS) else 1


if __name__ == "__main__":
    raise SystemExit(main())
