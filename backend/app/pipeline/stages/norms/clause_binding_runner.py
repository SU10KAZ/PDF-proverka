"""
clause_binding_runner.py
------------------------
Запуск нормативной привязки: модель называет пункт → база сверяет → ненайденное
возвращается модели на исправление.

Ядро метода (промпт, разбор, сверка, запись) живёт в `clause_binding.py` и не
знает, кто вызывает модель. Здесь — только исполнение: выбор провайдера по имени
модели и раунды.

Замер 06.08.2026, 20 замечаний КЖ-АС-02.1, у которых свод дал только документ:

    claude-opus-5   20/20 подтверждено базой, 1 раунд,  91 с
    codex/gpt-5.4   20/20 подтверждено базой, 2 раунда, 80 с (4 номера
                    исправлены после возврата «в этом ГОСТе нет такого пункта»)

До введения обязательности ответа те же модели давали 3/20 и 0/20: в промпте
было разрешение «не знаешь — не включай», и обе им пользовались. Отсюда правило,
которое стоит помнить при правке промптов конвейера: любая оговорка «можешь
промолчать» читается моделью как «молчать безопаснее». Точность обеспечивает
сверка с базой, а не запреты в тексте задачи.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Callable

from backend.app.pipeline.stages.norms import clause_binding as cb

CLAUSE_BINDING_TIMEOUT = 900
CLAUSE_BINDING_BATCH = 25


def is_enabled() -> bool:
    """Этап включается флагом; выключенный — не трогает findings вовсе."""
    return os.environ.get(
        "NORM_CLAUSE_BINDING_ENABLED", "false",
    ).strip().lower() in ("true", "1", "yes", "on")


def resolve_model() -> str:
    """Модель этапа: своя, иначе модель верификации норм."""
    from backend.app.core.config import get_stage_model

    explicit = os.environ.get("NORM_CLAUSE_BINDING_MODEL", "").strip()
    if explicit:
        return explicit
    return get_stage_model("norm_verify") or "claude-sonnet-5"


async def _ask_model(messages: list[dict], model: str, on_output=None) -> str:
    """Один вызов модели. Провайдер выбирается так же, как в остальных этапах."""
    from backend.app.services.llm.claude_runner import _run_cli
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    from backend.app.core.config import is_codex_model

    if is_codex_model(model):
        bare = model.split("/", 1)[1] if "/" in model else model
        result = await run_codex_json_messages(
            messages, timeout=CLAUSE_BINDING_TIMEOUT, on_output=on_output,
            stage="clause_binding", model=bare,
        )
        return result.text or ""

    task_text = messages[0]["content"] + "\n\n" + messages[1]["content"]
    _, _, cli_result = await _run_cli(
        task_text, "Read", CLAUSE_BINDING_TIMEOUT, on_output,
        stage="clause_binding", model=model, clean_cwd=True,
    )
    return getattr(cli_result, "result_text", "") or ""


async def bind_clauses(
    output_dir: Path,
    *,
    model: str | None = None,
    rounds: int = cb.DEFAULT_ROUNDS,
    log: Callable[..., Any] | None = None,
) -> dict:
    """Дать замечаниям без пункта ссылку с пунктом, подтверждённым базой.

    Ничего не портит при сбое: замечание, для которого пункт не подтвердился,
    остаётся ровно таким, каким его оставил свод.

    Returns:
        {"targets": N, "bound": M, "rejected": K, "rounds": R, "model": str}
    """
    from norms._native_verify import _import_norms_api, _resolve_norm_code

    async def _log(msg: str, level: str = "info"):
        if log:
            await log(msg, level)

    findings_path = output_dir / "03_findings.json"
    if not findings_path.exists():
        return {"targets": 0, "bound": 0, "rejected": 0, "rounds": 0, "model": ""}

    try:
        fd = json.loads(findings_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {"targets": 0, "bound": 0, "rejected": 0, "rounds": 0, "model": ""}

    findings = fd.get("findings", [])
    targets = cb.select_targets(findings)
    if not targets:
        return {"targets": 0, "bound": 0, "rejected": 0, "rounds": 0, "model": ""}

    resolved_model = model or resolve_model()
    await _log(
        f"Нормативная привязка: {len(targets)} замечаний без пункта, модель {resolved_model}"
    )

    api = _import_norms_api()
    accepted_all: dict[str, dict] = {}
    rejected: dict[str, str] = {}
    rounds_used = 0

    # Крупные проекты режем на пачки: один запрос на 200 замечаний модели не
    # осилить внимательно, а на 25 — вполне.
    for start in range(0, len(targets), CLAUSE_BINDING_BATCH):
        batch = targets[start:start + CLAUSE_BINDING_BATCH]
        pending, batch_rejected = batch, {}
        for rnd in range(1, rounds + 1):
            if not pending:
                break
            rounds_used = max(rounds_used, rnd)
            messages = cb.build_messages(pending, rejected=batch_rejected or None)
            try:
                text = await _ask_model(messages, resolved_model)
            except Exception as exc:  # noqa: BLE001 — привязка не должна ронять нормы
                await _log(f"Нормативная привязка: вызов модели упал ({exc})", "warn")
                break
            answers = cb.parse_answer(text)
            accepted, batch_rejected = cb.validate(answers, api, _resolve_norm_code)
            accepted_all.update(accepted)
            pending = [f for f in pending if str(f.get("id")) in batch_rejected]
        rejected.update(batch_rejected)

    bound = cb.apply(findings, accepted_all)
    if bound:
        findings_path.write_text(
            json.dumps(fd, ensure_ascii=False, indent=2), encoding="utf-8",
        )
    await _log(
        f"Нормативная привязка: пункт подтверждён базой у {bound} из {len(targets)} "
        f"замечаний (раундов: {rounds_used})"
    )
    if rejected:
        await _log(
            f"Нормативная привязка: {len(rejected)} замечаний остались без пункта — "
            "ссылка на документ сохранена",
            "warn",
        )
    return {
        "targets": len(targets),
        "bound": bound,
        "rejected": len(rejected),
        "rounds": rounds_used,
        "model": resolved_model,
    }
