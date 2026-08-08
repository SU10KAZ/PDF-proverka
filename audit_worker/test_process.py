"""Встроенный тестовый процесс `test_pipeline_v1`.

Запускается ТОЛЬКО воркером, фиксированным argv:
    <python> -u <путь к этому файлу> <путь к params.json>

Центр не участвует в построении команды: он не передаёт ни исполняемого файла,
ни аргументов, ни путей, ни переменных окружения (§4 задания). Единственный
вход — файл параметров, который воркер написал сам из уже провалидированных
скаляров.

Процесс печатает в stdout построчный JSONL с прогрессом и обычные текстовые
строки — на них воркер строит события stage_progress и log_line.

Последним действием процесс пишет `work/process_exit.json` — собственную
отметку «я закончил и вот с каким кодом». Она нужна потому, что наблюдатель
может не дожить до этого момента: если исполнитель перезапустят посреди
работы, единственный, кто достоверно знает исход, — сам процесс. Без этой
отметки готовая работа выглядела бы как «процесс исчез».
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

MAX_STEPS = 100
MAX_STEP_SECONDS = 10.0
MAX_RESULT_BYTES = 8 * 1024 * 1024


def emit(kind: str, **fields) -> None:
    payload = {"type": kind, **fields}
    sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: test_process.py <params.json>", file=sys.stderr)
        return 2

    params_path = Path(argv[1])
    try:
        params = json.loads(params_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        print(f"не удалось прочитать параметры: {exc}", file=sys.stderr)
        return 2
    exit_marker = params_path.parent / "process_exit.json"

    # Третий рубеж зажима: воркер уже проверил, центр уже проверил — но
    # процесс не обязан верить ни тому, ни другому.
    label = str(params.get("label", "smoke"))[:64]
    steps = max(1, min(MAX_STEPS, int(params.get("steps", 5))))
    step_seconds = max(0.0, min(MAX_STEP_SECONDS, float(params.get("step_seconds", 0.5))))
    result_bytes = max(0, min(MAX_RESULT_BYTES, int(params.get("result_bytes", 4096))))
    fail_at = params.get("fail_at_step")
    fail_at = int(fail_at) if fail_at not in (None, "") else None
    result_dir = Path(params["result_dir"])
    result_dir.mkdir(parents=True, exist_ok=True)

    started = time.time()
    emit("started", label=label, steps=steps, pid=os.getpid())
    print(f"[test_pipeline_v1] старт: label={label}, шагов={steps}")

    log_path = result_dir / "run_log.txt"
    with log_path.open("w", encoding="utf-8") as log:
        log.write(f"test_pipeline_v1 label={label} steps={steps}\n")
        for step in range(1, steps + 1):
            if step_seconds:
                time.sleep(step_seconds)
            if fail_at is not None and step == fail_at:
                emit("failed", step=step, message=f"смоделирован сбой на шаге {step}")
                print(f"[test_pipeline_v1] СБОЙ на шаге {step}", file=sys.stderr)
                log.write(f"FAILED at step {step}\n")
                _write_exit(exit_marker, 3, started, step, steps)
                return 3
            elapsed = time.time() - started
            emit(
                "progress",
                step=step,
                total=steps,
                elapsed_sec=round(elapsed, 3),
                message=f"шаг {step} из {steps} завершён",
            )
            print(f"[test_pipeline_v1] шаг {step}/{steps} готов")
            log.write(f"step {step}/{steps} ok at {elapsed:.3f}s\n")

    payload_path = result_dir / "payload.bin"
    if result_bytes:
        # Детерминированное содержимое: воспроизводимый sha256 у одинаковых
        # параметров — удобно для smoke-проверок.
        block = (f"{label}:".encode("utf-8") * 64)[:1024] or b"0" * 1024
        with payload_path.open("wb") as fh:
            written = 0
            while written < result_bytes:
                take = min(len(block), result_bytes - written)
                fh.write(block[:take])
                written += take

    duration = time.time() - started
    summary = {
        "job_label": label,
        "steps": steps,
        "step_seconds": step_seconds,
        "result_bytes": result_bytes,
        "duration_sec": round(duration, 3),
        "finished_at": time.time(),
        "status": "ok",
    }
    (result_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    emit("completed", duration_sec=round(duration, 3), steps=steps)
    print(f"[test_pipeline_v1] завершено за {duration:.2f} с")
    _write_exit(exit_marker, 0, started, steps, steps)
    return 0


def _write_exit(path: Path, code: int, started: float, done: int, total: int) -> None:
    """Отметка процесса о собственном завершении. Пишется атомарно.

    Читает её тот, кто наблюдает за процессом со стороны, — в том числе
    исполнитель, поднявшийся уже ПОСЛЕ старта этого процесса.
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(
            json.dumps(
                {
                    "exit_code": code,
                    "pid": os.getpid(),
                    "duration_sec": round(time.time() - started, 3),
                    "steps_done": done,
                    "steps_total": total,
                    "finished_at": time.time(),
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        os.replace(tmp, path)
    except OSError:
        # Отметка — страховка, а не условие работы. Её отсутствие означает
        # «исход неизвестен», и наблюдатель обязан сказать это честно.
        pass


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
