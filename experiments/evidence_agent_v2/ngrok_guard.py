"""EV2 ngrok-guard — координация доступа к локальным моделям LM Studio.

Андрей Иванович: «следи, чтоб у вас не пересеклись тесты на локальных моделях
через ngrok — если оба запустите по модели, они не выдержат».

Что делает guard:
  1. Сериализует МОИ запросы (file-lock) — EV2 никогда не шлёт два запроса разом.
  2. Снимок загруженных в LM Studio моделей — перед прогоном видно, не висит ли
     уже модель (например, оставленная прогоном Cursor).
  3. Кооперативный lock-файл с TTL: если и Cursor возьмёт тот же файл — оба
     сериализуются. Путь печатается, чтобы можно было попросить Cursor его уважать.

Guard НЕ может технически запретить Cursor слать запросы (другой процесс). Поэтому
финальная страховка — ручное подтверждение оператора перед тяжёлым бенчмарком.
"""
from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[2]
LOCK_FILE = ROOT / "knowledge_base" / ".local_llm_ngrok.lock"
LOCK_TTL_SEC = 900  # протухший lock старше 15 мин игнорируем


def loaded_models_snapshot() -> list[dict]:
    """Список загруженных в LM Studio инстансов (read-only). [] при ошибке.

    Работает и вне, и внутри уже запущенного event loop (в последнем случае —
    отдельный поток со своим loop'ом, чтобы не падать на вложенном asyncio.run)."""
    try:
        from backend.app.services.common.local_vision_provider import (
            snapshot_loaded_models,
        )
    except Exception:
        return []

    def _run() -> list[dict]:
        return asyncio.run(snapshot_loaded_models())

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        try:
            return _run()
        except Exception:
            return []
    # уже внутри loop — выполняем в отдельном потоке
    import concurrent.futures
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
            return ex.submit(_run).result(timeout=40)
    except Exception:
        return []


def print_loaded(label: str = "") -> list[dict]:
    snap = loaded_models_snapshot()
    keys = [s.get("model_key") for s in snap]
    print(f"[ngrok-guard] {label} loaded LM Studio instances: {keys or 'none'}")
    return snap


def _read_lock() -> Optional[dict]:
    if not LOCK_FILE.is_file():
        return None
    try:
        raw = LOCK_FILE.read_text(encoding="utf-8").strip().split("|")
        return {"owner": raw[0], "ts": float(raw[1]), "note": raw[2] if len(raw) > 2 else ""}
    except Exception:
        return None


def lock_status() -> dict:
    info = _read_lock()
    if not info:
        return {"held": False}
    age = time.time() - info["ts"]
    return {
        "held": age < LOCK_TTL_SEC,
        "stale": age >= LOCK_TTL_SEC,
        "owner": info["owner"],
        "age_sec": round(age, 1),
        "note": info["note"],
    }


class LocalLLMLock:
    """Кооперативный файл-lock. EV2 берёт его на время серии ngrok-запросов.

    owner — кто держит ("ev2"). Если файл держит кто-то другой и не протух —
    ждём до timeout. acquire(wait=False) -> сразу вернёт False, если занято.
    """

    def __init__(self, owner: str = "ev2", note: str = "") -> None:
        self.owner = owner
        self.note = note
        self._held = False

    def acquire(self, *, wait: bool = True, timeout: float = 600.0, poll: float = 3.0) -> bool:
        deadline = time.time() + timeout
        while True:
            st = lock_status()
            if not st["held"] or st.get("owner") == self.owner:
                LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
                LOCK_FILE.write_text(
                    f"{self.owner}|{time.time()}|{self.note}", encoding="utf-8"
                )
                self._held = True
                return True
            if not wait or time.time() > deadline:
                return False
            print(f"[ngrok-guard] lock занят ({st.get('owner')}, age={st.get('age_sec')}s), жду…")
            time.sleep(poll)

    def release(self) -> None:
        if self._held and LOCK_FILE.is_file():
            info = _read_lock()
            if info and info["owner"] == self.owner:
                try:
                    LOCK_FILE.unlink()
                except OSError:
                    pass
        self._held = False

    def __enter__(self) -> "LocalLLMLock":
        if not self.acquire(wait=True):
            raise RuntimeError("Не удалось взять local-LLM lock (занят другим агентом)")
        return self

    def __exit__(self, *exc) -> None:
        self.release()


def preflight(require_idle: bool = False) -> dict:
    """Печатает состояние перед прогоном; при require_idle падает, если что-то висит."""
    snap = print_loaded("preflight")
    st = lock_status()
    print(f"[ngrok-guard] lock: {st}")
    busy = bool(snap)
    if require_idle and (busy or (st["held"] and st.get("owner") != "ev2")):
        raise RuntimeError(
            "ngrok-guard: endpoint занят (loaded models или чужой lock). "
            "Убедись, что Cursor не гоняет ngrok, и повтори."
        )
    return {"loaded": [s.get("model_key") for s in snap], "lock": st}
