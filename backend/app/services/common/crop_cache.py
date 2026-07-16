"""Скачивание и локальный кэш кропов блоков портала vibe.

Решение Андрея Ивановича (13-14.07.2026): кропы качать ВСЕ сразу при загрузке
проекта и кэшировать локально — crop-токены живут per-generation (при
пере-генерации документа на портале ВСЕ ссылки меняются), поэтому качать
лениво «когда понадобится» нельзя.

Факты о кропах (пробы 16.07, см. ОБСЛЕДОВАНИЕ_выгрузка_2026-07-16.md):
- каждый кроп — одностраничный PDF (не PNG!), отдаётся без auth;
- `cache-control: immutable` — скачанное можно хранить вечно;
- битый/чужой токен → HTTP 403 с JSON-ошибкой (не 404);
- размеры от ~130 КБ до ~15 МБ (полигоны с 600 DPI подложкой), средний
  документ — десятки-сотни МБ суммарно → лимиты обязательны;
- у stamp-блоков crop_url = null — качать нечего.

Модуль fail-soft: сетевые ошибки фиксируются в манифесте, наружу не летят —
загрузка проекта не должна падать из-за недоступности портала. Повторный
вызов докачивает только недостающее (идемпотентность по файлам на диске).
"""
from __future__ import annotations

import hashlib
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Callable, Optional

CROPS_DIRNAME = "crops"
MANIFEST_NAME = "crops_manifest.json"

# Гарды (разброс реальных кропов 128 КБ – 14.8 МБ; потолок с запасом)
MAX_FILE_BYTES = 64 * 1024 * 1024          # 64 МБ на кроп
MAX_TOTAL_BYTES = 2 * 1024 * 1024 * 1024   # 2 ГБ на документ
DEFAULT_TIMEOUT = 60
DEFAULT_CONCURRENCY = 4


def _default_fetch(url: str, timeout: int) -> tuple[int, bytes]:
    """GET url → (http_status, body). Без auth (кропы публичны по токену)."""
    import urllib.error
    import urllib.request
    req = urllib.request.Request(url, headers={"User-Agent": "pdf-proverka-crop-cache/1"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return int(resp.status), resp.read(MAX_FILE_BYTES + 1)
    except urllib.error.HTTPError as e:
        return int(e.code), b""


def crop_filename(block_id: str) -> str:
    """Имя файла кэша: кропы портала — PDF (не PNG)."""
    return f"{block_id}.pdf"


def download_crops(
    blocks_data: dict,
    dest_dir,
    *,
    fetch: Optional[Callable[[str, int], tuple[int, bytes]]] = None,
    timeout: int = DEFAULT_TIMEOUT,
    concurrency: int = DEFAULT_CONCURRENCY,
    max_total_bytes: int = MAX_TOTAL_BYTES,
) -> dict:
    """Скачать все кропы из blocks.json в dest_dir/crops/, вернуть манифест.

    Идемпотентно: уже скачанные файлы (size > 0) не перекачиваются — токены
    immutable. Никогда не бросает; итог в манифесте (ok/cached/error/skipped).
    Манифест пишется в dest_dir/crops_manifest.json.
    """
    fetch = fetch or _default_fetch
    dest = Path(dest_dir)
    crops_dir = dest / CROPS_DIRNAME
    entries: list[dict] = []
    started = time.time()

    targets: list[tuple[str, str]] = []
    for blk in blocks_data.get("blocks", []) if isinstance(blocks_data, dict) else []:
        bid = blk.get("block_id") or ""
        url = blk.get("crop_url") or ""
        if not bid:
            continue
        if not url:
            # штампы (crop_url = null) и прочие блоки без кропа
            entries.append({"block_id": bid, "status": "skipped", "reason": "no_crop_url"})
            continue
        targets.append((bid, url))

    total_bytes = 0
    lock_entries: list[dict] = []

    def _one(bid: str, url: str) -> dict:
        fname = crop_filename(bid)
        fpath = crops_dir / fname
        if fpath.is_file() and fpath.stat().st_size > 0:
            return {"block_id": bid, "status": "cached", "file": fname,
                    "bytes": fpath.stat().st_size}
        try:
            status, body = fetch(url, timeout)
        except Exception as e:
            return {"block_id": bid, "status": "error", "reason": f"fetch: {e}"}
        if status != 200:
            return {"block_id": bid, "status": "error", "reason": f"http_{status}"}
        if not body:
            return {"block_id": bid, "status": "error", "reason": "empty_body"}
        if len(body) > MAX_FILE_BYTES:
            return {"block_id": bid, "status": "error", "reason": "too_large"}
        try:
            crops_dir.mkdir(parents=True, exist_ok=True)
            fpath.write_bytes(body)
        except OSError as e:
            return {"block_id": bid, "status": "error", "reason": f"write: {e}"}
        return {"block_id": bid, "status": "ok", "file": fname, "bytes": len(body),
                "sha256": hashlib.sha256(body).hexdigest()}

    if targets:
        with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
            futures = {}
            for bid, url in targets:
                if total_bytes >= max_total_bytes:
                    lock_entries.append({"block_id": bid, "status": "error",
                                         "reason": "total_budget_exceeded"})
                    continue
                futures[pool.submit(_one, bid, url)] = bid
            for fut in as_completed(futures):
                rec = fut.result()
                total_bytes += int(rec.get("bytes") or 0)
                lock_entries.append(rec)

    entries.extend(sorted(lock_entries, key=lambda r: r["block_id"]))
    counts: dict[str, int] = {}
    for rec in entries:
        counts[rec["status"]] = counts.get(rec["status"], 0) + 1
    manifest = {
        "schema_version": 1,
        "generated_at_epoch": int(started),
        "duration_sec": round(time.time() - started, 1),
        "document_id": blocks_data.get("document_id") if isinstance(blocks_data, dict) else None,
        "blocks_json_generated_at": blocks_data.get("generated_at") if isinstance(blocks_data, dict) else None,
        "counts": counts,
        "total_bytes": total_bytes,
        "entries": entries,
    }
    try:
        dest.mkdir(parents=True, exist_ok=True)
        (dest / MANIFEST_NAME).write_text(
            json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8"
        )
    except OSError:
        pass
    return manifest


def ensure_crops_for_version(version_dir, *, background: bool = True) -> Optional[dict]:
    """Скачать кропы комплекта версии в 01_input/crops/ (кэш при загрузке).

    Источник — 01_input/*_blocks.json (или 02_work/blocks.json). Идемпотентно
    и fail-soft; повторный вызов докачивает недостающее. background=True —
    работа в daemon-потоке (загрузка проекта не ждёт сотни МБ кропов и
    event-loop не блокируется), возврат None. Отключение:
    AUDIT_CROP_CACHE_ON_UPLOAD=0.
    """
    import os
    if os.environ.get("AUDIT_CROP_CACHE_ON_UPLOAD", "1").strip().lower() in ("0", "false", "off"):
        return None

    version_dir = Path(version_dir)

    def _run() -> Optional[dict]:
        try:
            inp = version_dir / "01_input"
            candidates = sorted(inp.glob("*_blocks.json")) if inp.is_dir() else []
            if not candidates:
                work_bj = version_dir / "02_work" / "blocks.json"
                candidates = [work_bj] if work_bj.is_file() else []
            if not candidates:
                return None
            blocks_data = json.loads(candidates[0].read_text(encoding="utf-8"))
            if not isinstance(blocks_data, dict):
                return None
            if crops_complete(inp, blocks_data):
                return None
            return download_crops(blocks_data, inp)
        except Exception:
            return None

    if background:
        import threading
        threading.Thread(target=_run, name="crop-cache-download", daemon=True).start()
        return None
    return _run()


def crops_complete(dest_dir, blocks_data: dict) -> bool:
    """Все кропы с crop_url уже лежат в кэше dest_dir/crops/?"""
    crops_dir = Path(dest_dir) / CROPS_DIRNAME
    for blk in blocks_data.get("blocks", []) if isinstance(blocks_data, dict) else []:
        if not blk.get("crop_url"):
            continue
        f = crops_dir / crop_filename(blk.get("block_id") or "")
        if not (f.is_file() and f.stat().st_size > 0):
            return False
    return True
