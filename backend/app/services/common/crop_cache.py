"""Локальный кэш кропов блоков портала vibe: вырезка из PDF или скачивание.

Решение Андрея Ивановича (13-14.07.2026): кропы получать ВСЕ сразу при
загрузке проекта и кэшировать локально — crop-токены живут per-generation
(при пере-генерации документа на портале ВСЕ ссылки меняются), поэтому
качать лениво «когда понадобится» нельзя.

Два источника кропов (env AUDIT_CROP_CACHE_SOURCE):
- ``local_pdf`` (по умолчанию, решение АИ 16.07.2026) — временная заглушка:
  портал для большинства дисциплин отдаёт кропы растром 300 DPI, поэтому
  режем блоки сами из исходного PDF комплекта по page_index + coords_norm
  из blocks.json (см. pdf_crop.py) — кроп сохраняет вектор-слой. Блоки, у
  которых вырезка не удалась, докачиваются по crop_url (фолбэк).
- ``download`` — прежнее поведение: скачивание всех кропов по crop_url.
  Вернуть, когда портал начнёт отдавать вектор для всех документов.

Факты о кропах портала (пробы 16.07, см. ОБСЛЕДОВАНИЕ_выгрузка_2026-07-16.md):
- каждый кроп — одностраничный PDF (не PNG!), отдаётся без auth;
- `cache-control: immutable` — скачанное можно хранить вечно;
- битый/чужой токен → HTTP 403 с JSON-ошибкой (не 404);
- размеры от ~130 КБ до ~15 МБ (полигоны с 600 DPI подложкой), средний
  документ — десятки-сотни МБ суммарно → лимиты обязательны;
- у stamp-блоков crop_url = null — кроп не создаётся (паритет источников).

Модуль fail-soft: ошибки фиксируются в манифесте, наружу не летят —
загрузка проекта не должна падать из-за недоступности портала или брака
геометрии. Повторный вызов докачивает/дорезает только недостающее
(идемпотентность по файлам на диске).
"""
from __future__ import annotations

import hashlib
import json
import os
import threading
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

# Источник кропов: local_pdf (вырезка из PDF) | download (по crop_url)
CROP_SOURCE_ENV = "AUDIT_CROP_CACHE_SOURCE"
MODE_LOCAL = "local_pdf"
MODE_DOWNLOAD = "download"


def crop_source_mode() -> str:
    """Режим источника кропов из env (default — локальная вырезка)."""
    import os
    val = (os.environ.get(CROP_SOURCE_ENV) or "").strip().lower()
    return MODE_DOWNLOAD if val in (MODE_DOWNLOAD, "portal", "url") else MODE_LOCAL


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
    """Имя файла кэша: кропы (и портала, и локальные) — PDF (не PNG)."""
    return f"{block_id}.pdf"


def _collect_targets(blocks_data: dict) -> tuple[list[dict], list[dict]]:
    """blocks.json → (таргеты с crop_url, skipped-записи для остальных).

    Паритет с порталом: кроп создаётся ровно для блоков с crop_url
    (штампы crop_url=null — кропа нет ни при скачивании, ни при вырезке).
    """
    targets: list[dict] = []
    skipped: list[dict] = []
    for blk in blocks_data.get("blocks", []) if isinstance(blocks_data, dict) else []:
        bid = blk.get("block_id") or ""
        if not bid:
            continue
        if not blk.get("crop_url"):
            skipped.append({"block_id": bid, "status": "skipped", "reason": "no_crop_url"})
            continue
        targets.append({
            "block_id": bid,
            "crop_url": blk.get("crop_url") or "",
            "page_index": blk.get("page_index"),
            "coords_norm": blk.get("coords_norm"),
        })
    return targets, skipped


def _cached_entry(crops_dir: Path, bid: str) -> Optional[dict]:
    fpath = crops_dir / crop_filename(bid)
    if fpath.is_file() and fpath.stat().st_size > 0:
        return {"block_id": bid, "status": "cached", "file": fpath.name,
                "bytes": fpath.stat().st_size}
    return None


def _download_one(bid: str, url: str, crops_dir: Path,
                  fetch: Callable[[str, int], tuple[int, bytes]], timeout: int) -> dict:
    fname = crop_filename(bid)
    fpath = crops_dir / fname
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
    # запись атомарна (tmp + os.replace): прибитый процесс/ENOSPC не должны
    # оставить огрызок под целевым именем — _cached_entry принял бы его
    # за валидный кэш навсегда
    tmp = fpath.with_name(fpath.name + f".{os.getpid()}-{threading.get_ident()}.tmp")
    try:
        crops_dir.mkdir(parents=True, exist_ok=True)
        tmp.write_bytes(body)
        os.replace(tmp, fpath)
    except OSError as e:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        return {"block_id": bid, "status": "error", "reason": f"write: {e}"}
    return {"block_id": bid, "status": "ok", "source": "portal", "file": fname,
            "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}


def _download_entries(
    targets: list[dict],
    crops_dir: Path,
    fetch: Callable[[str, int], tuple[int, bytes]],
    timeout: int,
    concurrency: int,
    max_total_bytes: int,
    spent_bytes: int = 0,
) -> tuple[list[dict], int]:
    """Параллельное скачивание таргетов; кэш на диске не перекачивается.

    Возвращает (записи, байт на диске по этим таргетам — включая cached:
    бюджет max_total_bytes считается по фактическому объёму кэша документа,
    а не по скачанному в этом прогоне). Бюджет проверяется между чанками
    размера concurrency — перерасход ограничен одним чанком.
    """
    entries: list[dict] = []
    total_bytes = spent_bytes
    pending: list[dict] = []
    for tgt in targets:
        cached = _cached_entry(crops_dir, tgt["block_id"])
        if cached is not None:
            entries.append(cached)
            total_bytes += int(cached.get("bytes") or 0)
            continue
        pending.append(tgt)

    if pending:
        chunk_size = max(1, concurrency)
        with ThreadPoolExecutor(max_workers=chunk_size) as pool:
            idx = 0
            while idx < len(pending):
                if total_bytes >= max_total_bytes:
                    for tgt in pending[idx:]:
                        entries.append({"block_id": tgt["block_id"], "status": "error",
                                        "reason": "total_budget_exceeded"})
                    break
                chunk = pending[idx:idx + chunk_size]
                idx += len(chunk)
                futures = {pool.submit(
                    _download_one, tgt["block_id"], tgt["crop_url"],
                    crops_dir, fetch, timeout,
                ): tgt["block_id"] for tgt in chunk}
                for fut in as_completed(futures):
                    rec = fut.result()
                    total_bytes += int(rec.get("bytes") or 0)
                    entries.append(rec)
    return entries, total_bytes - spent_bytes


def _local_entries(
    targets: list[dict],
    pdf_path: Path,
    crops_dir: Path,
    max_total_bytes: int,
) -> tuple[list[dict], list[dict], int]:
    """Последовательная вырезка таргетов из PDF (fitz не потокобезопасен).

    Возвращает (готовые записи, таргеты для фолбэка-скачивания с причиной
    в ключе local_reason, потрачено байт).
    """
    from backend.app.services.common.pdf_crop import (
        PdfCropError, extract_block_crop, open_pdf,
    )
    entries: list[dict] = []
    fallback: list[dict] = []
    total_bytes = 0

    try:
        doc = open_pdf(pdf_path)
    except PdfCropError as e:
        return [], [dict(t, local_reason=str(e)) for t in targets], 0

    try:
        for tgt in targets:
            bid = tgt["block_id"]
            cached = _cached_entry(crops_dir, bid)
            if cached is not None:
                entries.append(cached)
                total_bytes += int(cached.get("bytes") or 0)
                continue
            if total_bytes >= max_total_bytes:
                entries.append({"block_id": bid, "status": "error",
                                "reason": "total_budget_exceeded"})
                continue
            fpath = crops_dir / crop_filename(bid)
            try:
                page_index = tgt.get("page_index")
                size = extract_block_crop(
                    doc,
                    int(page_index) if page_index is not None else -1,
                    tgt.get("coords_norm"),
                    fpath,
                )
            except PdfCropError as e:
                fallback.append(dict(tgt, local_reason=str(e)))
                continue
            except Exception as e:  # неожиданный брак fitz — fail-soft в фолбэк
                fallback.append(dict(tgt, local_reason=f"unexpected: {e}"))
                continue
            if size > MAX_FILE_BYTES:
                try:
                    fpath.unlink(missing_ok=True)
                except OSError:
                    pass
                fallback.append(dict(tgt, local_reason="too_large"))
                continue
            total_bytes += size
            entries.append({
                "block_id": bid, "status": "ok", "source": MODE_LOCAL,
                "file": fpath.name, "bytes": size,
                "sha256": hashlib.sha256(fpath.read_bytes()).hexdigest(),
            })
    finally:
        try:
            doc.close()
        except Exception:
            pass
    return entries, fallback, total_bytes


def _write_manifest(dest: Path, blocks_data: dict, entries: list[dict],
                    started: float, total_bytes: int, extra: dict) -> dict:
    counts: dict[str, int] = {}
    for rec in entries:
        counts[rec["status"]] = counts.get(rec["status"], 0) + 1
    manifest = {
        "schema_version": 1,
        "generated_at_epoch": int(started),
        "duration_sec": round(time.time() - started, 1),
        "document_id": blocks_data.get("document_id") if isinstance(blocks_data, dict) else None,
        "blocks_json_generated_at": blocks_data.get("generated_at") if isinstance(blocks_data, dict) else None,
        **extra,
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


def cache_crops(
    blocks_data: dict,
    dest_dir,
    *,
    pdf_path=None,
    mode: Optional[str] = None,
    fetch: Optional[Callable[[str, int], tuple[int, bytes]]] = None,
    timeout: int = DEFAULT_TIMEOUT,
    concurrency: int = DEFAULT_CONCURRENCY,
    max_total_bytes: int = MAX_TOTAL_BYTES,
) -> dict:
    """Получить все кропы blocks.json в dest_dir/crops/, вернуть манифест.

    mode=local_pdf — вырезка из pdf_path с пофайловым фолбэком на скачивание;
    mode=download — только скачивание (прежнее поведение); mode=None — из env.
    Без pdf_path режим local_pdf деградирует в download (факт — в манифесте).
    Идемпотентно, никогда не бросает; итог в dest_dir/crops_manifest.json
    (ok/cached/error/skipped, у ok — source: local_pdf|portal; total_bytes =
    объём кэша по учтённым записям, включая cached).
    """
    fetch = fetch or _default_fetch
    mode = mode or crop_source_mode()
    dest = Path(dest_dir)
    crops_dir = dest / CROPS_DIRNAME
    started = time.time()

    # осиротевшие tmp-файлы прибитых процессов (записи атомарны: tmp+replace)
    if crops_dir.is_dir():
        for stale in crops_dir.glob("*.tmp"):
            try:
                stale.unlink()
            except OSError:
                pass

    targets, skipped = _collect_targets(blocks_data)
    extra: dict = {"crop_source_mode": mode}

    entries: list[dict] = list(skipped)
    total_bytes = 0

    if mode == MODE_LOCAL:
        pdf = Path(pdf_path) if pdf_path else None
        if pdf is None or not pdf.is_file():
            extra["local_unavailable"] = "no_pdf"
            dl_entries, dl_bytes = _download_entries(
                targets, crops_dir, fetch, timeout, concurrency, max_total_bytes)
            entries.extend(dl_entries)
            total_bytes += dl_bytes
        else:
            extra["pdf_file"] = pdf.name
            loc_entries, fb_targets, loc_bytes = _local_entries(
                targets, pdf, crops_dir, max_total_bytes)
            entries.extend(loc_entries)
            total_bytes += loc_bytes
            if fb_targets:
                dl_entries, dl_bytes = _download_entries(
                    fb_targets, crops_dir, fetch, timeout, concurrency,
                    max_total_bytes, spent_bytes=total_bytes)
                by_id = {t["block_id"]: t.get("local_reason") for t in fb_targets}
                for rec in dl_entries:
                    reason = by_id.get(rec["block_id"])
                    if reason:
                        rec["fallback_from_local"] = reason
                        if rec.get("status") == "error":
                            rec["reason"] = f"local: {reason}; {rec.get('reason')}"
                entries.extend(dl_entries)
                total_bytes += dl_bytes
    else:
        dl_entries, dl_bytes = _download_entries(
            targets, crops_dir, fetch, timeout, concurrency, max_total_bytes)
        entries.extend(dl_entries)
        total_bytes += dl_bytes

    entries.sort(key=lambda r: r["block_id"])
    return _write_manifest(dest, blocks_data, entries, started, total_bytes, extra)


def download_crops(
    blocks_data: dict,
    dest_dir,
    *,
    fetch: Optional[Callable[[str, int], tuple[int, bytes]]] = None,
    timeout: int = DEFAULT_TIMEOUT,
    concurrency: int = DEFAULT_CONCURRENCY,
    max_total_bytes: int = MAX_TOTAL_BYTES,
) -> dict:
    """Скачать все кропы по crop_url (прежний API; см. cache_crops)."""
    return cache_crops(
        blocks_data, dest_dir, mode=MODE_DOWNLOAD, fetch=fetch,
        timeout=timeout, concurrency=concurrency, max_total_bytes=max_total_bytes,
    )


def ensure_crops_for_version(version_dir, *, background: bool = True) -> Optional[dict]:
    """Получить кропы комплекта версии в 01_input/crops/ (кэш при загрузке).

    Источник геометрии — 01_input/*_blocks.json (или 02_work/blocks.json);
    PDF для локальной вырезки — 02_work/document.pdf (см. resolve_version_pdf).
    Идемпотентно и fail-soft; повторный вызов дорезает/докачивает недостающее.
    background=True — работа в daemon-потоке (загрузка проекта не ждёт сотни
    МБ кропов и event-loop не блокируется), возврат None. Отключение:
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
            from backend.app.services.common.pdf_crop import resolve_version_pdf
            return cache_crops(blocks_data, inp, pdf_path=resolve_version_pdf(version_dir))
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
