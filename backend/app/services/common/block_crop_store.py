"""Единая точка разрешения и эвакуации PNG-кропов блоков.

Отличать от ``crop_cache.py`` (кэш исходных вектор-PDF портала в
``01_input/crops/``) — здесь речь о готовых PNG в
``blocks_stage02_100`` / ``blocks_gemma_100`` / ``blocks_gemma_300`` / ``blocks``.

Зачем
-----
Кропы занимают 12.2 ГБ при диске на 98% и при этом полностью воспроизводимы:
``02_work/document.pdf`` остаётся на диске навсегда, а координаты блока лежат
в sidecar. Контрольный замер 2026-08-03 на живом блоке: локальный ре-рендер дал
4877×2311 — ровно ``render_size`` из index.json, 99.52% пикселей в пределах 6%
яркости от облачного кропа.

Порядок восстановления НАМЕРЕННО local-first
--------------------------------------------
``crop_url`` живут per-generation (решение от 13-14.07.2026, см. crop_cache.py):
при пере-генерации документа на портале все ссылки меняются. Замер: 15% ссылок
в корпусе уже отдают 404, причём возраст смерть не предсказывает. Локальный PDF
не протухает, ре-рендер быстрее сети (0.07 с против 0.6 с) и не требует связи.
Поэтому сеть — только запасной рунд.

Две точки входа
---------------
``hydrate_blocks_dir`` восстанавливает пачку блоков ОБРАТНО В ИСХОДНУЮ ПАПКУ —
благодаря этому весь код пайплайна, который читает ``blocks_dir / block["file"]``,
продолжает работать без единой правки.

``resolve_block_image`` отдаёт одиночный блок через LRU-кэш — для UI, обсуждений
и агентов, где N мал и писать в папку версии не нужно.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

from backend.app.core.config import (
    BLOCK_CROP_RESTORE_ALLOW_NETWORK,
    BLOCK_CROP_RESTORE_ENABLED,
    BLOCK_CROP_RESTORE_ORDER,
    BLOCK_CROP_RESTORE_TIMEOUT_S,
)
from backend.app.pipeline.stages.block_context.contract import (
    MIN_USABLE_CROP_BYTES,
    block_file_for,
)
from backend.app.services.common import block_crop_lru

logger = logging.getLogger(__name__)

SIDECAR_NAME = "crops_evicted.json"
SIDECAR_SCHEMA_VERSION = 1

#: Допустимые расширения кропов. Всё прочее отвергаем ДО обращения к ФС.
ALLOWED_SUFFIXES = {".png", ".jpg", ".jpeg", ".webp"}

#: Соответствие имени папки политике рендера — последний рубеж, если ни в
#: index.json, ни в sidecar политики не оказалось.
_DIRNAME_POLICY = {
    "blocks_stage02_100": {"dpi": 100, "min_long_side": 800, "compact": False},
    "blocks_gemma_100": {"dpi": 100, "min_long_side": 800, "compact": False},
    "blocks_gemma_300": {"dpi": 300, "min_long_side": 800, "compact": False},
    "blocks": {"dpi": 100, "min_long_side": 800, "compact": False},
}


@dataclass
class HydrationReport:
    blocks_dir: Path
    requested: int = 0
    restored: int = 0
    from_cache: int = 0
    from_pdf: int = 0
    from_cloud: int = 0
    failed: list[str] = field(default_factory=list)
    elapsed_s: float = 0.0


@dataclass
class EvictionReport:
    blocks_dir: Path
    evicted: int = 0
    kept: int = 0
    freed_bytes: int = 0
    skipped_reason: str | None = None
    dry_run: bool = True


# ─────────────────────────── чтение состояния ────────────────────────────


def sidecar_path(blocks_dir: Path | str) -> Path:
    return Path(blocks_dir) / SIDECAR_NAME


def read_sidecar(blocks_dir: Path | str) -> dict:
    """Прочитать sidecar. Пустой dict, если его нет или он устарел.

    Устаревшим считается sidecar, чей ``index_sha256`` не совпадает с текущим
    index.json: значит папку пере-кропнули после эвакуации, и записи в sidecar
    описывают уже не те файлы.
    """
    path = sidecar_path(blocks_dir)
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    if not isinstance(data, dict) or data.get("schema_version") != SIDECAR_SCHEMA_VERSION:
        return {}
    recorded = data.get("index_sha256")
    if recorded:
        current = _index_sha256(Path(blocks_dir) / "index.json")
        if current and current != recorded:
            logger.warning(
                "block_crop_store: sidecar устарел (index изменился), игнорирую: %s",
                blocks_dir,
            )
            return {}
    return data


def is_evicted(blocks_dir: Path | str) -> bool:
    return read_sidecar(blocks_dir).get("state") == "evicted"


def is_restorable(blocks_dir: Path | str, block_id: str) -> bool:
    """Можно ли восстановить блок, если файла нет на диске."""
    desc = read_sidecar(blocks_dir).get("blocks", {}).get(str(block_id))
    if not isinstance(desc, dict):
        return False
    return bool(_local_render_ready(blocks_dir, desc) or desc.get("crop_url"))


def version_dir_for(blocks_dir: Path | str) -> Path | None:
    """Подняться от папки кропов к папке версии projects_v2 (или legacy-проекта)."""
    current = Path(blocks_dir).resolve()
    for parent in [current, *current.parents]:
        if (parent / "02_work").is_dir() or (parent / "version.json").is_file():
            return parent
        if parent.name == "_output" and parent.parent.is_dir():
            return parent.parent
    return None


def restore_policy_for_dir(blocks_dir: Path | str) -> dict:
    """Политика рендера: index.json → sidecar → карта по имени папки."""
    blocks_dir = Path(blocks_dir)
    index_path = blocks_dir / "index.json"
    if index_path.is_file():
        try:
            index = json.loads(index_path.read_text(encoding="utf-8"))
            if isinstance(index, dict) and index.get("dpi"):
                return {
                    "dpi": int(index.get("dpi") or 100),
                    "min_long_side": int(index.get("min_long_side") or 0),
                    "compact": bool(index.get("compact")),
                }
        except (OSError, ValueError):
            pass
    sidecar_policy = read_sidecar(blocks_dir).get("policy")
    if isinstance(sidecar_policy, dict) and sidecar_policy.get("dpi"):
        return {
            "dpi": int(sidecar_policy.get("dpi") or 100),
            "min_long_side": int(sidecar_policy.get("min_long_side") or 0),
            "compact": bool(sidecar_policy.get("compact")),
        }
    return dict(_DIRNAME_POLICY.get(blocks_dir.name, _DIRNAME_POLICY["blocks_stage02_100"]))


def _index_sha256(index_path: Path) -> str | None:
    try:
        return hashlib.sha256(index_path.read_bytes()).hexdigest()
    except OSError:
        return None


def _safe_file_name(file_name: str) -> str | None:
    """Отвергнуть traversal и чужие расширения ДО обращения к ФС."""
    name = str(file_name or "").strip()
    if not name or Path(name).name != name:
        return None
    if Path(name).suffix.lower() not in ALLOWED_SUFFIXES:
        return None
    return name


def _load_index_entries(blocks_dir: Path) -> list[dict]:
    index_path = blocks_dir / "index.json"
    if not index_path.is_file():
        return []
    try:
        index = json.loads(index_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    entries = index if isinstance(index, list) else (index.get("blocks") or [])
    return [e for e in entries if isinstance(e, dict)]


def _local_render_ready(blocks_dir: Path | str, desc: dict) -> bool:
    """Хватает ли данных для офлайн ре-рендера из локального PDF."""
    crop_px = desc.get("crop_px")
    page_px = desc.get("page_px")
    if not (isinstance(crop_px, (list, tuple)) and len(crop_px) == 4):
        return False
    if not (isinstance(page_px, (list, tuple)) and len(page_px) == 2):
        return False
    pdf_rel = desc.get("pdf")
    if not pdf_rel:
        return False
    version_dir = version_dir_for(blocks_dir)
    if version_dir is None:
        return False
    return (version_dir / str(pdf_rel)).is_file()


# ──────────────────────────── восстановление ─────────────────────────────


def _render_from_local_pdf(blocks_dir: Path, desc: dict, out_path: Path, policy: dict) -> bool:
    from backend.app.pipeline.stages.crop_blocks.blocks import crop_from_pdf

    version_dir = version_dir_for(blocks_dir)
    if version_dir is None:
        return False
    pdf_path = version_dir / str(desc.get("pdf") or "")
    if not pdf_path.is_file():
        return False
    page = int(desc.get("page") or 0)
    if page <= 0:
        return False
    crop_px = list(desc.get("crop_px") or [])
    page_px = list(desc.get("page_px") or [])
    try:
        w, h = crop_from_pdf(
            pdf_path,
            page,
            crop_px,
            int(page_px[0]),
            int(page_px[1]),
            out_path,
            dpi=int(policy.get("dpi") or 0),
            min_long_side=int(policy.get("min_long_side") or 0),
        )
    except Exception as exc:  # noqa: BLE001 — fail-soft, идём к следующему источнику
        logger.warning("block_crop_store: локальный ре-рендер не удался: %s", exc)
        return False
    _warn_on_size_drift(desc, w, h, out_path)
    return out_path.is_file() and out_path.stat().st_size >= MIN_USABLE_CROP_BYTES


def _render_from_cloud(desc: dict, out_path: Path, policy: dict, http_get) -> bool:
    url = str(desc.get("crop_url") or "")
    if not url:
        return False
    try:
        if http_get is not None:
            pdf_bytes = http_get(url, BLOCK_CROP_RESTORE_TIMEOUT_S)
            from backend.app.pipeline.stages.crop_blocks.blocks import (
                _render_pdf_bytes_to_png,
            )

            w, h = _render_pdf_bytes_to_png(
                pdf_bytes,
                out_path,
                dpi=int(policy.get("dpi") or 0),
                min_long_side=int(policy.get("min_long_side") or 0),
            )
        else:
            from backend.app.pipeline.stages.crop_blocks.blocks import download_and_convert

            w, h = download_and_convert(
                url,
                out_path,
                timeout=BLOCK_CROP_RESTORE_TIMEOUT_S,
                dpi=int(policy.get("dpi") or 0),
                min_long_side=int(policy.get("min_long_side") or 0),
            )
    except Exception as exc:  # noqa: BLE001 — мёртвый токен здесь штатен
        logger.warning("block_crop_store: crop_url не отдал кроп (%s): %s", url[-24:], exc)
        return False
    _warn_on_size_drift(desc, w, h, out_path)
    return out_path.is_file() and out_path.stat().st_size >= MIN_USABLE_CROP_BYTES


def _warn_on_size_drift(desc: dict, w: int, h: int, out_path: Path) -> None:
    expected = desc.get("render_size")
    if not (isinstance(expected, (list, tuple)) and len(expected) == 2):
        return
    try:
        ew, eh = int(expected[0]), int(expected[1])
    except (TypeError, ValueError):
        return
    if ew <= 0 or eh <= 0:
        return
    if abs(w - ew) / ew > 0.02 or abs(h - eh) / eh > 0.02:
        logger.warning(
            "crop_restore_size_drift: %s ожидалось %dx%d, получено %dx%d",
            out_path.name,
            ew,
            eh,
            w,
            h,
        )


def _restore_one(
    blocks_dir: Path,
    block_id: str,
    desc: dict,
    out_path: Path,
    policy: dict,
    *,
    allow_network: bool,
    http_get=None,
) -> str | None:
    """Восстановить один кроп в out_path. Возвращает источник или None."""
    order = [str(s).strip() for s in (BLOCK_CROP_RESTORE_ORDER or []) if str(s).strip()]
    if not order:
        order = ["local_pdf", "crop_url"]
    # Суффикс сохраняем: PyMuPDF определяет формат по расширению.
    tmp_path = out_path.with_name(
        f".{out_path.stem}.restore.{os.getpid()}.tmp{out_path.suffix}"
    )
    try:
        for source in order:
            if source == "local_pdf":
                if _render_from_local_pdf(blocks_dir, desc, tmp_path, policy):
                    os.replace(tmp_path, out_path)
                    return "pdf"
            elif source == "crop_url":
                if not (allow_network and BLOCK_CROP_RESTORE_ALLOW_NETWORK):
                    continue
                if _render_from_cloud(desc, tmp_path, policy, http_get):
                    os.replace(tmp_path, out_path)
                    return "cloud"
        return None
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass


def resolve_block_image(
    blocks_dir: Path | str,
    block_id: str,
    *,
    file_name: str | None = None,
    allow_restore: bool | None = None,
    allow_network: bool = True,
    http_get: Callable[[str, int], bytes] | None = None,
) -> Path | None:
    """Путь к картинке блока: локальный файл → LRU-кэш → восстановление.

    Возвращаемый путь МОЖЕТ лежать вне ``blocks_dir`` (в LRU-кэше), поэтому
    вызывающий обязан проверять безопасность ИМЕНИ, а не итогового пути.
    """
    blocks_dir = Path(blocks_dir)
    if file_name is None:
        for entry in _load_index_entries(blocks_dir):
            if str(entry.get("block_id")) == str(block_id):
                file_name = block_file_for(entry)
                break
    safe_name = _safe_file_name(file_name or f"block_{block_id}.png")
    if safe_name is None:
        logger.warning("block_crop_store: отвергнуто имя файла %r", file_name)
        return None

    # 1. локальный файл
    local = blocks_dir / safe_name
    try:
        if local.is_file() and local.stat().st_size >= MIN_USABLE_CROP_BYTES:
            return local
    except OSError:
        pass

    # 2. LRU-кэш
    cached = block_crop_lru.get(blocks_dir, safe_name)
    if cached is not None:
        return cached

    if allow_restore is None:
        allow_restore = BLOCK_CROP_RESTORE_ENABLED
    if not allow_restore:
        return None

    desc = read_sidecar(blocks_dir).get("blocks", {}).get(str(block_id))
    if not isinstance(desc, dict):
        return None

    # 3/4. восстановление во временный файл кэша, затем в кэш
    policy = restore_policy_for_dir(blocks_dir)
    staging = block_crop_lru.cache_root() / ".staging"
    try:
        staging.mkdir(parents=True, exist_ok=True)
    except OSError:
        return None
    staged = staging / f"{block_crop_lru.cache_key(blocks_dir, safe_name)}{Path(safe_name).suffix}"
    source = _restore_one(
        blocks_dir,
        str(block_id),
        desc,
        staged,
        policy,
        allow_network=allow_network,
        http_get=http_get,
    )
    if source is None:
        logger.warning(
            "block_crop_store: блок %s не восстановлен (папка %s)", block_id, blocks_dir.name
        )
        return None
    placed = block_crop_lru.put(blocks_dir, safe_name, staged)
    if placed is not None:
        try:
            staged.unlink()
        except OSError:
            pass
        return placed
    return staged


def hydrate_blocks_dir(
    blocks_dir: Path | str,
    *,
    block_ids: Sequence[str] | None = None,
    reason: str = "",
    allow_network: bool = True,
    http_get: Callable[[str, int], bytes] | None = None,
) -> HydrationReport:
    """Восстановить кропы ОБРАТНО в исходную папку.

    Именно поэтому весь код пайплайна, читающий ``blocks_dir / block["file"]``,
    не требует правок: после гидрации файлы снова на своих местах.
    """
    blocks_dir = Path(blocks_dir)
    started = time.time()
    report = HydrationReport(blocks_dir=blocks_dir)
    sidecar = read_sidecar(blocks_dir)
    descs = sidecar.get("blocks") or {}
    if not descs:
        report.elapsed_s = time.time() - started
        return report

    entries = {str(e.get("block_id")): e for e in _load_index_entries(blocks_dir)}
    targets = list(block_ids) if block_ids is not None else list(descs.keys())
    policy = restore_policy_for_dir(blocks_dir)

    for block_id in targets:
        desc = descs.get(str(block_id))
        if not isinstance(desc, dict):
            report.failed.append(str(block_id))
            continue
        entry = entries.get(str(block_id)) or {}
        safe_name = _safe_file_name(
            str(desc.get("file") or "") or (block_file_for(entry) if entry else "")
        )
        if safe_name is None:
            report.failed.append(str(block_id))
            continue
        report.requested += 1
        out_path = blocks_dir / safe_name
        try:
            if out_path.is_file() and out_path.stat().st_size >= MIN_USABLE_CROP_BYTES:
                report.restored += 1
                continue
        except OSError:
            pass
        cached = block_crop_lru.get(blocks_dir, safe_name)
        if cached is not None:
            try:
                out_path.write_bytes(cached.read_bytes())
                report.restored += 1
                report.from_cache += 1
                continue
            except OSError:
                pass
        source = _restore_one(
            blocks_dir,
            str(block_id),
            desc,
            out_path,
            policy,
            allow_network=allow_network,
            http_get=http_get,
        )
        if source is None:
            report.failed.append(str(block_id))
            continue
        report.restored += 1
        if source == "pdf":
            report.from_pdf += 1
        else:
            report.from_cloud += 1

    if report.restored and sidecar.get("state") == "evicted" and not report.failed:
        _write_sidecar(blocks_dir, {**sidecar, "state": "hydrated"})

    report.elapsed_s = time.time() - started
    logger.info(
        "block_crop_store: гидрация %s (%s): %d/%d (кэш=%d, pdf=%d, облако=%d), не удалось=%d",
        blocks_dir.name,
        reason or "-",
        report.restored,
        report.requested,
        report.from_cache,
        report.from_pdf,
        report.from_cloud,
        len(report.failed),
    )
    return report


# ───────────────────────────── эвакуация ──────────────────────────────────


def _write_sidecar(blocks_dir: Path, data: dict) -> None:
    path = sidecar_path(blocks_dir)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")
    with tmp.open("rb") as fh:
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def _result_json_maps(version_dir: Path) -> tuple[dict[str, str], dict[int, list[int]], str | None]:
    """(block_id → crop_url, page_index+1 → [w,h], относительный путь к PDF)."""
    urls: dict[str, str] = {}
    pages: dict[int, list[int]] = {}
    result_path = version_dir / "02_work" / "result.json"
    if not result_path.is_file():
        return urls, pages, None
    try:
        data = json.loads(result_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return urls, pages, None
    for page in data.get("pages") or []:
        if not isinstance(page, dict):
            continue
        try:
            page_no = int(page.get("page_number") or 0)
        except (TypeError, ValueError):
            page_no = 0
        width, height = page.get("width"), page.get("height")
        if page_no and width and height:
            pages[page_no] = [int(width), int(height)]
        for block in page.get("blocks") or []:
            if isinstance(block, dict) and block.get("id"):
                urls[str(block["id"])] = str(block.get("crop_url") or "")
    pdf_rel = "02_work/document.pdf" if (version_dir / "02_work" / "document.pdf").is_file() else None
    return urls, pages, pdf_rel


def build_sidecar(blocks_dir: Path | str, version_dir: Path | str | None = None) -> dict:
    """Собрать полное описание восстановления для папки кропов.

    ``page_px`` (размеры страницы в пиксельной системе result.json) обязателен и
    в index.json ОТСУТСТВУЕТ — ``crop_from_pdf`` без него не пересчитает
    координаты в точки PDF. Собираем один раз здесь, чтобы восстановление
    никогда не парсило result.json на 0.8-8.5 МБ.
    """
    blocks_dir = Path(blocks_dir)
    version_dir = Path(version_dir) if version_dir else version_dir_for(blocks_dir)
    entries = _load_index_entries(blocks_dir)
    urls, pages, pdf_rel = _result_json_maps(version_dir) if version_dir else ({}, {}, None)
    policy = restore_policy_for_dir(blocks_dir)

    blocks: dict[str, dict] = {}
    kept: list[str] = []
    for entry in entries:
        block_id = str(entry.get("block_id") or "")
        if not block_id:
            continue
        name = block_file_for(entry)
        path = blocks_dir / name
        try:
            size = path.stat().st_size
        except OSError:
            size = 0
        page = entry.get("page")
        try:
            page_no = int(page)
        except (TypeError, ValueError):
            page_no = 0
        desc = {
            "file": name,
            "bytes": size,
            "render_size": entry.get("render_size"),
            "page": page_no,
            "crop_px": entry.get("crop_px"),
            "page_px": pages.get(page_no),
            "pdf": pdf_rel,
            "crop_url": urls.get(block_id) or None,
            "source": entry.get("source"),
        }
        # Невоспроизводимые случаи — не эвакуируем никогда.
        unreproducible = (
            bool(entry.get("promoted_to_full"))
            or bool(entry.get("compact"))
            or (blocks_dir / f"block_{block_id}_full.png").is_file()
        )
        recoverable = (not unreproducible) and (
            _local_render_ready(blocks_dir, desc) or bool(desc["crop_url"])
        )
        if not recoverable:
            kept.append(block_id)
        blocks[block_id] = desc

    return {
        "schema_version": SIDECAR_SCHEMA_VERSION,
        "state": "hydrated",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "policy": policy,
        "index_sha256": _index_sha256(blocks_dir / "index.json"),
        "kept_block_ids": kept,
        "blocks": blocks,
    }


def evict_blocks_dir(
    blocks_dir: Path | str,
    *,
    version_dir: Path | str | None = None,
    dry_run: bool = True,
    evicted_by: str = "manual",
    verify_render: bool = True,
) -> EvictionReport:
    """Удалить восстановимые PNG, оставив index.json и sidecar.

    Порядок намеренно такой: sidecar пишется и fsync'ается ДО первого удаления,
    затем перечитывается. Падение посередине оставляет валидный sidecar и часть
    файлов — безобидное состояние, которое resolver отработает штатно.

    ``verify_render``: перед удалением делаем контрольный ре-рендер в память.
    Это превращает «мы считаем, что восстановимо» в «мы это только что проверили».
    """
    blocks_dir = Path(blocks_dir)
    report = EvictionReport(blocks_dir=blocks_dir, dry_run=dry_run)

    index_path = blocks_dir / "index.json"
    if not index_path.is_file():
        report.skipped_reason = "no_index"
        return report
    try:
        index = json.loads(index_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        report.skipped_reason = "bad_index"
        return report
    if isinstance(index, dict) and index.get("compact"):
        report.skipped_reason = "compact_profile"
        return report

    sidecar = build_sidecar(blocks_dir, version_dir)
    if not sidecar.get("blocks"):
        report.skipped_reason = "no_blocks"
        return report

    kept = set(sidecar.get("kept_block_ids") or [])
    evictable: list[tuple[str, Path, int]] = []
    for block_id, desc in sidecar["blocks"].items():
        if block_id in kept:
            report.kept += 1
            continue
        name = _safe_file_name(str(desc.get("file") or ""))
        if name is None:
            report.kept += 1
            continue
        path = blocks_dir / name
        if not path.is_file():
            continue
        if verify_render and not _verify_restorable(blocks_dir, desc, sidecar["policy"]):
            report.kept += 1
            continue
        evictable.append((block_id, path, path.stat().st_size))

    if not evictable:
        report.skipped_reason = report.skipped_reason or "nothing_evictable"
        return report

    report.freed_bytes = sum(size for _b, _p, size in evictable)
    report.evicted = len(evictable)
    if dry_run:
        return report

    sidecar["state"] = "evicted"
    sidecar["evicted_at"] = datetime.now(timezone.utc).isoformat()
    sidecar["evicted_by"] = evicted_by
    sidecar["freed_bytes"] = report.freed_bytes
    _write_sidecar(blocks_dir, sidecar)

    verified = read_sidecar(blocks_dir)
    covered = verified.get("blocks") or {}
    if not all(bid in covered for bid, _p, _s in evictable):
        report.skipped_reason = "sidecar_verify_failed"
        report.evicted = 0
        report.freed_bytes = 0
        return report

    # Удаляем не unlink'ом, а переносом в .evicted/ — ошибки первой недели
    # обратимы обычным mv, реальное удаление делает отложенный проход.
    trash = blocks_dir / ".evicted"
    trash.mkdir(exist_ok=True)
    for _block_id, path, _size in evictable:
        try:
            os.replace(path, trash / path.name)
        except OSError as exc:
            logger.warning("block_crop_store: не удалось эвакуировать %s: %s", path.name, exc)
    return report


def _verify_restorable(blocks_dir: Path, desc: dict, policy: dict) -> bool:
    """Контрольный ре-рендер во временный файл: реально ли восстановимо.

    Пишем ВНЕ проверяемой папки: иначе проверка (операция сугубо читающая по
    смыслу) сдвигает mtime продовой папки и сама же ломает эвристики вида
    «папку недавно трогали — возможно, идёт кроп».
    """
    if not _local_render_ready(blocks_dir, desc):
        return bool(desc.get("crop_url"))
    import tempfile

    with tempfile.TemporaryDirectory(prefix="crop_verify_") as tmpdir:
        tmp = Path(tmpdir) / "verify.png"
        return _render_from_local_pdf(blocks_dir, desc, tmp, policy)


def evict_run_dir(run_dir: Path | str, *, dry_run: bool = True, **kwargs) -> list[EvictionReport]:
    """Эвакуировать все папки кропов внутри одной run/latest-папки."""
    run_dir = Path(run_dir)
    reports: list[EvictionReport] = []
    for name in _DIRNAME_POLICY:
        candidate = run_dir / name
        if (candidate / "index.json").is_file():
            reports.append(evict_blocks_dir(candidate, dry_run=dry_run, **kwargs))
    return reports


def missing_block_files(
    blocks_dir: Path | str, block_ids: Iterable[str] | None = None
) -> list[str]:
    """block_id, чьи PNG отсутствуют или непригодны."""
    blocks_dir = Path(blocks_dir)
    wanted = {str(b) for b in block_ids} if block_ids is not None else None
    missing: list[str] = []
    for entry in _load_index_entries(blocks_dir):
        block_id = str(entry.get("block_id") or "")
        if wanted is not None and block_id not in wanted:
            continue
        path = blocks_dir / block_file_for(entry)
        try:
            if path.stat().st_size >= MIN_USABLE_CROP_BYTES:
                continue
        except OSError:
            pass
        missing.append(block_id)
    return missing
