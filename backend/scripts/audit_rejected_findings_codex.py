#!/usr/bin/env python3
"""Prepare and run a read-only Codex audit of rejected PDF findings.

Examples:

    python backend/scripts/audit_rejected_findings_codex.py prepare --month 2026-07
    python backend/scripts/audit_rejected_findings_codex.py run --month 2026-07 --confirm-external-codex --limit 10
    python backend/scripts/audit_rejected_findings_codex.py run --month 2026-07 --confirm-external-codex
    python backend/scripts/audit_rejected_findings_codex.py report --month 2026-07

``run`` is resumable: successful case ids in ``results.jsonl`` are skipped.
Source expert reviews and project artifacts are never modified.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
AUTO_RETRY_CONTRACT_VERSION = "rejected_finding_auto_retry.v2"
RECOVERY_CONTRACT_VERSION = "rejected_finding_recovery.v1"
DISCLOSURE_SCHEMA_VERSION = 2
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.services.findings.rejected_audit_service import (  # noqa: E402
    AUDIT_CONTRACT_VERSION,
    AUTO_RETRIEVAL_CONTRACT_VERSION,
    DEEP_RETRIEVAL_CONTRACT_VERSION,
    DEFAULT_PROJECTS_V2_ROOT,
    DEFAULT_REPORT_ROOT,
    align_batch_images,
    collect_rejected_cases,
    generate_report,
    load_latest_results,
    load_manifest,
    plan_batches,
    prepare_retrieval_cases,
    prepare_recovery_cases,
    run_codex_audit,
    utc_now_iso,
    write_manifest,
)


def _csv_set(raw: str, *, upper: bool = False) -> set[str]:
    values = {part.strip() for part in str(raw or "").split(",") if part.strip()}
    return {value.upper() for value in values} if upper else values


def _read_inventory(output_dir: Path) -> dict:
    path = output_dir / "inventory.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}

def _auto_retry_snapshot_compatible(path: Path) -> bool:
    inventory = _read_inventory(path)
    auto_meta = inventory.get("auto_retry") or {}
    disclosure_path = path / "external_codex_disclosure.json"
    try:
        disclosure = json.loads(disclosure_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return False
    return bool(
        auto_meta.get("contract_version") == AUTO_RETRY_CONTRACT_VERSION
        and auto_meta.get("retrieval_contract_version") == AUTO_RETRIEVAL_CONTRACT_VERSION
        and int(disclosure.get("schema_version") or 0) == DISCLOSURE_SCHEMA_VERSION
        and (path / "manifest.jsonl").is_file()
        and (path / "inventory.json").is_file()
    )


def _default_auto_retry_output_dir(source_output_dir: Path) -> Path:
    versions: list[tuple[int, Path]] = []
    for path in source_output_dir.glob("auto-retry-v*"):
        match = re.fullmatch(r"auto-retry-v(\d+)", path.name)
        if match and path.is_dir():
            versions.append((int(match.group(1)), path))
    if not versions:
        return source_output_dir / "auto-retry-v1"
    latest_number, latest_path = max(versions, key=lambda item: item[0])
    if _auto_retry_snapshot_compatible(latest_path):
        return latest_path
    return source_output_dir / f"auto-retry-v{latest_number + 1}"


def _recovery_snapshot_compatible(path: Path) -> bool:
    inventory = _read_inventory(path)
    meta = inventory.get("recovery") or {}
    disclosure_path = path / "external_codex_disclosure.json"
    try:
        disclosure = json.loads(disclosure_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return False
    return bool(
        meta.get("contract_version") == RECOVERY_CONTRACT_VERSION
        and meta.get("retrieval_contract_version") == DEEP_RETRIEVAL_CONTRACT_VERSION
        and int(disclosure.get("schema_version") or 0) == DISCLOSURE_SCHEMA_VERSION
        and (path / "manifest.jsonl").is_file()
        and (path / "inventory.json").is_file()
        and (path / "recovery_classification.json").is_file()
    )


def _default_recovery_output_dir(source_output_dir: Path) -> Path:
    versions: list[tuple[int, Path]] = []
    for path in source_output_dir.glob("recovery-v*"):
        match = re.fullmatch(r"recovery-v(\d+)", path.name)
        if match and path.is_dir():
            versions.append((int(match.group(1)), path))
    if not versions:
        return source_output_dir / "recovery-v1"
    latest_number, latest_path = max(versions, key=lambda item: item[0])
    if _recovery_snapshot_compatible(latest_path):
        return latest_path
    return source_output_dir / f"recovery-v{latest_number + 1}"


def _hybrid_second_pass_case_ids(
    results_path: Path,
    cases: list[dict],
) -> set[str]:
    expected_hashes = {
        str(case.get("case_id") or ""): str(case.get("input_hash") or "")
        for case in cases
    }
    latest: dict[str, dict] = {}
    with Path(results_path).open(encoding="utf-8") as handle:
        for raw in handle:
            try:
                payload = json.loads(raw)
            except (TypeError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict):
                continue
            case_id = str(payload.get("case_id") or "")
            if case_id:
                latest[case_id] = payload
    return {
        case_id
        for case_id, payload in latest.items()
        if case_id in expected_hashes
        and str(payload.get("input_hash") or "") == expected_hashes[case_id]
        and payload.get("status") == "success"
        and (
            payload.get("verdict") in {"expert_correct", "expert_may_be_wrong"}
            or bool(payload.get("guard_adjustments"))
        )
    }



def _frozen_scope_errors(
    args: argparse.Namespace,
    inventory: dict,
    *,
    check_filters: bool,
) -> list[str]:
    errors: list[str] = []
    if str(inventory.get("period") or "") != args.month:
        errors.append(f"month: frozen={inventory.get('period')!r}, requested={args.month!r}")
    if str(inventory.get("timezone") or "") != args.timezone:
        errors.append(f"timezone: frozen={inventory.get('timezone')!r}, requested={args.timezone!r}")
    if check_filters:
        if str(inventory.get("audit_contract_version") or "") != AUDIT_CONTRACT_VERSION:
            errors.append(
                "audit contract differs from frozen manifest: "
                f"frozen={inventory.get('audit_contract_version')!r}, "
                f"current={AUDIT_CONTRACT_VERSION!r}; prepare a new output directory"
            )
        filters = inventory.get("filters") or {}
        expected_objects = _csv_set(args.objects)
        expected_disciplines = _csv_set(args.disciplines, upper=True)
        expected_reviewers = _csv_set(args.reviewers)
        if set(filters.get("object_ids") or []) != expected_objects:
            errors.append("object filter differs from frozen manifest")
        if set(filters.get("disciplines") or []) != expected_disciplines:
            errors.append("discipline filter differs from frozen manifest")
        if set(filters.get("reviewers") or []) != expected_reviewers:
            errors.append("reviewer filter differs from frozen manifest")
        expected_include_carried = not bool(filters.get("explicit_carried_over_excluded", True))
        if bool(args.include_carried_over) != expected_include_carried:
            errors.append("carried-over filter differs from frozen manifest")
        if bool(args.include_optimizations) != bool(filters.get("include_optimizations")):
            errors.append("optimization filter differs from frozen manifest")
    return errors


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Независимый read-only аудит отклонённых замечаний через Codex subscription.",
    )
    parser.add_argument(
        "command",
        choices=("prepare", "retrieve", "auto-retry", "recover", "run", "report"),
        nargs="?",
        default="prepare",
    )
    parser.add_argument("--month", required=True, help="Календарный месяц YYYY-MM, например 2026-07")
    parser.add_argument("--timezone", default="Europe/Moscow", help="Часовой пояс календарного месяца")
    parser.add_argument("--projects-v2-root", type=Path, default=DEFAULT_PROJECTS_V2_ROOT)
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="По умолчанию comparison/rejected_findings_audit/<month>",
    )
    parser.add_argument(
        "--source-output-dir",
        type=Path,
        help="Для retrieve/auto-retry/recover: каталог исходного завершённого аудита",
    )
    parser.add_argument("--objects", default="", help="Фильтр object_id через запятую")
    parser.add_argument("--disciplines", default="", help="Фильтр дисциплин через запятую")
    parser.add_argument("--reviewers", default="", help="Фильтр reviewer через запятую")
    parser.add_argument("--include-carried-over", action="store_true", help="Включить автоматические переносы")
    parser.add_argument("--include-optimizations", action="store_true", help="Включить OPT, не только findings")
    parser.add_argument("--max-images-per-case", type=int, default=6)
    parser.add_argument(
        "--reuse-manifest",
        action="store_true",
        help="Для run не пересобирать manifest из live-данных",
    )
    parser.add_argument(
        "--confirm-external-codex",
        action="store_true",
        help="Явно разрешить передачу выбранных findings/OCR/изображений в подписочный Codex",
    )
    parser.add_argument(
        "--confirm-disclosure-sha256",
        default="",
        help="Для auto-retry/recover: SHA-256 предварительно показанного состава внешней передачи",
    )
    parser.add_argument(
        "--remote-crops",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Автоматически восстанавливать exact crop-PDF по сохранённым same-version URL",
    )

    parser.add_argument("--model", default="codex/gpt-5.6-sol")
    parser.add_argument(
        "--reasoning-effort",
        choices=("minimal", "low", "medium", "high", "xhigh", "max"),
        default="high",
    )
    parser.add_argument("--timeout", type=int, default=600, help="Таймаут одного Codex batch, секунд")
    parser.add_argument("--batch-size", type=int, default=4)
    parser.add_argument("--max-batch-images", type=int, default=6)
    parser.add_argument("--limit", type=int, default=0, help="Пилот: максимум кейсов в этом запуске")
    parser.add_argument("--max-calls", type=int, default=0, help="Максимум Codex-вызовов в этом запуске")
    parser.add_argument("--delay", type=float, default=0.0, help="Пауза между вызовами, не более 60 сек")
    parser.add_argument("--case-id", action="append", default=[], help="Обработать только указанный case_id")
    parser.add_argument(
        "--hybrid-source-results",
        type=Path,
        help="Sol-проход: определённые Luna-вердикты и кейсы с guard_adjustments",
    )
    return parser


def _prepare(args: argparse.Namespace, output_dir: Path) -> list[dict]:
    print(f"[prepare] период {args.month}; источник {args.projects_v2_root}", flush=True)
    cases, inventory = collect_rejected_cases(
        month=args.month,
        projects_v2_root=args.projects_v2_root,
        timezone_name=args.timezone,
        include_carried_over=args.include_carried_over,
        include_optimizations=args.include_optimizations,
        max_images_per_case=args.max_images_per_case,
        object_ids=_csv_set(args.objects) or None,
        disciplines=_csv_set(args.disciplines, upper=True) or None,
        reviewers=_csv_set(args.reviewers) or None,
    )
    manifest_path, inventory_path = write_manifest(output_dir, cases, inventory)
    counts = inventory.get("counts") or {}
    print(
        f"[prepare] кейсов: {len(cases)}; source найден: {counts.get('source_item_found', 0)}; "
        f"без source: {counts.get('source_item_missing', 0)}; без причины: {counts.get('missing_expert_reason', 0)}",
        flush=True,
    )
    print(f"[prepare] manifest: {manifest_path}", flush=True)
    print(f"[prepare] inventory: {inventory_path}", flush=True)
    return cases


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _has_symlink_component(path: Path) -> bool:
    lexical = Path(os.path.abspath(path))
    chain = [lexical, *lexical.parents]
    return any(item.is_symlink() for item in chain)


def _validate_output_destination(output_dir: Path, projects_v2_root: Path) -> None:
    lexical_output = Path(os.path.abspath(output_dir))
    if _has_symlink_component(lexical_output):
        raise ValueError("output-dir не должен проходить через symlink")
    resolved_output = lexical_output.resolve(strict=False)
    resolved_projects = Path(projects_v2_root).resolve(strict=False)
    if resolved_output in {Path("/"), ROOT.resolve()}:
        raise ValueError("слишком широкий output-dir")
    if (
        resolved_output == resolved_projects
        or resolved_output.is_relative_to(resolved_projects)
        or resolved_projects.is_relative_to(resolved_output)
    ):
        raise ValueError("output-dir не может находиться внутри projects_v2 или быть его предком")


def _write_new_json(path: Path, payload: dict) -> None:
    if path.exists():
        raise FileExistsError(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _build_external_disclosure(
    cases: list[dict],
    *,
    manifest_path: Path,
    inventory_path: Path,
) -> dict:
    rows: list[dict] = []
    total_images = 0
    total_text_chars = 0
    for case in cases:
        context = case.get("context") or {}
        images: list[dict] = []
        for image in context.get("images") or []:
            if not isinstance(image, dict):
                continue
            path = Path(str(image.get("path") or ""))
            item = {
                "block_id": image.get("block_id"),
                "page": image.get("page"),
                "path": str(path),
                "exists": path.is_file(),
            }
            if path.is_file():
                item.update({
                    "sha256": _sha256_file(path),
                    "bytes": path.stat().st_size,
                })
            images.append(item)
        text_chars = sum(
            len(str(value or ""))
            for value in (
                case.get("expert_reason"),
                (case.get("finding") or {}).get("problem"),
                (case.get("finding") or {}).get("description"),
                context.get("finding_evidence_text"),
                context.get("document_text_excerpt"),
                context.get("text_excerpt"),
            )
        ) + sum(
            len(str(block.get("ocr_or_description") or ""))
            for block in context.get("blocks") or []
            if isinstance(block, dict)
        )
        total_images += len(images)
        total_text_chars += text_chars
        rows.append({
            "case_id": case.get("case_id"),
            "finding_id": (case.get("finding") or {}).get("id"),
            "object": case.get("object_name"),
            "document": case.get("document"),
            "version": case.get("version_id"),
            "expert": case.get("expert_reviewer"),
            "text_chars": text_chars,
            "block_ids": [
                block.get("block_id")
                for block in context.get("blocks") or []
                if isinstance(block, dict) and block.get("block_id")
            ],
            "images": images,
        })
    return {
        "schema_version": DISCLOSURE_SCHEMA_VERSION,
        "purpose": "Повторная независимая проверка недостаточного контекста",
        "external_processor": "subscription Codex",
        "model": "codex/gpt-5.6-sol",
        "reasoning_effort": "high",
        "data_categories": [
            "замечание finding той же версии",
            "причина решения эксперта",
            "OCR/текстовые и векторные фрагменты",
            "изображения исходных и найденных блоков",
        ],
        "manifest": str(manifest_path.resolve()),
        "manifest_sha256": _sha256_file(manifest_path),
        "inventory": str(inventory_path.resolve()),
        "inventory_sha256": _sha256_file(inventory_path),
        "case_count": len(cases),
        "image_count": total_images,
        "text_chars": total_text_chars,
        "cases": rows,
    }


def _verify_external_disclosure(
    *,
    output_dir: Path,
    disclosure_path: Path,
    batch_size: int,
    requested_max_batch_images: int,
) -> tuple[list[dict], int]:
    """Fail closed unless frozen files exactly match the confirmed disclosure."""
    manifest_path = output_dir / "manifest.jsonl"
    inventory_path = output_dir / "inventory.json"
    for path in (manifest_path, inventory_path, disclosure_path):
        if _has_symlink_component(path):
            raise ValueError(f"frozen artifact проходит через symlink: {path.name}")
        if not path.is_file():
            raise ValueError(f"frozen artifact отсутствует: {path.name}")
    try:
        disclosure = json.loads(disclosure_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("disclosure не читается") from exc
    if not isinstance(disclosure, dict) or int(disclosure.get("schema_version") or 0) != DISCLOSURE_SCHEMA_VERSION:
        raise ValueError("неподдерживаемая версия disclosure")
    if Path(str(disclosure.get("manifest") or "")).resolve() != manifest_path.resolve():
        raise ValueError("disclosure указывает другой manifest")
    if Path(str(disclosure.get("inventory") or "")).resolve() != inventory_path.resolve():
        raise ValueError("disclosure указывает другой inventory")
    if str(disclosure.get("manifest_sha256") or "") != _sha256_file(manifest_path):
        raise ValueError("manifest изменился после disclosure")
    if str(disclosure.get("inventory_sha256") or "") != _sha256_file(inventory_path):
        raise ValueError("inventory изменился после disclosure")

    cases = load_manifest(manifest_path)
    rebuilt = _build_external_disclosure(
        cases,
        manifest_path=manifest_path,
        inventory_path=inventory_path,
    )
    for key in ("purpose", "data_categories"):
        if key in disclosure:
            rebuilt[key] = disclosure[key]
    if rebuilt != disclosure:
        raise ValueError("состав текста или изображений изменился после disclosure")

    context_root = (output_dir / "context_assets").resolve()
    disclosure_rows = {
        str(row.get("case_id") or ""): row
        for row in (disclosure.get("cases") or [])
        if isinstance(row, dict)
    }
    required_case_images = 0
    for case in cases:
        case_id = str(case.get("case_id") or "")
        disclosed_case = disclosure_rows.get(case_id)
        if not disclosed_case:
            raise ValueError(f"case отсутствует в disclosure: {case_id}")
        disclosed_images = disclosed_case.get("images") or []
        required_case_images = max(required_case_images, len(disclosed_images))
        for image in disclosed_images:
            if not isinstance(image, dict) or not image.get("exists"):
                raise ValueError(f"недоступное изображение в disclosure: {case_id}")
            image_path = Path(str(image.get("path") or ""))
            if _has_symlink_component(image_path):
                raise ValueError(f"изображение проходит через symlink: {case_id}")
            try:
                resolved_image = image_path.resolve(strict=True)
            except (OSError, RuntimeError) as exc:
                raise ValueError(f"изображение исчезло: {case_id}") from exc
            if not resolved_image.is_relative_to(context_root):
                raise ValueError(f"изображение вне frozen context_assets: {case_id}")
            if resolved_image.stat().st_size != int(image.get("bytes") or -1):
                raise ValueError(f"размер изображения изменился: {case_id}")
            if _sha256_file(resolved_image) != str(image.get("sha256") or ""):
                raise ValueError(f"SHA изображения изменился: {case_id}")

    max_batch_images = max(1, int(requested_max_batch_images), required_case_images)
    batches = plan_batches(
        cases,
        batch_size=max(1, int(batch_size)),
        max_batch_images=max_batch_images,
    )
    for batch in batches:
        _paths, aligned = align_batch_images(batch, max_total_images=max_batch_images)
        for case in batch:
            case_id = str(case.get("case_id") or "")
            expected = [
                {
                    "path": str(Path(str(image.get("path") or "")).resolve(strict=True)),
                    "block_id": str(image.get("block_id") or "").removeprefix("block_"),
                    "page": image.get("page"),
                }
                for image in disclosure_rows[case_id].get("images") or []
            ]
            actual = [
                {
                    "path": row.get("path"),
                    "block_id": row.get("block_id"),
                    "page": row.get("page"),
                }
                for row in aligned.get(case_id) or []
            ]
            if actual != expected:
                raise ValueError(f"фактический image alignment отличается от disclosure: {case_id}")
    return cases, max_batch_images


def _prepare_auto_retry_snapshot(
    args: argparse.Namespace,
    source_output_dir: Path,
    output_dir: Path,
) -> tuple[list[dict], Path, bool]:
    manifest_path = output_dir / "manifest.jsonl"
    inventory_path = output_dir / "inventory.json"
    disclosure_path = output_dir / "external_codex_disclosure.json"
    if manifest_path.is_file():
        if not inventory_path.is_file() or not disclosure_path.is_file():
            raise ValueError("auto-retry snapshot неполон: отсутствует inventory или disclosure")
        inventory = _read_inventory(output_dir)
        auto_meta = inventory.get("auto_retry") or {}
        if int(auto_meta.get("depth") or 0) != 1:
            raise ValueError("существующий каталог не является auto-retry snapshot")
        if (
            auto_meta.get("contract_version") != AUTO_RETRY_CONTRACT_VERSION
            or auto_meta.get("retrieval_contract_version") != AUTO_RETRIEVAL_CONTRACT_VERSION
        ):
            raise ValueError("существующий auto-retry snapshot несовместим с текущим retrieval contract")
        frozen_source = Path(str(auto_meta.get("source_output_dir") or "")).resolve()
        if frozen_source != source_output_dir.resolve():
            raise ValueError("source-output-dir отличается от замороженного auto-retry snapshot")
        return load_manifest(manifest_path), disclosure_path, False

    source_manifest_path = source_output_dir / "manifest.jsonl"
    source_results_path = source_output_dir / "results.jsonl"
    source_inventory = _read_inventory(source_output_dir)
    if not source_manifest_path.is_file() or not source_results_path.is_file():
        raise ValueError("auto-retry требует source manifest.jsonl и results.jsonl")
    scope_errors = _frozen_scope_errors(args, source_inventory, check_filters=False)
    if scope_errors:
        raise ValueError("source frozen scope mismatch: " + "; ".join(scope_errors))
    source_auto = source_inventory.get("auto_retry") or {}
    if int(source_auto.get("depth") or 0) >= 1:
        raise ValueError("автоматический повтор ограничен одним уровнем; третий проход запрещён")

    source_cases = load_manifest(source_manifest_path)
    source_results, malformed = load_latest_results(source_results_path)
    selection_limit = int(args.limit or 0) or max(1, len(source_cases))
    selected, retrieval_counts = prepare_retrieval_cases(
        source_cases,
        source_results,
        limit=selection_limit,
        only_case_ids=set(args.case_id) or None,
        max_images_per_case=max(0, int(args.max_images_per_case)),
        asset_dir=output_dir / "context_assets",
        allow_remote_crops=bool(args.remote_crops),
    )
    inventory = json.loads(json.dumps(source_inventory, ensure_ascii=False))
    inventory.update({
        "schema_version": 3,
        "generated_at": utc_now_iso(),
        "source": "autonomous second pass over frozen rejected-findings audit",
        "counts": retrieval_counts,
        "by_object": dict(Counter(case.get("object_name") for case in selected)),
        "by_discipline": dict(Counter(case.get("discipline") for case in selected)),
        "by_reviewer": dict(Counter(case.get("expert_reviewer") for case in selected)),
        "by_day": dict(Counter(str(case.get("expert_timestamp_local") or "")[:10] for case in selected)),
        "by_route": dict(Counter((case.get("context") or {}).get("route") for case in selected)),
        "auto_retry": {
            "contract_version": AUTO_RETRY_CONTRACT_VERSION,
            "retrieval_contract_version": AUTO_RETRIEVAL_CONTRACT_VERSION,
            "depth": 1,
            "max_depth": 1,
            "source_output_dir": str(source_output_dir.resolve()),
            "source_manifest": str(source_manifest_path.resolve()),
            "source_manifest_sha256": _sha256_file(source_manifest_path),
            "source_results": str(source_results_path.resolve()),
            "source_results_sha256_at_freeze": _sha256_file(source_results_path),
            "source_malformed_result_lines": malformed,
            "selected_case_ids": [case.get("case_id") for case in selected],
            "model": "codex/gpt-5.6-sol",
            "reasoning_effort": "high",
            "remote_crops_enabled": bool(args.remote_crops),
            "max_images_per_case": max(0, int(args.max_images_per_case)),
        },
    })
    written_manifest, written_inventory = write_manifest(output_dir, selected, inventory)
    generate_report(
        selected,
        output_dir=output_dir,
        results_path=output_dir / "results.jsonl",
    )
    disclosure = _build_external_disclosure(
        selected,
        manifest_path=written_manifest,
        inventory_path=written_inventory,
    )
    _write_new_json(disclosure_path, disclosure)
    return selected, disclosure_path, True


def _derived_source_inventory(
    source_output_dir: Path,
    source_cases: list[dict],
    args: argparse.Namespace,
) -> dict:
    inventory = _read_inventory(source_output_dir)
    if inventory:
        return inventory
    periods = {str(case.get("period") or "") for case in source_cases if case.get("period")}
    if periods and periods != {args.month}:
        raise ValueError(f"source manifest содержит другой период: {sorted(periods)}")
    return {
        "schema_version": 1,
        "period": args.month,
        "timezone": args.timezone,
        "source": "derived from merged frozen manifest",
        "filters": {},
        "counts": {"selected_cases": len(source_cases)},
    }


def _prepare_recovery_snapshot(
    args: argparse.Namespace,
    source_output_dir: Path,
    output_dir: Path,
) -> tuple[list[dict], Path, bool]:
    manifest_path = output_dir / "manifest.jsonl"
    inventory_path = output_dir / "inventory.json"
    disclosure_path = output_dir / "external_codex_disclosure.json"
    classification_path = output_dir / "recovery_classification.json"
    if manifest_path.is_file():
        if not all(path.is_file() for path in (inventory_path, disclosure_path, classification_path)):
            raise ValueError("recovery snapshot неполон")
        inventory = _read_inventory(output_dir)
        meta = inventory.get("recovery") or {}
        if (
            meta.get("contract_version") != RECOVERY_CONTRACT_VERSION
            or meta.get("retrieval_contract_version") != DEEP_RETRIEVAL_CONTRACT_VERSION
        ):
            raise ValueError("существующий recovery snapshot несовместим с текущим контрактом")
        if Path(str(meta.get("source_output_dir") or "")).resolve() != source_output_dir.resolve():
            raise ValueError("source-output-dir отличается от frozen recovery snapshot")
        return load_manifest(manifest_path), disclosure_path, False

    source_manifest_path = source_output_dir / "manifest.jsonl"
    source_results_path = source_output_dir / "results.jsonl"
    if not source_manifest_path.is_file() or not source_results_path.is_file():
        raise ValueError("recover требует source manifest.jsonl и results.jsonl")
    source_cases = load_manifest(source_manifest_path)
    source_inventory = _derived_source_inventory(source_output_dir, source_cases, args)
    source_recovery = source_inventory.get("recovery") or {}
    source_recovery_depth = 0
    if source_recovery:
        try:
            source_recovery_depth = int(source_recovery.get("depth") or 1)
        except (TypeError, ValueError):
            source_recovery_depth = 1
        if source_recovery_depth >= 2:
            raise ValueError(
                "recover exhaustion уже выполнен; третий рекурсивный уровень запрещён"
            )
    scope_errors = _frozen_scope_errors(args, source_inventory, check_filters=False)
    if scope_errors:
        raise ValueError("source frozen scope mismatch: " + "; ".join(scope_errors))
    source_results, malformed = load_latest_results(source_results_path)
    selected, recovery_counts, classifications = prepare_recovery_cases(
        source_cases,
        source_results,
        limit=max(0, int(args.limit or 0)),
        only_case_ids=set(args.case_id) or None,
        max_images_per_case=max(12, int(args.max_images_per_case)),
        asset_dir=output_dir / "context_assets",
        allow_remote_crops=bool(args.remote_crops),
    )
    inventory = json.loads(json.dumps(source_inventory, ensure_ascii=False))
    inventory.update({
        "schema_version": 4,
        "generated_at": utc_now_iso(),
        "source": "deep autonomous recovery over frozen rejected-findings audit",
        "counts": recovery_counts,
        "by_object": dict(Counter(case.get("object_name") for case in selected)),
        "by_discipline": dict(Counter(case.get("discipline") for case in selected)),
        "by_reviewer": dict(Counter(case.get("expert_reviewer") for case in selected)),
        "by_day": dict(Counter(str(case.get("expert_timestamp_local") or "")[:10] for case in selected)),
        "by_route": dict(Counter((case.get("context") or {}).get("route") for case in selected)),
        "recovery": {
            "contract_version": RECOVERY_CONTRACT_VERSION,
            "depth": source_recovery_depth + 1,
            "retrieval_contract_version": DEEP_RETRIEVAL_CONTRACT_VERSION,
            "source_output_dir": str(source_output_dir.resolve()),
            "source_manifest": str(source_manifest_path.resolve()),
            "source_manifest_sha256": _sha256_file(source_manifest_path),
            "source_results": str(source_results_path.resolve()),
            "source_results_sha256_at_freeze": _sha256_file(source_results_path),
            "source_malformed_result_lines": malformed,
            "selected_case_ids": [case.get("case_id") for case in selected],
            "model": "codex/gpt-5.6-sol",
            "reasoning_effort": "high",
            "remote_crops_enabled": bool(args.remote_crops),
            "max_images_per_case": max(12, int(args.max_images_per_case)),
        },
    })
    written_manifest, written_inventory = write_manifest(output_dir, selected, inventory)
    _write_new_json(classification_path, {
        "schema_version": 1,
        "generated_at": utc_now_iso(),
        "source_output_dir": str(source_output_dir.resolve()),
        "counts": recovery_counts,
        "cases": classifications,
    })
    generate_report(selected, output_dir=output_dir, results_path=output_dir / "results.jsonl")
    disclosure = _build_external_disclosure(
        selected,
        manifest_path=written_manifest,
        inventory_path=written_inventory,
    )
    disclosure["purpose"] = "Глубокая повторная проверка после автономного поиска недостающего контекста"
    disclosure["data_categories"] = [
        "замечание finding и причина решения эксперта",
        "OCR/текстовые и векторные фрагменты полного документа",
        "изображения блоков и полных страниц исходного PDF",
        "локально найденные нормативные фрагменты",
        "фрагменты явно связанных документов того же объекта с указанием версии",
    ]
    _write_new_json(disclosure_path, disclosure)
    return selected, disclosure_path, True


def _recovery(
    args: argparse.Namespace,
    source_output_dir: Path,
    output_dir: Path,
) -> int:
    if args.model != "codex/gpt-5.6-sol" or args.reasoning_effort != "high":
        print("recover использует только codex/gpt-5.6-sol с reasoning=high", file=sys.stderr)
        return 2
    try:
        cases, disclosure_path, prepared = _prepare_recovery_snapshot(
            args,
            source_output_dir,
            output_dir,
        )
    except (OSError, UnicodeError, ValueError, FileExistsError) as exc:
        print(f"recover не подготовлен: {exc}", file=sys.stderr)
        return 2

    disclosure_sha256 = _sha256_file(disclosure_path)
    disclosure = json.loads(disclosure_path.read_text(encoding="utf-8"))
    preflight = {
        "prepared_now": prepared,
        "output_dir": str(output_dir),
        "case_count": len(cases),
        "image_count": disclosure.get("image_count"),
        "text_chars": disclosure.get("text_chars"),
        "classification": str(output_dir / "recovery_classification.json"),
        "disclosure": str(disclosure_path),
        "disclosure_sha256": disclosure_sha256,
        "external_model": "codex/gpt-5.6-sol",
        "reasoning_effort": "high",
    }
    print(json.dumps({"recovery_preflight": preflight}, ensure_ascii=False, indent=2))
    if not cases:
        print("recover: кейсов с новым проверяемым контекстом нет", file=sys.stderr)
        return 5
    if not args.confirm_external_codex:
        print("recover подготовлен локально. Подтвердите показанный disclosure SHA-256.", file=sys.stderr)
        return 6
    if str(args.confirm_disclosure_sha256 or "").strip().lower() != disclosure_sha256:
        print("confirm-disclosure-sha256 не совпадает с frozen disclosure", file=sys.stderr)
        return 6
    try:
        cases, verified_max_batch_images = _verify_external_disclosure(
            output_dir=output_dir,
            disclosure_path=disclosure_path,
            batch_size=max(1, args.batch_size),
            requested_max_batch_images=max(12, int(args.max_batch_images)),
        )
    except (OSError, UnicodeError, ValueError) as exc:
        print(f"frozen disclosure не прошёл повторную проверку: {exc}", file=sys.stderr)
        return 6

    from backend.app.services.llm.codex_runner import find_codex_cli

    cli = find_codex_cli()
    if not cli:
        print("Codex CLI не найден. Авторизуйте подписочную Codex-сессию.", file=sys.stderr)
        return 3
    print(f"[recover] Codex CLI: {cli}", flush=True)
    print("[recover] model=codex/gpt-5.6-sol; reasoning=high; resume=on", flush=True)
    run_summary = asyncio.run(run_codex_audit(
        cases,
        results_path=output_dir / "results.jsonl",
        model="codex/gpt-5.6-sol",
        reasoning_effort="high",
        timeout=max(30, args.timeout),
        batch_size=max(1, args.batch_size),
        max_batch_images=verified_max_batch_images,
        limit=max(0, args.limit),
        max_calls=max(0, args.max_calls),
        delay_seconds=max(0.0, min(args.delay, 60.0)),
        only_case_ids=set(args.case_id) or None,
        progress=lambda message: print(f"[recover] {message}", flush=True),
    ))
    report_summary = generate_report(
        cases,
        output_dir=output_dir,
        results_path=output_dir / "results.jsonl",
    )
    print(json.dumps({"run": run_summary, "report": report_summary}, ensure_ascii=False, indent=2))
    if run_summary.get("halted_reason"):
        return 4
    return 5 if int((run_summary.get("counts") or {}).get("errors") or 0) > 0 else 0


def _auto_retry(
    args: argparse.Namespace,
    source_output_dir: Path,
    output_dir: Path,
) -> int:
    if args.model != "codex/gpt-5.6-sol" or args.reasoning_effort != "high":
        print("auto-retry использует только codex/gpt-5.6-sol с reasoning=high", file=sys.stderr)
        return 2
    try:
        cases, disclosure_path, prepared = _prepare_auto_retry_snapshot(
            args,
            source_output_dir,
            output_dir,
        )
    except (OSError, UnicodeError, ValueError, FileExistsError) as exc:
        print(f"auto-retry не подготовлен: {exc}", file=sys.stderr)
        return 2

    disclosure_sha256 = _sha256_file(disclosure_path)
    disclosure = json.loads(disclosure_path.read_text(encoding="utf-8"))
    preflight = {
        "prepared_now": prepared,
        "output_dir": str(output_dir),
        "selected_case_ids": [case.get("case_id") for case in cases],
        "case_count": len(cases),
        "image_count": disclosure.get("image_count"),
        "text_chars": disclosure.get("text_chars"),
        "disclosure": str(disclosure_path),
        "disclosure_sha256": disclosure_sha256,
        "external_model": "codex/gpt-5.6-sol",
        "reasoning_effort": "high",
    }
    print(json.dumps({"auto_retry_preflight": preflight}, ensure_ascii=False, indent=2))
    if not cases:
        print("auto-retry: подходящих кейсов с найденным контекстом нет", file=sys.stderr)
        return 5
    if not args.confirm_external_codex:
        print(
            "auto-retry подготовлен локально. Перед внешним запуском подтвердите показанный disclosure SHA-256.",
            file=sys.stderr,
        )
        return 6
    if str(args.confirm_disclosure_sha256 or "").strip().lower() != disclosure_sha256:
        print("confirm-disclosure-sha256 не совпадает с замороженным disclosure", file=sys.stderr)
        return 6
    try:
        cases, verified_max_batch_images = _verify_external_disclosure(
            output_dir=output_dir,
            disclosure_path=disclosure_path,
            batch_size=max(1, args.batch_size),
            requested_max_batch_images=max(
                1,
                int(args.max_batch_images),
                int(args.max_images_per_case),
            ),
        )
    except (OSError, UnicodeError, ValueError) as exc:
        print(f"frozen disclosure не прошёл повторную проверку: {exc}", file=sys.stderr)
        return 6

    from backend.app.services.llm.codex_runner import find_codex_cli

    cli = find_codex_cli()
    if not cli:
        print("Codex CLI не найден. Авторизуйте подписочную Codex-сессию.", file=sys.stderr)
        return 3
    print(f"[auto-retry] Codex CLI: {cli}", flush=True)
    print("[auto-retry] model=codex/gpt-5.6-sol; reasoning=high; depth=1/1; resume=on", flush=True)
    run_summary = asyncio.run(
        run_codex_audit(
            cases,
            results_path=output_dir / "results.jsonl",
            model="codex/gpt-5.6-sol",
            reasoning_effort="high",
            timeout=max(30, args.timeout),
            batch_size=max(1, args.batch_size),
            max_batch_images=verified_max_batch_images,
            limit=max(0, args.limit),
            max_calls=max(0, args.max_calls),
            delay_seconds=max(0.0, min(args.delay, 60.0)),
            only_case_ids=set(args.case_id) or None,
            progress=lambda message: print(f"[auto-retry] {message}", flush=True),
        )
    )
    report_summary = generate_report(
        cases,
        output_dir=output_dir,
        results_path=output_dir / "results.jsonl",
    )
    print(json.dumps({"run": run_summary, "report": report_summary}, ensure_ascii=False, indent=2))
    if run_summary.get("halted_reason"):
        return 4
    if int((run_summary.get("counts") or {}).get("errors") or 0) > 0:
        return 5
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    source_output_dir = (
        args.source_output_dir.resolve() if args.source_output_dir else None
    )
    if args.command in {"retrieve", "auto-retry", "recover"} and args.output_dir is None:
        if source_output_dir is None:
            print(f"{args.command} требует --source-output-dir", file=sys.stderr)
            return 2
        if args.command == "retrieve":
            raw_output_dir = source_output_dir / f"retrieval-pilot-{max(1, int(args.limit or 0))}"
        elif args.command == "auto-retry":
            raw_output_dir = _default_auto_retry_output_dir(source_output_dir)
        else:
            raw_output_dir = _default_recovery_output_dir(source_output_dir)
    else:
        raw_output_dir = args.output_dir or (DEFAULT_REPORT_ROOT / args.month)
    try:
        _validate_output_destination(raw_output_dir, args.projects_v2_root)
    except ValueError as exc:
        print(f"небезопасный output-dir: {exc}", file=sys.stderr)
        return 2
    output_dir = raw_output_dir.resolve()
    manifest_path = output_dir / "manifest.jsonl"
    results_path = output_dir / "results.jsonl"

    if args.command == "auto-retry":
        if source_output_dir is None:
            print("auto-retry требует --source-output-dir", file=sys.stderr)
            return 2
        if source_output_dir == output_dir:
            print("auto-retry требует отдельный output-dir", file=sys.stderr)
            return 2
        return _auto_retry(args, source_output_dir, output_dir)

    if args.command == "recover":
        if source_output_dir is None:
            print("recover требует --source-output-dir", file=sys.stderr)
            return 2
        if source_output_dir == output_dir:
            print("recover требует отдельный output-dir", file=sys.stderr)
            return 2
        return _recovery(args, source_output_dir, output_dir)

    if args.command == "retrieve":
        if source_output_dir is None:
            print("retrieve требует --source-output-dir", file=sys.stderr)
            return 2
        if source_output_dir == output_dir:
            print("retrieve требует отдельный --output-dir; source и destination совпадают", file=sys.stderr)
            return 2
        if int(args.limit or 0) <= 0:
            print("retrieve требует --limit больше нуля", file=sys.stderr)
            return 2
        if manifest_path.exists():
            print(
                f"destination manifest уже существует: {manifest_path}; выберите новый --output-dir",
                file=sys.stderr,
            )
            return 2

        source_manifest_path = source_output_dir / "manifest.jsonl"
        source_results_path = source_output_dir / "results.jsonl"
        source_inventory = _read_inventory(source_output_dir)
        if not source_manifest_path.is_file() or not source_results_path.is_file():
            print(
                "retrieve требует source manifest.jsonl и results.jsonl",
                file=sys.stderr,
            )
            return 2
        scope_errors = _frozen_scope_errors(args, source_inventory, check_filters=False)
        if scope_errors:
            print("source frozen scope mismatch: " + "; ".join(scope_errors), file=sys.stderr)
            return 2

        source_cases = load_manifest(source_manifest_path)
        source_results, malformed = load_latest_results(source_results_path)
        selected, retrieval_counts = prepare_retrieval_cases(
            source_cases,
            source_results,
            limit=int(args.limit),
            only_case_ids=set(args.case_id) or None,
            max_images_per_case=max(0, int(args.max_images_per_case)),
            asset_dir=output_dir / "context_assets",
            allow_remote_crops=bool(args.remote_crops),
        )
        inventory = json.loads(json.dumps(source_inventory, ensure_ascii=False))
        inventory.update({
            "schema_version": 2,
            "generated_at": utc_now_iso(),
            "source": "retrieval second pass over frozen rejected-findings audit",
            "counts": retrieval_counts,
            "by_object": dict(Counter(case.get("object_name") for case in selected)),
            "by_discipline": dict(Counter(case.get("discipline") for case in selected)),
            "by_reviewer": dict(Counter(case.get("expert_reviewer") for case in selected)),
            "by_day": dict(Counter(str(case.get("expert_timestamp_local") or "")[:10] for case in selected)),
            "by_route": dict(Counter((case.get("context") or {}).get("route") for case in selected)),
            "retrieval": {
                "contract_version": AUTO_RETRIEVAL_CONTRACT_VERSION,
                "scope": "same_document_same_version_only",
                "source_output_dir": str(source_output_dir),
                "source_manifest": str(source_manifest_path),
                "source_manifest_sha256": hashlib.sha256(
                    source_manifest_path.read_bytes()
                ).hexdigest(),
                "source_results": str(source_results_path),
                "source_malformed_result_lines": malformed,
                "requested_limit": int(args.limit),
                "remote_crops_enabled": bool(args.remote_crops),
                "selected_case_ids": [case["case_id"] for case in selected],
            },
        })
        written_manifest, written_inventory = write_manifest(
            output_dir,
            selected,
            inventory,
        )
        summary = generate_report(
            selected,
            output_dir=output_dir,
            results_path=results_path,
        )
        print(
            json.dumps(
                {
                    "retrieval": retrieval_counts,
                    "manifest": str(written_manifest),
                    "inventory": str(written_inventory),
                    "report": summary,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0 if len(selected) == int(args.limit) else 5

    if args.command == "report":
        if not manifest_path.is_file():
            print(f"manifest не найден: {manifest_path}", file=sys.stderr)
            return 2
        inventory = _read_inventory(output_dir)
        scope_errors = _frozen_scope_errors(args, inventory, check_filters=False)
        if scope_errors:
            print("frozen manifest scope mismatch: " + "; ".join(scope_errors), file=sys.stderr)
            return 2
        cases = load_manifest(manifest_path)
        summary = generate_report(cases, output_dir=output_dir, results_path=results_path)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    if args.command == "run" and args.reuse_manifest:
        if not manifest_path.is_file():
            print(f"manifest не найден: {manifest_path}", file=sys.stderr)
            return 2
        inventory = _read_inventory(output_dir)
        scope_errors = _frozen_scope_errors(args, inventory, check_filters=True)
        if scope_errors:
            print("frozen manifest scope mismatch: " + "; ".join(scope_errors), file=sys.stderr)
            return 2
        cases = load_manifest(manifest_path)
        print(f"[run] frozen period={inventory.get('period')}; filters={inventory.get('filters')}", flush=True)
        print(f"[run] используется замороженный manifest: {len(cases)} кейсов", flush=True)
    else:
        cases = _prepare(args, output_dir)

    if args.command == "prepare":
        summary = generate_report(cases, output_dir=output_dir, results_path=results_path)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    if not args.confirm_external_codex:
        print("run требует --confirm-external-codex: выбранный контекст будет передан подписочному Codex", file=sys.stderr)
        return 6

    only_case_ids = set(args.case_id)
    if args.hybrid_source_results:
        try:
            hybrid_ids = _hybrid_second_pass_case_ids(
                args.hybrid_source_results,
                cases,
            )
        except (OSError, UnicodeError) as exc:
            print(f"hybrid source results недоступен: {exc}", file=sys.stderr)
            return 2
        only_case_ids.update(hybrid_ids)
        print(
            f"[run] гибридный Sol-отбор: {len(hybrid_ids)} кейсов из {args.hybrid_source_results}",
            flush=True,
        )
        if not only_case_ids:
            print("[run] кейсов для второго прохода нет; Codex не вызывается", flush=True)
            summary = generate_report(cases, output_dir=output_dir, results_path=results_path)
            print(json.dumps(summary, ensure_ascii=False, indent=2))
            return 0

    from backend.app.services.llm.codex_runner import find_codex_cli

    cli = find_codex_cli()
    if not cli:
        print("Codex CLI не найден. Авторизуйте подписочную Codex-сессию и повторите run.", file=sys.stderr)
        return 3
    print(f"[run] Codex CLI: {cli}", flush=True)
    print(
        f"[run] model={args.model}; reasoning={args.reasoning_effort}; "
        f"batch={args.batch_size}; resume=on",
        flush=True,
    )
    run_summary = asyncio.run(
        run_codex_audit(
            cases,
            results_path=results_path,
            model=args.model,
            reasoning_effort=args.reasoning_effort,
            timeout=max(30, args.timeout),
            batch_size=max(1, args.batch_size),
            max_batch_images=max(1, args.max_batch_images),
            limit=max(0, args.limit),
            max_calls=max(0, args.max_calls),
            delay_seconds=max(0.0, min(args.delay, 60.0)),
            only_case_ids=only_case_ids or None,
            progress=lambda message: print(f"[run] {message}", flush=True),
        )
    )
    report_summary = generate_report(cases, output_dir=output_dir, results_path=results_path)
    print(json.dumps({"run": run_summary, "report": report_summary}, ensure_ascii=False, indent=2))
    if run_summary.get("halted_reason"):
        return 4
    if int((run_summary.get("counts") or {}).get("errors") or 0) > 0:
        return 5
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
