#!/usr/bin/env python3
"""Backfill shadow-пакетов профиля «АР. План потолков и освещения» для
реальных блоков проекта.

Для каждого выбранного image-блока версии:
  1) берёт исходный вектор-PDF (02_work/document.pdf, страница блока);
  2) запускает детерминированный профиль ar_ceiling_lighting (без LLM);
  3) пишет shadow block_graph_package в штатный каталог артефактов
     версии: block_vector_graphs/<block_id>.ar_ceiling_lighting.json
     (+ sidecar _graphs/<block_id>.ar_ceiling_lighting.graph.json).

Суффиксное имя выбрано намеренно: production-читатель Stage 01/02
(`load_prepared_package`) ищет строго <block_id>.json и shadow-артефакт
НЕ видит — аудит, findings и промпты не меняются. Читает shadow только
endpoint /blocks/llm-text (поле profiled_graph_markdown_full).

Примеры:
    python scripts/backfill_ar_ceiling_lighting_graphs.py \
      --project-id "13АВ-РД-АР4.2-К6" --version-id v001 --block-id YF7P-R6DK-PXT

    python scripts/backfill_ar_ceiling_lighting_graphs.py \
      --project-dir "projects_v2/objects/<объект>/disciplines/AR/documents/<док>/versions/v001"
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from backend.app.pipeline.stages.block_grounding.ar_ceiling_lighting import (  # noqa: E402
    PROFILE_ID, PROFILE_VERSION, run_profile)
from backend.app.pipeline.stages.block_grounding.ar_ceiling_lighting.render_md import (  # noqa: E402
    render_markdown)
from backend.app.pipeline.stages.block_grounding.ar_ceiling_lighting.registry import (  # noqa: E402
    load_legend_registry)
from backend.app.pipeline.stages.block_grounding.block_profile_registry import (  # noqa: E402
    ARTIFACT_DIRNAME, artifact_filename, make_package)

SHADOW_SUFFIX = ".ar_ceiling_lighting.json"
DEFAULT_REGISTRY = ROOT / "experiments/vectograf/ar_ceiling_lighting/legend_registry.json"


def shadow_artifact_path(output_dir: Path, block_id: str) -> Path:
    base = artifact_filename(block_id)
    if base.endswith(".json"):
        base = base[:-5]
    return Path(output_dir) / ARTIFACT_DIRNAME / f"{base}{SHADOW_SUFFIX}"


def _resolve_output_dir(args) -> tuple[Path, Path, str]:
    """(output_dir, version_dir, project_id) из --project-id или --project-dir."""
    if args.project_dir:
        version_dir = Path(args.project_dir)
        for cand in (version_dir / "03_analysis" / "latest", version_dir / "_output"):
            if cand.is_dir():
                return cand, version_dir, version_dir.name
        raise SystemExit(f"ОШИБКА: не найден каталог артефактов в {version_dir}")
    from backend.app.services.common import version_service
    ctx = version_service.resolve_project_version_context(args.project_id, args.version_id)
    output_dir = Path(ctx["output_dir"])
    version_dir = Path(ctx.get("version_dir") or output_dir.parent.parent)
    return output_dir, version_dir, args.project_id


def _iter_blocks(output_dir: Path, version_dir: Path, wanted: set[str]) -> list[dict]:
    """Image-блоки версии: из blocks-индекса, иначе из document_graph.json."""
    blocks: list[dict] = []
    try:
        from backend.app.pipeline.stages.block_context.contract import resolve_blocks_index
        index_path = resolve_blocks_index(output_dir)
        if index_path and Path(index_path).is_file():
            data = json.loads(Path(index_path).read_text(encoding="utf-8"))
            for b in data.get("blocks") or []:
                if b.get("block_id"):
                    blocks.append({"block_id": b["block_id"], "page": b.get("page")})
    except Exception:
        pass
    if not blocks:
        dgp = output_dir / "document_graph.json"
        if dgp.is_file():
            dg = json.loads(dgp.read_text(encoding="utf-8"))
            for page in dg.get("pages") or []:
                for b in page.get("image_blocks") or page.get("blocks") or []:
                    bid = b.get("block_id") or b.get("id")
                    if bid:
                        blocks.append({"block_id": bid,
                                       "page": b.get("page") or page.get("page")
                                       or page.get("page_num")})
    if wanted:
        blocks = [b for b in blocks if b["block_id"] in wanted]
    seen = set()
    uniq = []
    for b in blocks:
        if b["block_id"] not in seen:
            seen.add(b["block_id"])
            uniq.append(b)
    return uniq


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--project-id", default=None)
    parser.add_argument("--version-id", default=None)
    parser.add_argument("--project-dir", default=None,
                        help="путь к каталогу версии (альтернатива --project-id)")
    parser.add_argument("--block-id", action="append", default=[],
                        help="обработать только эти block_id (повторяемый)")
    parser.add_argument("--legend-registry", default=str(DEFAULT_REGISTRY))
    parser.add_argument("--pdf", default=None,
                        help="явный путь к вектор-PDF (default: 02_work/document.pdf версии)")
    parser.add_argument("--corpus-dir", default=None,
                        help="каталог одностраничных вектор-PDF блоков: PDF блока ищется "
                             "по block_id в имени файла (галерейные проекты, где у всех "
                             "блоков page=1 и общий document.pdf — склейка)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if not args.project_id and not args.project_dir:
        parser.error("нужен --project-id или --project-dir")

    output_dir, version_dir, project_id = _resolve_output_dir(args)
    pdf_path = Path(args.pdf) if args.pdf else version_dir / "02_work" / "document.pdf"
    corpus_pdfs: dict[str, Path] = {}
    if args.corpus_dir:
        for p in sorted(Path(args.corpus_dir).rglob("*.pdf")):
            stem = p.stem
            # block_id — последний токен имени (после последнего тире/пробела)
            tail = stem.replace("—", "-").split("-")
            for n in (3, 2, 1):
                token = "-".join(t.strip() for t in tail[-n:]).strip()
                if token:
                    corpus_pdfs.setdefault(token, p)
    if not pdf_path.is_file() and not corpus_pdfs:
        print(f"ОШИБКА: PDF не найден: {pdf_path}", file=sys.stderr)
        return 2
    registry_entries = load_legend_registry(args.legend_registry)

    blocks = _iter_blocks(output_dir, version_dir, set(args.block_id))
    if not blocks:
        print("ОШИБКА: блоки не найдены (index.json/document_graph.json)", file=sys.stderr)
        return 2
    print(f"Проект {project_id}: блоков к обработке {len(blocks)}; PDF: {pdf_path}")

    sha_cache: dict[Path, str] = {}

    def sha_of(path: Path) -> str:
        if path not in sha_cache:
            sha_cache[path] = hashlib.sha256(path.read_bytes()).hexdigest()
        return sha_cache[path]

    written, skipped = [], []
    for b in blocks:
        block_id = b["block_id"]
        page = b.get("page")
        block_pdf = corpus_pdfs.get(block_id)
        if block_pdf is not None:
            src_pdf, page_index = block_pdf, 0
        elif pdf_path.is_file() and page:
            src_pdf, page_index = pdf_path, int(page) - 1
        else:
            skipped.append((block_id, "нет источника PDF (ни corpus-dir, ни страницы)"))
            continue
        result = run_profile(str(src_pdf), page_index=page_index, block_id=block_id,
                             legend_registry=registry_entries)
        status = result["status"]
        if status in ("no_graph", "error"):
            skipped.append((block_id, f"{status}: {result.get('reason')}"))
            continue
        graph = result["graph"]
        md = render_markdown(graph)
        package = make_package(
            block_id=block_id, page=page, source_kind="structured_architecture",
            user_text=None,  # shadow: в LLM этот пакет не уходит
            graph=None, markdown=md, profile_id=PROFILE_ID,
            gate={"use": False, "shadow": True,
                  "reasons": ["shadow-профиль ar_ceiling_lighting, в Stage 01/02 не подаётся"]},
        )
        package.update({
            "profile_id": PROFILE_ID,
            "profile_version": PROFILE_VERSION,
            "status": status,
            "source_pdf": src_pdf.name,
            "source_sha256": sha_of(src_pdf),
            "warnings": graph.get("warnings") or [],
            "conflicts": graph.get("conflicts") or [],
            "ledger_summary": _ledger_summary(graph),
            "validation": graph.get("validation") or {},
            "provenance": {
                "method": "deterministic_vector_layer",
                "llm": False, "ocr": False,
                "legend_registry": str(args.legend_registry),
            },
            "graph_artifact": f"_graphs/{artifact_filename(block_id)[:-5]}.ar_ceiling_lighting.graph.json",
        })
        shadow_path = shadow_artifact_path(output_dir, block_id)
        graph_path = shadow_path.parent / "_graphs" / \
            f"{artifact_filename(block_id)[:-5]}.ar_ceiling_lighting.graph.json"
        if args.dry_run:
            written.append((block_id, status, str(shadow_path), "dry-run"))
            continue
        shadow_path.parent.mkdir(parents=True, exist_ok=True)
        graph_path.parent.mkdir(parents=True, exist_ok=True)
        graph_path.write_text(json.dumps(graph, ensure_ascii=False, indent=1, sort_keys=True),
                              encoding="utf-8")
        shadow_path.write_text(json.dumps(package, ensure_ascii=False, indent=1, sort_keys=True),
                               encoding="utf-8")
        written.append((block_id, status, str(shadow_path), f"{len(md)} байт MD"))

    for block_id, status, path, note in written:
        print(f"  ✔ {block_id}: {status} → {path} ({note})")
    for block_id, why in skipped:
        print(f"  – {block_id}: пропущен ({why})")
    print(f"Итого записано {len(written)}, пропущено {len(skipped)}")
    return 0


def _ledger_summary(graph: dict) -> dict:
    counts: dict[str, int] = {}
    for item in graph.get("semantic_ledger") or []:
        counts[item["kind"]] = counts.get(item["kind"], 0) + 1
    return dict(sorted(counts.items()))


if __name__ == "__main__":
    raise SystemExit(main())
