#!/usr/bin/env python3
"""Пакетный прогон профиля «АР. План потолков и освещения» по корпусу PDF.

Автоматически находит все однотипные PDF по нормализованному имени
(Unicode NFKC, нижний регистр, схлопывание пробелов, эквивалентность
тире —/–/-), строит кросс-листовой реестр легенд, обрабатывает каждый
файл (сбой одного файла не останавливает пакет) и пишет:

  <out-dir>/<block_slug>/description_full.md, semantic_graph.json,
      metrics.json, diagnostic_overlay.svg, compact_fixture.json, README.md
  corpus_manifest.json   — реестр файлов корпуса (sha256, slug, статус)
  legend_registry.json   — кросс-листовой реестр условных обозначений
  transferability_matrix.md — сводная матрица переносимости

Пример:
    python scripts/build_ar_ceiling_lighting_corpus.py \
      --corpus-dir "experiments/блоки разных дисциплин/АР" \
      --name-filter "план потолка и освещения — потолок и освещение" \
      --out-dir "experiments/vectograf/ar_ceiling_lighting/corpus"
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from backend.app.pipeline.stages.block_grounding.ar_ceiling_lighting import (  # noqa: E402
    run_profile, write_artifacts)
from backend.app.pipeline.stages.block_grounding.ar_ceiling_lighting.registry import (  # noqa: E402
    build_legend_registry, load_legend_registry, save_legend_registry)

DASHES_RE = re.compile(r"[—–‐‑‒−]")


def normalize_name(name: str) -> str:
    """NFKC → нижний регистр → все тире в '-' → схлопнуть пробелы."""
    s = unicodedata.normalize("NFKC", name)
    s = s.casefold()
    s = DASHES_RE.sub("-", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def name_matches(name: str, name_filter: str) -> bool:
    """Файл подходит, если нормализованное имя содержит ОБЕ смысловые части
    фильтра (части фильтра разделены тире)."""
    norm = normalize_name(name)
    parts = [p.strip() for p in normalize_name(name_filter).split("-") if len(p.strip()) > 3]
    if not parts:
        parts = [normalize_name(name_filter)]
    return all(part in norm for part in parts)


def block_slug(pdf_path: Path) -> str:
    """Стабильный slug блока: последний фрагмент имени после тире."""
    stem = unicodedata.normalize("NFKC", pdf_path.stem)
    frags = [f.strip() for f in DASHES_RE.sub("—", stem).split("—") if f.strip()]
    tail = frags[-1] if frags else stem
    slug = re.sub(r"[^0-9A-Za-zА-Яа-я_-]+", "_", tail).strip("_-")
    if not slug or len(slug) < 3:
        slug = hashlib.sha256(pdf_path.name.encode("utf-8")).hexdigest()[:12]
    return slug


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def discover(corpus_dir: Path, name_filter: str) -> list[Path]:
    files = [p for p in sorted(corpus_dir.rglob("*.pdf"))
             if name_matches(p.name, name_filter)]
    return files


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--corpus-dir", required=True)
    parser.add_argument("--name-filter",
                        default="план потолка и освещения — потолок и освещение")
    parser.add_argument("--out-dir",
                        default="experiments/vectograf/ar_ceiling_lighting/corpus")
    parser.add_argument("--registry-out",
                        default="experiments/vectograf/ar_ceiling_lighting/legend_registry.json")
    parser.add_argument("--manifest-out",
                        default="experiments/vectograf/ar_ceiling_lighting/corpus_manifest.json")
    parser.add_argument("--matrix-out",
                        default="experiments/vectograf/ar_ceiling_lighting/transferability_matrix.md")
    parser.add_argument("--include-raw-inventory", action="store_true",
                        help="писать полный raw_vector_inventory.json (большой)")
    args = parser.parse_args()

    corpus_dir = Path(args.corpus_dir)
    if not corpus_dir.is_dir():
        print(f"ОШИБКА: каталог не найден: {corpus_dir}", file=sys.stderr)
        return 2
    files = discover(corpus_dir, args.name_filter)
    if not files:
        print("ОШИБКА: ни один PDF не подошёл под фильтр имени", file=sys.stderr)
        return 2
    print(f"Найдено файлов: {len(files)}")
    for p in files:
        print(f"  - {p.name}")

    # --- кросс-листовой реестр легенд (строится ДО обработки блоков) ---
    registry = build_legend_registry([str(p) for p in files])
    registry_path = save_legend_registry(registry, args.registry_out)
    print(f"Реестр легенд: {registry_path} "
          f"(записей {len(registry['entries'])}, конфликтов {len(registry['conflicts'])})")
    registry_entries = load_legend_registry(registry_path)

    out_root = Path(args.out_dir)
    out_root.mkdir(parents=True, exist_ok=True)

    manifest = {"schema_version": 1, "profile_id": "ar_ceiling_lighting",
                "name_filter": args.name_filter, "files": []}
    matrix_rows = []
    for pdf in files:
        slug = block_slug(pdf)
        result_dir = out_root / slug
        entry = {
            "source_path": str(pdf),
            "source_name": pdf.name,
            "normalized_name": normalize_name(pdf.name),
            "sha256": sha256_of(pdf),
            "block_slug": slug,
            "status": None,
            "applicable": None,
            "reason": None,
            "result_dir": str(result_dir),
        }
        try:
            result = run_profile(str(pdf), block_id=slug,
                                 legend_registry=registry_entries)
            entry["status"] = result["status"]
            entry["reason"] = result.get("reason")
            entry["applicable"] = result["status"] not in ("no_graph",)
            write_artifacts(result, str(result_dir), pdf_path=str(pdf),
                            include_raw_inventory=args.include_raw_inventory,
                            markdown_name="description_full.md",
                            markdown_compact_name="description_compact.md",
                            markdown_audit_name="description_audit.md")
            matrix_rows.append(_matrix_row(pdf.name, result))
        except Exception as exc:  # noqa: BLE001 — пакет не останавливается
            entry["status"] = "error"
            entry["reason"] = f"{type(exc).__name__}: {exc}"
            entry["applicable"] = False
            matrix_rows.append({"file": pdf.name, "status": "error",
                                "reason": entry["reason"]})
        manifest["files"].append(entry)
        print(f"  {slug}: {entry['status']}"
              + (f" ({entry['reason']})" if entry.get("reason") else ""))

    manifest_path = Path(args.manifest_out)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=1,
                                        sort_keys=True), encoding="utf-8")
    matrix_path = Path(args.matrix_out)
    matrix_path.write_text(_render_matrix(matrix_rows, registry), encoding="utf-8")
    print(f"Манифест: {manifest_path}")
    print(f"Матрица:  {matrix_path}")
    return 0


def _matrix_row(name: str, result: dict) -> dict:
    row = {"file": name, "status": result["status"], "reason": result.get("reason"),
           "warnings": list(result.get("warnings") or []),
           "elapsed_s": result.get("elapsed_s")}
    gate = result.get("gate") or {}
    row["legend_rows"] = (gate.get("signals") or {}).get("legend_profile_rows", 0)
    graph = result.get("graph")
    if graph:
        v = graph["validation"]
        row.update({
            "apartments": v["apartments_total"],
            "rooms": v["rooms_total"],
            "ceiling_tags": v["ceiling_zones_total"],
            "lights": v["lights_total"],
            "wall_lights": v.get("wall_lights_total", 0),
            "switch_1": sum(1 for s in graph["switches"] if s["kind"] == "switch_1"),
            "switch_2": sum(1 for s in graph["switches"] if s["kind"] == "switch_2"),
            "changeover": sum(1 for s in graph["switches"] if s["kind"] == "switch_changeover"),
            "masters": v["master_switches_total"],
            "groups": v["groups_confirmed"] + v["groups_incomplete"],
            "groups_confirmed": v["groups_confirmed"],
            "groups_incomplete": v["groups_incomplete"],
            "unassigned": (v["lights_total"] - v["lights_in_rooms"])
                          + (v["switches_total"] - v["switches_in_rooms"])
                          + v["unresolved_symbols_total"],
            "conflicts": v["conflicts_total"],
        })
    return row


def _render_matrix(rows: list[dict], registry: dict) -> str:
    cols = [("file", "Файл"), ("status", "Статус"), ("apartments", "Кварт."),
            ("rooms", "Помещ."), ("ceiling_tags", "Потолоч. марки"),
            ("lights", "Свет. точки"), ("wall_lights", "Настенные"),
            ("switch_1", "Выкл. 1-кл"), ("switch_2", "Выкл. 2-кл"),
            ("changeover", "Перекл."), ("masters", "Мастер"),
            ("groups", "Группы"), ("groups_confirmed", "Полные"),
            ("groups_incomplete", "Неполные"), ("legend_rows", "Строк легенды"),
            ("unassigned", "Непривяз."), ("conflicts", "Конфл."),
            ("elapsed_s", "Время, с")]
    out = ["# Матрица переносимости профиля «АР. План потолков и освещения»", ""]
    out.append("Реестр легенд: записей "
               f"{len(registry['entries'])}, конфликтов {len(registry['conflicts'])}.")
    out.append("")
    out.append("| " + " | ".join(title for _, title in cols) + " |")
    out.append("|" + "|".join("---" for _ in cols) + "|")
    for row in rows:
        cells = []
        for key, _ in cols:
            val = row.get(key)
            cells.append("—" if val is None else str(val))
        out.append("| " + " | ".join(cells) + " |")
    out.append("")
    out.append("## Предупреждения и причины partial/no_graph/error")
    out.append("")
    for row in rows:
        notes = list(row.get("warnings") or [])
        if row.get("reason"):
            notes.insert(0, f"reason={row['reason']}")
        if notes:
            out.append(f"- **{row['file']}** — " + "; ".join(notes))
    if not any((row.get("warnings") or row.get("reason")) for row in rows):
        out.append("- нет")
    out.append("")
    return "\n".join(out)


if __name__ == "__main__":
    raise SystemExit(main())
