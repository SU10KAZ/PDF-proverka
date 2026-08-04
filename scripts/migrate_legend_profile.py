#!/usr/bin/env python3
"""Перенос блоков «Условные обозначения» в профиль `legend`.

Легенда встречается в любой дисциплине, но профиля для неё не существовало:
такие блоки наследовали тип листа («план потолка и освещения»), и вся
расшифровка марок терялась. Скрипт переводит подтверждённые блоки в новый
универсальный профиль и перестраивает их графы построчным разбором легенды.

Трогает три места, чтобы перенос пережил пересборку каталога:
  1) исследовательский корпус `experiments/блоки разных дисциплин` — источник
     для `build_catalog.py`;
  2) production-каталог `reference_catalog/disciplines/*.json` + manifest;
  3) витрину «ВЕКТОГРАФ — <дисциплина>» объекта в projects_v2.

По умолчанию — сухой прогон. Запись только с `--apply`; исходные файлы
сохраняются рядом с суффиксом `.before_legend_profile`.
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.app.pipeline.stages.block_grounding.legend_geometry import (  # noqa: E402
    PROFILE_LEGEND, build_legend_graph, evaluate_legend_gate,
    add_legend_secondary_description, render_legend_markdown,
)

REPO = Path(__file__).resolve().parents[1]
GALLERY_ROOT = REPO / ("projects_v2/objects/214_Alia_ASTERUS/disciplines/POS/documents")
CATALOG = REPO / "backend/app/pipeline/stages/block_context/reference_catalog"
CATALOG_FILES = {
    "АР": "AR.json", "ВК": "VK.json", "ГП": "GP.json", "КЖ": "KJ.json",
    "КМ": "KM.json", "ОВ": "HVAC.json", "СС": "SS.json", "ТХ": "TX.json",
    "ЭОМ": "EOM.json",
}
SUBTYPE = "условные обозначения"
BACKUP_SUFFIX = ".before_legend_profile"


def _backup(path: Path, apply: bool) -> None:
    target = path.with_suffix(path.suffix + BACKUP_SUFFIX)
    if apply and not target.exists():
        shutil.copy2(path, target)


def _write(path: Path, payload, apply: bool) -> None:
    if not apply:
        return
    _backup(path, apply)
    # Формат — как у build_catalog._write: иначе перезапись переформатирует
    # весь каталог и правка 14 записей утонет в диффе на десятки тысяч строк.
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
                    encoding="utf-8")


def _signature(graph: dict) -> dict:
    v = graph.get("validation") or {}
    return {
        "node_types": dict(v.get("node_types") or {}),
        "counts": {
            "nodes_total": v.get("nodes_total", 0),
            "containers_total": v.get("containers_total", 0),
            "networks_total": v.get("networks_total", 0),
            "edges_total": v.get("edges_total", 0),
            "legend_entries_total": v.get("legend_entries_total", 0),
            "legend_entries_with_code": v.get("legend_entries_with_code", 0),
            "legend_values_total": v.get("legend_values_total", 0),
            "legend_values_text_confirmed": v.get("legend_values_text_confirmed", 0),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--selection", default="scratchpad/legend_candidates.jsonl")
    parser.add_argument("--apply", action="store_true", help="записать изменения")
    args = parser.parse_args()

    rows = [json.loads(line) for line in
            (REPO / args.selection).read_text(encoding="utf-8").splitlines() if line.strip()]
    targets = [r for r in rows if r.get("verdict") == "legend"]
    print(f"Блоков к переносу: {len(targets)}"
          f"{'' if args.apply else '   (СУХОЙ ПРОГОН — файлы не изменяются)'}\n")

    graphs: dict[str, dict] = {}
    failed = []
    for row in targets:
        pdf = REPO / row["source_pdf"]
        graph = build_legend_graph(pdf, block_id=row["block_id"], subtype_hint=SUBTYPE)
        if graph is None or not evaluate_legend_gate(graph).get("use"):
            failed.append(row["block_id"])
            continue
        graphs[row["block_id"]] = graph
    if failed:
        print(f"⚠ гейт легенды не пройден, блоки пропущены: {', '.join(failed)}\n")

    by_id = {r["block_id"]: r for r in targets if r["block_id"] in graphs}

    # 1) исследовательский корпус — источник пересборки каталога.
    # Граф переписывается целиком, а не переименовывается: у профиля `legend`
    # своя грамматика (строки, коды, параметры), и старый граф плана под новым
    # именем сломал бы и рендер, и сигнатуру каталога. Файлы корпуса и sidecar
    # витрины связаны жёсткой ссылкой — запись в любой из них меняет оба.
    touched_corpus = 0
    for block_id, row in by_id.items():
        src = REPO / row["source_pdf"]
        candidates = sorted(src.parent.glob(f"*/{block_id}.structure.json"))
        for path in candidates:
            before = json.loads(path.read_text(encoding="utf-8")).get("profile_id")
            print(f"  корпус  {block_id}: {before} -> {PROFILE_LEGEND}  ({path.name})")
            _write(path, graphs[block_id], args.apply)
            touched_corpus += 1

    # 1b) манифесты корпусов дисциплин — на них опираются корпусные тесты
    for block_id, row in by_id.items():
        src = REPO / row["source_pdf"]
        for manifest_path in sorted(src.parent.glob("*_CORPUS.json")):
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            cases = payload if isinstance(payload, list) else (payload.get("blocks") or [])
            changed = False
            for case in cases:
                if not isinstance(case, dict) or case.get("block_id") != block_id:
                    continue
                before = case.get("profile_id")
                case["profile_id"] = PROFILE_LEGEND
                case["subtype"] = SUBTYPE
                print(f"  манифест {block_id}: {before} -> {PROFILE_LEGEND}"
                      f"  ({manifest_path.name})")
                changed = True
            if changed:
                _write(manifest_path, payload, args.apply)

    # 2) production-каталог
    print()
    touched_catalog = 0
    for discipline, filename in CATALOG_FILES.items():
        path = CATALOG / "disciplines" / filename
        if not path.exists():
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        changed = False
        for record in payload.get("records") or []:
            block_id = record.get("block_id")
            if block_id not in graphs:
                continue
            before = record.get("profile_id")
            record["profile_id"] = PROFILE_LEGEND
            record["subtype"] = SUBTYPE
            record["structure_signature"] = _signature(graphs[block_id])
            graph_validation = graphs[block_id].get("validation") or {}
            record["covered_facts"] = graph_validation.get("legend_entries_total", 0)
            print(f"  каталог {discipline:<4} {block_id}: {before} -> {PROFILE_LEGEND}")
            changed = True
            touched_catalog += 1
        if changed:
            _write(path, payload, args.apply)

    # 3) витрина ВЕКТОГРАФ в projects_v2
    print()
    touched_gallery = 0
    for doc_dir in sorted(GALLERY_ROOT.iterdir()) if GALLERY_ROOT.exists() else []:
        latest = doc_dir / "versions/v001/03_analysis/latest"
        gallery_path = latest / "vector_graph_gallery.json"
        if not gallery_path.exists():
            continue
        gallery = json.loads(gallery_path.read_text(encoding="utf-8"))
        changed = False
        for block in gallery.get("blocks") or []:
            block_id = block.get("block_id")
            if block_id not in graphs:
                continue
            block["profile_id"] = PROFILE_LEGEND
            changed = True

            pkg_path = latest / "block_vector_graphs" / f"{block_id}.json"
            if pkg_path.exists():
                pkg = json.loads(pkg_path.read_text(encoding="utf-8"))
                graph = graphs[block_id]
                add_legend_secondary_description(
                    graph, (pkg.get("classification") or {}).get("description"))
                gate = evaluate_legend_gate(graph)
                markdown = render_legend_markdown(graph)
                before = pkg.get("profile_id")
                pkg["profile_id"] = PROFILE_LEGEND
                pkg["source_kind"] = "structured_legend"
                pkg.setdefault("classification", {})["profile_id"] = PROFILE_LEGEND
                pkg["classification"]["source"] = "legend_rows_pdf"
                pkg["validation"] = graph.get("validation")
                pkg["gate"] = {k: gate[k] for k in ("use", "complete") if k in gate}
                pkg["readiness"] = graph.get("readiness")
                pkg["markdown"] = markdown
                pkg["user_text"] = f"# Блок {block_id}\n\n{markdown}"
                _write(pkg_path, pkg, args.apply)

                artifact = pkg.get("graph_artifact")
                if artifact:
                    graph_path = latest / "block_vector_graphs" / artifact
                    if graph_path.exists():
                        _write(graph_path, graph, args.apply)
                print(f"  витрина {gallery['discipline']:<4} {block_id}: "
                      f"{before} -> {PROFILE_LEGEND}; строк "
                      f"{(graph.get('validation') or {}).get('legend_entries_total', 0)}, "
                      f"рёбер {(graph.get('validation') or {}).get('edges_total', 0)}")
                touched_gallery += 1
        if changed:
            gallery["profiles_total"] = len({b.get("profile_id") for b in gallery["blocks"]})
            _write(gallery_path, gallery, args.apply)

    # manifest каталога: пересчёт профилей и версия
    manifest_path = CATALOG / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for discipline, filename in CATALOG_FILES.items():
        path = CATALOG / "disciplines" / filename
        if not path.exists() or discipline not in (manifest.get("disciplines") or {}):
            continue
        records = json.loads(path.read_text(encoding="utf-8")).get("records") or []
        manifest["disciplines"][discipline]["profiles"] = len(
            {r.get("profile_id") for r in records if r.get("profile_id")})
    # profiles_total в каталоге — число УНИКАЛЬНЫХ профилей по всем дисциплинам
    # (см. build_catalog.py: len(all_profiles)), а не сумма по дисциплинам.
    all_profiles = set()
    for filename in CATALOG_FILES.values():
        path = CATALOG / "disciplines" / filename
        if not path.exists():
            continue
        for record in json.loads(path.read_text(encoding="utf-8")).get("records") or []:
            if record.get("profile_id"):
                all_profiles.add(record["profile_id"])
    manifest["profiles_total"] = len(all_profiles)
    manifest["catalog_version"] = f"{date.today():%Y.%m.%d}-legend"
    _write(manifest_path, manifest, args.apply)

    print(f"\nИтого: корпус {touched_corpus}, каталог {touched_catalog}, "
          f"витрина {touched_gallery}; версия каталога -> {manifest['catalog_version']}")
    if not args.apply:
        print("Сухой прогон завершён. Для записи запустите с --apply")
    return 0


if __name__ == "__main__":
    sys.exit(main())
