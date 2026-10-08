#!/usr/bin/env python3
"""Восстановление исследовательского корпуса `experiments/блоки разных дисциплин`.

Корпус — фикстура девяти профильных тестов геометрии (АР, КЖ, КМ, ТХ, ГП, ОВ,
ЭОМ, ВК, СС). Он никогда не был в git: PDF-вырезки блоков и построенные по ним
графы лежали только на диске. Восстанавливается он полностью и детерминированно,
потому что уцелели два независимых перечня:

  * `backend/app/pipeline/stages/block_context/reference_catalog/disciplines/*.json`
    — эталон состава: block_id, profile_id, subtype, source_page (версия каталога
    2026.07.13-1, собран из этого же корпуса);
  * `docs/graphic_anchors/пробы/corpus_results.jsonl` — имена файлов вырезок.

Сами блоки живы в `projects_v2`: у каждого документа есть `02_work/result.json`
(страницы, блоки, `coords_norm`/`polygon_points_norm`) и `02_work/document.pdf`.
Вырезка и граф строятся ровно теми же функциями `build_<дисциплина>_graph_from_source`,
что и в бою, поэтому результат совпадает с исходным, а не приближает его.

Обращений к моделям нет: вся геометрия детерминированная.

Команды:
    python scripts/restore_diverse_corpus.py index            # индекс block_id → документ
    python scripts/restore_diverse_corpus.py build ТХ         # одна дисциплина
    python scripts/restore_diverse_corpus.py build --all      # весь корпус
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

CORPUS_ROOT = ROOT / "experiments" / "блоки разных дисциплин"
CATALOG_DIR = ROOT / "backend/app/pipeline/stages/block_context/reference_catalog/disciplines"
PROBE_RESULTS = ROOT / "docs/graphic_anchors/пробы/corpus_results.jsonl"
OBJECTS_ROOT = ROOT / "projects_v2/objects"
INDEX_CACHE = ROOT / "experiments" / "блоки разных дисциплин" / ".block_index.json"

BLOCK_ID_RE = re.compile(r"([0-9A-Z]{4}-[0-9A-Z]{4}-[0-9A-Z]{3})\.pdf$")

# дисциплина → (файл каталога, папка графов, имя манифеста, модуль, функция)
DISCIPLINES: dict[str, tuple[str, str, str, str, str]] = {
    "АР": ("AR", "ar_out", "AR_DIVERSE_CORPUS.json", "architecture_geometry", "build_ar_graph_from_source"),
    "КЖ": ("KJ", "kj_out", "KJ_DIVERSE_CORPUS.json", "structural_geometry", "build_kj_graph_from_source"),
    "КМ": ("KM", "km_out", "KM_DIVERSE_CORPUS.json", "structural_geometry", "build_kj_graph_from_source"),
    "ТХ": ("TX", "tx_out", "TX_DIVERSE_CORPUS.json", "technology_geometry", "build_tx_graph_from_source"),
    "ГП": ("GP", "gp_out", "GP_DIVERSE_CORPUS.json", "general_plan_geometry", "build_gp_graph_from_source"),
    "ОВ": ("HVAC", "hvac_out", "HVAC_DIVERSE_CORPUS.json", "hvac_geometry", "build_hvac_graph_from_source"),
    "ЭОМ": ("EOM", "eom_out", "EOM_DIVERSE_CORPUS.json", "electrical_geometry", "build_electrical_graph_from_source"),
    "ВК": ("VK", "vk_out", "VK_DIVERSE_CORPUS.json", "water_geometry", "build_water_graph_from_source"),
}


from backend.app.pipeline.stages.block_grounding.legend_geometry import (
    PROFILE_LEGEND,
    build_legend_graph_from_source,
)


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def build_index(force: bool = False) -> dict[str, list[str]]:
    """block_id → ВСЕ каталоги версий, где встречается блок (пути от ROOT).

    Один и тот же block_id встречается в нескольких документах — в том числе в
    служебных вроде «ВЕКТОГРАФ — ТХ», где `document.pdf` подменён заглушкой на
    полторы килобайты. Поэтому индекс мультизначный, а годный документ выбирает
    `_pick_version` уже при сборке.
    """
    if INDEX_CACHE.exists() and not force:
        return _load_json(INDEX_CACHE)

    index: dict[str, list[str]] = {}
    results = sorted(OBJECTS_ROOT.rglob("02_work/result.json"))
    print(f"result.json найдено: {len(results)}")
    for path in results:
        try:
            data = _load_json(path)
        except Exception:
            continue
        version_rel = str(path.parent.parent.relative_to(ROOT))
        for page in data.get("pages", []) or []:
            for block in page.get("blocks", []) or []:
                block_id = block.get("id")
                if block_id:
                    index.setdefault(block_id, [])
                    if version_rel not in index[block_id]:
                        index[block_id].append(version_rel)
    INDEX_CACHE.parent.mkdir(parents=True, exist_ok=True)
    INDEX_CACHE.write_text(json.dumps(index, ensure_ascii=False), encoding="utf-8")
    print(f"блоков в индексе: {len(index)}")
    return index


def _pick_version(block_id: str, candidates: list[str], expected_page: int | None):
    """Выбрать документ, в котором блок действительно вырезается.

    Годным считается тот, где PDF открывается, содержит нужную страницу и блок
    не занимает страницу целиком (`coords_norm` = [0,0,1,1] — признак служебной
    заглушки, а не вырезки). При равенстве предпочитается совпадение страницы с
    `source_page` каталога, затем более объёмный PDF.
    """
    import fitz

    scored = []
    for version_rel in candidates:
        version_dir = ROOT / version_rel
        source_pdf = version_dir / "02_work/document.pdf"
        source_result = version_dir / "02_work/result.json"
        if not source_pdf.exists() or not source_result.exists():
            continue
        try:
            located = _locate(_load_json(source_result), block_id)
        except Exception:
            continue
        if not located:
            continue
        page, block = located
        bbox = block.get("coords_norm")
        if not bbox:
            continue
        page_index = int(page["page_number"]) - 1
        try:
            with fitz.open(str(source_pdf)) as document:
                if page_index >= document.page_count:
                    continue
        except Exception:
            continue
        full_page = [round(float(v), 3) for v in bbox] == [0.0, 0.0, 1.0, 1.0]
        page_match = expected_page is not None and page_index + 1 == int(expected_page)
        scored.append((
            (0 if full_page else 1, 1 if page_match else 0, source_pdf.stat().st_size),
            version_dir, source_pdf, source_result, page, block,
        ))
    if not scored:
        return None
    scored.sort(key=lambda item: item[0], reverse=True)
    return scored[0][1:]


def corpus_filenames() -> dict[str, str]:
    """block_id → имя PDF-вырезки, как он назывался в корпусе."""
    names: dict[str, str] = {}
    for line in PROBE_RESULTS.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        name = record.get("pdf") or ""
        block_id = record.get("block_id")
        if not block_id:
            match = BLOCK_ID_RE.search(name)
            block_id = match.group(1) if match else None
        if block_id and name:
            names[block_id] = name
    return names


def _polygon_norm(page: dict, block: dict) -> list | None:
    points = block.get("polygon_points_norm")
    if points:
        return points
    raw = block.get("polygon_points")
    if raw and page.get("width") and page.get("height"):
        return [[x / page["width"], y / page["height"]] for x, y in raw]
    return None


def _locate(result: dict, block_id: str) -> tuple[dict, dict] | None:
    for page in result.get("pages", []) or []:
        for block in page.get("blocks", []) or []:
            if block.get("id") == block_id:
                return page, block
    return None


def _write_crop(source_pdf: Path, page_index: int, bbox_norm, polygon_norm, target: Path) -> dict:
    """Вырезка блока в отдельный одностраничный PDF — та же геометрия, что в профилях."""
    import fitz

    from backend.app.pipeline.stages.block_grounding.hvac_geometry import _clip_copied_page

    source = fitz.open(str(source_pdf))
    cropped = None
    try:
        source_page = source[page_index]
        width, height = source_page.rect.width, source_page.rect.height
        crop = fitz.Rect(
            bbox_norm[0] * width, bbox_norm[1] * height,
            bbox_norm[2] * width, bbox_norm[3] * height,
        ) & source_page.rect
        unrotated = crop * source_page.derotation_matrix
        unrotated.normalize()
        offset = source_page.cropbox_position
        unrotated = fitz.Rect(
            unrotated.x0 + offset.x, unrotated.y0 + offset.y,
            unrotated.x1 + offset.x, unrotated.y1 + offset.y,
        )
        cropped = fitz.open()
        cropped.insert_pdf(source, from_page=page_index, to_page=page_index)
        target_page = cropped[0]
        if polygon_norm:
            inverse = ~source_page.transformation_matrix
            points = [
                tuple(fitz.Point(float(x) * width, float(y) * height) * source_page.derotation_matrix * inverse)
                for x, y in polygon_norm
            ]
            _clip_copied_page(target_page, points)
        target_page.set_cropbox(unrotated)
        metrics = {
            "drawing_paths": len(target_page.get_drawings()),
            "embedded_images": len(target_page.get_images()),
            "text_characters": len(target_page.get_text() or ""),
        }
        target.parent.mkdir(parents=True, exist_ok=True)
        cropped.save(str(target), garbage=3, deflate=True)
        return metrics
    finally:
        if cropped is not None:
            cropped.close()
        source.close()
        fitz.TOOLS.store_shrink(100)


def build_discipline(discipline: str, index: dict[str, str], names: dict[str, str], limit: int | None = None) -> dict:
    code, out_dir_name, manifest_name, module_name, func_name = DISCIPLINES[discipline]
    module = __import__(
        f"backend.app.pipeline.stages.block_grounding.{module_name}", fromlist=[func_name]
    )
    build_graph = getattr(module, func_name)

    records = _load_json(CATALOG_DIR / f"{code}.json")["records"]
    if limit:
        records = records[:limit]
    disc_dir = CORPUS_ROOT / discipline
    out_dir = disc_dir / out_dir_name
    out_dir.mkdir(parents=True, exist_ok=True)

    manifest: list[dict] = []
    problems: list[dict] = []
    for number, record in enumerate(records, 1):
        block_id = record["block_id"]
        candidates = index.get(block_id) or []
        if isinstance(candidates, str):
            candidates = [candidates]
        if not candidates:
            problems.append({"block_id": block_id, "reason": "блок не найден в projects_v2"})
            continue
        expected_page = record.get("source_page")
        picked = _pick_version(block_id, candidates, expected_page)
        if not picked:
            problems.append({
                "block_id": block_id,
                "reason": f"нет годного документа среди {len(candidates)} кандидатов",
            })
            continue
        version_dir, source_pdf, source_result, page, block = picked
        page_index = int(page["page_number"]) - 1
        bbox_norm = block.get("coords_norm")
        polygon_norm = _polygon_norm(page, block)

        output_name = names.get(block_id) or f"{discipline} — {block_id}.pdf"
        try:
            metrics = _write_crop(source_pdf, page_index, bbox_norm, polygon_norm, disc_dir / output_name)
        except Exception as error:  # noqa: BLE001
            problems.append({"block_id": block_id, "reason": f"вырезка не удалась: {error}"})
            continue

        # «Условные обозначения» — надведомственный профиль: у него собственный
        # билдер и собственный гейт, дисциплинарный билдер легенду не наполняет.
        if record.get("profile_id") == PROFILE_LEGEND:
            graph = build_legend_graph_from_source(
                source_pdf,
                page_index=page_index,
                bbox_norm=bbox_norm,
                polygon_norm=polygon_norm,
                block_id=block_id,
                subtype_hint=record.get("subtype"),
            )
        else:
            graph = build_graph(
                source_pdf,
                page_index=page_index,
                bbox_norm=bbox_norm,
                polygon_norm=polygon_norm,
                block_id=block_id,
                profile_hint=record.get("profile_id"),
                subtype_hint=record.get("subtype"),
            )
        if not graph:
            problems.append({"block_id": block_id, "reason": "граф не построен"})
            continue
        (out_dir / f"{block_id}.structure.json").write_text(
            json.dumps(graph, ensure_ascii=False, indent=1), encoding="utf-8"
        )
        manifest.append({
            "block_id": block_id,
            "output": output_name,
            "profile_id": record.get("profile_id"),
            "subtype": record.get("subtype"),
            "source_pdf": str(source_pdf.relative_to(ROOT)),
            "source_result": str(source_result.relative_to(ROOT)),
            "page": page_index + 1,
            **metrics,
        })
        if number % 25 == 0:
            print(f"  {discipline}: {number}/{len(records)}")

    (disc_dir / manifest_name).write_text(
        json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    summary = {
        "discipline": discipline,
        "ожидалось": len(records),
        "восстановлено": len(manifest),
        "проблем": len(problems),
    }
    if problems:
        (disc_dir / f"{code}_RESTORE_PROBLEMS.json").write_text(
            json.dumps(problems, ensure_ascii=False, indent=1), encoding="utf-8"
        )
    print(json.dumps(summary, ensure_ascii=False))
    return summary


SS_DIR = CORPUS_ROOT / "СС"
SS_EXTRACTOR = SS_DIR / "extract_alia_scheme_corpus.py"
# Блоки, названные в самих тестах: план, АПС и принципиальные схемы. Их состав
# фиксирует не каталог, а проверки, поэтому они входят в корпус обязательно.
SS_REQUIRED_REMAINING = (
    "4EVJ-MYPD-7P7", "9KLR-W3LY-EAA", "TQVW-YHVA-NDM",
    "6FTW-WUQ7-MUY", "9WRW-TQYG-JG6", "6LCH-PCEN-6HU",
)
SS_REMAINING_TOTAL = 19


def _ss_scheme_corpus() -> list[tuple[str, str]]:
    """Перечень блоков структурных схем ALIA — из уцелевшего сборщика СС."""
    import ast

    tree = ast.parse(SS_EXTRACTOR.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "CORPUS" for target in node.targets
        ):
            return [(str(block_id), str(name)) for block_id, name in ast.literal_eval(node.value)]
    raise RuntimeError("в extract_alia_scheme_corpus.py не найден CORPUS")


def _ss_all_filenames() -> dict[str, str]:
    """block_id → имя вырезки для всех блоков СС из перечня зондов."""
    names: dict[str, str] = {}
    for line in PROBE_RESULTS.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        if record.get("discipline") != "СС":
            continue
        name = record.get("pdf") or ""
        block_id = record.get("block_id")
        if not block_id:
            match = BLOCK_ID_RE.search(name)
            block_id = match.group(1) if match else None
        if block_id and name:
            names[block_id] = name
    return names


def build_ss(index: dict[str, list[str]]) -> dict:
    """Восстановить оба корпуса СС: структурные схемы ALIA и «остальное»."""
    from backend.app.pipeline.stages.block_grounding.alia_remaining_geometry import (
        build_remaining_graph, evaluate_remaining_gate,
    )

    catalog = {record["block_id"]: record for record in _load_json(CATALOG_DIR / "SS.json")["records"]}

    # Тесты low_voltage и structural_access открывают вырезки по жёстким именам
    # («02_13АВ-РД-АПЗ.АПС-К3_V1__6W3K-9C4Y-VPY.pdf»), а не через манифест,
    # поэтому по СС восстанавливаются все вырезки перечня зондов, а не только
    # каталожные.
    extracted = 0
    for block_id, name in _ss_all_filenames().items():
        if (SS_DIR / name).exists():
            continue
        if isinstance(_prepare_block(block_id, index, catalog.get(block_id, {}), SS_DIR, name), dict):
            extracted += 1

    scheme: list[dict] = []
    problems: list[dict] = []
    probe_names = _ss_all_filenames()
    for block_id, name in _ss_scheme_corpus():
        output_name = probe_names.get(block_id) or f"{name}.pdf"
        entry = _prepare_block(block_id, index, catalog.get(block_id, {}), SS_DIR, output_name)
        if isinstance(entry, dict):
            scheme.append(entry)
        else:
            problems.append({"block_id": block_id, "corpus": "scheme", "reason": entry})
    (SS_DIR / "ALIA_SCHEME_CORPUS.json").write_text(
        json.dumps(scheme, ensure_ascii=False, indent=1), encoding="utf-8"
    )

    # «Остальное»: из 110 записей каталога берём те, чей граф действительно
    # проходит строгую полноту, — сначала названные в тестах, потом по одному
    # представителю каждого профиля, потом добор до 19.
    from backend.app.pipeline.stages.block_grounding.alia_remaining_geometry import ALL_REMAINING_PROFILES

    candidates = [record for record in catalog.values() if record["profile_id"] in ALL_REMAINING_PROFILES]
    order = sorted(
        candidates,
        key=lambda record: (record["block_id"] not in SS_REQUIRED_REMAINING, record["profile_id"]),
    )
    remaining: list[dict] = []
    seen_profiles: set[str] = set()
    passed: list[tuple[dict, str]] = []
    for record in order:
        output_name = probe_names.get(record["block_id"]) \
            or f"СС — {record['profile_id']} — {record['block_id']}.pdf"
        entry = _prepare_block(record["block_id"], index, record, SS_DIR, output_name)
        if not isinstance(entry, dict):
            continue
        graph = build_remaining_graph(SS_DIR / entry["output"], block_id=record["block_id"])
        if not graph or not evaluate_remaining_gate(graph).get("complete"):
            continue
        passed.append((entry, record["profile_id"]))

    for entry, profile_id in passed:
        if entry["block_id"] in SS_REQUIRED_REMAINING or profile_id not in seen_profiles:
            remaining.append(entry)
            seen_profiles.add(profile_id)
    for entry, _ in passed:
        if len(remaining) >= SS_REMAINING_TOTAL:
            break
        if entry not in remaining:
            remaining.append(entry)
    (SS_DIR / "ALIA_REMAINING_CORPUS.json").write_text(
        json.dumps(remaining, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    if problems:
        (SS_DIR / "SS_RESTORE_PROBLEMS.json").write_text(
            json.dumps(problems, ensure_ascii=False, indent=1), encoding="utf-8"
        )
    summary = {
        "discipline": "СС",
        "вырезок восстановлено": extracted,
        "структурные схемы": len(scheme),
        "прошли полноту": len(passed),
        "остальное": len(remaining),
        "профилей покрыто": len(seen_profiles),
        "проблем": len(problems),
    }
    print(json.dumps(summary, ensure_ascii=False))
    return summary


def _prepare_block(block_id: str, index, record: dict, target_dir: Path, output_name: str):
    """Вырезать блок в PDF; вернуть запись манифеста или текст причины отказа."""
    candidates = index.get(block_id) or []
    if isinstance(candidates, str):
        candidates = [candidates]
    if not candidates:
        return "блок не найден в projects_v2"
    picked = _pick_version(block_id, candidates, record.get("source_page"))
    if not picked:
        return f"нет годного документа среди {len(candidates)} кандидатов"
    _version_dir, source_pdf, source_result, page, block = picked
    page_index = int(page["page_number"]) - 1
    try:
        metrics = _write_crop(
            source_pdf, page_index, block.get("coords_norm"),
            _polygon_norm(page, block), target_dir / output_name,
        )
    except Exception as error:  # noqa: BLE001
        return f"вырезка не удалась: {error}"
    return {
        "block_id": block_id,
        "output": output_name,
        "profile_id": record.get("profile_id"),
        "subtype": record.get("subtype"),
        "source_pdf": str(source_pdf.relative_to(ROOT)),
        "source_result": str(source_result.relative_to(ROOT)),
        "page": page_index + 1,
        **metrics,
    }


def _normalize(text: str) -> str:
    return re.sub(r"\s+", "", str(text or "")).lower()


def _category(token: str) -> str:
    """Грубая типизация подписи — только для группировки отчёта."""
    if re.fullmatch(r"[+\-]?\d{1,2}[.,]\d{3}", token):
        return "отметки"
    if re.fullmatch(r"[\d\s.,x×хØø⌀]+", token):
        return "размеры и числа"
    if re.search(r"[A-Za-zА-Яа-я]", token):
        return "обозначения и подписи"
    return "прочее"


def build_coverage(discipline: str) -> dict:
    """Семантическое покрытие: подписи вырезки против реестра строк графа.

    `semantic_ledger` графа — плоский реестр ВСЕХ текстовых строк блока с
    координатами. Покрытие считается сверкой: подпись вырезки, которой нет ни в
    одной строке реестра, попадает в `missed`. Числа берутся из данных, а не
    проставляются: при неполном реестре отчёт покажет ненулевые пропуски.
    """
    import fitz

    code, out_dir_name, manifest_name, *_ = DISCIPLINES[discipline]
    disc_dir = CORPUS_ROOT / discipline
    out_dir = disc_dir / out_dir_name
    manifest = _load_json(disc_dir / manifest_name)

    records = []
    for case in manifest:
        block_id = case["block_id"]
        graph = _load_json(out_dir / f"{block_id}.structure.json")
        ledger = " ".join(_normalize(item.get("text")) for item in graph.get("semantic_ledger") or [])
        with fitz.open(disc_dir / case["output"]) as document:
            words = [word[4] for word in document[0].get_text("words")]
        categories: dict[str, dict[str, list]] = {}
        misses = 0
        for word in words:
            token = str(word).strip()
            if not token:
                continue
            bucket = categories.setdefault(_category(token), {"covered": [], "missed": [], "source": []})
            if _normalize(token) and _normalize(token) in ledger:
                bucket["covered"].append(token)
            else:
                bucket["missed"].append(token)
                misses += 1
        records.append({
            "block_id": block_id,
            "profile_id": case.get("profile_id"),
            "subtype": case.get("subtype"),
            "source_layer_state": "text_available" if words else "no_pdf_text_layer",
            "pdf_words_total": len(words),
            "pdf_misses_total": misses,
            "categories": categories,
        })

    report = {
        "discipline": discipline,
        "blocks_total": len(records),
        "method": "подписи вырезки PDF против semantic_ledger графа (совпадение по нормализованной строке)",
        "records": records,
    }
    (disc_dir / f"{code}_SEMANTIC_COVERAGE.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    total = sum(record["pdf_misses_total"] for record in records)
    print(json.dumps({"discipline": discipline, "блоков": len(records), "пропусков": total}, ensure_ascii=False))
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    index_parser = sub.add_parser("index", help="построить индекс block_id → документ")
    index_parser.add_argument("--force", action="store_true")
    build_parser = sub.add_parser("build", help="восстановить дисциплину")
    build_parser.add_argument("discipline", nargs="?")
    build_parser.add_argument("--all", action="store_true")
    build_parser.add_argument("--limit", type=int)
    coverage_parser = sub.add_parser("coverage", help="отчёт семантического покрытия")
    coverage_parser.add_argument("discipline", nargs="?")
    coverage_parser.add_argument("--all", action="store_true")
    sub.add_parser("ss", help="восстановить оба корпуса СС (ALIA)")
    args = parser.parse_args()

    if args.command == "index":
        build_index(force=args.force)
        return 0

    if args.command == "ss":
        build_ss(build_index())
        return 0

    if args.command == "coverage":
        targets = list(DISCIPLINES) if args.all else [args.discipline]
        if not targets or targets == [None]:
            parser.error("укажите дисциплину или --all")
        for item in targets:
            build_coverage(item)
        return 0

    index = build_index()
    names = corpus_filenames()
    targets = list(DISCIPLINES) if args.all else [args.discipline]
    if not targets or targets == [None]:
        parser.error("укажите дисциплину или --all")
    summaries = [build_discipline(item, index, names, limit=args.limit) for item in targets]
    print(json.dumps(summaries, ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
