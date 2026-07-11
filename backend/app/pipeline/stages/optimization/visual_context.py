"""Visual attachment selection for optimization-stage Codex runs.

The block analysis stage already crops drawing fragments into PNG files. This
module picks a small, high-signal subset of those graphics and builds the prompt
section that tells Codex how to use them for optimization proposals.
"""
from __future__ import annotations

import json
import os
import re
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

try:
    import fitz  # type: ignore
except ImportError:  # pragma: no cover - optional outside the audit runtime
    fitz = None


_DEFAULT_MAX_IMAGES = 12
_IMAGE_DIR_ENV = "AUDIT_CODEX_OPT_IMAGE_DIR"
_MAX_IMAGES_ENV = "AUDIT_CODEX_OPT_IMAGE_MAX"

_GRAPHIC_KEYWORDS = (
    "план",
    "схем",
    "аксоном",
    "однолин",
    "принципиаль",
    "разрез",
    "узел",
    "трасс",
    "маршрут",
    "развод",
    "стояк",
    "коллектор",
    "магистра",
    "щит",
    "кабель",
    "лоток",
    "воздуховод",
    "трубопровод",
    "фасад",
    "расклад",
    "армирован",
    "сечение",
    "floor plan",
    "schematic",
    "axonometric",
    "single line",
    "route",
    "layout",
    "riser",
    "section",
)

_OPTIMIZATION_KEYWORDS = (
    "коллектор",
    "узел",
    "стояк",
    "трасс",
    "маршрут",
    "магистра",
    "развод",
    "длина",
    "дублир",
    "пересеч",
    "поворот",
    "обход",
    "щит",
    "лоток",
    "короб",
    "воздуховод",
    "трубопровод",
)

_DISCIPLINE_KEYWORDS = {
    "AR": (
        "фасад", "витраж", "двер", "окн", "пол", "стяж", "перегород",
        "лестниц", "огражден", "кровл", "отделк", "люк", "расклад",
    ),
    "KJ": (
        "армирован", "арматур", "сетка", "колон", "плита", "балк",
        "заклад", "отверст", "стык", "нахлест", "нахлёст", "бетон",
        "сечение", "опалуб",
    ),
    "OV": (
        "воздуховод", "вентил", "отоплен", "теплоснаб", "коллектор",
        "стояк", "трубопровод", "магистра", "обвяз", "компенсатор",
        "клапан", "решет", "решётк", "трасс",
    ),
    "VK": (
        "водоснаб", "канализ", "стояк", "трубопровод", "выпуск",
        "ввод", "гильз", "узел", "свар", "изоляц", "антикор",
    ),
    "EOM": (
        "однолин", "щит", "кабель", "лоток", "нагруз", "автомат",
        "трансформатор", "заземлен", "схем", "трасс",
    ),
    "TX": (
        "расстанов", "оборудован", "контейнер", "поток", "маршрут",
        "зон", "план", "экспликац", "подъем", "подъём",
    ),
}


@dataclass(frozen=True)
class VisualAttachment:
    block_id: str
    page: int | None
    sheet: str
    label: str
    sheet_type: str
    image_path: Path
    source_dir: str
    score: int
    reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["image_path"] = str(self.image_path)
        return data


@dataclass(frozen=True)
class OptimizationVisualContext:
    image_paths: list[Path]
    attachments: list[VisualAttachment]
    prompt_section: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "image_paths": [str(path) for path in self.image_paths],
            "attachments": [attachment.to_dict() for attachment in self.attachments],
            "prompt_section": self.prompt_section,
        }


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return " ".join(_as_text(item) for item in value)
    if isinstance(value, dict):
        return " ".join(f"{key}: {_as_text(item)}" for key, item in value.items())
    return str(value)


def _normalize_text(value: Any) -> str:
    text = _as_text(value).lower().replace("ё", "е")
    return re.sub(r"\s+", " ", text).strip()


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _configured_max_images(max_images: int | None) -> int:
    if max_images is not None:
        return max(0, int(max_images))
    raw = os.environ.get(_MAX_IMAGES_ENV, "").strip()
    if raw:
        try:
            return max(0, int(raw))
        except ValueError:
            pass
    return _DEFAULT_MAX_IMAGES


def _preferred_dirs() -> list[str]:
    configured = os.environ.get(_IMAGE_DIR_ENV, "blocks_gemma_100").strip()
    order = [
        configured,
        "blocks_gemma_100",
        "blocks",
        "blocks_gemma_300",
    ]
    result: list[str] = []
    for item in order:
        if item and item not in result:
            result.append(item)
    return result


def _image_mapping_for_dir(output_dir: Path) -> dict[str, tuple[str, Path]]:
    mapping: dict[str, tuple[str, Path]] = {}
    for dirname in _preferred_dirs():
        directory = output_dir / dirname
        if not directory.is_dir():
            continue
        index = _read_json(directory / "index.json")
        blocks = index.get("blocks") if isinstance(index, dict) else None
        if isinstance(blocks, list):
            for block in blocks:
                if not isinstance(block, dict):
                    continue
                block_id = str(block.get("block_id") or "").strip()
                file_name = str(block.get("file") or "").strip()
                if not block_id or not file_name or block_id in mapping:
                    continue
                image_path = directory / file_name
                if image_path.is_file():
                    mapping[block_id] = (dirname, image_path)

    for dirname in _preferred_dirs():
        directory = output_dir / dirname
        if not directory.is_dir():
            continue
        for image_path in sorted(directory.glob("block_*.png")):
            block_id = image_path.stem.removeprefix("block_")
            mapping.setdefault(block_id, (dirname, image_path))
    return mapping


def _load_image_indexes(
    output_dir: Path,
    *,
    required_ids: set[str] | None = None,
) -> dict[str, tuple[str, Path]]:
    """Load images from latest, falling back to its best matching archived run."""
    primary = _image_mapping_for_dir(output_dir)
    if primary or output_dir.name != "latest":
        return primary

    runs_dir = output_dir.parent / "runs"
    if not runs_dir.is_dir():
        return primary
    best_mapping: dict[str, tuple[str, Path]] = {}
    best_score = -1
    for run_dir in runs_dir.iterdir():
        if not run_dir.is_dir():
            continue
        mapping = _image_mapping_for_dir(run_dir)
        if not mapping:
            continue
        overlap = len(set(mapping) & required_ids) if required_ids else len(mapping)
        if overlap > best_score:
            best_mapping = mapping
            best_score = overlap
    return best_mapping


def _block_records(output_dir: Path) -> list[dict[str, Any]]:
    analysis = _read_json(output_dir / "02_blocks_analysis.json")
    if isinstance(analysis, dict):
        for key in ("block_analyses", "blocks"):
            records = analysis.get(key)
            if isinstance(records, list):
                return [record for record in records if isinstance(record, dict)]
    return []


def _score_block(
    record: dict[str, Any],
    *,
    discipline: str = "",
) -> tuple[int, tuple[str, ...]]:
    text = _normalize_text(
        " ".join(
            _as_text(record.get(key))
            for key in (
                "sheet_type",
                "sheet",
                "label",
                "summary",
                "key_values_read",
                "evidence_text_refs",
                "final_profile",
            )
        )
    )
    score = 0
    reasons: list[str] = []

    if any(keyword in text for keyword in _GRAPHIC_KEYWORDS):
        score += 35
        reasons.append("drawing_or_scheme")
    if any(keyword in text for keyword in _OPTIMIZATION_KEYWORDS):
        score += 25
        reasons.append("optimization_geometry")
    discipline_keywords = _DISCIPLINE_KEYWORDS.get(discipline.upper(), ())
    if discipline_keywords and any(keyword in text for keyword in discipline_keywords):
        score += 30
        reasons.append(f"discipline_{discipline.lower()}")
    if record.get("findings"):
        score += 20
        reasons.append("has_block_findings")
    if record.get("key_values_read"):
        score += 10
        reasons.append("has_readable_values")
    if record.get("unreadable_text"):
        score -= 15
        reasons.append("partly_unreadable")
    if record.get("not_enriched"):
        score -= 10
        reasons.append("not_enriched")

    page = _safe_int(record.get("page"))
    if page is not None:
        score += max(0, 10 - min(page, 10))

    return score, tuple(reasons)


def collect_optimization_visual_context(
    output_dir: str | Path,
    *,
    max_images: int | None = None,
    discipline: str = "",
) -> OptimizationVisualContext:
    output_path = Path(output_dir)
    limit = _configured_max_images(max_images)
    if limit <= 0:
        return OptimizationVisualContext(image_paths=[], attachments=[], prompt_section="")

    records = _block_records(output_path)
    required_ids = {
        str(record.get("block_id") or "").strip()
        for record in records
        if str(record.get("block_id") or "").strip()
    }
    image_index = _load_image_indexes(output_path, required_ids=required_ids)
    if not image_index:
        return OptimizationVisualContext(image_paths=[], attachments=[], prompt_section="")

    candidates: list[VisualAttachment] = []
    seen_ids: set[str] = set()
    for record in records:
        block_id = str(record.get("block_id") or "").strip()
        if not block_id or block_id in seen_ids or block_id not in image_index:
            continue
        score, reasons = _score_block(record, discipline=discipline)
        if score <= 0:
            continue
        source_dir, image_path = image_index[block_id]
        candidates.append(
            VisualAttachment(
                block_id=block_id,
                page=_safe_int(record.get("page")),
                sheet=_as_text(record.get("sheet")).strip(),
                label=_as_text(record.get("label")).strip(),
                sheet_type=_as_text(record.get("sheet_type")).strip(),
                image_path=image_path,
                source_dir=source_dir,
                score=score,
                reasons=reasons,
            )
        )
        seen_ids.add(block_id)

    if not candidates:
        for block_id, (source_dir, image_path) in sorted(image_index.items())[:limit]:
            candidates.append(
                VisualAttachment(
                    block_id=block_id,
                    page=None,
                    sheet="",
                    label="",
                    sheet_type="",
                    image_path=image_path,
                    source_dir=source_dir,
                    score=1,
                    reasons=("fallback_image_index",),
                )
            )

    ranked = sorted(
        candidates,
        key=lambda item: (item.score, -(item.page or 9999), item.block_id),
        reverse=True,
    )
    selected = _select_page_diverse(ranked, limit=limit)
    image_paths = [attachment.image_path for attachment in selected]
    prompt_section = _build_prompt_section(selected, discipline=discipline)
    return OptimizationVisualContext(
        image_paths=image_paths,
        attachments=selected,
        prompt_section=prompt_section,
    )


def _select_page_diverse(
    ranked: list[VisualAttachment],
    *,
    limit: int,
) -> list[VisualAttachment]:
    """Take the strongest block from each page before filling remaining slots."""
    selected: list[VisualAttachment] = []
    selected_ids: set[str] = set()
    seen_pages: set[int] = set()
    for attachment in ranked:
        if attachment.page is None or attachment.page in seen_pages:
            continue
        selected.append(attachment)
        selected_ids.add(attachment.block_id)
        seen_pages.add(attachment.page)
        if len(selected) >= limit:
            return selected
    for attachment in ranked:
        if attachment.block_id in selected_ids:
            continue
        selected.append(attachment)
        if len(selected) >= limit:
            break
    return selected


def add_page_overviews(
    context: OptimizationVisualContext,
    *,
    pdf_path: str | Path,
    render_dir: str | Path,
    max_overviews: int = 3,
    long_side_px: int = 1800,
    discipline: str = "",
) -> OptimizationVisualContext:
    """Render full-page context for the pages represented by selected crops."""
    if fitz is None or max_overviews <= 0 or not context.attachments:
        return context
    source = Path(pdf_path)
    if not source.is_file():
        return context

    page_weights: Counter[int] = Counter()
    for attachment in context.attachments:
        if attachment.page is not None:
            page_weights[attachment.page] += max(1, attachment.score)
    pages = [page for page, _ in page_weights.most_common(max_overviews)]
    if not pages:
        return context

    target_dir = Path(render_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    overviews: list[VisualAttachment] = []
    try:
        document = fitz.open(str(source))
        for page_number in pages:
            page_index = page_number - 1
            if page_index < 0 or page_index >= len(document):
                continue
            page = document[page_index]
            long_side = max(float(page.rect.width), float(page.rect.height))
            zoom = max(0.25, long_side_px / long_side) if long_side else 1.0
            image_path = target_dir / f"overview_page_{page_number:03d}.png"
            page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), alpha=False).save(str(image_path))
            overviews.append(
                VisualAttachment(
                    block_id=f"overview_page_{page_number:03d}",
                    page=page_number,
                    sheet="",
                    label=f"Общий вид страницы {page_number}",
                    sheet_type="page_overview",
                    image_path=image_path,
                    source_dir="page_overviews",
                    score=page_weights[page_number],
                    reasons=("page_overview",),
                )
            )
        document.close()
    except Exception:
        return context

    attachments = overviews + context.attachments
    return OptimizationVisualContext(
        image_paths=[attachment.image_path for attachment in attachments],
        attachments=attachments,
        prompt_section=_build_prompt_section(attachments, discipline=discipline),
    )


def _build_prompt_section(
    attachments: list[VisualAttachment],
    *,
    discipline: str = "",
) -> str:
    if not attachments:
        return ""

    lines = [
        "## Графический контекст для поиска оптимизаций",
        "",
        f"К этому запуску Codex приложены {len(attachments)} PNG-фрагментов чертежей через `--image`.",
        "Проверь их не как источник замечаний по нормам, а как источник инженерных оптимизаций: сокращение трасс, укрупнение/упрощение узлов, дублирующие коллекторы/щиты/лотки, лишние обходы и повороты, чрезмерная фрагментация систем, более простая компоновка при сохранении функции.",
        "Если оптимизация подтверждается только графикой, включай её только при достаточной уверенности и обязательно привязывай к block_id/page/sheet в `spec_items`, `current` или `risks`. Не придумывай размеры и объёмы, которых не видно в MD/JSON/изображении.",
        "",
        "Приложенные изображения:",
    ]
    discipline_hints = {
        "AR": "Для АР отдельно проверь унификацию дверей/витражей и отделок, повторяющиеся узлы, толщины слоёв, фасадные позиции и несогласованную графическую маркировку.",
        "KJ": "Для КЖ отдельно проверь унификацию армирования и сечений, классы бетона, сетки, стыки, закладные и повторяющиеся отверстия/узлы. Любое снижение несущей способности допускай только после расчёта.",
        "OV": "Для ОВ отдельно проверь длину и сложность трасс, лишние отводы, дублирование арматуры, расположение коллекторов, типоразмеры воздуховодов/труб и доступность обслуживания.",
        "VK": "Для ВК отдельно проверь трассы и стояки, гильзы, выпуски, сварные узлы, изоляцию и унификацию арматуры без ухудшения ремонтопригодности.",
        "EOM": "Для ЭОМ отдельно проверь загрузку и унификацию щитов, повторяющиеся аппараты, кабельные трассы/лотки и избыточный сортамент при сохранении селективности и резервирования.",
        "TX": "Для ТХ отдельно проверь планировочные потоки, повторяющееся оборудование, занимаемую площадь, логистику и доступность обслуживания.",
    }
    hint = discipline_hints.get(discipline.upper())
    if hint:
        lines.insert(3, hint)
    for index, attachment in enumerate(attachments, start=1):
        page = f", page={attachment.page}" if attachment.page is not None else ""
        sheet = f", sheet={attachment.sheet}" if attachment.sheet else ""
        label = f", label={attachment.label}" if attachment.label else ""
        lines.append(
            f"{index}. IMG-{index:02d}: block_id={attachment.block_id}{page}{sheet}{label}, "
            f"source={attachment.source_dir}, path={attachment.image_path}"
        )
    return "\n".join(lines)


__all__ = [
    "OptimizationVisualContext",
    "VisualAttachment",
    "add_page_overviews",
    "collect_optimization_visual_context",
]
