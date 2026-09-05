#!/usr/bin/env python3
"""Compare Stage 02 OpenRouter GPT block findings with Codex exec.

The runner is read-only with respect to project artifacts. It selects existing
``01_blocks_analysis.json`` block records where GPT/OpenRouter produced
findings, reconstructs the same Stage 02 text context + PNG image, and asks
Codex exec to produce the same ``{"findings": [...]}`` schema in an isolated
comparison directory.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import shutil
import tempfile
import time
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from difflib import SequenceMatcher
from functools import lru_cache
from pathlib import Path
from typing import Any

from backend.app.pipeline.stages.block_analysis.gemma_findings_only import (
    RESPONSE_SCHEMA,
    SYSTEM_PROMPT_PROFILE_PRODUCTION,
    SYSTEM_PROMPT_PROFILES,
    build_effective_block_context,
    build_system_prompt,
    call_codex_for_block,
    get_enrichment,
    load_page_text,
)
from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import (
    STAGE02_BLOCKS_DIRNAME,
)
from backend.app.services.llm.codex_runner import find_codex_cli
from backend.app.core.config import resolve_codex_model
from backend.app.services.common.process_runner import run_command
from backend.app.services.storage.projects_v2_source_resolver import load_version_project_info


REPO_ROOT = Path(__file__).resolve().parents[2]
V2_OBJECTS_ROOT = REPO_ROOT / "projects_v2" / "objects"
OUT_ROOT = REPO_ROOT / "comparison" / "stage02_codex_block_ab"
DISCIPLINE_ORDER = ("AR", "AI", "KM", "KJ", "OV", "VK", "EOM", "SS", "TX", "GP", "PT")
TOKEN_USED_RE = re.compile(
    r"tokens used[ \t]*\r?\n[ \t]*([0-9][0-9 \t\u00a0,._]*)",
    re.I,
)
DOCUMENT_RETRIEVAL_PROFILE_NONE = "none"
DOCUMENT_RETRIEVAL_PROFILE_PRODUCTION = "production"
DOCUMENT_RETRIEVAL_PROFILE_TARGETED_V3 = "discipline_targeted_v3"
DOCUMENT_RETRIEVAL_PROFILES = frozenset(
    {
        DOCUMENT_RETRIEVAL_PROFILE_NONE,
        DOCUMENT_RETRIEVAL_PROFILE_PRODUCTION,
        DOCUMENT_RETRIEVAL_PROFILE_TARGETED_V3,
    }
)


@dataclass(frozen=True)
class BlockCandidate:
    object_slug: str
    discipline: str
    document: str
    version: str
    version_dir: Path
    latest_dir: Path
    block_id: str
    page: int
    image_path: Path
    image_source_dir: str
    block_record: dict[str, Any]
    gpt_findings: list[dict[str, Any]]
    enrichment_source: str

    @property
    def label(self) -> str:
        return (
            f"{self.object_slug}/{self.discipline}/{self.document}/"
            f"{self.version}/{self.block_id}"
        )


def utc_stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def safe_part(value: str, limit: int = 90) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_")
    return (safe or "item")[:limit]


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


@lru_cache(maxsize=256)
def _cached_block_package(
    latest_dir: str,
    block_id: str,
    page: int,
) -> dict[str, Any]:
    """Resolve the production block package once for a paired shadow run."""
    from backend.app.pipeline.stages.block_grounding.block_source_router import (
        resolve_block_package,
    )

    package = resolve_block_package(
        Path(latest_dir),
        block_id,
        page,
        prefer_prepared=False,
    )
    return package if isinstance(package, dict) else {}


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def extract_codex_tokens(text: str) -> int | None:
    matches = TOKEN_USED_RE.findall(text or "")
    if not matches:
        return None
    digits = re.sub(r"[^0-9]", "", matches[-1])
    return int(digits) if digits else None


def latest_parts(latest_dir: Path) -> tuple[str, str, str, str] | None:
    parts = latest_dir.parts
    try:
        marker = parts.index("projects_v2")
        return (
            parts[marker + 2],
            parts[marker + 4],
            parts[marker + 6],
            parts[marker + 8],
        )
    except (ValueError, IndexError):
        return None


def normalize_text(value: Any) -> str:
    if isinstance(value, list):
        value = " ".join(str(item) for item in value)
    elif isinstance(value, dict):
        value = " ".join(f"{k}: {v}" for k, v in value.items())
    text = str(value or "").lower().replace("ё", "е")
    text = text.replace("×", "x").replace("х", "x")
    text = re.sub(r"[^0-9a-zа-я]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def finding_text(item: dict[str, Any]) -> str:
    return normalize_text(
        " ".join(
            str(item.get(key) or "")
            for key in (
                "severity",
                "category",
                "finding",
                "norm",
                "norm_quote",
                "value_found",
                "recommendation",
                "block_evidence",
            )
        )
    )


_STOPWORDS = {
    "блок", "лист", "страница", "проект", "нужно", "требуется", "следует",
    "указать", "проверить", "предусмотреть", "отсутствует", "необходимо",
    "данных", "решение", "значение", "система", "системы", "для", "или",
    "при", "что", "это", "как", "есть", "без", "над", "под", "между",
}


def match_tokens(text: str) -> set[str]:
    tokens = set()
    for token in text.split():
        if token in _STOPWORDS:
            continue
        if len(token) < 4 and not any(ch.isdigit() for ch in token):
            continue
        tokens.add(token)
    return tokens


def truncate_text(value: Any, limit: int) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def compact_finding(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "severity": str(item.get("severity") or ""),
        "category": str(item.get("category") or ""),
        "finding": truncate_text(item.get("finding"), 320),
        "value_found": truncate_text(item.get("value_found"), 160),
        "recommendation": truncate_text(item.get("recommendation"), 220),
    }


def build_style_examples(
    candidates: list[BlockCandidate],
    current: BlockCandidate,
    *,
    limit: int,
    max_chars: int = 6500,
) -> str:
    if limit <= 0:
        return ""
    current_key = (
        current.object_slug,
        current.discipline,
        current.document,
        current.version,
        current.block_id,
    )
    ranked: list[tuple[tuple[int, int, int, int, str, str], BlockCandidate]] = []
    for candidate in candidates:
        key = (
            candidate.object_slug,
            candidate.discipline,
            candidate.document,
            candidate.version,
            candidate.block_id,
        )
        if key == current_key or not candidate.gpt_findings:
            continue
        same_discipline = int(candidate.discipline == current.discipline)
        same_object = int(candidate.object_slug == current.object_slug)
        same_document = int(candidate.document == current.document)
        ranked.append(
            (
                (
                    same_discipline,
                    same_object,
                    same_document,
                    min(len(candidate.gpt_findings), 6),
                    candidate.document,
                    candidate.block_id,
                ),
                candidate,
            )
        )
    ranked.sort(reverse=True)
    examples: list[dict[str, Any]] = []
    for _, candidate in ranked:
        item = {
            "discipline": candidate.discipline,
            "document": candidate.document,
            "block_id": candidate.block_id,
            "gpt_findings": [compact_finding(f) for f in candidate.gpt_findings[:4]],
        }
        probe = json.dumps(examples + [item], ensure_ascii=False, indent=2)
        if len(probe) > max_chars and examples:
            break
        examples.append(item)
        if len(examples) >= limit:
            break
    return json.dumps(examples, ensure_ascii=False, indent=2) if examples else ""


def default_style_examples(profile: str, explicit: int | None) -> int:
    if explicit is not None:
        return explicit
    return 4 if profile == "gpt_mimic_examples" else 0


def similarity(left: dict[str, Any], right: dict[str, Any]) -> float:
    left_text = finding_text(left)
    right_text = finding_text(right)
    seq = SequenceMatcher(None, left_text, right_text).ratio()
    lt, rt = match_tokens(left_text), match_tokens(right_text)
    if not lt or not rt:
        return seq
    inter = lt & rt
    if not inter:
        return seq
    containment = len(inter) / min(len(lt), len(rt))
    jaccard = len(inter) / len(lt | rt)
    return max(seq, containment * 0.75 + jaccard * 0.25)


def greedy_match(
    gpt_findings: list[dict[str, Any]],
    codex_findings: list[dict[str, Any]],
    *,
    threshold: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    pairs: list[tuple[float, int, int]] = []
    for gi, gf in enumerate(gpt_findings):
        for ci, cf in enumerate(codex_findings):
            score = similarity(gf, cf)
            if score >= threshold:
                pairs.append((score, gi, ci))
    pairs.sort(reverse=True)
    used_g: set[int] = set()
    used_c: set[int] = set()
    matches: list[dict[str, Any]] = []
    for score, gi, ci in pairs:
        if gi in used_g or ci in used_c:
            continue
        used_g.add(gi)
        used_c.add(ci)
        matches.append(
            {
                "score": round(score, 3),
                "gpt_index": gi,
                "codex_index": ci,
                "same_severity": str(gpt_findings[gi].get("severity") or "")
                == str(codex_findings[ci].get("severity") or ""),
                "same_category": str(gpt_findings[gi].get("category") or "")
                == str(codex_findings[ci].get("category") or ""),
                "gpt": gpt_findings[gi],
                "codex": codex_findings[ci],
            }
        )
    missed = [
        {"gpt_index": idx, "gpt": item}
        for idx, item in enumerate(gpt_findings)
        if idx not in used_g
    ]
    extra = [
        {"codex_index": idx, "codex": item}
        for idx, item in enumerate(codex_findings)
        if idx not in used_c
    ]
    return matches, missed, extra


def collect_candidates() -> list[BlockCandidate]:
    candidates: list[BlockCandidate] = []
    for latest_dir in V2_OBJECTS_ROOT.glob("*/disciplines/*/documents/*/versions/*/03_analysis/latest"):
        if not latest_dir.is_dir():
            continue
        parsed_parts = latest_parts(latest_dir)
        if not parsed_parts:
            continue
        object_slug, discipline, document, version = parsed_parts
        version_dir = latest_dir.parents[1]
        blocks_analysis_path = latest_dir / "01_blocks_analysis.json"
        graph_path = latest_dir / "document_graph.json"
        index_paths = [
            latest_dir / dirname / "index.json"
            for dirname in (STAGE02_BLOCKS_DIRNAME, "blocks", "blocks_gemma_100", "blocks_gemma_300")
        ]
        index_paths = [path for path in index_paths if path.is_file()]
        if not (blocks_analysis_path.is_file() and graph_path.is_file() and index_paths):
            continue
        try:
            data = load_json(blocks_analysis_path)
            project_info = load_version_project_info(version_dir)
        except Exception:
            continue
        block_analyses = data.get("block_analyses") if isinstance(data, dict) else None
        if not isinstance(block_analyses, list):
            continue
        by_id: dict[str, tuple[dict[str, Any], str, Path]] = {}
        for index_path in index_paths:
            try:
                index = load_json(index_path)
            except Exception:
                continue
            index_blocks = index.get("blocks") if isinstance(index, dict) else None
            if not isinstance(index_blocks, list):
                continue
            dirname = index_path.parent.name
            for block in index_blocks:
                if not isinstance(block, dict) or not block.get("block_id"):
                    continue
                block_id = str(block.get("block_id") or "")
                file_name = str(block.get("file") or "").strip()
                if not file_name:
                    continue
                image_path = latest_dir / dirname / file_name
                if image_path.is_file() and block_id not in by_id:
                    by_id[block_id] = (block, dirname, image_path)
        md_cache: dict[str, str] = {}
        for record in block_analyses:
            if not isinstance(record, dict):
                continue
            gpt_findings = record.get("findings") or []
            if not isinstance(gpt_findings, list) or not gpt_findings:
                continue
            block_id = str(record.get("block_id") or "").strip()
            block_tuple = by_id.get(block_id)
            if not block_tuple:
                continue
            block, image_source_dir, image_path = block_tuple
            enrichment, src = get_enrichment(version_dir, md_cache, project_info, block_id)
            if enrichment is None:
                continue
            try:
                page = int(block.get("page") or record.get("page") or 0)
            except (TypeError, ValueError):
                page = 0
            candidates.append(
                BlockCandidate(
                    object_slug=object_slug,
                    discipline=discipline,
                    document=document,
                    version=version,
                    version_dir=version_dir,
                    latest_dir=latest_dir,
                    block_id=block_id,
                    page=page,
                    image_path=image_path,
                    image_source_dir=image_source_dir,
                    block_record=block,
                    gpt_findings=[f for f in gpt_findings if isinstance(f, dict)],
                    enrichment_source=src,
                )
            )
    return candidates


def select_balanced(
    candidates: list[BlockCandidate],
    *,
    limit: int,
    object_filter: str | None,
    discipline_filter: str | None,
    document_filter: str | None,
    block_filter: str | None,
) -> list[BlockCandidate]:
    filtered = []
    seen_blocks: set[tuple[str, str, str, str, str]] = set()
    for candidate in candidates:
        if object_filter and object_filter.lower() not in candidate.object_slug.lower():
            continue
        if discipline_filter and candidate.discipline.lower() != discipline_filter.lower():
            continue
        if document_filter and document_filter.lower() not in candidate.document.lower():
            continue
        if block_filter:
            requested_blocks = [
                part.strip().lower()
                for part in block_filter.split(",")
                if part.strip()
            ]
            if requested_blocks and not any(
                part in candidate.block_id.lower() for part in requested_blocks
            ):
                continue
        key = (
            candidate.object_slug,
            candidate.discipline,
            candidate.document,
            candidate.version,
            candidate.block_id,
        )
        if key in seen_blocks:
            continue
        seen_blocks.add(key)
        filtered.append(candidate)

    by_disc: dict[str, list[BlockCandidate]] = defaultdict(list)
    for candidate in filtered:
        by_disc[candidate.discipline].append(candidate)
    for bucket in by_disc.values():
        bucket.sort(
            key=lambda c: (
                -len(c.gpt_findings),
                c.object_slug,
                c.document,
                c.version,
                c.page,
                c.block_id,
            )
        )

    selected: list[BlockCandidate] = []
    order = [d for d in DISCIPLINE_ORDER if d in by_disc] + sorted(set(by_disc) - set(DISCIPLINE_ORDER))
    round_idx = 0
    while len(selected) < limit:
        added = False
        for discipline in order:
            bucket = by_disc.get(discipline, [])
            if round_idx < len(bucket):
                selected.append(bucket[round_idx])
                added = True
                if len(selected) >= limit:
                    break
        if not added:
            break
        round_idx += 1
    return selected


def build_codex_task(
    *,
    system_prompt: str,
    user_text: str,
    image_path: Path,
    output_path: Path,
    candidate: BlockCandidate,
    profile: str,
    style_examples_text: str,
) -> str:
    if profile in {"gpt_mimic", "gpt_mimic_examples"}:
        profile_rules = """
Режим профиля: GPT-mimic.

Цель — максимально приблизить результат к production GPT/OpenRouter Stage 02,
а не сыграть роль самого осторожного проверяющего. В этом этапе GPT обычно
возвращает не только доказанные нарушения, но и инженерные замечания, риски
координации и неполноту оформления, если они видны в блоке, описании или
контексте страницы.

Правила полноты:
1. Используй только данные из этого задания и прикреплённое изображение.
2. Не читай другие файлы проекта и не запускай shell-команды.
3. Пройди блок минимум тремя мысленными проходами:
   - прямые противоречия размеров, отметок, марок, этажей, диаметров, классов;
   - внутренние расхождения похожих марок и названий: варианты одного изделия
     с разными последними символами, цифрами или "х" должны стать отдельным
     finding про унификацию маркировки;
   - неполнота документации: пустые/условные значения, "xxx", "?", ссылки без
     номера листа/узла, "***", нерасшифрованные позиции, неполные спецификации;
   - смежные проверки: архитектура/конструкции/ОВ/ВК/ЭОМ/ТХ, заземление,
     защитные меры, пожарные/эксплуатационные риски, антикоррозия, защитный
     слой, анкеровка, узлы примыканий.
4. Не отбрасывай замечание только потому, что оно требует проверки по смежным.
   Для таких случаев ставь severity "ПРОВЕРИТЬ ПО СМЕЖНЫМ".
5. Если замечание скорее про качество оформления или недостающую информацию,
   ставь severity "РЕКОМЕНДАТЕЛЬНОЕ".
6. Если в блоке есть несколько независимых сигналов, возвращай 2-6 отдельных
   findings, не объединяй их в одно общее замечание.
7. Не выдумывай чисел и марок. В value_found указывай точную видимую цитату,
   значение или короткий фрагмент из описания/контекста. Если точной цитаты нет,
   оставь пустую строку.
8. Пустой массив допустим только если после всех проходов нет ни одного
   читаемого инженерного, координационного или оформительского риска.
9. Верни финальным ответом ОДИН JSON объект, без markdown, без преамбулы,
   без статуса.
""".strip()
    else:
        profile_rules = """
Требование:
1. Используй только данные из этого задания и прикреплённое изображение.
2. Не читай другие файлы проекта и не запускай shell-команды.
3. Найди только инженерные проблемы в блоке. Если проблем нет, верни пустой массив.
4. Верни финальным ответом ОДИН JSON объект, без markdown, без преамбулы, без статуса.
""".strip()

    examples_block = ""
    if style_examples_text:
        examples_block = f"""

<GPT_STYLE_EXAMPLES_FROM_OTHER_BLOCKS>
Это НЕ ответы для текущего блока. Это калибровочные примеры того, как production
GPT формулировал findings на других блоках. Повтори стиль, уровень детализации,
разделение замечаний и severity/category, но не копируй факты, которых нет в
текущем блоке.
{style_examples_text}
</GPT_STYLE_EXAMPLES_FROM_OTHER_BLOCKS>
""".rstrip()

    return f"""
Ты выполняешь A/B-проверку Stage 02 block_analysis.

Ниже приведены РОВНО те же текстовые данные, которые production Stage 02
отправляет GPT-5.4 через OpenRouter для одного графического блока. PNG блока
прикреплён к этой Codex exec сессии через --image; путь также указан ниже.

{profile_rules}

Формат JSON должен соответствовать схеме:
{json.dumps(RESPONSE_SCHEMA["schema"], ensure_ascii=False, indent=2)}

Контекст блока:
- document: {candidate.object_slug}/{candidate.discipline}/{candidate.document}/{candidate.version}
- block_id: {candidate.block_id}
- page: {candidate.page}
- image: {image_path}

<SYSTEM_PROMPT_FROM_PRODUCTION>
{system_prompt}
</SYSTEM_PROMPT_FROM_PRODUCTION>
{examples_block}

<USER_TEXT_FROM_PRODUCTION>
{user_text}
</USER_TEXT_FROM_PRODUCTION>
""".strip()


def parse_codex_output(text: str) -> tuple[dict[str, Any] | None, str | None]:
    try:
        data = json.loads(text)
        if isinstance(data, dict) and isinstance(data.get("findings"), list):
            return data, None
        if isinstance(data, list):
            return {"findings": data}, None
        return None, "JSON output is not {'findings': [...]}"
    except Exception:
        pass

    match = re.search(r"\{[\s\S]*\"findings\"[\s\S]*\}", text or "")
    if match:
        try:
            data = json.loads(match.group(0))
            if isinstance(data, dict) and isinstance(data.get("findings"), list):
                return data, "parsed_from_stdout_fallback"
        except Exception:
            pass
    return None, f"codex_json_not_found; text={text[:500]!r}"


async def run_codex_image_json(
    *,
    task_text: str,
    image_path: Path,
    model: str,
    reasoning_effort: str,
    timeout: int,
    project_id: str,
) -> tuple[int, str, int, str, int | None]:
    """Run Codex with one attached image and parse the final `-o` answer.

    We intentionally do not ask Codex to write files through its sandbox. The
    CLI writes the final answer to `-o`; this wrapper then persists artifacts.
    """
    cli = find_codex_cli()
    if not cli:
        return 127, "codex_cli_not_found", 0, "", None
    resolved_model = resolve_codex_model(model)
    fd, out_name = tempfile.mkstemp(prefix="stage02_codex_image_", suffix=".json")
    os.close(fd)
    out_file = Path(out_name)
    cmd = [
        cli,
        "exec",
        "--ephemeral",
        "--ignore-user-config",
        "--ignore-rules",
        "--skip-git-repo-check",
        "--sandbox",
        "read-only",
        "--model",
        resolved_model,
        "-c",
        f'model_reasoning_effort="{reasoning_effort}"',
        "--image",
        str(image_path),
        "-C",
        str(REPO_ROOT),
        "-o",
        str(out_file),
        "-",
    ]
    env_overrides = {k: None for k in os.environ if k.startswith("CLAUDE")}
    started = time.monotonic()
    try:
        exit_code, stdout, stderr = await run_command(
            cmd,
            input_text=task_text,
            timeout=timeout,
            env_overrides=env_overrides,
            cwd=str(REPO_ROOT),
            project_id=project_id,
        )
        duration_ms = int((time.monotonic() - started) * 1000)
        try:
            final_text = out_file.read_text(encoding="utf-8", errors="replace")
        except OSError:
            final_text = ""
        combined = "\n".join(part for part in (stdout, stderr, final_text) if part)
        return (
            exit_code,
            combined,
            duration_ms,
            final_text or stdout or stderr or "",
            extract_codex_tokens(combined),
        )
    finally:
        try:
            out_file.unlink()
        except OSError:
            pass


async def run_one(
    candidate: BlockCandidate,
    *,
    index: int,
    run_dir: Path,
    model: str,
    reasoning_effort: str,
    timeout: int,
    threshold: float,
    profile: str,
    system_prompt_profile: str,
    all_candidates: list[BlockCandidate],
    style_examples_limit: int,
    resume: bool,
    document_retrieval_profile: str = DOCUMENT_RETRIEVAL_PROFILE_NONE,
) -> dict[str, Any]:
    block_dir = run_dir / "blocks" / f"{index:03d}_{safe_part(candidate.block_id)}"
    block_dir.mkdir(parents=True, exist_ok=True)
    image_copy = block_dir / candidate.image_path.name
    if not image_copy.exists():
        shutil.copy2(candidate.image_path, image_copy)

    project_info = load_version_project_info(candidate.version_dir)
    section = (project_info.get("section") or candidate.discipline or "_generic").strip() or "_generic"
    system_prompt = build_system_prompt(
        section,
        extended=True,
        prompt_profile=system_prompt_profile,
    )
    graph = load_json(candidate.latest_dir / "document_graph.json")
    enrichment, enrichment_source = get_enrichment(candidate.version_dir, {}, project_info, candidate.block_id)
    page_text = load_page_text(graph, candidate.page)
    routed_context: tuple[str, str] | None = None
    document_context = ""
    retrieval_receipt: dict[str, Any] = {
        "profile": DOCUMENT_RETRIEVAL_PROFILE_NONE,
        "status": "not_performed",
    }
    if document_retrieval_profile != DOCUMENT_RETRIEVAL_PROFILE_NONE:
        package = _cached_block_package(
            str(candidate.latest_dir),
            candidate.block_id,
            candidate.page,
        )
        package_text = str(package.get("user_text") or "")
        package_kind = str(package.get("source_kind") or "error")
        if package_text:
            routed_context = (package_text, package_kind)
        classification = package.get("classification") or {}
        production_query = str(
            classification.get("block_title")
            or classification.get("description")
            or package_text
        )
        if document_retrieval_profile == DOCUMENT_RETRIEVAL_PROFILE_PRODUCTION:
            from backend.app.pipeline.stages.block_analysis.document_retrieval import (
                retrieve_document_context,
            )

            document_context, retrieval_receipt = retrieve_document_context(
                graph,
                production_query,
                candidate.page,
            )
            retrieval_receipt["profile"] = DOCUMENT_RETRIEVAL_PROFILE_PRODUCTION
        elif document_retrieval_profile == DOCUMENT_RETRIEVAL_PROFILE_TARGETED_V3:
            from backend.app.pipeline.stages.block_analysis.document_retrieval import (
                retrieve_targeted_document_context,
            )

            targeted_query = "\n".join(
                part
                for part in (
                    package_text,
                    str(classification.get("block_title") or ""),
                    str(classification.get("description") or ""),
                    json.dumps(enrichment, ensure_ascii=False),
                    str(candidate.block_record.get("ocr_label") or ""),
                )
                if part
            )
            document_context, retrieval_receipt = retrieve_targeted_document_context(
                graph,
                targeted_query,
                candidate.page,
                discipline=section,
            )
        else:
            raise ValueError(
                f"Unknown document retrieval profile {document_retrieval_profile!r}; "
                f"expected one of {sorted(DOCUMENT_RETRIEVAL_PROFILES)}"
            )
    user_text, context_source = build_effective_block_context(
        candidate.block_record,
        enrichment,
        page_text,
        routed_context=routed_context,
        document_context=document_context,
    )
    style_examples_text = build_style_examples(
        all_candidates,
        candidate,
        limit=style_examples_limit,
    )

    output_path = block_dir / "codex_findings.json"
    input_path = block_dir / "input.json"
    result_path = block_dir / "comparison.json"

    write_json(
        input_path,
        {
            "candidate": {
                **asdict(candidate),
                "version_dir": str(candidate.version_dir),
                "latest_dir": str(candidate.latest_dir),
                "image_path": str(candidate.image_path),
                "image_source_dir": candidate.image_source_dir,
            },
            "image_copy": str(image_copy),
            "section": section,
            "system_prompt": system_prompt,
            "user_text": user_text,
            "page_text_chars": len(page_text or ""),
            "effective_user_text_chars": len(user_text),
            "context_source": context_source,
            "enrichment_source": enrichment_source,
            "enrichment": enrichment,
            "document_retrieval_profile": document_retrieval_profile,
            "document_retrieval": retrieval_receipt,
            "document_context": document_context,
            "gpt_findings": candidate.gpt_findings,
            "profile": profile,
            "system_prompt_profile": system_prompt_profile,
            "style_examples_limit": style_examples_limit,
            "style_examples": json.loads(style_examples_text) if style_examples_text else [],
        },
    )

    if resume and result_path.is_file():
        return load_json(result_path)

    started = datetime.now(UTC).isoformat()
    if profile == "baseline":
        # Keep the comparison on the exact production JSON transport. Besides
        # matching Stage 01 behaviour, JSONL exposes input/cache/output/reasoning
        # token counters instead of the CLI's single legacy total.
        codex_result = await call_codex_for_block(
            candidate.block_record,
            enrichment,
            page_text,
            block_dir,
            model=model,
            system_prompt=system_prompt,
            timeout=timeout,
            reasoning_effort=reasoning_effort,
            project_id=candidate.document,
            routed_context=routed_context,
            document_context=document_context,
        )
        parsed = codex_result.get("parsed")
        parse_error = codex_result.get("parse_error") or codex_result.get("error")
        final_text = str(codex_result.get("raw_content") or "")
        combined = final_text
        duration_ms = int(codex_result.get("elapsed_ms") or 0)
        exit_code = 0 if codex_result.get("ok") else 1
        input_tokens = int(codex_result.get("input_tokens") or 0)
        cached_input_tokens = int(codex_result.get("cached_input_tokens") or 0)
        output_tokens = int(codex_result.get("output_tokens") or 0)
        reasoning_tokens = int(codex_result.get("reasoning_tokens") or 0)
        tokens_used = input_tokens + output_tokens
        resolved_model = str(codex_result.get("model") or model)
    else:
        task_text = build_codex_task(
            system_prompt=system_prompt,
            user_text=user_text,
            image_path=image_copy,
            output_path=output_path,
            candidate=candidate,
            profile=profile,
            style_examples_text=style_examples_text,
        )
        (
            exit_code,
            combined,
            duration_ms,
            final_text,
            tokens_used,
        ) = await run_codex_image_json(
            task_text=task_text,
            image_path=image_copy,
            model=model,
            reasoning_effort=reasoning_effort,
            timeout=timeout,
            project_id=candidate.document,
        )
        parsed, parse_error = parse_codex_output(final_text)
        input_tokens = None
        cached_input_tokens = None
        output_tokens = None
        reasoning_tokens = None
        resolved_model = model
    codex_findings = []
    if isinstance(parsed, dict):
        codex_findings = [
            item for item in (parsed.get("findings") or []) if isinstance(item, dict)
        ]
        write_json(output_path, {"findings": codex_findings})
    matches, missed, extra = greedy_match(
        candidate.gpt_findings,
        codex_findings,
        threshold=threshold,
    )
    result = {
        "candidate": {
            "object_slug": candidate.object_slug,
            "discipline": candidate.discipline,
            "document": candidate.document,
            "version": candidate.version,
            "block_id": candidate.block_id,
            "page": candidate.page,
            "label": candidate.label,
        },
        "paths": {
            "input": str(input_path),
            "image": str(image_copy),
            "codex_output": str(output_path),
        },
        "started_at": started,
        "exit_code": exit_code,
        "ok": exit_code == 0 and parsed is not None,
        "parse_error": parse_error,
        "duration_ms": duration_ms,
        "profile": profile,
        "system_prompt_profile": system_prompt_profile,
        "document_retrieval_profile": document_retrieval_profile,
        "document_retrieval": retrieval_receipt,
        "model": resolved_model,
        "reasoning_effort": reasoning_effort,
        "tokens_used": tokens_used,
        "input_tokens": input_tokens,
        "cached_input_tokens": cached_input_tokens,
        "output_tokens": output_tokens,
        "reasoning_tokens": reasoning_tokens,
        "gpt_findings_count": len(candidate.gpt_findings),
        "codex_findings_count": len(codex_findings),
        "matches_count": len(matches),
        "missed_count": len(missed),
        "extra_count": len(extra),
        "matches": matches,
        "missed_gpt": missed,
        "extra_codex": extra,
        "gpt_findings": candidate.gpt_findings,
        "codex_findings": codex_findings,
        "codex_result_text": final_text,
        "combined_tail": combined[-2000:],
    }
    write_json(result_path, result)
    return result


def summarize(results: list[dict[str, Any]], *, threshold: float) -> dict[str, Any]:
    ok_results = [r for r in results if r.get("ok")]
    token_values = [
        int(r["tokens_used"])
        for r in results
        if isinstance(r.get("tokens_used"), int) and int(r["tokens_used"]) >= 0
    ]
    input_tokens = sum(int(r.get("input_tokens") or 0) for r in ok_results)
    cached_input_tokens = sum(int(r.get("cached_input_tokens") or 0) for r in ok_results)
    output_tokens = sum(int(r.get("output_tokens") or 0) for r in ok_results)
    reasoning_tokens = sum(int(r.get("reasoning_tokens") or 0) for r in ok_results)
    total_gpt = sum(int(r.get("gpt_findings_count") or 0) for r in ok_results)
    total_codex = sum(int(r.get("codex_findings_count") or 0) for r in ok_results)
    total_matches = sum(int(r.get("matches_count") or 0) for r in ok_results)
    total_missed = sum(int(r.get("missed_count") or 0) for r in ok_results)
    total_extra = sum(int(r.get("extra_count") or 0) for r in ok_results)
    blocks_with_no_codex = [r for r in ok_results if int(r.get("codex_findings_count") or 0) == 0]
    blocks_full_cover = [
        r for r in ok_results
        if int(r.get("missed_count") or 0) == 0 and int(r.get("gpt_findings_count") or 0) > 0
    ]
    severity_pairs = Counter()
    category_pairs = Counter()
    for result in ok_results:
        for match in result.get("matches") or []:
            g = match.get("gpt") or {}
            c = match.get("codex") or {}
            severity_pairs[(str(g.get("severity") or ""), str(c.get("severity") or ""))] += 1
            category_pairs[(str(g.get("category") or ""), str(c.get("category") or ""))] += 1
    threshold_sweep = {}
    for sweep_threshold in (0.46, 0.40, 0.35, 0.30, 0.25):
        sweep_matches = 0
        sweep_full_blocks = 0
        for result in ok_results:
            matches, missed, _extra = greedy_match(
                result.get("gpt_findings") or [],
                result.get("codex_findings") or [],
                threshold=sweep_threshold,
            )
            sweep_matches += len(matches)
            if not missed and result.get("gpt_findings"):
                sweep_full_blocks += 1
        threshold_sweep[f"{sweep_threshold:.2f}"] = {
            "matched_gpt_findings": sweep_matches,
            "gpt_recall_by_codex": round(sweep_matches / total_gpt, 3) if total_gpt else None,
            "blocks_with_full_gpt_coverage": sweep_full_blocks,
        }

    return {
        "threshold": threshold,
        "threshold_sweep": threshold_sweep,
        "blocks_requested": len(results),
        "blocks_ok": len(ok_results),
        "blocks_failed": len(results) - len(ok_results),
        "codex_tokens_counted_blocks": len(token_values),
        "codex_tokens_total": sum(token_values) if token_values else None,
        "codex_tokens_avg_per_block": round(sum(token_values) / len(token_values), 1) if token_values else None,
        "codex_tokens_estimated_per_100_blocks": round(sum(token_values) / len(token_values) * 100) if token_values else None,
        "input_tokens_total": input_tokens,
        "cached_input_tokens_total": cached_input_tokens,
        "output_tokens_total": output_tokens,
        "reasoning_tokens_total": reasoning_tokens,
        "input_tokens_avg_per_block": round(input_tokens / len(ok_results), 1) if ok_results else None,
        "output_tokens_avg_per_block": round(output_tokens / len(ok_results), 1) if ok_results else None,
        "gpt_findings_total": total_gpt,
        "codex_findings_total": total_codex,
        "matched_gpt_findings": total_matches,
        "missed_gpt_findings": total_missed,
        "extra_codex_findings": total_extra,
        "gpt_recall_by_codex": round(total_matches / total_gpt, 3) if total_gpt else None,
        "codex_extra_per_block": round(total_extra / len(ok_results), 2) if ok_results else None,
        "blocks_with_no_codex_findings": len(blocks_with_no_codex),
        "blocks_with_full_gpt_coverage": len(blocks_full_cover),
        "by_discipline": {
            discipline: {
                "blocks": len(items),
                "gpt_findings": sum(int(r.get("gpt_findings_count") or 0) for r in items),
                "codex_findings": sum(int(r.get("codex_findings_count") or 0) for r in items),
                "matches": sum(int(r.get("matches_count") or 0) for r in items),
                "missed": sum(int(r.get("missed_count") or 0) for r in items),
                "extra": sum(int(r.get("extra_count") or 0) for r in items),
            }
            for discipline, items in sorted(
                defaultdict(list, {
                    d: [r for r in ok_results if (r.get("candidate") or {}).get("discipline") == d]
                    for d in sorted({(r.get("candidate") or {}).get("discipline") for r in ok_results})
                }).items()
            )
        },
        "severity_pairs": [
            {"gpt": left, "codex": right, "count": count}
            for (left, right), count in severity_pairs.most_common()
        ],
        "category_pairs_top": [
            {"gpt": left, "codex": right, "count": count}
            for (left, right), count in category_pairs.most_common(30)
        ],
        "failed_blocks": [
            {
                "label": (r.get("candidate") or {}).get("label"),
                "exit_code": r.get("exit_code"),
                "parse_error": r.get("parse_error"),
            }
            for r in results
            if not r.get("ok")
        ],
    }


def write_markdown_report(run_dir: Path, summary: dict[str, Any], results: list[dict[str, Any]]) -> None:
    ok_results = [r for r in results if r.get("ok")]
    tokens_total = summary.get("codex_tokens_total")
    lines = [
        "# Stage 02 GPT vs Codex block A/B",
        "",
        f"- Blocks OK: {summary['blocks_ok']} / {summary['blocks_requested']}",
        f"- Codex tokens: {tokens_total if tokens_total is not None else 'n/a'}",
        f"- Input / cached input: {summary['input_tokens_total']} / {summary['cached_input_tokens_total']}",
        f"- Output / reasoning output: {summary['output_tokens_total']} / {summary['reasoning_tokens_total']}",
        f"- GPT findings: {summary['gpt_findings_total']}",
        f"- Codex findings: {summary['codex_findings_total']}",
        f"- Matched GPT findings: {summary['matched_gpt_findings']}",
        f"- Missed GPT findings: {summary['missed_gpt_findings']}",
        f"- Extra Codex findings: {summary['extra_codex_findings']}",
        f"- Codex recall vs GPT: {summary['gpt_recall_by_codex']}",
        "",
        "## By Discipline",
        "",
        "| Discipline | Blocks | GPT | Codex | Matched | Missed | Extra |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for discipline, stats in summary.get("by_discipline", {}).items():
        lines.append(
            f"| {discipline} | {stats['blocks']} | {stats['gpt_findings']} | "
            f"{stats['codex_findings']} | {stats['matches']} | {stats['missed']} | {stats['extra']} |"
        )
    lines.extend(["", "## Blocks", ""])
    for result in ok_results:
        candidate = result.get("candidate") or {}
        lines.append(
            f"### {candidate.get('discipline')} / {candidate.get('document')} / "
            f"{candidate.get('block_id')}"
        )
        lines.append(
            f"- GPT={result.get('gpt_findings_count')} Codex={result.get('codex_findings_count')} "
            f"matched={result.get('matches_count')} missed={result.get('missed_count')} "
            f"extra={result.get('extra_count')} tokens={result.get('tokens_used')}"
        )
        lines.append(
            f"- Token split: input={result.get('input_tokens')} "
            f"cached={result.get('cached_input_tokens')} output={result.get('output_tokens')} "
            f"reasoning={result.get('reasoning_tokens')}"
        )
        if result.get("missed_gpt"):
            lines.append("- Missed GPT:")
            for item in result["missed_gpt"][:3]:
                g = item.get("gpt") or {}
                lines.append(
                    f"  - [{g.get('severity')}] {str(g.get('finding') or '')[:240]}"
                )
        if result.get("extra_codex"):
            lines.append("- Extra Codex:")
            for item in result["extra_codex"][:3]:
                c = item.get("codex") or {}
                lines.append(
                    f"  - [{c.get('severity')}] {str(c.get('finding') or '')[:240]}"
                )
        lines.append("")
    (run_dir / "report.md").write_text("\n".join(lines), encoding="utf-8")


async def async_main(args: argparse.Namespace) -> int:
    if not find_codex_cli():
        raise RuntimeError("Codex CLI not found")
    candidates = collect_candidates()
    selected = select_balanced(
        candidates,
        limit=args.limit,
        object_filter=args.object,
        discipline_filter=args.discipline,
        document_filter=args.document,
        block_filter=args.block,
    )
    if not selected:
        raise RuntimeError("No candidate blocks selected")

    run_dir = Path(args.run_dir) if args.run_dir else OUT_ROOT / utc_stamp()
    run_dir.mkdir(parents=True, exist_ok=True)
    style_examples_limit = default_style_examples(args.profile, args.style_examples)
    write_json(
        run_dir / "selection.json",
        {
            "limit": args.limit,
            "profile": args.profile,
            "system_prompt_profile": args.system_prompt_profile,
            "document_retrieval_profile": args.document_retrieval_profile,
            "model": args.model,
            "reasoning_effort": args.reasoning_effort,
            "style_examples_limit": style_examples_limit,
            "available_candidates": len(candidates),
            "selected": [
                {
                    "label": item.label,
                    "discipline": item.discipline,
                    "document": item.document,
                    "version": item.version,
                    "block_id": item.block_id,
                    "page": item.page,
                    "gpt_findings_count": len(item.gpt_findings),
                    "image_path": str(item.image_path),
                    "image_source_dir": item.image_source_dir,
                    "enrichment_source": item.enrichment_source,
                }
                for item in selected
            ],
        },
    )
    print(f"[stage02-codex-ab] run_dir={run_dir}")
    print(f"[stage02-codex-ab] selected={len(selected)} from available={len(candidates)}")

    results: list[dict[str, Any]] = []
    for idx, candidate in enumerate(selected, start=1):
        print(
            f"[stage02-codex-ab] {idx:02d}/{len(selected)} {candidate.label} "
            f"gpt_findings={len(candidate.gpt_findings)}",
            flush=True,
        )
        result = await run_one(
            candidate,
            index=idx,
            run_dir=run_dir,
            model=args.model,
            reasoning_effort=args.reasoning_effort,
            timeout=args.timeout,
            threshold=args.threshold,
            profile=args.profile,
            system_prompt_profile=args.system_prompt_profile,
            all_candidates=candidates,
            style_examples_limit=style_examples_limit,
            resume=args.resume,
            document_retrieval_profile=args.document_retrieval_profile,
        )
        results.append(result)
        print(
            f"[stage02-codex-ab]   ok={result.get('ok')} codex={result.get('codex_findings_count')} "
            f"matched={result.get('matches_count')} missed={result.get('missed_count')} "
            f"extra={result.get('extra_count')} duration_ms={result.get('duration_ms')}",
            flush=True,
        )
        write_json(run_dir / "results.partial.json", results)

    summary = summarize(results, threshold=args.threshold)
    write_json(run_dir / "results.json", results)
    write_json(run_dir / "summary.json", summary)
    write_markdown_report(run_dir, summary, results)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=30)
    parser.add_argument("--model", default=os.environ.get("AUDIT_CODEX_STAGE_MODEL", "codex/gpt-5.4"))
    parser.add_argument(
        "--reasoning-effort",
        choices=("low", "medium", "high", "xhigh", "max", "ultra"),
        default="low",
    )
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--threshold", type=float, default=0.46)
    parser.add_argument(
        "--profile",
        choices=("baseline", "gpt_mimic", "gpt_mimic_examples"),
        default="baseline",
    )
    parser.add_argument(
        "--system-prompt-profile",
        choices=tuple(sorted(SYSTEM_PROMPT_PROFILES)),
        default=SYSTEM_PROMPT_PROFILE_PRODUCTION,
        help="Stage 01 system prompt variant; production remains the default",
    )
    parser.add_argument(
        "--document-retrieval-profile",
        choices=tuple(sorted(DOCUMENT_RETRIEVAL_PROFILES)),
        default=DOCUMENT_RETRIEVAL_PROFILE_NONE,
        help="Cross-sheet retrieval variant; none preserves historical benchmark input",
    )
    parser.add_argument(
        "--style-examples",
        type=int,
        default=None,
        help="Number of other-block GPT style examples to include; default is 4 for gpt_mimic_examples, else 0",
    )
    parser.add_argument("--object", default=None, help="Substring filter for object slug")
    parser.add_argument("--discipline", default=None, help="Exact discipline filter")
    parser.add_argument("--document", default=None, help="Substring filter for document")
    parser.add_argument(
        "--block",
        default=None,
        help="Block ID substring, or comma-separated substrings",
    )
    parser.add_argument("--run-dir", default=None)
    parser.add_argument("--resume", action="store_true")
    return parser.parse_args()


def main() -> int:
    return asyncio.run(async_main(parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
