"""Досборка прогона V3 из сохранённых ответов упавшего прогона (engine 3.11.0).

Прогон, упавший ПОСЛЕ Miner и разбора unmatched-страниц (например, на Dedupe),
уже оплатил и сохранил всё дорогое: карту Mapper, принятый ответ каждой области
и ответы пакетов unmatched.  Новый прогон с ``resume_from_run_id`` не повторяет
эти вызовы: он заново готовит источники, берёт карту и ответы донора, заново
проверяет каждый ответ теми же валидаторами на свежих страницах и продолжает
с Dedupe как обычный прогон.

Сознательное исключение из правила «контрольная точка не читается прогоном»:
его снимает только явный запрос досборки, при совпадении источников (sha PDF,
структуры и кадров), промптов, схемы Miner и усилия. Модель по умолчанию та же;
явная политика remaining_stages разрешает передать оставшиеся этапы другой
модели с сохранением авторства всех ответов донора.  Любое расхождение — отказ до первого вызова.

Результат досборки честно называет донора (``provenance.resumed_from``), вызовы
донора остаются в его квитанциях с пометкой ``resumed_from_run_id``.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

RESUME_VERSION = "projectchange_v3_resume/1"
#: Что должно совпасть у донора и досборки, иначе ответы донора — ответы на другой вопрос.
MODEL_KEYS = ("provider", "model", "reasoning", "model_profile",
              "mapper_prompt_sha256", "miner_prompt_sha256", "unmatched_prompt_sha256", "miner_output_format")
#: ``structure_sha256`` сюда не входит: структура хранит пути кадров внутри папки прогона,
#: поэтому она сверяется по содержимому с поправкой на папку (``check_compatible``).
SOURCE_KEYS = ("old_pdf_sha256", "new_pdf_sha256", "source_packaging_version")
MODEL_IDENTITY_KEYS = ("provider", "model", "model_profile")
TAIL_STAGES = frozenset({"DEDUPE", "SOURCE_VERIFICATION"})


def model_identity(provenance: dict[str, Any]) -> dict[str, Any]:
    return {key: provenance.get(key) for key in (*MODEL_IDENTITY_KEYS, "reasoning")}


def mismatched_calls(calls: list[dict[str, Any]], provenance: dict[str, Any],
                     donor: Donor | None = None) -> list[str]:
    """Imported receipts belong to the donor; new calls belong to this run.

    A model handoff permits new calls only for the remaining stages. It never
    permits changing model within a call or relabelling historical receipts.
    """
    bad = []
    for call in calls:
        imported = call.get("resumed_from_run_id")
        expected = donor.provenance if donor and imported == donor.run_id else provenance
        if (imported and (donor is None or imported != donor.run_id)) or (
            provenance.get("model_handoff") and not imported and call.get("stage") not in TAIL_STAGES
        ) or (call.get("model") and any(
            call.get(key) != expected.get(key) for key in ("provider", "model", "reasoning")
        )):
            bad.append(str(call.get("call_id")))
    return bad


class ResumeRefused(ValueError):
    """Донор не годится для досборки; ни один вызов модели не выполнен."""


def _read(path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        raise ResumeRefused(f"файл прогона-донора не читается: {path.name} ({type(exc).__name__})") from exc


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


@dataclass
class Donor:
    run_id: str
    state: dict[str, Any]
    provenance: dict[str, Any]
    semantic_map: dict[str, Any]
    mapper_portions: dict[str, Any] | None
    miner_results: dict[str, Any]
    supplemental: dict[str, Any] | None
    checkpoint: dict[str, Any]
    source_manifest: dict[str, Any]
    work_dir: Path
    replayed: dict[str, Any] = field(default_factory=dict)
    #: (папка источников донора, папка досборки): пути кадров в ответах донора переносятся сюда.
    rebase_dirs: tuple[str, str] | None = None
    source_equivalence: dict[str, Any] = field(default_factory=dict)

    def rebase(self, value: Any) -> Any:
        """Та же величина, но пути кадров указывают на источники досборки (байт в байт те же кадры)."""
        if self.rebase_dirs is None:
            raise ResumeRefused("источники досборки не сверены с источниками донора")
        old, new = self.rebase_dirs
        if isinstance(value, str):
            return new + value[len(old):] if value.startswith(old + "/") else value
        if isinstance(value, list):
            return [self.rebase(item) for item in value]
        if isinstance(value, dict):
            return {key: self.rebase(item) for key, item in value.items()}
        return value

    @property
    def model_profile(self) -> str | None:
        return self.provenance.get("model_profile")

    def summary(self) -> dict[str, Any]:
        return {
            "version": RESUME_VERSION,
            "run_id": self.run_id,
            "engine_version": self.provenance.get("engine_version"),
            **model_identity(self.provenance),
            "failed_reason": self.state.get("reason_code"),
            "failed_message": self.state.get("message"),
            "model_calls": int(self.state.get("model_calls") or 0),
            "usage_total": self.provenance.get("usage_total"),
            "regions": len(self.semantic_map.get("regions") or []),
            "reused": ["semantic_map", "miner_regions", "unmatched_review"],
            "source_equivalence": dict(self.source_equivalence),
            "note": "Mapper, Miner и разбор unmatched-страниц не повторялись: ответы взяты у прогона-донора "
                    "и заново проверены теми же валидаторами на заново подготовленных источниках.",
        }


def _model_view(provenance: dict[str, Any]) -> dict[str, Any]:
    view = {key: provenance.get(key) for key in MODEL_KEYS}
    view["miner_output_format"] = (provenance.get("miner_output") or {}).get("format") if isinstance(
        provenance.get("miner_output"), dict) else provenance.get("miner_output")
    return view


def load_donor(session_id: str, pair_id: str, donor_run_id: str) -> Donor:
    """Прочитать упавший прогон и проверить, что после Miner у него всё сохранено."""
    from . import run_storage

    directory = run_storage.run_dir(session_id, pair_id, donor_run_id)
    manifest = _read(directory / "run_manifest.json")
    if not manifest:
        raise ResumeRefused(f"прогон {donor_run_id} не найден у этой пары")
    if (manifest.get("session_id"), manifest.get("pair_id"), manifest.get("run_id")) != (
            session_id, pair_id, donor_run_id):
        raise ResumeRefused("прогон-донор принадлежит другой паре")
    state = _read(directory / "state.json") or {}
    if state.get("status") != "FAILED":
        raise ResumeRefused(f"дособрать можно только упавший прогон, а этот в состоянии {state.get('status')}")
    provenance = state.get("provenance") or {}
    semantic_map = _read(directory / "project_change_v3_semantic_map.json")
    miner_results = _read(directory / "project_change_v3_miner_results.json")
    source_manifest = _read(directory / "project_change_v3_source_manifest.json")
    if not semantic_map or not miner_results or not source_manifest:
        raise ResumeRefused("прогон-донор упал раньше, чем сохранил результаты Miner и разбора unmatched-страниц")
    if miner_results.get("run_id") != donor_run_id:
        raise ResumeRefused("результаты Miner донора принадлежат другому прогону")
    ref = miner_results.get("miner_checkpoint") or {}
    checkpoint_path = directory / "project_change_v3" / "miner_checkpoints" / (
        f"project_change_v3_miner_checkpoint_{donor_run_id}.json")
    if not checkpoint_path.is_file() or not ref.get("sha256"):
        raise ResumeRefused("у прогона-донора нет контрольной точки Miner")
    with checkpoint_path.open("rb") as handle:
        if hashlib.file_digest(handle, "sha256").hexdigest() != ref["sha256"]:
            raise ResumeRefused("контрольная точка Miner донора изменилась после записи")
    checkpoint = _read(checkpoint_path)
    if checkpoint.get("run_id") != donor_run_id or checkpoint.get("regions_total") != len(checkpoint.get("regions") or []):
        raise ResumeRefused("контрольная точка Miner донора неполная")
    if checkpoint.get("semantic_map_sha256") != _canonical_sha256(semantic_map):
        raise ResumeRefused("карта областей донора не совпадает с той, по которой работал Miner")
    return Donor(
        run_id=donor_run_id, state=state, provenance=provenance, semantic_map=semantic_map,
        mapper_portions=_read(directory / "project_change_v3_mapper_portions.json"),
        miner_results=miner_results, supplemental=_read(directory / "project_change_v3_supplemental_results.json"),
        checkpoint=checkpoint, source_manifest=source_manifest, work_dir=directory / "project_change_v3",
    )


def _file_sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def check_compatible(
    donor: Donor,
    *,
    provenance: dict[str, Any],
    manifest: dict[str, Any],
    structure: list[dict[str, Any]],
    work_dir: Path,
    model_policy: str = "same_model",
) -> None:
    """Сверка источников и контракта сохранённых ответов с донором.

    По умолчанию модель совпадает. remaining_stages разрешает сменить только
    модель/провайдера оставшихся этапов; промпты, формат и источники совпадают.

    Структура страниц сверяется целиком (тексты, рамки, таблицы, блоки) после
    переноса путей кадров из папки донора в папку досборки, а каждый кадр —
    побайтно: модель донора видела ровно эти изображения.
    """
    if model_policy not in {"same_model", "remaining_stages"}:
        raise ResumeRefused("неизвестная политика модели досборки")
    theirs, ours = _model_view(donor.provenance), _model_view(provenance)
    keys = [key for key in MODEL_KEYS
            if model_policy != "remaining_stages" or key not in MODEL_IDENTITY_KEYS]
    differs = [key for key in keys if theirs.get(key) != ours.get(key)]
    if differs:
        raise ResumeRefused("у досборки другая конфигурация модели или промптов: " + ", ".join(
            f"{key} {theirs.get(key)!r} → {ours.get(key)!r}" for key in differs))
    for key in SOURCE_KEYS:
        if donor.source_manifest.get(key) != manifest.get(key) or (donor.checkpoint.get("source") or {}).get(
                key) != manifest.get(key):
            raise ResumeRefused(f"источники пары изменились после прогона-донора ({key})")
    donor_structure = _read(donor.work_dir / "DOCUMENT_STRUCTURE.json")
    if donor_structure is None:
        raise ResumeRefused("у прогона-донора нет структуры страниц")
    donor.rebase_dirs = (str(donor.work_dir), str(work_dir))
    if _canonical_sha256(donor.rebase(donor_structure)) != _canonical_sha256(structure):
        raise ResumeRefused("источники пары изменились после прогона-донора (структура страниц)")
    crops = 0
    for page in structure:
        for block in page.get("blocks") or []:
            ref = block.get("graphic_crop_ref")
            if not ref:
                continue
            ours = Path(ref)
            theirs = Path(donor.rebase_dirs[0] + ref[len(donor.rebase_dirs[1]):])
            if not ours.is_file() or not theirs.is_file() or _file_sha256(ours) != _file_sha256(theirs):
                raise ResumeRefused(f"кадр {ours.name} (стр. {page.get('physical_page')}) отличается от кадра донора")
            crops += 1
    donor.source_equivalence = {"structure_equal_after_rebase": True, "graphic_crops_byte_identical": crops}


def replay(donor: Donor, *, pair_id: str, pages_by_key: dict, validate_miner, validate_supplemental_answer) -> dict[str, Any]:
    """Заново проверить каждый ответ донора и вернуть то, что прогон получил бы сам."""
    regions = donor.semantic_map.get("regions") or []
    accepted = donor.checkpoint["regions"]
    if [row["region_id"] for row in accepted] != [region["region_id"] for region in regions]:
        raise ResumeRefused("области контрольной точки не совпадают с картой донора")
    mined_regions: list[dict[str, Any]] = []
    for region, row in zip(regions, accepted, strict=True):
        try:
            validate_miner(pair_id, donor.rebase(region), donor.rebase(row["result"]), pages_by_key)
        except Exception as exc:  # noqa: BLE001
            raise ResumeRefused(f"ответ области {region['region_id']} не проходит проверку заново: {exc}") from exc
        mined_regions.append(row["result"])
    supplemental = donor.supplemental or {"enabled": False, "batches": []}
    reviewed: set[tuple[str, int]] = set()
    failed: set[tuple[str, int]] = set()
    for batch in supplemental.get("batches") or []:
        keys = {(str(side), int(page)) for side, page in batch.get("primary_keys") or []}
        if batch.get("status") != "ACCEPTED":
            failed.update(keys)
            continue
        try:
            validate_miner(pair_id, donor.rebase(batch["region"]), donor.rebase(batch["result"]), pages_by_key)
            # JSON хранит ключи страниц списками, проверка сравнивает кортежи.
            validate_supplemental_answer({**donor.rebase(batch), "primary_keys": sorted(keys)},
                                         donor.rebase(batch["result"]))
        except Exception as exc:  # noqa: BLE001
            raise ResumeRefused(f"ответ пакета {batch.get('batch_id')} не проходит проверку заново: {exc}") from exc
        reviewed.update(keys)
        mined_regions.append(batch["result"])
    changes = [c for region in mined_regions for c in region.get("projectchanges") or []]
    hints = [h for region in mined_regions for h in region.get("unresolved_hints") or []]
    saved = donor.miner_results
    if (_canonical_sha256(mined_regions), _canonical_sha256(changes), _canonical_sha256(hints)) != (
            _canonical_sha256(saved.get("regions")), _canonical_sha256(saved.get("projectchanges")),
            _canonical_sha256(saved.get("unresolved_hints"))):
        raise ResumeRefused("результаты Miner донора не сходятся с его контрольной точкой и пакетами unmatched")
    donor.replayed = {
        "mined_regions": donor.rebase(mined_regions), "changes": donor.rebase(changes), "hints": donor.rebase(hints),
        "analysed_region_ids": [str(region["region_id"]) for region in regions],
        "reviewed_unmatched_pages": reviewed, "failed_unmatched_pages": failed,
        "supplemental_enabled": bool(supplemental.get("enabled")),
        "supplemental_batches": donor.rebase(list(supplemental.get("batches") or [])),
    }
    return donor.replayed


def donor_receipts(donor: Donor) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Квитанции вызовов и попыток Miner донора — с пометкой, чей это вызов."""
    mark = {"resumed_from_run_id": donor.run_id}
    calls = [{**call, **mark} for call in donor.provenance.get("transport_calls") or []]
    attempts = [{**row, **mark} for row in donor.provenance.get("miner_attempts") or []]
    return calls, attempts


def resume_available(session_id: str, pair_id: str, run_id: str) -> bool:
    """Дешёвая проверка для интерфейса: у прогона сохранены результаты Miner и контрольная точка.

    Полная проверка (источники, модель, повторная проверка ответов) — при запуске досборки.
    """
    from . import run_storage
    directory = run_storage.run_dir(session_id, pair_id, run_id)
    return ((directory / "project_change_v3_miner_results.json").is_file()
            and (directory / "project_change_v3" / "miner_checkpoints"
                 / f"project_change_v3_miner_checkpoint_{run_id}.json").is_file())


def donor_model_profile(session_id: str, pair_id: str, donor_run_id: str) -> str | None:
    """Модель прогона-донора (для досборки без явного выбора модели); None — не прочитать."""
    from . import run_storage
    try:
        state = json.loads((run_storage.run_dir(session_id, pair_id, donor_run_id) / "state.json").read_text("utf-8"))
    except (OSError, ValueError):
        return None
    return (state.get("provenance") or {}).get("model_profile") or None


__all__ = ["Donor", "RESUME_VERSION", "ResumeRefused", "check_compatible", "donor_model_profile", "donor_receipts",
           "load_donor", "replay", "resume_available"]
