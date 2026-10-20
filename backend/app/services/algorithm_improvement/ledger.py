"""Журнал наблюдений по решениям инженеров (этап 0.1 плана).

Источник истины — `04_review/expert_review.json` каждой версии документа, а не
`decisions_log.json` (там версии склеены ключом `(source_project, item_id)`).
Журнал служит только для ПРОВЕРКИ привязки: при каждом сохранении он снимает
текст замечания тем же `_source_summary`, и совпадение этого текста с
замечанием версии доказывает, что инженер видел именно его.

Наблюдение = решение + версия документа + её входы + её замечание + её прогон.
Решение одной версии никогда не переносится на другую. Повторные оценки одной
находки в разных версиях связываются в группу зависимости, каждая остаётся при
своей версии.

Модуль только читает данные.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

from backend.app.services.algorithm_improvement.matcher import STATUS_EXACT, STATUS_FUZZY, match_findings
from backend.app.services.common.findings_files import use_verified_findings
from backend.app.services.findings.rejected_audit_service import _TRANSFER_REASON_RE
from backend.app.services.knowledge_base.knowledge_base_service import (
    _source_summary,
    canonical_source_project,
)

# ── Классы происхождения решения (план, 5.2) ───────────────────────────────
HUMAN = "human"
HUMAN_PRESERVED = "human_preserved"
CARRIED = "carried"
CUSTOMER = "customer"
NON_ALGORITHMIC = "non_algorithmic"
EMPTY = "empty"

# ── Надёжность привязки решения к замечанию ────────────────────────────────
LINK_VERIFIED = "verified"                    # текст журнала = замечание latest
LINK_VERIFIED_OTHER = "verified_other_file"   # текст журнала = замечание другого прогона/.bak
LINK_CONTRADICTED = "contradicted"            # журнал этой версии есть, текст другой
LINK_STABLE = "unverified_stable"             # подтверждения нет, признаков переаудита нет
LINK_RERUN = "unverified_rerun"               # подтверждения нет, после проверки был переаудит
LINK_MISSING = "missing"                      # item_id нет в замечаниях версии

RELIABLE_LINKS = frozenset({LINK_VERIFIED, LINK_VERIFIED_OTHER, LINK_STABLE})

CUSTOMER_REVIEWERS = frozenset({"su10_registry"})
NON_ALGO_PREFIXES = ("REG-", "Доп-", "MIG-")
BATCH_MIN_SIZE = 5

_V_SUFFIX = re.compile(r"[_\s-]*V\d+$", re.IGNORECASE)
_VERSION_NO = re.compile(r"(\d+)$")
_STAGE01_TS = re.compile(r"^(\d{8})_(\d{6})")
_PRECEDENT = re.compile(
    r"по аналогии|аналогично\s+(отклон|предыдущ|ранее)|прецедент|ранее\s+отклон"
    r"|как\s+в\s+(томе|книге|предыдущ)|отклонено\s+в\s+v\d",
    re.IGNORECASE,
)
_TEMPLATE_DECISION = re.compile(r"^\s*Решение:\s*(отклонить|принять)", re.IGNORECASE)


# ═══════════════════════════════════════════════════════════════════════════
# Чистые функции (покрыты тестами без IO)
# ═══════════════════════════════════════════════════════════════════════════

def family_key(object_folder: str, document_code: str) -> str:
    """Семейство документа: тот же шифр без суффикса _V1/_V2 в том же объекте."""
    base = _V_SUFFIX.sub("", str(document_code or "")).strip()
    return f"{object_folder}/{base}"


def version_no(version_id: Any) -> Optional[int]:
    """'v002' / 'v2' / 'V2' → 2; пусто → None."""
    m = _VERSION_NO.search(str(version_id or "").strip())
    return int(m.group(1)) if m else None


def parse_utc(value: Any) -> Optional[datetime]:
    """ISO-время; без зоны считается UTC (так пишется `reviewed_at`)."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def classify_provenance(
    decision: dict,
    *,
    file_reviewer: str,
    version_id: str,
    finding: Optional[dict],
) -> str:
    """Класс происхождения решения (план, 5.2)."""
    if not str(decision.get("decision") or "").strip():
        return EMPTY
    item_id = str(decision.get("item_id") or "")
    if item_id.startswith(NON_ALGO_PREFIXES):
        return NON_ALGORITHMIC
    if finding and (
        finding.get("reconstructed_from_kb")
        or finding.get("reconstructed_from_register")
        or str(finding.get("origin") or "") == "customer_registry"
    ):
        return NON_ALGORITHMIC
    reviewer = str(decision.get("reviewer") or "").strip() or str(file_reviewer or "").strip()
    if reviewer in CUSTOMER_REVIEWERS:
        return CUSTOMER
    if reviewer.startswith("Авто-восстановление"):
        return HUMAN_PRESERVED
    if decision.get("carried_over"):
        same_version = version_no(decision.get("carried_from_version")) == version_no(version_id)
        return HUMAN_PRESERVED if same_version else CARRIED
    if reviewer.startswith("Авто-перенос"):
        return CARRIED
    if _TRANSFER_REASON_RE.search(str(decision.get("rejection_reason") or "")):
        return CARRIED
    return HUMAN


def reason_flags(reason: str) -> list[str]:
    flags = []
    if _PRECEDENT.search(reason or ""):
        flags.append("precedent")
    if _TEMPLATE_DECISION.search(reason or ""):
        flags.append("template_decision")
    return flags


def batch_units(decisions: Iterable[dict], file_key: str) -> dict[str, str]:
    """item_id → ключ пачки «одна секунда + одна причина» (≥ BATCH_MIN_SIZE решений).

    Для порогов доказательств пачка весит как одна единица (план, 5.2).
    """
    groups: dict[tuple, list[str]] = defaultdict(list)
    for d in decisions:
        reason = re.sub(r"\s+", " ", str(d.get("rejection_reason") or "")).strip().lower()[:80]
        if not reason:
            continue
        key = (str(d.get("timestamp") or "")[:19], str(d.get("decision") or ""), reason)
        groups[key].append(str(d.get("item_id") or ""))
    out: dict[str, str] = {}
    for key, ids in groups.items():
        if len(ids) >= BATCH_MIN_SIZE:
            digest = hashlib.sha1("|".join(key).encode("utf-8")).hexdigest()[:10]
            for iid in ids:
                out[iid] = f"{file_key}#{digest}"
    return out


@dataclass
class FindingsVariant:
    label: str          # latest:03a | latest:03 | run:<id>:03 | bak:<name>
    items: dict[str, dict]


def link_decision(
    item_id: str,
    decision_value: str,
    variants: list[FindingsVariant],
    log_entry: Optional[dict],
    *,
    rerun_after_review: bool,
) -> tuple[str, Optional[str], Optional[dict]]:
    """Надёжность привязки решения к замечанию версии.

    `log_entry` — запись decisions_log ЭТОЙ версии (вызывающий проверил версию)
    с тем же решением, либо None.
    """
    latest = next(
        ((v.label, v.items[item_id]) for v in variants
         if v.label.startswith("latest:") and item_id in v.items),
        None,
    )
    if log_entry is not None:
        summary = str(log_entry.get("summary") or "").strip()
        if summary:
            for variant in variants:
                item = variant.items.get(item_id)
                if item and _source_summary(item, "finding").strip() == summary:
                    status = LINK_VERIFIED if variant.label.startswith("latest:") else LINK_VERIFIED_OTHER
                    return status, variant.label, item
            if latest:
                return LINK_CONTRADICTED, latest[0], latest[1]
            return LINK_MISSING, None, None
    if latest is None:
        return LINK_MISSING, None, None
    return (LINK_RERUN if rerun_after_review else LINK_STABLE), latest[0], latest[1]


def finding_snapshot(item: dict) -> dict[str, Any]:
    """Снимок замечания, при котором наблюдение хранится (план, 5.2)."""
    prov = item.get("provenance") if isinstance(item.get("provenance"), dict) else {}
    evidence = item.get("evidence") or []
    return {
        "problem": str(item.get("problem") or item.get("title") or "")[:600],
        "description": str(item.get("description") or "")[:800],
        "severity": str(item.get("severity") or ""),
        "category": str(item.get("category") or ""),
        "sheet": str(item.get("sheet") or ""),
        "page": item.get("page"),
        "source_finding_ids": [str(s) for s in (item.get("source_finding_ids") or [])],
        "source_stage": str(item.get("source_stage") or ""),
        "found_by": list(prov.get("found_by") or []),
        "detector_summary": str(prov.get("detector_summary") or ""),
        "evidence_block_ids": sorted({
            str(ev.get("block_id")) for ev in evidence
            if isinstance(ev, dict) and ev.get("block_id")
        }),
    }


def finding_origin(snapshot: dict) -> str:
    """Где в конвейере возникло замечание: по source_stage и префиксам source_finding_ids."""
    stage = snapshot.get("source_stage") or ""
    if stage:
        return stage
    prefixes = {s[:2] for s in snapshot.get("source_finding_ids") or []}
    if "G-" in prefixes and "T-" in prefixes:
        return "blocks+text"
    if "G-" in prefixes:
        return "blocks"
    if "T-" in prefixes:
        return "text"
    return "merge_or_unattributed"


# ═══════════════════════════════════════════════════════════════════════════
# Чтение версии (только чтение)
# ═══════════════════════════════════════════════════════════════════════════

def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _items(data: Any) -> dict[str, dict]:
    if isinstance(data, dict):
        data = data.get("findings", data.get("items", []))
    if not isinstance(data, list):
        return {}
    return {str(x["id"]): x for x in data if isinstance(x, dict) and x.get("id")}


def sha256_file(path: Path) -> Optional[str]:
    if not path.is_file():
        return None
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def findings_variants(vdir: Path) -> list[FindingsVariant]:
    """Все доступные редакции замечаний версии: latest первым, затем прогоны и .bak."""
    out: list[FindingsVariant] = []
    latest = vdir / "03_analysis" / "latest"
    if use_verified_findings(latest):
        out.append(FindingsVariant("latest:03a", _items(_load_json(latest / "03a_norms_verified.json"))))
    out.append(FindingsVariant("latest:03", _items(_load_json(latest / "03_findings.json"))))
    runs = vdir / "03_analysis" / "runs"
    if runs.is_dir():
        for run in sorted(p for p in runs.iterdir() if p.is_dir()):
            for name, tag in (("03a_norms_verified.json", "03a"), ("03_findings.json", "03")):
                f = run / name
                if f.is_file():
                    out.append(FindingsVariant(f"run:{run.name}:{tag}", _items(_load_json(f))))
    for bak in sorted(latest.glob("03_findings*.bak*")) if latest.is_dir() else []:
        out.append(FindingsVariant(f"bak:{bak.name}", _items(_load_json(bak))))
    return [v for v in out if v.items]


def renumbered_since_backup(variants: list[FindingsVariant]) -> bool:
    """Есть .bak, где то же замечание стоит под другим номером, чем в latest.

    Сам по себе .bak — обычно дословная копия latest и о переаудите не говорит;
    риск для привязки по item_id создаёт только перенумерация.
    """
    latest = next((v.items for v in variants if v.label == "latest:03"), None)
    if not latest:
        return False
    for variant in variants:
        if variant.label.startswith("bak:"):
            links = match_findings(variant.items.values(), latest.values())["links"]
            if any(link["left"] != link["right"] for link in links):
                return True
    return False


def run_starts(vdir: Path) -> list[datetime]:
    """Время старта прогонов по каталогам `_stage01_findings_only_runs/<UTC ts>__…`."""
    starts = []
    for d in (vdir / "03_analysis").glob("**/_stage01_findings_only_runs/*"):
        m = _STAGE01_TS.match(d.name)
        if m:
            try:
                starts.append(datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc))
            except ValueError:
                pass
    return sorted(starts)


def version_inputs(vdir: Path, *, hash_inputs: bool = True) -> dict[str, Any]:
    pdfs = sorted((vdir / "01_input").glob("*.pdf"))
    work_pdf = vdir / "02_work" / "document.pdf"
    work_md = vdir / "02_work" / "document.md"
    crops = 0
    for idx in [vdir / "03_analysis" / "latest" / "blocks_stage02_100" / "index.json",
                *sorted((vdir / "03_analysis" / "runs").glob("*/blocks_stage02_100/index.json"))]:
        data = _load_json(idx)
        blocks = data.get("blocks", data) if isinstance(data, dict) else data
        if isinstance(blocks, list) and blocks:
            pngs = sum(1 for b in blocks if isinstance(b, dict) and (idx.parent / str(b.get("file") or "")).is_file())
            crops = max(crops, pngs)
            n_blocks = len(blocks)
            break
    else:
        n_blocks = 0
    return {
        "input_pdf": [p.name for p in pdfs],
        "input_pdf_sha256": [sha256_file(p) for p in pdfs] if hash_inputs else [],
        "work_pdf_sha256": sha256_file(work_pdf) if hash_inputs else None,
        "work_md_sha256": sha256_file(work_md) if hash_inputs else None,
        "has_pdf": bool(pdfs) or work_pdf.is_file(),
        "blocks": n_blocks,
        "crops_present": crops,
    }


class LogIndex:
    """decisions_log по (source_project, item_id) — только для проверки привязки."""

    def __init__(self, entries: Iterable[dict]):
        self._by_key: dict[tuple[str, str], dict] = {}
        for e in entries:
            if str(e.get("item_type") or "finding") != "finding":
                continue
            self._by_key[(str(e.get("source_project") or ""), str(e.get("item_id") or ""))] = e

    @classmethod
    def from_file(cls, path: Path) -> "LogIndex":
        data = _load_json(path) or {}
        return cls(data.get("entries", []) if isinstance(data, dict) else [])

    def for_version(
        self, document_code: str, item_id: str, version_id: str, *, single_version: bool, decision: str,
    ) -> Optional[dict]:
        e = self._by_key.get((canonical_source_project(document_code), item_id))
        if e is None or str(e.get("expert_decision") or "") != decision:
            return None
        log_version = version_no(e.get("current_version_id"))
        if log_version is None:
            return e if single_version else None
        return e if log_version == version_no(version_id) else None


# ═══════════════════════════════════════════════════════════════════════════
# Сборка журнала
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class Ledger:
    observations: list[dict] = field(default_factory=list)
    versions: list[dict] = field(default_factory=list)
    family_matches: list[dict] = field(default_factory=list)


def _decision_rows(review: Any) -> list[dict]:
    rows = review.get("decisions") if isinstance(review, dict) else None
    if not isinstance(rows, list):
        return []
    out = []
    for d in rows:
        if not isinstance(d, dict):
            continue
        item_id = str(d.get("item_id") or "")
        item_type = str(d.get("item_type") or "")
        if item_type == "optimization" or item_id.startswith("OPT"):
            continue
        out.append(d)
    return out


def build_ledger(
    objects_root: Path,
    discipline: str,
    log_index: LogIndex,
    *,
    hash_inputs: bool = True,
) -> Ledger:
    """Журнал наблюдений по всем версиям дисциплины с `expert_review.json`."""
    ledger = Ledger()
    by_family: dict[str, list[tuple[str, dict[str, dict]]]] = defaultdict(list)

    for review_path in sorted(objects_root.glob(f"*/disciplines/{discipline}/documents/*/versions/*/04_review/expert_review.json")):
        vdir = review_path.parent.parent
        version_id = vdir.name
        document_code = vdir.parent.parent.name
        object_folder = review_path.relative_to(objects_root).parts[0]
        family = family_key(object_folder, document_code)
        review = _load_json(review_path) or {}
        decisions = _decision_rows(review)
        if not decisions:
            continue
        sibling_versions = [p for p in (vdir.parent).iterdir() if p.is_dir()]
        variants = findings_variants(vdir)
        starts = run_starts(vdir)
        reviewed_at = parse_utc(review.get("reviewed_at"))
        preservation = (vdir / "04_review" / "verdict_preservation_report.json").is_file()
        renumbered = renumbered_since_backup(variants)
        run_after_review = bool(starts and reviewed_at and starts[-1] > reviewed_at)
        rerun_after_review = bool(preservation or renumbered or run_after_review)
        inputs = version_inputs(vdir, hash_inputs=hash_inputs)
        file_key = f"{object_folder}/{document_code}/{version_id}"
        batches = batch_units(decisions, file_key)
        file_reviewer = str(review.get("reviewer") or "")
        latest_items = next((v.items for v in variants if v.label.startswith("latest:")), {})

        ledger.versions.append({
            "path": str(vdir.relative_to(objects_root)),
            "object": object_folder, "document": document_code, "version": version_id,
            "family": family, "decisions": len(decisions), "reviewer_file": file_reviewer,
            "reviewed_at": review.get("reviewed_at"), "runs": len(starts),
            "last_run_start": starts[-1].isoformat() if starts else None,
            "rerun_after_review": rerun_after_review, "preservation_report": preservation,
            "renumbered_since_backup": renumbered, "run_after_review": run_after_review,
            "variants": [v.label for v in variants], "inputs": inputs,
        })

        decided_items: dict[str, dict] = {}
        for d in decisions:
            item_id = str(d.get("item_id") or "")
            value = str(d.get("decision") or "")
            log_entry = log_index.for_version(
                document_code, item_id, version_id,
                single_version=len(sibling_versions) == 1, decision=value,
            )
            link, link_label, item = link_decision(
                item_id, value, variants, log_entry, rerun_after_review=rerun_after_review,
            )
            provenance = classify_provenance(
                d, file_reviewer=file_reviewer, version_id=version_id,
                finding=item or latest_items.get(item_id),
            )
            reason = str(d.get("rejection_reason") or "")
            snapshot = finding_snapshot(item) if item else None
            obs_id = f"{file_key}/{item_id}"
            if item and value:
                decided_items[obs_id] = {**item, "id": obs_id}
            ledger.observations.append({
                "obs_id": obs_id, "object": object_folder, "discipline": discipline,
                "document": document_code, "family": family, "version": version_id,
                "item_id": item_id, "decision": value, "reason": reason,
                "reviewer_decision": str(d.get("reviewer") or ""), "reviewer_file": file_reviewer,
                "timestamp": d.get("timestamp"), "reviewed_at": review.get("reviewed_at"),
                "carried_from_version": d.get("carried_from_version"),
                "carried_from_item_id": d.get("carried_from_item_id"),
                "provenance": provenance, "flags": reason_flags(reason),
                "batch_unit": batches.get(item_id), "link": link, "link_source": link_label,
                "log_checked": log_entry is not None,
                "finding": snapshot, "origin": finding_origin(snapshot) if snapshot else None,
                "inputs": {k: inputs[k] for k in ("input_pdf_sha256", "work_md_sha256", "has_pdf", "blocks", "crops_present")},
                "rerun_after_review": rerun_after_review,
            })
        by_family[family].append((file_key, decided_items))

    _attach_dependency_groups(ledger, by_family)
    return ledger


def _attach_dependency_groups(ledger: Ledger, by_family: dict[str, list[tuple[str, dict[str, dict]]]]) -> None:
    """Группы зависимости: одна находка в разных версиях одного семейства."""
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[max(ra, rb)] = min(ra, rb)

    for family, versions in by_family.items():
        for i in range(len(versions)):
            for j in range(i + 1, len(versions)):
                (key_a, items_a), (key_b, items_b) = versions[i], versions[j]
                if not items_a or not items_b:
                    continue
                result = match_findings(items_a.values(), items_b.values())
                for link in result["links"]:
                    if link["status"] in (STATUS_EXACT, STATUS_FUZZY):
                        union(link["left"], link["right"])
                ledger.family_matches.append({
                    "family": family, "left": key_a, "right": key_b, "counts": result["counts"],
                })

    obs_by_key = {(o["object"], o["document"], o["version"], o["item_id"]): o for o in ledger.observations}
    for o in ledger.observations:
        src_version = o.get("carried_from_version")
        src_item = o.get("carried_from_item_id")
        if o["provenance"] == CARRIED and src_version and src_item:
            for (obj, doc, ver, iid), other in obs_by_key.items():
                if obj == o["object"] and doc == o["document"] and iid == src_item \
                        and version_no(ver) == version_no(src_version):
                    union(o["obs_id"], other["obs_id"])
    sizes: dict[str, int] = defaultdict(int)
    for o in ledger.observations:
        o["group_id"] = find(o["obs_id"])
        sizes[o["group_id"]] += 1
    for o in ledger.observations:
        o["group_size"] = sizes[o["group_id"]]
