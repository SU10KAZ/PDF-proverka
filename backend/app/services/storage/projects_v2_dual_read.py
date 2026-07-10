"""
projects_v2_dual_read.py — READ-ONLY dual-read/parity слой для подготовки к
будущему cutover на `projects_v2`.

Для выбранного документа собирает legacy-snapshot (из `projects/` через
`old_to_new_map`) и v2-snapshot (через `ProjectsV2Adapter`), сравнивает поля,
важные для UI/API, и возвращает статус каждого поля + документа:

  match | expected_difference | mismatch | missing_legacy | missing_v2

ЖЁСТКИЕ ГАРАНТИИ:
  * только чтение: ничего не пишет ни в `projects/`, ни в `projects_v2`;
  * не запускает pipeline/анализ;
  * НЕ делает fallback из v2 в legacy (читает обе стороны независимо);
  * default production read-path не меняется (этот модуль никем из production
    endpoints не вызывается).

Backend-самодостаточен: legacy читается напрямую (read-only «сверка»), v2 — через
adapter. Аналогичная логика есть в scripts/check_ui_contract_parity.py для
оффлайн-прогона всего корпуса; здесь — per-document сервис для backend/canary.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Optional

from backend.app.services.storage.projects_v2_adapter import (
    ProjectsV2Adapter, _FINDINGS_PRIORITY, get_storage_backend,
)

MATCH = "match"
EXPECTED = "expected_difference"
MISMATCH = "mismatch"
MISSING_LEGACY = "missing_legacy"
MISSING_V2 = "missing_v2"
# v2-only документ (создан живым backend после cutover) — legacy-снимка нет,
# сравнивать не с чем. Не потеря: findings/версии живут в v2.
V2_ONLY = "v2_only"
_RANK = {MATCH: 0, EXPECTED: 1, MISSING_LEGACY: 2, MISSING_V2: 3, MISMATCH: 4}

_CRIT = ("01_text_analysis.json", "02_blocks_analysis.json", "03_findings.json")
_LEGACY_PRESERVE_STATUSES = {"legacy_partial", "source_only"}


def _read_json(p: Path) -> Optional[dict]:
    try:
        return json.loads(Path(p).read_text(encoding="utf-8"))
    except Exception:
        return None


def _findings_count_in_dir(d: Optional[Path]) -> int:
    if not d or not Path(d).is_dir():
        return 0
    for n in _FINDINGS_PRIORITY:
        p = Path(d) / n
        if p.is_file():
            data = _read_json(p) or {}
            if isinstance(data, list):
                return len(data)
            return len(data.get("findings", data.get("items", [])) or [])
    return 0


def _severity_in_dir(d: Optional[Path]) -> dict:
    if not d or not Path(d).is_dir():
        return {}
    for n in _FINDINGS_PRIORITY:
        p = Path(d) / n
        if p.is_file():
            data = _read_json(p) or {}
            items = data if isinstance(data, list) else data.get("findings", data.get("items", []))
            out: dict = {}
            for f in items or []:
                if isinstance(f, dict):
                    sev = str(f.get("severity") or f.get("category") or "unknown")
                    out[sev] = out.get(sev, 0) + 1
            return out
    return {}


def _legacy_output_with_findings(folder: Path) -> Optional[Path]:
    folder = Path(folder)
    if not folder.is_dir():
        return None
    outs = [d for d in folder.rglob("_output") if d.is_dir()]
    for d in sorted(outs):
        if any((d / n).is_file() for n in _FINDINGS_PRIORITY):
            return d
    return sorted(outs)[0] if outs else None


def _derive_legacy_status(legacy_out: Optional[Path]) -> str:
    if legacy_out is None:
        return "none"
    n = sum(1 for c in _CRIT if (Path(legacy_out) / c).is_file())
    return "complete" if n == 3 else ("partial" if n else "none")


def _legacy_version_count(legacy_project_path: Optional[str]) -> Optional[int]:
    if not legacy_project_path:
        return None
    p = Path(legacy_project_path)
    if p.name.endswith("(main)"):
        vg = _read_json(p / "version_group.json") or {}
        if isinstance(vg.get("versions"), list):
            return len(vg["versions"])
    return 1


def _legacy_current_version_no(legacy_project_path: Optional[str]) -> Optional[int]:
    if not legacy_project_path:
        return None
    p = Path(legacy_project_path)
    if p.name.endswith("(main)"):
        vg = _read_json(p / "version_group.json") or {}
        m = re.match(r"v(\d+)$", str(vg.get("latest_version_id") or "").strip())
        if m:
            return int(m.group(1))
        return len(vg.get("versions", []) or []) or 1
    return 1


def _cmp(name, legacy, v2, *, expected=False, soft=False, na=False) -> dict:
    if na:
        status = MATCH
    elif legacy == v2:
        status = MATCH
    elif expected:
        status = EXPECTED
    elif v2 is None and legacy is not None:
        status = MISSING_V2
    elif legacy is None and v2 is not None:
        status = MISSING_LEGACY
    else:
        status = MISMATCH
    return {"field": name, "legacy": legacy, "v2": v2, "status": status, "soft": soft}


class DualReadService:
    """Read-only per-document dual-read сравнение legacy ↔ projects_v2."""

    def __init__(self, v2_root: Optional[Path] = None):
        self.adapter = ProjectsV2Adapter(v2_root)
        self._migrations_cache: Optional[list] = None
        self._decisions_cache: Optional[list] = None

    # -- shared read-only sources --
    def _migrations(self) -> list:
        if self._migrations_cache is None:
            m = _read_json(self.adapter.v2_root / "_system" / "old_to_new_map.json") or {}
            self._migrations_cache = m.get("migrations", [])
        return self._migrations_cache

    def _decisions(self) -> list:
        if self._decisions_cache is None:
            kb = self.adapter.v2_root.parent / "knowledge_base" / "decisions_log.json"
            self._decisions_cache = (_read_json(kb) or {}).get("entries", []) if kb.exists() else []
        return self._decisions_cache

    def _map_for(self, object_id, document_code) -> dict:
        out = {}
        for r in self._migrations():
            if r.get("object_id") == object_id and r.get("document_code") == document_code:
                out[r.get("version_id")] = r
        return out

    def _doc_type(self, snap: dict) -> str:
        # v2-only документы (созданы живым backend после cutover) не имеют
        # legacy-снимка — snap=None. Дуал-read не рассчитан на них: graceful
        # "unknown" вместо AttributeError.
        if not snap:
            return "unknown"
        if snap.get("migration_kind") == "legacy_findings_preserve":
            return "king_sons_legacy_preserve"
        if snap.get("version_count", 0) > 1:
            return "versioned"
        cur = snap.get("current_version")
        for v in snap.get("versions", []):
            if v["version_id"] == cur:
                return v.get("analysis_status") or "none"
        return "unknown"

    # -- core --
    def compare_document(self, document_code: str,
                         object_id: Optional[str] = None) -> dict:
        doc = self.adapter.find_document(document_code, object_id=object_id)
        if doc is None:
            return {"document_code": document_code, "status": MISSING_V2,
                    "fields": [], "note": "document not present in projects_v2",
                    "findings_loss": False, "version_loss": False}
        doc_dir = Path(doc["doc_dir"])
        snap = self.adapter.document_snapshot(doc["object_folder"], doc["discipline"],
                                              doc["document_code"])
        if snap is None:
            # v2-only документ (создан после cutover) — legacy-снимка нет,
            # сравнивать не с чем. Не потеря данных.
            return {"document_code": document_code, "status": V2_ONLY,
                    "fields": [], "note": "v2-only document (no legacy snapshot)",
                    "findings_loss": False, "version_loss": False}
        dtype = self._doc_type(snap)
        dj = self.adapter.read_document_json(doc_dir) or {}
        is_kingsons = snap.get("migration_kind") == "legacy_findings_preserve"
        cur = snap["current_version"]
        cur_meta = self.adapter.version_metadata(doc_dir, cur)
        recs = self._map_for(snap["object_id"], snap["document_code"])
        cur_rec = recs.get(cur)
        lp = dj.get("legacy_project_path") or (cur_rec or {}).get("legacy_folder_path")

        # legacy object/discipline из legacy_project_path: <...>/projects/<object>/<disc>/<code...>
        legacy_object = legacy_disc = None
        if lp:
            parts = Path(lp).resolve().parts
            if "projects" in parts:
                i = len(parts) - 1 - parts[::-1].index("projects")
                if i + 1 < len(parts):
                    legacy_object = parts[i + 1]
                if i + 2 < len(parts):
                    legacy_disc = parts[i + 2]

        v2_obj = next((o for o in self.adapter.list_objects()
                       if o["folder_name"] == snap["object_folder"]), {})

        legacy_out = _legacy_output_with_findings(Path(cur_rec["legacy_folder_path"])) \
            if cur_rec and cur_rec.get("legacy_folder_path") else None
        v2_latest = self.adapter.latest_dir(doc_dir, cur)

        v2_status = cur_meta.get("analysis_status")
        legacy_status = _derive_legacy_status(legacy_out)
        status_expected = v2_status in _LEGACY_PRESERVE_STATUSES

        v2_has = {n: (v2_latest / n).is_file() for n in _CRIT}
        legacy_has = {n: bool(legacy_out) and (Path(legacy_out) / n).is_file() for n in _CRIT}

        v2_fc = _findings_count_in_dir(v2_latest)
        legacy_fc = _findings_count_in_dir(legacy_out)

        v2_plog = self.adapter.pipeline_log_path(doc_dir, cur)
        legacy_plog = (Path(legacy_out) / "pipeline_log.json") if legacy_out and \
            (Path(legacy_out) / "pipeline_log.json").is_file() else None

        fields = [
            _cmp("object", legacy_object, v2_obj.get("display_name")),
            _cmp("discipline", legacy_disc, snap["discipline"]),
            _cmp("document_code",
                 Path(lp).name.replace("(main)", "").strip() if lp else None,
                 None, na=True),  # информативно; код совпадает по построению
            _cmp("version_count", _legacy_version_count(lp), snap["version_count"],
                 expected=is_kingsons),
            _cmp("current_version_no", _legacy_current_version_no(lp),
                 cur_meta.get("version_no"), expected=is_kingsons),
            _cmp("analysis_status", legacy_status, v2_status, expected=status_expected),
            _cmp("has_01_text_analysis", legacy_has[_CRIT[0]], v2_has[_CRIT[0]]),
            _cmp("has_02_blocks_analysis", legacy_has[_CRIT[1]], v2_has[_CRIT[1]]),
            _cmp("has_blocks_analysis", legacy_has[_CRIT[1]], v2_has[_CRIT[1]]),
            _cmp("has_03_findings", legacy_has[_CRIT[2]], v2_has[_CRIT[2]]),
            _cmp("findings_count", legacy_fc, v2_fc),
            _cmp("severity_counts", _severity_in_dir(legacy_out),
                 _severity_in_dir(v2_latest), soft=True),
            _cmp("pipeline_log_present", legacy_plog is not None, v2_plog is not None),
        ]
        if is_kingsons:
            legacy_kb = sum(1 for e in self._decisions()
                            if str(e.get("source_project") or "") == snap["document_code"])
            kb_link = doc_dir / "versions" / str(cur) / "04_review" / "kb_decisions_link.json"
            v2_kb = (_read_json(kb_link) or {}).get("entry_count", 0) if kb_link.is_file() else 0
            fields.append(_cmp("kb_link_entry_count", legacy_kb, v2_kb))

        hard = [f for f in fields if not f["soft"]]
        doc_status = MATCH
        for f in hard:
            if _RANK[f["status"]] > _RANK[doc_status]:
                doc_status = f["status"]

        ff = next(f for f in fields if f["field"] == "findings_count")
        vf = next(f for f in fields if f["field"] == "version_count")
        return {
            "document_code": snap["document_code"],
            "object_folder": snap["object_folder"],
            "discipline": snap["discipline"],
            "type": dtype,
            "is_kingsons_preserve": is_kingsons,
            "status": doc_status,
            "fields": fields,
            "findings_loss": ff["status"] == MISMATCH and v2_fc < legacy_fc,
            "findings_legacy": legacy_fc, "findings_v2": v2_fc,
            "version_loss": vf["status"] == MISMATCH,
            "version_legacy": vf["legacy"], "version_v2": vf["v2"],
        }

    def _select_sample(self, per_type: int) -> list[dict]:
        buckets: dict[str, int] = {}
        out = []
        for d in self.adapter.list_documents():
            snap = self.adapter.document_snapshot(d["object_folder"], d["discipline"],
                                                  d["document_code"])
            t = self._doc_type(snap)
            cap = max(per_type, 3) if t == "king_sons_legacy_preserve" else per_type
            if buckets.get(t, 0) < cap:
                out.append(d)
                buckets[t] = buckets.get(t, 0) + 1
        return out

    def sample(self, per_type: int = 2) -> dict:
        docs = self._select_sample(per_type)
        results = [self.compare_document(d["document_code"], object_id=d["object_id"])
                   for d in docs]
        counts: dict[str, int] = {}
        for r in results:
            counts[r["status"]] = counts.get(r["status"], 0) + 1
        mism = [r["document_code"] for r in results if r["status"] == MISMATCH]
        return {
            "documents_checked": len(results),
            "status_counts": counts,
            "mismatches": mism,
            "findings_losses": [r["document_code"] for r in results if r["findings_loss"]],
            "version_losses": [r["document_code"] for r in results if r["version_loss"]],
            "ok": not mism,
            "results": results,
        }


# ---------------------------------------------------------------------------
# cutover readiness (read-only; assembles last runtime reports + dual-read)
# ---------------------------------------------------------------------------

REC_NOT_READY = "not_ready"
REC_SHADOW_PROD = "ready_for_shadow_prod"
REC_CANARY = "ready_for_read_only_canary"


def cutover_readiness(adapter: ProjectsV2Adapter, *,
                      validate_status: Optional[str] = None,
                      per_type: int = 2) -> dict:
    """Собирает статус готовности к cutover из последних runtime-отчётов +
    свежей dual-read выборки. READ-ONLY (ничего не пишет)."""
    sysd = adapter.v2_root / "_system"

    drift = (_read_json(sysd / "migrated_drift_scan_report.json") or {}).get("summary", {})
    # предпочитаем полнокорпусный отчёт (если есть) — он даёт full_corpus=True
    contract = (_read_json(sysd / "full_corpus_parity_report.json")
                or _read_json(sysd / "ui_contract_parity_report.json") or {})
    bparity = _read_json(sysd / "backend_parity_report.json") or {}

    drift_docs = drift.get("drift_documents")
    drift_unstable = drift.get("unstable")
    drift_ok = (drift_docs == 0 and (drift_unstable or 0) == 0) if drift else None

    contract_mismatch = (contract.get("doc_status_counts", {}) or {}).get("MISMATCH")
    contract_ok = contract.get("contract_ok")
    contract_checked = contract.get("documents_checked")

    bparity_ok = bparity.get("parity_ok")
    bparity_noloss = bparity.get("findings_no_loss_overall")

    # validate: если не передан — пытаемся прочитать из cutover-отчёта (если был),
    # иначе unknown (endpoint не запускает subprocess в реквесте).
    if validate_status is None:
        prev = _read_json(sysd / "cutover_readiness_report.json") or {}
        validate_status = (prev.get("validate") or {}).get("status")
    validate_ok = (validate_status == "PASS") if validate_status else None

    dr = DualReadService(adapter.v2_root).sample(per_type=per_type)

    v2_docs = len(adapter.list_documents())
    total_mismatches = (len(dr["mismatches"])
                        + (contract_mismatch or 0)
                        + (0 if bparity_ok in (True, None) else 1))

    hard_fail = (
        validate_ok is False
        or drift_ok is False
        or (contract_ok is False)
        or (bparity_ok is False)
        or (not dr["ok"])
        or bool(dr["findings_losses"]) or bool(dr["version_losses"])
    )
    # полное покрытие contract parity (на всех документах) → сильнее, чем sample
    full_contract = bool(contract_checked and v2_docs and contract_checked >= v2_docs)

    if hard_fail or validate_ok is None or drift_ok is None:
        rec = REC_NOT_READY
    elif full_contract and dr["ok"] and bparity_noloss in (True, None):
        rec = REC_CANARY
    else:
        rec = REC_SHADOW_PROD

    return {
        "generated_at_source": "runtime_reports+live_dual_read",
        "storage_backend_default": get_storage_backend(),
        "v2_documents": v2_docs,
        "validate": {"status": validate_status, "ok": validate_ok},
        "drift": {"drift_documents": drift_docs, "unstable": drift_unstable, "ok": drift_ok},
        "ui_contract_parity": {"ok": contract_ok, "documents_checked": contract_checked,
                               "mismatch": contract_mismatch, "full_corpus": full_contract},
        "backend_parity": {"ok": bparity_ok, "findings_no_loss": bparity_noloss},
        "dual_read_sample": {"ok": dr["ok"], "documents_checked": dr["documents_checked"],
                             "status_counts": dr["status_counts"],
                             "mismatches": dr["mismatches"],
                             "findings_losses": dr["findings_losses"],
                             "version_losses": dr["version_losses"]},
        "total_mismatches": total_mismatches,
        "recommendation": rec,
    }
