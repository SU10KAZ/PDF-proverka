"""Frozen Stable Domain Keys v1: explicit semantic schemas, not runtime IDs.

Legacy serializers are intentionally independent of this module. Keys retain a
full digest; a registry rejects truncated-hash collisions before publication.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import unicodedata
from typing import Any, Mapping

VERSION = "v1"
QUESTION_CLASSES = frozenset({"SHEET_MATCHING", "ENTITY_MATCHING", "CHANGE_CONFIRMATION", "MISSING_DATA", "CONFLICT"})


def _nfc(value: Any) -> Any:
    if isinstance(value, str):
        return unicodedata.normalize("NFC", value)
    if isinstance(value, dict):
        out = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("canonical object keys must be strings")
            key = _nfc(key)
            if key in out:
                raise ValueError("duplicate NFC object key")
            out[key] = _nfc(item)
        return out
    if isinstance(value, (tuple, list)):
        return [_nfc(item) for item in value]
    return value


def canonical(value: Any) -> str:
    return json.dumps(_nfc(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def signature(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Key:
    key: str
    digest: str
    payload: str

    def fields(self) -> dict:
        return {"domain_key": self.key, "domain_key_version": VERSION, "domain_key_sha256": self.digest}


class CollisionError(ValueError):
    pass


class KeyRegistry:
    def __init__(self):
        self.payloads: dict[str, str] = {}

    def add(self, key: Key) -> Key:
        old = self.payloads.setdefault(key.key, key.payload)
        if old != key.payload:
            raise CollisionError(f"different canonical payloads for {key.key}")
        return key


def _key(namespace: str, payload: Mapping[str, Any]) -> Key:
    encoded = canonical({"domain_key_version": VERSION, "namespace": namespace, "payload": dict(payload)})
    digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    return Key(f"dk1_{namespace}_{digest[:32]}", digest, encoded)


def _set(values):
    return sorted(set(values))


def normalized_semantic_label(value: str) -> str:
    """Normalize proved labels without erasing punctuation or digits."""
    return " ".join(unicodedata.normalize("NFC", str(value or "")).casefold().split())


def document_version(document_id: str, version_id: str, content_set: list[dict]) -> Key:
    if not document_id or not version_id or not content_set:
        raise ValueError("exact document/version/content binding is required")
    content = []
    for item in content_set:
        clean = {k: item[k] for k in ("kind", "sha256", "size", "missing")}
        if not clean["missing"] and (not clean["sha256"] or clean["size"] is None):
            raise ValueError("declared content must have hash and size")
        content.append(clean)
    if len({item['kind'] for item in content}) != len(content):
        raise ValueError("duplicate content kind")
    return _key("document_version", {"document_id": document_id, "version_id": version_id, "content_set": sorted(content, key=lambda x: x['kind'])})


def sheet(document_version_key: str, page_ordinal: int) -> Key:
    if type(page_ordinal) is not int or page_ordinal < 1:
        raise ValueError("sheet page ordinal must be positive")
    return _key("sheet", {"document_version_key": document_version_key, "sheet_identity_version": "page-ordinal.v1", "page_ordinal": page_ordinal})


def sheet_relation(left: str, right: str, left_sheets: list[str], right_sheets: list[str], relation_type: str, relation_scope="DOCUMENT") -> Key:
    return _key("sheet_relation", {"left_document_version_key": left, "right_document_version_key": right, "relation_scope": relation_scope, "left_sheet_keys": _set(left_sheets), "right_sheet_keys": _set(right_sheets), "relation_type": relation_type})


def text_unit(document_version_key: str, source_kind: str, source: str, text: str, ordinal: int, semantic_owner_key=None) -> Key:
    return _key("text_unit", {"document_version_key": document_version_key, "semantic_owner_key": semantic_owner_key, "source_kind": source_kind, "source": source, "normalized_text": " ".join(text.split()), "within_document_occurrence_ordinal": ordinal})


def entity(document_version_key: str, entity_type: str, canonical_designation: str, semantic_owner_key=None, stable_disambiguator=None) -> Key:
    if not canonical_designation:
        raise ValueError("entity requires semantic designation")
    return _key("engineering_entity", {"document_version_key": document_version_key, "semantic_owner_key": semantic_owner_key, "entity_type": entity_type, "canonical_designation": canonical_designation, "stable_disambiguator": stable_disambiguator})


def unresolved_endpoint(
    document_version_key: str,
    *,
    endpoint_role: str,
    scope_kind: str,
    owner_kind: str,
    source_kind: str,
    sheet_key: str | None,
    container_label: str | None,
    subject_label: str,
    field_schema: list[str],
) -> Key:
    """Identify a proved semantic owner without asserting EngineeringEntity."""
    role = str(endpoint_role or "").strip().upper()
    scope = str(scope_kind or "").strip().upper()
    owner = str(owner_kind or "").strip().upper()
    source = str(source_kind or "").strip().casefold()
    container = normalized_semantic_label(container_label) if container_label else None
    subject = normalized_semantic_label(subject_label)
    fields = [normalized_semantic_label(value) for value in field_schema]
    if not document_version_key or not role or not scope or not owner or not source:
        raise ValueError("unresolved endpoint requires exact semantic type and scope")
    if not subject or not fields or any(not value for value in fields):
        raise ValueError("unresolved endpoint requires subject label and field schema")
    if scope == "DOCUMENT_SHARED":
        if sheet_key is not None:
            raise ValueError("document-shared endpoint must not be sheet-bound")
    elif not sheet_key:
        raise ValueError("local unresolved endpoint requires SheetKey")
    if scope == "TABLE_LOCAL" and (owner != "TABLE_ROW" or source != "table_row" or not container):
        raise ValueError("table-local endpoint requires proved table row and title")
    return _key("unresolved_endpoint", {
        "document_version_key": document_version_key,
        "endpoint_role": role,
        "semantic_scope": {
            "scope_kind": scope,
            "owner_kind": owner,
            "source_kind": source,
            "sheet_key": sheet_key,
            "normalized_container_label": container,
            "normalized_subject_label": subject,
            "normalized_field_schema": fields,
        },
    })


def entity_relation(left: str, right: str, left_entity_key: str, right_entity_key: str, relation_type: str, relation_scope="DOCUMENT") -> Key:
    return _key("entity_relation", {"left_document_version_key": left, "right_document_version_key": right, "left_entity_key": left_entity_key, "right_entity_key": right_entity_key, "relation_type": relation_type, "relation_scope": relation_scope})


def typed(value: Any, *, missing=False, unit=None) -> dict:
    if missing:
        return {"type": "missing"}
    kind = {str: "string", bool: "boolean", int: "integer", float: "number", type(None): "null"}.get(type(value))
    if kind is None:
        raise ValueError("producer must explicitly type compound values")
    result = {"type": kind, "value": value}
    if unit is not None:
        result["unit"] = {"type": "string", "value": unit}
    return result


@dataclass(frozen=True)
class Claim:
    left: str
    right: str
    owner: str | None
    facet: str | None
    dimension: str
    direction: str
    change_type: str
    before: dict
    after: dict
    scope: str = "DOCUMENT"

    def payload(self) -> dict:
        return {"left_document_version_key": self.left, "right_document_version_key": self.right, "semantic_owner_key": self.owner, "relation_scope": self.scope, "facet_ref": self.facet, "dimension": self.dimension, "direction": self.direction, "change_type": self.change_type, "before_value": self.before, "after_value": self.after}


def text_fact(claim: Claim) -> Key:
    return _key("text_fact", claim.payload())


def text_atom(claim: Claim, text_unit_keys=()) -> Key:
    payload = claim.payload()
    if text_unit_keys:
        payload["text_unit_keys"] = _set(text_unit_keys)
    return _key("text_atom", payload)


def atomic_change(claim: Claim, *, field: str | None) -> Key:
    return _key("atomic_change", {**claim.payload(), "field": field})


def atomic_review(claim: Claim, *, text_unit_keys, reason_family, field=None, resolved_change: Key | None = None) -> Key:
    if resolved_change is not None:
        return resolved_change
    if not text_unit_keys:
        raise ValueError("unresolved review requires content-bound text units")
    return _key("atomic_review_item", {"left_document_version_key": claim.left, "right_document_version_key": claim.right, "text_unit_keys": _set(text_unit_keys), "semantic_owner_key": claim.owner, "dimension": claim.dimension, "field": field, "before_value": claim.before, "after_value": claim.after, "direction": claim.direction, "reason_family": _set(reason_family)})


def question(left: str, right: str, question_class: str, target_domain_keys, candidate_endpoint_keys, decision_scope="ATOMIC") -> Key:
    if question_class not in QUESTION_CLASSES:
        raise ValueError("unsupported question class")
    if not target_domain_keys and not candidate_endpoint_keys:
        raise ValueError("question requires a semantic target")
    return _key("engineer_question", {"left_document_version_key": left, "right_document_version_key": right, "question_class": question_class, "target_domain_keys": _set(target_domain_keys), "candidate_endpoint_keys": _set(candidate_endpoint_keys), "decision_scope": decision_scope})


def evidence(document_version_key, producer_key, source_artifact_content_hash, *, text_unit_key=None, sheet_key=None, span=None, bbox=(), claim_role="supports") -> Key:
    return _key("evidence_provenance", {"document_version_key": document_version_key, "producer_key": producer_key, "source_artifact_content_hash": source_artifact_content_hash, "text_unit_key": text_unit_key, "sheet_key": sheet_key, "span": span, "bbox": list(bbox), "claim_role": claim_role})


def run_instance(*, session_id, pair_id, run_id) -> Key:
    return _key("run_instance", {"session_id": session_id, "pair_id": pair_id, "run_id": run_id})


def input_signature(left: str, right: str, manual_mapping_keys=(), semantic_scope="DOCUMENT") -> str:
    return signature({"signature_schema": "input-content.v1", "left_document_version_key": left, "right_document_version_key": right, "manual_mapping_domain_keys": _set(manual_mapping_keys), "semantic_scope": semantic_scope})


def algorithm_signature(producers: list[dict], semantic_config: dict) -> str:
    return signature({"signature_schema": "algorithm.v1", "producers": sorted(producers, key=lambda p: p["producer_name"]), "semantic_config": semantic_config})


def result_signature(rows: list[dict]) -> str:
    return signature({"signature_schema": "result.v1", "results": sorted(rows, key=canonical)})
