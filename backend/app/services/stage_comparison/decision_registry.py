"""Append-only authority ledger for Stable Domain Keys v1.

The registry is deliberately independent from a comparison session. Runtime
identifiers are retained only inside ``source_instance`` provenance and never
participate in lookup. Generated PENDING rows and legacy links are not
imported as human authority.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import uuid


AUTHORITY = {"DETERMINISTIC_SUGGESTION": 0, "AI": 1, "HUMAN": 2}
STATES = {"ACTIVE", "STALE", "SUPERSEDED", "LOCKED"}
VALIDATION_STATES = {"VALID", "REQUIRES_REVALIDATION"}


class RegistryConflict(ValueError):
    """The requested append no longer describes the current registry."""


class DecisionRegistry:
    """Small transactional SQLite implementation of DecisionRegistry v1."""

    def __init__(self, path: str | Path):
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        self.path = str(target)
        with self._connect() as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS events ("
                "revision INTEGER PRIMARY KEY AUTOINCREMENT, "
                "decision_id TEXT UNIQUE NOT NULL, "
                "domain_key TEXT NOT NULL, scope TEXT NOT NULL, "
                "input_signature TEXT NOT NULL, authority TEXT NOT NULL, "
                "state TEXT NOT NULL, supersedes_decision_id TEXT, "
                "payload TEXT NOT NULL)"
            )
            columns = {
                str(row[1]) for row in db.execute("PRAGMA table_info(events)")
            }
            for name in (
                "input_signature",
                "authority",
                "state",
                "supersedes_decision_id",
            ):
                if name not in columns:
                    db.execute(f"ALTER TABLE events ADD COLUMN {name} TEXT")
            # M1 test/preview databases used a payload-only projection. Keep
            # every immutable payload and backfill only the new index columns.
            for revision, payload in db.execute(
                "SELECT revision,payload FROM events WHERE input_signature IS NULL "
                "OR authority IS NULL OR state IS NULL"
            ):
                row = json.loads(payload)
                db.execute(
                    "UPDATE events SET input_signature=?,authority=?,state=?,"
                    "supersedes_decision_id=? WHERE revision=?",
                    (
                        row.get("based_on_input_content_signature"),
                        row.get("authority"),
                        row.get("state", "ACTIVE"),
                        row.get("supersedes_decision_id"),
                        revision,
                    ),
                )
            db.execute(
                "CREATE INDEX IF NOT EXISTS target "
                "ON events(domain_key, scope, revision)"
            )
            db.execute(
                "CREATE INDEX IF NOT EXISTS input_signature "
                "ON events(input_signature, revision)"
            )
            db.execute(
                "CREATE INDEX IF NOT EXISTS supersession "
                "ON events(supersedes_decision_id)"
            )

    def _connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.execute("PRAGMA busy_timeout=10000")
        return db

    @staticmethod
    def _decode(rows: Iterable[tuple[str]]) -> list[dict]:
        return [json.loads(row[0]) for row in rows]

    def raw_history(self, domain_key: str, scope: str) -> list[dict]:
        with self._connect() as db:
            return self._decode(
                db.execute(
                    "SELECT payload FROM events WHERE domain_key=? AND scope=? "
                    "ORDER BY revision",
                    (domain_key, scope),
                )
            )

    def raw_histories(
        self, domain_keys: Iterable[str], scope: str
    ) -> dict[str, list[dict]]:
        """Load all requested histories with one indexed query per 500 keys."""
        keys = sorted({str(value) for value in domain_keys if str(value)})
        output = {key: [] for key in keys}
        with self._connect() as db:
            for start in range(0, len(keys), 500):
                chunk = keys[start : start + 500]
                placeholders = ",".join("?" for _ in chunk)
                rows = db.execute(
                    "SELECT domain_key,payload FROM events WHERE scope=? "
                    f"AND domain_key IN ({placeholders}) ORDER BY revision",
                    [scope, *chunk],
                )
                for domain_key, payload in rows:
                    output[str(domain_key)].append(json.loads(payload))
        return output

    @staticmethod
    def _effective_rows(rows: Iterable[Mapping]) -> list[dict]:
        rows = [dict(row) for row in rows]
        superseded_ids = {
            str(row.get("supersedes_decision_id"))
            for row in rows
            if row.get("supersedes_decision_id")
        }
        return [
            {
                **row,
                "state": (
                    "SUPERSEDED"
                    if row.get("decision_id") in superseded_ids
                    else row.get("state", "ACTIVE")
                ),
                "stored_state": row.get("state", "ACTIVE"),
            }
            for row in rows
        ]

    def history(self, domain_key: str, scope: str) -> list[dict]:
        """Return stored immutable events (the original M1 API contract)."""
        return self.raw_history(domain_key, scope)

    def effective_history(self, domain_key: str, scope: str) -> list[dict]:
        """Return events with a derived SUPERSEDED projection."""
        return self._effective_rows(self.raw_history(domain_key, scope))

    def effective_histories(
        self, domain_keys: Iterable[str], scope: str
    ) -> dict[str, list[dict]]:
        return {
            key: self._effective_rows(rows)
            for key, rows in self.raw_histories(domain_keys, scope).items()
        }

    def revision(self) -> int:
        with self._connect() as db:
            return int(
                db.execute("SELECT COALESCE(MAX(revision),0) FROM events").fetchone()[0]
            )

    @staticmethod
    def _validate_record(record: Mapping, current_input_signature: str) -> dict:
        row = dict(record)
        required = (
            "domain_key",
            "domain_key_version",
            "target_kind",
            "decision_scope",
            "decision",
            "authority",
            "based_on_input_content_signature",
            "based_on_algorithm_signature",
        )
        if (
            any(not row.get(key) for key in required)
            or row["domain_key_version"] != "v1"
            or not row["domain_key"].startswith("dk1_")
        ):
            raise ValueError("complete version-bound registry record required")
        if row["authority"] not in AUTHORITY or row["decision"] == "PENDING_REVIEW":
            raise ValueError("generated or unknown authority cannot create a decision")
        if row["authority"] == "HUMAN" and not row.get("author"):
            raise ValueError("named human author required")
        if row.get("state", "ACTIVE") not in STATES:
            raise ValueError("invalid lifecycle state")
        if row.get("validation_state", "VALID") not in VALIDATION_STATES:
            raise ValueError("invalid validation state")
        if row["based_on_input_content_signature"] != current_input_signature:
            raise RegistryConflict("input changed")
        return row

    def append_many(
        self,
        records: Iterable[Mapping],
        *,
        expected_revision: int,
        current_input_signature: str,
        authorized_human_supersession: bool = False,
    ) -> list[dict]:
        """Append one atomic batch after optimistic-concurrency validation."""
        rows = [
            self._validate_record(record, current_input_signature)
            for record in records
        ]
        if not rows:
            return []
        targets = [(row["domain_key"], row["decision_scope"]) for row in rows]
        if len(targets) != len(set(targets)):
            raise ValueError("duplicate domain/scope update in one transaction")

        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            revision = int(
                db.execute("SELECT COALESCE(MAX(revision),0) FROM events").fetchone()[0]
            )
            if revision != expected_revision:
                raise RegistryConflict("registry revision changed")
            now = datetime.now(timezone.utc).isoformat()
            output: list[dict] = []
            for row in rows:
                prior = db.execute(
                    "SELECT payload FROM events WHERE domain_key=? AND scope=? "
                    "ORDER BY revision DESC LIMIT 1",
                    (row["domain_key"], row["decision_scope"]),
                ).fetchone()
                if prior:
                    previous = json.loads(prior[0])
                    if row.get("supersedes_decision_id") != previous["decision_id"]:
                        raise RegistryConflict("explicit supersession link required")
                    if AUTHORITY[row["authority"]] < AUTHORITY[previous["authority"]]:
                        raise RegistryConflict(
                            "lower authority cannot replace higher authority"
                        )
                    if previous["state"] == "LOCKED" and not (
                        row["authority"] == "HUMAN"
                        and authorized_human_supersession
                    ):
                        raise RegistryConflict(
                            "locked decision requires authorized human supersession"
                        )
                elif row.get("supersedes_decision_id"):
                    raise RegistryConflict("unknown supersession target")

                row.update(
                    decision_id="dec_" + uuid.uuid4().hex,
                    registry_schema_version="decision-registry.v1",
                    created_at=now,
                    state=row.get("state", "ACTIVE"),
                    validation_state=row.get("validation_state", "VALID"),
                )
                db.execute(
                    "INSERT INTO events("
                    "decision_id,domain_key,scope,input_signature,authority,"
                    "state,supersedes_decision_id,payload"
                    ") VALUES (?,?,?,?,?,?,?,?)",
                    (
                        row["decision_id"],
                        row["domain_key"],
                        row["decision_scope"],
                        row["based_on_input_content_signature"],
                        row["authority"],
                        row["state"],
                        row.get("supersedes_decision_id"),
                        json.dumps(row, ensure_ascii=False, allow_nan=False),
                    ),
                )
                output.append(dict(row))
        return output

    def append(
        self,
        record: Mapping,
        *,
        expected_revision: int,
        current_input_signature: str,
        authorized_human_supersession: bool = False,
    ) -> dict:
        return self.append_many(
            [record],
            expected_revision=expected_revision,
            current_input_signature=current_input_signature,
            authorized_human_supersession=authorized_human_supersession,
        )[0]

    def resolve(
        self,
        domain_key: str,
        scope: str,
        input_signature: str,
        algorithm_signature: str,
        *,
        compatibility_policy: dict | None = None,
    ) -> dict:
        history = self.effective_history(domain_key, scope)
        return self.resolve_history(
            history,
            input_signature,
            algorithm_signature,
            compatibility_policy=compatibility_policy,
        )

    @staticmethod
    def resolve_history(
        history: Iterable[Mapping],
        input_signature: str,
        algorithm_signature: str,
        *,
        compatibility_policy: dict | None = None,
    ) -> dict:
        history = [dict(row) for row in history]
        if not history:
            return {
                "decision": None,
                "state": "UNAVAILABLE",
                "validation_state": "VALID",
                "reason": "no exact domain/scope decision",
            }
        row = history[-1]
        lifecycle = row.get("stored_state", row.get("state"))
        if lifecycle not in {"ACTIVE", "LOCKED"}:
            return {"decision": None, "state": lifecycle, "record": row}
        if row["based_on_input_content_signature"] != input_signature:
            return {
                "decision": None,
                "state": "STALE",
                "lifecycle_state": lifecycle,
                "validation_state": row.get("validation_state", "VALID"),
                "record": row,
            }
        validation = row.get("validation_state", "VALID")
        if row["based_on_algorithm_signature"] != algorithm_signature:
            policy = compatibility_policy or {}
            compatible = (
                policy.get("from_algorithm_signature")
                == row["based_on_algorithm_signature"]
                and policy.get("to_algorithm_signature") == algorithm_signature
                and policy.get("change_class") in {"ID_ONLY", "EVIDENCE_ONLY"}
                and policy.get("decision_policy")
                in {"REUSE", "REUSE_WITH_EVIDENCE_REVISION"}
                and bool(policy.get("approved_by"))
            )
            if not compatible:
                validation = "REQUIRES_REVALIDATION"
        if validation == "REQUIRES_REVALIDATION":
            return {
                "decision": None,
                "state": "REQUIRES_REVALIDATION",
                "lifecycle_state": lifecycle,
                "validation_state": validation,
                "record": row,
            }
        return {
            "decision": row["decision"],
            "state": lifecycle,
            "validation_state": "VALID",
            "record": row,
        }


def legacy_alias_policy(
    row: dict,
    *,
    exact_document_versions: bool = False,
    unique_domain_key: str | None = None,
    named_author: str | None = None,
) -> dict:
    """Produce a reviewable migration proposal, never import or assert truth."""
    eligible = row.get("source") == "manual" and "user_corrected" in (
        row.get("reason") or []
    )
    bound = eligible and exact_document_versions and unique_domain_key and named_author
    return {
        "legacy_record": row,
        "domain_key": unique_domain_key if bound else None,
        "migration_state": (
            "REQUIRES_EXPLICIT_IMPORT" if bound else "REQUIRES_REVALIDATION"
        ),
        "authority": None,
        "historical_rewrite": False,
    }
