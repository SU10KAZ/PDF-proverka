"""Durable public-certificate registry and central security audit trail.

The registry intentionally has no signing ability and never accepts private
key bytes.  Gateway runtime code depends on this module, while the separate
issuer boundary lives in :mod:`backend.app.security.certificate_issuer`.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any

from backend.app.services.distributed_workers import database
from backend.app.services.distributed_workers.settings import (
    DistributedWorkersSettings,
    get_settings,
)


class CertificateStatus(str, Enum):
    ACTIVE = "ACTIVE"
    REPLACED = "REPLACED"
    REVOKED = "REVOKED"
    EXPIRED = "EXPIRED"


class RevocationReason(str, Enum):
    COMPROMISED = "COMPROMISED"
    DECOMMISSIONED = "DECOMMISSIONED"
    REPLACED = "REPLACED"
    ADMIN_REVOKED = "ADMIN_REVOKED"


class CertificateRegistryError(RuntimeError):
    pass


@dataclass(frozen=True)
class PresentedCertificate:
    serial_hex: str
    fingerprint_sha256: str
    worker_id: str
    not_before: float
    not_after: float
    issuer_id: str
    certificate_pem: bytes


def _row(row: Any) -> dict[str, Any] | None:
    return dict(row) if row is not None else None


class CertificateRegistry:
    def __init__(self, settings: DistributedWorkersSettings | None = None) -> None:
        self.settings = settings or get_settings()

    def by_serial(self, serial_hex: str) -> dict[str, Any] | None:
        with database.read_conn(self.settings) as conn:
            return _row(conn.execute(
                "SELECT * FROM worker_certificates WHERE serial_hex = ?",
                (serial_hex.lower(),),
            ).fetchone())

    def active_for_worker(self, worker_id: str) -> dict[str, Any] | None:
        """Действующий сертификат воркера — только чтение.

        Нужен экрану диагностики. До этого он показывал `mtls: "unavailable"`
        жёстко зашитой строкой с комментарием «факт проверки сертификата не
        сохраняется». Комментарий не соответствовал устройству: реестр хранит
        и серийный номер, и отпечаток, и срок действия, а статус ACTIVE
        выставляется при выдаче. Показывать «неизвестно» там, где сведения
        есть, — это выдуманное значение с обратным знаком.
        """
        with database.read_conn(self.settings) as conn:
            return _row(conn.execute(
                "SELECT * FROM worker_certificates "
                "WHERE worker_id = ? AND status = 'ACTIVE' "
                "ORDER BY not_after DESC LIMIT 1",
                (worker_id,),
            ).fetchone())

    def by_request(self, request_id: str) -> dict[str, Any] | None:
        with database.read_conn(self.settings) as conn:
            return _row(conn.execute(
                "SELECT * FROM worker_certificates WHERE request_id = ?",
                (request_id,),
            ).fetchone())

    def record_issuance(
        self,
        cert: PresentedCertificate,
        *,
        csr_sha256: str,
        request_id: str,
        instance_id: str | None = None,
        profile_version: int = 1,
        predecessor_serial: str | None = None,
        renewed: bool = False,
    ) -> dict[str, Any]:
        """Persist once; identical request retries return the original leaf."""
        now = time.time()
        with database.write_txn(self.settings) as conn:
            existing = conn.execute(
                "SELECT * FROM worker_certificates WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if existing is not None:
                existing = dict(existing)
                if (
                    existing["worker_id"] != cert.worker_id
                    or existing["csr_sha256"] != csr_sha256
                ):
                    raise CertificateRegistryError(
                        "certificate request_id was reused with different identity or CSR"
                    )
                return existing
            conn.execute(
                """INSERT INTO worker_certificates(
                    serial_hex,fingerprint_sha256,worker_id,instance_id,csr_sha256,
                    request_id,certificate_pem,issuer_id,profile_version,issued_at,
                    not_before,not_after,status,predecessor_serial,created_at,updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    cert.serial_hex.lower(), cert.fingerprint_sha256.lower(),
                    cert.worker_id, instance_id, csr_sha256.lower(), request_id,
                    cert.certificate_pem, cert.issuer_id, int(profile_version), now,
                    cert.not_before, cert.not_after, CertificateStatus.ACTIVE.value,
                    predecessor_serial.lower() if predecessor_serial else None, now, now,
                ),
            )
            self._event_txn(
                conn, "RENEWED" if renewed else "ISSUED", worker_id=cert.worker_id,
                serial_hex=cert.serial_hex, fingerprint=cert.fingerprint_sha256,
            )
            return dict(conn.execute(
                "SELECT * FROM worker_certificates WHERE serial_hex = ?",
                (cert.serial_hex.lower(),),
            ).fetchone())

    def replace(self, old_serial: str, new_serial: str) -> None:
        now = time.time()
        with database.write_txn(self.settings) as conn:
            old = conn.execute(
                "SELECT * FROM worker_certificates WHERE serial_hex = ?",
                (old_serial.lower(),),
            ).fetchone()
            new = conn.execute(
                "SELECT * FROM worker_certificates WHERE serial_hex = ?",
                (new_serial.lower(),),
            ).fetchone()
            if (
                old is None or new is None or old["worker_id"] != new["worker_id"]
                or new["predecessor_serial"] != old_serial.lower()
            ):
                raise CertificateRegistryError("replacement certificates must exist for one worker")
            conn.execute(
                """UPDATE worker_certificates
                   SET status='REPLACED', revoked_at=?, revocation_reason='REPLACED',
                       replaced_by_serial=?, updated_at=? WHERE serial_hex=?""",
                (now, new_serial.lower(), now, old_serial.lower()),
            )
            self._event_txn(
                conn, "REPLACED", worker_id=old["worker_id"], serial_hex=old_serial,
                fingerprint=old["fingerprint_sha256"],
                detail={"replaced_by": new_serial.lower()},
            )

    def revoke_serial(self, serial_hex: str, reason: RevocationReason) -> bool:
        now = time.time()
        with database.write_txn(self.settings) as conn:
            row = conn.execute(
                "SELECT * FROM worker_certificates WHERE serial_hex = ?",
                (serial_hex.lower(),),
            ).fetchone()
            if row is None:
                return False
            if row["status"] == CertificateStatus.REVOKED.value:
                return True
            conn.execute(
                """UPDATE worker_certificates SET status='REVOKED', revoked_at=?,
                   revocation_reason=?, updated_at=? WHERE serial_hex=?""",
                (now, reason.value, now, serial_hex.lower()),
            )
            self._event_txn(
                conn, "REVOKED", worker_id=row["worker_id"], serial_hex=serial_hex,
                fingerprint=row["fingerprint_sha256"], reason=reason.value,
            )
            return True

    def revoke_worker(self, worker_id: str, reason: RevocationReason) -> int:
        with database.read_conn(self.settings) as conn:
            serials = [row[0] for row in conn.execute(
                "SELECT serial_hex FROM worker_certificates WHERE worker_id=? AND status='ACTIVE'",
                (worker_id,),
            ).fetchall()]
        return sum(self.revoke_serial(serial, reason) for serial in serials)

    def validate_presented(
        self,
        *,
        serial_hex: str,
        fingerprint_sha256: str,
        worker_id: str,
        now: float | None = None,
    ) -> dict[str, Any]:
        current = time.time() if now is None else float(now)
        row = self.by_serial(serial_hex)
        if row is None:
            raise CertificateRegistryError("certificate is not enrolled")
        if row["fingerprint_sha256"] != fingerprint_sha256.lower():
            raise CertificateRegistryError("certificate fingerprint does not match registry")
        if row["worker_id"] != worker_id:
            raise CertificateRegistryError("certificate worker identity does not match registry")
        if row["status"] != CertificateStatus.ACTIVE.value:
            raise CertificateRegistryError(f"certificate status is {row['status']}")
        if current < float(row["not_before"]):
            raise CertificateRegistryError("certificate is not yet valid")
        if current >= float(row["not_after"]):
            self._mark_expired(serial_hex, current)
            raise CertificateRegistryError("certificate is expired")
        return row

    def record_rejection(
        self, event_type: str, *, worker_id: str | None = None,
        serial_hex: str | None = None, fingerprint: str | None = None,
        connection_id: str | None = None, reason: str | None = None,
    ) -> None:
        with database.write_txn(self.settings) as conn:
            self._event_txn(
                conn, event_type, worker_id=worker_id, serial_hex=serial_hex,
                fingerprint=fingerprint, connection_id=connection_id, reason=reason,
            )

    def _mark_expired(self, serial_hex: str, now: float) -> None:
        with database.write_txn(self.settings) as conn:
            conn.execute(
                """UPDATE worker_certificates SET status='EXPIRED',updated_at=?
                   WHERE serial_hex=? AND status='ACTIVE'""",
                (now, serial_hex.lower()),
            )

    @staticmethod
    def _event_txn(
        conn: Any, event_type: str, *, worker_id: str | None = None,
        serial_hex: str | None = None, fingerprint: str | None = None,
        connection_id: str | None = None, reason: str | None = None,
        detail: dict[str, Any] | None = None,
    ) -> None:
        conn.execute(
            """INSERT INTO worker_certificate_security_events(
                event_type,worker_id,serial_hex,fingerprint_sha256,connection_id,
                reason,detail_json,occurred_at) VALUES (?,?,?,?,?,?,?,?)""",
            (
                event_type, worker_id, serial_hex.lower() if serial_hex else None,
                fingerprint.lower() if fingerprint else None, connection_id,
                (reason or "")[:200] or None,
                json.dumps(detail or {}, sort_keys=True, separators=(",", ":")),
                time.time(),
            ),
        )
