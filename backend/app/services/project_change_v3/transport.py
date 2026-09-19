"""Versioned, lossless provider transport for V3 model payloads.

A Codex turn rejects more than ``CODEX_TURN_MAX_CHARS`` characters of user text
before inference (``input_too_large``, observed with codex-cli 0.153.0 on the
frozen A-R003 miner prompt: 1 654 794 characters).  Such a payload is NEVER
shortened: it is split into ordered exact chunks, every chunk but the last is
appended to the model-visible history of a fresh ephemeral thread
(``thread/inject_items``), and the last chunk starts the turn together with the
images.  The concatenation of the chunks is the payload, byte for byte — the
same method as the research transport that produced the frozen V3 results
(``experiments/project_change_272/ai_first_semantic_mapping_projectchange_v3_pair_a_recover.py``).

A payload within the limit keeps the ordinary ``codex exec`` transport.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

PROVIDER_TRANSPORT_VERSION = "projectchange_v3_codex_transport/1"
CODEX_TURN_MAX_CHARS = 1_048_576
CHUNK_CHARS = 800_000  # research value; every chunk also fits a turn on its own
STANDARD_TRANSPORT = "codex_exec_stdin"
OVERSIZE_TRANSPORT = "codex_app_server_injected_history_plus_final_turn_chunk"
# Research parameter of the validated oversize transport (TurnStartParams,
# experimental): explicit standard treatment instead of the automatic one.
OVERSIZE_CYBER_ACCESS_PROGRAM = "standard"


class TransportIntegrityError(RuntimeError):
    """What would reach the model differs from the payload — nothing is sent."""


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class TransportPlan:
    payload_sha256: str
    payload_chars: int
    payload_bytes: int
    oversize: bool
    chunks: tuple[str, ...]

    @property
    def transport(self) -> str:
        return OVERSIZE_TRANSPORT if self.oversize else STANDARD_TRANSPORT

    def receipt(self, image_paths: Sequence[str] = ()) -> dict[str, Any]:
        return {
            "provider_transport_version": PROVIDER_TRANSPORT_VERSION,
            "transport": self.transport,
            "oversize_transport_used": self.oversize,
            "model_visible_payload_sha256": self.payload_sha256,
            "model_visible_payload_size": self.payload_chars,
            "model_visible_payload_bytes": self.payload_bytes,
            "codex_turn_max_chars": CODEX_TURN_MAX_CHARS,
            "chunk_chars": [len(chunk) for chunk in self.chunks],
            "chunk_sha256": [sha256_text(chunk) for chunk in self.chunks],
            "injected_history_items": len(self.chunks) - 1 if self.oversize else 0,
            "images": len(image_paths),
            "image_sha256": [hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in image_paths],
        }


def plan(payload: str, *, limit: int = CODEX_TURN_MAX_CHARS, chunk_chars: int = CHUNK_CHARS) -> TransportPlan:
    if not isinstance(payload, str) or not payload:
        raise TransportIntegrityError("empty model payload")
    if not 0 < chunk_chars <= limit:
        raise TransportIntegrityError("chunk size must fit one Codex turn")
    oversize = len(payload) > limit
    chunks = (tuple(payload[i:i + chunk_chars] for i in range(0, len(payload), chunk_chars))
              if oversize else (payload,))
    result = TransportPlan(sha256_text(payload), len(payload), len(payload.encode("utf-8")), oversize, chunks)
    verify(chunks, result)
    return result


def verify(texts: Sequence[str], expected: TransportPlan) -> None:
    """Fail closed unless the ordered texts are exactly the payload."""
    joined = "".join(texts)
    if len(joined) != expected.payload_chars or sha256_text(joined) != expected.payload_sha256:
        raise TransportIntegrityError("model-visible text differs from the payload")
    if expected.oversize and any(len(t) > CODEX_TURN_MAX_CHARS for t in texts):
        raise TransportIntegrityError("a transport chunk exceeds one Codex turn")
