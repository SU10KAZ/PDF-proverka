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

Since engine 3.5.0 the production provider is Claude Opus through the Claude
Code CLI (``claude -p``, stream-json), and the Codex transport above is kept
only for its own tests.  The Claude transport has NO per-turn text limit: the
whole payload is one user message and the model's native context window takes
it.  What the Claude CLI does have is a media envelope, read in the CLI itself
(2.1.270), not in its documentation:

* more than ``CLAUDE_MEDIA_COUNT_MAX`` images in a request, or more than
  ``CLAUDE_MEDIA_BASE64_BYTES_MAX`` base64 bytes of images, and the CLI
  SILENTLY REMOVES the oldest images before the request leaves the machine;
* an image above ``CLAUDE_IMAGE_MAX_PX`` on a side is downscaled;
* an image above ``CLAUDE_IMAGE_PASSTHROUGH_BYTES`` is re-encoded as JPEG.

The first two would drop or shrink evidence, so ``plan_claude`` refuses such a
call before the provider is contacted (nothing is sent, nothing is trimmed).
The third cannot be switched off from outside; it changes no image's content
or size in pixels, and every call receipt names the images it applies to.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

CODEX_TRANSPORT_VERSION = "projectchange_v3_codex_transport/1"
CLAUDE_TRANSPORT_VERSION = "projectchange_v3_claude_cli_transport/1"
# The transport of the provider production runs on (provenance of every result).
PROVIDER_TRANSPORT_VERSION = CLAUDE_TRANSPORT_VERSION
CODEX_TURN_MAX_CHARS = 1_048_576
CHUNK_CHARS = 800_000  # research value; every chunk also fits a turn on its own
STANDARD_TRANSPORT = "codex_exec_stdin"
OVERSIZE_TRANSPORT = "codex_app_server_injected_history_plus_final_turn_chunk"
# Research parameter of the validated oversize transport (TurnStartParams,
# experimental): explicit standard treatment instead of the automatic one.
OVERSIZE_CYBER_ACCESS_PROGRAM = "standard"

CLAUDE_TRANSPORT = "claude_cli_stream_json_single_user_message"
# Media envelope of the Claude Code CLI (values read in CLI 2.1.270).
CLAUDE_MEDIA_COUNT_MAX = 100
CLAUDE_MEDIA_BASE64_BYTES_MAX = 25_165_824
CLAUDE_IMAGE_MAX_PX = 2000
CLAUDE_IMAGE_PASSTHROUGH_BYTES = 512_000
# One Messages API request; the text is sent as UTF-8, the images as base64.
CLAUDE_REQUEST_BYTES_MAX = 33_554_432
CLAUDE_CONTEXT_WINDOW_TOKENS = 1_000_000


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
            "provider_transport_version": CODEX_TRANSPORT_VERSION,
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


def _base64_len(raw_bytes: int) -> int:
    return 4 * ((raw_bytes + 2) // 3)


def image_size_px(data: bytes) -> tuple[int, int] | None:
    """Pixel size from the file header (PNG, JPEG, GIF, WebP VP8X); None when unreadable."""
    if data[:8] == b"\x89PNG\r\n\x1a\n" and len(data) >= 24:
        return int.from_bytes(data[16:20], "big"), int.from_bytes(data[20:24], "big")
    if data[:6] in (b"GIF87a", b"GIF89a") and len(data) >= 10:
        return int.from_bytes(data[6:8], "little"), int.from_bytes(data[8:10], "little")
    if data[:4] == b"RIFF" and data[8:16] == b"WEBPVP8X" and len(data) >= 30:
        return int.from_bytes(data[24:27], "little") + 1, int.from_bytes(data[27:30], "little") + 1
    if data[:3] == b"\xff\xd8\xff":
        index = 2
        while index + 9 < len(data):
            if data[index] != 0xFF:
                index += 1
                continue
            marker = data[index + 1]
            if marker in (0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF):
                return int.from_bytes(data[index + 7:index + 9], "big"), int.from_bytes(data[index + 5:index + 7], "big")
            if marker in (0xD8, 0x01) or 0xD0 <= marker <= 0xD7:
                index += 2
                continue
            index += 2 + int.from_bytes(data[index + 2:index + 4], "big")
    return None


@dataclass(frozen=True)
class ClaudeTransportPlan:
    payload_sha256: str
    payload_chars: int
    payload_bytes: int
    images: tuple[dict[str, Any], ...]

    transport = CLAUDE_TRANSPORT
    oversize = False  # no per-turn text limit: one user message, native context

    @property
    def media_base64_bytes(self) -> int:
        return sum(_base64_len(row["bytes"]) for row in self.images)

    def receipt(self, image_paths: Sequence[str] = ()) -> dict[str, Any]:
        recompressed = [row["sha256"] for row in self.images if row["bytes"] > CLAUDE_IMAGE_PASSTHROUGH_BYTES]
        return {
            "provider_transport_version": CLAUDE_TRANSPORT_VERSION,
            "transport": self.transport,
            "oversize_transport_used": False,
            "oversize_strategy": "native_context_single_user_message",
            "model_visible_payload_sha256": self.payload_sha256,
            "model_visible_payload_size": self.payload_chars,
            "model_visible_payload_bytes": self.payload_bytes,
            "chunk_chars": [self.payload_chars],
            "chunk_sha256": [self.payload_sha256],
            "injected_history_items": 0,
            "images": len(self.images),
            "image_sha256": [row["sha256"] for row in self.images],
            "image_bytes": [row["bytes"] for row in self.images],
            "image_px": [row["px"] for row in self.images],
            "media_base64_bytes": self.media_base64_bytes,
            "media_envelope": {
                "count_max": CLAUDE_MEDIA_COUNT_MAX,
                "base64_bytes_max": CLAUDE_MEDIA_BASE64_BYTES_MAX,
                "image_max_px": CLAUDE_IMAGE_MAX_PX,
                "image_passthrough_bytes": CLAUDE_IMAGE_PASSTHROUGH_BYTES,
                "request_bytes_max": CLAUDE_REQUEST_BYTES_MAX,
            },
            # Not dropped and not resized: the provider CLI re-encodes these as JPEG.
            "images_reencoded_by_provider_cli": len(recompressed),
            "images_reencoded_by_provider_cli_sha256": recompressed,
            "evidence_dropped": 0,
            "evidence_truncated": 0,
        }


def plan_claude(payload: str, image_paths: Sequence[str] = ()) -> ClaudeTransportPlan:
    """Fail closed unless the Claude CLI would deliver every image and the whole text."""
    if not isinstance(payload, str) or not payload:
        raise TransportIntegrityError("empty model payload")
    rows: list[dict[str, Any]] = []
    for index, path in enumerate(image_paths, start=1):
        data = Path(path).read_bytes()
        size = image_size_px(data)
        if size is None:
            raise TransportIntegrityError(
                f"image {index}: pixel size unreadable — the provider CLI could replace it with a text stub"
            )
        if max(size) > CLAUDE_IMAGE_MAX_PX:
            raise TransportIntegrityError(
                f"image {index} is {size[0]}x{size[1]} px: above {CLAUDE_IMAGE_MAX_PX} px the provider CLI "
                "downscales the evidence"
            )
        rows.append({"sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data), "px": list(size)})
    result = ClaudeTransportPlan(sha256_text(payload), len(payload), len(payload.encode("utf-8")), tuple(rows))
    if len(rows) > CLAUDE_MEDIA_COUNT_MAX:
        raise TransportIntegrityError(
            f"{len(rows)} images in one call: above {CLAUDE_MEDIA_COUNT_MAX} the provider CLI silently removes "
            "the oldest images"
        )
    if result.media_base64_bytes > CLAUDE_MEDIA_BASE64_BYTES_MAX:
        raise TransportIntegrityError(
            f"{result.media_base64_bytes} base64 bytes of images in one call: above "
            f"{CLAUDE_MEDIA_BASE64_BYTES_MAX} the provider CLI silently removes the oldest images"
        )
    if result.payload_bytes + result.media_base64_bytes > CLAUDE_REQUEST_BYTES_MAX:
        raise TransportIntegrityError(
            f"request of {result.payload_bytes + result.media_base64_bytes} bytes exceeds one provider request "
            f"({CLAUDE_REQUEST_BYTES_MAX})"
        )
    return result
