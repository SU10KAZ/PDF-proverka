"""Local context estimates; no model, credentials, tokenizer downloads or real corpus."""
import pytest

from backend.app.services.project_change_v3 import transport
from backend.app.services.stage_comparison.ai.gateway import classify_failure
from backend.tests.project_change_v3.test_claude_opus_provider import png


def test_text_and_images_share_one_budget(tmp_path, monkeypatch):
    path = tmp_path / "diagram.png"
    path.write_bytes(png(1024, 1024))
    monkeypatch.setattr(transport, "CODEX_MAPPER_INPUT_TOKENS", 2000)
    assert transport.plan_codex_mapper("x" * 3000, [], {})
    assert transport.plan_codex_mapper("x", [str(path)], {})
    with pytest.raises(transport.TransportIntegrityError, match="бюджет контекста Codex"):
        transport.plan_codex_mapper("x" * 3000, [str(path)], {})


def test_image_resolution_matters_even_for_one_image(tmp_path):
    path = tmp_path / "diagram.png"
    path.write_bytes(png(1024, 1024))
    small = transport.plan_codex_mapper("x", [str(path)], {})
    path.write_bytes(png(4096, 4096))
    large = transport.plan_codex_mapper("x", [str(path)], {})
    assert large["estimated_image_tokens"] > 10 * small["estimated_image_tokens"]


def test_unreadable_image_is_refused(tmp_path):
    path = tmp_path / "broken.png"
    path.write_bytes(b"not an image")
    with pytest.raises(transport.TransportIntegrityError, match="размеры"):
        transport.plan_codex_mapper("x", [str(path)], {})


def test_multibyte_text_and_output_schema_are_counted(monkeypatch):
    monkeypatch.setattr(transport, "CODEX_MAPPER_INPUT_TOKENS", 1000)
    assert transport.plan_codex_mapper("x" * 1500, [], {})
    with pytest.raises(transport.TransportIntegrityError):
        transport.plan_codex_mapper("Ж" * 1500, [], {})
    with pytest.raises(transport.TransportIntegrityError):
        transport.plan_codex_mapper("x" * 1500, [], {"description": "x" * 1500})


@pytest.mark.parametrize("message", [
    '{"codexErrorInfo":"contextWindowExceeded"}',
    "context_length_exceeded", "context_window_exceeded",
    "Codex ran out of room in the model's context window.",
    "maximum context length exceeded; stream disconnected",
])
def test_context_failure_is_specific_and_not_transient(message):
    assert classify_failure(message) == "CONTEXT_WINDOW_EXCEEDED"
