import json
from pathlib import Path

import fitz
import pytest
from PIL import Image

from backend.scripts import build_vector_graph_gallery as gallery


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def test_rerender_existing_previews_at_exact_300_dpi(tmp_path, monkeypatch):
    corpus = tmp_path / "experiments" / "блоки разных дисциплин"
    source = corpus / "АР" / "sample.pdf"
    source.parent.mkdir(parents=True)
    document = fitz.open()
    page = document.new_page(width=72, height=36)
    page.draw_line((0, 0), (72, 36))
    document.save(source)
    document.close()

    pos_documents = tmp_path / "pos" / "documents"
    output = (
        pos_documents / "ВЕКТОГРАФ — АР" / "versions" / gallery.VERSION_ID
        / "03_analysis" / "latest"
    )
    blocks_dir = output / "blocks_stage02_100"
    old_preview = blocks_dir / "block_TEST.webp"
    old_preview.parent.mkdir(parents=True)
    Image.new("RGB", (10, 5), "white").save(old_preview, format="WEBP")
    _write_json(blocks_dir / "index.json", {
        "schema_version": 1,
        "gallery": True,
        "blocks": [{"block_id": "TEST", "file": old_preview.name}],
    })
    _write_json(output / "vector_graph_gallery.json", {
        "schema_version": 1,
        "blocks": [{
            "block_id": "TEST",
            "file": old_preview.name,
            "source_pdf": str(source.relative_to(tmp_path)),
        }],
    })
    monkeypatch.setattr(gallery, "ROOT", tmp_path)
    monkeypatch.setattr(gallery, "CORPUS_ROOT", corpus)
    monkeypatch.setattr(gallery, "POS_DOCUMENTS", pos_documents)

    summary = gallery.rerender_existing_previews(
        "АР", dpi=300, preview_format="png", preview_quality=90, force=True
    )

    rendered = blocks_dir / "block_TEST.png"
    with Image.open(rendered) as image:
        assert image.size == (300, 150)
        assert image.info["dpi"][0] == pytest.approx(300, abs=0.1)
        assert image.info["dpi"][1] == pytest.approx(300, abs=0.1)
    index = json.loads((blocks_dir / "index.json").read_text(encoding="utf-8"))
    manifest = json.loads(
        (output / "vector_graph_gallery.json").read_text(encoding="utf-8")
    )
    assert summary["blocks"] == 1
    assert index["dpi"] == 300
    assert index["preview_format"] == "png"
    assert index["blocks"][0]["file"] == "block_TEST.png"
    assert index["blocks"][0]["render_size"] == [300, 150]
    assert manifest["preview"]["quality"] == "lossless"
    assert old_preview.is_file()


def test_rerender_existing_previews_rejects_source_outside_corpus(tmp_path, monkeypatch):
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    source = tmp_path / "outside.pdf"
    document = fitz.open()
    document.new_page()
    document.save(source)
    document.close()
    pos_documents = tmp_path / "pos"
    output = (
        pos_documents / "ВЕКТОГРАФ — АР" / "versions" / gallery.VERSION_ID
        / "03_analysis" / "latest"
    )
    _write_json(output / "blocks_stage02_100" / "index.json", {
        "blocks": [{"block_id": "TEST", "file": "block_TEST.webp"}],
    })
    _write_json(output / "vector_graph_gallery.json", {
        "blocks": [{
            "block_id": "TEST",
            "file": "block_TEST.webp",
            "source_pdf": source.name,
        }],
    })
    monkeypatch.setattr(gallery, "ROOT", tmp_path)
    monkeypatch.setattr(gallery, "CORPUS_ROOT", corpus)
    monkeypatch.setattr(gallery, "POS_DOCUMENTS", pos_documents)

    with pytest.raises(RuntimeError, match="source escapes gallery corpus"):
        gallery.rerender_existing_previews(
            "АР", dpi=300, preview_format="png", preview_quality=90, force=True
        )
