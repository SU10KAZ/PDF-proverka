"""Regression tests for blocks.py runtime dependency guards."""

from pathlib import Path

import pytest

import blocks


def test_crop_from_pdf_requires_pymupdf(monkeypatch, tmp_path):
    monkeypatch.setattr(blocks, "fitz", None)

    with pytest.raises(RuntimeError, match="PyMuPDF"):
        blocks.crop_from_pdf(
            pdf_path=tmp_path / "missing.pdf",
            page_num=1,
            coords_px=[0, 0, 10, 10],
            page_width=100,
            page_height=100,
            out_png=tmp_path / "out.png",
        )
