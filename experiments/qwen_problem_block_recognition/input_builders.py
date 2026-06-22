from __future__ import annotations

import base64
from typing import Iterable


def _data_url(mime: str, payload: bytes) -> str:
    return f"data:{mime};base64," + base64.b64encode(payload).decode("ascii")


def _text(prompt: str) -> dict:
    return {"type": "text", "text": prompt}


def _image(url: str) -> dict:
    return {"type": "image_url", "image_url": {"url": url}}


def build_image_message(prompt: str, png_bytes: bytes):
    content = [_text(prompt), _image(_data_url("image/png", png_bytes))]
    return content, {"input_mode": "image_data_url", "input_size_bytes": len(png_bytes)}


def build_pdf_base64_messages(prompt: str, pdf_bytes: bytes):
    pdf_url = _data_url("application/pdf", pdf_bytes)
    b64 = base64.b64encode(pdf_bytes).decode("ascii")
    shapes = [
        ("image_url_pdf_data", [_text(prompt), _image(pdf_url)]),
        ("file_filedata", [_text(prompt), {"type": "file", "file": {"file_data": b64, "mime_type": "application/pdf"}}]),
    ]
    return shapes, {"input_mode": "pdf_base64", "input_size_bytes": len(pdf_bytes)}


def build_pdf_url_messages(prompt: str, pdf_url: str):
    shapes = [("image_url_remote_pdf", [_text(prompt), _image(pdf_url)])]
    return shapes, {"input_mode": "pdf_url", "input_size_bytes": 0}


def build_multi_image_message(prompt: str, png_images: Iterable[bytes]):
    imgs = list(png_images)
    content = [_text(prompt)]
    for payload in imgs:
        content.append(_image(_data_url("image/png", payload)))
    return content, {
        "input_mode": "multi_image_data_url",
        "n_images": len(imgs),
        "input_size_bytes": sum(len(p) for p in imgs),
    }
