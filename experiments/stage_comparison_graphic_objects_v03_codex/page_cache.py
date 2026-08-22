"""Page-level drawing cache keyed by PDF SHA, page and extractor version."""
from __future__ import annotations

import hashlib
import pickle
from pathlib import Path
from typing import Any

import fitz


EXTRACTOR_VERSION = "graphic-objects-v03-codex-2-uncompressed-cache"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class PageDrawingCache:
    """Call ``get_drawings`` once per unique page and retain coordinate metadata."""

    def __init__(self, directory: str | Path, *, extractor_version: str = EXTRACTOR_VERSION) -> None:
        self.directory = Path(directory)
        self.extractor_version = extractor_version
        self._memory: dict[tuple[str, int, str], dict[str, Any]] = {}
        self._sha: dict[tuple[str, int, int], str] = {}
        self.stats = {"requests": 0, "memory_hits": 0, "disk_hits": 0, "misses": 0, "get_drawings_calls": 0}

    def _pdf_sha(self, path: Path) -> str:
        stat = path.stat(); key = (str(path.resolve()), stat.st_size, stat.st_mtime_ns)
        if key not in self._sha:
            self._sha[key] = sha256_file(path)
        return self._sha[key]

    def cache_path(self, pdf_path: str | Path, page_index: int) -> Path:
        path = Path(pdf_path).resolve(); digest = self._pdf_sha(path)
        version = hashlib.sha256(self.extractor_version.encode()).hexdigest()[:12]
        return self.directory / f"{digest}-page-{int(page_index):05d}-{version}.pickle"

    def get(self, pdf_path: str | Path, page_index: int) -> dict[str, Any]:
        path = Path(pdf_path).resolve(); page_index = int(page_index); self.stats["requests"] += 1
        digest = self._pdf_sha(path); key = (digest, page_index, self.extractor_version)
        if key in self._memory:
            self.stats["memory_hits"] += 1
            return self._memory[key]
        cache_path = self.cache_path(path, page_index)
        if cache_path.is_file():
            with cache_path.open("rb") as stream:
                payload = pickle.load(stream)
            if payload.get("pdf_sha256") == digest and payload.get("extractor_version") == self.extractor_version:
                self.stats["disk_hits"] += 1; self._memory[key] = payload; return payload
        document = fitz.open(path)
        try:
            page = document[page_index]
            raw_rotation = document.xref_get_key(page.xref, "Rotate")
            if raw_rotation and raw_rotation[0] == "int" and int(raw_rotation[1]) % 90:
                raise ValueError(f"unsupported /Rotate {raw_rotation[1]}")
            mediabox = page.mediabox
            visible = page.cropbox & fitz.Rect(mediabox.x0, 0.0, mediabox.x1, mediabox.y1 - mediabox.y0)
            drawings = page.get_drawings(); self.stats["get_drawings_calls"] += 1
            payload = {
                "schema_version": "graphic-page-drawings-v0.3",
                "extractor_version": self.extractor_version,
                "pdf_sha256": digest,
                "page_index": page_index,
                "page_rotation": int(page.rotation),
                "page_rect": list(page.rect),
                "cropbox": list(page.cropbox),
                "cropbox_position": [float(page.cropbox_position.x), float(page.cropbox_position.y)],
                "visible_box": list(visible),
                "rotation_matrix": tuple(page.rotation_matrix),
                "derotation_matrix": tuple(page.derotation_matrix),
                "transformation_matrix": tuple(page.transformation_matrix),
                "unrotated_width": float(visible.width),
                "unrotated_height": float(visible.height),
                "drawings": drawings,
            }
        finally:
            document.close()
        self.directory.mkdir(parents=True, exist_ok=True)
        # PyMuPDF drawing payloads are highly nested. gzip saved disk but added
        # 30-100 s on dense pages, defeating the page cache's latency purpose.
        # The research cache is ignored by git, so prefer fast local pickle IO.
        with cache_path.open("wb") as stream:
            pickle.dump(payload, stream, protocol=pickle.HIGHEST_PROTOCOL)
        self.stats["misses"] += 1; self._memory[key] = payload; return payload

    def disk_size_bytes(self) -> int:
        return sum(path.stat().st_size for path in self.directory.glob("*.pickle")) if self.directory.is_dir() else 0


__all__ = ["PageDrawingCache", "EXTRACTOR_VERSION"]
