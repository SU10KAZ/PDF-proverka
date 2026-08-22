"""Page-level cache for raw PyMuPDF vector/text payloads.

The cache is deliberately confined to this experiment.  Payloads are trusted
local pickle files: they must never be accepted from an untrusted source.
"""
from __future__ import annotations

import gzip
import hashlib
import pickle
import time
from pathlib import Path
from typing import Any

import fitz


DEFAULT_EXTRACTOR_VERSION = "vector-block-v02-codex-1"


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class PageCache:
    """Load each PDF page once, then reuse its raw vector and text payload."""

    def __init__(
        self,
        cache_dir: str | Path,
        *,
        extractor_version: str = DEFAULT_EXTRACTOR_VERSION,
    ) -> None:
        self.cache_dir = Path(cache_dir)
        self.extractor_version = extractor_version
        self._memory: dict[tuple[str, int, str], dict[str, Any]] = {}
        self._sha_memory: dict[tuple[str, int, int], str] = {}
        self.stats = {
            "requests": 0,
            "memory_hits": 0,
            "disk_hits": 0,
            "misses": 0,
            "get_drawings_calls": 0,
            "get_text_calls": 0,
            "pdf_sha_calls": 0,
        }

    def _pdf_sha(self, pdf_path: Path) -> str:
        stat = pdf_path.stat()
        key = (str(pdf_path.resolve()), stat.st_size, stat.st_mtime_ns)
        if key not in self._sha_memory:
            self._sha_memory[key] = sha256_file(pdf_path)
            self.stats["pdf_sha_calls"] += 1
        return self._sha_memory[key]

    def cache_key(self, pdf_path: str | Path, page_index: int) -> str:
        pdf_sha = self._pdf_sha(Path(pdf_path))
        version_sha = hashlib.sha256(self.extractor_version.encode("utf-8")).hexdigest()[:12]
        return f"{pdf_sha}-page-{int(page_index):05d}-{version_sha}"

    def cache_path(self, pdf_path: str | Path, page_index: int) -> Path:
        return self.cache_dir / f"{self.cache_key(pdf_path, page_index)}.pickle.gz"

    def get(
        self,
        pdf_path: str | Path,
        page_index: int,
        *,
        force_rebuild: bool = False,
    ) -> dict[str, Any]:
        pdf_path = Path(pdf_path).resolve()
        page_index = int(page_index)
        self.stats["requests"] += 1
        pdf_sha = self._pdf_sha(pdf_path)
        memory_key = (pdf_sha, page_index, self.extractor_version)
        if not force_rebuild and memory_key in self._memory:
            self.stats["memory_hits"] += 1
            payload = self._memory[memory_key]
            payload["cache_access"] = "memory_hit"
            return payload

        cache_path = self.cache_path(pdf_path, page_index)
        if not force_rebuild and cache_path.is_file():
            with gzip.open(cache_path, "rb") as stream:
                payload = pickle.load(stream)
            if (
                payload.get("pdf_sha256") == pdf_sha
                and payload.get("page_index") == page_index
                and payload.get("extractor_version") == self.extractor_version
            ):
                self.stats["disk_hits"] += 1
                payload["cache_access"] = "disk_hit"
                payload["cache_path"] = str(cache_path)
                self._memory[memory_key] = payload
                return payload

        started = time.perf_counter()
        document = fitz.open(pdf_path)
        try:
            if page_index < 0 or page_index >= len(document):
                raise ValueError(f"page_index {page_index} outside PDF with {len(document)} pages")
            page = document[page_index]
            drawings = page.get_drawings()
            self.stats["get_drawings_calls"] += 1
            text_dict = page.get_text("dict")
            self.stats["get_text_calls"] += 1
            payload = {
                "schema_version": "vector-page-payload-v0.2",
                "extractor_version": self.extractor_version,
                "pdf": str(pdf_path),
                "pdf_sha256": pdf_sha,
                "page_index": page_index,
                "page_count": len(document),
                "page_width": float(page.rect.width),
                "page_height": float(page.rect.height),
                "page_rotation": int(page.rotation),
                "drawings": drawings,
                "text_dict": text_dict,
                "page_drawings_total": len(drawings),
                "page_text_blocks_total": len(text_dict.get("blocks") or []),
                "build_seconds": time.perf_counter() - started,
                "cache_access": "miss",
            }
        finally:
            document.close()

        self.cache_dir.mkdir(parents=True, exist_ok=True)
        with gzip.open(cache_path, "wb", compresslevel=5) as stream:
            pickle.dump(payload, stream, protocol=pickle.HIGHEST_PROTOCOL)
        payload["cache_path"] = str(cache_path)
        payload["cache_bytes"] = cache_path.stat().st_size
        self.stats["misses"] += 1
        self._memory[memory_key] = payload
        return payload

    def clear_memory(self) -> None:
        self._memory.clear()

    def disk_size_bytes(self) -> int:
        if not self.cache_dir.is_dir():
            return 0
        return sum(path.stat().st_size for path in self.cache_dir.glob("*.pickle.gz"))
