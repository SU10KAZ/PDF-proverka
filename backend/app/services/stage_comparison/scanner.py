"""List source PDFs in a comparison stage without interpreting their content."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from . import stage_storage


@dataclass(frozen=True)
class PdfEntry:
    pdf_path: Path
    relative: str
    filename: str
    document_code: str | None = None
    discipline: str | None = None
    version_id: str | None = None

    def to_dict(self) -> dict:
        return {
            "pdf_path": str(self.pdf_path),
            "relative": self.relative,
            "filename": self.filename,
            "document_code": self.document_code,
            "discipline": self.discipline,
            "version_id": self.version_id,
        }


def _safe_pdf_iter(root: Path) -> Iterator[Path]:
    try:
        for path in root.rglob("*.pdf"):
            try:
                if path.is_file():
                    yield path
            except OSError:
                continue
    except (OSError, PermissionError):
        return


def scan_stage_folder(folder: str | Path) -> tuple[list[PdfEntry], list[str]]:
    """Return current PDFs only; MD/JSON/blocks remain stored but are not read."""
    root = Path(folder).expanduser()
    if not root.exists():
        return [], [f"Папка не существует: {root}"]
    if not root.is_dir():
        return [], [f"Не папка: {root}"]

    entries: list[PdfEntry] = []
    if stage_storage.is_versioned_stage(root):
        for item in stage_storage.iter_current_documents(root):
            document = item["document"]
            code = str(document.get("document_code") or "") or None
            filename = str(item.get("source_filename") or "") or f"{code or 'document'}.pdf"
            entries.append(PdfEntry(
                pdf_path=item["pdf_path"],
                relative=str(Path("documents") / (code or Path(filename).stem) / "versions" /
                             str(item["version_id"]) / filename),
                filename=filename,
                document_code=code,
                discipline=str(document.get("discipline") or "") or None,
                version_id=str(item["version_id"]),
            ))
    else:
        for pdf in _safe_pdf_iter(root):
            try:
                relative = str(pdf.relative_to(root))
            except ValueError:
                relative = pdf.name
            entries.append(PdfEntry(pdf, relative, pdf.name))

    entries.sort(key=lambda entry: entry.relative.casefold())
    warnings = [] if entries else [f"В папке не найдено PDF: {root}"]
    return entries, warnings


__all__ = ["PdfEntry", "scan_stage_folder"]
