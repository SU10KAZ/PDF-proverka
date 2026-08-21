"""List current PDFs and their canonical results HTML in a comparison stage."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from . import stage_storage


@dataclass(frozen=True)
class PdfEntry:
    pdf_path: Path
    md_path: Path | None
    html_path: Path | None
    relative: str
    filename: str
    document_code: str | None = None
    discipline: str | None = None
    version_id: str | None = None

    def to_dict(self) -> dict:
        return {
            "pdf_path": str(self.pdf_path),
            "md_path": str(self.md_path) if self.md_path else None,
            "html_path": str(self.html_path) if self.html_path else None,
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


def _markdown_for_pdf(pdf: Path) -> Path | None:
    """Resolve only a neighbouring current Markdown source for a PDF."""
    try:
        candidates = [path for path in pdf.parent.glob("*.md") if path.is_file()]
    except OSError:
        return None
    if not candidates:
        return None
    stem = pdf.stem.casefold()
    exact_names = (
        f"{stem}.md",
        f"{stem}_results.md",
        "document.md",
    )
    by_name = {path.name.casefold(): path for path in candidates}
    for name in exact_names:
        if name in by_name:
            return by_name[name]
    related = [
        path for path in candidates
        if path.stem.casefold().startswith(stem) or stem.startswith(path.stem.casefold())
    ]
    if related:
        return sorted(related, key=lambda path: path.name.casefold())[0]
    try:
        pdf_count = sum(1 for path in pdf.parent.glob("*.pdf") if path.is_file())
    except OSError:
        pdf_count = 0
    return candidates[0] if len(candidates) == 1 and pdf_count == 1 else None


def _results_html_for_pdf(pdf: Path) -> Path | None:
    """Resolve a neighbouring generated results HTML without inspecting its body."""
    try:
        candidates = [
            path for path in pdf.parent.iterdir()
            if path.is_file() and path.suffix.casefold() in {".html", ".htm"}
        ]
    except OSError:
        return None
    if not candidates:
        return None
    stem = pdf.stem.casefold()
    exact_names = ("ocr.html", f"{stem}_results.html", f"{stem}_results.htm")
    by_name = {path.name.casefold(): path for path in candidates}
    for name in exact_names:
        if name in by_name:
            return by_name[name]
    related = [path for path in candidates if path.stem.casefold().startswith(stem)]
    if related:
        return sorted(related, key=lambda path: path.name.casefold())[0]
    try:
        pdf_count = sum(1 for path in pdf.parent.glob("*.pdf") if path.is_file())
    except OSError:
        pdf_count = 0
    return candidates[0] if len(candidates) == 1 and pdf_count == 1 else None


def scan_stage_folder(folder: str | Path) -> tuple[list[PdfEntry], list[str]]:
    """Return current PDFs and their optional source/index paths."""
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
                md_path=item.get("md_path"),
                html_path=item.get("html_path"),
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
            entries.append(PdfEntry(
                pdf_path=pdf,
                md_path=_markdown_for_pdf(pdf),
                html_path=_results_html_for_pdf(pdf),
                relative=relative,
                filename=pdf.name,
            ))

    entries.sort(key=lambda entry: entry.relative.casefold())
    warnings = [] if entries else [f"В папке не найдено PDF: {root}"]
    return entries, warnings


__all__ = ["PdfEntry", "scan_stage_folder"]
