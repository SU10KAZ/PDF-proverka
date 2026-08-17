"""Сканирование папок стадий: PDF + рядом MD + result.json + сопоставление PDF между стадиями."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from pathlib import Path
from typing import Optional

from . import stage_storage


# Слова, которые подавляются при нормализации имени PDF, чтобы стадии
# "_П"/"_Р", "v1"/"v2" не мешали матчингу. Лежат отдельным constant'ом,
# чтобы при необходимости расширить без переписывания логики.
_STAGE_NOISE_WORDS = {
    "stage", "стадия", "stadia", "ред", "rev",
    "v1", "v2", "v3", "v4",
    "old", "new", "draft", "final",
    "пд", "рд", "ии",
    "изм", "edition",
}

# Расширение бинарных мусорных хвостов: "_(1)", "копия", "(копия)" и т.п.
_NOISE_SUFFIX_PATTERN = re.compile(
    r"(?:\(?\s*коп(?:ия|ия\s*\d+)?\s*\)?|\s*\(\d+\)\s*$|\s*-\s*копия\s*$)",
    re.IGNORECASE | re.UNICODE,
)


def _strip_noise_words(tokens: list[str]) -> list[str]:
    """Удалить «шумные» токены (stage/v1/...), но не если они единственные."""
    cleaned = [t for t in tokens if t and t.lower() not in _STAGE_NOISE_WORDS]
    if cleaned:
        return cleaned
    return tokens


def _normalize_pdf_name(name: str) -> str:
    """Нормализация для сопоставления PDF между стадиями.

    Приводит к lowercase, режет расширение, убирает мусорные суффиксы и
    разделители, опционально подавляет stage/v1/.../pd/rd, оставляет
    последовательность токенов через одиночный пробел.

    Никогда не падает: всегда возвращает str (возможно пустую).
    """
    s = (name or "").strip()
    # Срезаем расширение PDF
    if s.lower().endswith(".pdf"):
        s = s[:-4]
    s = s.lower()
    # «копия», «(1)» и т.п.
    s = _NOISE_SUFFIX_PATTERN.sub(" ", s)
    # Разделители → пробелы
    s = re.sub(r"[\s_\-\.\(\)\[\]]+", " ", s)
    # Любая последовательность из не-букв/цифр/пробела → пробел
    s = re.sub(r"[^0-9a-zа-я\s]+", " ", s, flags=re.IGNORECASE | re.UNICODE)
    s = re.sub(r"\s+", " ", s).strip()
    tokens = s.split(" ") if s else []
    tokens = _strip_noise_words(tokens)
    return " ".join(tokens)


def _similarity(a: str, b: str) -> float:
    """Похожесть нормализованных имён в [0, 1]."""
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    return SequenceMatcher(None, a, b).ratio()


# Пороги: выше exact = matched, между fuzzy и exact = maybe.
EXACT_THRESHOLD = 0.97
FUZZY_THRESHOLD = 0.70


@dataclass
class PdfEntry:
    """Найденный PDF с найденными рядом MD/result.json."""

    pdf_path: Path
    md_path: Optional[Path] = None
    result_json_path: Optional[Path] = None
    relative: str = ""   # путь относительно корневой папки стадии
    source_filename: Optional[str] = None
    document_code: Optional[str] = None
    discipline: Optional[str] = None
    version_id: Optional[str] = None

    @property
    def match_name(self) -> str:
        """Стабильное имя документа, даже когда PDF канонично зовётся document.pdf."""
        return self.document_code or self.source_filename or self.pdf_path.name

    def to_dict(self) -> dict:
        filename = self.source_filename or self.pdf_path.name
        return {
            "pdf_path": str(self.pdf_path),
            "md_path": str(self.md_path) if self.md_path else None,
            "result_json_path": str(self.result_json_path) if self.result_json_path else None,
            "relative": self.relative,
            "filename": filename,
            "stem": Path(filename).stem,
            "has_md": self.md_path is not None,
            "has_result_json": self.result_json_path is not None,
            "document_code": self.document_code,
            "discipline": self.discipline,
            "version_id": self.version_id,
        }


@dataclass
class PdfPair:
    """Сопоставленная пара PDF (или left-only/right-only)."""

    left: Optional[PdfEntry] = None
    right: Optional[PdfEntry] = None
    match_score: float = 0.0
    status: str = "unmatched"  # matched | maybe | unmatched

    def to_dict(self) -> dict:
        return {
            "left": self.left.to_dict() if self.left else None,
            "right": self.right.to_dict() if self.right else None,
            "match_score": round(self.match_score, 3),
            "status": self.status,
        }


# ─── Поиск файлов в папке ────────────────────────────────────────────────

def _safe_iter(root: Path):
    """rglob по `*.pdf` с защитой от PermissionError.

    Важно: `Path.rglob("*.pdf")` матчит и файлы, и каталоги — поэтому
    папка-обёртка вида `Foo.pdf/` попадает в результаты и потом
    выглядит в UI как "лишняя" PDF-строка без MD/result.json. Отдаём
    только файлы.
    """
    try:
        for p in root.rglob("*.pdf"):
            try:
                if p.is_file():
                    yield p
            except OSError:
                continue
    except PermissionError:
        return


def _find_md_near(pdf_path: Path) -> Optional[Path]:
    """Поиск Markdown рядом с PDF.

    Порядок:
      1) same_basename.md
      2) document.md в той же папке
      3) любой ближайший .md в той же папке
    """
    folder = pdf_path.parent
    base_candidate = folder / (pdf_path.stem + ".md")
    if base_candidate.exists() and base_candidate.is_file():
        return base_candidate

    doc_candidate = folder / "document.md"
    if doc_candidate.exists() and doc_candidate.is_file():
        return doc_candidate

    # Любой .md в той же папке (без рекурсии)
    try:
        md_candidates = sorted(folder.glob("*.md"))
    except PermissionError:
        return None
    for cand in md_candidates:
        if cand.is_file():
            return cand
    return None


def _find_result_json_near(pdf_path: Path) -> Optional[Path]:
    """Поиск result.json рядом с PDF.

    Порядок:
      1) same_basename_result.json
      2) same_basename.result.json
      3) same_basename.json (если внутри pages[].blocks[])
      4) result.json в той же папке
      5) любой *result.json в той же папке
    """
    folder = pdf_path.parent
    stem = pdf_path.stem

    # 1) <stem>_result.json
    cand = folder / f"{stem}_result.json"
    if cand.exists() and cand.is_file():
        return cand

    # 2) <stem>.result.json
    cand = folder / f"{stem}.result.json"
    if cand.exists() and cand.is_file():
        return cand

    # 4) result.json
    cand = folder / "result.json"
    if cand.exists() and cand.is_file():
        return cand

    # 5) *result.json в этой же папке (без рекурсии)
    try:
        results = sorted(folder.glob("*result*.json"))
    except PermissionError:
        return None
    for r in results:
        if r.is_file() and not r.name.endswith("_annotation.json"):
            return r

    return None


def scan_stage_folder(folder: str | Path) -> tuple[list[PdfEntry], list[str]]:
    """Рекурсивно найти PDF в папке стадии. Возвращает (entries, warnings).

    Каждому PDF при возможности подставляются md_path и result_json_path
    из ближайшего окружения.
    """
    warnings: list[str] = []
    root = Path(folder).expanduser()
    if not root.exists():
        return [], [f"Папка не существует: {root}"]
    if not root.is_dir():
        return [], [f"Не папка: {root}"]

    if stage_storage.is_versioned_stage(root):
        entries = []
        for item in stage_storage.iter_current_documents(root):
            document = item["document"]
            document_code = str(document.get("document_code") or "") or None
            discipline = str(document.get("discipline") or "") or None
            source_filename = str(item.get("source_filename") or "") or None
            logical_filename = source_filename or f"{document_code or 'document'}.pdf"
            relative = str(
                Path("documents")
                / (document_code or Path(logical_filename).stem)
                / "versions"
                / str(item["version_id"])
                / logical_filename
            )
            md_path = item["md_path"]
            result_json_path = item["result_json_path"]
            entries.append(PdfEntry(
                pdf_path=item["pdf_path"],
                md_path=md_path if md_path.is_file() else None,
                result_json_path=result_json_path if result_json_path.is_file() else None,
                relative=relative,
                source_filename=source_filename,
                document_code=document_code,
                discipline=discipline,
                version_id=str(item["version_id"]),
            ))
        entries.sort(key=lambda entry: entry.relative.casefold())
        if not entries:
            warnings.append(f"В папке не найдено PDF: {root}")
        return entries, warnings

    entries: list[PdfEntry] = []
    for pdf in _safe_iter(root):
        try:
            relative = str(pdf.relative_to(root))
        except ValueError:
            relative = pdf.name
        entry = PdfEntry(
            pdf_path=pdf,
            md_path=_find_md_near(pdf),
            result_json_path=_find_result_json_near(pdf),
            relative=relative,
        )
        entries.append(entry)

    entries.sort(key=lambda e: e.relative.lower())
    if not entries:
        warnings.append(f"В папке не найдено PDF: {root}")
    return entries, warnings


# ─── Сопоставление PDF между стадиями ─────────────────────────────────────

def _build_candidate_pairs(
    left_entries: list[PdfEntry],
    right_entries: list[PdfEntry],
) -> list[tuple[int, int, float]]:
    """Все пары (i, j, score) ниже отсортированные по score desc."""
    rows: list[tuple[int, int, float]] = []
    left_norm = [_normalize_pdf_name(e.match_name) for e in left_entries]
    right_norm = [_normalize_pdf_name(e.match_name) for e in right_entries]
    for i, ln in enumerate(left_norm):
        for j, rn in enumerate(right_norm):
            sc = _similarity(ln, rn)
            if sc <= 0.0:
                continue
            rows.append((i, j, sc))
    rows.sort(key=lambda x: -x[2])
    return rows


def match_pdfs(
    left_entries: list[PdfEntry],
    right_entries: list[PdfEntry],
) -> list[PdfPair]:
    """Жадное сопоставление: лучший score, без повторного использования сторон.

    Сначала точные совпадения (>= EXACT_THRESHOLD), затем fuzzy
    (>= FUZZY_THRESHOLD). Что не сопоставилось — формируется как
    left-only / right-only.
    """
    pairs: list[PdfPair] = []
    used_left: set[int] = set()
    used_right: set[int] = set()

    candidates = _build_candidate_pairs(left_entries, right_entries)

    # Точные
    for i, j, sc in candidates:
        if sc < EXACT_THRESHOLD:
            break
        if i in used_left or j in used_right:
            continue
        pairs.append(PdfPair(
            left=left_entries[i], right=right_entries[j],
            match_score=sc, status="matched",
        ))
        used_left.add(i)
        used_right.add(j)

    # Fuzzy
    for i, j, sc in candidates:
        if sc < FUZZY_THRESHOLD:
            break
        if i in used_left or j in used_right:
            continue
        if sc >= EXACT_THRESHOLD:
            continue  # уже взяли выше
        pairs.append(PdfPair(
            left=left_entries[i], right=right_entries[j],
            match_score=sc, status="maybe",
        ))
        used_left.add(i)
        used_right.add(j)

    # Left-only
    for i, e in enumerate(left_entries):
        if i in used_left:
            continue
        pairs.append(PdfPair(left=e, right=None, match_score=0.0, status="unmatched"))

    # Right-only
    for j, e in enumerate(right_entries):
        if j in used_right:
            continue
        pairs.append(PdfPair(left=None, right=e, match_score=0.0, status="unmatched"))

    # Удобная сортировка: сначала matched, затем maybe, затем unmatched
    status_order = {"matched": 0, "maybe": 1, "unmatched": 2}
    pairs.sort(key=lambda p: (
        status_order.get(p.status, 9),
        -p.match_score,
        (p.left.relative if p.left else p.right.relative if p.right else ""),
    ))
    return pairs


__all__ = [
    "PdfEntry",
    "PdfPair",
    "scan_stage_folder",
    "match_pdfs",
    "EXACT_THRESHOLD",
    "FUZZY_THRESHOLD",
]
