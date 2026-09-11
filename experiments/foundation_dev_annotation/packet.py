"""Read-only presentation of the frozen packet, with an allowlisted PDF map."""
from pathlib import Path
import hashlib
import json
import re

import fitz

NAMESPACE = "FOUNDATION_V3_DEV_WAVE1"
PACKET_SHA256 = "0ac9497509ff41652a045af0089ef1f4c118c3df720608905eb0f11068910819"
DOCUMENTS_SHA256 = "bfd27ff9561f7dee42987616eabf830041c43914c84a587953b447bd5ddd338b"
FROZEN_ROOT = Path("/home/coder/auditmanager/corpus-audits/20260911_semantic_foundation_v3")
QUESTIONS = {
    "SECTION": ("Это один и тот же смысловой раздел?", "Да, это один раздел", "Нет, здесь начинается другой"),
    "TABLE": ("Это одна и та же таблица, которая продолжается?", "Да, это та же таблица", "Нет, это другая таблица"),
    "OWNER": ("Этот фрагмент относится к показанному разделу или таблице?", "Да", "Нет"),
}
REASONS = {
    "WRONG_FRAGMENTS": "Неправильно выделены фрагменты",
    "WRONG_SOURCE": "Показаны не те документы/страницы",
    "MISSING_FRAGMENT": "Нужный фрагмент есть в PDF, но не показан",
    "OTHER": "Другое",
}


def sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def verified_json(path, expected):
    raw = Path(path).read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected:
        raise ValueError("Frozen input hash mismatch")
    return json.loads(raw)


def is_content(line):
    return bool(line.strip()) and not line.startswith(("### BLOCK", "> **", "<!--", "---")) and not re.match(r"^## (?:Page|СТРАНИЦА) \d+\s*$", line)


class Packet:
    def __init__(self, root=FROZEN_ROOT):
        self.root = Path(root)
        self.sha256 = PACKET_SHA256
        self.packet = verified_json(self.root / "reports/FIRST_WAVE_DEV_PACKET.json", self.sha256)
        documents = verified_json(self.root / "dev_documents.json", DOCUMENTS_SHA256)
        self.cases = [c for c in self.packet["cases"] if c["annotation_mode"] == "HUMAN"]
        self.controls = [c for c in self.packet["cases"] if c["annotation_mode"] == "AUTOMATIC_CONTROL"]
        if (len(self.packet["cases"]), len(self.cases), len(self.controls)) != (126, 104, 22):
            raise ValueError("Unexpected packet counts")
        self.by_id = {c["case_id"]: c for c in self.cases}
        self.sources, self.presentations = {}, []
        docs = {d["document_version"]: d for d in documents}
        lines_by_doc = {}
        for c in self.cases:
            d = docs[c["document_version"]]
            pdf, md = d["artifacts"]["pdf"], d["artifacts"]["work_md"]
            source_id = "src_" + pdf["sha256"]
            if source_id not in self.sources:
                # Serve only the pre-existing verified DEV snapshot, never a URL path.
                path = self.root / "dev_namespace/pdf" / (pdf["sha256"] + ".pdf")
                if sha(path) != pdf["sha256"] or sha(md["path"]) != md["sha256"]:
                    raise ValueError("Frozen source hash mismatch")
                with fitz.open(path) as document:
                    pages = len(document)
                self.sources[source_id] = {"path": path, "pages": pages, "sha256": pdf["sha256"]}
                lines_by_doc[c["document_version"]] = Path(md["path"]).read_text().splitlines()
            lines = lines_by_doc[c["document_version"]]
            panels = []
            for side, a in enumerate(c["anchors"]):
                pos = a["markdown_line"] - 1
                if hashlib.sha256(lines[pos].encode()).hexdigest() != a["line_sha256"]:
                    raise ValueError("Frozen source anchor mismatch")
                if not 1 <= a["page"] <= self.sources[source_id]["pages"]:
                    raise ValueError("Physical PDF page unavailable")
                page_marks = [(i, int(m[1])) for i, line in enumerate(lines)
                              if (m := re.match(r"^## (?:Page|СТРАНИЦА) (\d+)\s*$", line))]
                starts = [i for i, page in page_marks if page == a["page"]]
                if len(starts) != 1 or starts[0] >= pos:
                    raise ValueError("Physical source page mismatch")
                begin = starts[0] + 1
                end = next((i for i, _ in page_marks if i > starts[0]), len(lines))
                if pos >= end:
                    raise ValueError("Anchor is outside its source page")
                content = [{"text": line, "focus": i == pos} for i, line in enumerate(lines[begin:end], begin) if is_content(line)]
                focus = next(i for i, row in enumerate(content) if row["focus"])
                lo, hi = max(0, focus - 10), min(len(content), focus + 16)
                if c["kind"] == "TABLE":
                    start_line, end_line = c["source_segment_spans"][side]["markdown_line_span"]
                    indexes = [i for i in range(begin, end) if is_content(lines[i])]
                    lo = min(lo, next((j for j, i in enumerate(indexes) if i + 1 >= start_line), focus))
                    hi = max(hi, max((j + 1 for j, i in enumerate(indexes) if i + 1 <= end_line), default=focus + 1))
                elif c["kind"] == "OWNER":
                    if side == 1:
                        # The candidate owner must be the prominent right fragment,
                        # rather than another copy of the source table above it.
                        lo = focus
                    else:
                        candidate_line = c["anchors"][1]["markdown_line"] - 1
                        hi = sum(is_content(lines[i]) for i in range(begin, min(candidate_line, end)))
                heading = next((re.sub(r"^#+\s*", "", line) for line in reversed(lines[:pos])
                                if re.match(r"^#{4,}\s+", line)), None)
                panels.append({"title": ("Исходный фрагмент" if side == 0 else "Показанный раздел") if c["kind"] == "OWNER" else ("Левый фрагмент" if side == 0 else "Правый фрагмент"),
                               "page": a["page"], "pdf_url": f"/pdf/{source_id}#page={a['page']}",
                               "image_url": f"/page/{source_id}/{a['page']}.png", "context": content[lo:hi],
                               "full_context": content, "preceding_heading": heading if side == 0 and c["kind"] == "OWNER" else None})
            question, yes, no = QUESTIONS[c["kind"]]
            self.presentations.append({"case_id": c["case_id"], "question": question, "yes": yes, "no": no,
                                       "document": d["document_code"], "panels": panels,
                                       "diagnostics": {"kind": c["kind"], "stratum": c["stratum"], "anchors": c["anchors"],
                                                       "owner_candidate": "FOLLOWING_HEADING" if c["kind"] == "OWNER" else None}})

    def mapped_answer(self, case_id, answer):
        if answer == "BROKEN_CASE":
            return None
        if answer == "UNSURE":
            return "UNSURE"
        if self.by_id[case_id]["kind"] == "OWNER":
            return "OWNER_FOLLOWING_HEADING" if answer == "YES" else "OWNER_PRECEDING_SECTION"
        return "SAME" if answer == "YES" else "NEW"

    def page_image(self, source_id, page_number):
        source = self.sources.get(source_id)
        if source is None or not 1 <= page_number <= source["pages"]:
            raise KeyError("Unknown PDF page")
        with fitz.open(source["path"]) as pdf:
            page = pdf[page_number - 1]
            scale = min(2, 1800 / max(page.rect.width, page.rect.height))
            return page.get_pixmap(matrix=fitz.Matrix(scale, scale), alpha=False).tobytes("png")
