"""Targeted numeric/symbol witness retrieval. Never replaces Markdown text."""
from .common import file_hash, norm


def recover(document, facts, max_requests=12):
    requests = [f for f in facts if f.get("native_recovery_request")]
    if not requests:
        return []
    receipt = document["artifacts"].get("pdf")
    if not receipt or file_hash(receipt["path"]) != receipt["sha256"]:
        return [{"fact_id": f["fact_id"], "status": "REVIEW", "reason": "MISSING_OR_CHANGED_NATIVE_PDF"} for f in requests]
    import fitz
    import re
    result = []
    with fitz.open(receipt["path"]) as pdf:
        for f in requests:
            row = {"fact_id": f["fact_id"], "status": "REVIEW", "source_pdf_sha256": receipt["sha256"],
                   "request": f["native_recovery_request"], "witnesses": [], "markdown_replaced": False}
            if len(result) >= max_requests:
                row["reason"] = "TARGETED_RECOVERY_BUDGET"
                result.append(row)
                continue
            wanted = set(re.findall(r"[а-яa-z]{4,}", norm(f["quote"])))
            for n in sorted({r["page"] for r in f["source_refs"]}):
                if not 1 <= n <= len(pdf):
                    continue
                # A hit is only a witness near matching narrative words, not a
                # replacement or extracted table fact. Unlocated content abstains.
                lines = pdf[n - 1].get_text("text").splitlines()
                for i in range(len(lines)):
                    chunk = " ".join(lines[max(0, i - 1):i + 2])
                    seen = set(re.findall(r"[а-яa-z]{4,}", norm(chunk)))
                    if len(wanted & seen) >= min(3, max(2, len(wanted))) and re.search(r"\d", chunk):
                        row["witnesses"].append({"page": n, "native_quote": chunk[:700], "native_line": i + 1})
                        if len(row["witnesses"]) >= 3:
                            break
                if len(row["witnesses"]) >= 3:
                    break
            row["reason"] = "NATIVE_LOCAL_WITNESS_REQUIRES_REVIEW" if row["witnesses"] else "NO_UNIQUE_LOCAL_NATIVE_WITNESS"
            result.append(row)
    return result
