"""Canonical deterministic artifacts; provenance never participates in semantic keys."""
import hashlib
import json
import re
from pathlib import Path


def canonical(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def file_hash(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical(value))


def read(path):
    return json.loads(Path(path).read_text())


def norm(text):
    text = text.casefold().replace("ё", "е").replace("²", "2").replace("³", "3")
    text = re.sub(r"[*_#]", "", text)
    return re.sub(r"\s+", " ", text).strip()


def words(text):
    return set(re.findall(r"[а-яa-z0-9]+", norm(text)))


def similarity(a, b):
    a, b = words(a), words(b)
    return len(a & b) / len(a | b) if a or b else 0.0


def section_text(section):
    return "\n".join(b["text"] for b in section["ordered_text_blocks"])
