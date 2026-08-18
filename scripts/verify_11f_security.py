#!/usr/bin/env python3
"""Этап 11F — проверки безопасности ПОСЛЕ боевого прогона.

Запускается НА ВОРКЕРЕ: всё, что проверяется, там и лежит.

Три независимых вопроса, и ни один не решается обещанием:

  1. канарейка — появилась ли где-нибудь строка из файла, лежащего ВНЕ каталога
     попытки. Её появление означало бы, что модель добралась до файловой
     системы, чего у неё нет по построению;
  2. учётные данные — не просочились ли они в argv, окружение, журналы,
     привязку, разрешение, пакет результата;
  3. клиентские данные — не уехало ли содержимое документа туда, где ему не
     место (отчёты о прогоне, манифест, метаданные).

Секретные строки НЕ печатаются: выводится только факт совпадения и место.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import tarfile
from pathlib import Path
from typing import Any, Optional

#: Шаблоны, похожие на учётные данные. Ищутся ПО СОДЕРЖИМОМУ, а не по именам:
#: файл с безобидным именем — самый частый носитель.
_SECRET_PATTERNS = [
    ("openai/anthropic key", re.compile(r"\bsk-[A-Za-z0-9_\-]{16,}")),
    ("github token", re.compile(r"\bgh[pousr]_[A-Za-z0-9]{16,}")),
    ("slack token", re.compile(r"\bxox[baprs]-[A-Za-z0-9\-]{10,}")),
    ("jwt", re.compile(r"\beyJ[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}")),
    ("bearer", re.compile(r"(?i)\bauthorization:\s*bearer\s+\S{12,}")),
    ("worker token", re.compile(r"(?i)\b(worker_token|execution_token|claim_secret)\b\s*[:=]\s*\S{8,}")),
    ("private key", re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")),
]

#: Каталоги, которые проверяются на утечку. `inference/` включён намеренно: это
#: единственное место, где живут полные ответы модели, и надо знать, что там
#: НЕТ учётных данных, даже если есть клиентские. `project/` — дерево версии,
#: куда конвейер пишет артефакты аудита: если бы модель как-то дотянулась до
#: канарейки, след остался бы именно здесь.
_SCAN_DIRS = ("metadata", "result", "logs", "usage", "work", "inference", "project")

#: Единственное место, где строка канарейки лежит ЗАКОНОМЕРНО: файл привязки
#: провайдера хранит её как контрольное значение — она передаётся прогону
#: параметром `--forbidden-literal` именно для того, чтобы адаптер мог отвергать
#: ответы модели с этой строкой. Совпадение здесь — устройство проверки, а не
#: утечка. Любое ДРУГОЕ место означало бы, что строка куда-то утекла.
_CANARY_EXPECTED = ("metadata/provider_binding.json",)


def scan_text(text: str) -> list[str]:
    hits = []
    for name, pattern in _SECRET_PATTERNS:
        if pattern.search(text):
            hits.append(name)
    return hits


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="11F — проверки безопасности после прогона")
    parser.add_argument("--job-dir", required=True, type=Path)
    parser.add_argument("--canary-file", required=True, type=Path)
    parser.add_argument("--result-package", type=Path, default=None)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args(argv)

    job_dir = args.job_dir.resolve()
    canary_line = ""
    for line in args.canary_file.read_text(encoding="utf-8").splitlines():
        if line.strip():
            canary_line = line.strip()
            break
    if not canary_line:
        raise SystemExit("канарейка пуста — проверять нечем")

    report: dict[str, Any] = {
        "stage": "11F",
        "job_dir": str(job_dir),
        "canary": {
            "file": str(args.canary_file),
            "outside_attempt_dir": not str(args.canary_file.resolve()).startswith(str(job_dir)),
            "length": len(canary_line),
            "expected_locations": list(_CANARY_EXPECTED),
            "hits": [],
            "unexpected_hits": [],
        },
        "credentials": {"hits": [], "files_scanned": 0},
        "client_data": {},
    }

    # ── 1. Канарейка и учётные данные по всему каталогу попытки ────────────
    scanned = 0
    for sub in _SCAN_DIRS:
        root = job_dir / sub
        if not root.is_dir():
            continue
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            try:
                text = path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            scanned += 1
            if canary_line in text:
                report["canary"]["hits"].append(str(path.relative_to(job_dir)))
            for name in scan_text(text):
                report["credentials"]["hits"].append(
                    {"file": str(path.relative_to(job_dir)), "pattern": name}
                )
    report["credentials"]["files_scanned"] = scanned
    report["canary"]["unexpected_hits"] = [
        hit for hit in report["canary"]["hits"] if hit not in _CANARY_EXPECTED
    ]

    # ── 2. Пакет результата отдельно: он УЕЗЖАЕТ ───────────────────────────
    if args.result_package and args.result_package.is_file():
        pkg: dict[str, Any] = {"canary_hits": [], "credential_hits": [], "entries": 0}
        with tarfile.open(args.result_package) as tar:
            for member in tar.getmembers():
                if not member.isfile():
                    continue
                pkg["entries"] += 1
                fh = tar.extractfile(member)
                if fh is None:
                    continue
                blob = fh.read()
                text = blob.decode("utf-8", errors="replace")
                if canary_line in text:
                    pkg["canary_hits"].append(member.name)
                for name in scan_text(text):
                    pkg["credential_hits"].append({"file": member.name, "pattern": name})
        report["result_package"] = pkg

    # ── 3. Клиентские данные в отчётах о прогоне ───────────────────────────
    md_sample = ""
    for candidate in job_dir.rglob("02_work/document.md"):
        md_sample = candidate.read_text(encoding="utf-8", errors="replace")
        break
    # Берём длинную характерную строку документа: короткая совпала бы случайно.
    probe = ""
    for line in md_sample.splitlines():
        stripped = line.strip()
        if len(stripped) >= 60:
            probe = stripped
            break
    leaks: list[str] = []
    if probe:
        for name in (
            "metadata/11F_RUN.json",
            "result/audit_manifest.json",
        ):
            path = job_dir / name
            if path.is_file() and probe in path.read_text(encoding="utf-8", errors="replace"):
                leaks.append(name)
        for path in job_dir.rglob("*_provider_run.json"):
            if probe in path.read_text(encoding="utf-8", errors="replace"):
                leaks.append(str(path.relative_to(job_dir)))
    report["client_data"] = {
        "probe_found_in_md": bool(probe),
        "probe_chars": len(probe),
        "reports_containing_document_text": leaks,
        "ok": not leaks,
    }

    report["verdict"] = (
        "PASS"
        if not report["canary"]["unexpected_hits"]
        and not report["credentials"]["hits"]
        and not (report.get("result_package") or {}).get("canary_hits")
        and not (report.get("result_package") or {}).get("credential_hits")
        and report["client_data"]["ok"]
        else "FAIL"
    )
    out = args.out or (job_dir / "metadata" / "11F_SECURITY.json")
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2)[:2500])
    return 0 if report["verdict"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
