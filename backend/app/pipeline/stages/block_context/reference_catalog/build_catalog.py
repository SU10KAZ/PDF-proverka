#!/usr/bin/env python3
"""Одноразовая компиляция исследовательского корпуса в production-каталог.

Скрипт нужен только при осознанном обновлении эталонов. Runtime никогда его не
вызывает и не читает ``--source``. В production попадают компактные семантические
примеры и сигнатуры графов, а не исходные PDF и промежуточные файлы экспериментов.
"""
from __future__ import annotations

import argparse
import collections
import hashlib
import json
import re
from datetime import date
from pathlib import Path
from typing import Any


DISCIPLINE_FILES = {
    "АР": "AR.json",
    "ВК": "VK.json",
    "ГП": "GP.json",
    "КЖ": "KJ.json",
    "КМ": "KM.json",
    "ОВ": "HVAC.json",
    "СС": "SS.json",
    "ТХ": "TX.json",
    "ЭОМ": "EOM.json",
}

PROFILE_DOCS = {
    "АР": ["AR_PROFILES.md"],
    "ВК": ["VK_PROFILES.md"],
    "ГП": ["GP_PROFILES.md"],
    "КЖ": ["KJ_PROFILES.md"],
    "КМ": ["KM_PROFILES.md"],
    "ОВ": ["HVAC_PROFILES.md"],
    "СС": ["ALIA_SCHEME_PROFILES.md", "ALIA_REMAINING_PROFILES.md", "STRUCTURAL_PROFILES.md"],
    "ТХ": ["TX_PROFILES.md"],
    "ЭОМ": ["EOM_PROFILES.md"],
}


def _read(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        rows = payload.get("records") or payload.get("blocks") or []
        return [row for row in rows if isinstance(row, dict)]
    return []


def _description(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        return " ".join(
            str(value.get(key) or "").strip()
            for key in ("short_description", "content_summary", "description", "summary", "title")
            if str(value.get(key) or "").strip()
        )
    return ""


def _record_score(record: dict[str, Any]) -> tuple[int, int, int]:
    missed = covered = evidence = 0
    for value in (record.get("categories") or {}).values():
        if not isinstance(value, dict):
            continue
        missed += len(value.get("missed") or [])
        covered += len(value.get("covered") or [])
        evidence += len(value.get("source") or [])
    text_layer = record.get("source_layer_state") == "text_available"
    return (1 if text_layer and missed == 0 else 0, covered, evidence)


def _signature(graph: Any) -> dict[str, Any]:
    if not isinstance(graph, dict):
        return {}
    node_types = collections.Counter(
        str(node.get("node_type") or "")
        for node in graph.get("nodes") or []
        if isinstance(node, dict) and node.get("node_type")
    )
    counts: dict[str, float | int] = {
        "nodes_total": len(graph.get("nodes") or []),
        "containers_total": len(graph.get("containers") or []),
        "networks_total": len(graph.get("networks") or []),
        "edges_total": len(graph.get("edges") or []),
    }
    for key, value in (graph.get("validation") or {}).items():
        if (str(key).endswith("_total") or str(key).endswith("_segments_total")) \
                and isinstance(value, (int, float)):
            counts[str(key)] = max(0, value)
    if not node_types and not any(float(value) > 0 for value in counts.values()):
        return {}
    return {"node_types": dict(sorted(node_types.items())), "counts": counts}


def _structure_index(source: Path) -> dict[tuple[str, str], dict[str, Any]]:
    result: dict[tuple[str, str], dict[str, Any]] = {}
    for path in sorted(source.glob("*/*_out/*.structure.json")):
        key = (path.parent.parent.name, path.name.removesuffix(".structure.json"))
        if key in result:
            continue
        try:
            result[key] = _signature(_read(path))
        except (OSError, json.JSONDecodeError):
            continue
    return result


def _write(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False) + "\n",
        encoding="utf-8",
    )


def build(source: Path, target: Path, catalog_version: str) -> dict[str, Any]:
    source, target = source.resolve(), target.resolve()
    signatures = _structure_index(source)
    manifest_disciplines: dict[str, Any] = {}
    total = 0
    all_profiles: set[tuple[str, str]] = set()
    source_hash = hashlib.sha256()
    for discipline, filename in DISCIPLINE_FILES.items():
        corpora = sorted((source / discipline).glob("*_DIVERSE_CORPUS.json"))
        audits = sorted((source / discipline).glob("*_SEMANTIC_COVERAGE.json"))
        if len(corpora) != 1 or len(audits) != 1:
            raise RuntimeError(f"{discipline}: ожидался один corpus и один coverage")
        corpus_path, audit_path = corpora[0], audits[0]
        corpus_bytes, audit_bytes = corpus_path.read_bytes(), audit_path.read_bytes()
        source_hash.update(corpus_bytes); source_hash.update(audit_bytes)
        corpus = _records(json.loads(corpus_bytes))
        audit_rows = _records(json.loads(audit_bytes))
        audit = {str(row.get("block_id")): row for row in audit_rows if row.get("block_id")}
        rows = []
        for item in corpus:
            block_id = str(item.get("block_id") or "").strip()
            profile_id = str(item.get("profile_id") or "").strip()
            if not block_id or not profile_id:
                continue
            coverage = audit.get(block_id) or {}
            quality = _record_score(coverage) if coverage else (
                1 if item.get("text_characters") else 0,
                0,
                int(item.get("text_characters") or 0),
            )
            description = " ".join(
                part for part in (
                    _description(item.get("description")),
                    _description(item.get("ocr_preview")),
                    _description(item.get("sheet_name")),
                ) if part
            )
            rows.append({
                "block_id": block_id,
                "discipline": discipline,
                "profile_id": profile_id,
                "subtype": str(item.get("subtype") or "").strip(),
                "title": str(item.get("sheet_name") or "").strip(),
                "description": description,
                "source_page": item.get("source_page") or item.get("page"),
                "source_layer_state": str(
                    coverage.get("source_layer_state")
                    or ("text_available" if item.get("text_characters") else "unknown")
                ),
                "quality": list(quality),
                "covered_facts": quality[1],
                "structure_signature": signatures.get((discipline, block_id), {}),
            })
            all_profiles.add((discipline, profile_id))
        rows.sort(key=lambda row: (row["profile_id"], row["block_id"]))
        output = {
            "schema_version": 1,
            "catalog_version": catalog_version,
            "discipline": discipline,
            "records": rows,
        }
        rel = f"disciplines/{filename}"
        _write(target / rel, output)
        profile_count = len({row["profile_id"] for row in rows})
        signature_count = sum(bool(row["structure_signature"]) for row in rows)
        manifest_disciplines[discipline] = {
            "file": rel,
            "profile_document": f"profiles/{filename.removesuffix('.json')}.md",
            "records": len(rows),
            "profiles": profile_count,
            "structure_examples": signature_count,
        }
        total += len(rows)

        doc_parts = [f"# Профили дисциплины {discipline}\n"]
        for doc_name in PROFILE_DOCS[discipline]:
            doc_path = source / discipline / doc_name
            if doc_path.is_file():
                source_hash.update(doc_path.read_bytes())
                profile_text = doc_path.read_text(encoding="utf-8").strip()
                # Исследовательские команды запуска не являются частью production-правил.
                profile_text = re.sub(
                    r'^python\s+["\']?experiments/.*$',
                    '',
                    profile_text,
                    flags=re.MULTILINE,
                )
                doc_parts.append(profile_text.strip())
        (target / "profiles").mkdir(parents=True, exist_ok=True)
        (target / f"profiles/{filename.removesuffix('.json')}.md").write_text(
            "\n\n".join(doc_parts).strip() + "\n", encoding="utf-8"
        )

    manifest = {
        "schema_version": 1,
        "catalog_version": catalog_version,
        "generated_on": date.today().isoformat(),
        "records_total": total,
        "profiles_total": len(all_profiles),
        "source_digest_sha256": source_hash.hexdigest(),
        "runtime_dependency_on_experiments": False,
        "disciplines": manifest_disciplines,
    }
    _write(target / "manifest.json", manifest)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--target", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument("--catalog-version", default="2026.07.13-1")
    args = parser.parse_args()
    manifest = build(args.source, args.target, args.catalog_version)
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
