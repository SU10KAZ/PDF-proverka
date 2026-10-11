#!/usr/bin/env python3
"""Phase 0 of the OpenRouter-leg replacement study: offline history analysis.

Read-only. For every projects_v2 version with an expert review, pick the run
whose ``03_findings.json`` the expert actually reviewed (verified by matching
``problem`` text against ``knowledge_base/decisions_log.json``), then classify
every reviewed finding by the Stage 01 legs that produced it.

The key bucket is ``gpt_only``: every detection came from the OpenRouter leg
(``openai/gpt-5.4``), the text stage did not find it, and the Codex legs were
healthy on all of its source blocks. Expert-accepted ``gpt_only`` findings are
the target set T of the study; rejected ones are the noise set R.

Nothing here calls a model. Output: ``findings.jsonl`` (one row per reviewed
finding), ``summary.json`` and ``targets.json`` in ``--out``.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import Counter, defaultdict
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
OBJECTS_DIR = ROOT / "projects_v2" / "objects"
DECISIONS_LOG = ROOT / "knowledge_base" / "decisions_log.json"

GPT_MODEL = "openai/gpt-5.4"
GPT_DETECTOR = "gpt_openrouter"
CODEX_DETECTOR = "codex"
TEXT_PREFIX = "T-"
MATCH_PREFIX_CHARS = 120


def _norm(text: Any) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip().lower()[:MATCH_PREFIX_CHARS]


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_decisions_log() -> dict[tuple[str, str, int, str], dict[str, Any]]:
    """Index KB entries by (object_id, document code, version_no, item_id)."""
    data = _load_json(DECISIONS_LOG) or {}
    index: dict[tuple[str, str, int, str], dict[str, Any]] = {}
    for entry in data.get("entries") or []:
        if entry.get("item_type") != "finding":
            continue
        match = re.fullmatch(r"v(\d+)", str(entry.get("current_version_id") or ""))
        if not match:
            continue
        key = (
            str(entry.get("object_id") or ""),
            str(entry.get("source_project") or ""),
            int(match.group(1)),
            str(entry.get("item_id") or ""),
        )
        index[key] = entry
    return index


def _version_no(document_json: dict[str, Any], version_id: str) -> int | None:
    for item in document_json.get("versions") or []:
        if item.get("version_id") == version_id:
            try:
                return int(item.get("version_no"))
            except (TypeError, ValueError):
                return None
    match = re.fullmatch(r"v(\d+)", version_id)
    return int(match.group(1)) if match else None


def _candidate_findings(version_dir: Path) -> list[tuple[str, Path]]:
    analysis = version_dir / "03_analysis"
    candidates = [
        (run.name, run / "03_findings.json")
        for run in sorted((analysis / "runs").glob("*"))
        if (run / "03_findings.json").is_file()
    ]
    latest = analysis / "latest" / "03_findings.json"
    if latest.is_file():
        candidates.append(("latest", latest))
    return candidates


def _pick_reviewed_run(
    version_dir: Path,
    decisions: dict[str, dict[str, Any]],
    kb_texts: dict[str, str],
) -> tuple[str, Path, list[dict[str, Any]], int, int] | None:
    """Return (run_name, path, findings, text_matches, id_matches) of the best run."""
    best = None
    for run_name, path in _candidate_findings(version_dir):
        data = _load_json(path)
        if not isinstance(data, dict):
            continue
        findings = [f for f in data.get("findings") or [] if isinstance(f, dict)]
        by_id = {str(f.get("id")): f for f in findings}
        id_matches = sum(1 for fid in decisions if fid in by_id)
        text_matches = sum(
            1
            for fid, text in kb_texts.items()
            if fid in by_id and text and _norm(by_id[fid].get("problem")) == text
        )
        # Prefer a concrete run over "latest" when scores tie; latest is a copy.
        score = (text_matches, id_matches, run_name != "latest")
        if best is None or score > best[0]:
            best = (score, run_name, path, findings, text_matches, id_matches)
    if best is None:
        return None
    _, run_name, path, findings, text_matches, id_matches = best
    if run_name == "latest":
        latest_sha = _sha(path)
        for other_name, other_path in _candidate_findings(version_dir):
            if other_name != "latest" and _sha(other_path) == latest_sha:
                run_name = other_name
                break
    return run_name, path, findings, text_matches, id_matches


RUN_ID_STAMP = re.compile(r"stage01-(\d{8})T(\d{6})Z")
RAW_DIR_STAMP = re.compile(r"(\d{8})_(\d{6})__")
STAMP_TOLERANCE_S = 300


def _stamp_seconds(date: str, time: str) -> int:
    return int(datetime.strptime(date + time, "%Y%m%d%H%M%S").timestamp())


def _stage01_raw_dir(
    version_dir: Path, run_name: str, findings: list[dict[str, Any]]
) -> Path | None:
    """Locate the Stage 01 raw folder that produced the reviewed findings.

    A reviewed run often reuses Stage 01 from an earlier run (resume of later
    stages), so its own folder has no raw block files. The detection
    ``run_id`` carries the Stage 01 start time; the raw folder with the closest
    stamp anywhere in the version is the producer.
    """
    raw_dirs = [
        p
        for p in (version_dir / "03_analysis" / "runs").glob("*/_stage01_findings_only_runs/*")
        if p.is_dir()
    ]
    if not raw_dirs:
        return None
    stamps: Counter[int] = Counter()
    for finding in findings:
        for detection in (finding.get("provenance") or {}).get("detections") or []:
            match = RUN_ID_STAMP.search(str(detection.get("run_id") or ""))
            if match:
                stamps[_stamp_seconds(*match.groups())] += 1
    if stamps:
        target = stamps.most_common(1)[0][0]
        best = None
        for raw_dir in raw_dirs:
            match = RAW_DIR_STAMP.match(raw_dir.name)
            if not match:
                continue
            delta = abs(_stamp_seconds(*match.groups()) - target)
            if delta <= STAMP_TOLERANCE_S and (best is None or delta < best[0]):
                best = (delta, raw_dir)
        if best is not None:
            return best[1]
    own = sorted(
        p
        for p in (version_dir / "03_analysis" / "runs" / run_name / "_stage01_findings_only_runs").glob("*")
        if p.is_dir()
    )
    return own[-1] if own else None


def _block_health(raw_dir: Path | None) -> tuple[dict[str, set[str]], set[str], str]:
    """Map block_id -> models that returned ok in the Stage 01 raw folder."""
    health: dict[str, set[str]] = {}
    legs: set[str] = set()
    stamp = ""
    for raw_dir in [raw_dir] if raw_dir is not None else []:
        stamp = raw_dir.name
        for block_file in raw_dir.glob("block_*.json"):
            data = _load_json(block_file)
            if not isinstance(data, dict):
                continue
            block_id = str(data.get("block_id") or "")
            ok_models = {
                str(item.get("model"))
                for item in (data.get("result") or {}).get("detector_results") or []
                if (item.get("result") or {}).get("ok")
            }
            health[block_id] = ok_models
            legs |= ok_models
    return health, legs, stamp


def classify(finding: dict[str, Any]) -> dict[str, Any]:
    provenance = finding.get("provenance") or {}
    detections = [d for d in provenance.get("detections") or [] if isinstance(d, dict)]
    detectors = {str(d.get("detector")) for d in detections}
    models = sorted({str(d.get("model")) for d in detections})
    modes = {str(d.get("mode")) for d in detections}
    sources = [str(s) for s in finding.get("source_finding_ids") or []]
    has_text = any(s.startswith(TEXT_PREFIX) for s in sources)
    if not detections:
        origin = "text_only" if has_text else "unattributed"
    elif detectors == {GPT_DETECTOR}:
        origin = "gpt_text" if has_text else "gpt_only"
    elif GPT_DETECTOR in detectors and CODEX_DETECTOR in detectors:
        origin = "both"
    elif detectors == {CODEX_DETECTOR}:
        gap = modes == {"gap_search"}
        if has_text:
            origin = "codex_text"
        else:
            origin = "codex_gap_only" if gap else "codex_only"
    else:
        origin = "other"
    return {
        "origin": origin,
        "detectors": sorted(detectors),
        "models": models,
        "modes": sorted(modes),
        "has_text_source": has_text,
    }


def analyse(out_dir: Path) -> dict[str, Any]:
    kb = load_decisions_log()
    rows: list[dict[str, Any]] = []
    versions_seen = versions_used = 0
    join_stats = Counter()
    for review_path in sorted(OBJECTS_DIR.glob("*/disciplines/*/documents/*/versions/*/04_review/expert_review.json")):
        version_dir = review_path.parents[1]
        parts = version_dir.relative_to(OBJECTS_DIR).parts
        object_folder, discipline, document, version = parts[0], parts[2], parts[4], parts[6]
        review = _load_json(review_path) or {}
        decisions = {
            str(d.get("item_id")): d
            for d in review.get("decisions") or []
            if isinstance(d, dict)
            and d.get("item_type", "finding") == "finding"
            and d.get("decision") in {"accepted", "rejected"}
        }
        if not decisions:
            continue
        versions_seen += 1
        object_json = _load_json(OBJECTS_DIR / object_folder / "object.json") or {}
        document_json = _load_json(version_dir.parents[1] / "document.json") or {}
        version_no = _version_no(document_json, version)
        object_id = str(object_json.get("object_id") or document_json.get("object_id") or "")
        kb_texts = {
            fid: _norm(kb[(object_id, document, version_no, fid)].get("summary"))
            for fid in decisions
            if (object_id, document, version_no, fid) in kb
        }
        picked = _pick_reviewed_run(version_dir, decisions, kb_texts)
        if picked is None:
            join_stats["no_findings_file"] += 1
            continue
        run_name, _, findings, text_matches, id_matches = picked
        raw_dir = _stage01_raw_dir(version_dir, run_name, findings)
        health, legs, stamp = _block_health(raw_dir)
        gpt_run = GPT_MODEL in legs
        if gpt_run:
            versions_used += 1
        by_id = {str(f.get("id")): f for f in findings}
        for fid, decision in decisions.items():
            finding = by_id.get(fid)
            if finding is None:
                join_stats["decision_without_finding"] += 1
                continue
            kb_text = kb_texts.get(fid)
            if kb_text is None:
                text_status = "no_kb_entry"
            elif kb_text == _norm(finding.get("problem")):
                text_status = "text_match"
            else:
                text_status = "text_mismatch"
            join_stats[text_status] += 1
            cls = classify(finding)
            source_blocks = [str(b) for b in finding.get("source_block_ids") or []]
            codex_legs = sorted(m for m in legs if m.startswith("codex/"))
            codex_healthy = bool(source_blocks) and all(
                all(m in health.get(b, set()) for m in codex_legs) for b in source_blocks
            ) if codex_legs else False
            gpt_healthy = bool(source_blocks) and all(
                GPT_MODEL in health.get(b, set()) for b in source_blocks
            )
            rows.append(
                {
                    "object": object_folder,
                    "discipline": discipline,
                    "document": document,
                    "version": version,
                    "run": run_name,
                    "stage01_stamp": stamp,
                    "run_legs": sorted(legs),
                    "gpt_run": gpt_run,
                    "finding_id": fid,
                    "decision": decision.get("decision"),
                    "rejection_reason": decision.get("rejection_reason") or "",
                    "carried_over": bool(decision.get("carried_over")),
                    "text_status": text_status,
                    "severity": finding.get("severity"),
                    "category": finding.get("category"),
                    "problem": finding.get("problem") or "",
                    "description": finding.get("description") or "",
                    "source_block_ids": source_blocks,
                    "source_finding_ids": finding.get("source_finding_ids") or [],
                    "evidence_types": sorted(
                        {str(e.get("type")) for e in finding.get("evidence") or [] if isinstance(e, dict)}
                    ),
                    "codex_healthy_on_sources": codex_healthy,
                    "gpt_healthy_on_sources": gpt_healthy,
                    **cls,
                }
            )
        join_stats["versions_with_text_matches"] += int(text_matches > 0)
        join_stats["versions_text_matches_total"] += text_matches
        join_stats["versions_id_matches_total"] += id_matches

    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / "findings.jsonl").open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    summary = summarise(rows)
    summary["versions_with_review"] = versions_seen
    summary["versions_reviewed_run_has_gpt"] = versions_used
    summary["join"] = dict(join_stats)
    (out_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    targets = [
        row
        for row in rows
        if row["gpt_run"]
        and row["origin"] == "gpt_only"
        and row["codex_healthy_on_sources"]
        and row["text_status"] != "text_mismatch"
    ]
    (out_dir / "targets.json").write_text(
        json.dumps(targets, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return summary


def _table(rows: list[dict[str, Any]], key) -> dict[str, dict[str, Any]]:
    table: dict[str, Counter] = defaultdict(Counter)
    for row in rows:
        table[key(row)][row["decision"]] += 1
    result = {}
    for name, counter in sorted(table.items(), key=lambda kv: -sum(kv[1].values())):
        total = counter["accepted"] + counter["rejected"]
        result[name] = {
            "accepted": counter["accepted"],
            "rejected": counter["rejected"],
            "acceptance": round(counter["accepted"] / total, 4) if total else None,
        }
    return result


def summarise(rows: list[dict[str, Any]]) -> dict[str, Any]:
    clean = [
        r for r in rows
        if r["gpt_run"] and r["text_status"] != "text_mismatch"
    ]
    gpt_only = [r for r in clean if r["origin"] == "gpt_only"]
    return {
        "rows_total": len(rows),
        "rows_gpt_runs_clean": len(clean),
        "by_origin": _table(clean, lambda r: r["origin"]),
        "gpt_only_by_codex_health": _table(
            gpt_only, lambda r: "codex_healthy" if r["codex_healthy_on_sources"] else "codex_missing"
        ),
        "gpt_only_by_codex_legs": _table(
            [r for r in gpt_only if r["codex_healthy_on_sources"]],
            lambda r: "+".join(m for m in r["run_legs"] if m.startswith("codex/")),
        ),
        "gpt_only_by_discipline": _table(
            [r for r in gpt_only if r["codex_healthy_on_sources"]], lambda r: r["discipline"]
        ),
        "gpt_only_by_object": _table(
            [r for r in gpt_only if r["codex_healthy_on_sources"]], lambda r: r["object"]
        ),
        "gpt_only_by_severity": _table(
            [r for r in gpt_only if r["codex_healthy_on_sources"]], lambda r: str(r["severity"])
        ),
        "eras": era_breakdown(clean),
    }


BLOCK_ORIGINS = ("gpt_only", "gpt_text", "both", "codex_only", "codex_gap_only", "codex_text")


def _era(row: dict[str, Any]) -> str:
    return "+".join(m for m in row["run_legs"] if m.startswith("codex/")) or "none"


def era_breakdown(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Accepted gpt_only findings per era (Codex side composition) and object.

    The object split matters: expert strictness differs per object, and the
    eras cover different objects, so only same-object comparisons are fair.
    """
    result: dict[str, Any] = {}
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row["origin"] == "gpt_only" and not row["codex_healthy_on_sources"]:
            continue
        groups[(_era(row), "ALL")].append(row)
        groups[(_era(row), row["object"])].append(row)
    for (era, obj), items in sorted(groups.items()):
        versions = {(r["object"], r["document"], r["version"]) for r in items}
        accepted = Counter(r["origin"] for r in items if r["decision"] == "accepted")
        rejected = Counter(r["origin"] for r in items if r["decision"] == "rejected")
        block_accepted = sum(accepted[o] for o in BLOCK_ORIGINS)
        result.setdefault(era, {})[obj] = {
            "versions": len(versions),
            "gpt_only_accepted": accepted["gpt_only"],
            "gpt_only_rejected": rejected["gpt_only"],
            "block_accepted": block_accepted,
            "gpt_only_accepted_per_version": round(accepted["gpt_only"] / len(versions), 3),
            "gpt_only_share_of_block_accepted": (
                round(accepted["gpt_only"] / block_accepted, 4) if block_accepted else None
            ),
        }
    return result


def scan_raw_runs() -> list[dict[str, Any]]:
    """Inventory every Stage 01 raw folder: blocks, legs, per-leg block results."""
    inventory = []
    for raw_dir in sorted(OBJECTS_DIR.glob(
        "*/disciplines/*/documents/*/versions/*/03_analysis/runs/*/_stage01_findings_only_runs/*"
    )):
        if not raw_dir.is_dir():
            continue
        blocks: dict[str, dict[str, dict[str, Any]]] = {}
        for block_file in raw_dir.glob("block_*.json"):
            data = _load_json(block_file)
            if not isinstance(data, dict):
                continue
            legs = {}
            for item in (data.get("result") or {}).get("detector_results") or []:
                res = item.get("result") or {}
                legs[str(item.get("model"))] = {
                    "ok": bool(res.get("ok")),
                    "from_cache": bool(res.get("from_cache")),
                    "findings": [
                        _raw_text(f)
                        for f in (res.get("parsed") or {}).get("findings") or []
                        if isinstance(f, dict)
                    ],
                    "input_tokens": res.get("input_tokens") or 0,
                    "output_tokens": res.get("output_tokens") or 0,
                    "reasoning_tokens": res.get("reasoning_tokens") or 0,
                    "elapsed_ms": res.get("elapsed_ms") or 0,
                }
            blocks[str(data.get("block_id") or "")] = legs
        parts = raw_dir.relative_to(OBJECTS_DIR).parts
        inventory.append(
            {
                "version_key": (parts[0], parts[2], parts[4], parts[6]),
                "stamp": raw_dir.name,
                "blocks": blocks,
            }
        )
    return inventory


def _raw_text(finding: dict[str, Any]) -> str:
    head = finding.get("finding") or finding.get("problem") or ""
    tail = finding.get("value_found") or ""
    return re.sub(r"\s+", " ", f"{head} {tail}").strip().lower()[:400]


def leg_profile(inventory: list[dict[str, Any]]) -> dict[str, Any]:
    """Per-leg verbosity and reasoning on blocks where every leg returned ok.

    Restricted to runs with the OpenRouter leg, so all legs saw the same block,
    prompt and context; the only difference is the model and the transport.
    """
    totals: dict[str, Counter] = defaultdict(Counter)
    for run in inventory:
        for legs in run["blocks"].values():
            if GPT_MODEL not in legs or not all(leg["ok"] for leg in legs.values()):
                continue
            for model, leg in legs.items():
                totals[model]["blocks"] += 1
                totals[model]["findings"] += len(leg["findings"])
                totals[model]["nonempty"] += int(bool(leg["findings"]))
                for key in ("input_tokens", "output_tokens", "reasoning_tokens", "elapsed_ms"):
                    totals[model][key] += leg[key]
    profile = {}
    for model, c in sorted(totals.items()):
        n = c["blocks"] or 1
        profile[model] = {
            "blocks": c["blocks"],
            "findings_per_block": round(c["findings"] / n, 3),
            "nonempty_share": round(c["nonempty"] / n, 4),
            "input_tokens_per_block": round(c["input_tokens"] / n),
            "output_tokens_per_block": round(c["output_tokens"] / n),
            "reasoning_tokens_per_block": round(c["reasoning_tokens"] / n),
            "seconds_per_block": round(c["elapsed_ms"] / n / 1000, 1),
        }
    return profile


def reproducibility(
    targets: list[dict[str, Any]], inventory: list[dict[str, Any]]
) -> dict[str, Any]:
    """How often a gpt_only finding reappears in another fresh run of the block.

    The reviewed run's raw GPT text is compared with raw texts of other runs on
    the same block (same style on both sides). Cached GPT responses
    (``from_cache``) are skipped: they replay the old answer verbatim.
    """
    by_version: dict[tuple, list[dict[str, Any]]] = defaultdict(list)
    for run in inventory:
        by_version[run["version_key"]].append(run)
    scores: dict[str, list[tuple[float, float]]] = defaultdict(list)
    skipped_cached = Counter()
    for target in targets:
        key = (target["object"], target["discipline"], target["document"], target["version"])
        runs = by_version.get(key) or []
        own = next((r for r in runs if r["stamp"] == target["stage01_stamp"]), None)
        if own is None:
            continue
        query = _norm(target["problem"])
        raw = None
        best = 0.0
        for block_id in target["source_block_ids"]:
            leg = (own["blocks"].get(block_id) or {}).get(GPT_MODEL) or {}
            for text in leg.get("findings") or []:
                score = SequenceMatcher(None, query[:200], text[:200]).ratio()
                if score > best:
                    best, raw = score, (block_id, text)
        if raw is None:
            continue
        block_id, raw_text = raw
        for run in runs:
            if run is own:
                continue
            legs = run["blocks"].get(block_id) or {}
            gpt = legs.get(GPT_MODEL)
            if not gpt or not gpt["ok"]:
                continue
            if gpt["from_cache"]:
                skipped_cached[target["decision"]] += 1
                continue
            gpt_score = max([SequenceMatcher(None, raw_text, t).ratio() for t in gpt["findings"]] + [0.0])
            other_score = max(
                [
                    SequenceMatcher(None, raw_text, t).ratio()
                    for model, leg in legs.items()
                    if model != GPT_MODEL and leg["ok"]
                    for t in leg["findings"]
                ]
                + [0.0]
            )
            scores[target["decision"]].append((gpt_score, other_score))
    result: dict[str, Any] = {"skipped_cached": dict(skipped_cached)}
    for decision, pairs in scores.items():
        n = len(pairs)
        result[decision] = {
            "rerun_pairs": n,
            "gpt_refound_share_ge_0_5": round(sum(g >= 0.5 for g, _ in pairs) / n, 3),
            "gpt_refound_share_ge_0_35": round(sum(g >= 0.35 for g, _ in pairs) / n, 3),
            "codex_found_share_ge_0_5": round(sum(o >= 0.5 for _, o in pairs) / n, 3),
            "codex_found_share_ge_0_35": round(sum(o >= 0.35 for _, o in pairs) / n, 3),
        }
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    summary = analyse(args.out)
    targets = json.loads((args.out / "targets.json").read_text(encoding="utf-8"))
    inventory = scan_raw_runs()
    summary["leg_profile"] = leg_profile(inventory)
    summary["reproducibility"] = reproducibility(targets, inventory)
    (args.out / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
