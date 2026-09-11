"""Read-only integrity audit; writes only a new isolated readiness report."""
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen
import argparse
import json
import subprocess

from .packet import FROZEN_ROOT, PACKET_SHA256, Packet, sha
from .store import DEFAULT_STATE, atomic_write, encoded


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=DEFAULT_STATE.parent / "READINESS.json")
    args = parser.parse_args()
    baseline = json.loads(args.baseline.read_text())
    changed = [name for name, digest in baseline["files"].items() if not Path(name).is_file() or sha(name) != digest]
    if changed:
        raise ValueError(f"Protected inputs changed: {changed}")
    packet = Packet()
    separation = json.loads((FROZEN_ROOT / "reports/DEV_EVAL_SEPARATION.json").read_text())
    if not separation["pass"] or any(separation[name] for name in ("version_overlap", "document_code_overlap", "source_hash_overlap", "page_hash_overlap")):
        raise ValueError("DEV/EVAL overlap")
    for receipt in separation["proof_inputs"]:
        if sha(receipt["path"]) != receipt["sha256"]:
            raise ValueError("DEV/EVAL proof input changed")
    frozen_code = json.loads((FROZEN_ROOT / "reports/FOUNDATION_FREEZE.json").read_text())
    for receipt in [*frozen_code["code"], *packet.packet["selection_code"]]:
        if sha(receipt["path"]) != receipt["sha256"]:
            raise ValueError("Frozen Foundation/selection code changed")
    origin = subprocess.check_output(["git", "rev-parse", "origin/main"], text=True).strip()
    production = str(Path("/home/coder/auditmanager/current").resolve())
    if origin != baseline["origin_main"] or production != baseline["production"]:
        raise ValueError("Origin or production release changed")
    with urlopen("http://127.0.0.1:8768/api/bootstrap", timeout=10) as response:
        live = json.load(response)
    with urlopen("http://127.0.0.1:8768/api/export", timeout=10) as response:
        exported = json.load(response)
    if live["packet_sha256"] != PACKET_SHA256 or len(live["cases"]) != 104 or len(exported["cases"]) != 104:
        raise ValueError("Live service packet mismatch")
    report = {"status": "READY FOR SIMPLE DEV ANNOTATION", "checked_at": datetime.now(timezone.utc).isoformat(),
              "url": "http://127.0.0.1:8768/", "namespace": live["namespace"], "packet_sha256": PACKET_SHA256,
              "packet": 126, "human_questions": 104, "automatic_controls": 22,
              "progress": live["progress"], "annotator": live["annotator"],
              "primary_choices": ["YES", "NO", "UNSURE"], "broken_case": "SUPPORTED",
              "protected_files_verified": len(baseline["files"]), "protected_changes": changed,
              "frozen_code_receipts_verified": len(frozen_code["code"]) + len(packet.packet["selection_code"]),
              "dev_eval_overlap": 0, "packet_sha_unchanged": True, "existing_eval_truth_modified": False,
              "foundation_modified": False, "production_modified": False, "push": 0, "deploy": 0,
              "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
              "source_pdfs_verified": len(packet.sources), "human_answers_generated_by_audit": 0,
              "baseline_sha256": sha(args.baseline)}
    atomic_write(args.output, encoded(report))
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
