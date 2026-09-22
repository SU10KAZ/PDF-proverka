#!/usr/bin/env python3
"""Read one versioned source and freeze its human review. Never runs a provider."""
from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from backend.app.services.project_change_v3.human_mapping_bridge import build_snapshot, BridgeError


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('object-id', 'comparison-id', 'pair-id', 'source-run-id'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--created-at', help='Optional fixed ISO timestamp for reproducible dry-runs')
    parser.add_argument('--output', type=Path, help='Exclusive-create snapshot outside production storage')
    args = parser.parse_args()
    try:
        # Output must not sit inside any comparison storage, particularly a source run.
        if args.output:
            from backend.app.services.stage_comparison.paths import comparison_root_path
            destination = args.output.resolve()
            if destination.is_relative_to(comparison_root_path().resolve()):
                raise BridgeError('OUTPUT_INSIDE_COMPARISON_STORAGE')
        snapshot = build_snapshot(object_id=args.object_id, comparison_id=args.comparison_id,
            pair_id=args.pair_id, source_run_id=args.source_run_id, created_at=args.created_at)
        if args.output:
            snapshot.write(args.output)
        print(json.dumps({'status': 'PASS', 'model_calls': 0, 'bridge_snapshot_sha256': snapshot.sha256,
                          'snapshot': snapshot.value()}, ensure_ascii=False, indent=2))
        return 0
    except (BridgeError, OSError) as exc:
        print(json.dumps({'status': 'FAIL', 'model_calls': 0,
                          'conflicts': [{'code': getattr(exc, 'code', 'BRIDGE_IO_ERROR'),
                                         'reason': getattr(exc, 'reason', str(exc))}]}))
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
