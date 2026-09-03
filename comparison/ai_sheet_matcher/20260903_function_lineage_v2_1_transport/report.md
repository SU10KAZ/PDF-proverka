# Function Lineage v2.1 — bounded selector transport

Candidate source: `2bcb832f51c46867c56d49d81549d9cac5918e96`; failed repeat record: `46e7a26e`.

Deterministic only: model calls `0`; production runs `0`; deploy `NO`; shadow `OFF`; materialization `NO`; Vision `NO`.

## Architecture

The full persisted candidate/passport/evidence artifacts remain verifier and forensic input. The model-facing projection contains only one LEFT function task, its complete ordered candidate set, candidate-owned RIGHT functions/fragments, and a deduplicated local evidence dictionary. Whole tasks are greedily packed in deterministic order to a 250,000-character target with a 350,000 hard gate.

## Corpus metrics

| Project | Tasks | Shards | Chars min / median / p95 / max | Max tasks/shard | Candidate edges | Evidence errors | Oversized |
|---|---:|---:|---:|---:|---:|---:|---:|
| ИОС 1.1 | 61 | 33 | 114804 / 199749 / 246919 / 247445 | 3 | 629/629 | 0 | 0 |
| ИОС 3.1 | 26 | 9 | 159816 / 215168 / 248881 / 248881 | 4 | 241/241 | 0 | 0 |
| ИОС 2.1 | 58 | 36 | 112465 / 199252.5 / 242932 / 246849 | 2 | 676/676 | 0 | 0 |

Candidate Recall remains R@1 `0.578947`, R@3 `0.684211`, R@5 `0.842105`, R@10 `0.947368`.

## IOS2.1 controls

- LEFT17 R27 rank 1 present: `True`.
- LEFT18 R24 rank 1 present: `True`.
- LEFT19 R30 rank 1 and R25 rank 2 in one task context: `True`.
- LEFT20 `lcand_9c617494b14c2b922d3f` [26,28,29] intact: `True`.

## Safety

Payloads over 350,000 chars: `0`; payloads over 250,000 target: `0`; capacity defects: `0`; RIGHT_MAP_CONFLICT: `0`.

## Verdict

**A — bounded selector transport готов к isolated AI repeat**
