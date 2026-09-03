# Function Lineage v2.2.1 — IOS2.1 provider-safe critical AI smoke

Frozen compact transport: `67b9f4e43067590d952d805f72c590c30fce1375`. Model: `gpt-5.6-sol/low`.

Exactly four frozen task contexts; three cold repeats; Pass A/B; no majority vote.

| Task | Candidate count | Cold 1 A/B | Cold 2 A/B | Cold 3 A/B | Distribution | Stable repeats | Verifier | Capacity | Schema failures¹ | Model failures¹ |
|---|---:|---|---|---|---|---:|---|---|---:|---:|
| LEFT17 | 12 | lcand_cd6c87ed7f043a937b27 / lcand_cd6c87ed7f043a937b27 | lcand_cd6c87ed7f043a937b27 / lcand_cd6c87ed7f043a937b27 | lcand_cd6c87ed7f043a937b27 / lcand_cd6c87ed7f043a937b27 | `{"lcand_cd6c87ed7f043a937b27": 6}` | 3/3 | PASS | PASS | 0 | 0 |
| LEFT18 | 7 | lcand_d9f1abdb7469869363ad / lcand_d9f1abdb7469869363ad | lcand_d9f1abdb7469869363ad / lcand_d9f1abdb7469869363ad | lcand_d9f1abdb7469869363ad / lcand_d9f1abdb7469869363ad | `{"lcand_d9f1abdb7469869363ad": 6}` | 3/3 | PASS | PASS | 0 | 0 |
| LEFT19 | 12 | lcand_26bcd544f168ff9ccea5 / lcand_26bcd544f168ff9ccea5 | lcand_26bcd544f168ff9ccea5 / lcand_26bcd544f168ff9ccea5 | lcand_26bcd544f168ff9ccea5 / lcand_26bcd544f168ff9ccea5 | `{"lcand_26bcd544f168ff9ccea5": 6}` | 3/3 | PASS | PASS | 0 | 0 |
| LEFT20 | 12 | lcand_1d1f175a30c34b88c6e0 / lcand_9c617494b14c2b922d3f | lcand_9c617494b14c2b922d3f / lcand_9c617494b14c2b922d3f | lcand_9c617494b14c2b922d3f / lcand_9c617494b14c2b922d3f | `{"lcand_1d1f175a30c34b88c6e0": 1, "lcand_9c617494b14c2b922d3f": 5}` | 2/3 | PASS | PASS | 0 | 0 |

¹ Per-task affected observations; request-level failure counters below are not derived by summing these columns.

## LEFT20 distributed candidate

Candidate `lcand_9c617494b14c2b922d3f`; RIGHT pages `[26, 28, 29]`; functions `['func_d7f66f9e67cecffa855f', 'func_011ba53858207da5c1a5', 'func_f1d8d521aa0b649e0b09']`; fragments `['frag_7a19d07a14974eefda68', 'frag_85df66f19ac87cb93212', 'frag_c1e2de111d4d31073cdc']`; capacity keys `['RIGHT:26:frag_c1e2de111d4d31073cdc', 'RIGHT:28:frag_7a19d07a14974eefda68', 'RIGHT:29:frag_85df66f19ac87cb93212']`; evidence refs `79`.

Atomic component mapping: `[{"capacity_key": "RIGHT:26:frag_c1e2de111d4d31073cdc", "component_role": "DOMESTIC_PRESSURE_BOOST", "left_fragment_id": "frag_f1b4378224832f41a1b1", "left_function_id": "func_2767e2d48433038ab2c5", "left_physical_page": 20, "right_fragment_id": "frag_c1e2de111d4d31073cdc", "right_function_id": "func_d7f66f9e67cecffa855f", "right_physical_page": 26}, {"capacity_key": "RIGHT:28:frag_7a19d07a14974eefda68", "component_role": "FIRE_PRESSURE_BOOST", "left_fragment_id": "frag_7a8e38bc7f10500fe490", "left_function_id": "func_ded02102e4a5e67fdbc7", "left_physical_page": 20, "right_fragment_id": "frag_7a19d07a14974eefda68", "right_function_id": "func_011ba53858207da5c1a5", "right_physical_page": 28}, {"capacity_key": "RIGHT:29:frag_85df66f19ac87cb93212", "component_role": "INCOMING_METERING", "left_fragment_id": "frag_6a0fef370c463f5ba71e", "left_function_id": "func_c70c920df990623ffde3", "left_physical_page": 20, "right_fragment_id": "frag_85df66f19ac87cb93212", "right_function_id": "func_f1d8d521aa0b649e0b09", "right_physical_page": 29}]`.

## Runtime and safety

Request attempts `18`; successful inference requests `18`; request-start failures `0`; affected task observations `0`.

Schema failures `0`; model runtime failures `0`; semantic response failures `0`.

Input/output/total tokens `0/0/0`; model runtime `145120 ms`; wall time `56680 ms`.

Telemetry defect: `True` — Successful calls returned usage={} / zero tokens; token telemetry is defective.

Capacity errors `0`; RIGHT_MAP_CONFLICT `0`.

Production runs `0`; deploy `NO`; shadow `OFF`; materialization `NO`; Vision `NO`.

## Verdict

**B — candidate/transport are valid, but selector is not cold-repeat stable.**
