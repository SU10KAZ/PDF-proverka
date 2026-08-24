# Structural comparison — LEFT/P ↔ RIGHT/RD

## Result

The system backbone is functionally preserved. The pair is not “everything removed + everything added”: it is a two-source, two-section GRSh whose layout, branch order, detail level, section device, and outgoing implementation were revised.

## Level A — system backbone

```text
LEFT:  2 sources → 2 input QF → 2 bus sections ← QF3/АВР tie
RIGHT: 2 sources → 2 input QF → 2 bus sections ← QS1 tie
```

- `UNCHANGED_FUNCTIONAL_STRUCTURE`: two sources, two inputs, two sections, one tie.
- `DETAIL_LEVEL_INCREASED`: RIGHT exposes `Т1/Т2`; LEFT stops at connections to `ТП1/ТП2`.
- `SECTIONING_CHANGED` / `NODE_TYPE_CHANGED`: the central tie remains, but `QF3` is replaced by `QS1` and control is represented differently.

## Level B — functional groups

| Group | LEFT | RIGHT | Decision |
|---|---:|---:|---|
| Bus sections | 2 | 2 | preserved |
| Compensation groups | 2 | 2 | preserved, moved in layout |
| Outgoing devices | 30 (`15+15`) | 27 (`13+14`) | changed |
| Explicit free reserves | 2 | 0 | changed |
| Reliable branch identities matched | — | 25 | preserved correspondence |
| Matched branches with a different slot | — | 18 | reorganized, not add/remove |

Representative order-independent matches:

| Functional identity | LEFT slot | RIGHT slot |
|---|---:|---:|
| ВРУ1 / section 1 | S1:1 | S1:6 |
| ВРУ2 / section 1 | S1:2 | S1:7 |
| ДР1-ХМ1 | S1:10 | S1:1 |
| ХМ1 | S1:12 | S1:2 |
| АУКРМ-1 | S1:15 | S1:3 |
| ДР2-ХМ2 | S2:10 | S2:12 |
| ХМ2 | S2:12 | S2:11 |
| АУКРМ-2 | S2:15 | S2:10 |

The full 25-row correspondence table is in `structural_comparison.json`.

## Level C — devices and connections

Confirmed:

- the central node type changes from motorized circuit breaker to `QS1` section switch/disconnector;
- outgoing-device count decreases by three;
- the two free-reserve positions are no longer free-reserve positions;
- metering topology is present on both sides but represented/grouped differently.

Uncertain:

- RIGHT `2QF3` repeats a feeder identity that conflicts with section symmetry;
- RIGHT `2QF14` has conflicting terminal and feeder anchors;
- therefore the tail of the GVS/TP-auxiliary group is not forced into false `NODE_ADDED`/`NODE_REMOVED` events.

## Overlay contrast

Even after independently stretching both blocks to one canvas—a deliberately invalid but generous upper bound—the ink IoU is only **0.0576**. The diagnostic yields **3,603** LEFT-only and **2,262** RIGHT-only connected ink islands. Feature registration is unreliable: only 9 of 96 ORB matches support the affine fit, and the fit degenerates.

The existing generic object graph likewise produces 2,423 apparent events from 2,271 LEFT objects versus 148 RIGHT objects, with a matched fraction of 0.0084. Those are CAD packaging/layout effects, not an engineering ledger.

The EOM structural graph compresses the pair to seven typed conclusions: preserved backbone, major reorganization, changed section device, increased source detail, changed outgoing-group count, and two explicit uncertainty caps.

## Verdict

**B — structural graph comparison works, but reliable correspondence requires an EOM/single-line profile.** Generic geometry can suggest repeated branch arrays and two halves, but it cannot reliably name sources, distinguish bus/tie semantics, normalize branch identities, or separate explicit transformer detail from a real source addition.
