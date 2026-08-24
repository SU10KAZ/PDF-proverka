# Human structural ground truth

This ground truth was fixed by manual side-by-side raster review **before** running the existing generic object comparator. Upstream `results.md` descriptions were treated as hints only; the visible vector/raster geometry was the deciding evidence.

## Review sequence

1. Render the two existing upstream polygons independently.
2. Inspect each complete block without attempting registration.
3. Inspect the central input/section region at high resolution.
4. Count outgoing device positions from visible QF stems and vector anchors.
5. Use functional text only to identify branches; do not compare wording or tabular values.
6. Mark conflicts and insufficiently supported identity as uncertain.

## Ground truth at three levels

### Level A — system backbone

| Question | LEFT | RIGHT | Ground truth |
|---|---|---|---|
| Sources | 2 connections to ТП1/ТП2 | 2 explicit transformers Т1/Т2 | Same two-source concept; RIGHT is more explicit |
| Input devices | QF1, QF2 | QF1, QF2 | Two parallel source-to-input paths retained |
| Bus sections | РП1, РП2 | РП1, РП2 | Two-section architecture retained |
| Section tie | QF3 + АВР | QS1 + SA/`Секц.` | Tie retained; device implementation changed |

### Level B — functional groups

- Metering exists on both inputs in both versions.
- Compensation groups АУКРМ-1 and АУКРМ-2 exist on both versions.
- The recurring VRU, refrigeration, ITP, pump-control, and lighting families remain recognizable.
- Branch order is not identity: 25 correspondences are reliable despite different slots; 18 of those 25 moved to another ordinal position.
- Outgoing-device counts change from `15+15` to `13+14`; the two explicit free-reserve slots in LEFT are not shown as free reserves in RIGHT.

### Level C — devices and connections

- `QF3` → `QS1` is a real node-type/sectioning-implementation change.
- Exact one-to-one identity is not asserted for RIGHT section 2 slot 3 and slot 14.
- The GVS/TP auxiliary tail is reorganized, but the evidence is insufficient to label every local edge as added/removed.

## Answers A–I

**A. Is the two-section architecture retained?** Yes.

**B. How many sources are shown?** Two on each side. LEFT abstracts them as connections to ТП1/ТП2; RIGHT draws Т1/Т2.

**C. Source → input → bus?** Two parallel paths on both sides, each source feeding one input device and one bus section.

**D. How are sections connected?** By one central inter-section device.

**E. Did sectioning change?** The topology did not; the sectional device and its control representation did.

**F. What happened to outgoing groups?** `15+15` becomes `13+14`; order is substantially changed, and the free reserves disappear as free slots.

**G. Preserved but moved branches?** Yes. At least 18 reliable correspondences have a different ordinal slot.

**H. New/removed nodes?** No confirmed new/removed Level A function. Explicit transformer nodes are detail increase. Outgoing slots decrease by three, while two RIGHT branch identities remain locally uncertain.

**I. New system principle or more detail?** The core principle remains. Greater RD detail coexists with real changes to the sectional device and outgoing implementation.

## Vision/raster adjudication used

Raster inspection was limited to these questions:

- confirm that both complete blocks contain two buses and a central tie;
- distinguish the central `QF3` and `QS1` devices;
- verify whether `Т1/Т2` are explicitly drawn;
- verify the repeated outgoing arrays and inspect the two conflicting branch identities.

Vision was not asked to produce an unrestricted change list.
