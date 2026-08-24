# Strong P → RD GRSh report

## Ten answers in plain language

1. **Why is ordinary overlay insufficient?** The same system is redrawn with different page proportions, branch order, coordinates, and CAD primitive packaging. Registration is unreliable, and a forced overlay turns the pair into thousands of colored ink fragments.
2. **What is LEFT?** Two feeds from ТП1/ТП2, two input breakers, two bus sections, a motorized QF3/AVR section tie, two metering paths, two compensation groups, and `15+15` outgoing devices.
3. **What is RIGHT?** Two explicit transformers Т1/Т2, two input breakers, two bus sections, a QS1 section tie, explicit metering, two compensation groups, and `13+14` outgoing devices.
4. **What is functionally preserved?** The two-source/two-section backbone, inter-section connectivity, metering, compensation, and most recurring load families. Twenty-five branch identities are reliably matched.
5. **What is genuinely rebuilt?** The section device changes type and control representation; outgoing-device count changes; free-reserve positions disappear; branch grouping/order is substantially reorganized.
6. **What is only greater RD detail?** Explicit Т1/Т2 symbols and more explicit input/metering internals. They must not be reported as two newly added sources.
7. **Could the structures be matched without coordinate overlay?** Yes. Cross-version coordinates were not used. Functional EOM identity matched 25 branches, 18 of them at different slots.
8. **What needed EOM knowledge?** Source/input/bus roles, QF versus QS semantics, section-tie meaning, metering and АУКРМ roles, normalization of ВРУ/ШУ families, and detail-vs-change adjudication.
9. **Where was Vision needed?** Only to adjudicate the central tie, confirm explicit transformers and two-section geometry, count/verify branch arrays, and inspect two local identity conflicts.
10. **What should future Mode 2 be?** Build two independent discipline-profiled system graphs, compare them at backbone/group/device levels, emit a common typed ledger, and call Vision only for unresolved local nodes/edges.

## What the measurements say

The two block aspect ratios are about 2.03 and 1.55. ORB finds 96 tentative cross-image matches, but only 9 support the affine fit; the estimated scale collapses to zero, so registration is rejected. A forced non-isotropic whole-bbox normalization still gives only **5.76% ink IoU** and produces **3,603 lost** plus **2,262 new** ink components of at least four pixels.

The generic prepared-object graph is also dominated by export packaging:

- LEFT: 2,271 generic objects, with segment/symbol/relation caps reached;
- RIGHT: 148 generic objects, relation cap reached;
- reliable alignment: no;
- matched fraction: 0.0084;
- apparent generic changes: 2,423.

The structural result is much smaller and more useful:

- backbone preserved;
- branch order reorganized;
- section device changed `QF3 → QS1`;
- source representation becomes more detailed;
- outgoing devices change `30 → 27`;
- two local branch identities remain explicitly uncertain.

## Generic graph versus EOM profile

A generic method can potentially recover low-semantic facts such as two long bus-like bands, two repeated branch arrays, two incoming trunks, and a central cross-link. On this file pair, the existing generic object layer does not even provide stable object correspondence because the primitive decomposition differs too much.

An EOM/single-line profile is required to make the useful claims:

- `Т1/Т2` or `ТП1/ТП2` represent the two source paths;
- `QF1/QF2` are input devices rather than arbitrary symbols;
- `РП1/РП2` are bus sections;
- `QF3/QS1` are alternative implementations of the section tie;
- АУКРМ and metering groups are preserved functions despite relocation;
- ВРУ/ШУ labels can identify branches after reordering;
- explicit transformer symbols can be a detail increase rather than `NODE_ADDED`.

So the winning design is not Vision-first and not generic-geometry-only. It is a structural engine with a discipline profile and targeted raster adjudication.

## Proposed routing signals, not production thresholds

No thresholds are implemented. The following measurable signals should be evaluated on a broader corpus:

### Signals favoring Mode 1 — registration + local ink diff

- high RANSAC inlier ratio with a non-degenerate near-uniform transform;
- similar block aspect ratios and stable vector-object counts;
- high aligned ink overlap;
- residual changes spatially local rather than distributed across the full block;
- high generic-object matched fraction.

### Signals favoring Mode 2 — independent structural graphs

- shared block purpose/discipline and recurring functional anchors;
- recoverable source/input/bus/section backbone on both sides;
- low or degenerate registration confidence;
- low aligned ink overlap with changes distributed across the block;
- large layout/order change but stable functional node families;
- generic-object decomposition instability across CAD exports.

### Signals favoring Mode 3 — Vision structural fallback

- vector layer missing, capped beyond usefulness, or dominated by raster;
- structural backbone cannot be recovered even with a discipline profile;
- too few functional anchors for defensible graph identity.

## Future architecture

```text
prepared graphic block pair
        ↓
purpose + vector/raster quality + registration signals
        ↓
┌───────────────────────┬──────────────────────────┬────────────────────┐
│ MODE 1                │ MODE 2                   │ MODE 3             │
│ near revision         │ strong redesign          │ graph unavailable  │
│ registration          │ independent EOM graphs   │ Vision structural  │
│ + local ink events    │ + graph identity compare │ fallback           │
└───────────────┬───────┴──────────────┬───────────┴──────────┬─────────┘
                ↓                      ↓                      ↓
                    common typed graphic change ledger
                                  ↓
                    targeted Vision for uncertainties
```

Mode 2 should emit the same common ledger vocabulary as Mode 1, but its evidence is node/edge correspondence rather than pixel proximity. It must preserve multi-level truth: a Level C change must not erase an unchanged Level A backbone.

## Final verdict

**B. Structural graph comparison works, but only after a discipline-specific profile.**

It should become the second comparison mode next to overlay, with the profile selected from block purpose/discipline and with Vision kept as a local adjudicator or Mode 3 fallback.
