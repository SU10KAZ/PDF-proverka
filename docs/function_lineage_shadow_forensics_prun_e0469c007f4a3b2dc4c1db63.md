# Function Lineage shadow forensics: `prun_e0469c007f4a3b2dc4c1db63`

## Scope and outcome

- Pair: `p19cd7f695a` (`ИОС 1.1`).
- Production code: `c736aafb66f238e45ac2a9c18b3f734083e54847`.
- Saved outcome: `shadow_status=FAILED`, `diagnostic_reason=SHADOW_FAILED`.
- Fail-closed state was preserved: zero stable lineages, zero derived sheet
  relations, `materialization.applied=false`, and
  `production_result_changed=false`.
- Four tasks were rejected in both passes, solely with
  `EVIDENCE_FRAGMENT_OWNER_MISMATCH`.

The root cause is an evidence-identity/provenance bug in Function Passport
construction. `_field_evidence()` keyed an evidence ID by pair, side, physical
page, field, and value, but not by function or fragment. Sheet-derived values
such as `systems`, `equipment_roles`, and `document_role` are identical in
several function passports on a multi-function sheet. Those passports therefore
received the same evidence ID, while the shared `evidence_catalog` row was
overwritten with the last fragment's owner. Candidate generation copied the
affected passport refs correctly, but the refs had already lost their claimed
owner identity.

In the saved dataset, 94 evidence refs were used by more than one function
passport and 17 of 20 bounded candidates contained at least one overwritten
owner. The model selected four of those candidates. The prompt builder did not
change the refs, and the model could return only a candidate ID; it could not
return evidence or fragment IDs. The verifier correctly rejected the corrupted
candidate provenance.

## Classification of the chain

- Candidate generator: affected because it consumed already-colliding passport
  refs; it did not deliberately concatenate every fragment on the page.
- Prompt assembly: exposed the affected candidates unchanged; it did not create
  or rewrite evidence refs.
- Model output: not the cause. Pass A and Pass B were syntactically valid and
  chose the same bounded candidate IDs.
- Verifier semantics: correct to fail closed. No owner check was weakened.
- Function Fragment / evidence semantics: sheet-derived fields needed an
  explicit `SHEET_SHARED_EVIDENCE` provenance type instead of a falsely
  fragment-owned, colliding catalog row.

The correction assigns owner-scoped identities to `FRAGMENT_OWNED_EVIDENCE`.
Only declared fields copied directly from a physical-sheet passport can be
`SHEET_SHARED_EVIDENCE`; they have no function/fragment owner and must match the
candidate's exact expected ref set, side, page, and field allowlist. Same-page
fragment-owned evidence is still rejected.

## Pass A / Pass B and deterministic replay

All four affected tasks had `model_ok=true` in both passes, no global verifier
errors, identical candidate IDs across passes, and no rejection reason other
than `EVIDENCE_FRAGMENT_OWNER_MISMATCH`.

The replay rebuilt the 46 tasks and 20 candidates from the same production
sheet indexes plus the saved `sheet_relations.json`. Candidate IDs were
unchanged; the input/payload signature changed as expected with the corrected
algorithm and provenance. Raw parsed model JSON is intentionally not persisted,
but each saved verification record contains every returned task/candidate
selection plus its model/schema validation outcome. The replay placed those
stored selections under the newly built payload signature and invoked only the
corrected deterministic verifier and consensus; no gateway or model call was
made.

- Pass A: 4 prior rejects became PASS; 0 remain rejected.
- Pass B: 4 prior rejects became PASS; 0 remain rejected.
- Consensus: 4 stable `CONTINUED_1_TO_1` lineages, 42 unresolved
  `NEED_MORE_EVIDENCE`, and no capacity conflict.

## Evidence-owner matrices from the saved run

### `ltask_3a0620cf939abcc47088`

- LEFT: page `24`, function `func_4c4d5fbea910168e7eed`, fragment
  `frag_5ae5ba3e1e90d0db42c4`.
- Selected candidate: `lcand_8b041c749a8f39e329ad`.
- RIGHT: page `24`, function `func_8bd58e92517900a6ee03`, fragment
  `frag_10689bbfbeffe0d2871e`.
- Pass A / Pass B: `lcand_8b041c749a8f39e329ad` /
  `lcand_8b041c749a8f39e329ad`.

| evidence_ref | side | page | field | expected function / fragment | actual function / fragment | result |
|---|---:|---:|---|---|---|---|
| `flev_02d370a9f5fbff69c0f6` | LEFT | 24 | `systems` | `func_4c4d5fbea910168e7eed` / `frag_5ae5ba3e1e90d0db42c4` | `func_0ba690372cefbc64af09` / `frag_7e261848c15fffd6215f` | MISMATCH |
| `flev_0692add3893cc2831bbf` | LEFT | 24 | `topology_role` | `func_4c4d5fbea910168e7eed` / `frag_5ae5ba3e1e90d0db42c4` | `func_4c4d5fbea910168e7eed` / `frag_5ae5ba3e1e90d0db42c4` | OK |
| `flev_095f9b8d03856877c791` | RIGHT | 24 | `section` | `func_8bd58e92517900a6ee03` / `frag_10689bbfbeffe0d2871e` | `func_8bd58e92517900a6ee03` / `frag_10689bbfbeffe0d2871e` | OK |
| `flev_14d1544f50b2bcab0c86` | RIGHT | 24 | `document_role` | `func_8bd58e92517900a6ee03` / `frag_10689bbfbeffe0d2871e` | `func_8bd58e92517900a6ee03` / `frag_10689bbfbeffe0d2871e` | OK |
| `flev_264101d326a4e1a876e8` | RIGHT | 24 | `component_role` | `func_8bd58e92517900a6ee03` / `frag_10689bbfbeffe0d2871e` | `func_8bd58e92517900a6ee03` / `frag_10689bbfbeffe0d2871e` | OK |
| `flev_5a7ef8dac3dbbbd37cb3` | LEFT | 24 | `document_role` | `func_4c4d5fbea910168e7eed` / `frag_5ae5ba3e1e90d0db42c4` | `func_0ba690372cefbc64af09` / `frag_7e261848c15fffd6215f` | MISMATCH |
| `flev_5d8dabc93500c5c1b5d2` | RIGHT | 24 | `topology_role` | `func_8bd58e92517900a6ee03` / `frag_10689bbfbeffe0d2871e` | `func_8bd58e92517900a6ee03` / `frag_10689bbfbeffe0d2871e` | OK |
| `flev_86ae2b6ddc98b979ac54` | LEFT | 24 | `function_class` | `func_4c4d5fbea910168e7eed` / `frag_5ae5ba3e1e90d0db42c4` | `func_4c4d5fbea910168e7eed` / `frag_5ae5ba3e1e90d0db42c4` | OK |
| `flev_8f8352f4724f39b212f6` | RIGHT | 24 | `function_class` | `func_8bd58e92517900a6ee03` / `frag_10689bbfbeffe0d2871e` | `func_8bd58e92517900a6ee03` / `frag_10689bbfbeffe0d2871e` | OK |
| `flev_ae6d147a4ddfc79350f3` | LEFT | 24 | `component_role` | `func_4c4d5fbea910168e7eed` / `frag_5ae5ba3e1e90d0db42c4` | `func_4c4d5fbea910168e7eed` / `frag_5ae5ba3e1e90d0db42c4` | OK |
| `flev_aea48c7ab8f69c6bd216` | RIGHT | 24 | `systems` | `func_8bd58e92517900a6ee03` / `frag_10689bbfbeffe0d2871e` | `func_8bd58e92517900a6ee03` / `frag_10689bbfbeffe0d2871e` | OK |
| `flev_ce3bb1f4bfa582d0ecf6` | LEFT | 24 | `equipment_roles` | `func_4c4d5fbea910168e7eed` / `frag_5ae5ba3e1e90d0db42c4` | `func_0ba690372cefbc64af09` / `frag_7e261848c15fffd6215f` | MISMATCH |
| `flev_f821e31269f9a885f7e6` | RIGHT | 24 | `equipment_roles` | `func_8bd58e92517900a6ee03` / `frag_10689bbfbeffe0d2871e` | `func_8bd58e92517900a6ee03` / `frag_10689bbfbeffe0d2871e` | OK |

### `ltask_64aec886f5b6e44c1113`

- LEFT: page `37`, function `func_190cbda1ccdd7e922af8`, fragment
  `frag_e855dd2e9e3cdd1f0fd5`.
- Selected candidate: `lcand_56a0d60e350e0a7ef17b`.
- RIGHT: page `45`, function `func_da43132a42d8f935ad6f`, fragment
  `frag_1989d2d2137747aafdcd`.
- Pass A / Pass B: `lcand_56a0d60e350e0a7ef17b` /
  `lcand_56a0d60e350e0a7ef17b`.

| evidence_ref | side | page | field | expected function / fragment | actual function / fragment | result |
|---|---:|---:|---|---|---|---|
| `flev_01617cd461cad12860e4` | LEFT | 37 | `document_role` | `func_190cbda1ccdd7e922af8` / `frag_e855dd2e9e3cdd1f0fd5` | `func_29a6c1d0d6d1867e6314` / `frag_7c824dab11e1984c8e46` | MISMATCH |
| `flev_068d9cae14d1694a8145` | LEFT | 37 | `systems` | `func_190cbda1ccdd7e922af8` / `frag_e855dd2e9e3cdd1f0fd5` | `func_29a6c1d0d6d1867e6314` / `frag_7c824dab11e1984c8e46` | MISMATCH |
| `flev_15a8ecfc21262c2a4b03` | LEFT | 37 | `component_role` | `func_190cbda1ccdd7e922af8` / `frag_e855dd2e9e3cdd1f0fd5` | `func_190cbda1ccdd7e922af8` / `frag_e855dd2e9e3cdd1f0fd5` | OK |
| `flev_2d00a21eecb2d9c500a5` | RIGHT | 45 | `topology_role` | `func_da43132a42d8f935ad6f` / `frag_1989d2d2137747aafdcd` | `func_da43132a42d8f935ad6f` / `frag_1989d2d2137747aafdcd` | OK |
| `flev_4bbed3242eeea04a1300` | RIGHT | 45 | `equipment_roles` | `func_da43132a42d8f935ad6f` / `frag_1989d2d2137747aafdcd` | `func_075f045f4d674deb8e99` / `frag_5a3e0138a8276dbfe182` | MISMATCH |
| `flev_4d60edff6b098658180e` | RIGHT | 45 | `systems` | `func_da43132a42d8f935ad6f` / `frag_1989d2d2137747aafdcd` | `func_075f045f4d674deb8e99` / `frag_5a3e0138a8276dbfe182` | MISMATCH |
| `flev_562fd7c805b7f11b0488` | RIGHT | 45 | `function_class` | `func_da43132a42d8f935ad6f` / `frag_1989d2d2137747aafdcd` | `func_da43132a42d8f935ad6f` / `frag_1989d2d2137747aafdcd` | OK |
| `flev_9126881d25ca94b301cd` | RIGHT | 45 | `component_role` | `func_da43132a42d8f935ad6f` / `frag_1989d2d2137747aafdcd` | `func_da43132a42d8f935ad6f` / `frag_1989d2d2137747aafdcd` | OK |
| `flev_c2ec4dec390e830e5b26` | RIGHT | 45 | `document_role` | `func_da43132a42d8f935ad6f` / `frag_1989d2d2137747aafdcd` | `func_075f045f4d674deb8e99` / `frag_5a3e0138a8276dbfe182` | MISMATCH |
| `flev_c719608f117e75ac8347` | LEFT | 37 | `function_class` | `func_190cbda1ccdd7e922af8` / `frag_e855dd2e9e3cdd1f0fd5` | `func_190cbda1ccdd7e922af8` / `frag_e855dd2e9e3cdd1f0fd5` | OK |
| `flev_c88bd60bf3ea7bd45ec1` | LEFT | 37 | `topology_role` | `func_190cbda1ccdd7e922af8` / `frag_e855dd2e9e3cdd1f0fd5` | `func_190cbda1ccdd7e922af8` / `frag_e855dd2e9e3cdd1f0fd5` | OK |
| `flev_d13af472931ea20f58ab` | LEFT | 37 | `equipment_roles` | `func_190cbda1ccdd7e922af8` / `frag_e855dd2e9e3cdd1f0fd5` | `func_29a6c1d0d6d1867e6314` / `frag_7c824dab11e1984c8e46` | MISMATCH |

### `ltask_bb8a0e69f8cf2af99b95`

- LEFT: page `37`, function `func_1247db3075973cf766a6`, fragment
  `frag_4041e8378a4b02c6aeeb`.
- Selected candidate: `lcand_ce3d685003a83a4cefda`.
- RIGHT: page `45`, function `func_075f045f4d674deb8e99`, fragment
  `frag_5a3e0138a8276dbfe182`.
- Pass A / Pass B: `lcand_ce3d685003a83a4cefda` /
  `lcand_ce3d685003a83a4cefda`.

| evidence_ref | side | page | field | expected function / fragment | actual function / fragment | result |
|---|---:|---:|---|---|---|---|
| `flev_01617cd461cad12860e4` | LEFT | 37 | `document_role` | `func_1247db3075973cf766a6` / `frag_4041e8378a4b02c6aeeb` | `func_29a6c1d0d6d1867e6314` / `frag_7c824dab11e1984c8e46` | MISMATCH |
| `flev_068d9cae14d1694a8145` | LEFT | 37 | `systems` | `func_1247db3075973cf766a6` / `frag_4041e8378a4b02c6aeeb` | `func_29a6c1d0d6d1867e6314` / `frag_7c824dab11e1984c8e46` | MISMATCH |
| `flev_2387e943220d71790165` | LEFT | 37 | `function_class` | `func_1247db3075973cf766a6` / `frag_4041e8378a4b02c6aeeb` | `func_1247db3075973cf766a6` / `frag_4041e8378a4b02c6aeeb` | OK |
| `flev_4bbed3242eeea04a1300` | RIGHT | 45 | `equipment_roles` | `func_075f045f4d674deb8e99` / `frag_5a3e0138a8276dbfe182` | `func_075f045f4d674deb8e99` / `frag_5a3e0138a8276dbfe182` | OK |
| `flev_4d60edff6b098658180e` | RIGHT | 45 | `systems` | `func_075f045f4d674deb8e99` / `frag_5a3e0138a8276dbfe182` | `func_075f045f4d674deb8e99` / `frag_5a3e0138a8276dbfe182` | OK |
| `flev_68f926fc806b05eb09d9` | LEFT | 37 | `topology_role` | `func_1247db3075973cf766a6` / `frag_4041e8378a4b02c6aeeb` | `func_1247db3075973cf766a6` / `frag_4041e8378a4b02c6aeeb` | OK |
| `flev_98e3fc58805895885063` | RIGHT | 45 | `component_role` | `func_075f045f4d674deb8e99` / `frag_5a3e0138a8276dbfe182` | `func_075f045f4d674deb8e99` / `frag_5a3e0138a8276dbfe182` | OK |
| `flev_a004670d5ea65d1ba0a7` | RIGHT | 45 | `function_class` | `func_075f045f4d674deb8e99` / `frag_5a3e0138a8276dbfe182` | `func_075f045f4d674deb8e99` / `frag_5a3e0138a8276dbfe182` | OK |
| `flev_a464751fb607491a88ff` | RIGHT | 45 | `topology_role` | `func_075f045f4d674deb8e99` / `frag_5a3e0138a8276dbfe182` | `func_075f045f4d674deb8e99` / `frag_5a3e0138a8276dbfe182` | OK |
| `flev_c2ec4dec390e830e5b26` | RIGHT | 45 | `document_role` | `func_075f045f4d674deb8e99` / `frag_5a3e0138a8276dbfe182` | `func_075f045f4d674deb8e99` / `frag_5a3e0138a8276dbfe182` | OK |
| `flev_d13af472931ea20f58ab` | LEFT | 37 | `equipment_roles` | `func_1247db3075973cf766a6` / `frag_4041e8378a4b02c6aeeb` | `func_29a6c1d0d6d1867e6314` / `frag_7c824dab11e1984c8e46` | MISMATCH |
| `flev_d21ccd6c9fd6e0b5ce1c` | LEFT | 37 | `component_role` | `func_1247db3075973cf766a6` / `frag_4041e8378a4b02c6aeeb` | `func_1247db3075973cf766a6` / `frag_4041e8378a4b02c6aeeb` | OK |

### `ltask_f881239a06618fa7ccf6`

- LEFT: page `51`, function `func_0daf199b949924195f69`, fragment
  `frag_cc434191e10b97005303`.
- Selected candidate: `lcand_9677f053e180ebc0136b`.
- RIGHT: page `32`, function `func_2afbe122bc9c11fd860c`, fragment
  `frag_0f66d829160fbb6ad224`.
- Pass A / Pass B: `lcand_9677f053e180ebc0136b` /
  `lcand_9677f053e180ebc0136b`.

| evidence_ref | side | page | field | expected function / fragment | actual function / fragment | result |
|---|---:|---:|---|---|---|---|
| `flev_0a94e14448bcaa896f94` | LEFT | 51 | `component_role` | `func_0daf199b949924195f69` / `frag_cc434191e10b97005303` | `func_0daf199b949924195f69` / `frag_cc434191e10b97005303` | OK |
| `flev_0ea37d7a656b62f6ce3a` | LEFT | 51 | `function_class` | `func_0daf199b949924195f69` / `frag_cc434191e10b97005303` | `func_0daf199b949924195f69` / `frag_cc434191e10b97005303` | OK |
| `flev_211181e6dd8b57311abe` | RIGHT | 32 | `equipment_roles` | `func_2afbe122bc9c11fd860c` / `frag_0f66d829160fbb6ad224` | `func_2afbe122bc9c11fd860c` / `frag_0f66d829160fbb6ad224` | OK |
| `flev_220795b752c49795933d` | LEFT | 51 | `document_role` | `func_0daf199b949924195f69` / `frag_cc434191e10b97005303` | `func_fd4bdb71b51d9c35ea89` / `frag_2832c67c6ef32f4e2b76` | MISMATCH |
| `flev_2dc74c9e002f69a02f5d` | RIGHT | 32 | `topology_role` | `func_2afbe122bc9c11fd860c` / `frag_0f66d829160fbb6ad224` | `func_2afbe122bc9c11fd860c` / `frag_0f66d829160fbb6ad224` | OK |
| `flev_30eec09414c4ffb68c8e` | RIGHT | 32 | `document_role` | `func_2afbe122bc9c11fd860c` / `frag_0f66d829160fbb6ad224` | `func_2afbe122bc9c11fd860c` / `frag_0f66d829160fbb6ad224` | OK |
| `flev_7150565646c9d76f398a` | LEFT | 51 | `systems` | `func_0daf199b949924195f69` / `frag_cc434191e10b97005303` | `func_fd4bdb71b51d9c35ea89` / `frag_2832c67c6ef32f4e2b76` | MISMATCH |
| `flev_98ed5d0caa857033273f` | RIGHT | 32 | `function_class` | `func_2afbe122bc9c11fd860c` / `frag_0f66d829160fbb6ad224` | `func_2afbe122bc9c11fd860c` / `frag_0f66d829160fbb6ad224` | OK |
| `flev_a9886671b0c7b6077bbe` | LEFT | 51 | `topology_role` | `func_0daf199b949924195f69` / `frag_cc434191e10b97005303` | `func_0daf199b949924195f69` / `frag_cc434191e10b97005303` | OK |
| `flev_bfbaae24cc587ae8d1fa` | RIGHT | 32 | `systems` | `func_2afbe122bc9c11fd860c` / `frag_0f66d829160fbb6ad224` | `func_2afbe122bc9c11fd860c` / `frag_0f66d829160fbb6ad224` | OK |
| `flev_c968490d21ba0a6d372d` | RIGHT | 32 | `component_role` | `func_2afbe122bc9c11fd860c` / `frag_0f66d829160fbb6ad224` | `func_2afbe122bc9c11fd860c` / `frag_0f66d829160fbb6ad224` | OK |
| `flev_f5c63e4e3b98045093a4` | LEFT | 51 | `equipment_roles` | `func_0daf199b949924195f69` / `frag_cc434191e10b97005303` | `func_fd4bdb71b51d9c35ea89` / `frag_2832c67c6ef32f4e2b76` | MISMATCH |
