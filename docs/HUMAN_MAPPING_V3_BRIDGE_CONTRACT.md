# Human Mapping → V3 Bridge V1

Version: `human_mapping_bridge/1`. Scope: deterministic dry-run and concrete
in-process `FakeProvider` only. Real engine entry points are unchanged. There
is no UI button, provider discovery, automatic resumption or inference path.
The source base is production commit `7a552edc609c3b57331714a30f7857f566523358`.

## Source and review contract

`build_snapshot(object_id, comparison_id, pair_id, source_run_id)` reads an
explicit completed/frozen versioned generation. No current-run fallback, HM
ContextVar fallback, legacy adoption or writes are permitted. It verifies the
object registry, session/pair existence and ownership, frozen manifest and its
artifact hashes, source result identity, current pair PDF hashes, prepared
structure hash, source HM identity and membership derived from the source map.
The registry must already contain the object; missing registries fail before
legacy helpers could create defaults.

Reviews and BlockLink edits are read exclusively from that run's
`human_mapping/` directory. Each event must carry that run, object and pair.
Existing HM events use `comparison_id=pair_id`; the snapshot's comparison ID
is the session ID. This difference is intentional and checked explicitly.
Malformed/misplaced events, stale regions, unknown/wrong-side IDs, invalid
edits and effective confirmed/rejected edge conflicts fail with
`BRIDGE_CONFLICT_REVIEW_REQUIRED` before a fake Mapper is called. Concurrent
history changes during the read fail closed; there is no automatic retry.

Replay uses existing `effective_links`/`validate_block_link_event`: ADD,
DELETE and REASSIGN by previous_link_id. Review replay matches the established
UI `latest(region)` rule: last append per region wins, even for a subset
selection. This is not a new per-edge review history model. A review selects
only effective links whose two endpoints are in its stored selections.
Selection arrays never become a Cartesian product. AI-proposed links follow
the existing 1→1/1→N/N→1 rule; arbitrary N↔N groups have no implied links.
Manual N↔N uses only the explicitly added links. Manual links added/reassigned
after a review require a later explicit review; old confirmations cannot
silently apply to new endpoints. Both event streams use timezone-aware times
for this cross-stream check. Deleted/reassigned old links are not anchors.

- HUMAN_CONFIRMED: each effective selected exact link is a HARD_ANCHOR.
- HUMAN_MANUAL + explicit confirmation: the same HARD_ANCHOR semantics.
- HUMAN_REJECTED: only the selected effective exact edge is forbidden.
- HUMAN_UNCERTAIN / UNREVIEWED: no constraints.

Anchors are non-exclusive; endpoints may still map to other blocks. The
unconstrained-block list describes untouched endpoints, not a candidate
blacklist. All other edges remain available. Different physical pages and
many-to-many page arrangements are allowed.

## Snapshot and provenance

`Snapshot` owns immutable canonical UTF-8 JSON bytes. Its full-byte SHA256 is
also its snapshot ID. Dictionary access returns detached copies. Exclusive
file creation refuses overwrite. The schema is `BRIDGE_SNAPSHOT_SCHEMA.json`.
Snapshots include scope, both PDF hashes, source-manifest and structure hashes,
created_at, raw frozen event history and its hash, event hashes, exact anchors,
rejections, unconstrained remainder and an empty conflict list. Snapshots can
contain review comments; handle them as review data, not public catalog data.

At consumption, a caller supplies the expected SHA256 and full source identity.
Validation reloads the frozen source, verifies both embedded event streams
are exact prefixes of the stored append-only history, and replays that frozen
history, requiring byte equality. Later human edits do not replace this history. Identical source,
history and explicit created_at yield identical bytes. Current time is used
when created_at is omitted. SHA256 provides integrity, not authentication.

`fake_mapper_run` requires a new, unused run_id and concrete `FakeProvider`.
It returns an unpublished future-run artifact with object/comparison/pair,
new/source run IDs, source hashes, validated map and provenance:
`mapping_mode`, `bridge_version`, `bridge_snapshot_id`,
`bridge_snapshot_sha256`, `source_run_id`, anchor/rejection counts and enforcement.
The dry-run never creates a versioned run or changes current_run. Tests also
prove that a caller can persist these bytes and this provenance inside a new
isolated run through existing versioned storage. Catalog already keys by run;
its multi-run regression proves separate entries without identity collisions.
Real anchored engine execution/publication is deliberately not enabled in V1.

## Mapper package and enforcement

`mapper_package` defaults to baseline. Its prompt, schema, source pages and
image list equal the current engine input. Supplying a snapshot in baseline,
an unknown mode or a missing snapshot in human_anchored mode is an error.
The anchored package adds one deterministic `human_mapping_constraints`
section. Frozen Mapper/Miner/Dedupe prompt bytes and schemas remain unchanged.
No quality tuning, confidence policy or engineering heuristics are added.

V1 chooses **reject response**, not injection or region surgery. The response
first passes JSON schema, V3 page coverage and source-block traceability checks.
Exact links are projected from region membership using the same existing
BlockLink rule as HM. Each hard anchor needs an explicit 1→1/1→N/N→1 witness;
an arbitrary N↔N group alone cannot prove those edges. A Mapper can represent
sparse N↔N anchors using separate explicit regions. No Cartesian expansion
is made at either snapshot construction or enforcement.

Missing anchors or returned rejected exact links produce
`BRIDGE_MAPPING_REJECTED`, listing only missing/forbidden edges and the snapshot
hash. The whole response is unaccepted: no partly filtered map reaches Miner.
For rejected A→B, A→C and D→B remain permitted; a subsequent valid response
containing them is accepted. There is no block blacklist and no automatic retry.
This intentionally avoids inventing confidence or changing region meaning.
An accepted map is unchanged and can be consumed normally by Miner/Dedupe.

## Dry-run CLI

From the isolated clone, with the correct read-only data configuration:

```sh
python scripts/human_mapping_bridge_dry_run.py \
  --object-id OBJECT --comparison-id SESSION --pair-id PAIR \
  --source-run-id SOURCE_RUN --output /outside/comparison/bridge.json
```

`--created-at` optionally fixes the timestamp for deterministic reproduction.
Output contains snapshot, hash and zero model calls, or explicit conflicts and
exit code 2. Output inside comparison storage and overwriting existing files
are forbidden. Production tests use counts only if real reviews exist.

## Verification and deployment

Tests use isolated synthetic storage, including real V3 source preparation and
existing FakeProvider production flow. Required cardinality, rejection, sparse,
conflict, stale-source, snapshot immutability, cross-run, replay, CLI, schema,
new-run persistence and baseline tests emit receipts with `BRIDGE_RECEIPTS`.
No validation/final reserve evidence, quality labels or tuning corpus is used.
Normal runtime code is untouched: this bridge is an opt-in backend preparation
module. A release must still pass the existing production source guard; the
user's directory exception does not disable the publication/ancestry guard.
