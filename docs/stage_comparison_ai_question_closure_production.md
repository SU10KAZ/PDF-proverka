# AI Question Closure — production contract

`AI Question Closure` is an independently flagged layer between the frozen
FAST/Preliminary Report result and the Human Review Orchestrator projection.
It does not enable AI Analyst v2 or the general AI Analyst v3 analyzer.

## Flags

- `STAGE_COMPARISON_AI_ANALYST_V2=false` — required production setting;
- `STAGE_COMPARISON_AI_ANALYST_V3=false` — required production setting;
- `STAGE_COMPARISON_AI_QUESTION_CLOSURE=true` — enables only question closure;
- `STAGE_COMPARISON_AI_QUESTION_CLOSURE_CACHE_ENABLED=true` — enables the
  version-bound two-pass response cache.

All flags default to disabled except the cache flag. The cache has no effect
while Question Closure itself is disabled.

## Production sequence

`FAST → Preliminary Report → deterministic HRO → closure eligibility →
two-pass selector → deterministic verifier → HRO projection → Engineer Review
→ Final Report`.

Eligibility is computed for every HRO interaction before a model call. The
current policy permits only a bounded, backend-generated N/PE bus-requirement
comparison identified by its evidence shape. Acceptance question IDs and
titles are not policy inputs. Mode groups, missing evidence, partial graph
correspondence, measurement-function ambiguity and table-row ambiguity remain
with the engineer.

The Candidate Factory may build its deterministic inventory, but the selector
receives only the projected closure task. On the GRSH acceptance pair this is
one selector task rather than the 61-task general v3 inventory.

## Fail-closed gate

A question is removed from the active HRO queue only when all of these checks
hold:

1. its `QuestionClosureContract` allows automatic closure;
2. the task and candidate IDs came from the backend Candidate Factory;
3. both independent passes select the same existing candidate;
4. the deterministic verifier returns `VERIFIED_SELECTION`;
5. the candidate effect is `RESOLVE_HUMAN_QUESTION` (an
   `INSUFFICIENT_EVIDENCE` selection cannot close a question);
6. no human answer or previous human reopen exists;
7. the FAST, HRO, human-decision and generation signatures are still current.

There are no model retries for the closure selector. CLI unavailability,
timeout, malformed output, disagreement, verifier rejection, stale generation
or any exception leaves the original HRO plan in place. FAST and Preliminary
Report artifacts remain usable and the overall analysis does not fail.

## Cache and audit

The cache key includes the FAST signature, HRO-question signature, closure
contract signature, candidate-set and task-batch signatures, prompt and schema
versions/digests, model, reasoning effort, selector pass identity and closure
layer version. Changing any evidence or input axis invalidates the old result.
Pass 1 and Pass 2 use distinct identities, including during cache replay.

An automatically removed question is retained in `ai_closed_questions` with
the selected backend candidate, both pass IDs, verifier status, exact evidence
references and viewer-ready LEFT/RIGHT source locations. The UI states that a
bounded option set was compared, both passes agreed and rules verified the
result. The engineer can open the evidence and return the question to the
manual queue. That action is persisted as a human-priority closure override,
preventing automatic reclosure on a later identical run.

Question closure never writes Engineer Approval. Final Report continues to
contain only findings explicitly marked `APPROVED` by an engineer.

## Profile gates

The release gate covers the Question Closure, v3/v2 regression, HRO,
production orchestrator, preliminary report, evidence navigation, Engineer
Review and frontend HRO suites, plus `compileall` and the production frontend
build. Full-suite collection failures caused by optional grpc/protobuf
dependencies or by an absent ignored research corpus are recorded separately;
they are not runtime or profile-gate failures.
