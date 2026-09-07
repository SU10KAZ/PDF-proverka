# Stage Comparison Human Contour v1

Human Contour v1 is a default-off consumer of Stable Domain Keys v1. It does
not change Sheet Matcher, entity matching, synthesis, Astra, or presentation
grouping semantics.

## Feature gates

- `QUESTION_VISIBILITY_V1_ENABLED=false` keeps the legacy UI branch. When
  enabled, the atomic question section is shown from explicit question states
  and can coexist with HRO output.
- `HUMAN_CONTOUR_V1_ENABLED=false` keeps the legacy answer path. When enabled,
  it implies explicit question visibility and stores human authority in the M1
  `DecisionRegistry`.
- `HUMAN_CONTOUR_DECISION_REGISTRY_PATH` optionally selects the SQLite file.
  The default is `<COMPARISON_ROOT>/human_contour/decision_registry.sqlite`.

The contour fails closed when Stable Domain Key metadata is absent. Enabling
durable writes also requires portal authentication and a portal login mapped
to an employee with the `expert` or `admin` role.

## API contract

Existing endpoints are extended rather than duplicated:

- `GET .../production/questions` lists the current atomic questions, their
  decision state, evidence references, decision history, and registry
  revision.
- `GET .../production/questions/{domain_key}` reads one current question.
- `PUT .../production/answers` accepts the existing answer shape plus required
  `domain_key` while Human Contour is enabled. `question_id` is checked only as
  a legacy alias. The request uses the returned input signature and registry
  revision for optimistic concurrency.

The server derives `author` from the signed portal cookie and employee record;
the request body's legacy `author` field is discarded.

## Identity and lifecycle

The durable lookup key is `AtomicQuestion DomainKey + ATOMIC scope`. Session,
pair, run, presentation-group, and legacy question IDs are provenance only.

Question read states are:

- `ACTIONABLE`: current semantic question, no exact human decision;
- `RESOLVED`: compatible active human decision;
- `LOCKED`: compatible human decision protected from automatic replacement;
- `STALE`: the same DomainKey exists but input content is incompatible, or the
  whole generation is stale;
- `REQUIRES_REVALIDATION`: input is unchanged but the algorithm signature is
  not covered by an approved ID-only/evidence-only compatibility policy;
- `SUPERSEDED`: historical decision replaced through an explicit forward link;
- `UNAVAILABLE`: version-bound question metadata is incomplete.

`REQUIRES_REVALIDATION` is the effective validation state of an otherwise
active/locked lifecycle record. Old answers are shown as prior history, never
preselected for a stale or revalidation-required question.

Supersession is append-only. A replacement event links
`supersedes_decision_id`; history projects the earlier event as `SUPERSEDED`
without rewriting its stored payload. Authority order is:

`HUMAN > AI > DETERMINISTIC_SUGGESTION`.

Lower authority cannot replace higher authority. A locked record can be
superseded only by an explicitly authorized human action.

## Resolution boundary

The initial contour is deliberately atomic. Answering a question changes only
the AtomicQuestion decision state. It does not implicitly approve or mutate an
AtomicReviewItem, SheetRelation, EntityRelation, AtomicChange, matcher
suggestion, or manual sheet mapping. Recompute is not scheduled by the write;
its receipt explicitly reports `recompute_scheduled=false`.

Presentation groups remain display-only. There is no group-wide answer
propagation and no `UNIFORM_DECISION_GROUP` write path.

## Evidence and language

Each row keeps exact document-version DomainKeys, document/version labels when
available, left/right sheet references, fragment/bbox references when the
producer supplied them, evidence-provenance keys, and reason codes. The normal
card presents a Russian prompt, reason, options, state, and atomic effect. Raw
codes and DomainKeys remain diagnostics.

## Legacy migration

There is no automatic migration. Generated `PENDING_REVIEW` findings, HRO
placeholders, `manual/user_reordered`, and automatic `user_accepted` links are
not human authority. The 22 historic `manual/user_corrected` links remain only
explicit migration candidates after author, exact DocumentVersions, and a
unique DomainKey have independently been proved. This release imports zero
legacy records and never rewrites historical artifacts.
