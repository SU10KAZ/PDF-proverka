# Presentation Human Integration V1

`PRESENTATION_HUMAN_V1_ENABLED` enables read-only consumption of Presentation
Grouping B when Human Contour is enabled. The existing artifact generation flag
remains independent. Both flags default off. No matcher, DomainKey formula,
DecisionRegistry, model topology, or historical materialization is changed.
Integration is in the API adapter, outside the M1 producer-file registry.
All registered producer bytes and therefore algorithm signatures remain
identical to the canonical base; a display-only change cannot force revalidation.

The existing questions and changes GET endpoints add `presentation.items` and
read-only load metrics. Atomic arrays retain their existing contracts. Groups
reference those arrays; they do not copy evidence bytes or decision history.
QuestionGroup and ReviewGroup are separate types, always DISPLAY_ONLY_GROUP.
The existing deterministic builder is invoked on the current generation;
its group ID formula remains unchanged. A generation fence rejects a read that
races publication. Current builder aliases are immediately translated to stable
DomainKeys. A DomainKey reference is never rebound using an old runtime ID.
Legacy-only rows remain accessible as diagnostics, including historical rows.

Group status derives exclusively from child states. ACTIVE/RESOLVED/LOCKED
count as resolved; STALE, REQUIRES_REVALIDATION, UNAVAILABLE and SUPERSEDED
retain their child lifecycle. A mixed group can appear in multiple filters;
opening it shows its complete membership and each child's own state/controls.
The default filter selects groups/standalone questions containing ACTIONABLE
children. Historical unavailable questions have a separate filter. There is
no backfill, group answer endpoint, implicit decision propagation, or tracking.

The browser resolves group membership from the atomic arrays already returned
by the existing six-request production refresh. Expansion makes zero requests.
Question children and review rows are mounted only when expanded. A button
inside each question submits exactly that AtomicQuestion DomainKey through the
existing authenticated answer endpoint. Successful writes clear only the saved
draft; other edited drafts retain optimistic concurrency protection. Refresh
recomputes all statuses from the registry. Golden Human Truth continues to bind
to atomic DomainKeys and never depends on presentation membership.

Integration also fixes the existing frontend null-answer parser: a fresh Human
Contour question legitimately has `human_answer: null`; this is treated as an
empty answer instead of dereferencing null. It does not create a decision or
change API visibility, answerability, or identity when the presentation flag is
off. Presentation styles use the portal theme variables in both themes.

Verification covers group/standalone expansion, null answers, historical and
lifecycle states, runtime ID drift, rerun persistence, membership change,
unchanged siblings, group-ID answer rejection, generation races, and the
existing M1, Human Contour, Sheet v3, auth and OSA regressions. Operational
receipts and isolated V002/browser proofs are stored outside the source tree in
`/home/coder/auditmanager/corpus-audits/20260908_presentation_human/`.

Rollout order: clean candidate and immutable build; published-source guard;
no RUNNING/QUEUED/PREPARING work; code deployment with consumption OFF;
flag-OFF smoke; isolated deployed-code ON verification; another active-work
check; consumption ON; read-only production verification. On a presentation
failure disable the consumption flag; roll back the release if the code still
regresses with the flag off. Sheet v4, Astra selectors and Function Lineage are
outside this gate.
