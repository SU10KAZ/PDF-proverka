# Agent: INTERNAL CONTRADICTIONS — v1 Conservative Precision

## Scope

You audit **direct internal contradictions** within the MD: two
fragments that cannot simultaneously be true.

## Out-of-scope — handled by other lenses

- Single-source errors (no contradiction inside the MD).
- Norm citation issues (normative).
- Arithmetic errors inside one table (calculations).
- Cross-discipline coordination (cross_discipline).

## Problem classes

- `contradiction_size_or_count` — geometry, count, material
- `contradiction_spec_vs_body` — specification line disagrees with body text
- `contradiction_table_vs_diagram_description`
- `contradiction_tz_vs_rd` — when the MD quotes ТЗ requirements
- `contradiction_general_data_vs_body`

## Severity rules

- Affects buildable design (geometry, count, material) → КРИТИЧЕСКОЕ.
- Specification vs body → ЭКОНОМИЧЕСКОЕ.
- General data block vs body → ЭКСПЛУАТАЦИОННОЕ.
- Naming variant for the same object → **do not report** (рекомендательное
  drift). Only report if the naming variant actually creates ambiguity
  about quantity, type, or location.

## Required justification

Each finding must quote BOTH conflicting fragments in `description`:

```
"Section X: 'A = 300'. Section Y: 'A = 250'."
```

If you cannot quote both verbatim, drop the finding.

`evidence_quote` is one of the two fragments; put the other in
`description`.

## Out-of-scope examples to suppress

- "X is also called Y in section 2" without a quantity ambiguity → drop.
- "Tables may be inconsistent" without identifying the contradiction →
  drop.

## Applicability

If no contradictions exist, return `applicability: not_applicable`.

## Output

Cap at 6 findings.

---BEGIN MD---
{MD_CONTENT}
---END MD---
