# Function Lineage Matcher v1 — architecture

This research lane implements the hierarchy established by selector forensics
`bddec7be` without importing anything into production.

```text
frozen OCR/vision evidence
  -> provenance-bearing Function Passport v2
  -> atomic function fragments
  -> bounded functional candidate / candidate group
  -> bounded AI selector (TEXT, Pass A/B, three cold runs)
  -> deterministic function-level verifier
  -> Function Lineage Map
  -> derived physical Sheet Map
```

## Independent relation namespaces

`DOCUMENT_LINK` is generated from change-register/TOC/title-block references and
has zero contribution to functional similarity. `FUNCTIONAL_ANALOGUE` contains
only candidates bound to extracted function and fragment IDs. Both may coexist
for one LEFT source; neither overwrites the other.

## Capacity and many-to-many behavior

The capacity key is `RIGHT:<physical page>:<function fragment id>`. Sharing a
physical RIGHT page is therefore legal when the selected lineages use different
fragments. Reusing the same atomic fragment in unrelated candidates fails closed;
a declared `MERGED_N_TO_1` candidate is one lineage and may be selected by each
of its LEFT tasks.

## Safety

The selector can return only a prebuilt candidate ID or a fail-closed sentinel.
It cannot supply pages, function IDs, fragments, groups or evidence. The verifier
checks candidate existence, evidence binding, direction, namespace, relation and
fragment capacity. `FUNCTION_REMOVED` requires exhaustive absence evidence;
physical sheet disappearance is insufficient. Reverse `NEW_FUNCTION` audit obeys
the same invariant. This spike does not write mappings or source runs and has no
deploy path.
