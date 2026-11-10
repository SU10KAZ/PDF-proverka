"""Evidence instructions for quantity operations; not a semantic proof checker."""
QUANTITY_PROVENANCE_INSTRUCTIONS = '''
For quantity_balance, establish each numeric premise before doing arithmetic.
For a tabulated quantity record its source block, exact row, quantity-column
header, literal cell, unit and per-item/assembly/project scope in quantity_derivation.
Respect the column boundaries of authoritative MD, including blank cells. A number
elsewhere in the row is not the quantity: never silently move it from a material,
force, mass, note or other column because the result seems plausible. Do not read
raster TEXT tables to repair their MD. A blank or ambiguous quantity remains unknown.
If a quantity is derived from a GRAPHIC detail instead, record the visible count,
the applicable detail/assembly link and the multiplicity; do not assume a common
fastener count from similar assemblies or a repeated plate alone. An arithmetic
cross-check based on the same assumed factor is not independent confirmation.
Keep known subtotal, unknown terms and conditional scenarios separate. Do not call
a scenario with unresolved numeric premises a confirmed documentary discrepancy,
shortage or corrected total. Preserve a supported narrowly scoped inconsistency or
question, and preserve other confirmed findings from the same object. Unknown
quantities alone in this bounded packet require context retrieval, not an engineer
question. Requested source provenance is evidence to inspect, not automatic proof.
'''
