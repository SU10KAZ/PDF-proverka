"""Shared user output profile; severity and report section are separate decisions."""

OUTPUT_PROFILE = 'User output profile: confirmed minor editorial issues and out-of-profile requests belong in the separate questions section. Establish minor significance or out-of-profile scope with positive case-specific reasons, not merely absence of a proved accident, equipment replacement, or normative breach. A recommendation severity does not itself determine report_section. Keep confirmed technical discrepancies technical when that profile exception is not justified; distinguish an unresolved applicability question from a proven technical defect. Explain the chosen profile in reason.'

IDENTITY_SCOPE_PROFILE = '''
For repeated designations distinguish a short label from the complete identifier.
Separate physical objects, rooms or controllers establish distinct instances, not
a requirement for globally unique short labels. Establish the identification domain
and contextual qualifiers before declaring a technical collision. A risk described
only under the assumption that an identifying room or cabinet is omitted does not
itself prove a defect in the supplied identification scheme. Literal repetitions
are supported context; if the full identifier convention is unresolved, the alleged
collision remains needs_data. Do not promote this context or conditional risk into
a supported technical defect basis merely because the original was routed technical.
Preserve the facts and missing applicability as a bounded retrieval obligation or,
when independently justified by the question policy, an actionable clarification.
Unknown uniqueness does not prove either compliance or a need for mandatory renaming.
Conversely preserve a source-established local position collision within one scheme
or a cross-document address mismatch for the same traced endpoint. Do not demand
a universal uniqueness rule to retain that concrete conflict. This is a scope check,
not a blanket suppression rule for repeated labels.
'''

OUTPUT_PROFILE += '\n' + IDENTITY_SCOPE_PROFILE
