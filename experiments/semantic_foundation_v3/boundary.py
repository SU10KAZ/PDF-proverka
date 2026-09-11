"""Order-independent shared boundary decisions, including explicit abstention."""
from dataclasses import dataclass, asdict


@dataclass(frozen=True)
class BoundaryDecision:
    kind: str
    left_anchor: int | None
    right_anchor: int
    decision: str
    basis: str
    evidence_codes: tuple[str, ...]
    conflict: bool
    join_evidence: tuple[str, ...]
    split_evidence: tuple[str, ...]

    @classmethod
    def collect(cls, kind, left, right, *, join=(), split=(), observed=()):
        join, split, observed = set(join), set(split), set(observed)
        conflict = bool(join and split)
        if conflict:
            decision, basis = "REVIEW", "DEFAULT"
            observed.add("CONFLICT")
        elif not join and not split:
            decision, basis = "REVIEW", "DEFAULT"
            observed.add("NO_EVIDENCE")
        else:
            decision, basis = ("SAME" if join else "NEW"), "PROVEN"
        return cls(kind, left, right, decision, basis, tuple(sorted(join | split | observed)),
                   conflict, tuple(sorted(join)), tuple(sorted(split)))

    def artifact(self):
        return asdict(self)
