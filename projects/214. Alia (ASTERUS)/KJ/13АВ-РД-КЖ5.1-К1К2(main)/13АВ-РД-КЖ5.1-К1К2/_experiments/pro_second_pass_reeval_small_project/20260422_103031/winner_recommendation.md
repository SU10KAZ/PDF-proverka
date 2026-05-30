# Winner Recommendation

**Winner**: `claude_reused`

## Ranking logic

1. completeness
2. improved blocks
3. additional findings
4. degraded blocks
5. cost/time

## Pairwise Pro vs Claude

### pro_high_p2
- Pairwise winner vs Claude: `Claude (claude-opus-4-7)`
- Rationale: Claude (claude-opus-4-7) improved more blocks (15 vs 11) with acceptable degradations (1).
### pro_high_p1
- Pairwise winner vs Claude: `Claude (claude-opus-4-7)`
- Rationale: Claude (claude-opus-4-7) improved more blocks (15 vs 11) with acceptable degradations (1).
### pro_low_p2
- Pairwise winner vs Claude: `Claude (claude-opus-4-7)`
- Rationale: Claude (claude-opus-4-7) improved more blocks (15 vs 10) with acceptable degradations (1).

- Claude still wins as second-pass engine on the small KJ benchmark.
- Pro can return to the candidate set in: `pro_high_p2`, `pro_high_p1`, `pro_low_p2`.
- Low reasoning remains useful as a completeness/cost control, not the leading recall choice.
