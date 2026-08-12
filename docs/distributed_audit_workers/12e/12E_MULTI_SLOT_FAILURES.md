# Multi-slot failures

The executed real-Agent local test proves that `max_slots=2` can complete two
attempts through one stream without a third concurrent Executor. The required
chaos variants remain C30 (stream loss for both), C31 (cancel A without
affecting B) and C32 (last-slot offer race).

Physical `.31` testing will use at most two synthetic slots and never increase
production Worker concurrency.
