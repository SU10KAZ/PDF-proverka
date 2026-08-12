# Resource pressure

Disk pressure is simulated through worker health/slot abstractions; no shared
VPS disk will be filled. The existing local slot test proves a critical disk
signal drops free slots to zero. C37 still needs the end-to-end health and
offer rejection observation. C38 needs a concrete capability-ready →
unavailable transition while a fake job remains unaffected.

Provider inference remains disabled: Claude/Codex/OpenRouter = `0/0/0`.
