# 12D identity before

Read-only inventory on 2026-08-11 found Worker `wrk_19c87718`, instance
`inst_boot_e129036dddf5c59049080ddd15624e72`, running as dedicated Linux user
`auditworker_11l`. Its token and environment file were mode 0600. The Agent and
Executor user units were active. The control identity was still the hashed-at-
Center bearer token; no Worker certificate or private-key store existed.

The existing polling unit/config and provider homes are outside the isolated
12D identity directory and are not overwritten.
