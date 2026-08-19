# 12F hardened shared-state permission boundary — final report

Verdict: **PASS / READY_FOR_PERMISSION_FIX_DEPLOY=YES**, with
`operator_deploy_authorization=false`. Phase A did not deploy or restart
production.

The root cause was an unconditional request-time `chmod(02770)` in candidate
`5e3750a2`. The production backend is `coder:coder`; issuer is
`web-ocr-cert-issuer:web-ocr-agent-gateway`; Gateway is
`web-ocr-agent-gateway:web-ocr-agent-gateway`. SGID is therefore required for
gid-984 inheritance, but only the deployment bootstrap may establish it.
Runtime now validates and fails early; authenticated request paths never chmod a
directory or chown state.

Fix commit `dd8c760e1bf21abcb061557efe2bfe436108ee51` is a direct child of
production `e6015d33…` and supersedes failed `5e3750a2` while retaining its
reviewed PKI changes. Durable release:
`/home/coder/auditmanager/releases/permission-boundary-dd8c760e`, tree
`955940d56e38ee5c2bb7f15d1e745d34a4f874c0`, bundle SHA-256
`60e16594700e3df6cc9e0a5e238d29f2ef3ff75137698d8270518ff0b9c500b5`.

Exact immutable tests: **155 passed** (106 critical including the prior exact
100 recovery subset, plus 49 Gateway). Real systemd A/B reproduced old EPERM
and proved new authenticated commands/heartbeat 200/200. Unsafe mode failed
before listener. Re-enrollment, isolated issuer, signing, idempotent partial PKI,
schema 13 and e601→new→e601 rollback all pass. Provider inference is 0/0/0.

Production remained on e601 PID 2522606, health 200, no restarts. Worker
`wrk_19c87718` remains online polling; Agent PID 2212836 and Executor PID
1384880 remain active without restart. Issuer/Gateway and 8443/9443 remain
inactive/absent. UFW, nginx and the independent production cloudflared target
127.0.0.1:8081 match baseline.

Future post-deploy PKI script:
`/tmp/12f_prepare_production_pki_permission_fixed.sh`, SHA-256
`eb563f41c0e63ffc9ec1ee460e76b51577cd0e57c28cae6f7ef3cb5e486c7da7`.
It is release-guarded and was not run. A future production deploy still requires
new explicit authorization. Push/merge: **NO/NO**.
