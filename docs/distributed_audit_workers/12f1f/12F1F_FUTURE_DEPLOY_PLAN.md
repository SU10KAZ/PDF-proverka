# Future re-enrollment patch deployment plan

This plan is prepared but was **not executed** in 12F.1F. A new explicit
operator authorization is required even though the technical deploy gate is
ready.

1. Reverify production is still commit `e2b98c3b`, PID/restart state is stable,
   writers/deployers remain zero, and health is 200.
2. Back up the external Worker DB and effective production configuration.
3. Select exact durable release
   `/home/coder/auditmanager/releases/reenrollment-e6015d33`.
4. Perform one controlled `:8081` restart and verify core/distributed smoke;
   keep the scheduler disabled.
5. Create exactly one ADMIN authorization for
   `wrk_19c87718` + `inst_boot_e129036dddf5c59049080ddd15624e72`.
6. Deliver the one-time token to `.31` through the separately approved secure
   operator channel; never put it in argv, URL, logs, or evidence.
7. Run `CompleteIdentityReenrollment`; verify the exact IDs and new runtime
   credential. Do not import the old credential.
8. Configure the polling endpoint `https://auditmanager.app` and perform one
   controlled polling-Agent restart. Executor remains untouched.
9. Verify authenticated heartbeat, polling ownership, registry visibility,
   zero job offers/actions, and at least ten minutes of stable observation.
10. Produce a new `12F_RESUME` decision before any canary job or later cutover.

Immediate rollback is the durable release `ui-e2b98c3b`. Deep fallback is
`baseline-46bcd527`. Migration 13 is additive; do not delete or downgrade the
DB. Production deployment/restart authorization remains **false**.
