# Revocation flow

Center can revoke one serial or all ACTIVE leaves for a Worker. New RPCs with a
revoked/replaced leaf fail. Each active Agent stream rechecks registry state on
a configurable bounded interval and also has a certificate-expiry deadline.
Invalidation closes only the control stream; it does not kill an Executor or
delete job/audit history. Decommission additionally uses existing Worker
registration/scheduler controls.
