# RBAC

Authorization creation and revocation use `require_admin`, the existing portal
actor resolver, permission `distributed_workers.admin`, the existing CSRF
intent header and mandatory idempotency key. Role data never comes from body,
query, machine headers or Worker token.

Tested results:

- admin: create/revoke allowed;
- viewer: denied;
- operator: denied despite having drain/resume permission;
- Worker bearer principal: denied by the portal contour;
- anonymous: denied by the portal contour.

Human drain/resume and historical identity authority remain separate scopes.
Production role configuration was not read, edited or restarted in Phase A.
