# Certificate profiles

Canonical Worker identity is exactly one URI SAN:
`urn:auditmanager:worker:<percent-encoded-worker_id>`. CN is ignored. Worker
leaves require `clientAuth` only, digitalSignature key usage, CA=false, unique
serial and bounded validity. Gateway leaves require an exact configured DNS or
IP SAN, `serverAuth` only and CA=false. Key/cert mismatch and unsafe server-key
permissions fail startup.

Certificates bind the stable `worker_id`; `instance_id` remains registry
metadata and is not part of the SAN, so a safe repair does not create a new
logical Worker.
