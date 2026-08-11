# PKI architecture

`offline root → protected online issuing intermediate → leaves` is implemented.
The Gateway reads only its server key, server chain and Worker CA bundle. It
cannot read the issuing key. Renewal is proxied through a bounded Unix socket
to a separate issuer process; filesystem mode and `SO_PEERCRED` restrict the
caller. The issuer independently checks the durable certificate registry.

The bootstrap/admin Center may invoke the issuer boundary for first enrollment.
The public Agent Gateway has no initial “certificate for worker_id” endpoint.
No CA or leaf private key is stored in Git.
