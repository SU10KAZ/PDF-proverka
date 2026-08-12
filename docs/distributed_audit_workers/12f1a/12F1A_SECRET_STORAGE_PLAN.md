# 12F.1A secret/config boundary

Production `.env` remains unchanged at mode `0664`. That mode is not suitable
for new worker bootstrap or issuer material.

The staged boundary is:

1. ordinary typed configuration in a versioned safe template;
2. runtime non-secret environment in a service-owned config file;
3. sensitive provider/session values in a separate mode-`0600` credential or
   env file outside releases;
4. worker CA/private issuer material in its existing protected issuer boundary,
   never in the web process or Git;
5. bootstrap enrollment through one-time, TTL- and instance-scoped tokens,
   hashed at rest and delivered through the proven secure admin exchange.

The current 12E code still accepts a fallback reusable
`DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET` and reports configuration failure when it
is absent. Production candidate work must remove/disable that fallback and make
the existing `wbt_` one-time token path canonical. No reusable raw bootstrap
credential belongs in production Center config.

No secret was generated, moved, chmodded or committed in 12F.1A.
