# Offer recovery

An offer lease is durable before any network send. If the process dies before delivery, reconnect can replay the outstanding row. If it is not accepted before `expires_at`, recovery conditionally moves only `source_uploading` back to `assigned`, journals why, and marks the offer expired. A later scheduler pass may safely offer it again.

Delivery timestamp is diagnostic; `accepted` is the durable execution boundary. Therefore “sent” is never confused with “worker accepted,” and a claim cannot remain permanently stuck after the claim-before-send crash window.

See `12B_OFFER_RECOVERY.json` for machine-readable outcomes.
