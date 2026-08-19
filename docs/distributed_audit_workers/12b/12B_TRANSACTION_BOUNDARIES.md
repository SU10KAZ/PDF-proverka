# Transaction boundaries and crash windows

| Boundary | Committed atomically | Network after commit | Recovery |
|---|---|---|---|
| connection hello | greater epoch, active connection, transport mode | CenterHello | lower/equal rejected after restart; higher reconnects |
| job claim | attempt state, transition, offer lease, transfer identity | JobOffer | replay outstanding offer or expire to assigned |
| event ingest | event rows, dedupe, contiguous cursor | EventAck | replay returns persisted cursor |
| cancel | existing command persistence/ACK transactions | CancelCommand | same pending command id redelivered |
| result | existing HTTPS validation and retention | ResultAck | outcome regenerated from persisted attempt |

There is no undefined database-write/network-send/database-write protocol whose final write is required for correctness. Delivery markers are diagnostic or retry state; authoritative domain state is committed before dependent acknowledgements.
