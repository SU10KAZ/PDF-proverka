# Backpressure and reconnect herd

The control client has a bounded critical queue; heartbeat and capability
updates coalesce. Gateway local tests accepted and acknowledged 50 ordered
event batches and the completed 12B suite includes 20 concurrent fake Worker
connections.

The remaining C36 test must add an observed Gateway-unavailable/recovery phase
to those clients and record reconnect timing/jitter. It must not turn into a
host-wide load test.
