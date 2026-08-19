# Cutover stop conditions

Stop a future rollout immediately if any of these occurs: unstable mTLS,
reconnect storm, duplicate Executor, non-converging EventOutbox, failed source
or result transfer, stuck ResultAck, incorrect retention, ownership ambiguity,
certificate renewal/expiry fault, unexpected provider degradation, Gateway DB
errors, cross-worker authorization issue or any data corruption.

The safe immediate action is to stop **new offers** and preserve evidence. It
is never to start polling beside an unresolved gRPC attempt.
