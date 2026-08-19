# Transport abstraction

`WorkerAgent` remains the only Agent core. `CenterClient` owns polling; the
explicit `GrpcStreamControlTransport` implements the same narrow control
surface. It delegates source/result bytes to the same `CenterClient`, so package
validation, `worker.db`, Executor, EventOutbox, uploader and retention logic are
not forked.

Selection is startup-only. Default is `polling`; `grpc_stream` requires a
controlled Agent restart and has no automatic polling fallback.
