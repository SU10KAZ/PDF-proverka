# Bootstrap integration

11K/11L flow accepts explicit `grpc_stream + mtls`, persists those fields, syncs
the grpc/cryptography overlay, writes certificate paths into worker.env, and
adds a resumable certificate phase after assigned Worker approval but before
service start. Existing key/CSR is reused on resume. Polling sessions skip the
phase and retain old behavior.
