# Key storage

KeyStore is platform-specific. Keys never enter environment, argv, gRPC/HTTP
payloads, job protocol, logs or Center storage. grpcio receives private-key bytes
directly from Worker process memory. No decrypted temporary key file exists.
