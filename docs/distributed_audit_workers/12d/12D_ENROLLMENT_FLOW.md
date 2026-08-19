# Enrollment flow

Bootstrap registers/approves the Worker, then the Worker-side KeyStore creates
or reuses a P-256 key and returns only a CSR through the existing SSH admin
plane. The bootstrap session and assigned worker ID authorize issuance. The
Center signs the CSR and returns public leaf/chain/bundle; Worker atomically
installs public files. Request ID `bootstrap-cert-<session>` and CSR hash make
lost-response retries deterministic. Provider login phases are not repeated.
