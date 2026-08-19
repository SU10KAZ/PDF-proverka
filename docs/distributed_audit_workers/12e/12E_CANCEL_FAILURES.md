# Cancel failure evidence

Verdict: `PASS` for C21–C24.

The durable `command_id` is the idempotency boundary. Online cancel stops only
the process whose PID/start identity/fingerprint matches. Offline and
Gateway-restart cases replay the same pending command after reconnect. A late
cancel of a terminal attempt returns the typed already-completed outcome and
preserves its result.

Physical C31 cancelled only job A (`3f99a4ed-47b5-4d95-9924-6b0306e9bbba`);
concurrent job B (`d3a51e17-7479-42d2-8244-f77b187fffc1`) completed with
sequence 216, validated ResultAck and retention. Final local state contains one
reported cancel command and no pending command.
