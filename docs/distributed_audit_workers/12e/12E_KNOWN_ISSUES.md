# Known issues and non-gates

- `DEDICATED VPS REBOOT PROOF = NOT_YET_TESTED`. Shared `.31` was not rebooted.
  Process-level Agent/Gateway/service recovery is proven and this is not a
  blocker for one explicitly authorized canary.
- `WINDOWS DPAPI = IMPLEMENTED_NOT_PHYSICALLY_PROVEN` remains inherited from
  12D and is irrelevant to the tested Linux `.31` canary.
- Production backend PID changed independently during the long 12E observation
  window. 12E never signalled or configured it; final command, loopback bind,
  HTTP 200 and production data roots are healthy. This churn should be watched
  during a future canary but is not a 12E isolation failure.
- The auxiliary gRPC test venv has newer FastAPI 0.141 router introspection;
  four route-probe tests pass in the authoritative project environment.
- Pre-existing production cloudflared PID 1263127 targeting
  `127.0.0.1:8081` is present and untouched. `USED BY 12E = NO`.
