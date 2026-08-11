# OpenRouter

There is no central secret field. Status checks only the worker-local credential metadata and reports `configured`, never `verified`.

If absent, session becomes `openrouter_secret_required`. `provider-auth <session> openrouter` attaches a strict-host-key SSH TTY and runs hidden `read -s` on the VPS. The shell variable is written directly to `<data>/providers/openrouter/home/.openrouter/credentials.json`, mode 0600, then unset. It does not cross bootstrap stdin/stdout, API, DB, job, argv, history, repo or report.

No paid request is made to validate the key in 11K.
