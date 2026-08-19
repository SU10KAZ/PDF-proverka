# Linux key storage

`LINUX_KEY_STORAGE = OS_PERMISSION_PROTECTED`. A dedicated Worker user owns a
0700 identity directory and a 0600 PKCS#8 key. Writes use same-directory temp,
fsync, replace and directory fsync. Owner, regular-file type and modes are
validated; symlinks anywhere in the path are rejected. No claim of TPM-equivalent
machine binding is made.
