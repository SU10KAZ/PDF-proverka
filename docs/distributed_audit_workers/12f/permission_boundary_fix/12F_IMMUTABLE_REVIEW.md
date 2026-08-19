# Detached immutable review — permission-boundary-dd8c760e

The detached git archive SHA-256 is
`8a7a37428f69278df05e90e260fb02e9fd9ab45133c8b4b58a6bfcfb62d4c441`.
`diff -qr` found no difference between that archive and the durable release
`app/` tree. Bundle verification passed and the release has zero writable
non-symlink paths.

1. **RestrictSUIDSGID compatibility — PASS.** Real systemd old/fixed A/B proof;
   property remains true.
2. **Filesystem privilege boundary — PASS.** Only the explicit operator tool
   creates owner/group/SGID/default ACL state.
3. **No forbidden runtime chmod — PASS.** No runtime `chmod(02770)` or runtime
   `chown`; authenticated request under the kernel boundary passed.
4. **Unsafe permissions fail closed — PASS.** Wrong mode, owner and default ACL
   have typed negative coverage; real wrong-mode service never listened.
5. **Worker polling — PASS.** Commands and heartbeat were authenticated HTTP 200.
6. **PKI shared state — PASS.** Recovery, issuer and signing tests pass; existing
   production PKI was not touched.
7. **Key separation — PASS.** Gateway unit has no issuer key path or permission.
8. **Re-enrollment — PASS.** All 34 identity-preserving tests pass.
9. **Rollback — PASS.** Exact e601→dd8→e601 logical hash was stable.
10. **Release immutability — PASS.** Commit, parent, tree, archive, bundle,
    dependency freeze and write-bit checks agree.

Hardening delta: `RestrictSUIDSGID`, `NoNewPrivileges`, `ProtectSystem`,
`ProtectHome`, `ReadWritePaths`, `User`, `Group`, Gateway `UMask=0077`, issuer
`UMask=0007`, and empty service capability sets were not weakened. Unexpected
runtime files: zero. Review verdict: **PASS**.
