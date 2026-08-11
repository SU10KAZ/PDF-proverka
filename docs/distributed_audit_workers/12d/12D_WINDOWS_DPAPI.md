# Windows DPAPI

`WindowsDpapiKeyStore` uses `CryptProtectData` with machine scope and UI
forbidden, stores only the DPAPI blob, and decrypts into Worker memory for
grpcio. Linux has an explicit platform guard against selecting it. No Windows
host was available, therefore final status cannot exceed
`IMPLEMENTED_NOT_PHYSICALLY_PROVEN`.
