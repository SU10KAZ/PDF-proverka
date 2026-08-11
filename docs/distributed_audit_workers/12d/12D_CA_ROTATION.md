# CA rotation

Trust bundles parse multiple CA certificates. Rotation sequence is: distribute
`old + new` roots, issue new server/Worker leaves, prove reconnects, then remove
the old root. Workers pin the bundle, not one leaf, so ordinary server-leaf
rotation requires no Worker reinstall. The overlap test uses two ephemeral roots.
